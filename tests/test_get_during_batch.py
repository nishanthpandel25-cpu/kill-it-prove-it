
import multiprocessing
import os
import tempfile

import src.database as database_module
from src.database import Database


BATCH_SIZE = 100
INITIAL_KEY = b"status"
INITIAL_VALUE = b"before"
UPDATED_VALUE = b"after"


def batch_worker(path, checkpoint_connection):
    """Pause the batch after fsync but before index update."""

    db = Database(path)

    def checkpoint(stage):

        if stage == "after_sync":
            checkpoint_connection.send("batch_synced")

            # Wait until the parent allows the batch to continue.
            if checkpoint_connection.recv() != "continue":
                raise RuntimeError("Unexpected batch instruction")

    database_module.BATCH_TEST_HOOK = checkpoint

    try:

        operations = [
            ("put", INITIAL_KEY, UPDATED_VALUE)
        ]

        for index in range(BATCH_SIZE):
            operations.append(
                (
                    "put",
                    f"batch_key_{index}".encode(),
                    f"batch_value_{index}".encode()
                )
            )

        db.batch(operations)

        checkpoint_connection.send("batch_done")

    finally:
        db.close()
        checkpoint_connection.close()


def reader_worker(path, result_connection):
    """Attempt GET while the batch process holds the file lock."""

    db = Database(path)

    try:

        # The GET must wait until the batch releases its lock.
        value = db.get(INITIAL_KEY)

        # Read every key written by the batch.
        records = {}

        for index in range(BATCH_SIZE):

            key = f"batch_key_{index}".encode()
            records[key] = db.get(key)

        result_connection.send(
            ("result", value, records)
        )

    except Exception as error:

        result_connection.send(
            ("error", repr(error))
        )

    finally:
        db.close()
        result_connection.close()


class TestGetDuringBatch:

    def test_get_never_observes_partial_batch(self):

        with tempfile.TemporaryDirectory() as folder:

            path = os.path.join(folder, "get_during_batch.db")

            # Create the initial database state.
            db = Database(path)
            db.put(INITIAL_KEY, INITIAL_VALUE)
            db.close()

            batch_parent, batch_child = multiprocessing.Pipe()

            batch_process = multiprocessing.Process(
                target=batch_worker,
                args=(path, batch_child)
            )

            reader_parent = None
            reader_process = None

            try:

                # Start the batch.
                batch_process.start()
                batch_child.close()

                # Wait until the batch has been fsynced.
                assert batch_parent.poll(30), (
                    "Batch did not reach after_sync checkpoint"
                )

                assert batch_parent.recv() == "batch_synced"

                # Start the reader while the batch is paused.
                reader_parent, reader_child = multiprocessing.Pipe()

                reader_process = multiprocessing.Process(
                    target=reader_worker,
                    args=(path, reader_child)
                )

                reader_process.start()
                reader_child.close()

                # Give the reader time to attempt its GET.
                if reader_parent.poll(0.5):
                    message = reader_parent.recv()

                    raise AssertionError(
                        "Reader returned while the batch was "
                        f"holding its lock: {message!r}"
                    )

                # Allow the batch to update its index and finish.
                batch_parent.send("continue")

                assert batch_parent.poll(30), (
                    "Batch did not finish"
                )

                assert batch_parent.recv() == "batch_done"

                batch_process.join(timeout=15)

                assert not batch_process.is_alive()
                assert batch_process.exitcode == 0

                # The reader should now complete successfully.
                assert reader_parent.poll(30), (
                    "Reader did not finish after batch completion"
                )

                result = reader_parent.recv()

                if result[0] == "error":
                    raise RuntimeError(result[1])

                assert result[0] == "result"

                _, observed_value, observed_records = result

                # The reader must see the complete after-state.
                assert observed_value == UPDATED_VALUE

                assert len(observed_records) == BATCH_SIZE

                for index in range(BATCH_SIZE):

                    key = f"batch_key_{index}".encode()
                    expected = f"batch_value_{index}".encode()

                    assert observed_records[key] == expected

                print("Batch operations:", BATCH_SIZE + 1)
                print("Initial value:", INITIAL_VALUE)
                print("Reader observed value:", observed_value)
                print("Batch records observed:", len(observed_records))
                print("Partial batch visibility: NONE")
                print("Atomicity verification: PASSED")

            finally:

                if batch_process.is_alive():
                    batch_process.terminate()

                batch_process.join(timeout=10)
                batch_parent.close()

                if reader_process is not None:

                    if reader_process.is_alive():
                        reader_process.terminate()

                    reader_process.join(timeout=10)

                if reader_parent is not None:
                    reader_parent.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
