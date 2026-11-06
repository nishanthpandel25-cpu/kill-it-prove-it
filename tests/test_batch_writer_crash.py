
import multiprocessing
import os
import tempfile
import time

import src.storage as storage_module
from src.database import Database


BATCH_SIZE = 10000
SINGLE_WRITES = 30


def batch_worker(path, connection):
    """Pause a large batch after its first partial physical write."""

    db = None

    try:
        db = Database(path)

        original_write = os.write
        first_write = True

        def controlled_write(fd, data):
            nonlocal first_write

            if first_write:
                first_write = False

                # Write only five bytes of the first batch record.
                written = original_write(fd, data[:5])

                connection.send(("partial_write", written))

                # Wait for the parent to terminate this process.
                # The parent deliberately sends no response.
                connection.recv()

                return written

            return original_write(fd, data)

        storage_module.os.write = controlled_write

        operations = [
            (
                "put",
                f"batch_key_{index}".encode(),
                f"batch_value_{index}".encode()
            )
            for index in range(BATCH_SIZE)
        ]

        db.batch(operations)

    finally:
        if db is not None:
            db.close()

        connection.close()


def individual_writer(path, connection):
    """Attempt individual PUTs while the batch holds the database lock."""

    db = None

    try:
        # Tell the parent this process is about to open the database.
        connection.send(("started",))

        # This waits for the batch process to release its lock.
        db = Database(path)

        acknowledged = []

        for index in range(SINGLE_WRITES):

            key = f"single_key_{index}".encode()
            value = f"single_value_{index}".encode()

            db.put(key, value)

            # Record only successful, acknowledged writes.
            acknowledged.append((key, value))

        connection.send(("done", acknowledged))

    except Exception as error:

        try:
            connection.send(("error", repr(error)))
        except (BrokenPipeError, EOFError, OSError):
            pass

    finally:
        if db is not None:
            db.close()

        connection.close()


class TestBatchAndIndividualWriterCrash:

    def test_long_batch_and_single_puts_during_crash(self):

        with tempfile.TemporaryDirectory() as folder:

            path = os.path.join(folder, "batch_writer_crash.db")

            # Create one previously committed record.
            db = Database(path)
            db.put(b"existing", b"preserve_me")
            db.close()

            # Create a pipe for the batch worker.
            batch_parent, batch_child = multiprocessing.Pipe()

            batch_process = multiprocessing.Process(
                target=batch_worker,
                args=(path, batch_child)
            )

            batch_process.start()
            batch_child.close()

            writer_parent = None
            writer_process = None

            try:

                # Wait until the batch has written five bytes.
                assert batch_parent.poll(30), (
                    "Batch did not reach its partial-write checkpoint"
                )

                message = batch_parent.recv()

                assert message[0] == "partial_write"
                assert message[1] == 5

                # Start the individual writer while the batch
                # process is paused and holds the database lock.
                writer_parent, writer_child = multiprocessing.Pipe()

                writer_process = multiprocessing.Process(
                    target=individual_writer,
                    args=(path, writer_child)
                )

                writer_process.start()
                writer_child.close()

                assert writer_parent.poll(15), (
                    "Individual writer did not start"
                )

                message = writer_parent.recv()

                assert message[0] == "started"

                # Give the individual writer time to attempt
                # acquiring the database lock.
                time.sleep(0.2)

                # Kill the batch while its transaction is incomplete.
                batch_process.terminate()
                batch_process.join(timeout=15)

                assert not batch_process.is_alive(), (
                    "Batch process did not terminate"
                )

                assert batch_process.exitcode is not None
                assert batch_process.exitcode != 0

                # The individual writer should now acquire the lock,
                # recover the incomplete batch and finish its PUTs.
                assert writer_parent.poll(60), (
                    "Individual writer did not finish after batch termination"
                )

                result = writer_parent.recv()

                if result[0] == "error":
                    raise RuntimeError(result[1])

                assert result[0] == "done"

                acknowledged = result[1]

                writer_process.join(timeout=15)

                assert not writer_process.is_alive()
                assert writer_process.exitcode == 0

                assert len(acknowledged) == SINGLE_WRITES

            finally:

                if batch_process.is_alive():
                    batch_process.terminate()

                batch_process.join(timeout=10)
                batch_parent.close()

                if writer_process is not None:

                    if writer_process.is_alive():
                        writer_process.terminate()

                    writer_process.join(timeout=10)

                if writer_parent is not None:
                    writer_parent.close()

            # Reopen and verify recovery.
            db = Database(path)

            try:

                assert db.get(b"existing") == b"preserve_me"

                # Every acknowledged individual PUT must survive.
                for key, expected_value in acknowledged:

                    assert db.get(key) == expected_value, (
                        f"Acknowledged PUT was lost: {key!r}"
                    )

                # No operation from the interrupted batch may appear.
                for index in range(BATCH_SIZE):

                    key = f"batch_key_{index}".encode()

                    assert db.get(key) is None, (
                        f"Partial batch operation recovered: {key!r}"
                    )

                expected_records = SINGLE_WRITES + 1

                assert len(db.data) == expected_records

                print("Batch operations attempted:", BATCH_SIZE)
                print("Partial bytes written before crash: 5")
                print("Individual PUTs acknowledged:", len(acknowledged))
                print("Recovered records:", len(db.data))
                print("Incomplete batch: discarded")
                print("Recovery verification: PASSED")

            finally:
                db.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
