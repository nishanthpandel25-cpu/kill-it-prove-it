
import multiprocessing
import os
import tempfile

import src.database as database_module
from src.database import Database


BATCH_SIZE = 10
CRASH_EXIT_CODE = 68


def batch_worker(path, connection):
    """Execute a batch and pause immediately after successful fsync."""

    db = Database(path)

    try:
        operations = [
            (
                "put",
                f"batch_key_{i}".encode(),
                f"batch_value_{i}".encode()
            )
            for i in range(BATCH_SIZE)
        ]

        def checkpoint(stage):
            if stage == "after_sync":
                connection.send("fsync_completed")

                # Wait until the parent terminates this process.
                connection.recv()

        database_module.BATCH_TEST_HOOK = checkpoint

        db.batch(operations)

    finally:
        database_module.BATCH_TEST_HOOK = None
        db.close()
        connection.close()


class TestBatchAfterSyncCrash:

    def test_batch_survives_crash_after_fsync_before_acknowledgement(self):

        with tempfile.TemporaryDirectory() as folder:

            path = os.path.join(folder, "after_sync.db")

            # Create previously committed data.
            db = Database(path)
            db.put(b"existing_key", b"existing_value")
            db.close()

            parent_connection, child_connection = (
                multiprocessing.Pipe(duplex=True)
            )

            process = multiprocessing.Process(
                target=batch_worker,
                args=(path, child_connection)
            )

            process.start()
            child_connection.close()

            try:
                # Wait until the child confirms fsync completed.
                assert parent_connection.poll(30), (
                    "Worker did not reach the after_sync checkpoint"
                )

                message = parent_connection.recv()

                assert message == "fsync_completed", (
                    f"Unexpected worker message: {message}"
                )

                # Kill the process before batch() returns.
                process.terminate()
                process.join(timeout=15)

                assert not process.is_alive(), (
                    "Batch process did not terminate"
                )

                # Recover the database in a fresh instance.
                db = Database(path)

                try:
                    assert db.get(b"existing_key") == b"existing_value"

                    # All 10 operations must be recovered.
                    for i in range(BATCH_SIZE):

                        key = f"batch_key_{i}".encode()
                        expected = f"batch_value_{i}".encode()

                        actual = db.get(key)

                        assert actual == expected, (
                            f"Batch operation {i} was not recovered"
                        )

                    assert len(db.data) == BATCH_SIZE + 1, (
                        f"Expected {BATCH_SIZE + 1} records, "
                        f"recovered {len(db.data)}"
                    )

                finally:
                    db.close()

            finally:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=15)

                parent_connection.close()

                if process.exitcode is not None:
                    process.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
