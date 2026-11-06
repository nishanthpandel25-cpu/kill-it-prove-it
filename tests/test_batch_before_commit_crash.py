
import multiprocessing
import os
import tempfile

from src.database import Database
from src import storage


BATCH_SIZE = 10
CRASH_EXIT_CODE = 70


def crash_before_commit(path):
    """Kill the process immediately before writing COMMIT."""

    db = Database(path)

    original_write = os.write

    def controlled_write(fd, data):
        # Record format:
        # Bytes 0-3: Magic
        # Byte 4: Operation type
        operation = data[4]

        if operation == storage.COMMIT:
            # All 10 PUT records have already been written.
            # Kill before the COMMIT record is written.
            os._exit(CRASH_EXIT_CODE)

        return original_write(fd, data)

    storage.os.write = controlled_write

    operations = [
        (
            "put",
            f"batch_key_{i}".encode(),
            f"batch_value_{i}".encode()
        )
        for i in range(BATCH_SIZE)
    ]

    db.batch(operations)


class TestBatchBeforeCommitCrash:

    def test_crash_after_all_operations_before_commit(self):

        with tempfile.TemporaryDirectory() as folder:

            path = os.path.join(folder, "before_commit.db")

            # Create a previously committed record.
            db = Database(path)
            db.put(b"existing_key", b"existing_value")

            original_size = os.path.getsize(path)
            db.close()

            process = multiprocessing.Process(
                target=crash_before_commit,
                args=(path,)
            )

            process.start()
            process.join(timeout=15)

            assert not process.is_alive(), (
                "Batch process did not terminate"
            )

            assert process.exitcode == CRASH_EXIT_CODE, (
                f"Expected exit code {CRASH_EXIT_CODE}, "
                f"got {process.exitcode}"
            )

            # Reopen the database and trigger recovery.
            db = Database(path)

            try:
                # Previously committed data must remain.
                assert db.get(b"existing_key") == b"existing_value"

                # All 10 uncommitted operations must be discarded.
                for i in range(BATCH_SIZE):
                    key = f"batch_key_{i}".encode()

                    assert db.get(key) is None, (
                        f"Uncommitted operation was recovered: {key}"
                    )

                assert len(db.data) == 1, (
                    f"Expected 1 record, found {len(db.data)}"
                )

                assert os.path.getsize(path) == original_size, (
                    "Incomplete transaction was not truncated"
                )

            finally:
                db.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
