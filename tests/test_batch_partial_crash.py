
import multiprocessing
import os
import tempfile

from src.database import Database
from src import storage


BATCH_SIZE = 10
CRASH_EXIT_CODE = 71


def crash_after_three_operations(path):
    """Kill the process after BEGIN and 3 PUT records are written."""

    db = Database(path)

    original_write = os.write
    write_count = 0

    def controlled_write(fd, data):
        nonlocal write_count

        # Call 1: BEGIN
        # Calls 2-4: First three PUT operations
        # Call 5: Kill before the fourth PUT is written.
        if write_count == 4:
            os._exit(CRASH_EXIT_CODE)

        result = original_write(fd, data)
        write_count += 1

        return result

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


class TestBatchPartialCrash:

    def test_crash_after_three_of_ten_batch_operations(self):

        with tempfile.TemporaryDirectory() as folder:

            path = os.path.join(folder, "partial_batch.db")

            # Create one previously committed record.
            db = Database(path)
            db.put(b"existing_key", b"existing_value")

            original_size = os.path.getsize(path)

            db.close()

            # Run the batch in a separate process.
            process = multiprocessing.Process(
                target=crash_after_three_operations,
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

            # Reopen the database to trigger recovery.
            db = Database(path)

            try:
                # Previously committed data must survive.
                assert db.get(b"existing_key") == b"existing_value"

                # Every operation in the interrupted batch
                # must be discarded.
                for i in range(BATCH_SIZE):
                    key = f"batch_key_{i}".encode()

                    assert db.get(key) is None, (
                        f"Partial batch operation was recovered: {key}"
                    )

                assert len(db.data) == 1, (
                    f"Expected 1 record, found {len(db.data)}"
                )

                # The incomplete transaction should be removed.
                assert os.path.getsize(path) == original_size, (
                    "Incomplete batch was not fully truncated"
                )

            finally:
                db.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
