
import multiprocessing
import os
import tempfile

from src.database import Database
from src import storage


BATCH_SIZE = 10
CRASH_EXIT_CODE = 69


def crash_after_commit_before_fsync(path):
    """Write the complete batch, then terminate before fsync."""

    db = Database(path)

    original_fsync = os.fsync

    def kill_before_fsync(fd):
        # append_records() calls fsync only after writing
        # BEGIN, all operations, and COMMIT.
        os._exit(CRASH_EXIT_CODE)

    storage.os.fsync = kill_before_fsync

    operations = [
        (
            "put",
            f"batch_key_{i}".encode(),
            f"batch_value_{i}".encode()
        )
        for i in range(BATCH_SIZE)
    ]

    db.batch(operations)


class TestBatchCommitBeforeFsync:

    def test_crash_after_commit_record_before_fsync(self):

        with tempfile.TemporaryDirectory() as folder:

            path = os.path.join(folder, "commit_before_fsync.db")

            # Save one previously committed record.
            db = Database(path)
            db.put(b"existing_key", b"existing_value")
            db.close()

            process = multiprocessing.Process(
                target=crash_after_commit_before_fsync,
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

            # Recover the database.
            db = Database(path)

            try:
                assert db.get(b"existing_key") == b"existing_value"

                recovered_batch_count = 0

                for i in range(BATCH_SIZE):

                    key = f"batch_key_{i}".encode()
                    expected = f"batch_value_{i}".encode()

                    actual = db.get(key)

                    # A complete valid COMMIT record may be recovered
                    # even though fsync was not reached.
                    if actual is not None:
                        assert actual == expected
                        recovered_batch_count += 1

                assert recovered_batch_count in (0, BATCH_SIZE), (
                    "Partial batch recovery detected: "
                    f"{recovered_batch_count}/{BATCH_SIZE}"
                )

                print(
                    "\nRecovered batch operations:",
                    recovered_batch_count
                )

            finally:
                db.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
