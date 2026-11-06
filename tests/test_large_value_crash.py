
import multiprocessing
import os
import tempfile

from src.database import Database
from src import storage


ONE_MB = 1024 * 1024


def crash_during_large_put_process(path):
    """Write part of a 1 MB value, then terminate."""

    db = Database(path)

    original_write = os.write

    def partial_large_write(fd, data):
        # Write only the first 4 KB of the record.
        original_write(fd, data[:4096])

        # Terminate before the remaining bytes are written.
        os._exit(72)

    storage.os.write = partial_large_write

    large_value = b"A" * ONE_MB

    db.put(b"large_key", large_value)


class TestLargeValueCrash:

    def test_crash_during_one_mb_put(self):

        with tempfile.TemporaryDirectory() as folder:

            path = os.path.join(folder, "large_crash.db")

            # Create an existing record before the crash.
            db = Database(path)

            db.put(b"existing_key", b"existing_value")

            original_size = os.path.getsize(path)

            db.close()

            # Start a separate process for the large PUT.
            process = multiprocessing.Process(
                target=crash_during_large_put_process,
                args=(path,)
            )

            process.start()
            process.join(timeout=15)

            assert not process.is_alive(), (
                "Large PUT process did not terminate"
            )

            assert process.exitcode == 72, (
                f"Expected exit code 72, got {process.exitcode}"
            )

            # Reopen the database after the crash.
            db = Database(path)

            try:
                # Previously committed data must survive.
                assert db.get(b"existing_key") == b"existing_value", (
                    "Existing record was lost"
                )

                # Incomplete 1 MB record must not appear.
                assert db.get(b"large_key") is None, (
                    "Incomplete large value was incorrectly recovered"
                )

                assert len(db.data) == 1, (
                    f"Expected 1 record, found {len(db.data)}"
                )

                # Recovery must truncate the incomplete record.
                assert os.path.getsize(path) == original_size, (
                    "Incomplete large record was not truncated"
                )

            finally:
                db.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
