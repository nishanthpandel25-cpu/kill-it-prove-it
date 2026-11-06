
import multiprocessing
import os
import tempfile

from src.database import Database
from src import storage


def crash_during_delete_process(path):
    """Interrupt DELETE after writing only part of its record."""

    db = Database(path)

    # Create a valid record first.
    db.put(b"important_key", b"original_value")

    # Save the original function before replacing it.
    original_write = os.write

    def partial_delete_write(fd, data):
        # Use the original function to write only 5 bytes.
        original_write(fd, data[:5])

        # Terminate without rollback or cleanup.
        os._exit(73)

    storage.os.write = partial_delete_write

    db.delete(b"important_key")


class TestDeleteCrash:

    def test_interrupted_delete_preserves_original_value(self):

        with tempfile.TemporaryDirectory() as folder:

            path = os.path.join(folder, "delete_crash.db")

            process = multiprocessing.Process(
                target=crash_during_delete_process,
                args=(path,)
            )

            process.start()
            process.join(timeout=10)

            assert not process.is_alive(), (
                "DELETE process did not terminate"
            )

            assert process.exitcode == 73, (
                f"Expected exit code 73, got {process.exitcode}"
            )

            # Reopen the database after the crash.
            db = Database(path)

            try:
                actual = db.get(b"important_key")

                assert actual == b"original_value", (
                    "Interrupted DELETE incorrectly removed "
                    "the original value"
                )

                assert len(db.data) == 1, (
                    f"Expected 1 record, found {len(db.data)}"
                )

            finally:
                db.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
