
import multiprocessing
import os
import tempfile

import src.recovery as recovery_module
from src.database import Database


CRASH_EXIT_CODE = 68


def crash_after_truncation(path):
    """Terminate immediately after recovery truncates the invalid tail."""

    def controlled_checkpoint(stage):

        if stage == "after_truncate":
            os._exit(CRASH_EXIT_CODE)

    recovery_module.RECOVERY_TEST_HOOK = controlled_checkpoint

    db = Database(path)
    db.close()


class TestRecoveryTruncationCrash:

    def test_process_killed_after_torn_tail_truncation(self):

        with tempfile.TemporaryDirectory() as folder:

            path = os.path.join(folder, "truncation_crash.db")

            # Create one valid committed record.
            db = Database(path)
            db.put(b"name", b"Nishanth")
            db.close()

            # Append an incomplete record to simulate a crash.
            with open(path, "ab") as file:
                file.write(b"KVR1\x01\x00\x00")

            original_size = os.path.getsize(path)

            # Run recovery in a separate process.
            process = multiprocessing.Process(
                target=crash_after_truncation,
                args=(path,)
            )

            process.start()
            process.join(timeout=15)

            assert not process.is_alive(), (
                "Recovery process did not terminate"
            )

            assert process.exitcode == CRASH_EXIT_CODE, (
                f"Expected exit code {CRASH_EXIT_CODE}, "
                f"got {process.exitcode}"
            )

            truncated_size = os.path.getsize(path)

            assert truncated_size < original_size, (
                "The incomplete tail was not truncated"
            )

            # Start recovery again after the forced termination.
            db = Database(path)

            try:

                assert db.get(b"name") == b"Nishanth", (
                    "Previously committed data was lost"
                )

                assert len(db.data) == 1, (
                    "Unexpected records found after recovery"
                )

                assert os.path.getsize(path) == truncated_size, (
                    "Recovery changed the already-truncated file"
                )

            finally:
                db.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
