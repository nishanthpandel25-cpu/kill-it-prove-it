
import multiprocessing
import os
import tempfile

import src.recovery as recovery_module
from src.database import Database


RECORD_COUNT = 20
CRASH_AFTER_RECORDS = 3
CRASH_EXIT_CODE = 66


def crash_during_recovery(path):
    """Terminate recovery after replaying three records."""

    replayed = 0

    def controlled_checkpoint(stage):
        nonlocal replayed

        if stage == "record_replayed":
            replayed += 1

            if replayed == CRASH_AFTER_RECORDS:
                os._exit(CRASH_EXIT_CODE)

    recovery_module.RECOVERY_TEST_HOOK = controlled_checkpoint

    # Opening the database starts recovery.
    db = Database(path)
    db.close()


class TestRecoveryCrash:

    def test_process_killed_during_recovery_replay(self):

        with tempfile.TemporaryDirectory() as folder:

            path = os.path.join(folder, "recovery_crash.db")

            # Prepare a database with 20 valid records.
            db = Database(path)

            for index in range(RECORD_COUNT):
                db.put(
                    f"key_{index}".encode(),
                    f"value_{index}".encode()
                )

            db.close()

            # Start a separate process that will be killed
            # during recovery replay.
            process = multiprocessing.Process(
                target=crash_during_recovery,
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

            # Restart again. Recovery must safely replay
            # the complete database from the beginning.
            db = Database(path)

            try:
                for index in range(RECORD_COUNT):

                    key = f"key_{index}".encode()
                    expected = f"value_{index}".encode()

                    assert db.get(key) == expected, (
                        f"Record {index} was lost after recovery restart"
                    )

                assert len(db.data) == RECORD_COUNT

            finally:
                db.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
