
import multiprocessing
import os

import src.recovery as recovery_module
from src.database import Database


RECORD_COUNT = 100
CRASH_AFTER_RECORDS = 3
CRASH_EXIT_CODE = 67
RECOVERY_CRASH_COUNT = 3


def crash_during_recovery(path):
    """Terminate the process after replaying three records."""

    replayed = 0

    def controlled_checkpoint(stage):
        nonlocal replayed

        if stage == "record_replayed":
            replayed += 1

            if replayed == CRASH_AFTER_RECORDS:
                os._exit(CRASH_EXIT_CODE)

    recovery_module.RECOVERY_TEST_HOOK = controlled_checkpoint

    # Database construction triggers recovery.
    db = Database(path)
    db.close()


def verify_all_records(path):
    """Open normally and verify every acknowledged record."""

    db = Database(path)

    try:
        assert len(db.data) == RECORD_COUNT

        for index in range(RECORD_COUNT):
            key = f"key_{index}".encode()
            expected = f"value_{index}".encode()

            assert db.get(key) == expected, (
                f"Record {index} is missing or incorrect"
            )

    finally:
        db.close()


def test_repeated_kills_during_recovery_are_idempotent(tmp_path):
    """
    F2:
    Kill during recovery, restart, kill again,
    restart, kill again, and finally recover normally.
    """

    path = str(tmp_path / "repeated_recovery.db")

    # Step 1: Create a database with acknowledged records.
    db = Database(path)

    try:
        for index in range(RECORD_COUNT):
            db.put(
                f"key_{index}".encode(),
                f"value_{index}".encode()
            )
    finally:
        db.close()

    # Step 2: Interrupt recovery three consecutive times.
    for attempt in range(1, RECOVERY_CRASH_COUNT + 1):

        process = multiprocessing.Process(
            target=crash_during_recovery,
            args=(path,)
        )

        process.start()
        process.join(timeout=30)

        if process.is_alive():
            process.terminate()
            process.join(timeout=10)
            raise AssertionError(
                f"Recovery attempt {attempt} timed out"
            )

        assert process.exitcode == CRASH_EXIT_CODE, (
            f"Attempt {attempt}: expected exit code "
            f"{CRASH_EXIT_CODE}, got {process.exitcode}"
        )

    # Step 3: Restart without injecting another crash.
    # Every previously acknowledged record must survive.
    verify_all_records(path)
