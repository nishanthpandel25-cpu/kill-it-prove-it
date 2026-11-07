
import argparse
import multiprocessing
import os
import random
import tempfile

import src.recovery as recovery_module
from src.database import Database


RECORD_COUNT = 100
BATCH_RECORD_COUNT = 50
CRASH_EXIT_CODE = 69

CHECKPOINTS = [
    "record_replayed",
    "transaction_replayed",
    "before_truncate",
    "after_truncate",
]


def prepare_database(path, add_incomplete_tail):
    """Prepare standalone records and one committed batch."""
    db = Database(path)

    try:
        for index in range(RECORD_COUNT - BATCH_RECORD_COUNT):
            db.put(
                f"key_{index}".encode(),
                f"value_{index}".encode()
            )

        operations = [
            (
                "put",
                f"key_{index}".encode(),
                f"value_{index}".encode()
            )
            for index in range(
                RECORD_COUNT - BATCH_RECORD_COUNT,
                RECORD_COUNT
            )
        ]

        db.batch(operations)

    finally:
        db.close()

    if add_incomplete_tail:
        with open(path, "ab") as file:
            file.write(b"KVR1\x01\x00\x00")


def recovery_crash_worker(path, target_stage, target_count):
    """Exit abruptly at the selected recovery checkpoint."""
    observed = 0

    def checkpoint(stage):
        nonlocal observed

        if stage != target_stage:
            return

        observed += 1

        if observed == target_count:
            os._exit(CRASH_EXIT_CODE)

    recovery_module.RECOVERY_TEST_HOOK = checkpoint

    # Database construction invokes recovery.
    db = Database(path)
    db.close()


def verify_database(path):
    """Verify all committed records after interrupted recovery."""
    db = Database(path)

    try:
        if len(db.data) != RECORD_COUNT:
            raise AssertionError(
                f"Expected {RECORD_COUNT} records, "
                f"found {len(db.data)}"
            )

        for index in range(RECORD_COUNT):
            key = f"key_{index}".encode()
            expected = f"value_{index}".encode()
            actual = db.get(key)

            if actual != expected:
                raise AssertionError(
                    f"Incorrect recovery for {key!r}: "
                    f"expected {expected!r}, found {actual!r}"
                )

    finally:
        db.close()


def run_recovery_crash_torture(iterations, seed):
    rng = random.Random(seed)
    passed = 0

    with tempfile.TemporaryDirectory() as folder:
        for iteration in range(1, iterations + 1):
            path = os.path.join(
                folder,
                f"recovery_{iteration}.db"
            )

            target_stage = rng.choice(CHECKPOINTS)

            # Select a replay occurrence that exists in the prepared log.
            if target_stage == "record_replayed":
                target_count = rng.randint(1, RECORD_COUNT - BATCH_RECORD_COUNT)
            elif target_stage == "transaction_replayed":
                target_count = 1
            else:
                target_count = 1

            add_incomplete_tail = target_stage in (
                "before_truncate",
                "after_truncate",
            )

            prepare_database(path, add_incomplete_tail)

            process = multiprocessing.Process(
                target=recovery_crash_worker,
                args=(path, target_stage, target_count)
            )

            process.start()

            try:
                process.join(timeout=30)

                if process.is_alive():
                    process.kill()
                    process.join(timeout=15)
                    raise RuntimeError(
                        f"Iteration {iteration}: recovery worker timed out "
                        f"at {target_stage}"
                    )

                if process.exitcode != CRASH_EXIT_CODE:
                    raise RuntimeError(
                        f"Iteration {iteration}: expected exit code "
                        f"{CRASH_EXIT_CODE}, got {process.exitcode}; "
                        f"checkpoint={target_stage}"
                    )

                verify_database(path)

                passed += 1

                print(
                    f"Iteration {iteration}/{iterations}: "
                    f"PASSED | checkpoint={target_stage}"
                )

            finally:
                if process.is_alive():
                    process.kill()
                    process.join(timeout=15)

    print()
    print(f"Iterations passed: {passed}")
    print(f"Iterations failed: {iterations - passed}")

    if passed != iterations:
        raise RuntimeError("Recovery crash torture failed")

    print("RECOVERY PROCESS CRASH: PASSED")


def main():
    parser = argparse.ArgumentParser(
        description="Process crash torture during database recovery"
    )

    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)

    args = parser.parse_args()

    if args.iterations <= 0:
        parser.error("--iterations must be greater than zero")

    multiprocessing.freeze_support()

    run_recovery_crash_torture(
        iterations=args.iterations,
        seed=args.seed
    )


if __name__ == "__main__":
    main()
