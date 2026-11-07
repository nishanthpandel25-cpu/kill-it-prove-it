
import argparse
import multiprocessing
import os
import random
import tempfile

import src.database as database_module
from src.database import Database


RECORD_COUNT = 100
DELETE_COUNT = 50

CHECKPOINTS = [
    "temp_written",
    "before_replace",
    "after_replace",
]


def prepare_database(path):
    """Create a database with live and deleted records."""
    db = Database(path)

    try:
        for index in range(RECORD_COUNT):
            db.put(
                f"key_{index}".encode(),
                f"value_{index}".encode()
            )

        for index in range(DELETE_COUNT):
            db.delete(f"key_{index}".encode())

    finally:
        db.close()


def compaction_worker(path, target_stage, reached_event):
    """Run compaction and pause at the selected checkpoint."""
    db = Database(path)

    def checkpoint(stage):
        if stage == target_stage:
            reached_event.set()

            # Remain here until the parent terminates this process.
            multiprocessing.Event().wait()

    database_module.COMPACTION_TEST_HOOK = checkpoint
    db.compact()


def verify_database(path):
    """Verify the complete expected database state after recovery."""
    db = Database(path)

    try:
        expected_count = RECORD_COUNT - DELETE_COUNT

        if len(db.data) != expected_count:
            raise AssertionError(
                f"Expected {expected_count} records, "
                f"found {len(db.data)}"
            )

        for index in range(DELETE_COUNT):
            key = f"key_{index}".encode()

            if db.get(key) is not None:
                raise AssertionError(
                    f"Deleted key unexpectedly recovered: {key!r}"
                )

        for index in range(DELETE_COUNT, RECORD_COUNT):
            key = f"key_{index}".encode()
            expected_value = f"value_{index}".encode()
            actual_value = db.get(key)

            if actual_value != expected_value:
                raise AssertionError(
                    f"Incorrect value for {key!r}: "
                    f"expected {expected_value!r}, "
                    f"found {actual_value!r}"
                )

    finally:
        db.close()


def run_compaction_crash_torture(iterations, seed):
    rng = random.Random(seed)

    passed = 0

    with tempfile.TemporaryDirectory() as folder:
        for iteration in range(1, iterations + 1):
            path = os.path.join(
                folder,
                f"compaction_{iteration}.db"
            )

            target_stage = rng.choice(CHECKPOINTS)
            reached_event = multiprocessing.Event()

            prepare_database(path)

            process = multiprocessing.Process(
                target=compaction_worker,
                args=(path, target_stage, reached_event)
            )

            process.start()

            try:
                if not reached_event.wait(timeout=30):
                    raise RuntimeError(
                        f"Iteration {iteration}: worker did not reach "
                        f"checkpoint {target_stage}; "
                        f"exit code={process.exitcode}"
                    )

                # Abruptly terminate the worker at the checkpoint.
                process.kill()
                process.join(timeout=15)

                if process.is_alive():
                    raise RuntimeError(
                        f"Iteration {iteration}: worker did not terminate"
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
        raise RuntimeError("Compaction crash torture failed")

    print("COMPACTION PROCESS CRASH: PASSED")


def main():
    parser = argparse.ArgumentParser(
        description="Process crash torture for database compaction"
    )

    parser.add_argument(
        "--iterations",
        type=int,
        default=100
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=7
    )

    args = parser.parse_args()

    if args.iterations <= 0:
        parser.error("--iterations must be greater than zero")

    multiprocessing.freeze_support()

    run_compaction_crash_torture(
        iterations=args.iterations,
        seed=args.seed
    )


if __name__ == "__main__":
    main()
