
import argparse
import os
import random
import tempfile

from src.database import Database
from src.storage import DatabaseFullError


def expect_disk_full(operation, description):
    """Require an operation to fail because the database is full."""
    try:
        operation()
    except DatabaseFullError:
        return

    raise AssertionError(
        f"{description}: expected DatabaseFullError, but operation succeeded"
    )


def run_disk_full_torture(iterations, seed):
    rng = random.Random(seed)
    passed = 0

    print("========== DISK FULL TORTURE ==========")
    print("Iterations:", iterations)
    print("Seed:", seed)

    with tempfile.TemporaryDirectory() as folder:
        for iteration in range(1, iterations + 1):
            path = os.path.join(folder, f"disk_full_{iteration}.db")

            # Prepare previously committed data.
            original = {
                b"account": b"active",
                b"balance": str(rng.randint(1000, 999999)).encode(),
            }

            db = Database(path)

            try:
                for key, value in original.items():
                    db.put(key, value)
            finally:
                db.close()

            original_size = os.path.getsize(path)

            # Set the limit to the current file size.
            db = Database(path, max_size=original_size)

            try:
                expect_disk_full(
                    lambda: db.put(
                        b"new_account",
                        b"new_value_" + str(rng.random()).encode()
                    ),
                    "PUT"
                )

                if os.path.getsize(path) != original_size:
                    raise AssertionError(
                        "File size changed after rejected PUT"
                    )

                expect_disk_full(
                    lambda: db.delete(b"account"),
                    "DELETE"
                )

                if os.path.getsize(path) != original_size:
                    raise AssertionError(
                        "File size changed after rejected DELETE"
                    )

                batch_operations = [
                    ("put", b"batch_key_1", b"batch_value_1"),
                    ("put", b"batch_key_2", b"batch_value_2"),
                ]

                expect_disk_full(
                    lambda: db.batch(batch_operations),
                    "BATCH"
                )

                if os.path.getsize(path) != original_size:
                    raise AssertionError(
                        "File size changed after rejected BATCH"
                    )

                # The original records must remain available.
                for key, value in original.items():
                    if db.get(key) != value:
                        raise AssertionError(
                            f"Existing record changed: {key!r}"
                        )

                if db.get(b"new_account") is not None:
                    raise AssertionError(
                        "Rejected PUT unexpectedly changed database"
                    )

                if db.get(b"batch_key_1") is not None:
                    raise AssertionError(
                        "Rejected batch partially changed database"
                    )

                if db.get(b"batch_key_2") is not None:
                    raise AssertionError(
                        "Rejected batch partially changed database"
                    )

            finally:
                db.close()

            # Reopen without a size restriction and verify persistence.
            reopened = Database(path)

            try:
                for key, value in original.items():
                    if reopened.get(key) != value:
                        raise AssertionError(
                            f"Recovery mismatch for {key!r}"
                        )

                if reopened.get(b"new_account") is not None:
                    raise AssertionError(
                        "Rejected PUT appeared after restart"
                    )

                if reopened.get(b"batch_key_1") is not None:
                    raise AssertionError(
                        "Rejected batch appeared after restart"
                    )

                if reopened.get(b"batch_key_2") is not None:
                    raise AssertionError(
                        "Rejected batch appeared after restart"
                    )

            finally:
                reopened.close()

            passed += 1

            print(
                f"Iteration {iteration}/{iterations}: PASSED | "
                "PUT rejected | DELETE rejected | "
                "BATCH rejected | recovery verified",
                flush=True
            )

    print()
    print("Iterations passed:", passed)
    print("Iterations failed:", iterations - passed)
    print("DISK FULL TORTURE: PASSED")


def main():
    parser = argparse.ArgumentParser(
        description="Database capacity-limit and rollback torture test"
    )

    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)

    args = parser.parse_args()

    if args.iterations <= 0:
        parser.error("--iterations must be greater than zero")

    run_disk_full_torture(args.iterations, args.seed)


if __name__ == "__main__":
    main()
