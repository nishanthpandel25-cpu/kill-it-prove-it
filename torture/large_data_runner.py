
import os
import tempfile

from src.database import Database
from torture.oracle import Oracle


MAX_VALUE_SIZE = 1024 * 1024
BATCH_SIZE = 10_000


def generate_value(index, size):
    """Generate a deterministic byte value of exactly the requested size."""
    prefix = f"value_{index}_".encode()
    repeated = (prefix * ((size // len(prefix)) + 1))[:size]
    return repeated


def run_large_data_test():
    print("\n========== LARGE DATA TORTURE ==========")

    oracle = Oracle()

    with tempfile.TemporaryDirectory() as folder:
        path = os.path.join(folder, "large_data.db")

        db = Database(path)

        try:
            # Test values of different sizes, including exactly 1 MB.
            value_sizes = [
                1,
                1024,
                64 * 1024,
                256 * 1024,
                512 * 1024,
                MAX_VALUE_SIZE
            ]

            print("\nTesting large values:")

            for index, size in enumerate(value_sizes):
                key = f"large_value_{index}".encode()
                value = generate_value(index, size)

                db.put(key, value)
                oracle.apply(("put", key, value))

                actual = db.get(key)

                assert actual == value, (
                    f"Large value verification failed at size {size}"
                )

                print(
                    f"Value {index + 1}: "
                    f"{size:,} bytes - PASSED"
                )

            # Test a batch containing exactly 10,000 PUT operations.
            print("\nTesting 10,000-operation batch...")

            batch_operations = []

            for index in range(BATCH_SIZE):
                key = f"batch_key_{index}".encode()
                value = f"batch_value_{index}".encode()

                batch_operations.append(
                    ("put", key, value)
                )

            db.batch(batch_operations)

            # Update the independent oracle only after batch succeeds.
            oracle.apply_all(batch_operations)

            print("Batch operation count:", len(batch_operations))
            print("Batch execution: PASSED")

            # Verify every batch record before restart.
            for index in range(BATCH_SIZE):
                key = f"batch_key_{index}".encode()
                expected = f"batch_value_{index}".encode()

                assert db.get(key) == expected, (
                    f"Batch verification failed for {key!r}"
                )

            print("Pre-restart batch verification: PASSED")

            expected_count = len(oracle.data)

            print("Expected total records:", expected_count)

        finally:
            db.close()

        # Reopen the database to test recovery.
        print("\nReopening database...")

        db = Database(path)

        try:
            actual_data = db.data.copy()

            assert oracle.verify(actual_data), (
                "Recovered database does not match the oracle"
            )

            # Verify the large values after recovery.
            for index, size in enumerate(value_sizes):
                key = f"large_value_{index}".encode()
                expected = generate_value(index, size)

                assert db.get(key) == expected, (
                    f"Large value recovery failed at size {size}"
                )

            # Verify all 10,000 batch records after recovery.
            for index in range(BATCH_SIZE):
                key = f"batch_key_{index}".encode()
                expected = f"batch_value_{index}".encode()

                assert db.get(key) == expected, (
                    f"Batch recovery failed for {key!r}"
                )

            print("Expected records:", expected_count)
            print("Recovered records:", len(actual_data))
            print("Oracle comparison: PASSED")
            print("Large value recovery: PASSED")
            print("10,000-operation batch recovery: PASSED")

        finally:
            db.close()

    print("\nLARGE DATA TORTURE: PASSED")
    print("========================================\n")


if __name__ == "__main__":
    run_large_data_test()
