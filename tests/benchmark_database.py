
import os
import time
import tempfile

from src.database import Database


def run_benchmark(record_count=100000, batch_size=1000):

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "benchmark.db")

        print("\n========== BULK DATABASE BENCHMARK ==========")
        print("Total records:", record_count)
        print("Batch size:", batch_size)

        db = Database(path)

        insertion_start = time.perf_counter()

        for start in range(0, record_count, batch_size):

            end = min(start + batch_size, record_count)

            operations = []

            for i in range(start, end):
                key = f"benchmark_key_{i}".encode()
                value = f"benchmark_value_{i}".encode()

                operations.append(("put", key, value))

            db.batch(operations)

            print(f"Inserted {end}/{record_count} records")

        insertion_time = time.perf_counter() - insertion_start

        db.close()

        file_size = os.path.getsize(path)

        print("\nInsertion time:", round(insertion_time, 2), "seconds")
        print("Database size:", round(file_size / (1024 * 1024), 2), "MB")

        recovery_start = time.perf_counter()

        recovered_db = Database(path)

        recovery_time = time.perf_counter() - recovery_start

        print("Recovery time:", round(recovery_time, 2), "seconds")
        print("Recovered records:", len(recovered_db.data))

        # Verify selected records.
        checks = [
            0,
            record_count // 4,
            record_count // 2,
            record_count - 1
        ]

        for i in checks:

            key = f"benchmark_key_{i}".encode()
            expected = f"benchmark_value_{i}".encode()

            assert recovered_db.get(key) == expected

        assert len(recovered_db.data) == record_count

        print("Data verification: PASSED")

        recovered_db.close()

        print("==============================================")


if __name__ == "__main__":
    run_benchmark(100000, 1000)
