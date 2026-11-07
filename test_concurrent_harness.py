
import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor

from src.database import Database
from torture_tests.process_harness import ServeProcess


def main():
    with tempfile.TemporaryDirectory() as folder:
        database_path = os.path.join(folder, "concurrent.db")
        oracle_path = os.path.join(folder, "oracle.jsonl")

        total_writes = 200
        worker_count = 4

        print("Starting database child process...")

        with ServeProcess(
            database_path,
            oracle_log_path=oracle_path,
        ) as server:

            def writer(worker_id):
                for number in range(total_writes // worker_count):
                    key = f"worker_{worker_id}_key_{number}".encode()
                    value = f"value_{worker_id}_{number}".encode()

                    server.put(key, value)

                return True

            print("Starting concurrent writers...")

            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                results = list(
                    executor.map(writer, range(worker_count))
                )

            assert all(results)
            print("Concurrent writers: PASSED")

        print("Database child process stopped.")

        with open(oracle_path, "r", encoding="utf-8") as file:
            entries = [
                json.loads(line)
                for line in file
                if line.strip()
            ]

        assert len(entries) == total_writes, (
            f"Expected {total_writes} ACK records, found {len(entries)}"
        )

        print("Oracle ACK records:", len(entries))

        with Database(database_path) as db:
            assert len(db.data) == total_writes

            for worker_id in range(worker_count):
                for number in range(total_writes // worker_count):
                    key = f"worker_{worker_id}_key_{number}".encode()
                    expected = f"value_{worker_id}_{number}".encode()

                    assert db.get(key) == expected

        print("Recovered database verification: PASSED")
        print("\nCONCURRENT HARNESS TEST: PASSED")


if __name__ == "__main__":
    main()
