
import os
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor

from src.database import Database


class TestThreadConcurrency:

    def test_multiple_threads_write_without_data_loss(self):
        thread_count = 4
        writes_per_thread = 100

        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "thread_test.db")

            # Create separate database instances pointing
            # to the same database file.
            databases = [
                Database(path) for _ in range(thread_count)
            ]

            errors = []
            errors_lock = threading.Lock()

            def writer(thread_number):
                db = databases[thread_number]

                try:
                    for i in range(writes_per_thread):
                        key = f"thread{thread_number}_{i}".encode()
                        value = f"value_{thread_number}_{i}".encode()

                        db.put(key, value)

                except Exception as error:
                    with errors_lock:
                        errors.append(error)

            try:
                with ThreadPoolExecutor(
                    max_workers=thread_count
                ) as executor:

                    futures = [
                        executor.submit(writer, thread_number)
                        for thread_number in range(thread_count)
                    ]

                    for future in futures:
                        future.result()

                # Reopen the database to verify persistence.
                recovered_db = Database(path)

                try:
                    expected_records = thread_count * writes_per_thread
                    missing = []

                    for thread_number in range(thread_count):
                        for i in range(writes_per_thread):
                            key = f"thread{thread_number}_{i}".encode()
                            expected = f"value_{thread_number}_{i}".encode()
                            actual = recovered_db.get(key)

                            if actual != expected:
                                missing.append(
                                    (key, actual, expected)
                                )

                    assert errors == [], f"Writer errors: {errors}"
                    assert len(recovered_db.data) == expected_records
                    assert missing == [], f"Missing records: {missing[:10]}"

                finally:
                    recovered_db.close()

            finally:
                for db in databases:
                    db.close()
