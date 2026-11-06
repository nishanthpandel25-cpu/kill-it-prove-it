
import multiprocessing
import os
import tempfile
import unittest

from src.database import Database


def writer_process(path, prefix, count):
    db = Database(path)

    try:
        for i in range(count):
            key = f"{prefix}_{i}".encode()
            value = f"value_{i}".encode()
            db.put(key, value)
    finally:
        db.close()


class TestConcurrentProcesses(unittest.TestCase):

    def test_two_processes_write_without_corrupting_log(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "database.db")

            # Create the database file before starting workers.
            db = Database(path)
            db.close()

            process1 = multiprocessing.Process(
                target=writer_process,
                args=(path, "worker1", 100)
            )

            process2 = multiprocessing.Process(
                target=writer_process,
                args=(path, "worker2", 100)
            )

            process1.start()
            process2.start()

            process1.join(30)
            process2.join(30)

            self.assertFalse(process1.is_alive(), "Worker 1 timed out")
            self.assertFalse(process2.is_alive(), "Worker 2 timed out")

            self.assertEqual(process1.exitcode, 0)
            self.assertEqual(process2.exitcode, 0)

            # Reopen and verify all records can be recovered.
            db = Database(path)

            try:
                for i in range(100):
                    self.assertEqual(
                        db.get(f"worker1_{i}".encode()),
                        f"value_{i}".encode()
                    )

                    self.assertEqual(
                        db.get(f"worker2_{i}".encode()),
                        f"value_{i}".encode()
                    )
            finally:
                db.close()


if __name__ == "__main__":
    unittest.main()
