
import multiprocessing
import os
import tempfile

from src.database import Database


def writer_process(path, ready_event, stop_event, count):
    """
    Child process writes records and signals when
    all acknowledged writes are complete.
    """

    db = Database(path)

    try:
        for i in range(count):
            key = f"crash_key_{i}".encode()
            value = f"crash_value_{i}".encode()

            # put() returns only after append_records()
            # has completed its fsync.
            db.put(key, value)

        # All writes have returned successfully.
        ready_event.set()

        # Keep the process alive until the parent kills it.
        stop_event.wait()

    finally:
        db.close()


class TestCrashDurability:

    def test_acknowledged_writes_survive_forced_termination(self):

        count = 100

        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "crash_test.db")

            ready_event = multiprocessing.Event()
            stop_event = multiprocessing.Event()

            process = multiprocessing.Process(
                target=writer_process,
                args=(path, ready_event, stop_event, count)
            )

            process.start()

            try:
                # Wait until all writes have been acknowledged.
                assert ready_event.wait(timeout=30), (
                    "Writer did not finish its writes in time"
                )

                # Forcefully terminate the database process.
                process.terminate()
                process.join(timeout=10)

                assert not process.is_alive(), (
                    "Writer process did not terminate"
                )

                # Reopen the database in a fresh instance.
                db = Database(path)

                try:
                    missing = []

                    for i in range(count):
                        key = f"crash_key_{i}".encode()
                        expected = f"crash_value_{i}".encode()
                        actual = db.get(key)

                        if actual != expected:
                            missing.append(
                                (key, actual, expected)
                            )

                    assert len(db.data) == count, (
                        f"Expected {count} records, "
                        f"recovered {len(db.data)}"
                    )

                    assert missing == [], (
                        f"Missing records: {missing[:10]}"
                    )

                finally:
                    db.close()

            finally:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=10)
