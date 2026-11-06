
import multiprocessing
import os
import tempfile

from src.database import Database
from src import storage


def writer_process(path, ready_event, stop_event, count):
    """Write records and wait after all writes are acknowledged."""

    db = Database(path)

    try:
        for i in range(count):
            key = f"crash_key_{i}".encode()
            value = f"crash_value_{i}".encode()
            db.put(key, value)

        ready_event.set()
        stop_event.wait()

    finally:
        db.close()


def torn_write_process(path):
    """Write only 5 bytes, then terminate immediately."""

    db = Database(path)
    original_write = os.write

    def partial_write_then_kill(fd, data):
        original_write(fd, data[:5])
        os._exit(77)

    storage.os.write = partial_write_then_kill
    db.put(b"torn_key", b"this_record_will_be_incomplete")


def crash_before_write_process(path):
    """Terminate immediately before the first physical write."""

    db = Database(path)

    def kill_before_write(fd, data):
        os._exit(76)

    storage.os.write = kill_before_write
    db.put(b"never_written", b"this_value_must_not_exist")


def crash_before_fsync_process(path):
    """Write the complete record, then terminate before fsync."""

    db = Database(path)

    def kill_before_fsync(fd):
        os._exit(75)

    storage.os.fsync = kill_before_fsync
    db.put(b"complete_key", b"complete_value")


def crash_after_fsync_process(path):
    """Synchronize the record, then terminate before put returns."""

    db = Database(path)
    original_fsync = os.fsync

    def fsync_then_kill(fd):
        original_fsync(fd)
        os._exit(74)

    storage.os.fsync = fsync_then_kill
    db.put(b"durable_key", b"durable_value")


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
                assert ready_event.wait(timeout=30)

                process.terminate()
                process.join(timeout=10)

                assert not process.is_alive()

                db = Database(path)

                try:
                    missing = []

                    for i in range(count):
                        key = f"crash_key_{i}".encode()
                        expected = f"crash_value_{i}".encode()
                        actual = db.get(key)

                        if actual != expected:
                            missing.append((key, actual, expected))

                    assert len(db.data) == count
                    assert missing == [], (
                        f"Missing records: {missing[:10]}"
                    )

                finally:
                    db.close()

            finally:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=10)

    def test_torn_record_is_removed_after_process_kill(self):

        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "torn_write.db")

            process = multiprocessing.Process(
                target=torn_write_process,
                args=(path,)
            )

            process.start()
            process.join(timeout=10)

            assert not process.is_alive()
            assert process.exitcode == 77

            db = Database(path)

            try:
                assert db.get(b"torn_key") is None
                assert len(db.data) == 0
                assert os.path.getsize(path) == 0

            finally:
                db.close()

    def test_process_killed_before_first_physical_write(self):

        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "before_write.db")

            process = multiprocessing.Process(
                target=crash_before_write_process,
                args=(path,)
            )

            process.start()
            process.join(timeout=10)

            assert not process.is_alive()
            assert process.exitcode == 76

            db = Database(path)

            try:
                assert db.get(b"never_written") is None
                assert len(db.data) == 0
                assert os.path.getsize(path) == 0

            finally:
                db.close()

    def test_complete_record_written_before_fsync(self):

        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "before_fsync.db")

            process = multiprocessing.Process(
                target=crash_before_fsync_process,
                args=(path,)
            )

            process.start()
            process.join(timeout=10)

            assert not process.is_alive()
            assert process.exitcode == 75

            db = Database(path)

            try:
                actual = db.get(b"complete_key")

                if actual is not None:
                    assert actual == b"complete_value"

                assert os.path.getsize(path) > 0

            finally:
                db.close()

    def test_complete_record_survives_crash_after_fsync(self):

        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "after_fsync.db")

            process = multiprocessing.Process(
                target=crash_after_fsync_process,
                args=(path,)
            )

            process.start()
            process.join(timeout=10)

            assert not process.is_alive()
            assert process.exitcode == 74

            # Reopen after the process has terminated.
            db = Database(path)

            try:
                actual = db.get(b"durable_key")

                assert actual == b"durable_value", (
                    "Record was not recovered after successful fsync"
                )

                assert len(db.data) == 1

            finally:
                db.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
