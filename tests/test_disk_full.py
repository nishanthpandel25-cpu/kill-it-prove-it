import os
import tempfile

import pytest

import src.storage as storage
from src.database import Database


def simulate_disk_full(monkeypatch):
    """Write part of a record, then simulate disk-full error."""
    original_write = storage.os.write
    write_count = {"value": 0}

    def failing_write(fd, data):
        write_count["value"] += 1

        if write_count["value"] == 1:
            partial_size = max(1, len(data) // 2)
            return original_write(fd, data[:partial_size])

        raise OSError(28, "No space left on device")

    monkeypatch.setattr(storage.os, "write", failing_write)


def test_partial_put_failure_rolls_back(monkeypatch):
    with tempfile.TemporaryDirectory() as folder:
        path = os.path.join(folder, "test.db")
        db = Database(path)

        try:
            db.put(b"existing", b"important_data")
            original_size = os.path.getsize(path)

            simulate_disk_full(monkeypatch)

            with pytest.raises(OSError) as error:
                db.put(b"new_key", b"new_value")

            assert error.value.errno == 28
            assert os.path.getsize(path) == original_size
            assert db.get(b"existing") == b"important_data"
            assert db.get(b"new_key") is None

        finally:
            db.close()


def test_partial_batch_failure_rolls_back(monkeypatch):
    with tempfile.TemporaryDirectory() as folder:
        path = os.path.join(folder, "test.db")
        db = Database(path)

        try:
            db.put(b"existing", b"important_data")
            original_size = os.path.getsize(path)

            simulate_disk_full(monkeypatch)

            operations = [
                ("put", b"key1", b"value1"),
                ("put", b"key2", b"value2"),
                ("put", b"key3", b"value3"),
            ]

            with pytest.raises(OSError) as error:
                db.batch(operations)

            assert error.value.errno == 28
            assert os.path.getsize(path) == original_size
            assert db.get(b"existing") == b"important_data"
            assert db.get(b"key1") is None
            assert db.get(b"key2") is None
            assert db.get(b"key3") is None

        finally:
            db.close()


def test_database_recovers_after_simulated_disk_full(monkeypatch):
    with tempfile.TemporaryDirectory() as folder:
        path = os.path.join(folder, "test.db")
        db = Database(path)

        db.put(b"account", b"active")

        try:
            simulate_disk_full(monkeypatch)

            with pytest.raises(OSError):
                db.put(b"another_account", b"active")

        finally:
            db.close()

        reopened = Database(path)

        try:
            assert reopened.get(b"account") == b"active"
            assert reopened.get(b"another_account") is None

        finally:
            reopened.close()


def test_fsync_failure_rolls_back_and_preserves_data(monkeypatch):
    with tempfile.TemporaryDirectory() as folder:
        path = os.path.join(folder, "test.db")
        db = Database(path)

        try:
            db.put(b"existing", b"important_data")
            original_size = os.path.getsize(path)

            original_fsync = storage.os.fsync
            fsync_calls = {"count": 0}

            def failing_first_fsync(fd):
                fsync_calls["count"] += 1

                if fsync_calls["count"] == 1:
                    raise OSError(5, "Simulated I/O error")

                return original_fsync(fd)

            monkeypatch.setattr(
                storage.os,
                "fsync",
                failing_first_fsync,
            )

            with pytest.raises(OSError) as error:
                db.put(b"new_key", b"new_value")

            assert error.value.errno == 5
            assert fsync_calls["count"] == 2
            assert os.path.getsize(path) == original_size
            assert db.get(b"existing") == b"important_data"
            assert db.get(b"new_key") is None

        finally:
            db.close()

        reopened = Database(path)

        try:
            assert reopened.get(b"existing") == b"important_data"
            assert reopened.get(b"new_key") is None

        finally:
            reopened.close()


def test_ftruncate_failure_poisoned_database_until_reopen(monkeypatch):
    with tempfile.TemporaryDirectory() as folder:
        path = os.path.join(folder, "test.db")
        db = Database(path)

        db.put(b"existing", b"important_data")
        original_size = os.path.getsize(path)

        simulate_disk_full(monkeypatch)

        def failing_ftruncate(fd, size):
            raise OSError(5, "Simulated truncate failure")

        monkeypatch.setattr(
            storage.os,
            "ftruncate",
            failing_ftruncate,
        )

        with pytest.raises(
            RuntimeError,
            match="rollback could not be confirmed",
        ):
            db.put(b"new_key", b"new_value")

        assert db.failed is True
        assert os.path.getsize(path) > original_size

        # The failed instance must reject every normal operation.
        with pytest.raises(RuntimeError, match="failed state"):
            db.get(b"existing")

        with pytest.raises(RuntimeError, match="failed state"):
            db.put(b"another", b"value")

        with pytest.raises(RuntimeError, match="failed state"):
            db.delete(b"existing")

        with pytest.raises(RuntimeError, match="failed state"):
            db.batch([("put", b"another", b"value")])

        with pytest.raises(RuntimeError, match="failed state"):
            db.compact()

        # close() must remain available.
        assert db.close() is True

        # Reopening invokes recovery on the persisted file.
        reopened = Database(path)

        try:
            assert reopened.get(b"existing") == b"important_data"
            assert reopened.get(b"new_key") is None
        finally:
            reopened.close()


def test_rollback_fsync_failure_poisoned_database_until_reopen(monkeypatch):
    with tempfile.TemporaryDirectory() as folder:
        path = os.path.join(folder, "test.db")
        db = Database(path)

        db.put(b"existing", b"important_data")
        original_size = os.path.getsize(path)

        fsync_calls = {"count": 0}

        def failing_all_fsync_calls(fd):
            fsync_calls["count"] += 1
            raise OSError(5, "Simulated synchronization failure")

        monkeypatch.setattr(
            storage.os,
            "fsync",
            failing_all_fsync_calls,
        )

        with pytest.raises(
            RuntimeError,
            match="rollback could not be confirmed",
        ):
            db.put(b"new_key", b"new_value")

        assert fsync_calls["count"] == 2
        assert db.failed is True
        assert os.path.getsize(path) == original_size

        with pytest.raises(RuntimeError, match="failed state"):
            db.get(b"existing")

        with pytest.raises(RuntimeError, match="failed state"):
            db.put(b"another", b"value")

        assert db.close() is True