
import os
import tempfile

import pytest

import src.storage as storage
from src.database import Database


def simulate_disk_full(monkeypatch):

    original_write = storage.os.write
    write_count = {"value": 0}

    def failing_write(fd, data):

        write_count["value"] += 1

        # Allow a partial write first.
        if write_count["value"] == 1:

            partial_size = max(1, len(data) // 2)

            return original_write(
                fd,
                data[:partial_size]
            )

        # Simulate operating-system disk-full error.
        raise OSError(
            28,
            "No space left on device"
        )

    monkeypatch.setattr(
        storage.os,
        "write",
        failing_write
    )


def test_partial_put_failure_rolls_back(monkeypatch):

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "test.db")

        db = Database(path)

        db.put(b"existing", b"important_data")

        original_size = os.path.getsize(path)

        simulate_disk_full(monkeypatch)

        with pytest.raises(OSError) as error:

            db.put(b"new_key", b"new_value")

        assert error.value.errno == 28

        # File must return to its original size.
        assert os.path.getsize(path) == original_size

        # Existing data must remain available.
        assert db.get(b"existing") == b"important_data"

        # Failed write must not appear.
        assert db.get(b"new_key") is None

        db.close()


def test_partial_batch_failure_rolls_back(monkeypatch):

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "test.db")

        db = Database(path)

        db.put(b"existing", b"important_data")

        original_size = os.path.getsize(path)

        simulate_disk_full(monkeypatch)

        operations = [
            ("put", b"key1", b"value1"),
            ("put", b"key2", b"value2"),
            ("put", b"key3", b"value3")
        ]

        with pytest.raises(OSError) as error:

            db.batch(operations)

        assert error.value.errno == 28

        # Partial transaction must be removed.
        assert os.path.getsize(path) == original_size

        # Existing record must remain.
        assert db.get(b"existing") == b"important_data"

        # No batch operation may be visible.
        assert db.get(b"key1") is None
        assert db.get(b"key2") is None
        assert db.get(b"key3") is None

        db.close()


def test_database_recovers_after_simulated_disk_full(monkeypatch):

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "test.db")

        db = Database(path)

        db.put(b"account", b"active")

        simulate_disk_full(monkeypatch)

        with pytest.raises(OSError):

            db.put(b"another_account", b"active")

        db.close()

        # Reopen after the failed write.
        reopened = Database(path)

        assert reopened.get(b"account") == b"active"

        assert reopened.get(b"another_account") is None

        reopened.close()
