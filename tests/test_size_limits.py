
import os
import tempfile

import pytest

from src.database import Database
from src.storage import DatabaseFullError, PUT, encode_record


def test_put_respects_maximum_size():

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "test.db")

        # Calculate the exact size of one record.
        record_size = len(
            encode_record(PUT, b"key1", b"value1")
        )

        db = Database(path, max_size=record_size)

        # First write should fit.
        assert db.put(b"key1", b"value1") is True

        original_size = os.path.getsize(path)

        # Second write must be rejected.
        with pytest.raises(DatabaseFullError):
            db.put(b"key2", b"value2")

        # Existing data must remain unchanged.
        assert os.path.getsize(path) == original_size
        assert db.get(b"key1") == b"value1"
        assert db.get(b"key2") is None

        db.close()


def test_failed_batch_changes_nothing():

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "test.db")

        db = Database(path, max_size=0)

        original_size = os.path.getsize(path)

        operations = [
            ("put", b"key1", b"value1"),
            ("put", b"key2", b"value2"),
            ("put", b"key3", b"value3")
        ]

        with pytest.raises(DatabaseFullError):
            db.batch(operations)

        assert os.path.getsize(path) == original_size

        assert db.get(b"key1") is None
        assert db.get(b"key2") is None
        assert db.get(b"key3") is None

        db.close()


def test_failed_delete_preserves_existing_record():

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "test.db")

        # First create a database with one record.
        db = Database(path)

        db.put(b"important", b"keep_this_value")

        db.close()

        original_size = os.path.getsize(path)

        # Reopen with a limit equal to the current file size.
        db = Database(path, max_size=original_size)

        # DELETE needs additional log space and must fail.
        with pytest.raises(DatabaseFullError):
            db.delete(b"important")

        assert os.path.getsize(path) == original_size

        # The original record must still exist.
        assert db.get(b"important") == b"keep_this_value"

        db.close()


def test_failed_compaction_preserves_original_database():

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "test.db")

        # Create existing data without a size restriction.
        db = Database(path)

        db.put(b"key1", b"value1")
        db.put(b"key2", b"value2")

        db.close()

        original_size = os.path.getsize(path)

        # A zero-byte limit cannot hold the compacted records.
        db = Database(path, max_size=0)

        with pytest.raises(DatabaseFullError):
            db.compact()

        # Original database must remain untouched.
        assert os.path.getsize(path) == original_size

        assert db.get(b"key1") == b"value1"
        assert db.get(b"key2") == b"value2"

        db.close()


def test_data_survives_restart_after_failed_write():

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "test.db")

        db = Database(path)

        db.put(b"account", b"active")

        db.close()

        original_size = os.path.getsize(path)

        db = Database(path, max_size=original_size)

        with pytest.raises(DatabaseFullError):
            db.put(b"new_account", b"active")

        db.close()

        # Reopen without a size restriction.
        reopened = Database(path)

        assert reopened.get(b"account") == b"active"
        assert reopened.get(b"new_account") is None

        reopened.close()
