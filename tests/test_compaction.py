
import os
import tempfile

from src.database import Database


def test_compaction_removes_deleted_records():

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "test.db")
        db = Database(path)

        # Insert 1000 records.
        for i in range(1000):
            db.put(
                f"key_{i}".encode(),
                f"value_{i}".encode()
            )

        # Delete 800 records.
        for i in range(800):
            db.delete(f"key_{i}".encode())

        size_before = os.path.getsize(path)

        result = db.compact()

        size_after = os.path.getsize(path)

        # Compaction should reduce the file size.
        assert size_after < size_before
        assert result["bytes_saved"] > 0

        # Deleted keys must remain deleted.
        for i in range(800):
            assert db.get(f"key_{i}".encode()) is None

        # Remaining keys must retain their values.
        for i in range(800, 1000):
            assert db.get(f"key_{i}".encode()) == (
                f"value_{i}".encode()
            )

        db.close()


def test_compaction_survives_restart():

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "test.db")

        db = Database(path)

        db.put(b"name", b"Nishanth")
        db.put(b"project", b"Kill-It-and-Prove-It")
        db.delete(b"name")

        db.compact()
        db.close()

        # Reopen the database.
        reopened = Database(path)

        assert reopened.get(b"name") is None
        assert reopened.get(b"project") == (
            b"Kill-It-and-Prove-It"
        )

        reopened.close()


def test_empty_database_compaction():

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "test.db")

        db = Database(path)

        db.put(b"temporary", b"value")
        db.delete(b"temporary")

        result = db.compact()

        assert result["new_size"] == 0
        assert os.path.getsize(path) == 0

        db.close()

        reopened = Database(path)

        assert len(reopened.data) == 0

        reopened.close()
