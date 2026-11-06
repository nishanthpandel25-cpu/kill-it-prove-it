import os
import tempfile
import unittest

from src.database import Database


class TestBatch(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.temp_dir.name, "database.db")
        self.db = Database(self.path)

    def tearDown(self):
        self.db.close()
        self.temp_dir.cleanup()

    def test_batch_put(self):
        self.db.batch([
            ("put", b"name", b"Mohith"),
            ("put", b"age", b"20")
        ])

        self.assertEqual(self.db.get(b"name"), b"Mohith")
        self.assertEqual(self.db.get(b"age"), b"20")

    def test_batch_delete(self):
        self.db.put(b"name", b"Mohith")
        self.db.put(b"city", b"Mangalore")

        self.db.batch([
            ("delete", b"name"),
            ("delete", b"city")
        ])

        self.assertIsNone(self.db.get(b"name"))
        self.assertIsNone(self.db.get(b"city"))

    def test_batch_mixed_operations(self):
        self.db.put(b"city", b"Mangalore")

        self.db.batch([
            ("put", b"name", b"Mohith"),
            ("delete", b"city"),
            ("put", b"age", b"20")
        ])

        self.assertEqual(self.db.get(b"name"), b"Mohith")
        self.assertIsNone(self.db.get(b"city"))
        self.assertEqual(self.db.get(b"age"), b"20")

    def test_invalid_batch_changes_nothing(self):
        with self.assertRaises(ValueError):
            self.db.batch([
                ("put", b"name", b"Mohith"),
                ("invalid", b"age")
            ])

        self.assertIsNone(self.db.get(b"name"))

    def test_batch_survives_restart(self):
        self.db.batch([
            ("put", b"name", b"Mohith"),
            ("put", b"age", b"20")
        ])

        self.db.close()
        self.db = Database(self.path)

        self.assertEqual(self.db.get(b"name"), b"Mohith")
        self.assertEqual(self.db.get(b"age"), b"20")

    def test_incomplete_batch_is_discarded(self):
        self.db.put(b"existing", b"keep")
        self.db.close()

        # Write a BEGIN record and one operation, but no COMMIT.
        import struct
        import uuid

        from src.storage import BEGIN, PUT, encode_record

        transaction_id = uuid.uuid4().bytes
        count = struct.pack(">I", 2)

        with open(self.path, "ab") as file:
            file.write(encode_record(BEGIN, transaction_id, count))
            file.write(encode_record(PUT, b"new", b"value"))

        self.db = Database(self.path)

        self.assertEqual(self.db.get(b"existing"), b"keep")
        self.assertIsNone(self.db.get(b"new"))

    def test_batch_limit(self):
        operations = [
            ("put", b"k", b"v")
        ] * 10001

        with self.assertRaises(ValueError):
            self.db.batch(operations)

        self.assertIsNone(self.db.get(b"k"))


if __name__ == "__main__":
    unittest.main()