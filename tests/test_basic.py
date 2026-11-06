
import os
import tempfile
import unittest

from src.database import Database


class TestDatabase(unittest.TestCase):

    def setUp(self):

        # Create a separate temporary folder for each test.
        self.temp_dir = tempfile.TemporaryDirectory()

        self.db_path = os.path.join(
            self.temp_dir.name,
            "database.db"
        )

        self.db = Database(self.db_path)

    def tearDown(self):

        self.db.close()
        self.temp_dir.cleanup()

    def test_put_and_get(self):

        self.db.put(b"name", b"Nishanth")

        self.assertEqual(
            self.db.get(b"name"),
            b"Nishanth"
        )

    def test_overwrite(self):

        self.db.put(b"age", b"20")
        self.db.put(b"age", b"21")

        self.assertEqual(
            self.db.get(b"age"),
            b"21"
        )

    def test_delete(self):

        self.db.put(b"city", b"Mangalore")
        self.db.delete(b"city")

        self.assertIsNone(
            self.db.get(b"city")
        )

    def test_missing_key(self):

        self.assertIsNone(
            self.db.get(b"unknown")
        )

    def test_empty_value(self):

        self.db.put(b"empty", b"")

        self.assertEqual(
            self.db.get(b"empty"),
            b""
        )

    def test_persistence_after_restart(self):

        self.db.put(b"name", b"Nishanth")
        self.db.put(b"age", b"20")

        # Close the first database instance.
        self.db.close()

        # Open the same database file again.
        self.db = Database(self.db_path)

        # Verify previously saved information.
        self.assertEqual(
            self.db.get(b"name"),
            b"Nishanth"
        )

        self.assertEqual(
            self.db.get(b"age"),
            b"20"
        )


if __name__ == "__main__":
    unittest.main()
