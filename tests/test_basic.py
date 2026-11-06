
import unittest

from src.database import Database


class TestDatabase(unittest.TestCase):

    def setUp(self):
        # Create a fresh database before each test.
        self.db = Database()

    def test_put_and_get(self):

        self.db.put(b"name", b"Nishanth")

        result = self.db.get(b"name")

        self.assertEqual(result, b"Nishanth")

    def test_overwrite(self):

        self.db.put(b"age", b"20")

        self.db.put(b"age", b"21")

        self.assertEqual(self.db.get(b"age"), b"21")

    def test_delete(self):

        self.db.put(b"city", b"Mangalore")

        self.db.delete(b"city")

        self.assertIsNone(self.db.get(b"city"))

    def test_missing_key(self):

        result = self.db.get(b"unknown")

        self.assertIsNone(result)

    def test_empty_value(self):

        self.db.put(b"empty", b"")

        self.assertEqual(self.db.get(b"empty"), b"")


if __name__ == "__main__":
    unittest.main()
