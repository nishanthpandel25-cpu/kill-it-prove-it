
import os
import tempfile
import unittest

from src.database import Database
from src.storage import encode_record, PUT


class TestCrashRecovery(unittest.TestCase):

    def setUp(self):

        self.temp_dir = tempfile.TemporaryDirectory()

        self.path = os.path.join(
            self.temp_dir.name,
            "database.db"
        )

        self.db = Database(self.path)

    def tearDown(self):

        if not self.db.closed:
            self.db.close()

        self.temp_dir.cleanup()

    def test_recover_after_incomplete_record(self):

        # First write a valid record.
        self.db.put(b"name", b"Nishanth")

        # Close database.
        self.db.close()

        # Create another record.
        incomplete = encode_record(
            PUT,
            b"age",
            b"20"
        )

        # Write only part of the record.
        with open(self.path, "ab") as file:
            file.write(incomplete[:10])

        # Reopen database.
        self.db = Database(self.path)

        # Valid record must survive.
        self.assertEqual(
            self.db.get(b"name"),
            b"Nishanth"
        )

        # Incomplete record must not appear.
        self.assertIsNone(
            self.db.get(b"age")
        )

        # Recovery should remove incomplete trailing bytes.
        with open(self.path, "rb") as file:
            content = file.read()

        self.assertNotIn(
            incomplete[:10],
            content
        )

    def test_recover_after_bad_checksum(self):

        # Write a valid record.
        self.db.put(b"name", b"Nishanth")

        self.db.close()

        # Create another valid encoded record.
        damaged = bytearray(
            encode_record(PUT, b"age", b"20")
        )

        # Change one byte in the record.
        damaged[-1] ^= 0xFF

        # Append the damaged record.
        with open(self.path, "ab") as file:
            file.write(damaged)

        # Reopen and recover.
        self.db = Database(self.path)

        # Previous valid record must remain.
        self.assertEqual(
            self.db.get(b"name"),
            b"Nishanth"
        )

        # Damaged record must not be returned.
        self.assertIsNone(
            self.db.get(b"age")
        )


if __name__ == "__main__":
    unittest.main()
