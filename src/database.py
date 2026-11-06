
import os

from src.storage import (
    append_record,
    encode_record,
    PUT,
    DELETE
)

from src.recovery import recover_database


class Database:

    def __init__(self, path="data/database.db"):

        self.path = path
        self.closed = False

        # Create the parent directory.
        directory = os.path.dirname(os.path.abspath(path))

        os.makedirs(directory, exist_ok=True)

        # Recover previously stored data.
        self.data = recover_database(self.path)

    def _check_open(self):

        if self.closed:
            raise RuntimeError("Database is closed")

    def _validate_key(self, key):

        if not isinstance(key, bytes):
            raise TypeError("Key must be bytes")

        if not 1 <= len(key) <= 1024:
            raise ValueError("Invalid key length")

    def _validate_value(self, value):

        if not isinstance(value, bytes):
            raise TypeError("Value must be bytes")

        if len(value) > 1024 * 1024:
            raise ValueError("Value exceeds 1 MB")

    def put(self, key, value):

        self._check_open()

        self._validate_key(key)
        self._validate_value(value)

        # Encode operation into binary record.
        record = encode_record(PUT, key, value)

        # Write and synchronize before updating memory.
        append_record(self.path, record)

        # Update in-memory index only after success.
        self.data[key] = value

        return True

    def get(self, key):

        self._check_open()
        self._validate_key(key)

        return self.data.get(key)

    def delete(self, key):

        self._check_open()
        self._validate_key(key)

        # Write a DELETE record.
        record = encode_record(DELETE, key)

        append_record(self.path, record)

        # Update memory after successful synchronization.
        self.data.pop(key, None)

        return True

    def close(self):

        if not self.closed:
            self.closed = True
