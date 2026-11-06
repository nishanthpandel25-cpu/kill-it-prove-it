
import os
import struct
import threading
import uuid

from src.storage import (
    append_record,
    append_records,
    encode_record,
    PUT,
    DELETE,
    BEGIN,
    COMMIT
)

from src.recovery import recover_database
from src.file_lock import FileLock


class Database:

    # Shared locks ensure different Database objects in the same
    # Python process also coordinate access to the same file.
    _locks_guard = threading.Lock()
    _path_locks = {}

    def __init__(self, path="data/database.db"):

        self.path = os.path.abspath(path)
        self.closed = False

        # Create parent directory if necessary.
        directory = os.path.dirname(self.path)
        os.makedirs(directory, exist_ok=True)

        # Get a shared thread lock for this database path.
        normalized_path = os.path.normcase(self.path)

        with Database._locks_guard:
            if normalized_path not in Database._path_locks:
                Database._path_locks[normalized_path] = (
                    threading.RLock()
                )

            self.lock = Database._path_locks[normalized_path]

        # Ensure the database file exists.
        with open(self.path, "ab"):
            pass

        # Recover while holding both locks.
        with self.lock, FileLock(self.path):
            self.data = recover_database(self.path)

    @classmethod
    def open(cls, path="data/database.db"):
        """
        Open or create a database.
        """
        return cls(path)

    def _check_open(self):

        if self.closed:
            raise RuntimeError("Database is closed")

    def _validate_key(self, key):

        if not isinstance(key, bytes):
            raise TypeError("Key must be bytes")

        if not 1 <= len(key) <= 1024:
            raise ValueError(
                "Key must contain between 1 and 1024 bytes"
            )

    def _validate_value(self, value):

        if not isinstance(value, bytes):
            raise TypeError("Value must be bytes")

        if len(value) > 1024 * 1024:
            raise ValueError("Value cannot exceed 1 MB")

    def _refresh_index(self):
        """
        Reload the latest committed database state.

        This is necessary because another process may have
        modified the database after this object was opened.
        """
        self.data = recover_database(self.path)

    # --------------------------------------------------
    # PUT
    # --------------------------------------------------

    def put(self, key, value):

        with self.lock, FileLock(self.path):

            self._check_open()

            self._validate_key(key)
            self._validate_value(value)

            # Refresh before modifying the database.
            self._refresh_index()

            record = encode_record(PUT, key, value)

            # Append and sync before reporting success.
            append_record(self.path, record)

            self.data[key] = value

            return True

    # --------------------------------------------------
    # GET
    # --------------------------------------------------

    def get(self, key):

        with self.lock, FileLock(self.path):

            self._check_open()

            self._validate_key(key)

            # Read the latest committed state from disk.
            self._refresh_index()

            return self.data.get(key)

    # --------------------------------------------------
    # DELETE
    # --------------------------------------------------

    def delete(self, key):

        with self.lock, FileLock(self.path):

            self._check_open()

            self._validate_key(key)

            self._refresh_index()

            record = encode_record(DELETE, key)

            append_record(self.path, record)

            self.data.pop(key, None)

            return True

    # --------------------------------------------------
    # ATOMIC BATCH
    # --------------------------------------------------

    def batch(self, operations):
        """
        Apply multiple PUT and DELETE operations atomically.

        Supported operations:

            ("put", key, value)
            ("delete", key)

        Maximum operations: 10,000
        """

        with self.lock, FileLock(self.path):

            self._check_open()

            if not isinstance(operations, (list, tuple)):
                raise TypeError(
                    "Operations must be a list or tuple"
                )

            if not 1 <= len(operations) <= 10000:
                raise ValueError(
                    "Batch must contain 1 to 10000 operations"
                )

            # Refresh the latest state before the transaction.
            self._refresh_index()

            records = []
            validated_operations = []

            # Validate every operation before writing anything.
            for operation in operations:

                if not isinstance(operation, (list, tuple)):
                    raise TypeError(
                        "Each operation must be a tuple or list"
                    )

                if len(operation) == 3 and operation[0] == "put":

                    _, key, value = operation

                    self._validate_key(key)
                    self._validate_value(value)

                    records.append(
                        encode_record(PUT, key, value)
                    )

                    validated_operations.append(
                        (PUT, key, value)
                    )

                elif len(operation) == 2 and operation[0] == "delete":

                    _, key = operation

                    self._validate_key(key)

                    records.append(
                        encode_record(DELETE, key)
                    )

                    validated_operations.append(
                        (DELETE, key, b"")
                    )

                else:

                    raise ValueError(
                        "Invalid batch operation"
                    )

            # Generate a unique transaction ID.
            transaction_id = uuid.uuid4().bytes

            operation_count = len(validated_operations)

            count_bytes = struct.pack(
                ">I",
                operation_count
            )

            # BEGIN marker.
            begin_record = encode_record(
                BEGIN,
                transaction_id,
                count_bytes
            )

            # COMMIT marker.
            commit_record = encode_record(
                COMMIT,
                transaction_id,
                count_bytes
            )

            # Append BEGIN + operations + COMMIT.
            # append_records performs fsync before returning.
            append_records(
                self.path,
                [begin_record] + records + [commit_record]
            )

            # Update the in-memory index only after successful sync.
            for operation, key, value in validated_operations:

                if operation == PUT:

                    self.data[key] = value

                elif operation == DELETE:

                    self.data.pop(key, None)

            return True

    # --------------------------------------------------
    # CLOSE
    # --------------------------------------------------

    def close(self):

        with self.lock:

            if not self.closed:
                self.closed = True

            return True
