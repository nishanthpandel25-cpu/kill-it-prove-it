
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

    _locks_guard = threading.Lock()
    _path_locks = {}

    def __init__(self, path="data/database.db"):

        self.path = os.path.abspath(path)
        self.closed = False

        directory = os.path.dirname(self.path)
        os.makedirs(directory, exist_ok=True)

        normalized_path = os.path.normcase(self.path)

        with Database._locks_guard:
            if normalized_path not in Database._path_locks:
                Database._path_locks[normalized_path] = (
                    threading.RLock()
                )

            self.lock = Database._path_locks[normalized_path]

        with open(self.path, "ab"):
            pass

        with self.lock, FileLock(self.path):
            self.data = recover_database(self.path)
            self._known_signature = self._get_file_signature()

    @classmethod
    def open(cls, path="data/database.db"):
        return cls(path)

    def _check_open(self):

        if self.closed:
            raise RuntimeError("Database is closed")

    def _get_file_signature(self):

        stat = os.stat(self.path)

        return (
            stat.st_size,
            stat.st_mtime_ns,
            stat.st_ctime_ns
        )

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

    def _refresh_index_if_changed(self):

        current_signature = self._get_file_signature()

        if current_signature != self._known_signature:

            self.data = recover_database(self.path)

            self._known_signature = self._get_file_signature()

    # --------------------------------------------------
    # PUT
    # --------------------------------------------------

    def put(self, key, value):

        with self.lock, FileLock(self.path):

            self._check_open()
            self._validate_key(key)
            self._validate_value(value)

            self._refresh_index_if_changed()

            record = encode_record(PUT, key, value)

            append_record(self.path, record)

            self.data[key] = value

            self._known_signature = self._get_file_signature()

            return True

    # --------------------------------------------------
    # GET
    # --------------------------------------------------

    def get(self, key):

        with self.lock, FileLock(self.path):

            self._check_open()
            self._validate_key(key)

            self._refresh_index_if_changed()

            return self.data.get(key)

    # --------------------------------------------------
    # DELETE
    # --------------------------------------------------

    def delete(self, key):

        with self.lock, FileLock(self.path):

            self._check_open()
            self._validate_key(key)

            self._refresh_index_if_changed()

            record = encode_record(DELETE, key)

            append_record(self.path, record)

            self.data.pop(key, None)

            self._known_signature = self._get_file_signature()

            return True

    # --------------------------------------------------
    # ATOMIC BATCH
    # --------------------------------------------------

    def batch(self, operations):

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

            self._refresh_index_if_changed()

            records = []
            validated_operations = []

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
                    raise ValueError("Invalid batch operation")

            transaction_id = uuid.uuid4().bytes
            operation_count = len(validated_operations)

            count_bytes = struct.pack(
                ">I",
                operation_count
            )

            begin_record = encode_record(
                BEGIN,
                transaction_id,
                count_bytes
            )

            commit_record = encode_record(
                COMMIT,
                transaction_id,
                count_bytes
            )

            append_records(
                self.path,
                [begin_record] + records + [commit_record]
            )

            for operation, key, value in validated_operations:

                if operation == PUT:
                    self.data[key] = value

                elif operation == DELETE:
                    self.data.pop(key, None)

            self._known_signature = self._get_file_signature()

            return True

    # --------------------------------------------------
    # COMPACTION
    # --------------------------------------------------

    def compact(self):

        with self.lock, FileLock(self.path):

            self._check_open()

            # Include any changes made by other processes.
            self._refresh_index_if_changed()

            old_size = os.path.getsize(self.path)

            directory = os.path.dirname(self.path)

            temp_path = os.path.join(
                directory,
                os.path.basename(self.path)
                + "."
                + uuid.uuid4().hex
                + ".compact.tmp"
            )

            try:

                # Write only the latest live values.
                compacted_records = [
                    encode_record(PUT, key, value)
                    for key, value in self.data.items()
                ]

                # This also creates and syncs an empty file
                # when the database contains no live keys.
                append_records(temp_path, compacted_records)

                # Confirm the temporary file is complete.
                new_size = os.path.getsize(temp_path)

                # Atomically replace the original file.
                # Both files are in the same directory.
                os.replace(temp_path, self.path)

                # On POSIX, sync the directory entry so the
                # replacement is made durable where supported.
                if os.name != "nt":

                    directory_fd = os.open(directory, os.O_RDONLY)

                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)

                self._known_signature = self._get_file_signature()

                return {
                    "old_size": old_size,
                    "new_size": new_size,
                    "bytes_saved": old_size - new_size
                }

            finally:

                # If writing or replacement failed, remove
                # the unused temporary file. The original
                # database remains authoritative unless
                # os.replace has already succeeded.
                if os.path.exists(temp_path):
                    os.remove(temp_path)

    # --------------------------------------------------
    # CLOSE
    # --------------------------------------------------

    def close(self):

        with self.lock:

            if not self.closed:
                self.closed = True

            return True
