
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


# --------------------------------------------------
# TEST-ONLY COMPACTION HOOK
# --------------------------------------------------

COMPACTION_TEST_HOOK = None


def _compaction_checkpoint(stage):

    if COMPACTION_TEST_HOOK is not None:
        COMPACTION_TEST_HOOK(stage)


# --------------------------------------------------
# DATABASE CLASS
# --------------------------------------------------

class Database:

    _locks_guard = threading.Lock()
    _path_locks = {}

    def __init__(self, path="data/database.db", max_size=None):

        self.path = os.path.abspath(path)
        self.closed = False

        # None means unlimited database file size.
        if max_size is not None:

            if not isinstance(max_size, int) or isinstance(max_size, bool):
                raise TypeError("max_size must be an integer or None")

            if max_size < 0:
                raise ValueError("max_size cannot be negative")

        self.max_size = max_size

        directory = os.path.dirname(self.path)
        os.makedirs(directory, exist_ok=True)

        normalized_path = os.path.normcase(self.path)

        with Database._locks_guard:

            if normalized_path not in Database._path_locks:

                Database._path_locks[normalized_path] = (
                    threading.RLock()
                )

            self.lock = Database._path_locks[normalized_path]

        # Ensure database file exists.
        with open(self.path, "ab"):
            pass

        # Recover existing database.
        with self.lock, FileLock(self.path):

            self.data = recover_database(self.path)

            self._known_signature = self._get_file_signature()

    # --------------------------------------------------
    # OPEN
    # --------------------------------------------------

    @classmethod
    def open(cls, path="data/database.db", max_size=None):

        return cls(path, max_size=max_size)

    # --------------------------------------------------
    # INTERNAL HELPERS
    # --------------------------------------------------

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

    def _refresh_index_if_changed(self):

        current_signature = self._get_file_signature()

        if current_signature != self._known_signature:

            self.data = recover_database(self.path)

            self._known_signature = self._get_file_signature()

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

            # The storage layer checks the size limit
            # before writing anything.
            append_record(
                self.path,
                record,
                max_size=self.max_size
            )

            # Update memory only after successful write.
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

            append_record(
                self.path,
                record,
                max_size=self.max_size
            )

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

            # Validate all operations before writing.
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

            complete_transaction = (
                [begin_record] + records + [commit_record]
            )

            # Check and append the entire transaction together.
            # If it exceeds max_size, nothing is written.
            append_records(
                self.path,
                complete_transaction,
                max_size=self.max_size
            )

            # Update memory only after successful append.
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

                # Encode only currently active records.
                compacted_records = [

                    encode_record(PUT, key, value)

                    for key, value in self.data.items()

                ]

                # The completed compacted file must also
                # respect the configured maximum size.
                append_records(
                    temp_path,
                    compacted_records,
                    max_size=self.max_size
                )

                _compaction_checkpoint("temp_written")

                new_size = os.path.getsize(temp_path)

                _compaction_checkpoint("before_replace")

                # Replace original only after successful write.
                os.replace(temp_path, self.path)

                _compaction_checkpoint("after_replace")

                # Sync directory entry on POSIX systems.
                if os.name != "nt":

                    directory_fd = os.open(
                        directory,
                        os.O_RDONLY
                    )

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
