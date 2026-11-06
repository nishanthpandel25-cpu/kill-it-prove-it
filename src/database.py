
import os
import struct
import threading
import uuid

from src.storage import (
    PUT,
    DELETE,
    BEGIN,
    COMMIT,
    encode_record,
    append_record,
    append_records,
)

from src.recovery import recover_database
from src.file_lock import FileLock


# --------------------------------------------------
# TEST HOOKS
# --------------------------------------------------

COMPACTION_TEST_HOOK = None
BATCH_TEST_HOOK = None


def _compaction_checkpoint(stage):
    """Allow tests to pause compaction at specific stages."""
    if COMPACTION_TEST_HOOK is not None:
        COMPACTION_TEST_HOOK(stage)


def _batch_checkpoint(stage):
    """Allow tests to pause batch operations at specific stages."""
    if BATCH_TEST_HOOK is not None:
        BATCH_TEST_HOOK(stage)


# --------------------------------------------------
# DATABASE ENGINE
# --------------------------------------------------

class Database:

    def __init__(self, path="data/database.db", max_size=None):

        if max_size is not None:
            if (
                not isinstance(max_size, int)
                or isinstance(max_size, bool)
                or max_size < 0
            ):
                raise ValueError(
                    "max_size must be a non-negative integer or None"
                )

        self.path = os.path.abspath(path)
        self.max_size = max_size

        directory = os.path.dirname(self.path)

        if directory:
            os.makedirs(directory, exist_ok=True)

        if not os.path.exists(self.path):
            with open(self.path, "wb"):
                pass

        self.data = {}
        self.closed = False

        # Thread-level synchronization.
        self.lock = threading.RLock()

        # Recover the database while holding the process lock.
        with self.lock, FileLock(self.path):
            self.data = recover_database(self.path)
            self._file_signature = self._get_file_signature()

    # --------------------------------------------------
    # FILE SIGNATURE
    # --------------------------------------------------

    def _get_file_signature(self):

        stat = os.stat(self.path)

        return (
            stat.st_size,
            stat.st_mtime_ns,
            stat.st_ctime_ns,
        )

    def _refresh_index_if_changed(self):

        current_signature = self._get_file_signature()

        if current_signature != self._file_signature:

            self.data = recover_database(self.path)

            self._file_signature = self._get_file_signature()

    # --------------------------------------------------
    # VALIDATION
    # --------------------------------------------------

    def _validate_key(self, key):

        if not isinstance(key, bytes):
            raise TypeError("Key must be bytes")

        if not 1 <= len(key) <= 1024:
            raise ValueError(
                "Key length must be between 1 and 1024 bytes"
            )

    def _validate_value(self, value):

        if not isinstance(value, bytes):
            raise TypeError("Value must be bytes")

        if len(value) > 1024 * 1024:
            raise ValueError(
                "Value cannot exceed 1 MB"
            )

    # --------------------------------------------------
    # PUT
    # --------------------------------------------------

    def put(self, key, value):

        with self.lock, FileLock(self.path):

            if self.closed:
                raise RuntimeError("Database is closed")

            self._validate_key(key)
            self._validate_value(value)

            self._refresh_index_if_changed()

            record = encode_record(PUT, key, value)

            append_record(
                self.path,
                record,
                max_size=self.max_size,
            )

            self.data[key] = value

            self._file_signature = self._get_file_signature()

            return True

    # --------------------------------------------------
    # GET
    # --------------------------------------------------

    def get(self, key):

        with self.lock, FileLock(self.path):

            if self.closed:
                raise RuntimeError("Database is closed")

            self._validate_key(key)

            self._refresh_index_if_changed()

            return self.data.get(key)

    # --------------------------------------------------
    # DELETE
    # --------------------------------------------------

    def delete(self, key):

        with self.lock, FileLock(self.path):

            if self.closed:
                raise RuntimeError("Database is closed")

            self._validate_key(key)

            self._refresh_index_if_changed()

            record = encode_record(DELETE, key)

            append_record(
                self.path,
                record,
                max_size=self.max_size,
            )

            self.data.pop(key, None)

            self._file_signature = self._get_file_signature()

            return True

    # --------------------------------------------------
    # ATOMIC BATCH
    # --------------------------------------------------

    def batch(self, operations):

        with self.lock, FileLock(self.path):

            if self.closed:
                raise RuntimeError("Database is closed")

            if not isinstance(operations, (list, tuple)):
                raise TypeError(
                    "Operations must be a list or tuple"
                )

            if not 1 <= len(operations) <= 10000:
                raise ValueError(
                    "Batch must contain between 1 and 10000 operations"
                )

            self._refresh_index_if_changed()

            encoded_operations = []
            validated_operations = []

            # Validate the complete batch before writing anything.
            for operation in operations:

                if not isinstance(operation, (list, tuple)):
                    raise ValueError("Invalid batch operation")

                if len(operation) == 3 and operation[0] == "put":

                    _, key, value = operation

                    self._validate_key(key)
                    self._validate_value(value)

                    encoded_operations.append(
                        encode_record(PUT, key, value)
                    )

                    validated_operations.append(
                        ("put", key, value)
                    )

                elif len(operation) == 2 and operation[0] == "delete":

                    _, key = operation

                    self._validate_key(key)

                    encoded_operations.append(
                        encode_record(DELETE, key)
                    )

                    validated_operations.append(
                        ("delete", key)
                    )

                else:
                    raise ValueError(
                        "Operation must be ('put', key, value) "
                        "or ('delete', key)"
                    )

            # Generate a unique transaction ID.
            transaction_id = uuid.uuid4().bytes

            operation_count = struct.pack(
                ">I",
                len(encoded_operations),
            )

            begin_record = encode_record(
                BEGIN,
                transaction_id,
                operation_count,
            )

            commit_record = encode_record(
                COMMIT,
                transaction_id,
                operation_count,
            )

            # The complete transaction is appended and synchronized
            # before the in-memory index is modified.
            transaction_records = (
                [begin_record]
                + encoded_operations
                + [commit_record]
            )

            _batch_checkpoint("before_write")

            append_records(
                self.path,
                transaction_records,
                max_size=self.max_size,
            )

            _batch_checkpoint("after_sync")

            # Apply operations in their original order.
            for operation in validated_operations:

                if operation[0] == "put":

                    _, key, value = operation
                    self.data[key] = value

                elif operation[0] == "delete":

                    _, key = operation
                    self.data.pop(key, None)

            self._file_signature = self._get_file_signature()

            _batch_checkpoint("after_commit")

            return True

    # --------------------------------------------------
    # COMPACTION
    # --------------------------------------------------

    def compact(self):

        with self.lock, FileLock(self.path):

            if self.closed:
                raise RuntimeError("Database is closed")

            self._refresh_index_if_changed()

            temp_path = self.path + ".compact.tmp"

            old_size = os.path.getsize(self.path)

            try:

                # STEP 1:
                # Remove a temporary file left by an interrupted
                # previous compaction.
                if os.path.exists(temp_path):
                    os.remove(temp_path)

                # STEP 2:
                # Create records only for currently active keys.
                compacted_records = [
                    encode_record(PUT, key, value)
                    for key, value in self.data.items()
                ]

                # STEP 3:
                # Write to a fresh temporary database file.
                append_records(
                    temp_path,
                    compacted_records,
                    max_size=self.max_size,
                )

                _compaction_checkpoint("temp_written")

                new_size = os.path.getsize(temp_path)

                # STEP 4:
                # The original database remains untouched
                # until the temporary file is complete.
                _compaction_checkpoint("before_replace")

                # STEP 5:
                # Atomically replace the original database.
                os.replace(temp_path, self.path)

                _compaction_checkpoint("after_replace")

                # STEP 6:
                # Synchronize the directory entry on POSIX.
                if os.name != "nt":

                    directory = os.path.dirname(self.path)

                    if not directory:
                        directory = "."

                    directory_fd = os.open(
                        directory,
                        os.O_RDONLY,
                    )

                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)

                # STEP 7:
                # Update the file signature.
                self._file_signature = self._get_file_signature()

                return {
                    "old_size": old_size,
                    "new_size": new_size,
                    "bytes_saved": old_size - new_size,
                }

            finally:

                # Clean up temporary files after normal completion
                # or an exception.
                if os.path.exists(temp_path):
                    os.remove(temp_path)

    # --------------------------------------------------
    # CLOSE
    # --------------------------------------------------

    def close(self):

        with self.lock, FileLock(self.path):

            if self.closed:
                return

            self._refresh_index_if_changed()

            self.closed = True

            return True

    # --------------------------------------------------
    # CONTEXT MANAGER SUPPORT
    # --------------------------------------------------

    def __enter__(self):

        if self.closed:
            raise RuntimeError("Database is closed")

        return self

    def __exit__(self, exc_type, exc_value, traceback):

        self.close()

        return False
