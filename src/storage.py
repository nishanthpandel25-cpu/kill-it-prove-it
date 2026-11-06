import os
import struct
import zlib


HEADER = struct.Struct(">4sBII")
MAGIC = b"KVR1"

PUT = 1
DELETE = 2
BEGIN = 3
COMMIT = 4


class DatabaseFullError(OSError):
    """Raised when a database write exceeds its configured size."""


class RollbackError(RuntimeError):
    """Raised when a failed write cannot be safely rolled back."""


def encode_record(operation, key, value=b""):
    header = HEADER.pack(MAGIC, operation, len(key), len(value))
    body = header + key + value
    checksum = zlib.crc32(body) & 0xFFFFFFFF
    return body + struct.pack(">I", checksum)


def append_records(path, records, max_size=None):
    """Append records and synchronize them to disk.

    If writing fails, attempt to restore the original file length.
    RollbackError means that restoration or its synchronization
    could not be confirmed.
    """
    if max_size is not None:
        if not isinstance(max_size, int) or isinstance(max_size, bool):
            raise TypeError("max_size must be an integer")
        if max_size < 0:
            raise ValueError("max_size cannot be negative")

    # Materialize once so generators and other iterables work correctly.
    records = list(records)

    total_size = 0
    for record in records:
        if not isinstance(record, bytes):
            raise TypeError("Every record must be bytes")
        total_size += len(record)

    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY

    fd = os.open(path, flags, 0o666)
    original_size = os.fstat(fd).st_size

    try:
        if max_size is not None and original_size + total_size > max_size:
            raise DatabaseFullError(
                "Database size limit exceeded: "
                f"current={original_size} bytes, "
                f"append={total_size} bytes, "
                f"maximum={max_size} bytes"
            )

        for record in records:
            view = memoryview(record)

            while view:
                written = os.write(fd, view)

                if written <= 0:
                    raise OSError("Unable to write database record")

                view = view[written:]

        os.fsync(fd)

    except BaseException:
        try:
            os.ftruncate(fd, original_size)
            os.fsync(fd)
        except OSError as rollback_error:
            raise RollbackError(
                "Database write failed and rollback "
                "could not be confirmed"
            ) from rollback_error

        raise

    finally:
        os.close(fd)


def append_record(path, record, max_size=None):
    append_records(path, [record], max_size=max_size)