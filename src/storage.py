
import os
import struct
import zlib

HEADER = struct.Struct(">4sBII")
MAGIC = b"KVR1"

PUT = 1
DELETE = 2
BEGIN = 3
COMMIT = 4


def encode_record(operation, key, value=b""):
    header = HEADER.pack(
        MAGIC,
        operation,
        len(key),
        len(value)
    )

    body = header + key + value
    checksum = zlib.crc32(body) & 0xFFFFFFFF

    return body + struct.pack(">I", checksum)


def append_records(path, records):
    """
    Append binary records and sync them to disk.
    """

    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND

    # Essential on Windows: prevent text-mode newline translation.
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY

    fd = os.open(path, flags, 0o666)

    original_size = os.fstat(fd).st_size

    try:
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
        except OSError:
            pass

        raise

    finally:
        os.close(fd)


def append_record(path, record):
    append_records(path, [record])
