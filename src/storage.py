
import os
import struct
import zlib


# Binary record header:
# Magic (4 bytes)
# Operation (1 byte)
# Key length (4 bytes)
# Value length (4 bytes)

HEADER = struct.Struct(">4sBII")

MAGIC = b"KVR1"

PUT = 1
DELETE = 2


def encode_record(operation, key, value=b""):

    # Create the record header.
    header = HEADER.pack(
        MAGIC,
        operation,
        len(key),
        len(value)
    )

    # Combine header, key and value.
    body = header + key + value

    # Calculate CRC32 checksum.
    checksum = zlib.crc32(body) & 0xFFFFFFFF

    # Convert checksum into 4 bytes.
    checksum_bytes = struct.pack(">I", checksum)

    # Return the complete binary record.
    return body + checksum_bytes


def append_record(path, record):

    # Open the file in append mode.
    fd = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_APPEND,
        0o666
    )

    try:

        # Remember the original file size.
        original_size = os.fstat(fd).st_size

        # Write the complete record.
        view = memoryview(record)

        while view:

            written = os.write(fd, view)

            if written <= 0:
                raise OSError("Unable to write database record")

            view = view[written:]

        # Force data to persistent storage.
        os.fsync(fd)

    except BaseException:

        # Attempt to remove an incomplete record
        # if writing fails.
        try:
            os.ftruncate(fd, original_size)
            os.fsync(fd)
        except OSError:
            pass

        raise

    finally:

        os.close(fd)
