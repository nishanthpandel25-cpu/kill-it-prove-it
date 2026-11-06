
import os
import struct
import zlib

from src.storage import HEADER, MAGIC, PUT, DELETE


MAX_KEY_SIZE = 1024
MAX_VALUE_SIZE = 1024 * 1024


def recover_database(path):

    data = {}

    # Create the database file if it doesn't exist.
    if not os.path.exists(path):
        open(path, "ab").close()

    # Track the last completely valid record.
    valid_end = 0

    with open(path, "r+b") as file:

        while True:

            record_start = file.tell()

            # Read the record header.
            header_bytes = file.read(HEADER.size)

            # Normal end of file.
            if not header_bytes:
                break

            # Incomplete header.
            if len(header_bytes) != HEADER.size:
                break

            magic, operation, key_len, value_len = (
                HEADER.unpack(header_bytes)
            )

            # Validate header fields.
            if magic != MAGIC:
                break

            if operation not in (PUT, DELETE):
                break

            if not 1 <= key_len <= MAX_KEY_SIZE:
                break

            if value_len > MAX_VALUE_SIZE:
                break

            if operation == DELETE and value_len != 0:
                break

            # Read key and value.
            payload_size = key_len + value_len

            payload = file.read(payload_size)

            if len(payload) != payload_size:
                break

            # Read stored checksum.
            checksum_bytes = file.read(4)

            if len(checksum_bytes) != 4:
                break

            stored_checksum = struct.unpack(
                ">I", checksum_bytes
            )[0]

            # Calculate checksum again.
            body = header_bytes + payload

            calculated_checksum = (
                zlib.crc32(body) & 0xFFFFFFFF
            )

            # Reject an invalid record.
            if stored_checksum != calculated_checksum:
                break

            # Separate key and value.
            key = payload[:key_len]
            value = payload[key_len:]

            # Apply the valid operation.
            if operation == PUT:
                data[key] = value

            elif operation == DELETE:
                data.pop(key, None)

            # Update the valid file position.
            valid_end = file.tell()

        # Remove incomplete or invalid trailing data.
        if os.path.getsize(path) != valid_end:

            file.seek(valid_end)
            file.truncate()

            file.flush()
            os.fsync(file.fileno())

    return data
