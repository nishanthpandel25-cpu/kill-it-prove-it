import os
import struct
import zlib

from src.storage import (
    HEADER,
    MAGIC,
    PUT,
    DELETE,
    BEGIN,
    COMMIT
)

MAX_KEY_SIZE = 1024
MAX_VALUE_SIZE = 1024 * 1024


def recover_database(path):
    data = {}

    if not os.path.exists(path):
        open(path, "ab").close()

    committed_end = 0
    pending_start = None
    pending_id = None
    pending_count = None
    pending_operations = []

    with open(path, "r+b") as file:

        while True:
            record_start = file.tell()

            header_bytes = file.read(HEADER.size)

            if not header_bytes:
                break

            if len(header_bytes) != HEADER.size:
                break

            magic, operation, key_len, value_len = HEADER.unpack(
                header_bytes
            )

            if magic != MAGIC:
                break

            if operation not in (PUT, DELETE, BEGIN, COMMIT):
                break

            if key_len < 1 or key_len > MAX_KEY_SIZE:
                break

            if value_len > MAX_VALUE_SIZE:
                break

            if operation == DELETE and value_len != 0:
                break

            if operation in (BEGIN, COMMIT):
                if key_len != 16 or value_len != 4:
                    break

            payload_size = key_len + value_len
            payload = file.read(payload_size)

            if len(payload) != payload_size:
                break

            checksum_bytes = file.read(4)

            if len(checksum_bytes) != 4:
                break

            stored_checksum = struct.unpack(
                ">I", checksum_bytes
            )[0]

            body = header_bytes + payload

            calculated_checksum = zlib.crc32(body) & 0xFFFFFFFF

            if stored_checksum != calculated_checksum:
                break

            key = payload[:key_len]
            value = payload[key_len:]

            if operation == BEGIN:
                if pending_start is not None:
                    break

                pending_start = record_start
                pending_id = key
                pending_count = struct.unpack(">I", value)[0]
                pending_operations = []

            elif operation == COMMIT:
                if pending_start is None:
                    break

                commit_count = struct.unpack(">I", value)[0]

                if key != pending_id:
                    break

                if commit_count != pending_count:
                    break

                if len(pending_operations) != pending_count:
                    break

                for op, op_key, op_value in pending_operations:
                    if op == PUT:
                        data[op_key] = op_value
                    else:
                        data.pop(op_key, None)

                pending_start = None
                pending_id = None
                pending_count = None
                pending_operations = []

                committed_end = file.tell()

            else:
                if operation == PUT:
                    current_operation = (PUT, key, value)
                else:
                    current_operation = (DELETE, key, b"")

                if pending_start is not None:
                    pending_operations.append(current_operation)

                    if len(pending_operations) > pending_count:
                        break
                else:
                    if operation == PUT:
                        data[key] = value
                    else:
                        data.pop(key, None)

                    committed_end = file.tell()

        # If a batch is unfinished, remove it entirely.
        if pending_start is not None:
            committed_end = pending_start

        if os.path.getsize(path) != committed_end:
            file.seek(committed_end)
            file.truncate()
            file.flush()
            os.fsync(file.fileno())

    return data