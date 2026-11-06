
import multiprocessing
import os
import tempfile
import struct
import zlib

from src.database import Database
from src.storage import HEADER, MAGIC


def writer(path, prefix, count):
    db = Database(path)

    try:
        for i in range(count):
            key = f"{prefix}_{i}".encode()
            value = f"value_{i}".encode()
            db.put(key, value)

        print(f"{prefix}: completed {count} writes")

    finally:
        db.close()


def inspect_raw_file(path):
    print("\n========== RAW FILE INSPECTION ==========")

    file_size = os.path.getsize(path)
    print("Database file size:", file_size, "bytes")

    valid_records = 0
    first_problem = None

    with open(path, "rb") as file:
        while True:
            offset = file.tell()
            header_bytes = file.read(HEADER.size)

            if not header_bytes:
                break

            if len(header_bytes) != HEADER.size:
                first_problem = (
                    offset,
                    "Incomplete header",
                    len(header_bytes)
                )
                break

            magic, operation, key_len, value_len = HEADER.unpack(
                header_bytes
            )

            if magic != MAGIC:
                first_problem = (offset, "Invalid magic", magic)
                break

            payload = file.read(key_len + value_len)

            if len(payload) != key_len + value_len:
                first_problem = (offset, "Incomplete payload")
                break

            checksum_bytes = file.read(4)

            if len(checksum_bytes) != 4:
                first_problem = (offset, "Incomplete checksum")
                break

            stored_checksum = struct.unpack(
                ">I", checksum_bytes
            )[0]

            calculated_checksum = zlib.crc32(
                header_bytes + payload
            ) & 0xFFFFFFFF

            if stored_checksum != calculated_checksum:
                first_problem = (offset, "Checksum mismatch")
                break

            valid_records += 1

    print("Valid raw records:", valid_records)
    print("Expected raw records: 200")

    if first_problem:
        print("FIRST INVALID RECORD:", first_problem)
        print("Invalid record starts at byte:", first_problem[0])
        print("Bytes remaining:", file_size - first_problem[0])
    else:
        print("No invalid record detected.")

    print("=========================================\n")


def main():
    with tempfile.TemporaryDirectory() as folder:
        path = os.path.join(folder, "database.db")

        db = Database(path)
        db.close()

        p1 = multiprocessing.Process(
            target=writer,
            args=(path, "worker1", 100)
        )

        p2 = multiprocessing.Process(
            target=writer,
            args=(path, "worker2", 100)
        )

        p1.start()
        p2.start()

        p1.join()
        p2.join()

        print("Worker 1 exit code:", p1.exitcode)
        print("Worker 2 exit code:", p2.exitcode)

        # Inspect the file BEFORE opening Database again.
        inspect_raw_file(path)

        # Recovery happens here.
        db = Database(path)

        missing = []

        for worker in ("worker1", "worker2"):
            for i in range(100):
                key = f"{worker}_{i}".encode()
                expected = f"value_{i}".encode()
                actual = db.get(key)

                if actual != expected:
                    missing.append((key, actual, expected))

        print("Expected records:", 200)
        print("Recovered records:", len(db.data))
        print("Missing or incorrect records:", len(missing))
        print("First 15 problems:", missing[:15])

        db.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
