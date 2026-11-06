
import multiprocessing
import os
import tempfile
import time

from src.database import Database
from src import storage


BATCH_SIZE = 10_000
CRASH_AFTER_PUTS = 5_000
EXISTING_RECORDS = 100
CRASH_EXIT_CODE = 67
RECOVERY_LIMIT = 2.0


def crash_halfway_through_batch(path):
    """Kill the process after 5,000 complete PUT records."""

    db = Database(path)

    original_write = os.write
    put_count = 0

    def controlled_write(fd, data):
        nonlocal put_count

        # Complete the physical write, handling partial writes.
        view = memoryview(data)

        while view:
            written = original_write(fd, view)

            if written <= 0:
                raise OSError("Physical write failed")

            view = view[written:]

        # Identify the record operation from its binary header.
        operation = storage.HEADER.unpack(
            bytes(data[:storage.HEADER.size])
        )[1]

        if operation == storage.PUT:
            put_count += 1

            if put_count == CRASH_AFTER_PUTS:
                # 5,000 PUT records have been fully written.
                # Terminate before the next record or COMMIT.
                os._exit(CRASH_EXIT_CODE)

        return len(data)

    storage.os.write = controlled_write

    operations = [
        (
            "put",
            f"batch_key_{index}".encode(),
            f"batch_value_{index}".encode()
        )
        for index in range(BATCH_SIZE)
    ]

    db.batch(operations)


class TestTenThousandBatchCrash:

    def test_10k_batch_crash_recovery_under_two_seconds(self):

        with tempfile.TemporaryDirectory() as folder:

            path = os.path.join(folder, "10k_crash.db")

            # Create 100 previously committed records.
            db = Database(path)

            for index in range(EXISTING_RECORDS):
                db.put(
                    f"existing_{index}".encode(),
                    f"existing_value_{index}".encode()
                )

            db.close()

            # Start the batch in a separate process.
            process = multiprocessing.Process(
                target=crash_halfway_through_batch,
                args=(path,)
            )

            process.start()
            process.join(timeout=60)

            assert not process.is_alive(), (
                "Batch process did not terminate"
            )

            assert process.exitcode == CRASH_EXIT_CODE, (
                f"Expected exit code {CRASH_EXIT_CODE}, "
                f"got {process.exitcode}"
            )

            # Measure the complete database reopening/recovery time.
            start_time = time.perf_counter()

            db = Database(path)

            recovery_time = time.perf_counter() - start_time

            try:
                # Verify all previously committed records.
                for index in range(EXISTING_RECORDS):
                    key = f"existing_{index}".encode()
                    expected = f"existing_value_{index}".encode()

                    assert db.get(key) == expected, (
                        f"Existing record {index} was lost"
                    )

                # No operation from the incomplete batch may survive.
                for index in range(BATCH_SIZE):
                    key = f"batch_key_{index}".encode()

                    assert db.get(key) is None, (
                        f"Incomplete batch operation survived: {key}"
                    )

                assert len(db.data) == EXISTING_RECORDS, (
                    f"Expected {EXISTING_RECORDS} records, "
                    f"recovered {len(db.data)}"
                )

                assert recovery_time < RECOVERY_LIMIT, (
                    f"Recovery took {recovery_time:.3f} seconds; "
                    f"limit is {RECOVERY_LIMIT:.3f} seconds"
                )

                print(f"\nBatch operations: {BATCH_SIZE}")
                print(f"Operations written before crash: {CRASH_AFTER_PUTS}")
                print(f"Recovered records: {len(db.data)}")
                print(f"Recovery time: {recovery_time:.4f} seconds")

            finally:
                db.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
