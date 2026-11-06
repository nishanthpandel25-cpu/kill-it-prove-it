
import multiprocessing
import os
import time

from src.database import Database


RECORD_COUNT = 1_000_000
BATCH_SIZE = 1_000
RECOVERY_LIMIT_SECONDS = 2.0


def million_key_writer(path, ready_event):
    """Write and acknowledge one million keys, then wait for termination."""

    db = Database(path)

    try:
        for start in range(0, RECORD_COUNT, BATCH_SIZE):
            end = min(start + BATCH_SIZE, RECORD_COUNT)

            operations = [
                (
                    "put",
                    f"benchmark_key_{i}".encode(),
                    f"benchmark_value_{i}".encode()
                )
                for i in range(start, end)
            ]

            db.batch(operations)

        # All one million keys have been acknowledged.
        ready_event.set()

        # Keep the process alive until the parent kills it.
        while True:
            time.sleep(1)

    finally:
        db.close()


def recover_and_verify(path, result_queue):
    """Open the database, measure recovery, and verify every key."""

    db = None

    try:
        recovery_start = time.perf_counter()

        db = Database(path)

        recovery_time = time.perf_counter() - recovery_start

        # Verify the total number of recovered records.
        if len(db.data) != RECORD_COUNT:
            raise AssertionError(
                f"Expected {RECORD_COUNT} records, "
                f"found {len(db.data)}"
            )

        # Verify every acknowledged key and its exact value.
        for i in range(RECORD_COUNT):
            key = f"benchmark_key_{i}".encode()
            expected = f"benchmark_value_{i}".encode()

            actual = db.data.get(key)

            if actual != expected:
                raise AssertionError(
                    f"Missing or incorrect record: {i}"
                )

        result_queue.put(
            ("success", recovery_time, len(db.data))
        )

    except BaseException as error:
        result_queue.put(
            ("error", repr(error), None)
        )

    finally:
        if db is not None:
            db.close()


def test_one_million_acknowledged_keys_survive_process_kill(tmp_path):

    path = str(tmp_path / "f3_million_keys.db")

    ready_event = multiprocessing.Event()
    result_queue = multiprocessing.Queue()

    writer = multiprocessing.Process(
        target=million_key_writer,
        args=(path, ready_event)
    )

    writer.start()

    try:
        # Wait until every batch has been acknowledged.
        assert ready_event.wait(timeout=600), (
            "Writer did not finish one million acknowledged PUTs"
        )

        assert os.path.exists(path)

        # Forcefully terminate the writer without graceful close.
        writer.terminate()
        writer.join(timeout=30)

        assert not writer.is_alive(), (
            "Writer process could not be terminated"
        )

        assert writer.exitcode is not None

    finally:
        if writer.is_alive():
            writer.terminate()
            writer.join(timeout=30)

    # Start a separate process to simulate database restart.
    recovery = multiprocessing.Process(
        target=recover_and_verify,
        args=(path, result_queue)
    )

    recovery.start()

    try:
        result = result_queue.get(timeout=180)

        recovery.join(timeout=30)

        assert not recovery.is_alive(), (
            "Recovery process did not terminate"
        )

        assert recovery.exitcode == 0, (
            f"Recovery process exited with {recovery.exitcode}"
        )

        status, recovery_time, recovered_count = result

        assert status == "success", (
            f"Recovery or verification failed: {recovery_time}"
        )

        print(f"Recovered keys: {recovered_count}")
        print(f"Recovery time: {recovery_time:.4f} seconds")

        assert recovered_count == RECORD_COUNT

        assert recovery_time < RECOVERY_LIMIT_SECONDS, (
            f"Recovery took {recovery_time:.4f} seconds; "
            f"required under {RECOVERY_LIMIT_SECONDS} seconds"
        )

    finally:
        if recovery.is_alive():
            recovery.terminate()
            recovery.join(timeout=30)

        result_queue.close()
