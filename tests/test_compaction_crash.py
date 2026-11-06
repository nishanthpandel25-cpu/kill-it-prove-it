
import multiprocessing
import os
import tempfile

import src.database as database_module
from src.database import Database


def prepare_database(path):

    db = Database(path)

    for i in range(100):
        db.put(
            f"key_{i}".encode(),
            f"value_{i}".encode()
        )

    for i in range(50):
        db.delete(f"key_{i}".encode())

    db.close()


def crash_worker(path, target_stage, reached_event):

    db = Database(path)

    def pause_at_checkpoint(stage):

        if stage == target_stage:
            reached_event.set()

            # Wait indefinitely until parent kills this process.
            multiprocessing.Event().wait()

    database_module.COMPACTION_TEST_HOOK = pause_at_checkpoint

    db.compact()


def verify_database(path):

    db = Database(path)

    assert len(db.data) == 50

    for i in range(50):
        assert db.get(f"key_{i}".encode()) is None

    for i in range(50, 100):
        assert db.get(
            f"key_{i}".encode()
        ) == f"value_{i}".encode()

    db.close()


def run_crash_test(path, target_stage):

    reached = multiprocessing.Event()

    process = multiprocessing.Process(
        target=crash_worker,
        args=(path, target_stage, reached)
    )

    process.start()

    try:

        assert reached.wait(timeout=15), (
            f"Worker did not reach checkpoint: {target_stage}"
        )

    finally:

        if process.is_alive():
            process.terminate()

        process.join(timeout=15)

    assert not process.is_alive()

    verify_database(path)


def test_crash_after_temporary_file_is_written():

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "test.db")

        prepare_database(path)

        run_crash_test(path, "temp_written")


def test_crash_immediately_before_replacement():

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "test.db")

        prepare_database(path)

        run_crash_test(path, "before_replace")


def test_crash_immediately_after_replacement():

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "test.db")

        prepare_database(path)

        run_crash_test(path, "after_replace")
