
import multiprocessing
import os
import tempfile
import time

import src.database as database_module
from src.database import Database


RECORD_COUNT = 100
DELETE_COUNT = 50
RACE_ITERATIONS = 20


def prepare_database(path):
    """Create a database with live and deleted records."""

    db = Database(path)

    for index in range(RECORD_COUNT):
        db.put(
            f"key_{index}".encode(),
            f"value_{index}".encode()
        )

    for index in range(DELETE_COUNT):
        db.delete(f"key_{index}".encode())

    db.close()


def compaction_worker(path, ready_event):
    """Pause immediately before the atomic replacement."""

    db = Database(path)

    def checkpoint(stage):
        if stage == "before_replace":
            ready_event.set()

            # Wait until the parent terminates this process.
            multiprocessing.Event().wait()

    database_module.COMPACTION_TEST_HOOK = checkpoint

    db.compact()


def verify_database(path):
    """Verify that the original committed state is preserved."""

    db = Database(path)

    try:
        assert len(db.data) == RECORD_COUNT - DELETE_COUNT

        for index in range(DELETE_COUNT):
            assert db.get(f"key_{index}".encode()) is None

        for index in range(DELETE_COUNT, RECORD_COUNT):
            assert db.get(
                f"key_{index}".encode()
            ) == f"value_{index}".encode()

    finally:
        db.close()


def test_process_termination_around_atomic_replace():

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "replace_race.db")

        for iteration in range(RACE_ITERATIONS):

            # Start each iteration from a valid database.
            prepare_database(path)

            ready_event = multiprocessing.Event()

            process = multiprocessing.Process(
                target=compaction_worker,
                args=(path, ready_event)
            )

            process.start()

            try:

                assert ready_event.wait(timeout=15), (
                    "Compaction did not reach before_replace "
                    f"checkpoint in iteration {iteration}"
                )

                # Vary the termination delay around the replacement
                # boundary. The checkpoint pauses before os.replace.
                delay = (iteration % 5) * 0.001

                if delay:
                    time.sleep(delay)

            finally:

                if process.is_alive():
                    process.terminate()

                process.join(timeout=15)

            assert not process.is_alive(), (
                f"Worker did not terminate in iteration {iteration}"
            )

            # Reopen after every forced termination.
            verify_database(path)

            print(
                f"Iteration {iteration + 1}/{RACE_ITERATIONS}: "
                "database recovered correctly"
            )


if __name__ == "__main__":
    multiprocessing.freeze_support()
