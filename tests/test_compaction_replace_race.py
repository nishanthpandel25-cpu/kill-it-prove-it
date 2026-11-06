
import multiprocessing
import os
import tempfile

from src.database import Database
import src.database as database_module


RECORD_COUNT = 100
DELETE_COUNT = 50
RACE_ITERATIONS = 20


def prepare_database(path):
    """Create a database containing live and deleted records."""

    db = Database(path)

    try:
        for index in range(RECORD_COUNT):
            db.put(
                f"key_{index}".encode(),
                f"value_{index}".encode()
            )

        for index in range(DELETE_COUNT):
            db.delete(f"key_{index}".encode())

    finally:
        db.close()


def compaction_worker(path, before_event, release_event, after_event):
    """Pause on both sides of the atomic replacement."""

    db = Database(path)

    def checkpoint(stage):
        if stage == "before_replace":
            before_event.set()
            release_event.wait()

        elif stage == "after_replace":
            after_event.set()
            multiprocessing.Event().wait()

    database_module.COMPACTION_TEST_HOOK = checkpoint

    db.compact()


def verify_database(path):
    """Verify that every committed key retains its correct state."""

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


def terminate_worker(process):
    """Terminate and collect the worker process."""

    if process.is_alive():
        process.terminate()

    process.join(timeout=15)

    assert not process.is_alive(), "Worker failed to terminate"


def test_process_termination_on_both_sides_of_atomic_replace():

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "replace_race.db")

        for iteration in range(RACE_ITERATIONS):

            prepare_database(path)

            before_event = multiprocessing.Event()
            release_event = multiprocessing.Event()
            after_event = multiprocessing.Event()

            process = multiprocessing.Process(
                target=compaction_worker,
                args=(
                    path,
                    before_event,
                    release_event,
                    after_event
                )
            )

            process.start()

            try:
                assert before_event.wait(timeout=15), (
                    f"Worker did not reach before_replace "
                    f"in iteration {iteration + 1}"
                )

                if iteration % 2 == 0:
                    # Terminate before os.replace().
                    terminate_worker(process)

                else:
                    # Allow the actual os.replace() to execute.
                    release_event.set()

                    assert after_event.wait(timeout=15), (
                        f"Worker did not reach after_replace "
                        f"in iteration {iteration + 1}"
                    )

                    # Terminate after replacement completed.
                    terminate_worker(process)

            finally:
                if process.is_alive():
                    terminate_worker(process)

            # Every restart must recover the complete logical state.
            verify_database(path)

            print(
                f"Iteration {iteration + 1}/{RACE_ITERATIONS}: "
                "recovery verified"
            )


if __name__ == "__main__":
    multiprocessing.freeze_support()
