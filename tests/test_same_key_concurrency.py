
import multiprocessing
import os
import tempfile

from src.database import Database


WRITES_PER_WORKER = 100
SHARED_KEY = b"shared_key"


def same_key_writer(path, worker_id, start_event, result_queue):
    """Repeatedly update the same database key."""

    db = None

    try:
        db = Database(path)

        start_event.wait()

        for index in range(WRITES_PER_WORKER):

            value = (
                f"worker_{worker_id}_value_{index}"
            ).encode()

            db.put(SHARED_KEY, value)

        result_queue.put(("success", worker_id))

    except Exception as error:

        result_queue.put(
            ("error", worker_id, repr(error))
        )

    finally:
        if db is not None:
            db.close()


class TestSameKeyConcurrency:

    def test_two_processes_update_the_same_key(self):

        with tempfile.TemporaryDirectory() as folder:

            path = os.path.join(folder, "same_key.db")

            # Initialize the database.
            db = Database(path)
            db.close()

            start_event = multiprocessing.Event()
            result_queue = multiprocessing.Queue()

            processes = []

            # Start two independent writer processes.
            for worker_id in range(2):

                process = multiprocessing.Process(
                    target=same_key_writer,
                    args=(
                        path,
                        worker_id,
                        start_event,
                        result_queue
                    )
                )

                processes.append(process)
                process.start()

            try:

                # Release both writers simultaneously.
                start_event.set()

                results = []

                for _ in range(2):
                    results.append(result_queue.get(timeout=30))

                for process in processes:
                    process.join(timeout=30)

                # Both workers must finish successfully.
                for process in processes:
                    assert not process.is_alive(), (
                        "Writer process timed out"
                    )

                    assert process.exitcode == 0

                assert all(
                    result[0] == "success"
                    for result in results
                ), f"Writer errors: {results}"

                # Reopen and recover the database.
                db = Database(path)

                try:

                    final_value = db.get(SHARED_KEY)

                    # Every possible complete value is valid.
                    valid_values = {
                        f"worker_{worker_id}_value_{index}".encode()
                        for worker_id in range(2)
                        for index in range(WRITES_PER_WORKER)
                    }

                    assert final_value in valid_values, (
                        f"Unexpected final value: {final_value!r}"
                    )

                    assert len(db.data) == 1, (
                        "Unexpected records found in database"
                    )

                    print("Writer processes:", len(processes))
                    print(
                        "Total PUT operations:",
                        2 * WRITES_PER_WORKER
                    )
                    print("Final shared-key value:", final_value)
                    print("Recovery verification: PASSED")

                finally:
                    db.close()

            finally:

                for process in processes:

                    if process.is_alive():
                        process.terminate()

                    process.join(timeout=10)

                result_queue.close()
                result_queue.join_thread()


if __name__ == "__main__":
    multiprocessing.freeze_support()
