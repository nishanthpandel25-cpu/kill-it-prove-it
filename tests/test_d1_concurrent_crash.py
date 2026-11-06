
import multiprocessing
import os
import random
import tempfile

from src.database import Database


WRITES_PER_WORKER = 500
MIN_ACKNOWLEDGEMENTS = 20
CRASH_EXIT_CODE = 71


def concurrent_writer(path, worker_id, connection):
    """Write unique keys and wait for parent confirmation."""

    db = None

    try:
        db = Database(path)

        connection.send(("ready", worker_id))

        if connection.recv() != "start":
            return

        for index in range(WRITES_PER_WORKER):

            key = f"worker_{worker_id}_key_{index}".encode()
            value = f"value_{worker_id}_{index}".encode()

            db.put(key, value)

            # Report only after put() returns successfully.
            connection.send(("ack", worker_id, key, value))

            # Wait for the parent before starting the next write.
            if connection.recv() != "continue":
                return

        connection.send(("done", worker_id))

    except Exception as error:

        try:
            connection.send(("error", worker_id, repr(error)))
        except (BrokenPipeError, EOFError, OSError):
            pass

    finally:
        if db is not None:
            db.close()

        connection.close()


class TestConcurrentWriterCrash:

    def test_two_concurrent_writers_with_random_termination(self):

        with tempfile.TemporaryDirectory() as folder:

            path = os.path.join(folder, "concurrent_crash.db")

            db = Database(path)
            db.close()

            rng = random.Random(42)

            parent_connections = []
            processes = []

            acknowledged = {}

            try:
                # Create and start both writers.
                for worker_id in range(2):

                    parent_conn, child_conn = multiprocessing.Pipe()

                    process = multiprocessing.Process(
                        target=concurrent_writer,
                        args=(path, worker_id, child_conn)
                    )

                    processes.append(process)
                    parent_connections.append(parent_conn)

                    process.start()
                    child_conn.close()

                # Wait until both workers are initialized.
                ready_workers = set()

                for connection in parent_connections:

                    if not connection.poll(15):
                        raise TimeoutError(
                            "A writer did not become ready"
                        )

                    message = connection.recv()

                    if message[0] != "ready":
                        raise RuntimeError(
                            f"Unexpected initialization response: {message}"
                        )

                    ready_workers.add(message[1])

                assert ready_workers == {0, 1}

                # Start both writers together.
                for connection in parent_connections:
                    connection.send("start")

                # Require at least one successful write from each.
                for worker_id, connection in enumerate(parent_connections):

                    if not connection.poll(15):
                        raise TimeoutError(
                            f"Worker {worker_id} did not acknowledge a write"
                        )

                    message = connection.recv()

                    if message[0] == "error":
                        raise RuntimeError(message[2])

                    assert message[0] == "ack"

                    _, actual_worker, key, value = message

                    assert actual_worker == worker_id

                    acknowledged[key] = value

                # Continue both writers until a random crash point.
                target_acknowledgements = rng.randint(
                    MIN_ACKNOWLEDGEMENTS,
                    MIN_ACKNOWLEDGEMENTS + 10
                )

                while len(acknowledged) < target_acknowledgements:

                    worker_id = rng.choice([0, 1])
                    connection = parent_connections[worker_id]

                    connection.send("continue")

                    if not connection.poll(15):
                        raise TimeoutError(
                            f"Worker {worker_id} did not respond"
                        )

                    message = connection.recv()

                    if message[0] == "error":
                        raise RuntimeError(message[2])

                    if message[0] == "done":
                        raise RuntimeError(
                            "Worker finished before the crash point"
                        )

                    assert message[0] == "ack"

                    _, actual_worker, key, value = message

                    assert actual_worker == worker_id

                    acknowledged[key] = value

                # Both independent writers are now active.
                # Terminate them at this randomized point.
                for process in processes:

                    if process.is_alive():
                        process.terminate()

                for process in processes:
                    process.join(timeout=15)

                assert len(acknowledged) >= MIN_ACKNOWLEDGEMENTS

                # Reopen and recover the database.
                db = Database(path)

                try:

                    for key, expected_value in acknowledged.items():

                        assert db.get(key) == expected_value, (
                            f"Acknowledged write was lost: {key!r}"
                        )

                    assert len(db.data) == len(acknowledged), (
                        "Unexpected or missing records after recovery"
                    )

                    print("Acknowledged writes:", len(acknowledged))
                    print("Recovered records:", len(db.data))
                    print("Workers terminated:", len(processes))
                    print("Oracle verification: PASSED")

                finally:
                    db.close()

            finally:

                for process in processes:

                    if process.is_alive():
                        process.terminate()

                    process.join(timeout=10)

                for connection in parent_connections:
                    connection.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
