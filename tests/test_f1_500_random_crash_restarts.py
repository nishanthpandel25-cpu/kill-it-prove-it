
import multiprocessing
import random

from src.database import Database
from torture.oracle import Oracle


ITERATIONS = 500
MAX_OPERATIONS_PER_ITERATION = 8
SEED = 42
PROCESS_EXIT_TIMEOUT = 10


def crash_worker(path, operations, connection):
    """
    Execute only when authorized by the parent.

    After each database operation returns, send its acknowledgement
    and wait for permission before executing another operation.
    """

    db = None

    try:
        db = Database(path)
        connection.send(("ready", None))

        for operation in operations:

            # Do not execute until the parent explicitly authorizes it.
            command = connection.recv()

            if command != "execute":
                break

            operation_type = operation[0]

            if operation_type == "put":
                _, key, value = operation
                db.put(key, value)

            elif operation_type == "delete":
                _, key = operation
                db.delete(key)

            else:
                raise ValueError(
                    f"Unknown operation: {operation_type}"
                )

            # Send acknowledgement only after the operation returns.
            connection.send(("ack", operation))

        # Remain alive until forcefully terminated by the parent.
        connection.recv()

    finally:
        if db is not None:
            db.close()

        connection.close()


def verify_database(path, oracle, iteration):
    """Restart the database and verify its complete state."""

    db = Database(path)

    try:
        assert oracle.verify(db.data), (
            f"Invariant failed after iteration {iteration}. "
            f"Expected {len(oracle.data)} records, "
            f"recovered {len(db.data)} records."
        )

        for key, expected_value in oracle.snapshot().items():

            actual_value = db.get(key)

            assert actual_value == expected_value, (
                f"Value mismatch after iteration {iteration}: "
                f"{key!r}"
            )

    finally:
        db.close()


def test_500_random_process_kills_and_restarts(tmp_path):
    """
    F1:
    Perform 500 consecutive process terminations and restarts.

    The parent authorizes each operation individually.
    The oracle is updated only after receiving its acknowledgement.
    """

    path = str(tmp_path / "f1_random_crashes.db")

    rng = random.Random(SEED)
    oracle = Oracle()

    key_pool = [
        f"random_key_{index}".encode()
        for index in range(50)
    ]

    total_acknowledged = 0
    total_kills = 0

    for iteration in range(1, ITERATIONS + 1):

        operation_count = rng.randint(
            1,
            MAX_OPERATIONS_PER_ITERATION
        )

        operations = []

        for _ in range(operation_count):

            operation_type = rng.choice(("put", "delete"))
            key = rng.choice(key_pool)

            if operation_type == "put":

                value = (
                    f"value_{iteration}_{rng.randint(0, 1000000)}"
                ).encode()

                operations.append(("put", key, value))

            else:
                operations.append(("delete", key))

        # Randomly select how many operations to acknowledge.
        # Zero means kill before authorizing any operation.
        kill_after = rng.randint(0, operation_count)

        parent_connection, child_connection = (
            multiprocessing.Pipe(duplex=True)
        )

        process = multiprocessing.Process(
            target=crash_worker,
            args=(path, operations, child_connection)
        )

        process.start()
        child_connection.close()

        try:
            assert parent_connection.poll(30), (
                f"Worker startup timeout at iteration {iteration}"
            )

            status, _ = parent_connection.recv()

            assert status == "ready", (
                f"Worker failed to initialize at iteration {iteration}"
            )

            acknowledged_this_iteration = 0

            while acknowledged_this_iteration < kill_after:

                # Authorize exactly one operation.
                parent_connection.send("execute")

                assert parent_connection.poll(30), (
                    f"Worker acknowledgement timeout at iteration "
                    f"{iteration}"
                )

                status, operation = parent_connection.recv()

                assert status == "ack", (
                    f"Unexpected worker response: {status}"
                )

                # Update oracle only after receiving acknowledgement.
                oracle.apply(operation)

                acknowledged_this_iteration += 1
                total_acknowledged += 1

            # The child is now waiting for authorization to execute
            # another operation. No next operation can race with the kill.
            process.terminate()
            process.join(timeout=PROCESS_EXIT_TIMEOUT)

            assert not process.is_alive(), (
                f"Worker did not terminate at iteration {iteration}"
            )

            assert process.exitcode is not None, (
                f"Worker has no exit code at iteration {iteration}"
            )

            total_kills += 1

        finally:
            if process.is_alive():
                process.terminate()
                process.join(timeout=PROCESS_EXIT_TIMEOUT)

            parent_connection.close()

        # Reopen and verify after every forced termination.
        verify_database(path, oracle, iteration)

    assert total_kills == ITERATIONS

    print(f"Crash iterations: {total_kills}")
    print(f"Acknowledged operations: {total_acknowledged}")
    print(f"Final oracle records: {len(oracle.data)}")
    print("Invariant verification: PASSED after every restart")


if __name__ == "__main__":
    multiprocessing.freeze_support()
