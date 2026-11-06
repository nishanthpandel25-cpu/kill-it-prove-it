
import multiprocessing
import os
import random
import tempfile

from src.database import Database
from torture_tests.oracle import Oracle


# --------------------------------------------------
# CHILD PROCESS: DATABASE WRITER
# --------------------------------------------------

def writer_process(path, operations, connection):

    db = None

    try:

        db = Database(path)

        # Tell the parent that the database is ready.
        connection.send(("ready",))

        # Wait for permission to start.
        if connection.recv() != "start":
            return

        for index, operation in enumerate(operations):

            operation_type, key, value = operation

            if operation_type != "put":
                raise ValueError("Only PUT is supported here")

            # Perform a durable database write.
            db.put(key, value)

            # Report only after put() returns successfully.
            connection.send(("ack", index, key, value))

            # Wait for parent acknowledgement before
            # starting the next operation.
            if connection.recv() != "continue":
                return

        connection.send(("done",))

    except Exception as error:

        try:
            connection.send(("error", repr(error)))
        except (BrokenPipeError, EOFError, OSError):
            pass

    finally:

        if db is not None:
            db.close()

        connection.close()


# --------------------------------------------------
# VERIFY RECOVERED DATABASE
# --------------------------------------------------

def verify_recovered_database(path, oracle):

    db = Database(path)

    try:

        if not oracle.verify(db.data):

            missing = {
                key: value
                for key, value in oracle.data.items()
                if db.data.get(key) != value
            }

            raise AssertionError(
                f"Recovered database differs from oracle. "
                f"Incorrect or missing records: {list(missing)[:10]}"
            )

        # Check every expected record individually.
        for key, expected_value in oracle.snapshot().items():

            actual_value = db.get(key)

            assert actual_value == expected_value, (
                f"Recovery mismatch for key {key!r}"
            )

    finally:

        db.close()


# --------------------------------------------------
# ONE RANDOM CRASH ITERATION
# --------------------------------------------------

def run_one_crash_iteration(
    path,
    oracle,
    iteration,
    operations_per_iteration,
    rng
):

    operations = []

    for index in range(operations_per_iteration):

        key = (
            f"crash_{iteration}_key_{index}"
        ).encode()

        value = (
            f"value_{rng.randint(1, 100000000)}"
        ).encode()

        operations.append(("put", key, value))

    # Randomly choose when to terminate the writer.
    kill_after = rng.randint(
        1,
        operations_per_iteration
    )

    parent_connection, child_connection = (
        multiprocessing.Pipe()
    )

    process = multiprocessing.Process(
        target=writer_process,
        args=(
            path,
            operations,
            child_connection
        )
    )

    process.start()
    child_connection.close()

    acknowledged = 0

    try:

        # Wait for child database initialization.
        if not parent_connection.poll(30):

            raise TimeoutError(
                "Writer did not initialize within 30 seconds"
            )

        message = parent_connection.recv()

        if message[0] != "ready":

            raise RuntimeError(
                f"Writer initialization failed: {message}"
            )

        parent_connection.send("start")

        while acknowledged < kill_after:

            if not parent_connection.poll(30):

                raise TimeoutError(
                    f"Writer timed out in iteration {iteration}"
                )

            message = parent_connection.recv()

            if message[0] == "error":

                raise RuntimeError(
                    f"Writer error: {message[1]}"
                )

            if message[0] != "ack":

                raise RuntimeError(
                    f"Unexpected writer response: {message}"
                )

            _, operation_index, key, value = message

            expected_operation = operations[operation_index]

            assert (
                key == expected_operation[1]
                and value == expected_operation[2]
            ), "Writer acknowledgement did not match operation"

            # Record the acknowledged operation in the
            # independent oracle before allowing another write.
            oracle.apply(("put", key, value))

            acknowledged += 1

            if acknowledged < kill_after:

                parent_connection.send("continue")

        # The writer is now waiting for permission to continue.
        # Kill it before it can begin another operation.
        process.terminate()
        process.join(timeout=15)

        if process.is_alive():

            process.kill()
            process.join(timeout=5)

        if process.is_alive():

            raise RuntimeError(
                "Writer process could not be terminated"
            )

    finally:

        parent_connection.close()

        if process.is_alive():

            process.terminate()
            process.join(timeout=15)

    # Reopen and verify all acknowledged operations.
    verify_recovered_database(path, oracle)

    return acknowledged


# --------------------------------------------------
# MAIN CRASH TORTURE RUNNER
# --------------------------------------------------

def run_crash_torture(
    iterations=500,
    operations_per_iteration=10,
    seed=42
):

    if not isinstance(iterations, int) or isinstance(iterations, bool):
        raise TypeError("iterations must be an integer")

    if iterations < 1:
        raise ValueError("iterations must be positive")

    if (
        not isinstance(operations_per_iteration, int)
        or isinstance(operations_per_iteration, bool)
    ):
        raise TypeError(
            "operations_per_iteration must be an integer"
        )

    if operations_per_iteration < 1:
        raise ValueError(
            "operations_per_iteration must be positive"
        )

    rng = random.Random(seed)

    oracle = Oracle()

    total_acknowledged = 0

    print("\n========== RANDOM CRASH TORTURE ==========")

    print("Iterations:", iterations)
    print("Operations per iteration:", operations_per_iteration)
    print("Random seed:", seed)

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "crash_torture.db")

        for iteration in range(1, iterations + 1):

            acknowledged = run_one_crash_iteration(
                path,
                oracle,
                iteration,
                operations_per_iteration,
                rng
            )

            total_acknowledged += acknowledged

            if iteration % 10 == 0 or iteration == iterations:

                print(
                    f"Completed {iteration}/{iterations} "
                    f"| Acknowledged writes: {total_acknowledged} "
                    f"| Recovered keys: {len(oracle.data)}"
                )

    print("\nCrash iterations:", iterations)
    print("Acknowledged writes:", total_acknowledged)
    print("Final oracle keys:", len(oracle.data))
    print("Recovery verification: PASSED")
    print("\nRANDOM CRASH TORTURE: PASSED")
    print("==========================================\n")


if __name__ == "__main__":

    run_crash_torture(
        iterations=500,
        operations_per_iteration=10,
        seed=42
    )
