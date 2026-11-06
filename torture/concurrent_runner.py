
import multiprocessing
import os
import random
import tempfile

from src.database import Database
from torture.oracle import Oracle


def writer_process(path, worker_id, operations, result_queue):
    """Execute one worker's operations in a separate process."""
    db = None

    try:
        db = Database(path)

        for operation in operations:
            operation_type = operation[0]

            if operation_type == "put":
                _, key, value = operation
                db.put(key, value)

            elif operation_type == "delete":
                _, key = operation
                db.delete(key)

            else:
                raise ValueError(
                    f"Unsupported operation: {operation_type}"
                )

        result_queue.put(
            ("success", worker_id, len(operations))
        )

    except Exception as error:
        result_queue.put(
            ("error", worker_id, repr(error))
        )

    finally:
        if db is not None:
            db.close()


def create_worker_operations(worker_id, seed):
    """Generate deterministic operations for one worker."""
    rng = random.Random(seed)
    operations = []

    # Each worker gets a unique range of 100 keys.
    for index in range(100):
        key = f"worker_{worker_id}_key_{index}".encode()

        value = (
            f"value_{worker_id}_{index}_"
            f"{rng.randint(1, 100000)}"
        ).encode()

        operations.append(("put", key, value))

    # Delete 25 keys belonging to this worker.
    delete_indices = rng.sample(range(100), 25)

    for index in delete_indices:
        key = f"worker_{worker_id}_key_{index}".encode()
        operations.append(("delete", key))

    return operations


def build_expected_oracle(worker_operations):
    """Calculate the expected database state independently."""
    oracle = Oracle()

    for operations in worker_operations:
        oracle.apply_all(operations)

    return oracle


def run_concurrent_torture(
    workers=4,
    operations_per_worker=125,
    seed=42
):
    if not isinstance(workers, int) or isinstance(workers, bool):
        raise TypeError("workers must be an integer")

    if workers < 2:
        raise ValueError("At least two writer processes are required")

    if operations_per_worker != 125:
        raise ValueError(
            "Each worker must execute exactly 125 operations"
        )

    print("\n========== CONCURRENT WRITER TORTURE ==========")
    print("Writer processes:", workers)
    print("Operations per worker:", operations_per_worker)
    print("Total operations:", workers * operations_per_worker)
    print("Random seed:", seed)

    worker_operations = [
        create_worker_operations(worker_id, seed + worker_id)
        for worker_id in range(workers)
    ]

    oracle = build_expected_oracle(worker_operations)

    with tempfile.TemporaryDirectory() as folder:
        path = os.path.join(folder, "concurrent_torture.db")
        result_queue = multiprocessing.Queue()
        processes = []

        # Create the database before starting the writers.
        db = Database(path)
        db.close()

        # Start all independent writer processes.
        for worker_id in range(workers):
            process = multiprocessing.Process(
                target=writer_process,
                args=(
                    path,
                    worker_id,
                    worker_operations[worker_id],
                    result_queue
                )
            )

            processes.append(process)
            process.start()

        # Wait for all processes with a timeout.
        for process in processes:
            process.join(timeout=120)

        # Stop processes that did not finish in time.
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=10)

        # Collect one result from every worker.
        results = []

        for _ in range(workers):
            try:
                results.append(result_queue.get(timeout=10))
            except Exception:
                results.append(
                    ("error", "unknown", "Worker result unavailable")
                )

        result_queue.close()
        result_queue.join_thread()

        print("\nWorker results:")

        for result in results:
            print(result)

        errors = [
            result for result in results
            if result[0] != "success"
        ]

        failed_processes = [
            process.exitcode
            for process in processes
            if process.exitcode != 0
        ]

        if errors or failed_processes:
            raise RuntimeError(
                "Concurrent writer test failed. "
                f"Errors: {errors}; "
                f"Process exit codes: {failed_processes}"
            )

        # Reopen the database to test recovery.
        db = Database(path)

        try:
            actual_data = db.data.copy()
            expected_data = oracle.snapshot()

            # Compare the complete database with the oracle.
            if not oracle.verify(actual_data):
                missing = {
                    key: value
                    for key, value in expected_data.items()
                    if actual_data.get(key) != value
                }

                unexpected = {
                    key: value
                    for key, value in actual_data.items()
                    if key not in expected_data
                }

                raise AssertionError(
                    "Oracle mismatch. "
                    f"Missing/incorrect: {list(missing.items())[:10]}; "
                    f"Unexpected: {list(unexpected.items())[:10]}"
                )

            # Verify every expected key individually.
            for key, expected_value in expected_data.items():
                actual_value = db.get(key)

                assert actual_value == expected_value, (
                    f"Incorrect value for key {key!r}"
                )

            print("\nExpected records:", len(expected_data))
            print("Recovered records:", len(actual_data))
            print("Oracle comparison: PASSED")
            print("Individual key verification: PASSED")

        finally:
            db.close()

    print("\nCONCURRENT WRITER TORTURE: PASSED")
    print("================================================\n")


if __name__ == "__main__":
    multiprocessing.freeze_support()

    run_concurrent_torture(
        workers=4,
        operations_per_worker=125,
        seed=42
    )
