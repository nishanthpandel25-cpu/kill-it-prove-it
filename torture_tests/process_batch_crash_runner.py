
import argparse
import base64
import json
import os
import random
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from src.database import Database
from torture_tests.process_harness import (
    ServeProcess,
    ServeProcessError,
)


BATCH_SIZE = 10_000


def read_log(path):
    entries = []

    if not os.path.exists(path):
        return entries

    with open(path, "r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise AssertionError(
                    f"Invalid JSON at {path}:{line_number}: {error}"
                )

    return entries


def apply_operation(state, operation):
    operation_type = operation["type"]

    if operation_type == "put":
        key = base64.b64decode(operation["key"])
        value = base64.b64decode(operation["value"])
        state[key] = value

    elif operation_type == "delete":
        key = base64.b64decode(operation["key"])
        state.pop(key, None)

    elif operation_type == "batch":
        # Apply the complete batch to a temporary state.
        updated = state.copy()

        for item in operation["operations"]:
            apply_operation(updated, item)

        state.clear()
        state.update(updated)

    else:
        raise ValueError(
            f"Unknown operation type: {operation_type}"
        )


def build_batch_file(folder, worker_id):
    batch_path = os.path.join(
        folder,
        f"batch_worker_{worker_id}.txt",
    )

    oracle_operations = []

    with open(batch_path, "w", encoding="utf-8") as file:
        for index in range(BATCH_SIZE):
            key = (
                f"batch_worker_{worker_id}_key_{index}"
            ).encode("ascii")

            value = (
                f"batch_worker_{worker_id}_value_{index}"
            ).encode("ascii")

            file.write(
                f"put {key.decode()} {value.decode()}\n"
            )

            oracle_operations.append(
                ("put", key, value)
            )

    return batch_path, oracle_operations


def build_candidate_states(ack_entries, intent_entries):
    ack_ids = {
        entry["request_id"] for entry in ack_entries
    }

    intent_ids = {
        entry["request_id"] for entry in intent_entries
    }

    if ack_ids - intent_ids:
        raise AssertionError(
            "An acknowledged batch has no matching intent."
        )

    pending_entries = [
        entry
        for entry in intent_entries
        if entry["request_id"] not in ack_ids
    ]

    # This test intentionally uses only two concurrent batches.
    if len(pending_entries) > 2:
        raise AssertionError(
            f"Unexpected pending batch count: {len(pending_entries)}"
        )

    acknowledged_state = {}

    # ACK order is the server response order.
    for entry in ack_entries:
        operation = entry["operation"]

        if operation["type"] != "batch":
            raise AssertionError(
                "Unexpected non-batch operation in batch test."
            )

        if len(operation["operations"]) != BATCH_SIZE:
            raise AssertionError(
                "Acknowledged batch has an incorrect operation count."
            )

        apply_operation(
            acknowledged_state,
            operation,
        )

    candidates = []

    # Each pending batch is considered as one indivisible operation.
    # It is either entirely present or entirely absent.
    for mask in range(1 << len(pending_entries)):
        candidate = acknowledged_state.copy()

        for index, entry in enumerate(pending_entries):
            if mask & (1 << index):
                operation = entry["operation"]

                if operation["type"] != "batch":
                    raise AssertionError(
                        "Unexpected pending non-batch operation."
                    )

                if len(operation["operations"]) != BATCH_SIZE:
                    raise AssertionError(
                        "Pending batch has an incorrect operation count."
                    )

                apply_operation(candidate, operation)

        candidates.append(candidate)

    return acknowledged_state, candidates, ack_ids, pending_entries


def verify_recovery(ack_entries, intent_entries, actual):
    acknowledged, candidates, ack_ids, pending = (
        build_candidate_states(
            ack_entries,
            intent_entries,
        )
    )

    # Exact dictionary comparison catches partial batches,
    # missing acknowledged values, and unexpected values.
    if not any(actual == candidate for candidate in candidates):
        raise AssertionError(
            "Recovered database does not match any valid "
            "all-or-nothing batch outcome."
        )

    for key, value in acknowledged.items():
        if actual.get(key) != value:
            raise AssertionError(
                f"Acknowledged batch value missing: {key!r}"
            )

    return ack_ids, pending


def wait_for_batch_activity(
    oracle_path,
    intent_path,
    expected_batches,
    timeout=180,
):
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        intents = read_log(intent_path)
        acknowledgements = read_log(oracle_path)

        intent_ids = {
            entry["request_id"] for entry in intents
        }

        ack_ids = {
            entry["request_id"] for entry in acknowledgements
        }

        pending_ids = intent_ids - ack_ids

        if (
            len(intent_ids) == expected_batches
            and len(ack_ids) >= 1
            and len(pending_ids) >= 1
        ):
            return len(ack_ids), len(pending_ids)

        time.sleep(0.02)

    raise AssertionError(
        "Did not observe at least one acknowledged batch and "
        "one pending batch within the activity timeout."
    )


def run_one_iteration(iteration, rng, workers):
    with tempfile.TemporaryDirectory() as folder:
        db_path = os.path.join(folder, "batch_crash.db")
        oracle_path = os.path.join(folder, "oracle.jsonl")
        intent_path = os.path.join(folder, "intents.jsonl")

        batch_files = []
        batch_operations = []

        print(
            f"\nPreparing {workers} batches of "
            f"{BATCH_SIZE:,} operations...",
            flush=True,
        )

        for worker_id in range(workers):
            batch_path, operations = build_batch_file(
                folder,
                worker_id,
            )

            batch_files.append(batch_path)
            batch_operations.append(operations)

        server = ServeProcess(
            db_path,
            oracle_log_path=oracle_path,
            intent_log_path=intent_path,
            request_timeout=180.0,
        )

        barrier = threading.Barrier(workers + 1)
        worker_errors = []
        worker_errors_lock = threading.Lock()

        def writer(worker_id):
            barrier.wait()

            try:
                server.batch(
                    batch_files[worker_id],
                    batch_operations[worker_id],
                )

            except ServeProcessError:
                # Expected if the process is killed while a
                # batch request is in progress.
                pass

            except (BrokenPipeError, OSError, ValueError):
                pass

            except Exception as error:
                with worker_errors_lock:
                    worker_errors.append(
                        f"Worker {worker_id}: {error}"
                    )

        executor = ThreadPoolExecutor(
            max_workers=workers
        )

        try:
            for worker_id in range(workers):
                executor.submit(writer, worker_id)

            barrier.wait(timeout=15)

            initial_acks, initial_pending = (
                wait_for_batch_activity(
                    oracle_path,
                    intent_path,
                    workers,
                )
            )

            # Randomized delay after confirmed batch activity.
            delay_ms = rng.randint(10, 150)
            time.sleep(delay_ms / 1000.0)

            # Abrupt process termination.
            server.kill()

        finally:
            server.kill()
            executor.shutdown(wait=True)

        ack_entries = read_log(oracle_path)
        intent_entries = read_log(intent_path)

        with Database(db_path) as recovered:
            actual = recovered.data.copy()

            ack_ids, pending_entries = verify_recovery(
                ack_entries,
                intent_entries,
                actual,
            )

        if worker_errors:
            raise AssertionError(
                "; ".join(worker_errors)
            )

        if not ack_ids:
            raise AssertionError(
                "Invalid batch crash test: zero acknowledged batches."
            )

        if not pending_entries:
            raise AssertionError(
                "Invalid batch crash test: no pending batch at crash."
            )

        print(
            f"Iteration {iteration}: PASSED | "
            f"kill_delay={delay_ms}ms | "
            f"ACKed_batches={len(ack_ids)} | "
            f"pending_at_initial_check={initial_pending} | "
            f"recovered_records={len(actual)}",
            flush=True,
        )

        return len(ack_ids)


def run_batch_crashes(iterations, seed, workers):
    if iterations <= 0:
        raise ValueError("Iterations must be positive.")

    if workers != 2:
        raise ValueError(
            "This initial batch-crash test requires exactly 2 workers."
        )

    rng = random.Random(seed)
    total_acks = 0

    print("\n========== LARGE BATCH PROCESS CRASH ==========")
    print("Iterations:", iterations)
    print("Seed:", seed)
    print("Concurrent batch writers:", workers)
    print("Operations per batch:", f"{BATCH_SIZE:,}")

    for iteration in range(1, iterations + 1):
        total_acks += run_one_iteration(
            iteration,
            rng,
            workers,
        )

    print("-----------------------------------------------")
    print("Iterations passed:", iterations)
    print("Total acknowledged batches:", total_acks)
    print("LARGE BATCH PROCESS CRASH: PASSED")
    print("===============================================\n")


def main():
    parser = argparse.ArgumentParser(
        description="Crash testing for 10,000-operation batches."
    )

    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=2,
    )

    args = parser.parse_args()

    try:
        run_batch_crashes(
            iterations=args.iterations,
            seed=args.seed,
            workers=args.workers,
        )

    except Exception as error:
        print(
            f"LARGE BATCH PROCESS CRASH: FAILED - {error}"
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
