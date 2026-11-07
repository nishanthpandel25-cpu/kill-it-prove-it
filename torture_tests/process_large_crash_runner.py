
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


MAX_VALUE_SIZE = 1024 * 1024


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
        updated = state.copy()

        for item in operation["operations"]:
            apply_operation(updated, item)

        state.clear()
        state.update(updated)

    else:
        raise ValueError(
            f"Unknown operation type: {operation_type}"
        )


def verify_recovery(ack_entries, intent_entries, actual):
    ack_ids = {
        entry["request_id"] for entry in ack_entries
    }

    intent_ids = {
        entry["request_id"] for entry in intent_entries
    }

    if ack_ids - intent_ids:
        raise AssertionError(
            "An ACK has no matching intent."
        )

    pending_entries = [
        entry
        for entry in intent_entries
        if entry["request_id"] not in ack_ids
    ]

    if len(pending_entries) > 12:
        raise AssertionError(
            "Too many pending operations to enumerate safely: "
            f"{len(pending_entries)}"
        )

    acknowledged_state = {}

    for entry in ack_entries:
        apply_operation(
            acknowledged_state,
            entry["operation"],
        )

    # Enumerate every possible combination of complete pending PUTs.
    candidate_states = []

    combinations = 1 << len(pending_entries)

    for mask in range(combinations):
        candidate = acknowledged_state.copy()

        for index, entry in enumerate(pending_entries):
            if mask & (1 << index):
                apply_operation(
                    candidate,
                    entry["operation"],
                )

        candidate_states.append(candidate)

    if not any(actual == candidate for candidate in candidate_states):
        raise AssertionError(
            "Recovered state does not match any valid combination "
            "of acknowledged and in-flight operations."
        )

    for key, value in acknowledged_state.items():
        if actual.get(key) != value:
            raise AssertionError(
                f"Acknowledged value missing or incorrect: {key!r}"
            )

    return ack_ids, pending_entries


def make_large_value(worker_id, operation_number):
    prefix = (
        f"worker_{worker_id}_operation_{operation_number}_"
    ).encode("ascii")

    return (
        prefix * ((MAX_VALUE_SIZE // len(prefix)) + 1)
    )[:MAX_VALUE_SIZE]


def prepare_worker_data(workers, operations_per_worker=20):
    prepared = []

    for worker_id in range(workers):
        worker_operations = []

        for operation_number in range(operations_per_worker):
            key = (
                f"large_worker_{worker_id}_slot_"
                f"{operation_number % 4}"
            ).encode("ascii")

            value = make_large_value(
                worker_id,
                operation_number,
            )

            worker_operations.append((key, value))

        prepared.append(worker_operations)

    return prepared


def wait_for_initial_activity(
    intent_path,
    oracle_path,
    workers,
    timeout=30,
):
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        intents = read_log(intent_path)
        acknowledgements = read_log(oracle_path)

        active_workers = set()

        for entry in intents:
            operation = entry["operation"]

            if operation["type"] != "put":
                continue

            key = base64.b64decode(operation["key"])

            for worker_id in range(workers):
                prefix = f"large_worker_{worker_id}_".encode()

                if key.startswith(prefix):
                    active_workers.add(worker_id)
                    break

        ack_ids = {
            entry["request_id"]
            for entry in acknowledgements
        }

        intent_ids = {
            entry["request_id"]
            for entry in intents
        }

        pending_count = len(intent_ids - ack_ids)

        # Require all workers to have submitted an operation,
        # at least one durable ACK, and at least one in-flight request.
        if (
            len(active_workers) == workers
            and len(ack_ids) >= 1
            and pending_count >= 1
        ):
            return len(ack_ids), pending_count

        time.sleep(0.01)

    raise AssertionError(
        "Writers did not produce the required activity before timeout."
    )


def run_large_values(iterations, seed, workers):
    rng = random.Random(seed)

    print("\n========== LARGE VALUE PROCESS CRASH ==========")
    print("Iterations:", iterations)
    print("Seed:", seed)
    print("Workers:", workers)
    print("Value size:", f"{MAX_VALUE_SIZE:,} bytes")

    # Generate values before starting the child process.
    print("Preparing large values...")
    prepared_data = prepare_worker_data(workers)

    total_acks = 0

    for iteration in range(1, iterations + 1):
        with tempfile.TemporaryDirectory() as folder:
            db_path = os.path.join(folder, "large_values.db")
            oracle_path = os.path.join(folder, "oracle.jsonl")
            intent_path = os.path.join(folder, "intents.jsonl")

            server = ServeProcess(
                db_path,
                oracle_log_path=oracle_path,
                intent_log_path=intent_path,
                request_timeout=30.0,
            )

            stop_event = threading.Event()
            barrier = threading.Barrier(workers + 1)
            counts = [0] * workers
            errors = []
            errors_lock = threading.Lock()

            def writer(worker_id):
                barrier.wait()

                for operation_number, (key, value) in enumerate(
                    prepared_data[worker_id]
                ):
                    if stop_event.is_set():
                        break

                    try:
                        server.put(key, value)
                        counts[worker_id] += 1

                    except ServeProcessError:
                        break

                    except (BrokenPipeError, OSError, ValueError):
                        break

                    except Exception as error:
                        with errors_lock:
                            errors.append(
                                f"Worker {worker_id}: {error}"
                            )
                        break

            executor = ThreadPoolExecutor(
                max_workers=workers
            )

            try:
                for worker_id in range(workers):
                    executor.submit(writer, worker_id)

                barrier.wait(timeout=15)

                initial_acks, initial_pending = (
                    wait_for_initial_activity(
                        intent_path,
                        oracle_path,
                        workers,
                    )
                )

                # Randomize the kill after confirmed real activity.
                delay_ms = rng.randint(10, 150)
                time.sleep(delay_ms / 1000.0)

                server.kill()

            finally:
                stop_event.set()
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

                for key, value in actual.items():
                    if recovered.get(key) != value:
                        raise AssertionError(
                            f"GET verification failed: {key!r}"
                        )

            if errors:
                raise AssertionError(
                    "; ".join(errors)
                )

            if not ack_ids:
                raise AssertionError(
                    "Invalid test: zero acknowledged operations."
                )

            if not pending_entries:
                raise AssertionError(
                    "Invalid test: no in-flight operation at crash."
                )

            total_acks += len(ack_ids)

            print(
                f"Iteration {iteration}: PASSED | "
                f"kill_delay={delay_ms}ms | "
                f"completed={sum(counts)} | "
                f"ACKs={len(ack_ids)} | "
                f"pending_at_crash={initial_pending} "
                f"(initial observation) | "
                f"recovered_keys={len(actual)}",
                flush=True,
            )

    print("-----------------------------------------------")
    print("Iterations passed:", iterations)
    print("Total ACKs:", total_acks)
    print("LARGE VALUE PROCESS CRASH: PASSED")
    print("===============================================\n")


def main():
    parser = argparse.ArgumentParser(
        description="Crash testing for large 1 MB values."
    )

    parser.add_argument(
        "--mode",
        choices=["large-values"],
        required=True,
    )

    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=4,
    )

    args = parser.parse_args()

    if args.iterations <= 0:
        parser.error("--iterations must be positive.")

    if args.workers < 2:
        parser.error("--workers must be at least 2.")

    try:
        run_large_values(
            args.iterations,
            args.seed,
            args.workers,
        )

    except Exception as error:
        print(
            f"LARGE VALUE PROCESS CRASH: FAILED - {error}"
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
