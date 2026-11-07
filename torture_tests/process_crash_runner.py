
"""Serve-based random-time process crash torture with an independent oracle."""

import base64
import json
import os
import random
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from torture_tests.process_harness import ServeProcess, ServeProcessError

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _decode(value):
    return base64.b64decode(value.encode("ascii"))


def apply_operation(state, operation):
    kind = operation["type"]

    if kind == "put":
        state[_decode(operation["key"])] = _decode(operation["value"])

    elif kind == "delete":
        state.pop(_decode(operation["key"]), None)

    elif kind == "batch":
        updated = state.copy()
        for item in operation["operations"]:
            apply_operation(updated, item)
        state.clear()
        state.update(updated)

    else:
        raise AssertionError(
            f"Unknown oracle operation type: {kind}"
        )


def read_jsonl(path):
    entries = []

    if not os.path.exists(path):
        return entries

    with open(path, "r", encoding="utf-8") as stream:
        for line_no, line in enumerate(stream, 1):
            if line.strip():
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    raise AssertionError(
                        f"Malformed oracle log {path}, line {line_no}: {exc}"
                    ) from exc

    return entries


def _state_for_entries(entries):
    state = {}
    for entry in entries:
        apply_operation(state, entry["operation"])
    return state


def verify_oracle(ack_entries, intent_entries, actual_state):
    ack_ids = [entry["request_id"] for entry in ack_entries]
    ack_set = set(ack_ids)

    intents = {
        entry["request_id"]: entry
        for entry in intent_entries
    }

    missing = ack_set - set(intents)
    if missing:
        raise AssertionError(
            f"ACKs without intents: {sorted(missing)}"
        )

    acknowledged_state = _state_for_entries(ack_entries)

    pending = [
        entry
        for entry in intent_entries
        if entry["request_id"] not in ack_set
    ]

    if len(pending) > 16:
        raise AssertionError(
            "Too many simultaneous in-flight requests for "
            f"exhaustive oracle verification: {len(pending)}"
        )

    for mask in range(1 << len(pending)):
        candidate = acknowledged_state.copy()
        included_ids = []

        for index, entry in enumerate(pending):
            if mask & (1 << index):
                apply_operation(candidate, entry["operation"])
                included_ids.append(entry["request_id"])

        if candidate == actual_state:
            return acknowledged_state, ack_set, included_ids

    acked_keys = set(acknowledged_state)
    actual_keys = set(actual_state)

    missing_keys = sorted(acked_keys - actual_keys)
    extra_keys = sorted(actual_keys - acked_keys)

    mismatched = sorted(
        key
        for key in (acked_keys & actual_keys)
        if acknowledged_state[key] != actual_state[key]
    )

    pending_ids = [
        entry["request_id"]
        for entry in pending
    ]

    raise AssertionError(
        "Recovered dump does not match any legal oracle state. "
        f"ACK operation IDs={ack_ids}; "
        f"pending operation IDs={pending_ids}; "
        f"missing ACKed keys={missing_keys[:10]!r}; "
        f"unexpected keys={extra_keys[:10]!r}; "
        f"incorrect values={mismatched[:10]!r}; "
        f"expected ACK state keys={len(acknowledged_state)}, "
        f"found keys={len(actual_state)}"
    )


def _run_cli(*arguments, timeout=30):
    command = [
        sys.executable,
        str(PROJECT_ROOT / "main.py"),
        *map(str, arguments),
    ]

    result = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )

    if result.returncode != 0:
        raise AssertionError(
            f"CLI command failed ({' '.join(command)}), "
            f"exit={result.returncode}:\n{result.stdout}"
        )

    return result.stdout


def _cli_dump_state(database_path):
    output = _run_cli("dump", database_path)
    state = {}

    for line in output.splitlines():
        if line.startswith("Total records:"):
            continue

        if " = " not in line:
            if line.strip():
                raise AssertionError(
                    f"Unrecognized dump output line: {line!r}"
                )
            continue

        key_text, value_text = line.split(" = ", 1)
        key = key_text.encode("utf-8")
        value = value_text.encode("utf-8")

        if key in state:
            raise AssertionError(
                f"Duplicate key in dump: {key!r}"
            )

        state[key] = value

    return state


def _make_batch(worker_id, iteration, operation_number, rng):
    operations = []
    oracle_operations = []

    count = rng.randint(2, 8)

    for item_index in range(count):
        slot = rng.randrange(32)
        key = f"w{worker_id}_slot_{slot}".encode("ascii")

        if rng.random() < 0.72:
            value = (
                f"it{iteration}_w{worker_id}_"
                f"op{operation_number}_item{item_index}_"
                f"{rng.randrange(1_000_000)}"
            ).encode("ascii")

            operations.append(("put", key, value))
            oracle_operations.append(("put", key, value))
        else:
            operations.append(("delete", key))
            oracle_operations.append(("delete", key))

    return operations, oracle_operations


def run_one_iteration(
    iteration,
    rng,
    worker_count,
    delay_min,
    delay_max,
):
    with tempfile.TemporaryDirectory(
        prefix="kvdb_torture_"
    ) as folder:

        database_path = os.path.join(folder, "crash.db")
        oracle_path = os.path.join(folder, "oracle.jsonl")
        intent_path = os.path.join(folder, "intents.jsonl")

        stop_event = threading.Event()
        start_barrier = threading.Barrier(worker_count + 1)

        first_request_events = [
            threading.Event()
            for _ in range(worker_count)
        ]

        server = ServeProcess(
            database_path,
            oracle_log_path=oracle_path,
            intent_log_path=intent_path,
            request_timeout=2.0,
        )

        worker_errors = []
        error_lock = threading.Lock()
        submitted_counts = [0] * worker_count

        def writer(worker_id):
            local_rng = random.Random(rng.getrandbits(64))
            batch_path = os.path.join(
                folder,
                f"worker_{worker_id}.batch",
            )

            try:
                start_barrier.wait(timeout=10)
            except threading.BrokenBarrierError:
                return

            op_number = 0

            while not stop_event.is_set():
                key = (
                    f"w{worker_id}_slot_{op_number % 32}"
                ).encode("ascii")

                try:
                    choice = local_rng.random()

                    if choice < 0.58:
                        value = (
                            f"it{iteration}_w{worker_id}_"
                            f"op{op_number}_"
                            f"{local_rng.randrange(1_000_000)}"
                        ).encode("ascii")

                        server.put(key, value)

                    elif choice < 0.83:
                        server.delete(key)

                    else:
                        operations, oracle_operations = _make_batch(
                            worker_id,
                            iteration,
                            op_number,
                            local_rng,
                        )

                        with open(
                            batch_path,
                            "w",
                            encoding="utf-8",
                            newline="\n",
                        ) as stream:
                            for operation in operations:
                                if operation[0] == "put":
                                    _, batch_key, batch_value = operation
                                    stream.write(
                                        "put "
                                        + batch_key.decode("ascii")
                                        + " "
                                        + batch_value.decode("ascii")
                                        + "\n"
                                    )
                                else:
                                    _, batch_key = operation
                                    stream.write(
                                        "del "
                                        + batch_key.decode("ascii")
                                        + "\n"
                                    )

                        server.batch(
                            batch_path,
                            oracle_operations,
                        )

                    submitted_counts[worker_id] += 1
                    first_request_events[worker_id].set()
                    op_number += 1

                except (
                    ServeProcessError,
                    BrokenPipeError,
                    OSError,
                    ValueError,
                ):
                    break

                except Exception as exc:
                    with error_lock:
                        worker_errors.append(
                            f"worker {worker_id}: {exc}"
                        )
                    break

        executor = ThreadPoolExecutor(max_workers=worker_count)

        try:
            for worker_id in range(worker_count):
                executor.submit(writer, worker_id)

            start_barrier.wait(timeout=10)

            # Require one completed request from every worker.
            # An iteration cannot pass without exercising all writers.
            for worker_id, event in enumerate(first_request_events):
                if not event.wait(timeout=30):
                    raise AssertionError(
                        f"Worker {worker_id} did not complete its "
                        "first request before the startup deadline. "
                        f"Requests started: {submitted_counts}"
                    )

            if worker_errors:
                raise AssertionError("; ".join(worker_errors))

            # Only begin the random kill window after every worker
            # has completed at least one database operation.
            delay_ms = rng.randint(delay_min, delay_max)
            time.sleep(delay_ms / 1000.0)

            server.kill()

        finally:
            stop_event.set()
            server.kill()
            executor.shutdown(wait=True)

        if worker_errors:
            raise AssertionError("; ".join(worker_errors))

        if not all(count >= 1 for count in submitted_counts):
            raise AssertionError(
                "An iteration ended without activity from every worker: "
                f"{submitted_counts}"
            )

        ack_entries = read_jsonl(oracle_path)
        intent_entries = read_jsonl(intent_path)

        if not ack_entries:
            raise AssertionError(
                "No durable ACKs were recorded in this iteration."
            )

        open_output = _run_cli("open", database_path)
        if "OPEN OK" not in open_output:
            raise AssertionError(
                "CLI open did not report success:\n" + open_output
            )

        check_output = _run_cli("check", database_path)
        if "CHECK OK" not in check_output:
            raise AssertionError(
                "CLI check did not report success:\n" + check_output
            )

        actual_state = _cli_dump_state(database_path)

        acknowledged_state, ack_ids, included_pending = verify_oracle(
            ack_entries,
            intent_entries,
            actual_state,
        )

        print(
            f"Iteration {iteration}: PASSED | "
            f"kill_delay={delay_ms}ms | "
            f"workers={worker_count} | "
            f"requests_started={sum(submitted_counts)} | "
            f"ACKs={len(ack_ids)} | "
            f"recovered_keys={len(actual_state)} | "
            f"inflight_committed={len(included_pending)}",
            flush=True,
        )

        return len(ack_ids)


def run_process_crashes(
    iterations=10,
    seed=42,
    workers=4,
    delay_min=10,
    delay_max=150,
):
    if iterations <= 0:
        raise ValueError("Iterations must be positive.")

    if workers < 2:
        raise ValueError(
            "At least two concurrent writers are required."
        )

    if delay_min < 0 or delay_max < delay_min:
        raise ValueError("Invalid random delay range.")

    rng = random.Random(seed)

    print("\n========== PROCESS CRASH TORTURE ==========")
    print("Iterations:", iterations)
    print("Seed:", seed)
    print("Concurrent writers:", workers)
    print(f"Random kill delay: {delay_min}ms to {delay_max}ms")
    print("Operations: random PUT / DELETE / BATCH")

    passed = 0
    total_acks = 0

    for iteration in range(1, iterations + 1):
        try:
            total_acks += run_one_iteration(
                iteration,
                rng,
                workers,
                delay_min,
                delay_max,
            )
            passed += 1

        except Exception as exc:
            print(
                f"Iteration {iteration}: FAILED | "
                f"{type(exc).__name__}: {exc}",
                file=sys.stderr,
                flush=True,
            )
            print(
                f"PROCESS CRASH TORTURE: FAILED | "
                f"passed={passed} | failed=1 | "
                f"ACKs_before_failure={total_acks}",
                file=sys.stderr,
                flush=True,
            )
            raise

    print("-------------------------------------------")
    print("Iterations passed:", passed)
    print("Iterations failed: 0")
    print("Total acknowledged operations:", total_acks)
    print("PROCESS CRASH TORTURE: PASSED")
    print("===========================================\n")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Random-time child-process crash testing."
    )
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--delay-min", type=int, default=10)
    parser.add_argument("--delay-max", type=int, default=150)

    args = parser.parse_args()

    try:
        run_process_crashes(
            args.iterations,
            args.seed,
            args.workers,
            args.delay_min,
            args.delay_max,
        )
    except Exception as exc:
        print(
            f"PROCESS CRASH TORTURE: FAILED - {exc}",
            file=sys.stderr,
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
