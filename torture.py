
import argparse
import sys

from torture_tests.runner import run_torture
from torture_tests.process_crash_runner import run_process_crashes
from torture_tests.crash_runner import run_crash_torture
from torture_tests.process_compaction_crash_runner import (
    run_compaction_crash_torture,
)
from torture_tests.process_recovery_crash_runner import (
    run_recovery_crash_torture,
)
from torture_tests.process_large_crash_runner import run_large_values
from torture_tests.process_batch_crash_runner import run_batch_crashes
from torture_tests.process_disk_full_runner import run_disk_full_torture


def main():
    parser = argparse.ArgumentParser(
        description="Kill It and Prove It - Torture Test Launcher"
    )

    parser.add_argument(
        "--mode",
        choices=[
            "random",
            "crash",
            "compaction",
            "recovery-crash",
            "large-values",
            "large-batch",
            "disk-full",
        ],
        default="random",
        help="Torture test mode (default: random)",
    )

    parser.add_argument(
        "--iterations",
        type=int,
        default=500,
        help="Number of test iterations",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible testing",
    )

    parser.add_argument(
        "--operations-per-iteration",
        type=int,
        default=10,
        help="Writes per crash iteration",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Concurrent workers (large-batch requires exactly 2)",
    )

    args = parser.parse_args()

    if args.iterations <= 0:
        parser.error("--iterations must be greater than zero")

    if args.operations_per_iteration <= 0:
        parser.error(
            "--operations-per-iteration must be greater than zero"
        )

    if args.workers < 2:
        parser.error("--workers must be at least 2")

    if args.mode == "large-batch" and args.workers != 2:
        parser.error("large-batch mode requires exactly 2 workers")

    try:
        if args.mode == "random":
            run_process_crashes(
                iterations=args.iterations,
                seed=args.seed,
                workers=args.workers,
            )

        elif args.mode == "crash":
            run_crash_torture(
                iterations=args.iterations,
                operations_per_iteration=args.operations_per_iteration,
                seed=args.seed,
            )

        elif args.mode == "compaction":
            run_compaction_crash_torture(
                iterations=args.iterations,
                seed=args.seed,
            )

        elif args.mode == "recovery-crash":
            run_recovery_crash_torture(
                iterations=args.iterations,
                seed=args.seed,
            )

        elif args.mode == "large-values":
            run_large_values(
                iterations=args.iterations,
                seed=args.seed,
                workers=args.workers,
            )

        elif args.mode == "large-batch":
            run_batch_crashes(
                iterations=args.iterations,
                seed=args.seed,
                workers=args.workers,
            )

        elif args.mode == "disk-full":
            run_disk_full_torture(
                iterations=args.iterations,
                seed=args.seed,
            )

    except Exception as error:
        print(
            f"TORTURE TEST FAILED: {error}",
            file=sys.stderr,
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
