
import argparse
import sys

from torture_tests.runner import run_torture
from torture_tests.crash_runner import run_crash_torture


def main():
    parser = argparse.ArgumentParser(
        description="Kill It and Prove It - Torture Test Launcher"
    )

    parser.add_argument(
        "--mode",
        choices=["random", "crash"],
        default="random",
        help="Torture test mode (default: random)"
    )

    parser.add_argument(
        "--iterations",
        type=int,
        default=500,
        help="Number of test iterations or operations"
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible testing"
    )

    parser.add_argument(
        "--operations-per-iteration",
        type=int,
        default=10,
        help="Writes per crash iteration"
    )

    args = parser.parse_args()

    if args.iterations <= 0:
        parser.error("--iterations must be greater than zero")

    if args.operations_per_iteration <= 0:
        parser.error("--operations-per-iteration must be greater than zero")

    try:
        if args.mode == "random":
            run_torture(
                iterations=args.iterations,
                seed=args.seed
            )

        elif args.mode == "crash":
            run_crash_torture(
                iterations=args.iterations,
                operations_per_iteration=args.operations_per_iteration,
                seed=args.seed
            )

    except Exception as error:
        print(
            f"TORTURE TEST FAILED: {error}",
            file=sys.stderr
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
