
import argparse
import shlex
import sys

from src.database import Database


def open_database(path, max_bytes=None):
    return Database(path, max_size=max_bytes)


def read_batch_file(file_path):
    operations = []

    with open(file_path, "r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            try:
                parts = shlex.split(line)
            except ValueError as error:
                raise ValueError(
                    f"Invalid syntax at batch line {line_number}: {error}"
                ) from error

            if len(parts) == 3 and parts[0] == "put":
                operations.append((
                    "put",
                    parts[1].encode("utf-8"),
                    parts[2].encode("utf-8")
                ))

            elif len(parts) == 2 and parts[0] == "del":
                operations.append((
                    "delete",
                    parts[1].encode("utf-8")
                ))

            else:
                raise ValueError(
                    f"Invalid batch operation at line {line_number}: {line}"
                )

            if len(operations) > 10000:
                raise ValueError(
                    "Batch cannot exceed 10000 operations."
                )

    if not operations:
        raise ValueError("Batch file contains no operations.")

    return operations


def command_open(args):
    db = open_database(args.path, args.max_bytes)

    try:
        print("OPEN OK")
        print("Database:", args.path)
        print("Records:", len(db.data))
    finally:
        db.close()


def command_put(args):
    db = open_database(args.path)

    try:
        db.put(
            args.key.encode("utf-8"),
            args.value.encode("utf-8")
        )
        print("PUT OK")
    finally:
        db.close()


def command_get(args):
    db = open_database(args.path)

    try:
        value = db.get(args.key.encode("utf-8"))

        if value is None:
            print("NOT FOUND")
        else:
            print(value.decode("utf-8", errors="replace"))
    finally:
        db.close()


def command_delete(args):
    db = open_database(args.path)

    try:
        db.delete(args.key.encode("utf-8"))
        print("DELETE OK")
    finally:
        db.close()


def command_batch(args):
    operations = read_batch_file(args.file)
    db = open_database(args.path)

    try:
        db.batch(operations)
        print("BATCH OK")
        print("Operations:", len(operations))
    finally:
        db.close()


def command_dump(args):
    db = open_database(args.path)

    try:
        for key, value in sorted(db.data.items()):
            print(
                key.decode("utf-8", errors="replace"),
                "=",
                value.decode("utf-8", errors="replace")
            )

        print("Total records:", len(db.data))
    finally:
        db.close()


def command_check(args):
    db = open_database(args.path)

    try:
        print("CHECK OK")
        print("Recovery: successful")
        print("Records:", len(db.data))
    finally:
        db.close()


def command_serve(args):
    # The size limit remains active for this entire server session.
    db = open_database(args.path, args.max_bytes)

    print("READY", flush=True)

    try:
        for line in sys.stdin:
            line = line.strip()

            if not line:
                continue

            parts = []
            op_id = "UNKNOWN"

            try:
                parts = shlex.split(line)

                if not parts:
                    continue

                op_id = parts[0]

                if len(parts) < 2:
                    raise ValueError(
                        "Expected: <op-id> <put|get|del|batch> ..."
                    )

                operation = parts[1]

                if operation == "put" and len(parts) == 4:
                    db.put(
                        parts[2].encode("utf-8"),
                        parts[3].encode("utf-8")
                    )

                    print(f"ACK {op_id}", flush=True)

                elif operation == "get" and len(parts) == 3:
                    value = db.get(parts[2].encode("utf-8"))

                    if value is None:
                        print(f"NOT_FOUND {op_id}", flush=True)
                    else:
                        print(
                            f"VALUE {op_id} "
                            f"{value.decode('utf-8', errors='replace')}",
                            flush=True
                        )

                elif operation == "del" and len(parts) == 3:
                    db.delete(parts[2].encode("utf-8"))

                    print(f"ACK {op_id}", flush=True)

                elif operation == "batch" and len(parts) == 3:
                    operations = read_batch_file(parts[2])
                    db.batch(operations)

                    print(f"ACK {op_id}", flush=True)

                else:
                    raise ValueError(
                        "Invalid operation or arguments."
                    )

            except Exception as error:
                message = (
                    str(error)
                    .replace("\r", " ")
                    .replace("\n", " ")
                )

                print(
                    f"ERROR {op_id} {message}",
                    flush=True
                )

    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Kill It and Prove It - Embedded Key-Value Database"
        )
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True
    )

    # OPEN
    p = subparsers.add_parser("open")
    p.add_argument("path")
    p.add_argument("--max-bytes", type=int)
    p.set_defaults(func=command_open)

    # PUT
    p = subparsers.add_parser("put")
    p.add_argument("path")
    p.add_argument("key")
    p.add_argument("value")
    p.set_defaults(func=command_put)

    # GET
    p = subparsers.add_parser("get")
    p.add_argument("path")
    p.add_argument("key")
    p.set_defaults(func=command_get)

    # DELETE
    p = subparsers.add_parser("del")
    p.add_argument("path")
    p.add_argument("key")
    p.set_defaults(func=command_delete)

    # BATCH
    p = subparsers.add_parser("batch")
    p.add_argument("path")
    p.add_argument("file")
    p.set_defaults(func=command_batch)

    # DUMP
    p = subparsers.add_parser("dump")
    p.add_argument("path")
    p.set_defaults(func=command_dump)

    # CHECK
    p = subparsers.add_parser("check")
    p.add_argument("path")
    p.set_defaults(func=command_check)

    # SERVE
    p = subparsers.add_parser("serve")
    p.add_argument("path")
    p.add_argument("--max-bytes", type=int)
    p.set_defaults(func=command_serve)

    args = parser.parse_args()

    try:
        args.func(args)
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
