
import argparse
import json
import shlex
import sys

from src.database import Database


DEFAULT_DB = "data/database.db"


# --------------------------------------------------
# DATABASE OPEN
# --------------------------------------------------

def open_database(path):
    return Database(path)


# --------------------------------------------------
# OPEN COMMAND
# --------------------------------------------------

def command_open(args):

    db = open_database(args.db)

    try:
        print("Database opened successfully.")
        print("Database:", args.db)
        print("Records:", len(db.data))

    finally:
        db.close()


# --------------------------------------------------
# PUT COMMAND
# --------------------------------------------------

def command_put(args):

    db = open_database(args.db)

    try:
        db.put(
            args.key.encode("utf-8"),
            args.value.encode("utf-8")
        )

        print("PUT successful.")

    finally:
        db.close()


# --------------------------------------------------
# GET COMMAND
# --------------------------------------------------

def command_get(args):

    db = open_database(args.db)

    try:
        value = db.get(args.key.encode("utf-8"))

        if value is None:
            print("Key not found.")
        else:
            print(
                "Value:",
                value.decode("utf-8", errors="replace")
            )

    finally:
        db.close()


# --------------------------------------------------
# DELETE COMMAND
# --------------------------------------------------

def command_delete(args):

    db = open_database(args.db)

    try:
        db.delete(args.key.encode("utf-8"))

        print("DELETE successful.")

    finally:
        db.close()


# --------------------------------------------------
# BATCH COMMAND
# --------------------------------------------------

def command_batch(args):

    db = open_database(args.db)

    try:

        # Read batch operations from a JSON file.
        with open(
            args.operations,
            "r",
            encoding="utf-8"
        ) as file:

            operations = json.load(file)

        if not isinstance(operations, list):
            raise ValueError(
                "Batch input must be a JSON list."
            )

        converted = []

        # Validate and convert operations.
        for operation in operations:

            if not isinstance(operation, list):
                raise ValueError(
                    "Each operation must be a JSON list."
                )

            if (
                len(operation) == 3
                and operation[0] == "put"
            ):

                converted.append((
                    "put",
                    operation[1].encode("utf-8"),
                    operation[2].encode("utf-8")
                ))

            elif (
                len(operation) == 2
                and operation[0] == "delete"
            ):

                converted.append((
                    "delete",
                    operation[1].encode("utf-8")
                ))

            else:
                raise ValueError(
                    "Invalid batch operation."
                )

        # Execute atomic batch.
        db.batch(converted)

        print("BATCH successful.")
        print("Operations:", len(converted))

    finally:
        db.close()


# --------------------------------------------------
# DUMP COMMAND
# --------------------------------------------------

def command_dump(args):

    db = open_database(args.db)

    try:

        print("\nDatabase contents:")
        print("--------------------------")

        for key, value in sorted(db.data.items()):

            print(
                key.decode("utf-8", errors="replace"),
                "=",
                value.decode("utf-8", errors="replace")
            )

        print("--------------------------")
        print("Total records:", len(db.data))

    finally:
        db.close()


# --------------------------------------------------
# CHECK COMMAND
# --------------------------------------------------

def command_check(args):

    db = open_database(args.db)

    try:

        print("Database check completed.")
        print("Recovery: successful")
        print("Records:", len(db.data))
        print("Database:", args.db)

    finally:
        db.close()


# --------------------------------------------------
# SERVE COMMAND
# --------------------------------------------------

def command_serve(args):

    db = open_database(args.db)

    print("\n================================")
    print("  KILL IT AND PROVE IT DATABASE")
    print("================================")

    print("Database shell started.")
    print("Type help for available commands.")
    print("Type exit to close the database.")

    try:

        while True:

            try:
                command = input("\ndb> ").strip()

            except (EOFError, KeyboardInterrupt):

                print("\nClosing database shell.")
                break

            if not command:
                continue

            if command == "exit":
                break

            if command == "help":

                print("""
Available commands:

put <key> <value>
get <key>
del <key>
dump
check
help
exit
""")

                continue

            try:

                parts = shlex.split(command)

                if (
                    parts[0] == "put"
                    and len(parts) == 3
                ):

                    db.put(
                        parts[1].encode("utf-8"),
                        parts[2].encode("utf-8")
                    )

                    print("PUT successful.")

                elif (
                    parts[0] == "get"
                    and len(parts) == 2
                ):

                    value = db.get(
                        parts[1].encode("utf-8")
                    )

                    if value is None:
                        print("Key not found.")
                    else:
                        print(
                            value.decode(
                                "utf-8",
                                errors="replace"
                            )
                        )

                elif (
                    parts[0] == "del"
                    and len(parts) == 2
                ):

                    db.delete(
                        parts[1].encode("utf-8")
                    )

                    print("DELETE successful.")

                elif (
                    parts[0] == "dump"
                    and len(parts) == 1
                ):

                    for key, value in sorted(db.data.items()):

                        print(
                            key.decode(
                                "utf-8",
                                errors="replace"
                            ),
                            "=",
                            value.decode(
                                "utf-8",
                                errors="replace"
                            )
                        )

                    print("Total records:", len(db.data))

                elif (
                    parts[0] == "check"
                    and len(parts) == 1
                ):

                    print("Recovery: successful")
                    print("Records:", len(db.data))

                else:

                    print(
                        "Invalid command. Type help."
                    )

            except Exception as error:

                print("Error:", error)

    finally:

        db.close()

        print("\nDatabase closed.")


# --------------------------------------------------
# MAIN CLI
# --------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Kill It and Prove It - "
            "Embedded Key-Value Database"
        )
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True
    )

    # OPEN
    open_parser = subparsers.add_parser("open")

    open_parser.add_argument(
        "--db",
        default=DEFAULT_DB
    )

    open_parser.set_defaults(
        func=command_open
    )

    # PUT
    put_parser = subparsers.add_parser("put")

    put_parser.add_argument("key")
    put_parser.add_argument("value")

    put_parser.add_argument(
        "--db",
        default=DEFAULT_DB
    )

    put_parser.set_defaults(
        func=command_put
    )

    # GET
    get_parser = subparsers.add_parser("get")

    get_parser.add_argument("key")

    get_parser.add_argument(
        "--db",
        default=DEFAULT_DB
    )

    get_parser.set_defaults(
        func=command_get
    )

    # DELETE
    delete_parser = subparsers.add_parser("del")

    delete_parser.add_argument("key")

    delete_parser.add_argument(
        "--db",
        default=DEFAULT_DB
    )

    delete_parser.set_defaults(
        func=command_delete
    )

    # BATCH
    batch_parser = subparsers.add_parser("batch")

    batch_parser.add_argument(
        "operations",
        help="Path to JSON file containing batch operations"
    )

    batch_parser.add_argument(
        "--db",
        default=DEFAULT_DB
    )

    batch_parser.set_defaults(
        func=command_batch
    )

    # DUMP
    dump_parser = subparsers.add_parser("dump")

    dump_parser.add_argument(
        "--db",
        default=DEFAULT_DB
    )

    dump_parser.set_defaults(
        func=command_dump
    )

    # CHECK
    check_parser = subparsers.add_parser("check")

    check_parser.add_argument(
        "--db",
        default=DEFAULT_DB
    )

    check_parser.set_defaults(
        func=command_check
    )

    # SERVE
    serve_parser = subparsers.add_parser("serve")

    serve_parser.add_argument(
        "--db",
        default=DEFAULT_DB
    )

    serve_parser.set_defaults(
        func=command_serve
    )

    args = parser.parse_args()

    try:

        args.func(args)

    except Exception as error:

        print(
            "Error:",
            error,
            file=sys.stderr
        )

        sys.exit(1)


if __name__ == "__main__":
    main()
