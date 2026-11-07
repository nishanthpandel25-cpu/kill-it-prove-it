
import os
import tempfile

from torture_tests.process_harness import ServeProcess


def main():
    with tempfile.TemporaryDirectory() as folder:
        database_path = os.path.join(folder, "test.db")
        oracle_path = os.path.join(folder, "oracle.jsonl")

        print("Starting database child process...")

        with ServeProcess(
            database_path,
            oracle_log_path=oracle_path,
        ) as server:

            print("Server started successfully.")

            server.put(b"test_key", b"test_value")
            print("PUT acknowledgement received.")

            actual = server.get(b"test_key")

            assert actual == b"test_value", (
                f"Expected b'test_value', received {actual!r}"
            )

            print("GET verification: PASSED")

        print("Server stopped.")

        assert os.path.exists(oracle_path), (
            "Oracle log was not created."
        )

        with open(oracle_path, "r", encoding="utf-8") as file:
            entries = file.readlines()

        assert len(entries) == 1, (
            f"Expected one acknowledged write, found {len(entries)}"
        )

        print("Independent oracle log: PASSED")
        print("Acknowledged writes:", len(entries))
        print("\nPROCESS HARNESS TEST: PASSED")


if __name__ == "__main__":
    main()
