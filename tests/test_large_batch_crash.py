
import multiprocessing
import os
import tempfile
import unittest

import src.database as database_module
from src.database import Database


BATCH_SIZE = 10_000
EXISTING_RECORDS = 100


def batch_worker(path, stage, connection):
    """Execute a batch and pause at a controlled checkpoint."""

    db = None

    try:
        db = Database(path)

        operations = [
            (
                "put",
                f"batch_key_{index}".encode(),
                f"batch_value_{index}".encode()
            )
            for index in range(BATCH_SIZE)
        ]

        def checkpoint(current_stage):
            if current_stage == stage:
                connection.send(("checkpoint", current_stage))

                if connection.recv() != "continue":
                    raise RuntimeError("Unexpected checkpoint response")

        database_module.BATCH_TEST_HOOK = checkpoint

        db.batch(operations)

        connection.send(("success",))

    except Exception as error:
        try:
            connection.send(("error", repr(error)))
        except (BrokenPipeError, EOFError, OSError):
            pass

    finally:
        database_module.BATCH_TEST_HOOK = None

        if db is not None:
            db.close()

        connection.close()


def verify_database(path, expect_batch):
    """Verify the recovered database using its in-memory index."""

    db = Database(path)

    try:
        expected = {}

        for index in range(EXISTING_RECORDS):
            key = f"existing_{index}".encode()
            value = f"existing_value_{index}".encode()
            expected[key] = value

        if expect_batch:
            for index in range(BATCH_SIZE):
                key = f"batch_key_{index}".encode()
                value = f"batch_value_{index}".encode()
                expected[key] = value

        actual = db.data.copy()

        if actual != expected:
            raise AssertionError(
                f"Atomicity failure: expected {len(expected)} records, "
                f"recovered {len(actual)}"
            )

        return len(actual)

    finally:
        db.close()


class TestLargeBatchCrash(unittest.TestCase):

    def run_crash_scenario(self, stage, expect_batch):

        with tempfile.TemporaryDirectory() as folder:

            path = os.path.join(folder, "database.db")

            db = Database(path)

            for index in range(EXISTING_RECORDS):
                db.put(
                    f"existing_{index}".encode(),
                    f"existing_value_{index}".encode()
                )

            db.close()

            parent_connection, child_connection = (
                multiprocessing.Pipe(duplex=True)
            )

            process = multiprocessing.Process(
                target=batch_worker,
                args=(path, stage, child_connection)
            )

            process.start()
            child_connection.close()

            try:
                # Wait for the worker to reach the checkpoint.
                if not parent_connection.poll(90):
                    raise TimeoutError(
                        f"Worker did not reach checkpoint: {stage}"
                    )

                message = parent_connection.recv()

                self.assertEqual(
                    message,
                    ("checkpoint", stage)
                )

                if expect_batch:
                    # Continue execution after the durable checkpoint.
                    parent_connection.send("continue")

                    self.assertTrue(
                        parent_connection.poll(120),
                        "Worker did not finish the batch"
                    )

                    result = parent_connection.recv()

                    self.assertEqual(result, ("success",))

                    process.join(timeout=15)

                    self.assertFalse(process.is_alive())
                    self.assertEqual(process.exitcode, 0)

                else:
                    # Kill the process while it is paused.
                    process.terminate()
                    process.join(timeout=15)

                    self.assertFalse(process.is_alive())

                recovered = verify_database(
                    path,
                    expect_batch
                )

                print(
                    f"\nStage: {stage}"
                    f"\nExpected batch: {expect_batch}"
                    f"\nRecovered records: {recovered}"
                )

            finally:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=15)

                parent_connection.close()

                if process.exitcode is not None:
                    process.close()

    def test_crash_before_batch_write(self):
        self.run_crash_scenario(
            "before_write",
            False
        )

    def test_crash_after_batch_sync(self):
        self.run_crash_scenario(
            "after_sync",
            True
        )

    def test_batch_completed_before_process_exit(self):
        self.run_crash_scenario(
            "after_commit",
            True
        )


if __name__ == "__main__":
    multiprocessing.freeze_support()
    unittest.main()
