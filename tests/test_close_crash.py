
import multiprocessing
import os
import tempfile

from src.database import Database


CRASH_EXIT_CODE = 69


def crash_during_close(path):
    """Terminate the process during Database.close()."""

    db = Database(path)

    original_refresh = db._refresh_index_if_changed

    def controlled_refresh():
        # Perform the normal index refresh first.
        original_refresh()

        # Simulate process termination before close()
        # can set self.closed = True.
        os._exit(CRASH_EXIT_CODE)

    db._refresh_index_if_changed = controlled_refresh

    db.close()


class TestCloseCrash:

    def test_process_killed_during_close(self):

        with tempfile.TemporaryDirectory() as folder:

            path = os.path.join(folder, "close_crash.db")

            # Prepare a database with committed records.
            db = Database(path)

            for index in range(20):
                db.put(
                    f"key_{index}".encode(),
                    f"value_{index}".encode()
                )

            db.close()

            # Start a child process that terminates during close().
            process = multiprocessing.Process(
                target=crash_during_close,
                args=(path,)
            )

            process.start()
            process.join(timeout=15)

            assert not process.is_alive(), (
                "Close process did not terminate"
            )

            assert process.exitcode == CRASH_EXIT_CODE, (
                f"Expected exit code {CRASH_EXIT_CODE}, "
                f"got {process.exitcode}"
            )

            # Reopen the database after the forced termination.
            db = Database(path)

            try:

                for index in range(20):

                    key = f"key_{index}".encode()
                    expected = f"value_{index}".encode()

                    assert db.get(key) == expected, (
                        f"Committed record {index} was lost"
                    )

                assert len(db.data) == 20

            finally:
                db.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
