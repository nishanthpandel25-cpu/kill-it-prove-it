
import multiprocessing
import tempfile
import os
import time

from src.file_lock import FileLock


def worker(path, name):
    start = time.time()

    with FileLock(path):
        acquired = time.time()

        print(
            name,
            "acquired after",
            round(acquired - start, 2),
            "seconds"
        )

        time.sleep(3)

    print(name, "released the lock")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as folder:
        path = os.path.join(folder, "database.db")

        with open(path, "wb"):
            pass

        p1 = multiprocessing.Process(
            target=worker,
            args=(path, "Process A")
        )

        p2 = multiprocessing.Process(
            target=worker,
            args=(path, "Process B")
        )

        p1.start()
        time.sleep(0.3)
        p2.start()

        p1.join()
        p2.join()

        print("Both processes completed")
