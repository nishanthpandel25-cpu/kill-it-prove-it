
import os
import tempfile

from src.database import Database

from torture.generator import generate_operations
from torture.oracle import Oracle


def run_torture(iterations=500, seed=42):

    print("\n========== TORTURE TEST ==========")

    print("Iterations:", iterations)
    print("Random seed:", seed)

    with tempfile.TemporaryDirectory() as folder:

        path = os.path.join(folder, "torture.db")

        db = Database(path)

        oracle = Oracle()

        operations = generate_operations(
            iterations=iterations,
            seed=seed
        )

        successful_operations = 0
        get_checks = 0

        try:

            for index, operation in enumerate(operations, start=1):

                operation_type = operation[0]

                # ----------------------------------
                # PUT
                # ----------------------------------

                if operation_type == "put":

                    _, key, value = operation

                    db.put(key, value)

                    oracle.apply(operation)

                # ----------------------------------
                # DELETE
                # ----------------------------------

                elif operation_type == "delete":

                    _, key = operation

                    db.delete(key)

                    oracle.apply(operation)

                # ----------------------------------
                # GET
                # ----------------------------------

                elif operation_type == "get":

                    expected = oracle.apply(operation)

                    actual = db.get(operation[1])

                    assert actual == expected, (
                        f"GET mismatch at operation {index}: "
                        f"key={operation[1]!r}, "
                        f"expected={expected!r}, "
                        f"actual={actual!r}"
                    )

                    get_checks += 1

                else:

                    raise ValueError(
                        f"Unknown operation: {operation_type}"
                    )

                successful_operations += 1

            print("Operations executed:", successful_operations)

            print("GET checks:", get_checks)

            # --------------------------------------
            # VERIFY BEFORE RESTART
            # --------------------------------------

            assert oracle.verify(db.data), (
                "Database state differs from oracle "
                "before restart"
            )

            print("Pre-restart verification: PASSED")

            # --------------------------------------
            # RESTART DATABASE
            # --------------------------------------

            expected_data = oracle.snapshot()

            db.close()

            db = Database(path)

            # --------------------------------------
            # VERIFY AFTER RESTART
            # --------------------------------------

            assert oracle.verify(db.data), (
                "Recovered database differs from oracle"
            )

            print("Recovered keys:", len(db.data))

            print("Post-restart verification: PASSED")

            # Verify every expected key through GET.
            for key, expected_value in expected_data.items():

                actual_value = db.get(key)

                assert actual_value == expected_value, (
                    f"Recovered value mismatch for {key!r}"
                )

            print("Individual key verification: PASSED")

            print("\nTORTURE TEST: PASSED")

        finally:

            db.close()

    print("=================================\n")


if __name__ == "__main__":

    run_torture(500, 42)
