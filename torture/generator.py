
import random


def generate_operations(iterations=500, seed=42):
    """
    Generate reproducible random database operations.

    Supported operations:
        put
        delete
        get

    The same seed always generates the same sequence.
    """

    if not isinstance(iterations, int) or isinstance(iterations, bool):
        raise TypeError("iterations must be an integer")

    if iterations < 1:
        raise ValueError("iterations must be at least 1")

    rng = random.Random(seed)

    operations = []

    for _ in range(iterations):

        operation_type = rng.choices(
            ["put", "delete", "get"],
            weights=[60, 20, 20],
            k=1
        )[0]

        key_number = rng.randint(1, 100)

        key = f"key_{key_number}".encode("utf-8")

        if operation_type == "put":

            value_number = rng.randint(1, 100000)

            value = (
                f"value_{value_number}_"
                f"{rng.randint(1, 1000)}"
            ).encode("utf-8")

            operations.append(
                ("put", key, value)
            )

        elif operation_type == "delete":

            operations.append(
                ("delete", key)
            )

        else:

            operations.append(
                ("get", key)
            )

    return operations
