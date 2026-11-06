
import os

from src.database import Database
from src.storage import PUT, encode_record


def test_database_recovers_after_final_byte_is_truncated(tmp_path):
    """
    F4:
    Remove exactly the final byte of an incomplete,
    unacknowledged record.

    All previously acknowledged records must survive.
    """

    path = str(tmp_path / "f4_truncation.db")

    # Step 1: Write and acknowledge valid records.
    db = Database(path)

    acknowledged_records = {
        b"key_1": b"value_1",
        b"key_2": b"value_2",
        b"key_3": b"value_3",
    }

    try:
        for key, value in acknowledged_records.items():
            db.put(key, value)
    finally:
        db.close()

    # Remember the exact end of acknowledged data.
    acknowledged_size = os.path.getsize(path)

    # Step 2: Simulate a final unacknowledged record.
    incomplete_record = encode_record(
        PUT,
        b"unacknowledged_key",
        b"unacknowledged_value"
    )

    with open(path, "ab") as file:
        file.write(incomplete_record)

    # Step 3: Remove exactly one byte from the file's end.
    original_size = os.path.getsize(path)

    with open(path, "r+b") as file:
        file.truncate(original_size - 1)

    assert os.path.getsize(path) == original_size - 1

    # Step 4: Reopen the database and trigger recovery.
    recovered_db = Database(path)

    try:
        # Every acknowledged record must survive.
        for key, expected_value in acknowledged_records.items():
            assert recovered_db.get(key) == expected_value

        # The incomplete unacknowledged record must not appear.
        assert recovered_db.get(b"unacknowledged_key") is None

        # Recovery must remove the incomplete record entirely.
        assert os.path.getsize(path) == acknowledged_size

        assert len(recovered_db.data) == len(acknowledged_records)

    finally:
        recovered_db.close()
