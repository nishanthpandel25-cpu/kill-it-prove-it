
import os
import tempfile

from src.database import Database


class TestBatchPutDeleteOrder:

    def test_put_then_delete_same_key_in_batch(self):

        with tempfile.TemporaryDirectory() as folder:

            path = os.path.join(folder, "put_delete.db")

            db = Database(path)

            try:
                db.put(b"existing_key", b"existing_value")

                db.batch([
                    ("put", b"X", b"first_value"),
                    ("delete", b"X")
                ])

                assert db.get(b"X") is None, (
                    "X should be deleted after the batch"
                )

                assert db.get(b"existing_key") == b"existing_value"

                assert len(db.data) == 1

            finally:
                db.close()

            # Verify the same final state after restarting.
            db = Database(path)

            try:
                assert db.get(b"X") is None
                assert db.get(b"existing_key") == b"existing_value"
                assert len(db.data) == 1

            finally:
                db.close()


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
