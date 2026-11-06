
class Database:

    def __init__(self):
        # In-memory database
        self.data = {}

    # Validate keys
    def _validate_key(self, key):

        if not isinstance(key, bytes):
            raise TypeError("Key must be bytes")

        if not 1 <= len(key) <= 1024:
            raise ValueError("Key must contain 1 to 1024 bytes")

    # Validate values
    def _validate_value(self, value):

        if not isinstance(value, bytes):
            raise TypeError("Value must be bytes")

        if len(value) > 1024 * 1024:
            raise ValueError("Value cannot exceed 1 MB")

    # PUT operation
    def put(self, key, value):

        self._validate_key(key)
        self._validate_value(value)

        self.data[key] = value

        return True

    # GET operation
    def get(self, key):

        self._validate_key(key)

        return self.data.get(key)

    # DELETE operation
    def delete(self, key):

        self._validate_key(key)

        self.data.pop(key, None)

        return True
