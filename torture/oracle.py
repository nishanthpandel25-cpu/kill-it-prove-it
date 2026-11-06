
class Oracle:

    def __init__(self):

        # Independent reference state.
        self.data = {}

    def apply(self, operation):

        operation_type = operation[0]

        # ------------------------------------------
        # PUT
        # ------------------------------------------

        if operation_type == "put":

            _, key, value = operation

            self.data[key] = value

            return True

        # ------------------------------------------
        # DELETE
        # ------------------------------------------

        elif operation_type == "delete":

            _, key = operation

            self.data.pop(key, None)

            return True

        # ------------------------------------------
        # GET
        # ------------------------------------------

        elif operation_type == "get":

            _, key = operation

            return self.data.get(key)

        else:

            raise ValueError(
                f"Unknown operation: {operation_type}"
            )

    def apply_all(self, operations):

        results = []

        for operation in operations:

            results.append(
                self.apply(operation)
            )

        return results

    def get(self, key):

        return self.data.get(key)

    def snapshot(self):

        return self.data.copy()

    def verify(self, actual_data):

        return self.data == actual_data
