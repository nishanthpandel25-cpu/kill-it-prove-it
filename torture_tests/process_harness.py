
import base64
import json
import os
import queue
import shlex
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ServeProcessError(RuntimeError):
    """Raised when the database server cannot complete a request."""


class ServeProcess:
    """Manage a database child process using the serve protocol."""

    def __init__(
        self,
        database_path,
        oracle_log_path=None,
        intent_log_path=None,
        max_bytes=None,
        startup_timeout=10.0,
        request_timeout=30.0,
    ):
        self.database_path = str(Path(database_path).resolve())
        self.oracle_log_path = self._absolute_optional_path(
            oracle_log_path
        )
        self.intent_log_path = self._absolute_optional_path(
            intent_log_path
        )

        self.request_timeout = request_timeout
        self.process = None

        self.pending = {}
        self.pending_lock = threading.Lock()
        self.write_lock = threading.Lock()
        self.log_lock = threading.Lock()

        command = [
            sys.executable,
            "-u",
            str(PROJECT_ROOT / "main.py"),
            "serve",
            self.database_path,
        ]

        if max_bytes is not None:
            command.extend(["--max-bytes", str(max_bytes)])

        self.process = subprocess.Popen(
            command,
            cwd=str(PROJECT_ROOT),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )

        self.startup_response = queue.Queue()
        self.reader_thread = threading.Thread(
            target=self._read_output,
            daemon=True,
        )
        self.reader_thread.start()

        try:
            ready = self._wait_for_startup(startup_timeout)
        except Exception:
            self.kill()
            raise

        if ready != "READY":
            self.kill()
            raise ServeProcessError(
                f"Unexpected startup response: {ready}"
            )

    @staticmethod
    def _absolute_optional_path(path):
        if path is None:
            return None
        return str(Path(path).resolve())

    # --------------------------------------------------
    # LOGGING
    # --------------------------------------------------

    @staticmethod
    def _encode_operation(operation):
        operation_type = operation[0]

        if operation_type == "put":
            _, key, value = operation
            return {
                "type": "put",
                "key": base64.b64encode(key).decode("ascii"),
                "value": base64.b64encode(value).decode("ascii"),
            }

        if operation_type == "delete":
            _, key = operation
            return {
                "type": "delete",
                "key": base64.b64encode(key).decode("ascii"),
            }

        if operation_type == "batch":
            _, operations = operation
            return {
                "type": "batch",
                "operations": [
                    ServeProcess._encode_operation(item)
                    for item in operations
                ],
            }

        raise ValueError(
            f"Unsupported oracle operation: {operation_type}"
        )

    def _append_log(self, path, entry):
        if path is None:
            return

        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        with self.log_lock:
            with open(path, "a", encoding="utf-8") as log:
                log.write(json.dumps(entry) + "\n")
                log.flush()
                os.fsync(log.fileno())

    def _record_intent(self, request_id, operation):
        if self.intent_log_path is None:
            return

        self._append_log(
            self.intent_log_path,
            {
                "event": "intent",
                "request_id": request_id,
                "timestamp_ns": time.time_ns(),
                "operation": self._encode_operation(operation),
            },
        )

    def _record_ack(self, request_id, operation):
        if self.oracle_log_path is None or operation is None:
            return

        self._append_log(
            self.oracle_log_path,
            {
                "event": "ack",
                "request_id": request_id,
                "timestamp_ns": time.time_ns(),
                "operation": self._encode_operation(operation),
            },
        )

    # --------------------------------------------------
    # STARTUP AND OUTPUT
    # --------------------------------------------------

    def _wait_for_startup(self, timeout):
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise ServeProcessError(
                    "Database server exited during startup."
                )

            try:
                return self.startup_response.get(timeout=0.1)
            except queue.Empty:
                continue

        raise ServeProcessError(
            "Database server did not become ready in time."
        )

    def _read_output(self):
        try:
            for line in self.process.stdout:
                line = line.rstrip("\r\n")

                if line == "READY":
                    self.startup_response.put(line)
                    continue

                parts = line.split(maxsplit=2)

                if len(parts) >= 2:
                    response_type = parts[0]
                    request_id = parts[1]

                    if response_type in (
                        "ACK",
                        "VALUE",
                        "NOT_FOUND",
                        "ERROR",
                    ):
                        with self.pending_lock:
                            pending_item = self.pending.get(request_id)

                        if pending_item is not None:
                            response_queue, operation = pending_item

                            # Record ACK durably before waking the
                            # requesting thread.
                            if response_type == "ACK":
                                self._record_ack(
                                    request_id,
                                    operation,
                                )

                            response_queue.put(line)
                            continue

                # Preserve unexpected output for diagnostics.
                self.startup_response.put(line)

        except (OSError, ValueError):
            pass

    # --------------------------------------------------
    # REQUEST HANDLING
    # --------------------------------------------------

    def _request(
        self,
        operation_text,
        expected_responses,
        oracle_operation=None,
    ):
        if not self.is_running():
            raise ServeProcessError(
                "Database server is not running."
            )

        request_id = uuid.uuid4().hex
        response_queue = queue.Queue(maxsize=1)

        # Register before sending, so the reader can identify the ACK.
        with self.pending_lock:
            self.pending[request_id] = (
                response_queue,
                oracle_operation,
            )

        try:
            # Persist intent before the command can reach the server.
            if oracle_operation is not None:
                self._record_intent(
                    request_id,
                    oracle_operation,
                )

            command = f"{request_id} {operation_text}\n"

            with self.write_lock:
                if not self.is_running():
                    raise ServeProcessError(
                        "Database server exited before request."
                    )

                self.process.stdin.write(command)
                self.process.stdin.flush()

            try:
                response = response_queue.get(
                    timeout=self.request_timeout
                )
            except queue.Empty:
                raise ServeProcessError(
                    f"Timed out waiting for request {request_id}."
                )

            parts = response.split(maxsplit=2)

            if not parts or parts[0] == "ERROR":
                raise ServeProcessError(
                    f"Server rejected request: {response}"
                )

            if parts[0] not in expected_responses:
                raise ServeProcessError(
                    f"Unexpected response: {response}"
                )

            return response

        finally:
            with self.pending_lock:
                self.pending.pop(request_id, None)

    # --------------------------------------------------
    # DATABASE OPERATIONS
    # --------------------------------------------------

    def put(self, key, value):
        key_text = key.decode("utf-8")
        value_text = value.decode("utf-8")

        command = (
            f"put {shlex.quote(key_text)} "
            f"{shlex.quote(value_text)}"
        )

        self._request(
            command,
            {"ACK"},
            ("put", key, value),
        )
        return True

    def delete(self, key):
        key_text = key.decode("utf-8")

        self._request(
            f"del {shlex.quote(key_text)}",
            {"ACK"},
            ("delete", key),
        )
        return True

    def get(self, key):
        key_text = key.decode("utf-8")

        response = self._request(
            f"get {shlex.quote(key_text)}",
            {"VALUE", "NOT_FOUND"},
        )

        parts = response.split(maxsplit=2)

        if parts[0] == "NOT_FOUND":
            return None

        if len(parts) < 3:
            raise ServeProcessError(
                f"Malformed VALUE response: {response}"
            )

        return parts[2].encode("utf-8")

    def batch(self, batch_file, oracle_operations):
        batch_path = str(Path(batch_file).resolve())

        self._request(
            f"batch {shlex.quote(batch_path)}",
            {"ACK"},
            ("batch", oracle_operations),
        )
        return True

    # --------------------------------------------------
    # PROCESS CONTROL
    # --------------------------------------------------

    def _join_reader(self):
        if (
            hasattr(self, "reader_thread")
            and self.reader_thread.is_alive()
        ):
            self.reader_thread.join(timeout=5)

    def kill(self):
        """Abruptly terminate the child without graceful shutdown."""

        if self.process is not None:
            if self.process.poll() is None:
                self.process.kill()

            self.process.wait()
            self._join_reader()

    def stop(self):
        """Stop the server normally using stdin EOF."""

        if self.process is None:
            return

        if self.process.poll() is None:
            try:
                self.process.stdin.close()
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()

        self._join_reader()

    def is_running(self):
        return (
            self.process is not None
            and self.process.poll() is None
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        return False
