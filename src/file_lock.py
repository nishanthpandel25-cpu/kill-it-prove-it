
import os
import hashlib


class FileLock:

    def __init__(self, path):
        self.path = os.path.abspath(path)
        self.handle = None
        self.lock_file = None

    def __enter__(self):

        if os.name == "nt":
            self._acquire_windows_mutex()
        else:
            self._acquire_posix_lock()

        return self

    def _acquire_windows_mutex(self):

        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL(
            "kernel32",
            use_last_error=True
        )

        kernel32.CreateMutexW.argtypes = [
            wintypes.LPVOID,
            wintypes.BOOL,
            wintypes.LPCWSTR
        ]
        kernel32.CreateMutexW.restype = wintypes.HANDLE

        kernel32.WaitForSingleObject.argtypes = [
            wintypes.HANDLE,
            wintypes.DWORD
        ]
        kernel32.WaitForSingleObject.restype = wintypes.DWORD

        kernel32.ReleaseMutex.argtypes = [
            wintypes.HANDLE
        ]
        kernel32.ReleaseMutex.restype = wintypes.BOOL

        kernel32.CloseHandle.argtypes = [
            wintypes.HANDLE
        ]
        kernel32.CloseHandle.restype = wintypes.BOOL

        # Generate the same mutex name for every process
        # accessing the same database path.
        normalized = os.path.normcase(self.path)

        digest = hashlib.sha256(
            normalized.encode("utf-8")
        ).hexdigest()

        mutex_name = "Local\\KillItProveIt_" + digest

        self.kernel32 = kernel32

        self.handle = kernel32.CreateMutexW(
            None,
            False,
            mutex_name
        )

        if not self.handle:
            raise ctypes.WinError(ctypes.get_last_error())

        WAIT_OBJECT_0 = 0
        WAIT_ABANDONED = 0x80
        INFINITE = 0xFFFFFFFF

        result = kernel32.WaitForSingleObject(
            self.handle,
            INFINITE
        )

        if result not in (WAIT_OBJECT_0, WAIT_ABANDONED):
            error = ctypes.get_last_error()
            kernel32.CloseHandle(self.handle)
            self.handle = None
            raise ctypes.WinError(error)

        # A WAIT_ABANDONED result also means this process
        # now owns the mutex. It must inspect/recover the DB.

    def _acquire_posix_lock(self):

        import fcntl

        self.lock_file = open(
            self.path + ".lock",
            "a+b"
        )

        fcntl.flock(
            self.lock_file.fileno(),
            fcntl.LOCK_EX
        )

    def __exit__(self, exc_type, exc_value, traceback):

        if os.name == "nt":

            if self.handle:
                self.kernel32.ReleaseMutex(self.handle)
                self.kernel32.CloseHandle(self.handle)
                self.handle = None

        else:

            if self.lock_file:
                import fcntl

                fcntl.flock(
                    self.lock_file.fileno(),
                    fcntl.LOCK_UN
                )

                self.lock_file.close()
                self.lock_file = None
