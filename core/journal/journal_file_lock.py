"""
Cross-process exclusive lock for tick journal writers.

Pattern: Resource Guard — advisory file lock with context-manager lifecycle.
"""

from __future__ import annotations

import os
import sys
import time
from types import TracebackType
from typing import BinaryIO, Optional


def _exclusive_lock_file_descriptor(fileno: int) -> None:
    if sys.platform == "win32":
        import msvcrt

        msvcrt.locking(fileno, msvcrt.LK_LOCK, 1)
        return
    import fcntl

    fcntl.flock(fileno, fcntl.LOCK_EX)


def _release_file_descriptor_lock(fileno: int) -> None:
    if sys.platform == "win32":
        import msvcrt

        msvcrt.locking(fileno, msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(fileno, fcntl.LOCK_UN)


class JournalFileLock:
    """Advisory file lock shared by collector processes writing the same journal."""

    def __init__(self, lock_path: str, *, timeout_seconds: float = 30.0) -> None:
        if not isinstance(lock_path, str) or not lock_path.strip():
            raise ValueError("lock_path must be a non-empty string")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.__lock_path = lock_path
        self.__timeout_seconds = float(timeout_seconds)
        self.__handle: Optional[BinaryIO] = None

    @property
    def lock_path(self) -> str:
        return self.__lock_path

    def is_acquired(self) -> bool:
        return self.__handle is not None

    def acquire(self) -> None:
        if self.__handle is not None:
            raise RuntimeError(
                f"Journal lock at {self.__lock_path} is already acquired"
            )
        directory = os.path.dirname(self.__lock_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        deadline = time.time() + self.__timeout_seconds
        while True:
            handle: Optional[BinaryIO] = None
            try:
                handle = open(self.__lock_path, "a+b")
                handle.seek(0)
                _exclusive_lock_file_descriptor(handle.fileno())
                self.__handle = handle
                return
            except OSError:
                if handle is not None:
                    try:
                        handle.close()
                    except OSError:
                        pass
                if time.time() >= deadline:
                    raise TimeoutError(
                        f"Timed out acquiring journal lock at {self.__lock_path}"
                    )
                time.sleep(0.05)

    def release(self) -> None:
        if self.__handle is None:
            return
        handle = self.__handle
        self.__handle = None
        try:
            handle.seek(0)
            _release_file_descriptor_lock(handle.fileno())
        finally:
            try:
                handle.close()
            except OSError:
                pass

    def __enter__(self) -> "JournalFileLock":
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()
