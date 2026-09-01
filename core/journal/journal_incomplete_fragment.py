"""
Incomplete trailing journal fragment policy (D4-09).

Pattern: Strategy — wait-or-skip decision for EOF lines without newline.
"""

from __future__ import annotations

import time
from typing import Callable, Optional


def is_in_progress_journal_fragment(fragment: str) -> bool:
    """
    Return True when an incomplete trailing fragment may still become a valid
    JSONL record (writer mid-append). Torn suffixes that do not start with ``{``
    are never valid journal objects and must not block the reader (D4-09).
    """
    return fragment.lstrip().startswith("{")


def incomplete_trailing_skip_reason(fragment: str) -> str:
    """Return the quarantine reason for a skipped incomplete trailing fragment."""
    if is_in_progress_journal_fragment(fragment):
        return "incomplete_trailing_stale"
    return "incomplete_trailing_poison"


class IncompleteTrailingFragmentPolicy:
    """
    Decide whether an EOF fragment without newline must be skipped now.

    ``{``-prefixed fragments may still be in-flight; others are quarantined
    immediately. In-progress fragments are skipped after
    ``incomplete_record_max_wait_seconds``.
    """

    def __init__(
        self,
        *,
        incomplete_record_max_wait_seconds: float,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        if incomplete_record_max_wait_seconds <= 0:
            raise ValueError("incomplete_record_max_wait_seconds must be positive")
        if clock is not None and not callable(clock):
            raise TypeError("clock must be callable")
        self.__incomplete_record_max_wait_seconds = float(
            incomplete_record_max_wait_seconds
        )
        self.__clock: Callable[[], float] = (
            clock if clock is not None else time.monotonic
        )
        self.__pending_incomplete_offset: Optional[int] = None
        self.__pending_incomplete_started_at: Optional[float] = None
        self.__pending_incomplete_length: Optional[int] = None

    @property
    def is_stuck(self) -> bool:
        """True while a ``{``-prefixed fragment is within the wait window."""
        return self.__pending_incomplete_offset is not None

    def clear(self) -> None:
        self.__pending_incomplete_offset = None
        self.__pending_incomplete_started_at = None
        self.__pending_incomplete_length = None

    def should_skip_now(self, *, offset: int, fragment: str) -> bool:
        """
        Return True when the fragment must be quarantined on this poll.

        Postconditions:
            Returns True for torn non-JSON suffixes, or for ``{``-prefixed
            fragments that have remained incomplete past the configured wait.
        """
        if not fragment:
            return False
        if not is_in_progress_journal_fragment(fragment):
            return True
        now = self.__clock()
        fragment_len = len(fragment)
        if (
            self.__pending_incomplete_offset != offset
            or self.__pending_incomplete_length != fragment_len
            or self.__pending_incomplete_started_at is None
        ):
            self.__pending_incomplete_offset = offset
            self.__pending_incomplete_started_at = now
            self.__pending_incomplete_length = fragment_len
            return False
        elapsed = now - self.__pending_incomplete_started_at
        return elapsed >= self.__incomplete_record_max_wait_seconds
