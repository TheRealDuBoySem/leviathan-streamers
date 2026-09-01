"""
Persisted consumer cursor for durable tick-journal replay.

Pattern: Repository — owns cursor JSON load/save independently of the writer tip.
"""

from __future__ import annotations

import json
import os

from core.journal.journal_io import atomic_write_json
from core.journal.tick_journal_cursor import TickJournalCursor


class TickJournalCursorStore:
    """
    Load and save ``TickJournalCursor`` watermarks.

    Invariants:
        - Missing cursor files load as last_processed_seq=0.
        - save() only accepts a TickJournalCursor instance.
    """

    def __init__(self, cursor_path: str) -> None:
        if not isinstance(cursor_path, str) or not cursor_path.strip():
            raise ValueError("cursor_path must be a non-empty string")
        self.__cursor_path = cursor_path.strip()

    @property
    def cursor_path(self) -> str:
        return self.__cursor_path

    def load(self) -> TickJournalCursor:
        if not os.path.exists(self.__cursor_path):
            return TickJournalCursor()
        try:
            with open(self.__cursor_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"tick journal cursor is not valid JSON: {self.__cursor_path}"
            ) from exc
        return TickJournalCursor.from_dict(payload)

    def save(self, cursor: TickJournalCursor) -> None:
        if not isinstance(cursor, TickJournalCursor):
            raise TypeError("cursor must be a TickJournalCursor instance")
        atomic_write_json(self.__cursor_path, cursor.to_dict())
