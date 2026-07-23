"""
Consumer cursor for durable tick journal replay.

Pattern: Value Object — immutable last-processed sequence watermark.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TickJournalCursor:
    last_processed_seq: int = 0

    def to_dict(self) -> dict:
        return {"last_processed_seq": self.last_processed_seq}

    @staticmethod
    def from_dict(data: dict) -> "TickJournalCursor":
        if not isinstance(data, dict):
            raise TypeError("cursor data must be a dictionary")
        seq = data.get("last_processed_seq", 0)
        if not isinstance(seq, int) or seq < 0:
            raise ValueError("last_processed_seq must be a non-negative integer")
        return TickJournalCursor(last_processed_seq=seq)
