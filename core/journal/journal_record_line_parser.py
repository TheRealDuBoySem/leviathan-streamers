"""
Parse complete JSONL journal record lines into (seq, TradeTick).

Pattern: Codec — pure mapping for one durable journal line (read path).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional, Union

from core.journal.tick_journal_codec import tick_from_dict
from leviathan_common.models.trade_tick import TradeTick


@dataclass(frozen=True)
class JournalRecordParseError:
    reason: str
    line: str


JournalRecordParseResult = Union[tuple[int, TradeTick], JournalRecordParseError]


def parse_journal_record_line(line: str) -> Optional[JournalRecordParseResult]:
    """
    Parse one complete JSONL journal line.

    Returns:
        ``None`` for blank lines;
        ``(seq, tick)`` on success;
        ``JournalRecordParseError`` when the line is complete but invalid.
    """
    stripped = line.strip()
    if not stripped:
        return None
    try:
        record = json.loads(stripped)
    except json.JSONDecodeError as exc:
        return JournalRecordParseError(reason=str(exc), line=stripped)
    if not isinstance(record, dict):
        return JournalRecordParseError(
            reason="record is not a JSON object",
            line=stripped,
        )
    try:
        seq = int(record["seq"])
        tick = tick_from_dict(record["tick"])
    except (KeyError, TypeError, ValueError) as exc:
        return JournalRecordParseError(reason=str(exc), line=stripped)
    return seq, tick
