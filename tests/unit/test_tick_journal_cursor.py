"""Unit tests for TickJournalCursor value object."""

import pytest

from core.journal.tick_journal_cursor import TickJournalCursor


def test_tick_journal_cursor_from_dict_contracts():
    with pytest.raises(TypeError, match="must be a dictionary"):
        TickJournalCursor.from_dict([])  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="non-negative integer"):
        TickJournalCursor.from_dict({"last_processed_seq": -1})


def test_tick_journal_cursor_round_trip_dict():
    cursor = TickJournalCursor(last_processed_seq=7)
    restored = TickJournalCursor.from_dict(cursor.to_dict())
    assert restored.last_processed_seq == 7


def test_tick_journal_cursor_defaults_missing_seq_to_zero():
    assert TickJournalCursor.from_dict({}).last_processed_seq == 0
