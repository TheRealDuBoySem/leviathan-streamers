"""Unit tests for TickJournalCursorStore."""

import pytest

from core.journal.tick_journal import TickJournal
from core.journal.tick_journal_cursor import TickJournalCursor
from core.journal.tick_journal_cursor_store import TickJournalCursorStore


def test_cursor_store_rejects_blank_path():
    with pytest.raises(ValueError, match="cursor_path must be a non-empty string"):
        TickJournalCursorStore("   ")


def test_cursor_store_load_defaults_when_file_missing(tmp_path):
    store = TickJournalCursorStore(str(tmp_path / "tick_journal.cursor.json"))
    loaded = store.load()
    assert loaded.last_processed_seq == 0
    assert store.cursor_path == str(tmp_path / "tick_journal.cursor.json")


def test_cursor_store_round_trip(tmp_path):
    store = TickJournalCursorStore(str(tmp_path / "tick_journal.cursor.json"))
    store.save(TickJournalCursor(last_processed_seq=7))
    loaded = store.load()
    assert loaded.last_processed_seq == 7


def test_cursor_store_load_rejects_invalid_json(tmp_path):
    cursor_path = tmp_path / "tick_journal.cursor.json"
    cursor_path.write_text("{not-json", encoding="utf-8")
    store = TickJournalCursorStore(str(cursor_path))
    with pytest.raises(ValueError, match="not valid JSON"):
        store.load()


def test_cursor_store_save_rejects_invalid_type(tmp_path):
    store = TickJournalCursorStore(str(tmp_path / "tick_journal.cursor.json"))
    with pytest.raises(TypeError, match="TickJournalCursor"):
        store.save({"bad": 1})  # type: ignore[arg-type]


def test_tick_journal_facade_delegates_cursor_round_trip(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.save_cursor(TickJournalCursor(last_processed_seq=7))
    loaded = journal.load_cursor()
    assert loaded.last_processed_seq == 7
    assert journal.cursor_path == str(tmp_path / "tick_journal.cursor.json")


def test_tick_journal_facade_save_cursor_rejects_invalid_type(tmp_path):
    journal = TickJournal(str(tmp_path))
    with pytest.raises(TypeError, match="TickJournalCursor"):
        journal.save_cursor({"bad": 1})  # type: ignore[arg-type]


def test_tick_journal_facade_load_cursor_rejects_invalid_json(tmp_path):
    journal = TickJournal(str(tmp_path))
    with open(journal.cursor_path, "w", encoding="utf-8") as handle:
        handle.write("{not-json")
    with pytest.raises(ValueError, match="not valid JSON"):
        journal.load_cursor()
