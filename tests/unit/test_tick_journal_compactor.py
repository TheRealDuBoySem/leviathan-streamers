"""Unit tests for TickJournalCompactor."""

import logging
import os

import pytest

from core.journal.tick_journal import TickJournal
from core.journal.tick_journal_compactor import COMPACT_MIN_LAG_SEQ, TickJournalCompactor
from core.journal.tick_journal_cursor import TickJournalCursor
from core.journal.tick_journal_meta import TickJournalMetaStore
from core.journal.tick_journal_seq_index import TickJournalSeqIndex
from leviathan_common.models.trade_tick import TradeTick


def _tick(trade_id: str, ts: int = 1000) -> TradeTick:
    return TradeTick("BTCUSDT", ts, 100.0, 1.0, "buy", trade_id)


def test_compactor_rejects_invalid_dependencies(tmp_path):
    import threading

    store = TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=10)
    index = TickJournalSeqIndex(
        journal_path=str(tmp_path / "j.jsonl"),
        meta_store=store,
    )
    with pytest.raises(TypeError, match="thread_lock must be a threading.Lock-like object"):
        TickJournalCompactor(
            journal_path=str(tmp_path / "j.jsonl"),
            lock_path=str(tmp_path / "l.lock"),
            quarantine_path=str(tmp_path / "q.jsonl"),
            meta_store=store,
            seq_index=index,
            thread_lock=object(),  # type: ignore[arg-type]
        )
    # Valid construction still works.
    TickJournalCompactor(
        journal_path=str(tmp_path / "j.jsonl"),
        lock_path=str(tmp_path / "l.lock"),
        quarantine_path=str(tmp_path / "q.jsonl"),
        meta_store=store,
        seq_index=index,
        thread_lock=threading.Lock(),
    )


def test_compactor_rejects_blank_paths_and_wrong_types(tmp_path):
    import threading

    store = TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=10)
    index = TickJournalSeqIndex(
        journal_path=str(tmp_path / "j.jsonl"),
        meta_store=store,
    )
    lock = threading.Lock()
    kwargs = {
        "journal_path": str(tmp_path / "j.jsonl"),
        "lock_path": str(tmp_path / "l.lock"),
        "quarantine_path": str(tmp_path / "q.jsonl"),
        "meta_store": store,
        "seq_index": index,
        "thread_lock": lock,
    }
    with pytest.raises(ValueError, match="journal_path must be a non-empty string"):
        TickJournalCompactor(**{**kwargs, "journal_path": "  "})
    with pytest.raises(ValueError, match="lock_path must be a non-empty string"):
        TickJournalCompactor(**{**kwargs, "lock_path": "  "})
    with pytest.raises(ValueError, match="quarantine_path must be a non-empty string"):
        TickJournalCompactor(**{**kwargs, "quarantine_path": "  "})
    with pytest.raises(TypeError, match="meta_store must be a TickJournalMetaStore"):
        TickJournalCompactor(**{**kwargs, "meta_store": object()})  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="seq_index must be a TickJournalSeqIndex"):
        TickJournalCompactor(**{**kwargs, "seq_index": object()})  # type: ignore[arg-type]


def test_compactor_maybe_compact_rejects_invalid_last_processed_seq(tmp_path):
    import threading

    store = TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=10)
    index = TickJournalSeqIndex(
        journal_path=str(tmp_path / "j.jsonl"),
        meta_store=store,
    )
    compactor = TickJournalCompactor(
        journal_path=str(tmp_path / "j.jsonl"),
        lock_path=str(tmp_path / "l.lock"),
        quarantine_path=str(tmp_path / "q.jsonl"),
        meta_store=store,
        seq_index=index,
        thread_lock=threading.Lock(),
    )
    with pytest.raises(ValueError, match="last_processed_seq must be a non-negative integer"):
        compactor.maybe_compact(last_processed_seq=-1, lag_seq=1)
    with pytest.raises(ValueError, match="last_processed_seq must be a non-negative integer"):
        compactor.maybe_compact(last_processed_seq="8", lag_seq=1)  # type: ignore[arg-type]


def test_compact_before_seq(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("old"))
    journal.append(_tick("new", ts=1100))
    removed = journal.compact_before_seq(2)
    assert removed == 1
    replay = list(journal.tail_from(1))
    assert len(replay) == 1
    assert replay[0][1].trade_id == "new"


def test_compact_returns_zero_when_nothing_removed(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("only"))
    assert journal.compact_before_seq(1) == 0


def test_compact_returns_zero_when_journal_missing(tmp_path):
    journal = TickJournal(str(tmp_path))
    assert journal.compact_before_seq(2) == 0


def test_compact_skips_blank_lines_and_keeps_all_records(tmp_path):
    journal = TickJournal(str(tmp_path))
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write("\n\n")
    assert journal.compact_before_seq(2) == 0


def test_compact_quarantines_invalid_lines(tmp_path, caplog):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("old"))
    journal.append(_tick("keep"))
    with open(journal.journal_path, "a", encoding="utf-8") as handle:
        handle.write("{broken\n")
    with caplog.at_level(logging.WARNING):
        removed = journal.compact_before_seq(2)
    assert removed >= 1
    assert os.path.exists(journal.quarantine_path)
    assert any("compact skipped invalid" in record.message for record in caplog.records)


def test_maybe_compact_runs(tmp_path):
    journal = TickJournal(str(tmp_path))
    for index in range(5):
        journal.append(_tick(f"t{index}", ts=1000 + index))
    journal.save_cursor(TickJournalCursor(last_processed_seq=5))
    assert journal.maybe_compact(lag_seq=1) >= 0


def test_maybe_compact_skips_when_cursor_lag_is_small(tmp_path):
    journal = TickJournal(str(tmp_path))
    for index in range(5):
        journal.append(_tick(f"t{index}", ts=1000 + index))
    journal.save_cursor(TickJournalCursor(last_processed_seq=2))
    assert journal.maybe_compact(lag_seq=5_000) == 0


def test_maybe_compact_rejects_non_positive_lag(tmp_path):
    journal = TickJournal(str(tmp_path))
    with pytest.raises(ValueError, match="lag_seq must be positive"):
        journal.maybe_compact(lag_seq=0)


def test_compact_drops_processed_prefix(tmp_path):
    journal = TickJournal(str(tmp_path))
    for index in range(10):
        journal.append(_tick(f"t{index}", ts=1000 + index))
    journal.save_cursor(TickJournalCursor(last_processed_seq=8))
    removed = journal.compact_before_seq(6)
    assert removed == 5
    replay = list(journal.tail_from(6))
    assert replay[0][0] == 6
    assert replay[-1][0] == 10


def test_compactor_constant_exported():
    assert COMPACT_MIN_LAG_SEQ == 5_000
