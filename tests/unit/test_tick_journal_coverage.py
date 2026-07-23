"""Additional TickJournal facade coverage."""

import pytest

from core.journal.tick_journal import TickJournal
from leviathan_common.models.trade_tick import TradeTick


def _tick(trade_id: str, ts: int = 1000) -> TradeTick:
    return TradeTick("BTCUSDT", ts, 100.0, 1.0, "buy", trade_id)


def test_tick_journal_rejects_invalid_seq_index_interval(tmp_path):
    with pytest.raises(ValueError, match="seq_index_interval must be positive"):
        TickJournal(str(tmp_path), seq_index_interval=0)


def test_tick_journal_append_rejects_non_tick(tmp_path):
    journal = TickJournal(str(tmp_path))
    with pytest.raises(TypeError, match="tick must be a TradeTick"):
        journal.append({"bad": True})  # type: ignore[arg-type]


def test_tick_journal_save_cursor_rejects_invalid_type(tmp_path):
    journal = TickJournal(str(tmp_path))
    with pytest.raises(TypeError, match="TickJournalCursor"):
        journal.save_cursor({"bad": 1})  # type: ignore[arg-type]


def test_tick_journal_dedup_bucket_eviction(tmp_path):
    journal = TickJournal(str(tmp_path), dedup_window=2)
    journal.append(_tick("a"))
    journal.append(_tick("b"))
    journal.append(_tick("c"))
    assert journal.latest_seq() == 3
    replay = {trade_id for _, tick in journal.tail_from(1) for trade_id in [tick.trade_id]}
    assert replay == {"a", "b", "c"}


def test_tick_journal_rejects_blank_checkpoint_dir():
    with pytest.raises(ValueError, match="checkpoint_dir must be a non-empty string"):
        TickJournal("   ")


def test_tick_journal_quarantine_line_delegates(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.quarantine_line("{}", reason="facade")
    with open(journal.quarantine_path, "r", encoding="utf-8") as handle:
        assert "facade" in handle.read()
