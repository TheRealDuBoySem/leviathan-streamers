import pytest

from leviathan_common.models.trade_tick import TradeTick
from core.journal.tick_journal import TickJournal, DEFAULT_DEDUP_WINDOW, TickJournalCursor


def _tick(trade_id: str, ts: int = 1000) -> TradeTick:
    return TradeTick("BTCUSDT", ts, 100.0, 1.0, "buy", trade_id)


def test_dedup_window_is_bounded(tmp_path):
    journal = TickJournal(str(tmp_path), dedup_window=3)
    for index in range(5):
        journal.append(_tick(f"t{index}"))
    journal.flush_meta()
    with open(journal.journal_path, "r", encoding="utf-8") as handle:
        assert len(handle.readlines()) == 5
    seq = journal.append(_tick("t0"))
    assert seq == 6


def test_incremental_reader_does_not_rescan_whole_file(tmp_path):
    journal = TickJournal(str(tmp_path))
    for index in range(20):
        journal.append(_tick(f"t{index}", ts=1000 + index))
    reader = journal.create_incremental_reader()
    first_batch = reader.poll(1)
    assert len(first_batch) == 20
    journal.append(_tick("t20", ts=1020))
    second_batch = reader.poll(21)
    assert len(second_batch) == 1
    assert second_batch[0][1].trade_id == "t20"


def test_compact_drops_processed_prefix(tmp_path):
    journal = TickJournal(str(tmp_path), dedup_window=DEFAULT_DEDUP_WINDOW)
    for index in range(10):
        journal.append(_tick(f"t{index}", ts=1000 + index))
    journal.save_cursor(TickJournalCursor(last_processed_seq=8))
    removed = journal.compact_before_seq(6)
    assert removed == 5
    replay = list(journal.tail_from(6))
    assert replay[0][0] == 6
    assert replay[-1][0] == 10


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
