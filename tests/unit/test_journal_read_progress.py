"""Unit tests for journal read-progress snapshot computation."""

from __future__ import annotations

import os

from core.journal.journal_read_progress import (
    TIP_META_LAG_TOLERANCE_SEQ,
    compute_read_progress_snapshot,
)
from core.journal.journal_incremental_reader import JournalIncrementalReader
from core.journal.journal_io import atomic_write_json
from core.journal.tick_journal import META_PERSIST_INTERVAL, TickJournal
from leviathan_common.models.trade_tick import TradeTick


def _seed_journal_meta(journal, tmp_path, *, latest_seq, seq_index=None):
    payload = {
        "latest_seq": latest_seq,
        "seen_trade_ids": {},
        "seq_index": [[0, 0]] if seq_index is None else seq_index,
    }
    atomic_write_json(str(tmp_path / "tick_journal.meta.json"), payload)
    journal.reload_meta_from_disk()


def _tick(trade_id: str, ts: int = 1000) -> TradeTick:
    return TradeTick("XRPUSDT", ts, 1.0, 1.0, "buy", trade_id)


def test_compute_read_progress_snapshot_caught_up_with_incomplete_stuck():
    snapshot = compute_read_progress_snapshot(
        read_offset=100,
        journal_size=200,
        next_seq=5,
        disk_latest_seq=4,
        incomplete_stuck=True,
    )
    assert snapshot["lag_seq"] == 1
    assert snapshot["incomplete_stuck"] is True


def test_compute_read_progress_snapshot_no_lag_invented_when_caught_up():
    """J32: trailing bytes alone must not invent lag_seq when seq is past tip."""
    snapshot = compute_read_progress_snapshot(
        read_offset=500,
        journal_size=600,
        next_seq=10,
        disk_latest_seq=9,
        incomplete_stuck=False,
    )
    assert snapshot["lag_seq"] == 0


def test_compute_read_progress_snapshot_sticky_cursor_overhang():
    live_tip = 1_740_532
    sticky_cursor = 1_741_629
    assert sticky_cursor - live_tip > TIP_META_LAG_TOLERANCE_SEQ
    snapshot = compute_read_progress_snapshot(
        read_offset=0,
        journal_size=100,
        next_seq=sticky_cursor + 1,
        disk_latest_seq=live_tip,
        incomplete_stuck=False,
    )
    assert snapshot["latest_seq"] == live_tip
    assert snapshot["cursor_ahead_of_tip"] is True


def test_compute_read_progress_snapshot_coerces_stale_meta_within_tolerance():
    stale_watermark = 100
    consumed_through = stale_watermark + (META_PERSIST_INTERVAL // 2)
    snapshot = compute_read_progress_snapshot(
        read_offset=50,
        journal_size=100,
        next_seq=consumed_through + 1,
        disk_latest_seq=stale_watermark,
        incomplete_stuck=False,
    )
    assert snapshot["latest_seq"] == consumed_through
    assert snapshot["lag_seq"] == 0
    assert snapshot.get("cursor_ahead_of_tip") is False


def test_read_progress_snapshot_exposes_offset_size_and_lag(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("a"))
    journal.append(_tick("b"))
    journal.flush_meta()
    reader = JournalIncrementalReader(journal)
    reader.reset_from_seq(2)
    snapshot = reader.get_read_progress_snapshot()
    assert snapshot["next_seq"] == 2
    assert snapshot["read_offset"] >= 0
    assert snapshot["journal_size"] >= snapshot["read_offset"]
    assert snapshot["latest_seq"] == 2
    assert snapshot["lag_seq"] == 1
    assert "incomplete_stuck" in snapshot


def test_read_progress_snapshot_latest_seq_not_stale_vs_reader_progress(tmp_path):
    """
    Meta may lag (META_PERSIST_INTERVAL); snapshot latest_seq must not stay
    below records the reader has already consumed (next_seq >> disk watermark).
    """
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("a"))
    journal.append(_tick("b"))
    journal.append(_tick("c"))
    _seed_journal_meta(journal, tmp_path, latest_seq=1)

    reader = JournalIncrementalReader(journal)
    assert [seq for seq, _ in reader.poll(1)] == [1, 2, 3]
    snapshot = reader.get_read_progress_snapshot()
    assert snapshot["next_seq"] == 4
    assert snapshot["latest_seq"] == 3
    assert snapshot["lag_seq"] == 0
    assert snapshot["read_offset"] == snapshot["journal_size"]


def test_read_progress_snapshot_coerces_production_stale_meta_watermark(tmp_path):
    """
    D6-A04 / D5-08 within META_PERSIST lag: disk meta may lag the reader by a
    small persist window; snapshot latest_seq must still reflect consumed floor.

    Large checkpoint cursor overhang past live disk tip is BB-D23-02 sticky
    tip-split (see test_read_progress_snapshot_rejects_sticky_cursor_overhang).
    """
    stale_watermark = 100
    consumed_through = stale_watermark + (META_PERSIST_INTERVAL // 2)
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("seed"))
    _seed_journal_meta(journal, tmp_path, latest_seq=stale_watermark)
    assert journal.read_latest_seq_from_disk() == stale_watermark

    reader = JournalIncrementalReader(journal)
    reader._JournalIncrementalReader__next_seq = consumed_through + 1
    try:
        reader._JournalIncrementalReader__read_offset = os.path.getsize(
            journal.journal_path
        )
    except OSError:
        reader._JournalIncrementalReader__read_offset = 0

    snapshot = reader.get_read_progress_snapshot()
    assert snapshot["next_seq"] == consumed_through + 1
    assert snapshot["latest_seq"] == consumed_through
    assert snapshot["latest_seq"] >= snapshot["next_seq"] - 1
    assert snapshot["latest_seq"] > stale_watermark
    assert snapshot["lag_seq"] == 0
    assert snapshot.get("cursor_ahead_of_tip") is False


def test_read_progress_snapshot_rejects_sticky_cursor_overhang(tmp_path):
    """
    BB-D23-02: checkpoint cursor far past live disk tip must not invent a
    sticky tip (J23 H05 dual tip / famine until catch-up).
    """
    live_tip = 1_740_532
    sticky_cursor = 1_741_629
    assert sticky_cursor - live_tip > META_PERSIST_INTERVAL
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("seed"))
    _seed_journal_meta(journal, tmp_path, latest_seq=live_tip)

    reader = JournalIncrementalReader(journal)
    reader._JournalIncrementalReader__next_seq = sticky_cursor + 1
    try:
        reader._JournalIncrementalReader__read_offset = os.path.getsize(
            journal.journal_path
        )
    except OSError:
        reader._JournalIncrementalReader__read_offset = 0

    snapshot = reader.get_read_progress_snapshot()
    assert snapshot["latest_seq"] == live_tip
    assert snapshot["cursor_ahead_of_tip"] is True
    assert snapshot["next_seq"] == sticky_cursor + 1


def test_read_progress_snapshot_handles_missing_journal_file(tmp_path, mocker):
    journal = TickJournal(str(tmp_path))
    journal.append(TradeTick("XRPUSDT", 1000, 0.5, 1.0, "buy", "a"))
    reader = JournalIncrementalReader(journal)
    mocker.patch("os.path.getsize", side_effect=OSError("gone"))
    snapshot = reader.get_read_progress_snapshot()
    assert snapshot["journal_size"] == 0
