"""Unit tests for journal read-progress snapshot computation and classifiers."""

from __future__ import annotations

import os

import pytest

from core.journal.journal_read_progress import (
    TIP_META_LAG_TOLERANCE_SEQ,
    compute_read_progress_snapshot,
    is_eof_caught_up_progress_snapshot,
    is_seq_caught_up_trailing_byte_lag_snapshot,
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


# D6-A03 / H01 pre-restart storm signature (offset==size, lag_seq=0, not stuck).
_D6_EOF_CAUGHT_UP_SNAPSHOT = {
    "read_offset": 2_702_052,
    "journal_size": 2_702_052,
    "next_seq": 560_247,
    "latest_seq": 560_232,  # stale/regressive meta tip (H01-A03) — must not force WARNING
    "lag_seq": 0,
    "incomplete_stuck": False,
}


@pytest.mark.parametrize(
    "snapshot,expected_eof",
    [
        (_D6_EOF_CAUGHT_UP_SNAPSHOT, True),
        (
            {
                "read_offset": 100,
                "journal_size": 100,
                "next_seq": 5,
                "latest_seq": 4,
                "lag_seq": 0,
                "incomplete_stuck": False,
            },
            True,
        ),
        (
            {
                "read_offset": 0,
                "journal_size": 0,
                "next_seq": 1,
                "latest_seq": 0,
                "lag_seq": 0,
                "incomplete_stuck": False,
            },
            True,
        ),
        (
            {
                "read_offset": 50,
                "journal_size": 100,
                "next_seq": 2,
                "latest_seq": 5,
                "lag_seq": 4,
                "incomplete_stuck": False,
            },
            False,
        ),
        (
            {
                "read_offset": 100,
                "journal_size": 100,
                "next_seq": 6,
                "latest_seq": 5,
                "lag_seq": 1,
                "incomplete_stuck": True,
            },
            False,
        ),
        (
            {
                "read_offset": 99,
                "journal_size": 100,
                "next_seq": 5,
                "latest_seq": 4,
                "lag_seq": 0,
                "incomplete_stuck": False,
            },
            False,
        ),
    ],
)
def test_is_eof_caught_up_progress_snapshot_d6_contract(snapshot, expected_eof):
    """D6-A03: lag_seq=0 + offset>=size + not stuck ⇒ EOF wait, never 'unread lag'."""
    assert is_eof_caught_up_progress_snapshot(snapshot) is expected_eof


def test_is_eof_caught_up_progress_snapshot_rejects_invalid_payload():
    """D6-A03: malformed snapshot fields must raise ValueError (contract guard)."""
    with pytest.raises(ValueError, match="progress snapshot must expose"):
        is_eof_caught_up_progress_snapshot({"read_offset": 0})
    with pytest.raises(ValueError, match="progress snapshot must expose"):
        is_eof_caught_up_progress_snapshot(
            {
                "read_offset": "bad",
                "journal_size": 1,
                "lag_seq": 0,
                "incomplete_stuck": False,
            }
        )


def test_is_seq_caught_up_trailing_byte_lag_snapshot_j27_h07_contract():
    """J27 H07: exact forced-resync false-positive fields must be recognized."""
    h07 = {
        "read_offset": 254_937_853,
        "journal_size": 254_938_264,
        "next_seq": 2_063_837,
        "latest_seq": 2_063_836,
        "lag_seq": 1,
        "incomplete_stuck": False,
    }
    assert is_seq_caught_up_trailing_byte_lag_snapshot(h07) is True
    assert is_eof_caught_up_progress_snapshot(h07) is False

    # Real seq lag (tip at/ahead of next) is not the H07 false positive.
    assert (
        is_seq_caught_up_trailing_byte_lag_snapshot(
            {
                "read_offset": 50,
                "journal_size": 100,
                "next_seq": 10,
                "latest_seq": 12,
                "lag_seq": 3,
                "incomplete_stuck": False,
            }
        )
        is False
    )
    # Sticky incomplete still needs force-resync path (not the H07 no-op).
    assert (
        is_seq_caught_up_trailing_byte_lag_snapshot(
            {
                "read_offset": 50,
                "journal_size": 100,
                "next_seq": 10,
                "latest_seq": 9,
                "lag_seq": 1,
                "incomplete_stuck": True,
            }
        )
        is False
    )
    with pytest.raises(ValueError, match="progress snapshot must expose"):
        is_seq_caught_up_trailing_byte_lag_snapshot({"read_offset": 0})


def test_is_seq_caught_up_trailing_byte_lag_snapshot_j31_h11_contract():
    """
    REGRESSION J31 F-J31-08 / H11 @11:36:30 — exact forced-resync log fields
    (v0.18.35 before J27 gate in v0.18.36) must stay recognized as trailing-byte FP.
    """
    h11 = {
        "read_offset": 293_973_475,
        "journal_size": 293_973_613,
        "next_seq": 2_347_069,
        "latest_seq": 2_347_068,
        "lag_seq": 1,
        "incomplete_stuck": False,
    }
    assert is_seq_caught_up_trailing_byte_lag_snapshot(h11) is True
    assert is_eof_caught_up_progress_snapshot(h11) is False


def test_is_seq_caught_up_trailing_byte_lag_snapshot_j32_h01_h02_contract():
    """
    REGRESSION J32 F-J32-06 / H01–H02 — soft-stale journal_lag + forced tail
    resync under v0.18.35 with next_seq = latest+1 and incomplete_stuck=False
    (root lag_seq no longer artificially bumped) must stay H07 FP.
    """
    h02 = {
        "read_offset": 301_000_000,
        "journal_size": 301_000_140,
        "next_seq": 2_416_837,
        "latest_seq": 2_416_836,
        "lag_seq": 0,
        "incomplete_stuck": False,
    }
    assert is_seq_caught_up_trailing_byte_lag_snapshot(h02) is True
    assert is_eof_caught_up_progress_snapshot(h02) is False
