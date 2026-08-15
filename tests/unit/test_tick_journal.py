"""TickJournal facade unit tests (append, cursor, handoff, validation)."""

import os
import time

import pytest

from core.journal.journal_file_lock import JournalFileLock
from core.journal.tick_journal import META_PERSIST_INTERVAL, TickJournal
from core.journal.tick_journal_cursor import TickJournalCursor
from core.journal.tick_journal_meta import TickJournalMetaStore
from leviathan_common.models.trade_tick import TradeTick


def _tick(trade_id: str, ts: int = 1000) -> TradeTick:
    return TradeTick(
        inst_id="BTCUSDT",
        ts=ts,
        price=100.0,
        size=1.0,
        side="buy",
        trade_id=trade_id,
    )


def test_tick_journal_read_latest_seq_from_disk_sees_other_process_appends(tmp_path):
    writer = TickJournal(str(tmp_path))
    reader = TickJournal(str(tmp_path))
    assert reader.read_latest_seq_from_disk() == 0
    writer.append(_tick("t1"))
    writer.flush_meta()
    assert reader.latest_seq() == 0
    assert reader.read_latest_seq_from_disk() == 1


def test_tick_journal_append_and_replay(tmp_path):
    journal = TickJournal(str(tmp_path))
    seq1 = journal.append(_tick("t1"))
    seq2 = journal.append(_tick("t2", ts=1100))
    assert seq1 == 1
    assert seq2 == 2
    assert journal.latest_seq() == 2

    replay = list(journal.tail_from(1))
    assert len(replay) == 2
    assert replay[0][0] == 1
    assert replay[1][1].trade_id == "t2"


def test_tick_journal_deduplicates_trade_id(tmp_path):
    journal = TickJournal(str(tmp_path))
    first = journal.append(_tick("dup"))
    second = journal.append(_tick("dup", ts=1200))
    assert first == 1
    assert second == 1
    assert journal.latest_seq() == 1


def test_tick_journal_cursor_round_trip(tmp_path):
    journal = TickJournal(str(tmp_path))
    cursor = TickJournalCursor(last_processed_seq=7)
    journal.save_cursor(cursor)
    loaded = journal.load_cursor()
    assert loaded.last_processed_seq == 7


def test_tick_journal_strips_checkpoint_dir(tmp_path):
    journal = TickJournal(f"  {tmp_path}  ")
    assert journal.journal_path == os.path.join(str(tmp_path), "tick_journal.jsonl")


def test_tick_journal_load_cursor_rejects_invalid_json(tmp_path):
    journal = TickJournal(str(tmp_path))
    with open(journal.cursor_path, "w", encoding="utf-8") as handle:
        handle.write("{not-json")
    with pytest.raises(ValueError, match="not valid JSON"):
        journal.load_cursor()


def test_tick_journal_constructor_rejects_invalid_dedup_window(tmp_path):
    with pytest.raises(ValueError, match="dedup_window must be positive"):
        TickJournal(str(tmp_path), dedup_window=0)


def test_tick_journal_tail_from_rejects_negative_start_seq(tmp_path):
    journal = TickJournal(str(tmp_path))
    with pytest.raises(ValueError, match="start_seq must be a non-negative integer"):
        list(journal.tail_from(-1))


def test_tick_journal_read_latest_seq_from_disk_falls_back_on_corrupt_meta(tmp_path, mocker):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("t1"))
    mocker.patch.object(
        TickJournalMetaStore,
        "load_payload",
        side_effect=OSError("bad meta"),
    )
    assert journal.read_latest_seq_from_disk() == journal.latest_seq()


def test_tick_journal_append_supervisor_handoff_pulse(tmp_path):
    journal = TickJournal(str(tmp_path))
    seq = journal.append_supervisor_handoff_pulse("btcusdt")
    assert seq == 1
    replay = list(journal.tail_from(1))
    assert replay[0][1].inst_id == "BTCUSDT"
    assert replay[0][1].trade_id.startswith("LEV-HANDOFF-")


def test_tick_journal_append_supervisor_handoff_pulse_rejects_empty_symbol(tmp_path):
    journal = TickJournal(str(tmp_path))
    with pytest.raises(ValueError, match="symbol must be a non-empty string"):
        journal.append_supervisor_handoff_pulse("   ")


def test_j33_h22_partial_burst_leaves_disk_tip_behind_without_idle_flush(tmp_path):
    """
    REGRESSION J33 H22 root precondition: after META_PERSIST boundary, a
    mid-stream burst Δ=23 (disk 2518909 → cursor 2518932 class) leaves durable
    tip frozen until idle flush / explicit flush_meta.
    """
    journal = TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0)
    for index in range(META_PERSIST_INTERVAL):
        journal.append(_tick(f"seed{index}", ts=1000 + index))
    assert (
        TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0).read_latest_seq_from_disk()
        == META_PERSIST_INTERVAL
    )

    overhang = 23  # H22 cursor-disk Δ
    for index in range(overhang):
        journal.append(_tick(f"over{index}", ts=2000 + index))

    assert journal.latest_seq() == META_PERSIST_INTERVAL + overhang
    # Cross-process observer (no writer high-water) sees frozen durable tip.
    assert (
        TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0).read_latest_seq_from_disk()
        == META_PERSIST_INTERVAL
    )
    assert journal.has_unpersisted_meta() is True


def test_j33_h22_idle_meta_flush_publishes_disk_tip_after_partial_burst(tmp_path):
    """
    REGRESSION J33 H22 / F-J33-03: idle meta flush publishes tip after a partial
    burst so quiet-market silence cannot freeze disk tip mid-stream (Δ ≤
    META_PERSIST) while journal body / cursor already advanced.
    """
    journal = TickJournal(str(tmp_path), meta_idle_flush_seconds=0.05)
    for index in range(META_PERSIST_INTERVAL):
        journal.append(_tick(f"seed{index}", ts=1000 + index))

    overhang = 23
    for index in range(overhang):
        journal.append(_tick(f"over{index}", ts=2000 + index))
    expected = META_PERSIST_INTERVAL + overhang
    assert journal.latest_seq() == expected
    assert journal.has_unpersisted_meta() is True

    deadline = time.monotonic() + 2.0
    disk_tip = META_PERSIST_INTERVAL
    while time.monotonic() < deadline:
        disk_tip = TickJournal(
            str(tmp_path), meta_idle_flush_seconds=0.0
        ).read_latest_seq_from_disk()
        if disk_tip == expected and not journal.has_unpersisted_meta():
            break
        time.sleep(0.02)

    assert disk_tip == expected
    assert journal.has_unpersisted_meta() is False
    assert journal.flush_meta_if_dirty() is False


def test_tick_journal_flush_meta_if_dirty_publishes_partial_burst(tmp_path):
    journal = TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0)
    for index in range(META_PERSIST_INTERVAL):
        journal.append(_tick(f"seed{index}", ts=1000 + index))
    journal.append(_tick("partial", ts=3000))
    assert journal.has_unpersisted_meta() is True
    assert journal.flush_meta_if_dirty() is True
    assert journal.has_unpersisted_meta() is False
    assert (
        TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0).read_latest_seq_from_disk()
        == META_PERSIST_INTERVAL + 1
    )


def test_tick_journal_flush_meta_if_dirty_is_noop_when_clean(tmp_path):
    journal = TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0)
    for index in range(META_PERSIST_INTERVAL):
        journal.append(_tick(f"seed{index}", ts=1000 + index))
    assert journal.has_unpersisted_meta() is False
    assert journal.flush_meta_if_dirty() is False


def test_tick_journal_rejects_invalid_meta_idle_flush_seconds(tmp_path):
    with pytest.raises(ValueError, match="meta_idle_flush_seconds must be >= 0"):
        TickJournal(str(tmp_path), meta_idle_flush_seconds=-0.1)
    with pytest.raises(TypeError, match="meta_idle_flush_seconds must be a number"):
        TickJournal(str(tmp_path), meta_idle_flush_seconds="1")  # type: ignore[arg-type]


def test_j34_h20_stale_reader_flush_meta_does_not_rewind_collector_tip(tmp_path):
    """
    REGRESSION J34 / F-J34-01 (H20 ahead=508): engine checkpoint ``flush_meta``
    on a read-only TickJournal must not rewrite collector tip backward.

    Root: collector publishes tip every META_PERSIST / idle flush; engine holds
    a separate journal instance whose in-memory tip lags. Checkpoint flush was
    clobbering durable meta with the stale tip while journal body stayed ahead
    → mid-stream under-report repaired by F-J33-01.
    """
    collector = TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0)
    for index in range(META_PERSIST_INTERVAL):
        collector.append(_tick(f"seed{index}", ts=1000 + index))
    assert (
        TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0).read_latest_seq_from_disk()
        == META_PERSIST_INTERVAL
    )

    # Engine-like reader attached at the post-persist tip (boot / last observe).
    engine_journal = TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0)
    assert engine_journal.read_latest_seq_from_disk() == META_PERSIST_INTERVAL
    assert engine_journal.has_unpersisted_meta() is False

    # Collector burst past the reader snapshot (H20-class overhang 508).
    overhang = 508
    for index in range(overhang):
        collector.append(_tick(f"burst{index}", ts=2000 + index))
    collector.flush_meta()
    live_tip = META_PERSIST_INTERVAL + overhang
    assert collector.latest_seq() == live_tip
    assert (
        TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0).read_latest_seq_from_disk()
        == live_tip
    )

    # Checkpoint-style flush from the stale reader must leave durable tip intact.
    engine_journal.flush_meta()
    assert (
        TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0).read_latest_seq_from_disk()
        == live_tip
    )
    assert engine_journal.has_unpersisted_meta() is False


def test_j34_h17_stale_reader_flush_meta_noop_when_clean_keeps_tip(tmp_path):
    """
    REGRESSION J34 / F-J34-01 (H17 ahead=369): clean reader flush is a no-op
    and cannot under-report tip vs collector body progress.
    """
    collector = TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0)
    for index in range(META_PERSIST_INTERVAL):
        collector.append(_tick(f"seed{index}", ts=1000 + index))
    reader = TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0)

    overhang = 369
    for index in range(overhang):
        collector.append(_tick(f"h17{index}", ts=3000 + index))
    collector.flush_meta()
    live_tip = META_PERSIST_INTERVAL + overhang

    reader.flush_meta()
    assert reader.has_unpersisted_meta() is False
    assert (
        TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0).read_latest_seq_from_disk()
        == live_tip
    )


def test_j34_flush_meta_still_publishes_writer_dirty_tip(tmp_path):
    """Writer flush_meta must still publish a dirty partial burst (F-J33-03)."""
    journal = TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0)
    for index in range(META_PERSIST_INTERVAL):
        journal.append(_tick(f"seed{index}", ts=1000 + index))
    journal.append(_tick("partial", ts=4000))
    assert journal.has_unpersisted_meta() is True
    assert journal.flush_meta() is True
    assert journal.has_unpersisted_meta() is False
    assert (
        TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0).read_latest_seq_from_disk()
        == META_PERSIST_INTERVAL + 1
    )


def test_j34_flush_meta_returns_false_when_dirty_cleared_under_lock(tmp_path, mocker):
    """
    Coverage / race: outer dirty check passes, then another path clears dirty
    before the locked re-check → flush_meta returns False without persist.
    """
    journal = TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0)
    for index in range(META_PERSIST_INTERVAL):
        journal.append(_tick(f"seed{index}", ts=1000 + index))
    journal.append(_tick("race-dirty", ts=5000))
    assert journal.has_unpersisted_meta() is True
    # Durable tip still at interval boundary until a successful dirty flush.
    assert (
        TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0).read_latest_seq_from_disk()
        == META_PERSIST_INTERVAL
    )

    original_enter = JournalFileLock.__enter__

    def _enter_clearing_dirty(self):
        journal._TickJournal__meta_dirty = False
        return original_enter(self)

    mocker.patch.object(JournalFileLock, "__enter__", _enter_clearing_dirty)
    assert journal.flush_meta() is False
    assert journal.has_unpersisted_meta() is False
    # Persist skipped: disk tip remains at the last auto-persist boundary.
    assert (
        TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0).read_latest_seq_from_disk()
        == META_PERSIST_INTERVAL
    )
