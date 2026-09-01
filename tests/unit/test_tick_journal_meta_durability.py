"""Unit tests for TickJournalMetaDurability."""

import threading
import time

import pytest

from core.journal.journal_file_lock import JournalFileLock
from core.journal.tick_journal import META_PERSIST_INTERVAL, TickJournal
from core.journal.tick_journal_meta import TickJournalMetaStore
from core.journal.tick_journal_meta_durability import TickJournalMetaDurability
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


def _durability(
    tmp_path,
    *,
    persist_interval: int = 2,
    idle_flush_seconds: float = 0.0,
):
    store = TickJournalMetaStore(str(tmp_path / "tick_journal.meta.json"), dedup_window=10)
    lock = threading.Lock()
    durability = TickJournalMetaDurability(
        meta_store=store,
        lock_path=str(tmp_path / "tick_journal.lock"),
        thread_lock=lock,
        persist_interval=persist_interval,
        idle_flush_seconds=idle_flush_seconds,
    )
    return durability, store, lock


def test_durability_rejects_invalid_dependencies(tmp_path):
    store = TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=10)
    lock = threading.Lock()
    kwargs = {
        "meta_store": store,
        "lock_path": str(tmp_path / "l.lock"),
        "thread_lock": lock,
    }
    with pytest.raises(TypeError, match="meta_store must be a TickJournalMetaStore"):
        TickJournalMetaDurability(**{**kwargs, "meta_store": object()})  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="lock_path must be a non-empty string"):
        TickJournalMetaDurability(**{**kwargs, "lock_path": "  "})
    with pytest.raises(TypeError, match="thread_lock must be a threading.Lock-like object"):
        TickJournalMetaDurability(**{**kwargs, "thread_lock": object()})
    with pytest.raises(ValueError, match="persist_interval must be positive"):
        TickJournalMetaDurability(**{**kwargs, "persist_interval": 0})
    with pytest.raises(TypeError, match="persist_interval must be an integer"):
        TickJournalMetaDurability(**{**kwargs, "persist_interval": 1.5})  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="idle_flush_seconds must be >= 0"):
        TickJournalMetaDurability(**{**kwargs, "idle_flush_seconds": -0.1})
    with pytest.raises(TypeError, match="idle_flush_seconds must be a number"):
        TickJournalMetaDurability(**{**kwargs, "idle_flush_seconds": "1"})  # type: ignore[arg-type]


def test_note_append_persists_on_interval_and_marks_dirty_otherwise(tmp_path):
    durability, store, lock = _durability(tmp_path, persist_interval=2)
    store.set_latest_seq(1)
    with lock:
        durability.note_append_locked()
    assert durability.has_unpersisted_meta() is True
    observer = TickJournalMetaStore(store.meta_path, dedup_window=10)
    assert observer.latest_seq() == 0

    store.set_latest_seq(2)
    with lock:
        durability.note_append_locked()
    assert durability.has_unpersisted_meta() is False
    observer = TickJournalMetaStore(store.meta_path, dedup_window=10)
    assert observer.latest_seq() == 2


def test_flush_is_noop_when_clean(tmp_path):
    durability, _store, _lock = _durability(tmp_path)
    assert durability.flush() is False
    assert durability.has_unpersisted_meta() is False


def test_flush_publishes_dirty_tip(tmp_path):
    durability, store, lock = _durability(tmp_path, persist_interval=10)
    store.set_latest_seq(3)
    with lock:
        durability.note_append_locked()
    assert durability.has_unpersisted_meta() is True
    assert durability.flush() is True
    assert durability.has_unpersisted_meta() is False
    observer = TickJournalMetaStore(store.meta_path, dedup_window=10)
    assert observer.latest_seq() == 3


def test_flush_returns_false_when_dirty_cleared_under_lock(tmp_path, mocker):
    durability, store, lock = _durability(tmp_path, persist_interval=10)
    store.set_latest_seq(4)
    with lock:
        durability.note_append_locked()
    assert durability.has_unpersisted_meta() is True

    original_enter = JournalFileLock.__enter__

    def _enter_clearing_dirty(self):
        durability._TickJournalMetaDurability__meta_dirty = False
        return original_enter(self)

    mocker.patch.object(JournalFileLock, "__enter__", _enter_clearing_dirty)
    assert durability.flush() is False
    assert durability.has_unpersisted_meta() is False
    observer = TickJournalMetaStore(store.meta_path, dedup_window=10)
    assert observer.latest_seq() == 0


def test_idle_flush_publishes_after_quiet_gap(tmp_path):
    durability, store, lock = _durability(
        tmp_path, persist_interval=10, idle_flush_seconds=0.05
    )
    store.set_latest_seq(5)
    with lock:
        durability.note_append_locked()
    assert durability.has_unpersisted_meta() is True

    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if not durability.has_unpersisted_meta():
            break
        time.sleep(0.02)

    assert durability.has_unpersisted_meta() is False
    observer = TickJournalMetaStore(store.meta_path, dedup_window=10)
    assert observer.latest_seq() == 5


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


def test_j34_h20_stale_reader_flush_meta_does_not_rewind_collector_tip(tmp_path):
    """
    REGRESSION J34 / F-J34-01 (H20 ahead=508): engine checkpoint ``flush_meta``
    on a read-only TickJournal must not rewrite collector tip backward.
    """
    collector = TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0)
    for index in range(META_PERSIST_INTERVAL):
        collector.append(_tick(f"seed{index}", ts=1000 + index))
    assert (
        TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0).read_latest_seq_from_disk()
        == META_PERSIST_INTERVAL
    )

    engine_journal = TickJournal(str(tmp_path), meta_idle_flush_seconds=0.0)
    assert engine_journal.read_latest_seq_from_disk() == META_PERSIST_INTERVAL
    assert engine_journal.has_unpersisted_meta() is False

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
