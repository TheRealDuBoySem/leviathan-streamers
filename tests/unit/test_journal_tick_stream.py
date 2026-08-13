import asyncio
import logging
from unittest.mock import AsyncMock

import pytest

from core.interfaces.base import IPriceObserver
from leviathan_common.models.trade_tick import TradeTick
from core.journal.journal_incremental_reader import JournalIncrementalReader
from core.journal.tick_journal import TickJournal
from core.journal.tick_journal_cursor import TickJournalCursor
from core.journal.journal_tick_stream import (
    JournalStreamFatalError,
    JournalTickStream,
    is_eof_caught_up_progress_snapshot,
    is_seq_caught_up_trailing_byte_lag_snapshot,
)


def _tick(trade_id: str, ts: int = 1000) -> TradeTick:
    return TradeTick(
        inst_id="BTCUSDT",
        ts=ts,
        price=100.0,
        size=1.0,
        side="buy",
        trade_id=trade_id,
    )


async def _run_stream(stream: JournalTickStream):
    task = asyncio.create_task(stream.start_streaming())
    await asyncio.sleep(0)
    return task


async def _stop_stream(stream: JournalTickStream, task: asyncio.Task) -> None:
    await stream.stop()
    await task


@pytest.mark.asyncio
async def test_journal_tick_stream_replays_and_follows(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("t1"))
    journal.append(_tick("t2", ts=1100))

    stream = JournalTickStream(journal, poll_interval_seconds=0.01, symbols=["BTCUSDT"])
    stream_task = await _run_stream(stream)

    first = await stream.wait_for_next_tick()
    assert first.trade_id == "t1"
    stream.mark_tick_as_processed()
    assert stream.cursor.last_processed_seq == 1

    second = await stream.wait_for_next_tick()
    assert second.trade_id == "t2"
    stream.mark_tick_as_processed()
    assert stream.cursor.last_processed_seq == 2

    journal.append(_tick("t3", ts=1200))
    await asyncio.sleep(0.05)
    third = await stream.wait_for_next_tick()
    assert third.trade_id == "t3"

    await _stop_stream(stream, stream_task)


@pytest.mark.asyncio
async def test_journal_tick_stream_resumes_from_cursor(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("t1"))
    journal.save_cursor(TickJournalCursor(last_processed_seq=1))
    journal.append(_tick("t2", ts=1100))

    stream = JournalTickStream(journal, poll_interval_seconds=0.01)
    stream_task = await _run_stream(stream)

    tick = await stream.wait_for_next_tick()
    assert tick.trade_id == "t2"
    await _stop_stream(stream, stream_task)


@pytest.mark.asyncio
async def test_journal_tick_stream_notifies_attached_observers(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("t1"))

    observer = AsyncMock(spec=IPriceObserver)
    stream = JournalTickStream(journal, poll_interval_seconds=0.01)
    stream.attach_observer(observer)
    stream_task = await _run_stream(stream)

    tick = await stream.wait_for_next_tick()
    assert tick.trade_id == "t1"
    observer.on_price_update.assert_awaited_once_with(tick)

    await _stop_stream(stream, stream_task)


def test_journal_tick_stream_mark_without_pending_raises(tmp_path):
    stream = JournalTickStream(TickJournal(str(tmp_path)))
    with pytest.raises(RuntimeError, match="without a pending tick"):
        stream.mark_tick_as_processed()


@pytest.mark.asyncio
async def test_journal_tick_stream_set_cursor_clears_pending_mid_consume(tmp_path):
    """
    REGRESSION J32 / BB-D32-01: set_cursor (tip-split rewind) while a tick is
    pending must clear pending_seq, release the queue slot, and make the
    subsequent mark_tick a no-op (engine finally must not crash).
    """
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("t1"))
    stream = JournalTickStream(journal, poll_interval_seconds=0.01, symbols=["BTCUSDT"])
    stream_task = await _run_stream(stream)

    tick = await stream.wait_for_next_tick()
    assert tick.trade_id == "t1"
    assert stream.has_pending_tick() is True
    stream.set_cursor(TickJournalCursor(last_processed_seq=0))
    assert stream.has_pending_tick() is False
    # No-op after intentional invalidation (not a bare misuse of mark).
    stream.mark_tick_as_processed()
    assert stream.has_pending_tick() is False

    await _stop_stream(stream, stream_task)


@pytest.mark.asyncio
async def test_journal_tick_stream_set_cursor_tolerates_task_done_without_join(
    tmp_path,
):
    """
    J32 BB-D32-01: if the queue join counter was already released before
    set_cursor, task_done ValueError must be swallowed (no crash mid-rewind).
    """
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("t1"))
    stream = JournalTickStream(journal, poll_interval_seconds=0.01, symbols=["BTCUSDT"])
    stream_task = await _run_stream(stream)

    tick = await stream.wait_for_next_tick()
    assert tick.trade_id == "t1"
    assert stream.has_pending_tick() is True
    # Premature task_done → unfinished counter already balanced.
    stream._JournalTickStream__queue.task_done()
    stream.set_cursor(TickJournalCursor(last_processed_seq=0))
    assert stream.has_pending_tick() is False
    stream.mark_tick_as_processed()

    await _stop_stream(stream, stream_task)


def test_journal_tick_stream_attach_observer_validates_type(tmp_path):
    stream = JournalTickStream(TickJournal(str(tmp_path)))
    with pytest.raises(TypeError, match="IPriceObserver"):
        stream.attach_observer(object())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_journal_tick_stream_subscribe_validates_symbol(tmp_path):
    stream = JournalTickStream(TickJournal(str(tmp_path)))
    with pytest.raises(ValueError, match="cannot be empty"):
        await stream.subscribe_symbol("")


def test_journal_tick_stream_set_cursor_rejects_negative_seq(tmp_path):
    stream = JournalTickStream(TickJournal(str(tmp_path)))
    with pytest.raises(ValueError, match="non-negative"):
        stream.set_cursor(TickJournalCursor(last_processed_seq=-1))


def test_journal_tick_stream_rejects_invalid_constructor_args(tmp_path):
    journal = TickJournal(str(tmp_path))
    with pytest.raises(TypeError, match="TickJournal"):
        JournalTickStream(object())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="poll_interval_seconds must be positive"):
        JournalTickStream(journal, poll_interval_seconds=0)
    with pytest.raises(ValueError, match="empty_poll_diagnostic_seconds must be positive"):
        JournalTickStream(journal, empty_poll_diagnostic_seconds=0)
    with pytest.raises(TypeError, match="on_stream_fatal must be callable"):
        JournalTickStream(journal, on_stream_fatal=123)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="clock must be callable"):
        JournalTickStream(journal, clock=123)  # type: ignore[arg-type]


def test_journal_tick_stream_get_read_progress_snapshot_delegates(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("a"))
    stream = JournalTickStream(
        journal,
        incomplete_record_max_wait_seconds=1.5,
    )
    snapshot = stream.get_read_progress_snapshot()
    assert "read_offset" in snapshot
    assert "lag_seq" in snapshot


def _is_empty_poll_diagnostic(record) -> bool:
    msg = record.message.lower()
    return (
        "journal unread lag" in msg
        or "waiting for new journal" in msg
        or "caught up at eof" in msg
        or "trailing journal bytes" in msg
    )


class _FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += float(seconds)


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


@pytest.mark.asyncio
async def test_j32_trailing_byte_empty_poll_logs_debug_never_warning(
    tmp_path, caplog, monkeypatch
):
    """
    J32 quieter mitigation: seq-caught-up trailing bytes must emit DEBUG wait
    only — never WARNING « unread lag » (H01/H02 soft-stale spam).
    """
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("prior"))
    journal.save_cursor(TickJournalCursor(last_processed_seq=1))

    trailing = {
        "read_offset": 301_000_000,
        "journal_size": 301_000_140,
        "next_seq": 2_416_837,
        "latest_seq": 2_416_836,
        "lag_seq": 0,
        "incomplete_stuck": False,
    }
    monkeypatch.setattr(
        JournalIncrementalReader,
        "get_read_progress_snapshot",
        lambda self: dict(trailing),
    )

    clock = _FakeClock()
    stream = JournalTickStream(
        journal,
        poll_interval_seconds=0.01,
        empty_poll_diagnostic_seconds=0.05,
        clock=clock,
    )
    stream_task = asyncio.create_task(stream.start_streaming())
    try:
        with caplog.at_level(logging.DEBUG):
            for _ in range(40):
                clock.advance(0.02)
                await asyncio.sleep(0.01)
                if any(_is_empty_poll_diagnostic(r) for r in caplog.records):
                    break
        trailing_records = [
            r
            for r in caplog.records
            if "trailing journal bytes" in r.message.lower()
            or "seq caught up" in r.message.lower()
        ]
        assert trailing_records, "expected DEBUG trailing-byte wait diagnostic"
        assert all(r.levelno == logging.DEBUG for r in trailing_records)
        assert not any(
            "journal unread lag" in r.message.lower() and r.levelno >= logging.WARNING
            for r in caplog.records
        )
        assert not any(
            "forced tail resync" in r.message.lower() for r in caplog.records
        )
    finally:
        await stream.stop()
        stream_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stream_task


@pytest.mark.asyncio
async def test_d6_eof_caught_up_snapshot_logs_debug_never_warning(
    tmp_path, caplog, monkeypatch
):
    """
    Non-regression D6-A03: exact H01 storm fields (offset==size, lag_seq=0,
    incomplete_stuck=False, next_seq>latest_seq) must emit DEBUG EOF-wait only.
    """
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("prior"))
    journal.save_cursor(TickJournalCursor(last_processed_seq=1))

    monkeypatch.setattr(
        JournalIncrementalReader,
        "get_read_progress_snapshot",
        lambda self: dict(_D6_EOF_CAUGHT_UP_SNAPSHOT),
    )

    clock = _FakeClock()
    stream = JournalTickStream(
        journal,
        poll_interval_seconds=0.01,
        empty_poll_diagnostic_seconds=0.05,
        clock=clock,
    )
    stream_task = asyncio.create_task(stream.start_streaming())
    try:
        with caplog.at_level(logging.DEBUG):
            for _ in range(40):
                clock.advance(0.02)
                await asyncio.sleep(0.01)
                if any(_is_empty_poll_diagnostic(r) for r in caplog.records):
                    break
        stream_records = [
            r for r in caplog.records if "JournalTickStream" in r.message
        ]
        eof_records = [
            r
            for r in stream_records
            if "waiting for new journal records at EOF" in r.message
        ]
        assert eof_records, "expected DEBUG EOF-wait diagnostic for D6 snapshot"
        assert all(r.levelno == logging.DEBUG for r in eof_records)
        assert all("lag_seq=0" in r.message for r in eof_records)
        assert all("incomplete_stuck=False" in r.message for r in eof_records)
        assert not any(
            "journal unread lag" in r.message.lower() and r.levelno >= logging.WARNING
            for r in caplog.records
        )
    finally:
        await stream.stop()
        stream_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stream_task


@pytest.mark.asyncio
async def test_journal_tick_stream_empty_poll_diagnostic_is_rate_limited(tmp_path, caplog):
    """Second empty-poll diagnostic within the window must be suppressed."""
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("prior"))
    journal.save_cursor(TickJournalCursor(last_processed_seq=1))

    clock = _FakeClock()
    stream = JournalTickStream(
        journal,
        poll_interval_seconds=0.01,
        empty_poll_diagnostic_seconds=0.05,
        clock=clock,
    )
    stream_task = asyncio.create_task(stream.start_streaming())
    try:
        with caplog.at_level(logging.DEBUG):
            for _ in range(20):
                clock.advance(0.03)
                await asyncio.sleep(0.01)
                if any(_is_empty_poll_diagnostic(r) for r in caplog.records):
                    break
            first_count = sum(1 for r in caplog.records if _is_empty_poll_diagnostic(r))
            assert first_count >= 1
            for _ in range(5):
                clock.advance(0.01)
                await asyncio.sleep(0.01)
            second_count = sum(1 for r in caplog.records if _is_empty_poll_diagnostic(r))
            assert second_count == first_count
            clock.advance(0.06)
            for _ in range(10):
                clock.advance(0.02)
                await asyncio.sleep(0.01)
                if sum(1 for r in caplog.records if _is_empty_poll_diagnostic(r)) > first_count:
                    break
            assert (
                sum(1 for r in caplog.records if _is_empty_poll_diagnostic(r)) > first_count
            )
    finally:
        await stream.stop()
        stream_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stream_task
