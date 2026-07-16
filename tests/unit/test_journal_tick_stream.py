import asyncio
from unittest.mock import AsyncMock

import pytest

from core.interfaces.base import IPriceObserver
from leviathan_common.models.trade_tick import TradeTick
from core.journal.tick_journal import TickJournal, TickJournalCursor
from core.journal.journal_tick_stream import JournalStreamFatalError, JournalTickStream


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


@pytest.mark.asyncio
async def test_journal_tick_stream_unread_lag_log_is_rate_limited(tmp_path, caplog):
    """Second lag log within the diagnostic window must be suppressed (line 283)."""
    import logging
    import time as time_mod

    journal = TickJournal(str(tmp_path))
    journal.append(_tick("prior"))
    journal.save_cursor(TickJournalCursor(last_processed_seq=1))

    class Clock:
        def __init__(self):
            self.now = 1000.0

        def __call__(self):
            return self.now

        def advance(self, seconds: float) -> None:
            self.now += seconds

    clock = Clock()
    stream = JournalTickStream(
        journal,
        poll_interval_seconds=0.01,
        empty_poll_diagnostic_seconds=0.05,
        clock=clock,
    )
    stream_task = asyncio.create_task(stream.start_streaming())
    try:
        with caplog.at_level(logging.WARNING):
            for _ in range(20):
                clock.advance(0.03)
                await asyncio.sleep(0.01)
                if any("journal unread lag" in r.message.lower() for r in caplog.records):
                    break
            first_count = sum(
                1 for r in caplog.records if "journal unread lag" in r.message.lower()
            )
            assert first_count >= 1
            # Stay inside the rate-limit window relative to last log time.
            for _ in range(5):
                clock.advance(0.01)
                await asyncio.sleep(0.01)
            second_count = sum(
                1 for r in caplog.records if "journal unread lag" in r.message.lower()
            )
            assert second_count == first_count
            # Advance past the window → another log allowed.
            clock.advance(0.06)
            for _ in range(10):
                clock.advance(0.02)
                await asyncio.sleep(0.01)
                if (
                    sum(1 for r in caplog.records if "journal unread lag" in r.message.lower())
                    > first_count
                ):
                    break
            assert (
                sum(1 for r in caplog.records if "journal unread lag" in r.message.lower())
                > first_count
            )
    finally:
        await stream.stop()
        stream_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stream_task
