"""Additional coverage for journal tick stream helpers and APIs."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from core.interfaces.base import IPriceObserver
from core.journal.journal_tick_stream import (
    JournalStreamFatalError,
    JournalTickStream,
    _validate_initial_symbols,
    _validate_symbol,
    _validate_symbols_list,
)
from core.journal.tick_journal import TickJournal
from core.journal.tick_journal_cursor import TickJournalCursor
from leviathan_common.models.trade_tick import TradeTick


def _tick(trade_id: str = "t1", inst_id: str = "BTCUSDT") -> TradeTick:
    return TradeTick(inst_id, 1000, 100.0, 1.0, "buy", trade_id)


async def _run_stream(stream: JournalTickStream):
    task = asyncio.create_task(stream.start_streaming())
    await asyncio.sleep(0)
    return task


async def _stop_stream(stream: JournalTickStream, task: asyncio.Task) -> None:
    await stream.stop()
    await task


def test_validate_symbol_contracts():
    with pytest.raises(ValueError, match="cannot be empty"):
        _validate_symbol(None)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="must be a string"):
        _validate_symbol(123)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="cannot be empty"):
        _validate_symbol("")


def test_validate_initial_symbols_contracts():
    with pytest.raises(TypeError, match="symbols must be a list"):
        _validate_initial_symbols("BTC")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="symbols must be strings"):
        _validate_initial_symbols([123])  # type: ignore[list-item]
    with pytest.raises(ValueError, match="non-empty strings"):
        _validate_initial_symbols([""])


def test_validate_symbols_list_contracts():
    with pytest.raises(ValueError, match="cannot be empty"):
        _validate_symbols_list(None)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="symbols must be a list"):
        _validate_symbols_list("BTC")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="cannot be empty"):
        _validate_symbols_list([])


def test_journal_tick_stream_rejects_invalid_poll_interval(tmp_path):
    with pytest.raises(ValueError, match="poll_interval_seconds must be positive"):
        JournalTickStream(TickJournal(str(tmp_path)), poll_interval_seconds=0)


def test_journal_tick_stream_rejects_invalid_initial_symbols(tmp_path):
    with pytest.raises(ValueError, match="non-empty strings"):
        JournalTickStream(TickJournal(str(tmp_path)), symbols=[""])


def test_journal_tick_stream_rejects_invalid_on_stream_fatal(tmp_path):
    with pytest.raises(TypeError, match="on_stream_fatal must be callable"):
        JournalTickStream(TickJournal(str(tmp_path)), on_stream_fatal="bad")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_journal_tick_stream_on_stream_fatal_callback_error_is_logged(tmp_path, mocker, caplog):
    journal = TickJournal(str(tmp_path))
    reader = journal.create_incremental_reader()
    mocker.patch.object(journal, "create_incremental_reader", return_value=reader)
    mocker.patch.object(reader, "poll", side_effect=OSError("persistent read failure"))

    def failing_callback(_reason: str) -> None:
        raise RuntimeError("fatal callback boom")

    stream = JournalTickStream(
        journal,
        poll_interval_seconds=0.001,
        on_stream_fatal=failing_callback,
    )

    with caplog.at_level("ERROR"):
        with pytest.raises(JournalStreamFatalError):
            await stream.start_streaming()

    assert "on_stream_fatal callback failed" in caplog.text.lower()


@pytest.mark.asyncio
async def test_journal_tick_stream_filters_ticks_by_active_symbols(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("btc", inst_id="BTCUSDT"))
    journal.append(_tick("eth", inst_id="ETHUSDT"))

    stream = JournalTickStream(journal, poll_interval_seconds=0.01, symbols=["BTCUSDT"])
    stream_task = await _run_stream(stream)

    tick = await stream.wait_for_next_tick()
    assert tick.trade_id == "btc"
    stream.mark_tick_as_processed()
    assert stream.cursor.last_processed_seq == 2

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(stream.wait_for_next_tick(), timeout=0.05)

    await _stop_stream(stream, stream_task)


@pytest.mark.asyncio
async def test_journal_tick_stream_reconnect_callbacks(tmp_path):
    stream = JournalTickStream(TickJournal(str(tmp_path)))

    async def on_reconnect():
        return None

    stream.register_on_reconnect(on_reconnect)
    with pytest.raises(TypeError, match="async function"):
        stream.register_on_reconnect(lambda: None)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="callable awaitable"):
        stream.register_on_reconnect(None)  # type: ignore[arg-type]

    stream_task = await _run_stream(stream)
    stream.unregister_on_reconnect(on_reconnect)
    with pytest.raises(TypeError, match="callable awaitable"):
        stream.unregister_on_reconnect(None)  # type: ignore[arg-type]
    await _stop_stream(stream, stream_task)


@pytest.mark.asyncio
async def test_journal_tick_stream_reconnect_callback_error_is_logged(tmp_path, caplog):
    stream = JournalTickStream(TickJournal(str(tmp_path)), poll_interval_seconds=0.01)

    async def failing_callback():
        raise RuntimeError("boom")

    stream.register_on_reconnect(failing_callback)
    with caplog.at_level("ERROR"):
        stream_task = await _run_stream(stream)
        await asyncio.sleep(0.02)
        await _stop_stream(stream, stream_task)
    assert "reconnect callback failed" in caplog.text.lower()


@pytest.mark.asyncio
async def test_journal_tick_stream_subscription_management(tmp_path):
    stream = JournalTickStream(TickJournal(str(tmp_path)), symbols=["BTCUSDT"])
    await stream.subscribe_symbol("ETHUSDT")
    await stream.subscribe_symbols(["SOLUSDT"])
    assert set(stream.get_active_symbols()) == {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
    await stream.unsubscribe_symbol("BTCUSDT")
    await stream.unsubscribe_symbols(["SOLUSDT"])
    assert stream.get_active_symbols() == ["ETHUSDT"]


def test_journal_tick_stream_detach_observer_and_properties(tmp_path):
    stream = JournalTickStream(TickJournal(str(tmp_path)))
    observer = AsyncMock(spec=IPriceObserver)
    stream.attach_observer(observer)
    assert stream.observers == [observer]
    with pytest.raises(TypeError, match="IPriceObserver"):
        stream.detach_observer(object())  # type: ignore[arg-type]
    stream.detach_observer(observer)
    assert stream.observers == []


@pytest.mark.asyncio
async def test_journal_tick_stream_wait_until_connected_and_cursor_export(tmp_path):
    journal = TickJournal(str(tmp_path))
    stream = JournalTickStream(journal)
    stream_task = await _run_stream(stream)
    await stream.wait_until_connected()
    assert stream.export_cursor_dict() == {"last_processed_seq": 0}
    stream.set_cursor(TickJournalCursor(last_processed_seq=3))
    assert stream.export_cursor_dict() == {"last_processed_seq": 3}
    await _stop_stream(stream, stream_task)


@pytest.mark.asyncio
async def test_journal_tick_stream_duplicate_start_is_idempotent(tmp_path):
    stream = JournalTickStream(TickJournal(str(tmp_path)), poll_interval_seconds=0.01)
    stream_task = await _run_stream(stream)
    await asyncio.sleep(0.01)
    await stream.start_streaming()
    assert stream.is_streaming() is True
    await _stop_stream(stream, stream_task)


@pytest.mark.asyncio
async def test_journal_tick_stream_async_iteration(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("a"))
    stream = JournalTickStream(journal, poll_interval_seconds=0.01)
    stream_task = await _run_stream(stream)
    ticks = []
    async for tick in stream:
        ticks.append(tick.trade_id)
        stream.mark_tick_as_processed()
        if len(ticks) == 1:
            await stream.stop()
    await stream_task
    assert ticks == ["a"]


@pytest.mark.asyncio
async def test_journal_tick_stream_stopped_before_connect_raises(tmp_path):
    stream = JournalTickStream(TickJournal(str(tmp_path)))
    await stream.stop()
    with pytest.raises(ConnectionError, match="stopped before connecting"):
        await stream.wait_until_connected()


def test_journal_tick_stream_rejects_non_tick_journal(tmp_path):
    with pytest.raises(TypeError, match="journal must be a TickJournal"):
        JournalTickStream(object())  # type: ignore[arg-type]


def test_journal_tick_stream_journal_property(tmp_path):
    journal = TickJournal(str(tmp_path))
    stream = JournalTickStream(journal)
    assert stream.journal is journal


@pytest.mark.asyncio
async def test_journal_tick_stream_set_cursor_rejects_invalid_type(tmp_path):
    stream = JournalTickStream(TickJournal(str(tmp_path)))
    with pytest.raises(TypeError, match="TickJournalCursor"):
        stream.set_cursor({"bad": 1})  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_journal_tick_stream_validate_symbols_list_empty_symbol(tmp_path):
    stream = JournalTickStream(TickJournal(str(tmp_path)))
    with pytest.raises(ValueError, match="non-empty strings"):
        await stream.subscribe_symbols(["BTCUSDT", ""])
    with pytest.raises(TypeError, match="symbols must be strings"):
        await stream.subscribe_symbols([123])  # type: ignore[list-item]


@pytest.mark.asyncio
async def test_journal_tick_stream_async_iteration_reraises_active_errors(tmp_path, mocker):
    stream = JournalTickStream(TickJournal(str(tmp_path)))
    stream_task = await _run_stream(stream)
    mocker.patch.object(
        stream,
        "wait_for_next_tick",
        new=AsyncMock(side_effect=RuntimeError("boom")),
    )
    with pytest.raises(RuntimeError, match="boom"):
        async for _tick in stream:
            pass
    await _stop_stream(stream, stream_task)


@pytest.mark.asyncio
async def test_journal_tick_stream_wait_until_connected_waits_before_connected(tmp_path):
    stream = JournalTickStream(TickJournal(str(tmp_path)), poll_interval_seconds=0.01)
    stream_task = None

    async def connect_after_delay():
        nonlocal stream_task
        await asyncio.sleep(0.02)
        stream_task = await _run_stream(stream)

    connect_task = asyncio.create_task(connect_after_delay())
    await stream.wait_until_connected()
    await connect_task
    assert stream_task is not None
    await _stop_stream(stream, stream_task)


@pytest.mark.asyncio
async def test_journal_tick_stream_async_iteration_breaks_on_error_when_stopped(tmp_path, mocker):
    stream = JournalTickStream(TickJournal(str(tmp_path)))
    stream_task = await _run_stream(stream)

    async def fail_and_stop():
        await stream.stop()
        raise RuntimeError("stream closed")

    mocker.patch.object(stream, "wait_for_next_tick", new=fail_and_stop)
    ticks = []
    async for tick in stream:
        ticks.append(tick.trade_id)
    await stream_task
    assert ticks == []


@pytest.mark.asyncio
async def test_journal_tick_stream_tail_follow_continues_after_poll_error(tmp_path, mocker, caplog):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("t1"))
    reader = journal.create_incremental_reader()
    real_poll = reader.poll
    poll_calls = {"count": 0}

    def flaky_poll(next_seq: int):
        poll_calls["count"] += 1
        if poll_calls["count"] == 1:
            raise OSError("journal read failed")
        return real_poll(next_seq)

    mocker.patch.object(journal, "create_incremental_reader", return_value=reader)
    mocker.patch.object(reader, "poll", side_effect=flaky_poll)
    stream = JournalTickStream(journal, poll_interval_seconds=0.01)

    stream_task = await _run_stream(stream)
    await asyncio.sleep(0.05)
    await _stop_stream(stream, stream_task)

    assert poll_calls["count"] >= 2
    assert any("JournalTickStream tail-follow error" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_journal_tick_stream_raises_fatal_after_repeated_poll_failures(tmp_path, mocker):
    journal = TickJournal(str(tmp_path))
    reader = journal.create_incremental_reader()
    mocker.patch.object(journal, "create_incremental_reader", return_value=reader)
    mocker.patch.object(
        reader,
        "poll",
        side_effect=OSError("persistent read failure"),
    )
    stream = JournalTickStream(journal, poll_interval_seconds=0.001)

    with pytest.raises(JournalStreamFatalError, match="tail_follow_exhausted"):
        await stream.start_streaming()


@pytest.mark.asyncio
async def test_journal_tick_stream_tail_follow_propagates_cancelled_error(tmp_path):
    journal = TickJournal(str(tmp_path))
    stream = JournalTickStream(journal, poll_interval_seconds=3600.0)
    stream_task = asyncio.create_task(stream.start_streaming())
    await asyncio.sleep(0.01)
    stream_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stream_task
