"""Additional coverage for journal tick stream helpers and APIs."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from core.interfaces.base import IPriceObserver
from core.journal.journal_tick_stream import (
    JournalTickStream,
    _validate_initial_symbols,
    _validate_symbol,
    _validate_symbols_list,
)
from core.journal.tick_journal import TickJournal, TickJournalCursor
from leviathan_common.models.trade_tick import TradeTick


def _tick(trade_id: str = "t1") -> TradeTick:
    return TradeTick("BTCUSDT", 1000, 100.0, 1.0, "buy", trade_id)


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

    await stream.start_streaming()
    stream.unregister_on_reconnect(on_reconnect)
    with pytest.raises(TypeError, match="callable awaitable"):
        stream.unregister_on_reconnect(None)  # type: ignore[arg-type]
    await stream.stop()


@pytest.mark.asyncio
async def test_journal_tick_stream_reconnect_callback_error_is_logged(tmp_path, caplog):
    stream = JournalTickStream(TickJournal(str(tmp_path)), poll_interval_seconds=0.01)

    async def failing_callback():
        raise RuntimeError("boom")

    stream.register_on_reconnect(failing_callback)
    with caplog.at_level("ERROR"):
        await stream.start_streaming()
        await asyncio.sleep(0.02)
        await stream.stop()
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
    await stream.start_streaming()
    await stream.wait_until_connected()
    assert stream.export_cursor_dict() == {"last_processed_seq": 0}
    stream.set_cursor(TickJournalCursor(last_processed_seq=3))
    assert stream.export_cursor_dict() == {"last_processed_seq": 3}
    await stream.stop()


@pytest.mark.asyncio
async def test_journal_tick_stream_duplicate_start_is_idempotent(tmp_path):
    stream = JournalTickStream(TickJournal(str(tmp_path)), poll_interval_seconds=0.01)
    await stream.start_streaming()
    first_task = stream._JournalTickStream__consumer_task
    await stream.start_streaming()
    assert stream._JournalTickStream__consumer_task is first_task
    await stream.stop()


@pytest.mark.asyncio
async def test_journal_tick_stream_async_iteration(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("a"))
    stream = JournalTickStream(journal, poll_interval_seconds=0.01)
    await stream.start_streaming()
    ticks = []
    async for tick in stream:
        ticks.append(tick.trade_id)
        stream.mark_tick_as_processed()
        if len(ticks) == 1:
            await stream.stop()
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
    await stream.start_streaming()
    mocker.patch.object(
        stream,
        "wait_for_next_tick",
        new=AsyncMock(side_effect=RuntimeError("boom")),
    )
    with pytest.raises(RuntimeError, match="boom"):
        async for _tick in stream:
            pass
    await stream.stop()


@pytest.mark.asyncio
async def test_journal_tick_stream_wait_until_connected_waits_before_connected(tmp_path):
    stream = JournalTickStream(TickJournal(str(tmp_path)), poll_interval_seconds=0.01)

    async def connect_after_delay():
        await asyncio.sleep(0.02)
        await stream.start_streaming()

    connect_task = asyncio.create_task(connect_after_delay())
    await stream.wait_until_connected()
    await connect_task
    await stream.stop()


@pytest.mark.asyncio
async def test_journal_tick_stream_async_iteration_breaks_on_error_when_stopped(tmp_path, mocker):
    stream = JournalTickStream(TickJournal(str(tmp_path)))
    await stream.start_streaming()

    async def fail_and_stop():
        stream._JournalTickStream__stopped = True
        stream._JournalTickStream__connected = False
        raise RuntimeError("stream closed")

    mocker.patch.object(stream, "wait_for_next_tick", new=fail_and_stop)
    ticks = []
    async for tick in stream:
        ticks.append(tick.trade_id)
    assert ticks == []
