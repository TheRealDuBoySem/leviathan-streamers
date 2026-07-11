import logging

import pytest

from core.routing.async_queue_dispatcher import AsyncQueueDispatcher
from leviathan_common.models.trade_tick import TradeTick


def _tick(inst_id: str = "B", trade_id: str = "1") -> TradeTick:
    return TradeTick(inst_id, 1, 1.0, 1.0, "buy", trade_id)


@pytest.mark.asyncio
async def test_dispatcher_enqueue_and_consume():
    dispatcher = AsyncQueueDispatcher(maxsize=1)
    tick = _tick()
    await dispatcher.dispatch(tick)
    assert dispatcher.qsize() == 1

    await dispatcher.dispatch(tick)
    assert dispatcher.qsize() == 1
    assert dispatcher.dropped_tick_count == 1

    retrieved = await dispatcher.wait_for_next_tick()
    assert retrieved == tick
    dispatcher.mark_tick_as_processed()
    assert dispatcher.is_empty()


@pytest.mark.asyncio
async def test_dispatcher_contracts():
    """Verify Design by Contract preconditions for AsyncQueueDispatcher."""
    with pytest.raises(ValueError, match="maxsize must be positive"):
        AsyncQueueDispatcher(maxsize=0)

    dispatcher = AsyncQueueDispatcher()
    with pytest.raises(TypeError, match="Expected TradeTick"):
        await dispatcher.dispatch("not a tick")


def test_dispatcher_types():
    """Verify type contract preconditions for AsyncQueueDispatcher."""
    with pytest.raises(TypeError, match="maxsize must be an integer"):
        AsyncQueueDispatcher(maxsize="10")


def test_dispatcher_properties():
    """Verify queue introspection helpers."""
    dispatcher = AsyncQueueDispatcher(maxsize=5)
    assert dispatcher.maxsize == 5
    assert dispatcher.is_full() is False
    assert dispatcher.dropped_tick_count == 0

    with pytest.raises(AttributeError):
        dispatcher.maxsize = 20


@pytest.mark.asyncio
async def test_dispatcher_reports_full_queue(caplog):
    dispatcher = AsyncQueueDispatcher(maxsize=1)
    tick = _tick()
    await dispatcher.dispatch(tick)
    assert dispatcher.is_full() is True

    with caplog.at_level(logging.ERROR):
        await dispatcher.dispatch(tick)

    assert dispatcher.dropped_tick_count == 1
    assert "Consumer too slow" in caplog.text
    assert "BTCUSDT" not in caplog.text
    assert "B" in caplog.text


@pytest.mark.asyncio
async def test_dispatcher_wait_for_next_tick_rejects_non_trade_tick():
    dispatcher = AsyncQueueDispatcher()
    await dispatcher.__dict__["_AsyncQueueDispatcher__queue"].put("not-a-tick")
    with pytest.raises(TypeError, match="Invariant violation: expected TradeTick"):
        await dispatcher.wait_for_next_tick()


@pytest.mark.asyncio
async def test_dispatcher_dropped_tick_count_accumulates():
    dispatcher = AsyncQueueDispatcher(maxsize=1)
    tick = _tick()
    await dispatcher.dispatch(tick)

    for _ in range(3):
        await dispatcher.dispatch(tick)

    assert dispatcher.dropped_tick_count == 3
