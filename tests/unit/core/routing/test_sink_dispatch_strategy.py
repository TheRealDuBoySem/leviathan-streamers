"""Tests for SinkDispatchStrategy — journal-only / no-consumer dispatch sink."""

import pytest

from core.routing.sink_dispatch_strategy import SinkDispatchStrategy
from leviathan_common.models.trade_tick import TradeTick


def _tick(inst_id: str = "XRPUSDT", trade_id: str = "1") -> TradeTick:
    return TradeTick(inst_id, 1, 1.0, 1.0, "buy", trade_id)


@pytest.mark.asyncio
async def test_sink_accepts_ticks_without_queue_or_drops():
    """Collector journal-only path must not accumulate or drop ticks in-memory."""
    sink = SinkDispatchStrategy()

    for i in range(50_000):
        await sink.dispatch(_tick(trade_id=str(i)))

    assert sink.accepted_tick_count == 50_000
    assert sink.qsize() == 0
    assert sink.is_full() is False
    assert sink.is_empty() is True


@pytest.mark.asyncio
async def test_sink_dispatch_rejects_non_trade_tick():
    sink = SinkDispatchStrategy()
    with pytest.raises(TypeError, match="Expected TradeTick"):
        await sink.dispatch("not a tick")  # type: ignore[arg-type]
    assert sink.accepted_tick_count == 0


@pytest.mark.asyncio
async def test_sink_wait_for_next_tick_is_explicitly_unsupported():
    sink = SinkDispatchStrategy()
    await sink.dispatch(_tick())
    with pytest.raises(RuntimeError, match="no consumer queue"):
        await sink.wait_for_next_tick()


def test_sink_mark_tick_as_processed_is_explicitly_unsupported():
    sink = SinkDispatchStrategy()
    with pytest.raises(RuntimeError, match="no consumer queue"):
        sink.mark_tick_as_processed()


def test_sink_properties_are_read_only():
    sink = SinkDispatchStrategy()
    assert sink.accepted_tick_count == 0
    with pytest.raises(AttributeError):
        sink.accepted_tick_count = 9  # type: ignore[misc]
