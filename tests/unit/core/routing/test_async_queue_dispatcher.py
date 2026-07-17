import asyncio
import logging
import time

import pytest

from core.routing.async_queue_dispatcher import AsyncQueueDispatcher, OverflowPolicy
from leviathan_common.models.trade_tick import TradeTick


def _tick(inst_id: str = "B", trade_id: str = "1") -> TradeTick:
    return TradeTick(inst_id, 1, 1.0, 1.0, "buy", trade_id)


@pytest.mark.asyncio
async def test_dispatcher_enqueue_and_consume():
    dispatcher = AsyncQueueDispatcher(maxsize=1, overflow_policy=OverflowPolicy.DROP_NEWEST)
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

    with pytest.raises(ValueError, match="drop_log_interval_seconds must be positive"):
        AsyncQueueDispatcher(drop_log_interval_seconds=0)

    with pytest.raises(TypeError, match="overflow_policy must be an OverflowPolicy"):
        AsyncQueueDispatcher(overflow_policy="drop_oldest")

    dispatcher = AsyncQueueDispatcher()
    with pytest.raises(TypeError, match="Expected TradeTick"):
        await dispatcher.dispatch("not a tick")


def test_dispatcher_types():
    """Verify type contract preconditions for AsyncQueueDispatcher."""
    with pytest.raises(TypeError, match="maxsize must be an integer"):
        AsyncQueueDispatcher(maxsize="10")
    with pytest.raises(TypeError, match="drop_log_interval_seconds must be a number"):
        AsyncQueueDispatcher(drop_log_interval_seconds="slow")  # type: ignore[arg-type]


def test_dispatcher_properties():
    """Verify queue introspection helpers."""
    dispatcher = AsyncQueueDispatcher(maxsize=5)
    assert dispatcher.maxsize == 5
    assert dispatcher.is_full() is False
    assert dispatcher.dropped_tick_count == 0
    assert dispatcher.overflow_policy is OverflowPolicy.DROP_OLDEST
    assert dispatcher.drop_log_interval_seconds == 10.0
    assert dispatcher.saturation_error_after_seconds == 60.0
    assert dispatcher.saturation_error_drop_threshold == 1_000
    assert dispatcher.saturation_duration_seconds is None

    with pytest.raises(AttributeError):
        dispatcher.maxsize = 20


@pytest.mark.asyncio
async def test_drop_oldest_handles_concurrent_empty_and_full(monkeypatch):
    dispatcher = AsyncQueueDispatcher(
        maxsize=1,
        overflow_policy=OverflowPolicy.DROP_OLDEST,
    )
    await dispatcher.dispatch(_tick(trade_id="old"))

    queue = dispatcher.__dict__["_AsyncQueueDispatcher__queue"]

    def _empty_get():
        raise asyncio.QueueEmpty()

    monkeypatch.setattr(queue, "get_nowait", _empty_get)
    await dispatcher.dispatch(_tick(trade_id="new"))
    assert dispatcher.dropped_tick_count >= 1

    # Refill failure path after QueueFull race.
    await dispatcher.dispatch(_tick(trade_id="a"))

    def _full_put(_tick_arg):
        raise asyncio.QueueFull()

    monkeypatch.setattr(queue, "get_nowait", lambda: _tick(trade_id="drain"))
    monkeypatch.setattr(queue, "task_done", lambda: None)
    monkeypatch.setattr(queue, "put_nowait", _full_put)
    await dispatcher.dispatch(_tick(trade_id="race"))
    assert dispatcher.dropped_tick_count >= 2


@pytest.mark.asyncio
async def test_dispatcher_reports_full_queue(caplog):
    dispatcher = AsyncQueueDispatcher(
        maxsize=1,
        overflow_policy=OverflowPolicy.DROP_NEWEST,
    )
    tick = _tick()
    await dispatcher.dispatch(tick)
    assert dispatcher.is_full() is True

    with caplog.at_level(logging.WARNING):
        await dispatcher.dispatch(tick)

    assert dispatcher.dropped_tick_count == 1
    assert "Consumer too slow" in caplog.text
    assert "BTCUSDT" not in caplog.text
    assert "B" in caplog.text
    assert len([r for r in caplog.records if "Consumer too slow" in r.message]) == 1


@pytest.mark.asyncio
async def test_dispatcher_wait_for_next_tick_rejects_non_trade_tick():
    dispatcher = AsyncQueueDispatcher()
    await dispatcher.__dict__["_AsyncQueueDispatcher__queue"].put("not-a-tick")
    with pytest.raises(TypeError, match="Invariant violation: expected TradeTick"):
        await dispatcher.wait_for_next_tick()


@pytest.mark.asyncio
async def test_dispatcher_dropped_tick_count_accumulates():
    dispatcher = AsyncQueueDispatcher(
        maxsize=1,
        overflow_policy=OverflowPolicy.DROP_NEWEST,
    )
    tick = _tick()
    await dispatcher.dispatch(tick)

    for _ in range(3):
        await dispatcher.dispatch(tick)

    assert dispatcher.dropped_tick_count == 3


@pytest.mark.asyncio
async def test_drop_oldest_keeps_newest_tick():
    """When full, drop stale queued tick and enqueue the incoming one."""
    dispatcher = AsyncQueueDispatcher(
        maxsize=1,
        overflow_policy=OverflowPolicy.DROP_OLDEST,
    )
    stale = _tick(inst_id="XRPUSDT", trade_id="old")
    fresh = _tick(inst_id="XRPUSDT", trade_id="new")

    await dispatcher.dispatch(stale)
    await dispatcher.dispatch(fresh)

    assert dispatcher.dropped_tick_count == 1
    assert dispatcher.qsize() == 1
    retrieved = await dispatcher.wait_for_next_tick()
    assert retrieved is fresh
    assert retrieved.trade_id == "new"


@pytest.mark.asyncio
async def test_drop_newest_rejects_incoming_tick():
    """Legacy policy: keep queued ticks, drop the incoming one."""
    dispatcher = AsyncQueueDispatcher(
        maxsize=1,
        overflow_policy=OverflowPolicy.DROP_NEWEST,
    )
    queued = _tick(inst_id="XRPUSDT", trade_id="queued")
    incoming = _tick(inst_id="XRPUSDT", trade_id="incoming")

    await dispatcher.dispatch(queued)
    await dispatcher.dispatch(incoming)

    assert dispatcher.dropped_tick_count == 1
    retrieved = await dispatcher.wait_for_next_tick()
    assert retrieved is queued


@pytest.mark.asyncio
async def test_drop_logs_are_throttled_per_interval(caplog, monkeypatch):
    """One WARNING per window aggregating drops — not one log line per dropped tick."""
    clock = {"now": 1000.0}
    monkeypatch.setattr(time, "monotonic", lambda: clock["now"])

    dispatcher = AsyncQueueDispatcher(
        maxsize=1,
        overflow_policy=OverflowPolicy.DROP_NEWEST,
        drop_log_interval_seconds=5.0,
    )
    tick = _tick(inst_id="XRPUSDT", trade_id="1")
    await dispatcher.dispatch(tick)

    with caplog.at_level(logging.WARNING):
        await dispatcher.dispatch(tick)  # first drop → immediate WARNING
        clock["now"] = 1001.0
        await dispatcher.dispatch(tick)  # same window → silent
        await dispatcher.dispatch(tick)  # same window → silent
        clock["now"] = 1005.0
        await dispatcher.dispatch(tick)  # new window → aggregated WARNING

    drop_records = [r for r in caplog.records if "Consumer too slow" in r.message]
    assert len(drop_records) == 2
    assert dispatcher.dropped_tick_count == 4
    assert "dropped 1 tick" in drop_records[0].message or "dropped 1 " in drop_records[0].message
    assert "dropped 3 tick" in drop_records[1].message or "dropped 3 " in drop_records[1].message
    assert all(r.levelno == logging.WARNING for r in drop_records)
    assert "dropped_total=4" in drop_records[1].message
    assert "XRPUSDT" in drop_records[1].message
    assert "DROP_NEWEST" in drop_records[1].message or "drop_newest" in drop_records[1].message


@pytest.mark.asyncio
async def test_drop_oldest_preserves_queue_task_done_invariant():
    """Discarding the oldest tick must not leave unfinished_tasks unbalanced."""
    dispatcher = AsyncQueueDispatcher(
        maxsize=2,
        overflow_policy=OverflowPolicy.DROP_OLDEST,
    )
    t1 = _tick(trade_id="1")
    t2 = _tick(trade_id="2")
    t3 = _tick(trade_id="3")

    await dispatcher.dispatch(t1)
    await dispatcher.dispatch(t2)
    await dispatcher.dispatch(t3)  # drops t1, keeps t2 then t3

    assert dispatcher.dropped_tick_count == 1
    first = await dispatcher.wait_for_next_tick()
    second = await dispatcher.wait_for_next_tick()
    dispatcher.mark_tick_as_processed()
    dispatcher.mark_tick_as_processed()
    assert first is t2
    assert second is t3
    assert dispatcher.is_empty()
    # join must return promptly if unfinished_tasks stayed consistent
    await asyncio.wait_for(
        dispatcher.__dict__["_AsyncQueueDispatcher__queue"].join(),
        timeout=0.1,
    )


@pytest.mark.asyncio
async def test_sustained_saturation_escalates_to_error(caplog, monkeypatch):
    """Prolonged overflow must escalate from WARNING to ERROR (capacity signal)."""
    clock = {"now": 1000.0}
    monkeypatch.setattr(time, "monotonic", lambda: clock["now"])

    dispatcher = AsyncQueueDispatcher(
        maxsize=1,
        overflow_policy=OverflowPolicy.DROP_NEWEST,
        drop_log_interval_seconds=5.0,
        saturation_error_after_seconds=30.0,
        saturation_error_drop_threshold=10_000,
    )
    tick = _tick(inst_id="XRPUSDT", trade_id="1")
    await dispatcher.dispatch(tick)

    with caplog.at_level(logging.WARNING):
        await dispatcher.dispatch(tick)  # first drop → WARNING
        clock["now"] = 1030.0
        await dispatcher.dispatch(tick)  # saturated 30s → ERROR

    warnings = [r for r in caplog.records if "Consumer too slow" in r.message]
    errors = [r for r in caplog.records if "Queue saturation sustained" in r.message]
    assert len(warnings) == 1
    assert warnings[0].levelno == logging.WARNING
    assert len(errors) == 1
    assert errors[0].levelno == logging.ERROR
    assert "saturated_for=30.0s" in errors[0].message
    assert "dropped_total=2" in errors[0].message
    assert "XRPUSDT" in errors[0].message


@pytest.mark.asyncio
async def test_drop_threshold_escalates_to_error(caplog, monkeypatch):
    """Crossing dropped_total threshold escalates even before duration threshold."""
    clock = {"now": 5000.0}
    monkeypatch.setattr(time, "monotonic", lambda: clock["now"])

    dispatcher = AsyncQueueDispatcher(
        maxsize=1,
        overflow_policy=OverflowPolicy.DROP_NEWEST,
        drop_log_interval_seconds=1.0,
        saturation_error_after_seconds=3600.0,
        saturation_error_drop_threshold=3,
    )
    tick = _tick(inst_id="XRPUSDT", trade_id="1")
    await dispatcher.dispatch(tick)

    with caplog.at_level(logging.WARNING):
        await dispatcher.dispatch(tick)  # drop 1 → WARNING
        clock["now"] = 5001.0
        await dispatcher.dispatch(tick)  # drop 2 → WARNING (under threshold)
        clock["now"] = 5002.0
        await dispatcher.dispatch(tick)  # drop 3 → ERROR

    errors = [r for r in caplog.records if "Queue saturation sustained" in r.message]
    assert len(errors) == 1
    assert errors[0].levelno == logging.ERROR
    assert "dropped_total=3" in errors[0].message


@pytest.mark.asyncio
async def test_successful_enqueue_clears_saturation_timer(monkeypatch):
    """Draining below capacity resets sustained-saturation tracking."""
    clock = {"now": 1000.0}
    monkeypatch.setattr(time, "monotonic", lambda: clock["now"])

    dispatcher = AsyncQueueDispatcher(
        maxsize=1,
        overflow_policy=OverflowPolicy.DROP_NEWEST,
        saturation_error_after_seconds=10.0,
    )
    await dispatcher.dispatch(_tick(trade_id="a"))
    await dispatcher.dispatch(_tick(trade_id="drop"))  # starts saturation
    assert dispatcher.saturation_duration_seconds == 0.0

    clock["now"] = 1005.0
    assert dispatcher.saturation_duration_seconds == pytest.approx(5.0)

    retrieved = await dispatcher.wait_for_next_tick()
    dispatcher.mark_tick_as_processed()
    assert retrieved.trade_id == "a"

    await dispatcher.dispatch(_tick(trade_id="b"))  # succeeds → clear timer
    assert dispatcher.saturation_duration_seconds is None


def test_dispatcher_saturation_contracts():
    with pytest.raises(ValueError, match="saturation_error_after_seconds must be positive"):
        AsyncQueueDispatcher(saturation_error_after_seconds=0)
    with pytest.raises(ValueError, match="saturation_error_drop_threshold must be positive"):
        AsyncQueueDispatcher(saturation_error_drop_threshold=0)
    with pytest.raises(TypeError, match="saturation_error_after_seconds must be a number"):
        AsyncQueueDispatcher(saturation_error_after_seconds="slow")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="saturation_error_drop_threshold must be an integer"):
        AsyncQueueDispatcher(saturation_error_drop_threshold=1.5)  # type: ignore[arg-type]


def test_should_escalate_returns_false_when_saturation_timer_unset():
    """Defensive branch: no saturation episode and under drop threshold."""
    dispatcher = AsyncQueueDispatcher(saturation_error_drop_threshold=10_000)
    assert (
        dispatcher._AsyncQueueDispatcher__should_escalate_to_error(time.monotonic())
        is False
    )
