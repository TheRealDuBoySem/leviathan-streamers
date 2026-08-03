"""TDD: post-reconnect journal write-liveness guard (BB-B5-A1)."""
from __future__ import annotations

import asyncio
import logging
import time

import pytest

from core.state.post_reconnect_write_liveness import (
    COLLECTOR_WRITE_LIVENESS_EXIT_CODE,
    DEFAULT_MIN_HEAL_INTERVAL_SECONDS,
    DEFAULT_WRITE_LIVENESS_TIMEOUT_SECONDS,
    PostReconnectWriteLivenessGuard,
)


def test_defaults_are_reasonable_and_exit_code_nonzero():
    assert DEFAULT_WRITE_LIVENESS_TIMEOUT_SECONDS == 45.0
    assert DEFAULT_MIN_HEAL_INTERVAL_SECONDS == 120.0
    assert COLLECTOR_WRITE_LIVENESS_EXIT_CODE == 1


def test_guard_rejects_invalid_timeouts():
    with pytest.raises(TypeError, match="timeout_seconds must be a number"):
        PostReconnectWriteLivenessGuard(timeout_seconds="slow")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="timeout_seconds must be > 0"):
        PostReconnectWriteLivenessGuard(timeout_seconds=0)
    with pytest.raises(ValueError, match="min_heal_interval_seconds must be >= 0"):
        PostReconnectWriteLivenessGuard(min_heal_interval_seconds=-1)


@pytest.mark.asyncio
async def test_arm_then_write_satisfies_without_stale_callback():
    stale_calls: list[object] = []
    guard = PostReconnectWriteLivenessGuard(
        timeout_seconds=0.2,
        on_stale=lambda symbols, elapsed: stale_calls.append((symbols, elapsed)),
    )
    guard.arm_after_subscriptions_confirmed({"XRPUSDT"})
    assert guard.is_awaiting_write() is True

    guard.record_journal_write()
    assert guard.is_awaiting_write() is False

    await asyncio.sleep(0.25)
    assert stale_calls == []


@pytest.mark.asyncio
async def test_arm_without_write_fires_stale_once(caplog):
    stale_calls: list[tuple[set[str], float]] = []
    guard = PostReconnectWriteLivenessGuard(
        timeout_seconds=0.05,
        on_stale=lambda symbols, elapsed: stale_calls.append((set(symbols), elapsed)),
    )
    guard.arm_after_subscriptions_confirmed({"XRPUSDT", "BTCUSDT"})

    with caplog.at_level(logging.CRITICAL):
        await asyncio.sleep(0.12)

    assert len(stale_calls) == 1
    assert stale_calls[0][0] == {"XRPUSDT", "BTCUSDT"}
    assert stale_calls[0][1] >= 0.04
    assert guard.is_awaiting_write() is False
    assert any(
        "write-liveness" in record.message.lower()
        or "aucune écriture journal" in record.message.lower()
        for record in caplog.records
    )
    assert any(record.levelno >= logging.CRITICAL for record in caplog.records)


@pytest.mark.asyncio
async def test_cancel_prevents_stale_callback():
    stale_calls: list[object] = []
    guard = PostReconnectWriteLivenessGuard(
        timeout_seconds=0.08,
        on_stale=lambda *_: stale_calls.append(True),
    )
    guard.arm_after_subscriptions_confirmed({"XRPUSDT"})
    guard.cancel()
    assert guard.is_awaiting_write() is False

    await asyncio.sleep(0.15)
    assert stale_calls == []


@pytest.mark.asyncio
async def test_rearm_cancels_previous_window():
    stale_calls: list[set[str]] = []
    guard = PostReconnectWriteLivenessGuard(
        timeout_seconds=0.2,
        on_stale=lambda symbols, elapsed: stale_calls.append(set(symbols)),
    )
    guard.arm_after_subscriptions_confirmed({"OLD"})
    guard.arm_after_subscriptions_confirmed({"NEW"})
    guard.record_journal_write()

    await asyncio.sleep(0.25)
    assert stale_calls == []


@pytest.mark.asyncio
async def test_min_heal_interval_suppresses_storm(caplog):
    """Second stale within cooldown logs WARNING but does not re-invoke on_stale."""
    stale_calls: list[object] = []
    guard = PostReconnectWriteLivenessGuard(
        timeout_seconds=0.05,
        min_heal_interval_seconds=60.0,
        on_stale=lambda *_: stale_calls.append(time.monotonic()),
    )
    guard.arm_after_subscriptions_confirmed({"XRPUSDT"})
    with caplog.at_level(logging.WARNING):
        await asyncio.sleep(0.12)
    assert len(stale_calls) == 1

    guard.arm_after_subscriptions_confirmed({"XRPUSDT"})
    with caplog.at_level(logging.WARNING):
        await asyncio.sleep(0.12)

    assert len(stale_calls) == 1
    assert any(
        "cooldown" in record.message.lower() or "heal interval" in record.message.lower()
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_record_write_without_arm_is_noop():
    guard = PostReconnectWriteLivenessGuard(timeout_seconds=0.05)
    guard.record_journal_write()
    assert guard.is_awaiting_write() is False


def test_arm_rejects_empty_symbols():
    guard = PostReconnectWriteLivenessGuard(timeout_seconds=1.0)
    with pytest.raises(ValueError, match="confirmed_symbols"):
        guard.arm_after_subscriptions_confirmed(set())


def test_guard_rejects_invalid_min_heal_interval_type():
    with pytest.raises(TypeError, match="min_heal_interval_seconds must be a number"):
        PostReconnectWriteLivenessGuard(min_heal_interval_seconds="x")  # type: ignore[arg-type]


def test_guard_exposes_timeout_and_min_heal_properties():
    guard = PostReconnectWriteLivenessGuard(
        timeout_seconds=12.5,
        min_heal_interval_seconds=33.0,
    )
    assert guard.timeout_seconds == 12.5
    assert guard.min_heal_interval_seconds == 33.0


def test_arm_rejects_non_set_and_blank_symbol():
    guard = PostReconnectWriteLivenessGuard(timeout_seconds=1.0)
    with pytest.raises(TypeError, match="confirmed_symbols must be a set"):
        guard.arm_after_subscriptions_confirmed(["XRPUSDT"])  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="non-empty strings"):
        guard.arm_after_subscriptions_confirmed({""})


@pytest.mark.asyncio
async def test_evaluate_cancelled_error_propagates():
    guard = PostReconnectWriteLivenessGuard(timeout_seconds=1.0)
    guard.arm_after_subscriptions_confirmed({"XRPUSDT"})
    await asyncio.sleep(0)  # enter sleep inside evaluate
    task = guard._PostReconnectWriteLivenessGuard__watchdog_task
    assert task is not None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_stale_generation_mismatch_is_noop():
    stale_calls: list[object] = []
    guard = PostReconnectWriteLivenessGuard(
        timeout_seconds=0.05,
        on_stale=lambda *_: stale_calls.append(True),
    )
    guard.arm_after_subscriptions_confirmed({"XRPUSDT"})
    # Bump generation without cancelling the sleeping task's await path cleanly:
    # cancel() increments generation; after cancel the old task should no-op on wake.
    task = guard._PostReconnectWriteLivenessGuard__watchdog_task
    guard._PostReconnectWriteLivenessGuard__generation += 1
    guard._PostReconnectWriteLivenessGuard__watchdog_task = None
    if task is not None:
        await asyncio.sleep(0.12)
    assert stale_calls == []
