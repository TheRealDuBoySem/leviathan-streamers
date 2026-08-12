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


@pytest.mark.asyncio
async def test_first_boot_mute_tip_arms_and_heals(caplog):
    """Day20 WATCH #3 / H06: first confirm post-OS with 0 journal writes → heal.

    Must not skip connect_generation=1 (false-negative that left tip 1684815 mute).
    """
    stale_calls: list[tuple[set[str], float]] = []
    guard = PostReconnectWriteLivenessGuard(
        timeout_seconds=0.05,
        min_heal_interval_seconds=0.0,
        on_stale=lambda symbols, elapsed: stale_calls.append((set(symbols), elapsed)),
    )
    assert guard.arm_count == 0

    with caplog.at_level(logging.INFO):
        guard.arm_after_subscriptions_confirmed(
            {"XRPUSDT"},
            connect_generation=1,
        )

    assert guard.arm_count == 1
    assert guard.is_awaiting_write() is True
    assert any(
        "first_boot=True" in record.message or "connect_generation=1" in record.message
        for record in caplog.records
    )

    with caplog.at_level(logging.CRITICAL):
        await asyncio.sleep(0.12)

    assert len(stale_calls) == 1
    assert stale_calls[0][0] == {"XRPUSDT"}
    assert any(
        "first_boot=True" in record.message or "connect_generation=1" in record.message
        for record in caplog.records
        if record.levelno >= logging.CRITICAL
    )
    assert any("BB-B5-A1" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_arm_never_skips_first_confirm():
    """False-negative guard: first full confirm must always open a write window."""
    stale_calls: list[object] = []
    guard = PostReconnectWriteLivenessGuard(
        timeout_seconds=0.05,
        min_heal_interval_seconds=0.0,
        on_stale=lambda *_: stale_calls.append(True),
    )
    # Explicit first-boot arm — must not be a no-op.
    guard.arm_after_subscriptions_confirmed({"XRPUSDT"}, connect_generation=1)
    assert guard.is_awaiting_write() is True
    assert guard.arm_count == 1

    await asyncio.sleep(0.12)
    assert len(stale_calls) == 1


@pytest.mark.asyncio
async def test_first_boot_then_reconnect_both_arm_independently():
    """OS first boot and a later flap reconnect each get their own write deadline."""
    stale_calls: list[set[str]] = []
    guard = PostReconnectWriteLivenessGuard(
        timeout_seconds=0.05,
        min_heal_interval_seconds=0.0,
        on_stale=lambda symbols, _elapsed: stale_calls.append(set(symbols)),
    )
    guard.arm_after_subscriptions_confirmed({"XRPUSDT"}, connect_generation=1)
    guard.record_journal_write()
    assert guard.arm_count == 1

    guard.arm_after_subscriptions_confirmed({"XRPUSDT"}, connect_generation=2)
    assert guard.arm_count == 2
    assert guard.is_awaiting_write() is True

    await asyncio.sleep(0.12)
    assert stale_calls == [{"XRPUSDT"}]


def test_arm_rejects_non_positive_connect_generation():
    guard = PostReconnectWriteLivenessGuard(timeout_seconds=1.0)
    with pytest.raises(ValueError, match="connect_generation"):
        guard.arm_after_subscriptions_confirmed({"XRPUSDT"}, connect_generation=0)
    with pytest.raises(TypeError, match="connect_generation"):
        guard.arm_after_subscriptions_confirmed(
            {"XRPUSDT"},
            connect_generation="1",  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_j22_style_post_flap_confirm_without_write_exposes_detection():
    """After brief flap re-confirm, no journal write in grace → clear public detection.

    Complements post-flap tip correlation for BB-B5-A1 (WS UP / 0 write).
    """
    stale_calls: list[tuple[set[str], float]] = []
    guard = PostReconnectWriteLivenessGuard(
        timeout_seconds=0.05,
        min_heal_interval_seconds=0.0,
        on_stale=lambda symbols, elapsed: stale_calls.append((set(symbols), elapsed)),
    )
    # Post-flap path: connect_generation >= 2 (reconnect after brief public flap).
    guard.arm_after_subscriptions_confirmed({"XRPUSDT"}, connect_generation=2)
    assert guard.is_awaiting_write() is True
    assert guard.consume_last_stale_detection() is None

    await asyncio.sleep(0.12)

    assert guard.is_awaiting_write() is False
    assert len(stale_calls) == 1
    detection = guard.consume_last_stale_detection()
    assert detection is not None
    symbols, elapsed = detection
    assert symbols == {"XRPUSDT"}
    assert elapsed >= 0.04
    assert elapsed == stale_calls[0][1]
    # Consumed once — second call clears.
    assert guard.consume_last_stale_detection() is None


@pytest.mark.asyncio
async def test_j22_style_post_flap_confirm_with_write_clears_without_detection():
    """Tip/journal write in grace after flap re-confirm → no stale detection."""
    stale_calls: list[object] = []
    guard = PostReconnectWriteLivenessGuard(
        timeout_seconds=0.08,
        min_heal_interval_seconds=0.0,
        on_stale=lambda *_: stale_calls.append(True),
    )
    guard.arm_after_subscriptions_confirmed({"XRPUSDT"}, connect_generation=2)
    guard.record_journal_write()
    assert guard.is_awaiting_write() is False

    await asyncio.sleep(0.12)
    assert stale_calls == []
    assert guard.consume_last_stale_detection() is None


@pytest.mark.asyncio
async def test_j27_h19_h20_post_flap_write_liveness_pass_at_high_connect_generation(
    caplog,
):
    """J27 H19/H20: write-liveness armed after brief public flap, then PASS.

    Live: H19 arm_count=5 connect_generation=5; H20 arm_count=6 connect_generation=6;
    first_boot=False; journal writes resume well before 45s — 0 FAIL.
    Must not cancel/skip re-arm on high connect_generation (no race with flap).
    """
    import logging

    stale_calls: list[object] = []
    guard = PostReconnectWriteLivenessGuard(
        timeout_seconds=0.08,
        min_heal_interval_seconds=0.0,
        on_stale=lambda *_: stale_calls.append(True),
    )

    # Prior arms (boot + earlier flaps) so arm_count mirrors long-lived collector.
    for gen in range(1, 5):
        guard.arm_after_subscriptions_confirmed({"XRPUSDT"}, connect_generation=gen)
        guard.record_journal_write()
    assert guard.arm_count == 4
    assert guard.is_awaiting_write() is False

    with caplog.at_level(logging.INFO):
        # H19-style post-flap confirm (connect_generation=5).
        guard.arm_after_subscriptions_confirmed({"XRPUSDT"}, connect_generation=5)
        assert guard.arm_count == 5
        assert guard.is_awaiting_write() is True
        assert any(
            "Post-confirm write-liveness armée" in r.message
            and "arm_count=5" in r.message
            and "connect_generation=5" in r.message
            and "first_boot=False" in r.message
            for r in caplog.records
        )
        guard.record_journal_write()
        assert guard.is_awaiting_write() is False

        # H20-style subsequent flap (connect_generation=6) — cancel prior, re-arm.
        guard.cancel()  # mirrors base_stream.__handle_connect on reconnect
        guard.arm_after_subscriptions_confirmed({"XRPUSDT"}, connect_generation=6)
        assert guard.arm_count == 6
        assert any(
            "arm_count=6" in r.message
            and "connect_generation=6" in r.message
            and "first_boot=False" in r.message
            for r in caplog.records
        )
        guard.record_journal_write()

    await asyncio.sleep(0.12)
    assert stale_calls == []
    assert guard.consume_last_stale_detection() is None
    assert not any(
        "write-liveness FAILED" in r.message.lower()
        or "write-liveness stale" in r.message.lower()
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_j28_h20_h21_post_flap_write_liveness_pass_at_high_connect_generation(
    caplog,
):
    """J28 H20/H21: write-liveness armed after brief public flap, then PASS.

    Live: H20 arm_count=7 connect_generation=7; H21 arm_count=8 connect_generation=8;
    first_boot=False; journal writes resume well before 45s — 0 FAIL.
    Must not cancel/skip re-arm on high connect_generation (long-lived collector).
    """
    import logging

    stale_calls: list[object] = []
    guard = PostReconnectWriteLivenessGuard(
        timeout_seconds=0.08,
        min_heal_interval_seconds=0.0,
        on_stale=lambda *_: stale_calls.append(True),
    )

    # Prior arms (boot + earlier flaps) so arm_count mirrors long-lived collector.
    for gen in range(1, 7):
        guard.arm_after_subscriptions_confirmed({"XRPUSDT"}, connect_generation=gen)
        guard.record_journal_write()
    assert guard.arm_count == 6
    assert guard.is_awaiting_write() is False

    with caplog.at_level(logging.INFO):
        # H20-style post-flap confirm (connect_generation=7).
        guard.arm_after_subscriptions_confirmed({"XRPUSDT"}, connect_generation=7)
        assert guard.arm_count == 7
        assert guard.is_awaiting_write() is True
        assert any(
            "Post-confirm write-liveness armée" in r.message
            and "arm_count=7" in r.message
            and "connect_generation=7" in r.message
            and "first_boot=False" in r.message
            for r in caplog.records
        )
        guard.record_journal_write()
        assert guard.is_awaiting_write() is False

        # H21-style subsequent flap (connect_generation=8) — cancel prior, re-arm.
        guard.cancel()  # mirrors base_stream.__handle_connect on reconnect
        guard.arm_after_subscriptions_confirmed({"XRPUSDT"}, connect_generation=8)
        assert guard.arm_count == 8
        assert any(
            "arm_count=8" in r.message
            and "connect_generation=8" in r.message
            and "first_boot=False" in r.message
            for r in caplog.records
        )
        guard.record_journal_write()

    await asyncio.sleep(0.12)
    assert stale_calls == []
    assert guard.consume_last_stale_detection() is None
    assert not any(
        "write-liveness FAILED" in r.message.lower()
        or "write-liveness stale" in r.message.lower()
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_j28_h19_private_only_flap_does_not_arm_write_liveness(caplog):
    """J28 H19 private flap ~374 ms must not arm public write-liveness alone.

    Public collector flap was ABSENT that hour; without a public sub-confirm,
    the guard stays idle (contrast H20/H21 post-flap arm).
    """
    import logging

    stale_calls: list[object] = []
    guard = PostReconnectWriteLivenessGuard(
        timeout_seconds=0.08,
        min_heal_interval_seconds=0.0,
        on_stale=lambda *_: stale_calls.append(True),
    )
    # Boot already satisfied earlier in the long-lived session.
    guard.arm_after_subscriptions_confirmed({"XRPUSDT"}, connect_generation=1)
    guard.record_journal_write()
    assert guard.arm_count == 1
    assert guard.is_awaiting_write() is False

    with caplog.at_level(logging.INFO):
        # Private flap restores (~374 ms) — no public confirm → no re-arm.
        await asyncio.sleep(0.01)

    assert guard.is_awaiting_write() is False
    assert guard.arm_count == 1
    assert not any(
        "Post-confirm write-liveness armée" in r.message and "arm_count=2" in r.message
        for r in caplog.records
    )

    await asyncio.sleep(0.12)
    assert stale_calls == []
    assert guard.consume_last_stale_detection() is None
