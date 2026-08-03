"""TDD: post-flap tip-seq correlation + heal (BB-B5-A1 / flap-without-heal)."""
from __future__ import annotations

import asyncio
import logging
import time

import pytest

from core.network.post_flap_tip_correlation import (
    BRIEF_PUBLIC_FLAP_MS_THRESHOLD,
    COLLECTOR_POST_FLAP_TIP_STALE_EXIT_CODE,
    DEFAULT_MIN_HEAL_INTERVAL_SECONDS,
    DEFAULT_POST_FLAP_TIP_STALE_SECONDS,
    PostFlapTipCorrelationMonitor,
)
from core.network.reconnecting_ws_manager import ReconnectingWebSocketManager
from core.network.retry_policy import RetryPolicy
from core.network.silence_watchdog import SilenceWatchdog
from core.network.keep_alive_emitter import KeepAliveEmitter
from core.state.post_reconnect_write_liveness import COLLECTOR_WRITE_LIVENESS_EXIT_CODE


def test_defaults_are_reasonable_and_exit_code_nonzero():
    assert DEFAULT_POST_FLAP_TIP_STALE_SECONDS == 30.0
    assert DEFAULT_MIN_HEAL_INTERVAL_SECONDS == 120.0
    assert BRIEF_PUBLIC_FLAP_MS_THRESHOLD == 5_000
    assert COLLECTOR_POST_FLAP_TIP_STALE_EXIT_CODE == 1
    assert COLLECTOR_POST_FLAP_TIP_STALE_EXIT_CODE == COLLECTOR_WRITE_LIVENESS_EXIT_CODE


def test_monitor_rejects_invalid_stale_window():
    with pytest.raises(TypeError, match="stale_window_seconds must be a number"):
        PostFlapTipCorrelationMonitor(stale_window_seconds="slow")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="stale_window_seconds must be > 0"):
        PostFlapTipCorrelationMonitor(stale_window_seconds=0)
    with pytest.raises(ValueError, match="min_heal_interval_seconds must be >= 0"):
        PostFlapTipCorrelationMonitor(min_heal_interval_seconds=-1)
    with pytest.raises(TypeError, match="min_heal_interval_seconds must be a number"):
        PostFlapTipCorrelationMonitor(min_heal_interval_seconds="x")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="on_stale must be callable"):
        PostFlapTipCorrelationMonitor(on_stale=object())  # type: ignore[arg-type]


def test_min_heal_interval_property_and_setter_validate():
    monitor = PostFlapTipCorrelationMonitor(min_heal_interval_seconds=12.5)
    assert monitor.min_heal_interval_seconds == 12.5
    with pytest.raises(TypeError, match="min_heal_interval_seconds must be a number"):
        monitor.set_min_heal_interval_seconds("bad")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="min_heal_interval_seconds must be >= 0"):
        monitor.set_min_heal_interval_seconds(-0.1)
    monitor.set_min_heal_interval_seconds(0.0)
    assert monitor.min_heal_interval_seconds == 0.0


def test_set_stale_window_seconds_validates():
    monitor = PostFlapTipCorrelationMonitor()
    with pytest.raises(ValueError, match="stale_window_seconds must be > 0"):
        monitor.set_stale_window_seconds(0)
    monitor.set_stale_window_seconds(0.05)
    assert monitor.stale_window_seconds == 0.05


def test_set_tip_seq_provider_rejects_non_callable():
    monitor = PostFlapTipCorrelationMonitor()
    with pytest.raises(TypeError, match="tip_seq_provider must be callable"):
        monitor.set_tip_seq_provider(123)  # type: ignore[arg-type]


def test_set_on_stale_rejects_non_callable():
    monitor = PostFlapTipCorrelationMonitor()
    with pytest.raises(TypeError, match="on_stale must be callable"):
        monitor.set_on_stale(123)  # type: ignore[arg-type]
    monitor.set_on_stale(None)
    monitor.set_on_stale(lambda *_: None)


def test_first_connect_without_prior_close_is_noop(caplog):
    tip = {"seq": 100}
    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=lambda: tip["seq"],
        stale_window_seconds=0.05,
        url="wss://example/ws/public",
    )
    with caplog.at_level(logging.INFO):
        monitor.note_connection_restored(
            reconnect_wall_ms=1_000,
            reconnect_mono_ms=500,
        )
    assert not any("post_flap_correlation" in r.message for r in caplog.records)
    assert not any("post_flap_tip_stale" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_flap_logs_structured_correlation_with_tip_before_after(caplog):
    tip = {"seq": 1_684_815}
    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=lambda: tip["seq"],
        stale_window_seconds=5.0,
        url="wss://example/ws/public",
        now_wall_ms=lambda: 9_999,
        now_mono_ms=lambda: 8_888,
    )

    with caplog.at_level(logging.INFO):
        monitor.note_connection_closed(close_wall_ms=1_000, close_mono_ms=100)
        tip["seq"] = 1_684_815  # unchanged across flap (Day19 signature)
        monitor.note_connection_restored(
            reconnect_wall_ms=2_300,
            reconnect_mono_ms=1_400,
        )

    messages = [r.message for r in caplog.records]
    assert any("post_flap_correlation" in m for m in messages)
    correlation = next(m for m in messages if "post_flap_correlation" in m)
    assert "close_ts_ms=1000" in correlation
    assert "reconnect_ts_ms=2300" in correlation
    assert "since_close_ms=1300" in correlation
    assert "tip_seq_before=1684815" in correlation
    assert "tip_seq_after=1684815" in correlation
    assert "wss://example/ws/public" in correlation

    monitor.cancel()


@pytest.mark.asyncio
async def test_post_flap_tip_stale_invokes_on_stale_heal_callback(caplog):
    """BB-B5-A1: frozen tip after flap is fail-fast (CRITICAL + on_stale), not telemetry-only."""
    tip = {"seq": 42}
    stale_calls: list[tuple[int, int, float]] = []
    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=lambda: tip["seq"],
        stale_window_seconds=0.05,
        url="wss://example/ws/public",
        on_stale=lambda before, now, elapsed: stale_calls.append((before, now, elapsed)),
        min_heal_interval_seconds=0.0,
    )
    monitor.note_connection_closed(close_wall_ms=10, close_mono_ms=1)
    monitor.note_connection_restored(reconnect_wall_ms=20, reconnect_mono_ms=11)

    with caplog.at_level(logging.CRITICAL):
        await asyncio.sleep(0.12)

    assert len(stale_calls) == 1
    assert stale_calls[0][0] == 42
    assert stale_calls[0][1] == 42
    assert stale_calls[0][2] >= 0.04
    assert any(
        "post_flap_tip_stale" in r.message and r.levelno >= logging.CRITICAL
        for r in caplog.records
    )
    stale = next(r.message for r in caplog.records if "post_flap_tip_stale" in r.message)
    assert "tip_seq=42" in stale
    assert "tip_seq_before=42" in stale
    assert f"exit_code={COLLECTOR_POST_FLAP_TIP_STALE_EXIT_CODE}" in stale
    assert "self-heal" in stale.lower() or "heal" in stale.lower()


@pytest.mark.asyncio
async def test_j21_h20_tip_1684815_stagnant_after_1250ms_flap_heals(caplog):
    """J21 H20 chronology: ~1.25s public flap, tip 1684815 frozen → CRITICAL heal."""
    tip = {"seq": 1_684_815}
    stale_calls: list[tuple[int, int, float]] = []
    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=lambda: tip["seq"],
        stale_window_seconds=0.05,
        url="wss://ws.bitget.com/v2/ws/public",
        on_stale=lambda before, now, elapsed: stale_calls.append((before, now, elapsed)),
        min_heal_interval_seconds=0.0,
    )

    with caplog.at_level(logging.INFO):
        monitor.note_connection_closed(close_wall_ms=20_15_14_646, close_mono_ms=100)
        monitor.note_connection_restored(
            reconnect_wall_ms=20_15_15_893,
            reconnect_mono_ms=1_350,  # +1250 ms since close
        )
        await asyncio.sleep(0.12)

    assert len(stale_calls) == 1
    assert stale_calls[0][:2] == (1_684_815, 1_684_815)
    correlation = next(r.message for r in caplog.records if "post_flap_correlation" in r.message)
    assert "since_close_ms=1250" in correlation
    assert "tip_seq_before=1684815" in correlation
    assert "tip_seq_after=1684815" in correlation
    assert any(
        "post_flap_tip_stale" in r.message
        and r.levelno >= logging.CRITICAL
        and "BB-B5-A1" in r.message
        and f"exit_code={COLLECTOR_POST_FLAP_TIP_STALE_EXIT_CODE}" in r.message
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_j22_h20_brief_1240ms_flap_stagnant_tip_detects_and_heals(caplog):
    """J22 H20 @20:28: brief ~1.24s public flap + tip stagnant → public detection + heal.

    Tip did not re-freeze on that live day; this locks the historical WS-UP/0-write path.
    """
    tip = {"seq": 1_718_560}
    stale_calls: list[tuple[int, int, float]] = []
    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=lambda: tip["seq"],
        stale_window_seconds=0.05,
        url="wss://ws.bitget.com/v2/ws/public",
        on_stale=lambda before, now, elapsed: stale_calls.append((before, now, elapsed)),
        min_heal_interval_seconds=0.0,
    )

    with caplog.at_level(logging.INFO):
        monitor.note_connection_closed(close_wall_ms=20_28_43_016, close_mono_ms=100)
        assert monitor.is_awaiting_tip_progress() is False
        monitor.note_connection_restored(
            reconnect_wall_ms=20_28_44_259,
            reconnect_mono_ms=1_340,  # +1240 ms (J22 H20)
        )

    assert monitor.last_flap_duration_ms == 1240
    assert monitor.is_awaiting_tip_progress() is True
    correlation = next(r.message for r in caplog.records if "post_flap_correlation" in r.message)
    assert "since_close_ms=1240" in correlation
    assert "brief_public_flap=True" in correlation

    with caplog.at_level(logging.CRITICAL):
        await asyncio.sleep(0.12)

    assert monitor.is_awaiting_tip_progress() is False
    assert len(stale_calls) == 1
    assert stale_calls[0][:2] == (1_718_560, 1_718_560)
    detection = monitor.consume_last_tip_stale_detection()
    assert detection is not None
    assert detection == (1_718_560, 1_718_560, stale_calls[0][2])
    assert monitor.consume_last_tip_stale_detection() is None
    assert any(
        "post_flap_tip_stale" in r.message
        and "BB-B5-A1" in r.message
        and r.levelno >= logging.CRITICAL
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_j22_h21_brief_1300ms_flap_tip_advances_no_stale(caplog):
    """J22 H21 ~1.3s public flap where tip advances in grace → no heal."""
    tip = {"seq": 1_719_171}
    stale_calls: list[object] = []
    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=lambda: tip["seq"],
        stale_window_seconds=0.08,
        url="wss://ws.bitget.com/v2/ws/public",
        on_stale=lambda *_: stale_calls.append(True),
        min_heal_interval_seconds=0.0,
    )
    monitor.note_connection_closed(close_wall_ms=21_06_27_000, close_mono_ms=100)
    monitor.note_connection_restored(
        reconnect_wall_ms=21_06_28_300,
        reconnect_mono_ms=1_400,  # +1300 ms (J22 H21)
    )
    assert monitor.last_flap_duration_ms == 1300
    assert monitor.is_awaiting_tip_progress() is True

    tip["seq"] = 1_719_200
    assert monitor.record_tip_progress() is True
    assert monitor.is_awaiting_tip_progress() is False

    with caplog.at_level(logging.WARNING):
        await asyncio.sleep(0.15)

    assert stale_calls == []
    assert monitor.consume_last_tip_stale_detection() is None
    assert not any("post_flap_tip_stale" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_arm_uses_tip_after_as_baseline_when_tip_before_missing(caplog):
    """WS-UP / 0-write: still arm when tip only becomes readable after restore."""
    tip = {"seq": None}

    def _provider():
        return tip["seq"]

    stale_calls: list[tuple[int, int, float]] = []
    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=_provider,
        stale_window_seconds=0.05,
        on_stale=lambda before, now, elapsed: stale_calls.append((before, now, elapsed)),
        min_heal_interval_seconds=0.0,
    )
    monitor.note_connection_closed(close_wall_ms=1, close_mono_ms=1)
    tip["seq"] = 50
    monitor.note_connection_restored(reconnect_wall_ms=2, reconnect_mono_ms=2)
    assert monitor.is_awaiting_tip_progress() is True

    with caplog.at_level(logging.CRITICAL):
        await asyncio.sleep(0.12)

    assert len(stale_calls) == 1
    assert stale_calls[0][:2] == (50, 50)
    assert monitor.consume_last_tip_stale_detection() == (
        50,
        50,
        stale_calls[0][2],
    )


@pytest.mark.asyncio
async def test_post_flap_tip_stale_without_callback_still_logs_critical(caplog):
    tip = {"seq": 7}
    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=lambda: tip["seq"],
        stale_window_seconds=0.05,
        min_heal_interval_seconds=0.0,
    )
    monitor.note_connection_closed(close_wall_ms=1, close_mono_ms=1)
    monitor.note_connection_restored(reconnect_wall_ms=2, reconnect_mono_ms=2)

    with caplog.at_level(logging.CRITICAL):
        await asyncio.sleep(0.12)

    assert any(
        "post_flap_tip_stale" in r.message and r.levelno >= logging.CRITICAL
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_min_heal_interval_suppresses_repeated_on_stale(caplog):
    tip = {"seq": 3}
    stale_calls: list[object] = []
    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=lambda: tip["seq"],
        stale_window_seconds=0.05,
        min_heal_interval_seconds=60.0,
        on_stale=lambda *_: stale_calls.append(time.monotonic()),
    )
    monitor.note_connection_closed(close_wall_ms=1, close_mono_ms=1)
    monitor.note_connection_restored(reconnect_wall_ms=2, reconnect_mono_ms=2)
    with caplog.at_level(logging.WARNING):
        await asyncio.sleep(0.12)
    assert len(stale_calls) == 1

    monitor.note_connection_closed(close_wall_ms=3, close_mono_ms=3)
    monitor.note_connection_restored(reconnect_wall_ms=4, reconnect_mono_ms=4)
    with caplog.at_level(logging.WARNING):
        await asyncio.sleep(0.12)

    assert len(stale_calls) == 1
    assert any(
        "cooldown" in r.message.lower() or "heal interval" in r.message.lower()
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_no_stale_warning_when_tip_advances(caplog):
    tip = {"seq": 10}
    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=lambda: tip["seq"],
        stale_window_seconds=0.05,
    )
    monitor.note_connection_closed(close_wall_ms=1, close_mono_ms=1)
    monitor.note_connection_restored(reconnect_wall_ms=2, reconnect_mono_ms=2)
    tip["seq"] = 11

    with caplog.at_level(logging.WARNING):
        await asyncio.sleep(0.12)

    assert not any("post_flap_tip_stale" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_cancel_suppresses_pending_stale_check(caplog):
    tip = {"seq": 7}
    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=lambda: tip["seq"],
        stale_window_seconds=0.08,
    )
    monitor.note_connection_closed(close_wall_ms=1, close_mono_ms=1)
    monitor.note_connection_restored(reconnect_wall_ms=2, reconnect_mono_ms=2)
    monitor.cancel()

    with caplog.at_level(logging.WARNING):
        await asyncio.sleep(0.15)

    assert not any("post_flap_tip_stale" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_flap_without_tip_provider_still_logs_correlation(caplog):
    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=None,
        stale_window_seconds=0.05,
        url="wss://public",
    )
    with caplog.at_level(logging.INFO):
        monitor.note_connection_closed(close_wall_ms=5, close_mono_ms=5)
        monitor.note_connection_restored(reconnect_wall_ms=15, reconnect_mono_ms=15)

    assert any("post_flap_correlation" in r.message for r in caplog.records)
    correlation = next(
        r.message for r in caplog.records if "post_flap_correlation" in r.message
    )
    assert "tip_seq_before=n/a" in correlation
    assert "tip_seq_after=n/a" in correlation

    with caplog.at_level(logging.WARNING):
        await asyncio.sleep(0.12)
    # Without tip access, do not invent a stale tip warning.
    assert not any("post_flap_tip_stale" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_rearm_on_second_flap_cancels_previous_window(caplog):
    tip = {"seq": 1}
    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=lambda: tip["seq"],
        stale_window_seconds=0.2,
    )
    monitor.note_connection_closed(close_wall_ms=1, close_mono_ms=1)
    monitor.note_connection_restored(reconnect_wall_ms=2, reconnect_mono_ms=2)

    # Second flap shortly after: tip advances before the second window ends.
    monitor.note_connection_closed(close_wall_ms=3, close_mono_ms=3)
    monitor.note_connection_restored(reconnect_wall_ms=4, reconnect_mono_ms=4)
    tip["seq"] = 2

    with caplog.at_level(logging.WARNING):
        await asyncio.sleep(0.25)

    assert not any("post_flap_tip_stale" in r.message for r in caplog.records)


def test_base_stream_wires_tip_seq_provider_from_journal(tmp_path, mocker):
    """Collector journal dispatch auto-binds tip sampling on the WS manager."""
    from core.journal.journal_dispatch_decorator import JournalDispatchDecorator
    from core.journal.tick_journal import TickJournal
    from core.routing.sink_dispatch_strategy import SinkDispatchStrategy
    from exchanges.bitget.bitget_subscription_protocol import BitgetSubscriptionProtocol
    from exchanges.bitget.bitget_tick_stream import BitgetTickStream
    from exchanges.bitget.parsing.bitget_message_parser import BitgetMessageParser

    journal = TickJournal(str(tmp_path))
    dispatch = JournalDispatchDecorator(SinkDispatchStrategy(), journal)
    mgr = ReconnectingWebSocketManager(
        "wss://example/ws/public",
        RetryPolicy(max_retries=1),
        SilenceWatchdog(),
        KeepAliveEmitter(),
    )
    set_provider = mocker.spy(mgr, "set_tip_seq_provider")

    BitgetTickStream(
        network_manager=mgr,
        subscription_strategy=BitgetSubscriptionProtocol("USDT-FUTURES"),
        parsing_strategy=BitgetMessageParser.create_default(),
        dispatch_strategy=dispatch,
        symbols=["XRPUSDT"],
    )

    set_provider.assert_called_once()
    provider = set_provider.call_args.args[0]
    assert provider() == journal.latest_seq()


def test_monitor_rejects_non_callable_tip_provider_and_non_string_url():
    with pytest.raises(TypeError, match="tip_seq_provider must be callable"):
        PostFlapTipCorrelationMonitor(tip_seq_provider=object())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="url must be a string"):
        PostFlapTipCorrelationMonitor(url=123)  # type: ignore[arg-type]


def test_set_url_and_stale_window_type_errors():
    monitor = PostFlapTipCorrelationMonitor(url="wss://x")
    assert monitor.url == "wss://x"
    with pytest.raises(TypeError, match="url must be a string"):
        monitor.set_url(None)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="stale_window_seconds must be a number"):
        monitor.set_stale_window_seconds("fast")  # type: ignore[arg-type]
    monitor.set_url("wss://y")
    assert monitor.url == "wss://y"


def test_sample_tip_seq_handles_provider_errors_and_invalid_values():
    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
        stale_window_seconds=0.05,
    )
    monitor.note_connection_closed(close_wall_ms=1, close_mono_ms=1)
    # Provider raised → tip_before is None → restore logs correlation without stale arm.
    monitor.note_connection_restored(reconnect_wall_ms=2, reconnect_mono_ms=2)

    for bad in (None, "x", -1):
        m = PostFlapTipCorrelationMonitor(
            tip_seq_provider=lambda bad=bad: bad,
            stale_window_seconds=0.05,
        )
        m.note_connection_closed(close_wall_ms=1, close_mono_ms=1)
        m.note_connection_restored(reconnect_wall_ms=2, reconnect_mono_ms=2)


@pytest.mark.asyncio
async def test_stale_check_skips_when_tip_now_none_or_generation_mismatch(caplog):
    tip = {"seq": 5, "fail": False}
    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=lambda: None if tip["fail"] else tip["seq"],
        stale_window_seconds=0.05,
    )
    monitor.note_connection_closed(close_wall_ms=1, close_mono_ms=1)
    monitor.note_connection_restored(reconnect_wall_ms=2, reconnect_mono_ms=2)
    tip["fail"] = True
    with caplog.at_level(logging.WARNING):
        await asyncio.sleep(0.12)
    assert not any("post_flap_tip_stale" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_stale_elapsed_ms_falls_back_when_wall_clock_raises(caplog):
    tip = {"seq": 9}

    def _wall():
        raise RuntimeError("clock broken")

    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=lambda: tip["seq"],
        stale_window_seconds=0.05,
        now_wall_ms=_wall,
        min_heal_interval_seconds=0.0,
    )
    monitor.note_connection_closed(close_wall_ms=10, close_mono_ms=1)
    monitor.note_connection_restored(reconnect_wall_ms=20, reconnect_mono_ms=11)
    with caplog.at_level(logging.CRITICAL):
        await asyncio.sleep(0.12)
    stale = next(r.message for r in caplog.records if "post_flap_tip_stale" in r.message)
    assert "elapsed_since_reconnect_ms=50" in stale  # window_s * 1000 fallback


def test_manager_set_on_post_flap_tip_stale_rejects_non_callable():
    mgr = ReconnectingWebSocketManager(
        "wss://example/ws/public",
        RetryPolicy(max_retries=1),
        SilenceWatchdog(),
        KeepAliveEmitter(),
    )
    with pytest.raises(TypeError, match="on_stale must be callable"):
        mgr.set_on_post_flap_tip_stale(object())  # type: ignore[arg-type]
    mgr.set_on_post_flap_tip_stale(None)


@pytest.mark.asyncio
async def test_manager_post_flap_tip_stale_callback_fires():
    mgr = ReconnectingWebSocketManager(
        "wss://example/ws/public",
        RetryPolicy(max_retries=1),
        SilenceWatchdog(),
        KeepAliveEmitter(),
    )
    tip = {"seq": 99}
    stale_calls: list[tuple[int, int, float]] = []
    mgr.set_tip_seq_provider(lambda: tip["seq"])
    mgr.set_on_post_flap_tip_stale(
        lambda before, now, elapsed: stale_calls.append((before, now, elapsed))
    )
    mgr.flap_tip_monitor.set_stale_window_seconds(0.05)
    mgr.flap_tip_monitor.set_min_heal_interval_seconds(0.0)
    mgr.flap_tip_monitor.note_connection_closed(close_wall_ms=1, close_mono_ms=1)
    mgr.flap_tip_monitor.note_connection_restored(reconnect_wall_ms=2, reconnect_mono_ms=2)
    await asyncio.sleep(0.12)
    assert len(stale_calls) == 1
    assert stale_calls[0][:2] == (99, 99)


@pytest.mark.asyncio
async def test_cancel_during_stale_window_propagates_cancelled_error():
    tip = {"seq": 1}
    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=lambda: tip["seq"],
        stale_window_seconds=1.0,
    )
    monitor.note_connection_closed(close_wall_ms=1, close_mono_ms=1)
    monitor.note_connection_restored(reconnect_wall_ms=2, reconnect_mono_ms=2)
    await asyncio.sleep(0)  # start sleeping inside watchdog
    task = monitor._PostFlapTipCorrelationMonitor__watchdog_task
    assert task is not None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_stale_check_aborts_on_generation_mismatch(caplog):
    tip = {"seq": 5}
    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=lambda: tip["seq"],
        stale_window_seconds=0.05,
    )
    monitor.note_connection_closed(close_wall_ms=1, close_mono_ms=1)
    monitor.note_connection_restored(reconnect_wall_ms=2, reconnect_mono_ms=2)
    # Bump generation without cancelling the in-flight sleep task.
    monitor._PostFlapTipCorrelationMonitor__generation += 1
    with caplog.at_level(logging.WARNING):
        await asyncio.sleep(0.12)
    assert not any("post_flap_tip_stale" in r.message for r in caplog.records)


def test_record_tip_progress_false_when_not_awaiting():
    monitor = PostFlapTipCorrelationMonitor(tip_seq_provider=lambda: 10)
    assert monitor.record_tip_progress() is False


@pytest.mark.asyncio
async def test_record_tip_progress_false_when_tip_missing_or_not_advanced():
    tip = {"seq": 100}
    monitor = PostFlapTipCorrelationMonitor(
        tip_seq_provider=lambda: tip["seq"],
        stale_window_seconds=1.0,
    )
    monitor.note_connection_closed(close_wall_ms=1, close_mono_ms=1)
    monitor.note_connection_restored(reconnect_wall_ms=2, reconnect_mono_ms=2)
    assert monitor.is_awaiting_tip_progress() is True
    assert monitor.record_tip_progress() is False  # tip_now == baseline

    tip["seq"] = None
    assert monitor.record_tip_progress() is False  # tip_now is None

    monitor.cancel()
