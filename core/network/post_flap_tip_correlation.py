"""
Post-flap tip-seq correlation + self-heal (BB-B5-A1 / flap-without-heal).

Complements write-liveness (heal on missing journal writes after confirm): this
module correlates public WS flaps with journal tip progress and, when the tip
does not advance in the observation window, emits ``post_flap_tip_stale`` at
CRITICAL and invokes ``on_stale`` so the collector can fail-fast (non-zero exit).

Covers brief public flaps (J22 H20 ~1.24s / H21 ~1.3s): WS reconnects quickly
but tip may stay mute (historical WS-UP / 0-write). Public detection APIs
expose awaiting state and consumable stale results for heal correlation.

Wiring note (gap, intentional under BB-B5-A1 scope): ``record_tip_progress`` is
a public early-satisfy hook; collectors may call it from the journal write path.
``ReconnectingWebSocketManager`` already wires close/restore + tip provider;
optional early-satisfy call-site is not required for deadline-based detection.

Pattern: Observer — samples tip_seq around flap close/reconnect; Command-like
heal callback on confirmed tip stagnation.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from typing import Callable, Optional, Tuple

from core.state.post_reconnect_write_liveness import (
    COLLECTOR_WRITE_LIVENESS_EXIT_CODE,
    DEFAULT_MIN_HEAL_INTERVAL_SECONDS,
)

logger = logging.getLogger(__name__)

DEFAULT_POST_FLAP_TIP_STALE_SECONDS = 30.0
# J22 H20/H21 brief flaps were ~1.2–1.3s; tag anything under this as brief_public_flap.
BRIEF_PUBLIC_FLAP_MS_THRESHOLD = 5_000
# Same non-zero exit as write-liveness so the supervised collector path is uniform.
COLLECTOR_POST_FLAP_TIP_STALE_EXIT_CODE = COLLECTOR_WRITE_LIVENESS_EXIT_CODE

TipSeqProvider = Callable[[], Optional[int]]
# tip_seq_before, tip_seq_now, elapsed_seconds_since_reconnect
OnStaleCallback = Callable[[int, int, float], None]
TipStaleDetection = Tuple[int, int, float]


def _format_tip_seq(value: Optional[int]) -> str:
    return "n/a" if value is None else str(value)


class PostFlapTipCorrelationMonitor:
    """
    Correlate a public WS flap (close → reconnect) with journal tip progress.

    When tip does not advance within ``stale_window_seconds``, log CRITICAL
    ``post_flap_tip_stale`` and invoke ``on_stale`` (self-heal), subject to
    ``min_heal_interval_seconds`` cooldown.

    Invariants:
        - At most one post-flap tip-stale watchdog task is scheduled at a time.
        - ``note_connection_restored`` is a no-op unless a prior close was noted
          (first connect is not a flap).
        - Without a readable tip baseline (before or after restore), correlation
          still logs timestamps but never emits ``post_flap_tip_stale`` / ``on_stale``.
        - When tip_before is missing but tip_after is readable, the after value is
          used as the stagnation baseline (WS-UP / 0-write after reconnect).
        - ``on_stale`` fires at most once per armed generation and respects the
          min heal interval across generations.
    """

    def __init__(
        self,
        tip_seq_provider: Optional[TipSeqProvider] = None,
        stale_window_seconds: float = DEFAULT_POST_FLAP_TIP_STALE_SECONDS,
        *,
        url: str = "",
        on_stale: Optional[OnStaleCallback] = None,
        min_heal_interval_seconds: float = DEFAULT_MIN_HEAL_INTERVAL_SECONDS,
        now_wall_ms: Optional[Callable[[], int]] = None,
        now_mono_ms: Optional[Callable[[], int]] = None,
    ) -> None:
        if tip_seq_provider is not None and not callable(tip_seq_provider):
            raise TypeError("tip_seq_provider must be callable")
        if not isinstance(stale_window_seconds, (int, float)):
            raise TypeError("stale_window_seconds must be a number")
        if not math.isfinite(float(stale_window_seconds)) or float(stale_window_seconds) <= 0:
            raise ValueError("stale_window_seconds must be > 0")
        if not isinstance(url, str):
            raise TypeError("url must be a string")
        if on_stale is not None and not callable(on_stale):
            raise TypeError("on_stale must be callable")
        if not isinstance(min_heal_interval_seconds, (int, float)):
            raise TypeError("min_heal_interval_seconds must be a number")
        if float(min_heal_interval_seconds) < 0:
            raise ValueError("min_heal_interval_seconds must be >= 0")

        self.__tip_seq_provider = tip_seq_provider
        self.__stale_window_seconds = float(stale_window_seconds)
        self.__url = url
        self.__on_stale = on_stale
        self.__min_heal_interval_seconds = float(min_heal_interval_seconds)
        self.__now_wall_ms = now_wall_ms or (lambda: time.time_ns() // 1_000_000)
        self.__now_mono_ms = now_mono_ms or (lambda: time.monotonic_ns() // 1_000_000)

        self.__pending_close = False
        self.__tip_seq_before: Optional[int] = None
        self.__tip_seq_baseline: Optional[int] = None
        self.__close_wall_ms: Optional[int] = None
        self.__close_mono_ms: Optional[int] = None
        self.__reconnect_wall_ms: Optional[int] = None
        self.__reconnect_mono_ms: Optional[int] = None
        self.__last_flap_duration_ms: Optional[int] = None
        self.__generation = 0
        self.__watchdog_task: Optional[asyncio.Task] = None
        self.__last_heal_mono: Optional[float] = None
        self.__last_tip_stale_detection: Optional[TipStaleDetection] = None

    @property
    def stale_window_seconds(self) -> float:
        """Return the post-flap tip-stale observation window in seconds."""
        return self.__stale_window_seconds

    @property
    def min_heal_interval_seconds(self) -> float:
        """Return the minimum interval between self-heal signals."""
        return self.__min_heal_interval_seconds

    @property
    def url(self) -> str:
        """Return the WebSocket URL label included in structured logs."""
        return self.__url

    @property
    def last_flap_duration_ms(self) -> Optional[int]:
        """Return mono duration of the last completed flap, or None if none yet."""
        return self.__last_flap_duration_ms

    def is_awaiting_tip_progress(self) -> bool:
        """True while an armed post-flap window is waiting for tip_seq to advance."""
        return self.__watchdog_task is not None and not self.__watchdog_task.done()

    def consume_last_tip_stale_detection(self) -> Optional[TipStaleDetection]:
        """
        Return and clear the last tip-stale detection, if any.

        Returns:
            ``(tip_seq_before, tip_seq_now, elapsed_seconds)`` when a stale
            window completed without tip progress; otherwise ``None``.
        """
        detection = self.__last_tip_stale_detection
        self.__last_tip_stale_detection = None
        return detection

    def record_tip_progress(self) -> bool:
        """
        Satisfy the armed window early when journal tip_seq has advanced.

        Returns:
            True if an awaiting window was satisfied and cancelled; False if
            no window was active or tip has not advanced past the baseline.
        """
        if not self.is_awaiting_tip_progress():
            return False
        baseline = self.__tip_seq_baseline
        if baseline is None:  # pragma: no cover — cleared only with cancel (task gone)
            return False
        tip_now = self.__sample_tip_seq()
        if tip_now is None or tip_now <= baseline:
            return False
        logger.debug(
            "post_flap_correlation tip advanced early tip_seq_before=%s tip_seq_now=%s",
            baseline,
            tip_now,
        )
        self.cancel()
        return True

    def set_tip_seq_provider(self, provider: Optional[TipSeqProvider]) -> None:
        """
        Bind or clear the optional tip-seq sampler.

        Preconditions:
            - provider must be callable or None.
        """
        if provider is not None and not callable(provider):
            raise TypeError("tip_seq_provider must be callable")
        self.__tip_seq_provider = provider

    def set_on_stale(self, callback: Optional[OnStaleCallback]) -> None:
        """
        Bind or clear the self-heal callback invoked on post-flap tip stagnation.

        Preconditions:
            - callback must be callable or None.
        """
        if callback is not None and not callable(callback):
            raise TypeError("on_stale must be callable")
        self.__on_stale = callback

    def set_stale_window_seconds(self, seconds: float) -> None:
        """
        Update the post-flap tip-stale observation window.

        Preconditions:
            - seconds must be a strictly positive finite number.
        """
        if not isinstance(seconds, (int, float)):
            raise TypeError("stale_window_seconds must be a number")
        if not math.isfinite(float(seconds)) or float(seconds) <= 0:
            raise ValueError("stale_window_seconds must be > 0")
        self.__stale_window_seconds = float(seconds)

    def set_min_heal_interval_seconds(self, seconds: float) -> None:
        """
        Update the minimum interval between self-heal signals.

        Preconditions:
            - seconds must be a finite number >= 0.
        """
        if not isinstance(seconds, (int, float)):
            raise TypeError("min_heal_interval_seconds must be a number")
        if float(seconds) < 0:
            raise ValueError("min_heal_interval_seconds must be >= 0")
        self.__min_heal_interval_seconds = float(seconds)

    def set_url(self, url: str) -> None:
        """Update the URL label used in structured flap logs."""
        if not isinstance(url, str):
            raise TypeError("url must be a string")
        self.__url = url

    def note_connection_closed(
        self,
        *,
        close_wall_ms: Optional[int] = None,
        close_mono_ms: Optional[int] = None,
    ) -> None:
        """
        Record a WebSocket close that may become a flap once reconnect succeeds.

        Samples tip_seq_before when a provider is configured.
        """
        self.cancel()
        self.__pending_close = True
        self.__close_wall_ms = (
            int(close_wall_ms) if close_wall_ms is not None else self.__now_wall_ms()
        )
        self.__close_mono_ms = (
            int(close_mono_ms) if close_mono_ms is not None else self.__now_mono_ms()
        )
        self.__tip_seq_before = self.__sample_tip_seq()
        self.__tip_seq_baseline = None
        self.__reconnect_wall_ms = None
        self.__reconnect_mono_ms = None
        self.__last_flap_duration_ms = None

    def note_connection_restored(
        self,
        *,
        reconnect_wall_ms: Optional[int] = None,
        reconnect_mono_ms: Optional[int] = None,
    ) -> None:
        """
        After a prior close, emit ``post_flap_correlation`` and arm tip-stale check.

        No-op on first connect (no prior ``note_connection_closed``).
        Arms when tip_before is readable, or when tip_after alone is readable
        (baseline = tip_after) so brief flaps still catch WS-UP / 0-write mute.
        """
        if not self.__pending_close:
            return

        self.__pending_close = False
        self.__reconnect_wall_ms = (
            int(reconnect_wall_ms)
            if reconnect_wall_ms is not None
            else self.__now_wall_ms()
        )
        self.__reconnect_mono_ms = (
            int(reconnect_mono_ms)
            if reconnect_mono_ms is not None
            else self.__now_mono_ms()
        )
        tip_after = self.__sample_tip_seq()
        tip_before = self.__tip_seq_before
        since_close_ms = (
            self.__reconnect_mono_ms - self.__close_mono_ms
            if self.__close_mono_ms is not None
            else None
        )
        self.__last_flap_duration_ms = since_close_ms
        brief_public_flap = (
            since_close_ms is not None and 0 <= since_close_ms < BRIEF_PUBLIC_FLAP_MS_THRESHOLD
        )

        logger.info(
            "post_flap_correlation event=ws_flap_reconnect url=%s "
            "close_ts_ms=%s reconnect_ts_ms=%s close_mono_ms=%s reconnect_mono_ms=%s "
            "since_close_ms=%s brief_public_flap=%s tip_seq_before=%s tip_seq_after=%s",
            self.__url or "n/a",
            _format_tip_seq(self.__close_wall_ms),
            _format_tip_seq(self.__reconnect_wall_ms),
            _format_tip_seq(self.__close_mono_ms),
            _format_tip_seq(self.__reconnect_mono_ms),
            _format_tip_seq(since_close_ms),
            brief_public_flap,
            _format_tip_seq(tip_before),
            _format_tip_seq(tip_after),
        )

        baseline = tip_before if tip_before is not None else tip_after
        if baseline is None:
            return

        self.__tip_seq_baseline = baseline
        self.__generation += 1
        generation = self.__generation
        self.__watchdog_task = asyncio.create_task(
            self.__evaluate_tip_stale_after_window(
                generation=generation,
                tip_seq_before=baseline,
                close_wall_ms=self.__close_wall_ms,
                reconnect_wall_ms=self.__reconnect_wall_ms,
                brief_public_flap=brief_public_flap,
            ),
            name="post-flap-tip-stale",
        )

    def cancel(self) -> None:
        """Cancel any pending tip-stale evaluation without logging."""
        self.__generation += 1
        task = self.__watchdog_task
        self.__watchdog_task = None
        self.__tip_seq_baseline = None
        if task is not None and not task.done():
            task.cancel()

    def __sample_tip_seq(self) -> Optional[int]:
        if self.__tip_seq_provider is None:
            return None
        try:
            value = self.__tip_seq_provider()
        except Exception as exc:
            logger.debug("post_flap_correlation tip_seq_provider failed: %s", exc)
            return None
        if value is None:
            return None
        try:
            seq = int(value)
        except (TypeError, ValueError):
            return None
        if seq < 0:
            return None
        return seq

    async def __evaluate_tip_stale_after_window(
        self,
        *,
        generation: int,
        tip_seq_before: int,
        close_wall_ms: Optional[int],
        reconnect_wall_ms: Optional[int],
        brief_public_flap: bool = False,
    ) -> None:
        try:
            await asyncio.sleep(self.__stale_window_seconds)
        except asyncio.CancelledError:
            raise

        if generation != self.__generation:
            return

        self.__watchdog_task = None
        self.__tip_seq_baseline = None
        tip_now = self.__sample_tip_seq()
        if tip_now is None:
            return
        if tip_now > tip_seq_before:
            logger.debug(
                "post_flap_correlation tip advanced tip_seq_before=%s tip_seq_now=%s",
                tip_seq_before,
                tip_now,
            )
            return

        elapsed_ms = int(self.__stale_window_seconds * 1000)
        if reconnect_wall_ms is not None and self.__now_wall_ms is not None:
            try:
                elapsed_ms = max(0, int(self.__now_wall_ms()) - int(reconnect_wall_ms))
            except Exception:
                elapsed_ms = int(self.__stale_window_seconds * 1000)
        elapsed_seconds = float(elapsed_ms) / 1000.0

        now_mono = time.monotonic()
        if (
            self.__last_heal_mono is not None
            and (now_mono - self.__last_heal_mono) < self.__min_heal_interval_seconds
        ):
            logger.warning(
                "post_flap_tip_stale tip_seq=%s tip_seq_before=%s tip_seq_now=%s "
                "window_s=%.1f elapsed_since_reconnect_ms=%s close_ts_ms=%s "
                "reconnect_ts_ms=%s brief_public_flap=%s url=%s — self-heal suppressed "
                "(cooldown min_heal_interval=%.1fs) to avoid a storm",
                tip_now,
                tip_seq_before,
                tip_now,
                self.__stale_window_seconds,
                elapsed_ms,
                _format_tip_seq(close_wall_ms),
                _format_tip_seq(reconnect_wall_ms),
                brief_public_flap,
                self.__url or "n/a",
                self.__min_heal_interval_seconds,
            )
            return

        self.__last_heal_mono = now_mono
        self.__last_tip_stale_detection = (tip_seq_before, tip_now, elapsed_seconds)
        logger.critical(
            "post_flap_tip_stale tip_seq=%s tip_seq_before=%s tip_seq_now=%s "
            "window_s=%.1f elapsed_since_reconnect_ms=%s close_ts_ms=%s "
            "reconnect_ts_ms=%s brief_public_flap=%s url=%s — flap did not restore "
            "journal tip progress (BB-B5-A1 / WS-UP 0-write) — signal self-heal "
            "collector (exit_code=%s)",
            tip_now,
            tip_seq_before,
            tip_now,
            self.__stale_window_seconds,
            elapsed_ms,
            _format_tip_seq(close_wall_ms),
            _format_tip_seq(reconnect_wall_ms),
            brief_public_flap,
            self.__url or "n/a",
            COLLECTOR_POST_FLAP_TIP_STALE_EXIT_CODE,
        )
        if self.__on_stale is not None:
            self.__on_stale(tip_seq_before, tip_now, elapsed_seconds)
