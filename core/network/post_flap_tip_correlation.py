"""
Post-flap tip-seq correlation telemetry (WATCH Day19 #2).

Complements BB-B5-A1 write-liveness (heal on missing journal writes): this module
only observes and correlates public WS flaps with journal tip progress so ops can
grep ``post_flap_correlation`` / ``post_flap_tip_stale`` in collector logs.

Pattern: Observer — samples tip_seq around flap close/reconnect.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from typing import Callable, Optional

logger = logging.getLogger(__name__)

DEFAULT_POST_FLAP_TIP_STALE_SECONDS = 30.0

TipSeqProvider = Callable[[], Optional[int]]


def _format_tip_seq(value: Optional[int]) -> str:
    return "n/a" if value is None else str(value)


class PostFlapTipCorrelationMonitor:
    """
    Correlate a public WS flap (close → reconnect) with journal tip progress.

    Invariants:
        - At most one post-flap tip-stale watchdog task is scheduled at a time.
        - ``note_connection_restored`` is a no-op unless a prior close was noted
          (first connect is not a flap).
        - Without a tip_seq provider, correlation still logs timestamps but never
          emits ``post_flap_tip_stale``.
    """

    def __init__(
        self,
        tip_seq_provider: Optional[TipSeqProvider] = None,
        stale_window_seconds: float = DEFAULT_POST_FLAP_TIP_STALE_SECONDS,
        *,
        url: str = "",
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

        self.__tip_seq_provider = tip_seq_provider
        self.__stale_window_seconds = float(stale_window_seconds)
        self.__url = url
        self.__now_wall_ms = now_wall_ms or (lambda: time.time_ns() // 1_000_000)
        self.__now_mono_ms = now_mono_ms or (lambda: time.monotonic_ns() // 1_000_000)

        self.__pending_close = False
        self.__tip_seq_before: Optional[int] = None
        self.__close_wall_ms: Optional[int] = None
        self.__close_mono_ms: Optional[int] = None
        self.__reconnect_wall_ms: Optional[int] = None
        self.__reconnect_mono_ms: Optional[int] = None
        self.__generation = 0
        self.__watchdog_task: Optional[asyncio.Task] = None

    @property
    def stale_window_seconds(self) -> float:
        """Return the post-flap tip-stale observation window in seconds."""
        return self.__stale_window_seconds

    @property
    def url(self) -> str:
        """Return the WebSocket URL label included in structured logs."""
        return self.__url

    def set_tip_seq_provider(self, provider: Optional[TipSeqProvider]) -> None:
        """
        Bind or clear the optional tip-seq sampler.

        Preconditions:
            - provider must be callable or None.
        """
        if provider is not None and not callable(provider):
            raise TypeError("tip_seq_provider must be callable")
        self.__tip_seq_provider = provider

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
        self.__reconnect_wall_ms = None
        self.__reconnect_mono_ms = None

    def note_connection_restored(
        self,
        *,
        reconnect_wall_ms: Optional[int] = None,
        reconnect_mono_ms: Optional[int] = None,
    ) -> None:
        """
        After a prior close, emit ``post_flap_correlation`` and arm tip-stale check.

        No-op on first connect (no prior ``note_connection_closed``).
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

        logger.info(
            "post_flap_correlation event=ws_flap_reconnect url=%s "
            "close_ts_ms=%s reconnect_ts_ms=%s close_mono_ms=%s reconnect_mono_ms=%s "
            "since_close_ms=%s tip_seq_before=%s tip_seq_after=%s",
            self.__url or "n/a",
            _format_tip_seq(self.__close_wall_ms),
            _format_tip_seq(self.__reconnect_wall_ms),
            _format_tip_seq(self.__close_mono_ms),
            _format_tip_seq(self.__reconnect_mono_ms),
            _format_tip_seq(since_close_ms),
            _format_tip_seq(tip_before),
            _format_tip_seq(tip_after),
        )

        if tip_before is None:
            return

        self.__generation += 1
        generation = self.__generation
        self.__watchdog_task = asyncio.create_task(
            self.__evaluate_tip_stale_after_window(
                generation=generation,
                tip_seq_before=tip_before,
                close_wall_ms=self.__close_wall_ms,
                reconnect_wall_ms=self.__reconnect_wall_ms,
            ),
            name="post-flap-tip-stale",
        )

    def cancel(self) -> None:
        """Cancel any pending tip-stale evaluation without logging."""
        self.__generation += 1
        task = self.__watchdog_task
        self.__watchdog_task = None
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
    ) -> None:
        try:
            await asyncio.sleep(self.__stale_window_seconds)
        except asyncio.CancelledError:
            raise

        if generation != self.__generation:
            return

        self.__watchdog_task = None
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
        if (
            reconnect_wall_ms is not None
            and self.__now_wall_ms is not None
        ):
            try:
                elapsed_ms = max(0, int(self.__now_wall_ms()) - int(reconnect_wall_ms))
            except Exception:
                elapsed_ms = int(self.__stale_window_seconds * 1000)

        logger.warning(
            "post_flap_tip_stale tip_seq=%s tip_seq_before=%s tip_seq_now=%s "
            "window_s=%.1f elapsed_since_reconnect_ms=%s close_ts_ms=%s "
            "reconnect_ts_ms=%s url=%s — flap did not restore journal tip progress",
            tip_now,
            tip_seq_before,
            tip_now,
            self.__stale_window_seconds,
            elapsed_ms,
            _format_tip_seq(close_wall_ms),
            _format_tip_seq(reconnect_wall_ms),
            self.__url or "n/a",
        )
