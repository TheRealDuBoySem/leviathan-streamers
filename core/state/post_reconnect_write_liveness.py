"""Post-confirm journal write-liveness guard (BB-B5-A1 / Day20 WATCH #3).

After public WS connect (including **first boot post-OS restart**) + full
subscription confirmation, require at least one journal write within a
configurable window. Otherwise emit a clear CRITICAL and invoke a self-heal
callback (collector non-zero exit / stop) — once per window, with a min heal
interval to avoid aggressive storms.

Also covers post-flap reconnect confirms (J22 H20/H21 brief ~1.2–1.3s flaps;
J27 H19/H20 ~1.25–1.4s with write PASS when journal resumes):
WS can be UP again with 0 journal writes — ``consume_last_stale_detection``
exposes that outcome for heal correlation.

False-negative policy: every full confirm arms a window. Never skip
``connect_generation=1`` (first boot / post-OS collector respawn).

Pattern: State — armed awaiting-write window with timeout evaluation.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from typing import Callable, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# Liquid futures typically tick within seconds; stay well below engine cold-start
# stall (~120s) so the collector can self-heal before a famine storm.
DEFAULT_WRITE_LIVENESS_TIMEOUT_SECONDS = 45.0
# Suppress repeated heal signals if the process somehow stays up after a stale.
DEFAULT_MIN_HEAL_INTERVAL_SECONDS = 120.0
COLLECTOR_WRITE_LIVENESS_EXIT_CODE = 1

OnStaleCallback = Callable[[Set[str], float], None]
StaleDetection = Tuple[Set[str], float]


class PostReconnectWriteLivenessGuard:
    """
    Arms a write-deadline after subscription confirmation; satisfied by journal writes.

    Covers first connect (post-OS / fresh collector) and every subsequent reconnect.

    Invariants:
        - At most one await-write watchdog task is scheduled at a time.
        - ``on_stale`` fires at most once per armed generation, and respects
          ``min_heal_interval_seconds`` across generations.
        - ``arm_after_subscriptions_confirmed`` never no-ops on first confirm.
    """

    def __init__(
        self,
        timeout_seconds: float = DEFAULT_WRITE_LIVENESS_TIMEOUT_SECONDS,
        *,
        min_heal_interval_seconds: float = DEFAULT_MIN_HEAL_INTERVAL_SECONDS,
        on_stale: Optional[OnStaleCallback] = None,
    ) -> None:
        if not isinstance(timeout_seconds, (int, float)):
            raise TypeError("timeout_seconds must be a number")
        if not math.isfinite(float(timeout_seconds)) or float(timeout_seconds) <= 0:
            raise ValueError("timeout_seconds must be > 0")
        if not isinstance(min_heal_interval_seconds, (int, float)):
            raise TypeError("min_heal_interval_seconds must be a number")
        if float(min_heal_interval_seconds) < 0:
            raise ValueError("min_heal_interval_seconds must be >= 0")

        self.__timeout_seconds = float(timeout_seconds)
        self.__min_heal_interval_seconds = float(min_heal_interval_seconds)
        self.__on_stale = on_stale
        self.__watchdog_task: Optional[asyncio.Task] = None
        self.__generation = 0
        self.__confirmed_symbols: Set[str] = set()
        self.__armed_at_mono: Optional[float] = None
        self.__last_heal_mono: Optional[float] = None
        self.__arm_count = 0
        self.__armed_connect_generation: Optional[int] = None
        self.__armed_first_boot = False
        self.__last_stale_detection: Optional[StaleDetection] = None

    @property
    def timeout_seconds(self) -> float:
        """Return the post-confirm write deadline in seconds."""
        return self.__timeout_seconds

    @property
    def min_heal_interval_seconds(self) -> float:
        """Return the minimum interval between self-heal signals."""
        return self.__min_heal_interval_seconds

    @property
    def arm_count(self) -> int:
        """Return how many times a write-liveness window has been armed."""
        return self.__arm_count

    def is_awaiting_write(self) -> bool:
        """True while an armed window is waiting for a journal write."""
        return self.__watchdog_task is not None and not self.__watchdog_task.done()

    def consume_last_stale_detection(self) -> Optional[StaleDetection]:
        """
        Return and clear the last write-liveness stale detection, if any.

        Returns:
            ``(confirmed_symbols, elapsed_seconds)`` when a window expired
            without a journal write (and heal was not cooldown-suppressed);
            otherwise ``None``.
        """
        detection = self.__last_stale_detection
        self.__last_stale_detection = None
        return detection

    def arm_after_subscriptions_confirmed(
        self,
        confirmed_symbols: Set[str],
        *,
        connect_generation: Optional[int] = None,
    ) -> None:
        """
        Start a write-liveness window after full subscription confirmation.

        Always arms — including ``connect_generation=1`` (first boot / post-OS).
        Cancels any previous pending window. Empty ``confirmed_symbols`` is rejected.

        Args:
            confirmed_symbols: Non-empty set of confirmed subscription symbols.
            connect_generation: 1-based WS connect counter from the stream
                (1 = first boot). Optional; when omitted, first_boot is inferred
                from ``arm_count == 0`` before this call.
        """
        if not isinstance(confirmed_symbols, set):
            raise TypeError("confirmed_symbols must be a set")
        if not confirmed_symbols:
            raise ValueError("confirmed_symbols must be a non-empty set")
        for symbol in confirmed_symbols:
            if not isinstance(symbol, str) or not symbol:
                raise ValueError("confirmed_symbols must contain non-empty strings")
        if connect_generation is not None:
            if not isinstance(connect_generation, int) or isinstance(
                connect_generation, bool
            ):
                raise TypeError("connect_generation must be an int")
            if connect_generation < 1:
                raise ValueError("connect_generation must be >= 1")

        first_boot = (
            connect_generation == 1
            if connect_generation is not None
            else self.__arm_count == 0
        )

        self.cancel()
        self.__generation += 1
        generation = self.__generation
        self.__confirmed_symbols = set(confirmed_symbols)
        self.__armed_at_mono = time.monotonic()
        self.__arm_count += 1
        self.__armed_connect_generation = connect_generation
        self.__armed_first_boot = first_boot
        self.__watchdog_task = asyncio.create_task(
            self.__evaluate_after_timeout(generation),
            name="post-reconnect-write-liveness",
        )
        logger.info(
            "Post-confirm write-liveness armée: symboles=%s timeout=%.1fs "
            "arm_count=%s connect_generation=%s first_boot=%s "
            "(covers OS-restart / Day20 WATCH#3)",
            sorted(self.__confirmed_symbols),
            self.__timeout_seconds,
            self.__arm_count,
            connect_generation if connect_generation is not None else "n/a",
            first_boot,
        )

    def record_journal_write(self) -> None:
        """
        Satisfy the armed window when a tick is persisted to the journal.

        No-op when no window is active.
        """
        if not self.is_awaiting_write():
            return
        logger.debug(
            "Post-confirm write-liveness satisfaite (écriture journal) symboles=%s "
            "arm_count=%s first_boot=%s",
            sorted(self.__confirmed_symbols),
            self.__arm_count,
            self.__armed_first_boot,
        )
        self.cancel()

    def cancel(self) -> None:
        """Cancel any pending write-liveness window without evaluating."""
        self.__generation += 1
        task = self.__watchdog_task
        self.__watchdog_task = None
        self.__confirmed_symbols = set()
        self.__armed_at_mono = None
        self.__armed_connect_generation = None
        self.__armed_first_boot = False
        if task is not None and not task.done():
            task.cancel()

    async def __evaluate_after_timeout(self, generation: int) -> None:
        try:
            await asyncio.sleep(self.__timeout_seconds)
        except asyncio.CancelledError:
            raise

        if generation != self.__generation:
            return

        confirmed = set(self.__confirmed_symbols)
        armed_at = self.__armed_at_mono
        first_boot = self.__armed_first_boot
        connect_generation = self.__armed_connect_generation
        arm_count = self.__arm_count
        self.__watchdog_task = None
        self.__confirmed_symbols = set()
        self.__armed_at_mono = None
        self.__armed_connect_generation = None
        self.__armed_first_boot = False
        elapsed = (
            float(time.monotonic() - armed_at)
            if armed_at is not None
            else self.__timeout_seconds
        )

        now = time.monotonic()
        if (
            self.__last_heal_mono is not None
            and (now - self.__last_heal_mono) < self.__min_heal_interval_seconds
        ):
            logger.warning(
                "Post-confirm write-liveness stale (aucune écriture journal après "
                "abonnement confirmé) symboles=%s elapsed=%.1fs arm_count=%s "
                "connect_generation=%s first_boot=%s — self-heal supprimé "
                "(cooldown min_heal_interval=%.1fs) pour éviter un storm",
                sorted(confirmed),
                elapsed,
                arm_count,
                connect_generation if connect_generation is not None else "n/a",
                first_boot,
                self.__min_heal_interval_seconds,
            )
            return

        self.__last_heal_mono = now
        self.__last_stale_detection = (confirmed, elapsed)
        logger.critical(
            "Post-confirm write-liveness FAILED (BB-B5-A1 / Day20 WATCH#3): "
            "aucune écriture journal dans %.1fs après abonnements confirmés=%s "
            "arm_count=%s connect_generation=%s first_boot=%s — tip mute "
            "(OS-restart / post-flap / post-confirm path, WS-UP 0-write) — "
            "signal self-heal collector (exit_code=%s)",
            elapsed,
            sorted(confirmed),
            arm_count,
            connect_generation if connect_generation is not None else "n/a",
            first_boot,
            COLLECTOR_WRITE_LIVENESS_EXIT_CODE,
        )
        if self.__on_stale is not None:
            self.__on_stale(confirmed, elapsed)
