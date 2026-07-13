"""Asynchronous queue-based trade tick dispatch strategy."""

from __future__ import annotations

import asyncio
import logging
import time
from enum import Enum

from core.interfaces.base import IDispatchStrategy
from leviathan_common.models.trade_tick import TradeTick

logger = logging.getLogger(__name__)

# Default capacity tuned for multi-symbol market data bursts; under sustained
# overload the OverflowPolicy decides which ticks survive, not unbounded growth.
_DEFAULT_MAXSIZE = 20_000
_DEFAULT_DROP_LOG_INTERVAL_SECONDS = 10.0


class OverflowPolicy(str, Enum):
    """
    Policy applied when ``dispatch`` finds the queue full.

    DROP_OLDEST (default for live market data):
        Discard the oldest queued tick and enqueue the incoming one so consumers
        keep the freshest window of ticks. Each discarded tick increments
        ``dropped_tick_count``.

    DROP_NEWEST:
        Reject the incoming tick and leave the queue unchanged (legacy behaviour).
    """

    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"


class AsyncQueueDispatcher(IDispatchStrategy):
    """
    Dispatches trade ticks via a bounded asyncio queue.

    Pattern: Strategy (concrete IDispatchStrategy implementation).

    Backpressure policy
    -------------------
    The queue is intentionally bounded. When full, ticks are dropped according to
    ``overflow_policy`` (default: DROP_OLDEST) and ``dropped_tick_count`` is
    incremented. Drops are irreversible — the counter is monotone by design so
    operators can detect sustained consumer lag. Monitor ``is_full()``,
    ``qsize()``, ``maxsize``, and ``dropped_tick_count``.

    Drop logging is rate-limited: at most one WARNING per
    ``drop_log_interval_seconds``, aggregating how many ticks were dropped in
    that window (avoids log auto-DoS under bursty overflow).
    """

    def __init__(
        self,
        maxsize: int = _DEFAULT_MAXSIZE,
        *,
        overflow_policy: OverflowPolicy = OverflowPolicy.DROP_OLDEST,
        drop_log_interval_seconds: float = _DEFAULT_DROP_LOG_INTERVAL_SECONDS,
    ) -> None:
        """
        Initialize the dispatcher.

        Preconditions:
            - maxsize must be a positive integer.
            - overflow_policy must be an OverflowPolicy.
            - drop_log_interval_seconds must be a positive number.
        """
        if not isinstance(maxsize, int):
            raise TypeError("maxsize must be an integer")
        if maxsize <= 0:
            raise ValueError(f"maxsize must be positive, got {maxsize}")
        if not isinstance(overflow_policy, OverflowPolicy):
            raise TypeError("overflow_policy must be an OverflowPolicy")
        if not isinstance(drop_log_interval_seconds, (int, float)):
            raise TypeError("drop_log_interval_seconds must be a number")
        if drop_log_interval_seconds <= 0:
            raise ValueError(
                f"drop_log_interval_seconds must be positive, got {drop_log_interval_seconds}"
            )

        self.__queue: asyncio.Queue[TradeTick] = asyncio.Queue(maxsize=maxsize)
        self.__overflow_policy = overflow_policy
        self.__drop_log_interval_seconds = float(drop_log_interval_seconds)
        self.__dropped_tick_count = 0
        self.__drops_since_last_log = 0
        self.__last_drop_log_mono: float | None = None
        self.__last_drop_symbol: str | None = None

    @property
    def maxsize(self) -> int:
        """Return the maximum size of the queue."""
        return self.__queue.maxsize

    @property
    def overflow_policy(self) -> OverflowPolicy:
        """Return the policy applied when the queue is full."""
        return self.__overflow_policy

    @property
    def drop_log_interval_seconds(self) -> float:
        """Return the minimum interval between aggregated drop WARNING logs."""
        return self.__drop_log_interval_seconds

    @property
    def dropped_tick_count(self) -> int:
        """Return the number of ticks dropped because the queue was full."""
        return self.__dropped_tick_count

    def is_full(self) -> bool:
        """Return True if the queue is full."""
        return self.__queue.full()

    def qsize(self) -> int:
        """Return the current size of the queue."""
        return self.__queue.qsize()

    def is_empty(self) -> bool:
        """Return True if the queue has no pending ticks."""
        return self.__queue.empty()

    async def dispatch(self, tick: TradeTick) -> None:
        """
        Enqueue a tick for processing.

        Preconditions:
            - tick must be a TradeTick instance.

        Postconditions:
            - The tick is available via wait_for_next_tick(), unless DROP_NEWEST
              rejected it while the queue was full (dropped_tick_count incremented).
            - Under DROP_OLDEST with a full queue, the oldest tick is discarded,
              the incoming tick is enqueued, and dropped_tick_count is incremented.
        """
        if not isinstance(tick, TradeTick):
            raise TypeError(f"Expected TradeTick, got {type(tick).__name__}")

        try:
            self.__queue.put_nowait(tick)
        except asyncio.QueueFull:
            self.__handle_overflow(tick)

    async def wait_for_next_tick(self) -> TradeTick:
        """
        Wait for and return the next trade tick from the queue.

        Postconditions:
            - Returns a TradeTick instance.
        """
        tick = await self.__queue.get()
        if not isinstance(tick, TradeTick):
            raise TypeError(
                f"Invariant violation: expected TradeTick, got {type(tick).__name__}"
            )
        return tick

    def mark_tick_as_processed(self) -> None:
        """
        Notify that a previously dequeued tick has been fully processed.

        Preconditions:
            - A tick was retrieved via wait_for_next_tick() without a matching
              mark_tick_as_processed() call since.
        """
        self.__queue.task_done()

    def __handle_overflow(self, tick: TradeTick) -> None:
        """Apply overflow policy and record a drop (private)."""
        self.__dropped_tick_count += 1
        self.__last_drop_symbol = tick.inst_id

        if self.__overflow_policy is OverflowPolicy.DROP_OLDEST:
            self.__replace_oldest_with(tick)

        self.__record_drop_for_logging()

    def __replace_oldest_with(self, tick: TradeTick) -> None:
        """Discard the oldest queued tick and enqueue ``tick`` (private)."""
        try:
            self.__queue.get_nowait()
            self.__queue.task_done()
        except asyncio.QueueEmpty:
            # Concurrent drain emptied the queue between QueueFull and here.
            pass

        try:
            self.__queue.put_nowait(tick)
        except asyncio.QueueFull:
            # Extreme concurrent refill: count already incremented; give up enqueue.
            logger.debug(
                "AsyncQueueDispatcher DROP_OLDEST refill failed "
                "qsize=%s maxsize=%s symbol=%s",
                self.__queue.qsize(),
                self.__queue.maxsize,
                tick.inst_id,
            )

    def __record_drop_for_logging(self) -> None:
        """Aggregate drop events into at most one WARNING per window (private)."""
        self.__drops_since_last_log += 1
        now = time.monotonic()
        should_log = (
            self.__last_drop_log_mono is None
            or (now - self.__last_drop_log_mono) >= self.__drop_log_interval_seconds
        )
        if not should_log:
            return

        window_seconds = (
            0.0
            if self.__last_drop_log_mono is None
            else now - self.__last_drop_log_mono
        )
        logger.warning(
            "Consumer too slow: dropped %s tick(s) over %.1fs "
            "(symbol=%s, dropped_total=%s, qsize=%s, maxsize=%s, policy=%s).",
            self.__drops_since_last_log,
            window_seconds,
            self.__last_drop_symbol,
            self.__dropped_tick_count,
            self.__queue.qsize(),
            self.__queue.maxsize,
            self.__overflow_policy.value,
        )
        logger.debug(
            "AsyncQueueDispatcher queue full qsize=%s maxsize=%s symbol=%s",
            self.__queue.qsize(),
            self.__queue.maxsize,
            self.__last_drop_symbol,
        )
        self.__drops_since_last_log = 0
        self.__last_drop_log_mono = now
