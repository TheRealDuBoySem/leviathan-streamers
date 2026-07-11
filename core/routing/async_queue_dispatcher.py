"""Asynchronous queue-based trade tick dispatch strategy."""

from __future__ import annotations

import asyncio
import logging

from core.interfaces.base import IDispatchStrategy
from leviathan_common.models.trade_tick import TradeTick

logger = logging.getLogger(__name__)


class AsyncQueueDispatcher(IDispatchStrategy):
    """
    Dispatches trade ticks via a bounded asyncio queue.

    Pattern: Strategy (concrete IDispatchStrategy implementation).

    When the queue is full, incoming ticks are dropped and ``dropped_tick_count``
    is incremented. Consumers should monitor ``is_full()`` and ``dropped_tick_count``.
    """

    def __init__(self, maxsize: int = 10000) -> None:
        """
        Initialize the dispatcher.

        Preconditions:
            - maxsize must be a positive integer.
        """
        if not isinstance(maxsize, int):
            raise TypeError("maxsize must be an integer")
        if maxsize <= 0:
            raise ValueError(f"maxsize must be positive, got {maxsize}")

        self.__queue: asyncio.Queue[TradeTick] = asyncio.Queue(maxsize=maxsize)
        self.__dropped_tick_count = 0

    @property
    def maxsize(self) -> int:
        """Return the maximum size of the queue."""
        return self.__queue.maxsize

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
            - The tick is available via wait_for_next_tick(), unless the queue was
              already full (tick dropped, dropped_tick_count incremented).
        """
        if not isinstance(tick, TradeTick):
            raise TypeError(f"Expected TradeTick, got {type(tick).__name__}")

        try:
            self.__queue.put_nowait(tick)
        except asyncio.QueueFull:
            self.__dropped_tick_count += 1
            logger.error(
                "Consumer too slow: tick dropped (symbol=%s, dropped_total=%s).",
                tick.inst_id,
                self.__dropped_tick_count,
            )
            logger.debug(
                "AsyncQueueDispatcher queue full qsize=%s maxsize=%s symbol=%s",
                self.__queue.qsize(),
                self.__queue.maxsize,
                tick.inst_id,
            )

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
