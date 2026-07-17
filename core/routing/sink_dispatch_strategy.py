"""Null-object dispatch strategy for journal-only collectors (no in-memory queue)."""

from __future__ import annotations

from core.interfaces.base import IDispatchStrategy
from leviathan_common.models.trade_tick import TradeTick


class SinkDispatchStrategy(IDispatchStrategy):
    """
    Accepts ticks without buffering them for a local consumer.

    Pattern: Null Object (IDispatchStrategy) — use when durable IPC is external
    (e.g. TickJournal via JournalDispatchDecorator) and no in-process consumer
    calls ``wait_for_next_tick()``. Avoids a dead-end bounded queue that fills
    and permanently ``drop_oldest`` under live market load.

    Consumption APIs raise ``RuntimeError`` so misuse fails fast (KI-01 / KI-06).
    """

    def __init__(self) -> None:
        self.__accepted_tick_count = 0

    @property
    def accepted_tick_count(self) -> int:
        """Return how many ticks were accepted by ``dispatch``."""
        return self.__accepted_tick_count

    def is_full(self) -> bool:
        """A sink never buffers; it is never full."""
        return False

    def qsize(self) -> int:
        """A sink never buffers; size is always 0."""
        return 0

    def is_empty(self) -> bool:
        """A sink never buffers; it is always empty."""
        return True

    async def dispatch(self, tick: TradeTick) -> None:
        """
        Accept ``tick`` without enqueueing.

        Preconditions:
            - tick must be a TradeTick instance.

        Postconditions:
            - accepted_tick_count is incremented by 1.
            - No in-memory queue retains the tick (IPC must be elsewhere).
        """
        if not isinstance(tick, TradeTick):
            raise TypeError(f"Expected TradeTick, got {type(tick).__name__}")
        self.__accepted_tick_count += 1

    async def wait_for_next_tick(self) -> TradeTick:
        """Unsupported: this strategy has no consumer queue."""
        raise RuntimeError(
            "SinkDispatchStrategy has no consumer queue; "
            "ticks are not retained for wait_for_next_tick()"
        )

    def mark_tick_as_processed(self) -> None:
        """Unsupported: this strategy has no consumer queue."""
        raise RuntimeError(
            "SinkDispatchStrategy has no consumer queue; "
            "mark_tick_as_processed() is not applicable"
        )
