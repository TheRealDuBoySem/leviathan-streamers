"""
Decorator dispatch strategy that persists ticks to TickJournal before enqueueing.
"""

from __future__ import annotations

from core.interfaces.base import IDispatchStrategy
from core.journal.tick_journal import TickJournal
from leviathan_common.models.trade_tick import TradeTick


class JournalDispatchDecorator(IDispatchStrategy):
    """
    Wraps an inner dispatcher and appends each tick to the journal first.

    Pattern: Decorator — adds durable persistence around IDispatchStrategy.dispatch().
    """

    def __init__(self, inner: IDispatchStrategy, journal: TickJournal) -> None:
        """
        Preconditions:
            - inner must implement IDispatchStrategy.
            - journal must be a TickJournal instance.
        """
        if not isinstance(inner, IDispatchStrategy):
            raise TypeError("inner must be a IDispatchStrategy instance")
        if not isinstance(journal, TickJournal):
            raise TypeError("journal must be a TickJournal instance")
        self.__inner = inner
        self.__journal = journal

    @property
    def inner(self) -> IDispatchStrategy:
        """Return the wrapped dispatch strategy."""
        return self.__inner

    @property
    def journal(self) -> TickJournal:
        """Return the durable tick journal."""
        return self.__journal

    async def dispatch(self, tick: TradeTick) -> None:
        """
        Persist tick to the journal, then forward it to the inner dispatcher.

        Preconditions:
            - tick must be a TradeTick instance.

        Postconditions:
            - The tick is appended to the journal (deduplicated by trade_id).
            - The inner dispatcher receives the same tick instance.
        """
        if not isinstance(tick, TradeTick):
            raise TypeError(f"Expected TradeTick, got {type(tick).__name__}")
        self.__journal.append(tick)
        await self.__inner.dispatch(tick)

    async def wait_for_next_tick(self) -> TradeTick:
        """Delegate tick consumption to the inner dispatcher."""
        return await self.__inner.wait_for_next_tick()

    def mark_tick_as_processed(self) -> None:
        """Delegate processing acknowledgement to the inner dispatcher."""
        self.__inner.mark_tick_as_processed()
