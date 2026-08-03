"""
Decorator dispatch strategy that persists ticks to TickJournal before enqueueing.
"""

from __future__ import annotations

from typing import Optional, Protocol

from core.interfaces.base import IDispatchStrategy
from core.journal.tick_journal import TickJournal
from leviathan_common.models.trade_tick import TradeTick


class _WriteLivenessSink(Protocol):
    def record_journal_write(self) -> None:
        """Notify that a tick was persisted to the journal."""


class JournalDispatchDecorator(IDispatchStrategy):
    """
    Wraps an inner dispatcher and appends each tick to the journal first.

    Pattern: Decorator — adds durable persistence around IDispatchStrategy.dispatch().
    """

    def __init__(
        self,
        inner: IDispatchStrategy,
        journal: TickJournal,
        write_liveness_guard: Optional[_WriteLivenessSink] = None,
    ) -> None:
        """
        Preconditions:
            - inner must implement IDispatchStrategy.
            - journal must be a TickJournal instance.
            - write_liveness_guard, if provided, must expose record_journal_write().
        """
        if not isinstance(inner, IDispatchStrategy):
            raise TypeError("inner must be a IDispatchStrategy instance")
        if not isinstance(journal, TickJournal):
            raise TypeError("journal must be a TickJournal instance")
        if write_liveness_guard is not None and not callable(
            getattr(write_liveness_guard, "record_journal_write", None)
        ):
            raise TypeError(
                "write_liveness_guard must provide a callable record_journal_write method"
            )
        self.__inner = inner
        self.__journal = journal
        self.__write_liveness_guard = write_liveness_guard

    @property
    def inner(self) -> IDispatchStrategy:
        """Return the wrapped dispatch strategy."""
        return self.__inner

    @property
    def journal(self) -> TickJournal:
        """Return the durable tick journal."""
        return self.__journal

    @property
    def write_liveness_guard(self) -> Optional[_WriteLivenessSink]:
        """Return the optional post-reconnect write-liveness guard."""
        return self.__write_liveness_guard

    async def dispatch(self, tick: TradeTick) -> None:
        """
        Persist tick to the journal, then forward it to the inner dispatcher.

        Preconditions:
            - tick must be a TradeTick instance.

        Postconditions:
            - The tick is appended to the journal (deduplicated by trade_id).
            - The inner dispatcher receives the same tick instance.
            - When a write-liveness guard is attached, it is notified after append.
        """
        if not isinstance(tick, TradeTick):
            raise TypeError(f"Expected TradeTick, got {type(tick).__name__}")
        self.__journal.append(tick)
        if self.__write_liveness_guard is not None:
            self.__write_liveness_guard.record_journal_write()
        await self.__inner.dispatch(tick)

    async def wait_for_next_tick(self) -> TradeTick:
        """Delegate tick consumption to the inner dispatcher."""
        return await self.__inner.wait_for_next_tick()

    def mark_tick_as_processed(self) -> None:
        """Delegate processing acknowledgement to the inner dispatcher."""
        self.__inner.mark_tick_as_processed()
