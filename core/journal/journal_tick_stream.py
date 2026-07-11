"""
IExchangeStream implementation that reads ticks from a durable TickJournal.

Pattern: Adapter — presents TickJournal tail-follow as IExchangeStream.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import AsyncIterator, Awaitable, Callable, List, Optional

from core.interfaces.base import IExchangeStream, IPriceObserver
from core.journal.tick_journal import TickJournal, TickJournalCursor
from leviathan_common.models.trade_tick import TradeTick

logger = logging.getLogger(__name__)


def _validate_symbol(symbol: str, param_name: str = "symbol") -> None:
    if symbol is None:
        raise ValueError(f"{param_name} cannot be empty")
    if not isinstance(symbol, str):
        raise TypeError(f"{param_name} must be a string")
    if not symbol:
        raise ValueError(f"{param_name} cannot be empty")


def _validate_initial_symbols(symbols: List[str]) -> None:
    if not isinstance(symbols, list):
        raise TypeError("symbols must be a list")
    for symbol in symbols:
        if not isinstance(symbol, str):
            raise TypeError("symbols must be strings")
        if not symbol:
            raise ValueError("symbols must be non-empty strings")


def _validate_symbols_list(symbols: List[str]) -> None:
    if symbols is None:
        raise ValueError("symbols list cannot be empty")
    if not isinstance(symbols, list):
        raise TypeError("symbols must be a list")
    if not symbols:
        raise ValueError("symbols list cannot be empty")
    for symbol in symbols:
        if not isinstance(symbol, str):
            raise TypeError("symbols must be strings")
        if not symbol:
            raise ValueError("symbols must be non-empty strings")


class JournalTickStream(IExchangeStream):
    """
    Consumes ticks from TickJournal with replay then tail-follow polling.

    Does not open a public market WebSocket — the collector process owns WS I/O.
    Symbol subscription tracks active symbols for IExchangeStream parity; the journal
    delivers every persisted tick and downstream consumers apply symbol filtering.
    """

    def __init__(
        self,
        journal: TickJournal,
        *,
        poll_interval_seconds: float = 0.05,
        symbols: Optional[List[str]] = None,
    ) -> None:
        if not isinstance(journal, TickJournal):
            raise TypeError("journal must be a TickJournal instance")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        initial_symbols = list(symbols or [])
        if initial_symbols:
            _validate_initial_symbols(initial_symbols)
        self.__journal = journal
        self.__poll_interval = float(poll_interval_seconds)
        self.__symbols = initial_symbols
        self.__stopped = False
        self.__connected = False
        self.__queue: asyncio.Queue[tuple[int, TradeTick]] = asyncio.Queue()
        self.__consumer_task: Optional[asyncio.Task] = None
        self.__incremental_reader = journal.create_incremental_reader()
        self.__observers: List[IPriceObserver] = []
        self.__on_reconnect_callbacks: List[Callable[[], Awaitable[None]]] = []
        self.__cursor = journal.load_cursor()
        self.__next_seq = self.__cursor.last_processed_seq + 1
        self.__pending_seq: Optional[int] = None

    @property
    def journal(self) -> TickJournal:
        return self.__journal

    @property
    def cursor(self) -> TickJournalCursor:
        return self.__cursor

    def register_on_reconnect(self, callback: Callable[[], Awaitable[None]]) -> None:
        if callback is None or not callable(callback):
            raise TypeError("callback must be a callable awaitable")
        if not inspect.iscoroutinefunction(callback):
            raise TypeError("callback must be an async function")
        if callback not in self.__on_reconnect_callbacks:
            self.__on_reconnect_callbacks.append(callback)

    def unregister_on_reconnect(self, callback: Callable[[], Awaitable[None]]) -> None:
        if callback is None or not callable(callback):
            raise TypeError("callback must be a callable awaitable")
        if callback in self.__on_reconnect_callbacks:
            self.__on_reconnect_callbacks.remove(callback)

    def attach_observer(self, observer: IPriceObserver) -> None:
        if not isinstance(observer, IPriceObserver):
            raise TypeError("observer must be an IPriceObserver instance")
        if observer not in self.__observers:
            self.__observers.append(observer)

    def detach_observer(self, observer: IPriceObserver) -> None:
        if not isinstance(observer, IPriceObserver):
            raise TypeError("observer must be an IPriceObserver instance")
        if observer in self.__observers:
            self.__observers.remove(observer)

    @property
    def observers(self) -> List[IPriceObserver]:
        return list(self.__observers)

    async def __notify_observers(self, tick: TradeTick) -> None:
        for observer in self.__observers:
            await observer.on_price_update(tick)

    async def start_streaming(self) -> None:
        if self.__consumer_task is not None and not self.__consumer_task.done():
            return
        self.__stopped = False
        self.__connected = True
        for callback in list(self.__on_reconnect_callbacks):
            try:
                await callback()
            except Exception as exc:
                logger.error("JournalTickStream reconnect callback failed: %s", exc, exc_info=True)
        self.__consumer_task = asyncio.create_task(self.__tail_follow_loop())

    async def __tail_follow_loop(self) -> None:
        while not self.__stopped:
            records = self.__incremental_reader.poll(self.__next_seq)
            if records:
                for seq, tick in records:
                    await self.__queue.put((seq, tick))
                    await self.__notify_observers(tick)
                    self.__next_seq = seq + 1
            else:
                await asyncio.sleep(self.__poll_interval)

    async def stop(self) -> None:
        self.__stopped = True
        self.__connected = False
        if self.__consumer_task is not None and not self.__consumer_task.done():
            self.__consumer_task.cancel()
            try:
                await self.__consumer_task
            except asyncio.CancelledError:
                pass
        self.__consumer_task = None

    def is_stopped(self) -> bool:
        return self.__stopped

    def is_connected(self) -> bool:
        return self.__connected and not self.__stopped

    async def wait_until_connected(self) -> None:
        while not self.is_connected():
            if self.is_stopped():
                raise ConnectionError("Journal tick stream stopped before connecting")
            await asyncio.sleep(0.05)

    async def subscribe_symbol(self, symbol: str) -> None:
        _validate_symbol(symbol)
        if symbol not in self.__symbols:
            self.__symbols.append(symbol)

    async def subscribe_symbols(self, symbols: List[str]) -> None:
        _validate_symbols_list(symbols)
        for symbol in symbols:
            if symbol not in self.__symbols:
                self.__symbols.append(symbol)

    async def unsubscribe_symbol(self, symbol: str) -> None:
        _validate_symbol(symbol)
        if symbol in self.__symbols:
            self.__symbols.remove(symbol)

    async def unsubscribe_symbols(self, symbols: List[str]) -> None:
        _validate_symbols_list(symbols)
        for symbol in symbols:
            if symbol in self.__symbols:
                self.__symbols.remove(symbol)

    def get_active_symbols(self) -> List[str]:
        return list(self.__symbols)

    def export_cursor_dict(self) -> dict:
        """Return the durable journal cursor snapshot for supervised checkpoints."""
        return self.__cursor.to_dict()

    def set_cursor(self, cursor: TickJournalCursor) -> None:
        if not isinstance(cursor, TickJournalCursor):
            raise TypeError("cursor must be a TickJournalCursor instance")
        if cursor.last_processed_seq < 0:
            raise ValueError("last_processed_seq must be a non-negative integer")
        self.__cursor = cursor
        self.__next_seq = cursor.last_processed_seq + 1
        self.__pending_seq = None
        self.__incremental_reader.reset_from_seq(self.__next_seq)
        self.__journal.save_cursor(cursor)

    async def wait_for_next_tick(self) -> TradeTick:
        seq, tick = await self.__queue.get()
        self.__pending_seq = seq
        return tick

    def mark_tick_as_processed(self) -> None:
        seq = self.__pending_seq
        if seq is None:
            raise RuntimeError(
                "mark_tick_as_processed called without a pending tick from wait_for_next_tick()"
            )
        if seq > self.__cursor.last_processed_seq:
            self.__cursor = TickJournalCursor(last_processed_seq=seq)
            self.__journal.save_cursor(self.__cursor)
        self.__queue.task_done()
        self.__pending_seq = None

    async def __aiter__(self) -> AsyncIterator[TradeTick]:
        while not self.is_stopped():
            try:
                yield await self.wait_for_next_tick()
            except Exception:
                if self.is_stopped():
                    break
                raise
