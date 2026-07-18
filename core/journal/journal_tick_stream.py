"""
IExchangeStream implementation that reads ticks from a durable TickJournal.

Pattern: Adapter — presents TickJournal tail-follow as IExchangeStream.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from typing import AsyncIterator, Awaitable, Callable, List, Optional

from core.interfaces.base import IExchangeStream, IPriceObserver
from core.journal.tick_journal import (
    DEFAULT_EMPTY_POLL_DIAGNOSTIC_SECONDS,
    TickJournal,
    TickJournalCursor,
)
from leviathan_common.models.trade_tick import TradeTick

logger = logging.getLogger(__name__)

_TAIL_FOLLOW_FATAL_THRESHOLD = 10


class JournalStreamFatalError(RuntimeError):
    """Raised when the journal tail-follow loop cannot continue safely."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def is_eof_caught_up_progress_snapshot(snapshot: dict) -> bool:
    """
    Return True when an empty poll is idle EOF wait, not unread lag (D5-07 / D6-A03).

    The D6 pre-restart storm logged WARNING while already showing
    ``offset==size``, ``lag_seq=0``, ``incomplete_stuck=False`` (sometimes with
    stale ``latest_seq < next_seq``). That signature must never be WARNING.
    """
    try:
        read_offset = int(snapshot["read_offset"])
        journal_size = int(snapshot["journal_size"])
        lag_seq = int(snapshot["lag_seq"])
        incomplete_stuck = bool(snapshot["incomplete_stuck"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(
            "progress snapshot must expose read_offset, journal_size, "
            f"lag_seq, incomplete_stuck as numeric/bool fields: {exc}"
        ) from exc
    return read_offset >= journal_size and lag_seq == 0 and not incomplete_stuck


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
    ``start_streaming()`` blocks in the tail-follow loop (same contract as WS streams)
    so orchestrators can attach a FATAL guard to the streaming task.
    """

    def __init__(
        self,
        journal: TickJournal,
        *,
        poll_interval_seconds: float = 0.05,
        symbols: Optional[List[str]] = None,
        on_stream_fatal: Optional[Callable[[str], None]] = None,
        empty_poll_diagnostic_seconds: float = DEFAULT_EMPTY_POLL_DIAGNOSTIC_SECONDS,
        clock: Optional[Callable[[], float]] = None,
        incomplete_record_max_wait_seconds: Optional[float] = None,
    ) -> None:
        if not isinstance(journal, TickJournal):
            raise TypeError("journal must be a TickJournal instance")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if empty_poll_diagnostic_seconds <= 0:
            raise ValueError("empty_poll_diagnostic_seconds must be positive")
        if on_stream_fatal is not None and not callable(on_stream_fatal):
            raise TypeError("on_stream_fatal must be callable")
        if clock is not None and not callable(clock):
            raise TypeError("clock must be callable")
        initial_symbols = list(symbols or [])
        if initial_symbols:
            _validate_initial_symbols(initial_symbols)
        self.__journal = journal
        self.__poll_interval = float(poll_interval_seconds)
        self.__empty_poll_diagnostic_seconds = float(empty_poll_diagnostic_seconds)
        self.__clock: Callable[[], float] = clock if clock is not None else time.monotonic
        self.__symbols = initial_symbols
        self.__stopped = False
        self.__connected = False
        self.__streaming = False
        self.__queue: asyncio.Queue[tuple[int, TradeTick]] = asyncio.Queue()
        reader_kwargs = {}
        if incomplete_record_max_wait_seconds is not None:
            reader_kwargs["incomplete_record_max_wait_seconds"] = (
                incomplete_record_max_wait_seconds
            )
        if clock is not None:
            reader_kwargs["clock"] = clock
        self.__incremental_reader = journal.create_incremental_reader(**reader_kwargs)
        self.__observers: List[IPriceObserver] = []
        self.__on_reconnect_callbacks: List[Callable[[], Awaitable[None]]] = []
        self.__cursor = journal.load_cursor()
        self.__next_seq = self.__cursor.last_processed_seq + 1
        self.__pending_seq: Optional[int] = None
        self.__on_stream_fatal = on_stream_fatal
        self.__empty_poll_since: Optional[float] = None
        self.__last_unread_lag_log_at: Optional[float] = None

    @property
    def journal(self) -> TickJournal:
        return self.__journal

    @property
    def cursor(self) -> TickJournalCursor:
        return TickJournalCursor(last_processed_seq=self.__cursor.last_processed_seq)

    def get_invalid_line_skip_count(self) -> int:
        """Return lifetime invalid journal lines skipped by the incremental reader."""
        return self.__incremental_reader.get_invalid_line_skip_count()

    def get_consecutive_parse_failures(self) -> int:
        """Return current streak of skipped invalid lines since the last valid record."""
        return self.__incremental_reader.get_consecutive_parse_failures()

    def get_read_progress_snapshot(self) -> dict:
        """Return journal reader offset/size/lag snapshot (D4-04 observability)."""
        return self.__incremental_reader.get_read_progress_snapshot()

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

    def __notify_stream_fatal(self, reason: str) -> None:
        if self.__on_stream_fatal is None:
            return
        try:
            self.__on_stream_fatal(reason)
        except Exception as exc:
            logger.error(
                "JournalTickStream on_stream_fatal callback failed: %s",
                exc,
                exc_info=True,
            )

    def __is_tick_for_active_subscription(self, tick: TradeTick) -> bool:
        if not self.__symbols:
            return True
        return tick.inst_id in self.__symbols

    def __persist_cursor_through(self, seq: int) -> None:
        if seq > self.__cursor.last_processed_seq:
            self.__cursor = TickJournalCursor(last_processed_seq=seq)
            self.__journal.save_cursor(self.__cursor)

    async def start_streaming(self) -> None:
        if self.__streaming:
            return
        self.__streaming = True
        self.__stopped = False
        self.__connected = True
        try:
            for callback in list(self.__on_reconnect_callbacks):
                try:
                    await callback()
                except Exception as exc:
                    logger.error(
                        "JournalTickStream reconnect callback failed: %s",
                        exc,
                        exc_info=True,
                    )
            await self.__tail_follow_loop()
        finally:
            self.__streaming = False
            self.__connected = False

    async def __tail_follow_loop(self) -> None:
        consecutive_errors = 0
        while not self.__stopped:
            try:
                records = self.__incremental_reader.poll(self.__next_seq)
                if records:
                    consecutive_errors = 0
                    self.__empty_poll_since = None
                    self.__last_unread_lag_log_at = None
                    for seq, tick in records:
                        self.__next_seq = seq + 1
                        if not self.__is_tick_for_active_subscription(tick):
                            self.__persist_cursor_through(seq)
                            continue
                        await self.__queue.put((seq, tick))
                        await self.__notify_observers(tick)
                    logger.debug(
                        "JournalTickStream read %s records; cursor now seq=%s",
                        len(records),
                        self.__next_seq,
                    )
                else:
                    self.__maybe_log_unread_lag()
                    await asyncio.sleep(self.__poll_interval)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                consecutive_errors += 1
                logger.error(
                    "JournalTickStream tail-follow error (attempt %s): %s",
                    consecutive_errors,
                    exc,
                    exc_info=True,
                )
                if consecutive_errors >= _TAIL_FOLLOW_FATAL_THRESHOLD:
                    reason = "tail_follow_exhausted"
                    logger.critical(
                        "JournalTickStream tail-follow failed %s times consecutively",
                        consecutive_errors,
                    )
                    self.__notify_stream_fatal(reason)
                    self.__stopped = True
                    raise JournalStreamFatalError(reason) from exc
                await asyncio.sleep(min(self.__poll_interval * consecutive_errors, 5.0))

    def __maybe_log_unread_lag(self) -> None:
        """D4-04: if still waiting, log journal offset/size/lag after N seconds.

        EOF caught-up (``is_eof_caught_up_progress_snapshot``) is DEBUG only —
        never the WARNING « unread lag » wording (D5-07 / D6-A03). Real unread
        lag / sticky incomplete tip / byte cursor behind EOF stays WARNING.
        """
        now = self.__clock()
        if self.__empty_poll_since is None:
            self.__empty_poll_since = now
            return
        waited = now - self.__empty_poll_since
        if waited < self.__empty_poll_diagnostic_seconds:
            return
        if (
            self.__last_unread_lag_log_at is not None
            and (now - self.__last_unread_lag_log_at) < self.__empty_poll_diagnostic_seconds
        ):
            return
        snapshot = self.__incremental_reader.get_read_progress_snapshot()
        detail = (
            "(waited=%.1fs, offset=%s, size=%s, next_seq=%s, latest_seq=%s, "
            "lag_seq=%s, incomplete_stuck=%s)"
        )
        args = (
            waited,
            snapshot["read_offset"],
            snapshot["journal_size"],
            snapshot["next_seq"],
            snapshot["latest_seq"],
            snapshot["lag_seq"],
            snapshot["incomplete_stuck"],
        )
        if is_eof_caught_up_progress_snapshot(snapshot):
            logger.debug(
                "JournalTickStream waiting for new journal records at EOF " + detail,
                *args,
            )
        else:
            logger.warning(
                "JournalTickStream journal unread lag while waiting for ticks " + detail,
                *args,
            )
        self.__last_unread_lag_log_at = now

    async def stop(self) -> None:
        self.__stopped = True
        self.__connected = False

    def is_stopped(self) -> bool:
        return self.__stopped

    def is_streaming(self) -> bool:
        """Return True while the tail-follow loop is active."""
        return self.__streaming

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
