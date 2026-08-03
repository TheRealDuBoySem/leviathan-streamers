import asyncio
import inspect
import logging
from abc import ABC
from typing import Optional, List, AsyncIterator, Callable, Awaitable, Set

from core.models.trade_tick import TradeTick
from core.models.messages import TradeMessage, SystemMessage, ErrorMessage
from core.state.subscription_registry import SubscriptionRegistry
from core.state.subscription_confirmation_tracker import (
    DEFAULT_CONFIRMATION_TIMEOUT_SECONDS,
    SubscriptionConfirmationTracker,
)
from core.state.post_reconnect_write_liveness import PostReconnectWriteLivenessGuard
from core.network.reconnecting_ws_manager import ReconnectingWebSocketManager
from core.interfaces.base import (
    IExchangeStream,
    ISubscriptionStrategy,
    IParsingStrategy,
    IDispatchStrategy,
    IPriceObserver,
)

logger = logging.getLogger(__name__)


def _validate_symbol(symbol: str, param_name: str = "symbol") -> None:
    """Validate a single non-empty symbol string."""
    if symbol is None:
        raise ValueError(f"{param_name} cannot be empty")
    if not isinstance(symbol, str):
        raise TypeError(f"{param_name} must be a string")
    if not symbol:
        raise ValueError(f"{param_name} cannot be empty")


def _validate_initial_symbols(symbols: List[str]) -> None:
    """Validate an optional initial symbols list (may be empty)."""
    if not isinstance(symbols, list):
        raise TypeError("symbols must be a list")
    for symbol in symbols:
        if not isinstance(symbol, str):
            raise TypeError("symbols must be strings")
        if not symbol:
            raise ValueError("symbols must be non-empty strings")


def _validate_symbols_list(symbols: List[str]) -> None:
    """Validate a non-empty list of non-empty symbol strings."""
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


_REQUIRED_NETWORK_MANAGER_METHODS = (
    "set_on_connect_callback",
    "start_connection_and_listen",
    "send",
    "stop",
    "is_stopped",
    "is_connected",
)


def _validate_network_manager(network_manager: object) -> None:
    """Validate that network_manager exposes the streaming contract (duck typing)."""
    if network_manager is None:
        raise TypeError("network_manager must provide streaming network operations")
    for method_name in _REQUIRED_NETWORK_MANAGER_METHODS:
        if not callable(getattr(network_manager, method_name, None)):
            raise TypeError(
                f"network_manager must provide a callable {method_name} method"
            )


# Pattern: Template Method
# Base class defining the skeleton of the streaming algorithm.
# Subclasses may override _resubscribe_all for exchange-specific resubscription.


class BaseExchangeStream(IExchangeStream, ABC):
    def __init__(
        self,
        network_manager: ReconnectingWebSocketManager,
        subscription_strategy: ISubscriptionStrategy,
        parsing_strategy: IParsingStrategy,
        dispatch_strategy: IDispatchStrategy,
        symbols: Optional[List[str]] = None,
        confirmation_timeout_seconds: float = DEFAULT_CONFIRMATION_TIMEOUT_SECONDS,
        write_liveness_guard: Optional[PostReconnectWriteLivenessGuard] = None,
    ):
        _validate_network_manager(network_manager)
        if not isinstance(subscription_strategy, ISubscriptionStrategy):
            raise TypeError("subscription_strategy must be a ISubscriptionStrategy instance")
        if not isinstance(parsing_strategy, IParsingStrategy):
            raise TypeError("parsing_strategy must be a IParsingStrategy instance")
        if not isinstance(dispatch_strategy, IDispatchStrategy):
            raise TypeError("dispatch_strategy must be a IDispatchStrategy instance")
        if symbols is not None:
            _validate_initial_symbols(symbols)
        if write_liveness_guard is not None and not isinstance(
            write_liveness_guard, PostReconnectWriteLivenessGuard
        ):
            raise TypeError(
                "write_liveness_guard must be a PostReconnectWriteLivenessGuard instance"
            )

        self.__registry = SubscriptionRegistry(initial_symbols=symbols)
        self.__subscription_strategy = subscription_strategy
        self.__parsing_strategy = parsing_strategy
        self.__dispatch_strategy = dispatch_strategy
        self.__network_manager = network_manager
        self.__observers: List[IPriceObserver] = []
        self.__on_reconnect_callbacks: List[Callable[[], Awaitable[None]]] = []
        self.__write_liveness_guard = write_liveness_guard
        # 1-based: incremented on every WS connect (1 = first boot / post-OS).
        self.__connect_generation = 0
        self.__confirmation_tracker = SubscriptionConfirmationTracker(
            timeout_seconds=confirmation_timeout_seconds,
            on_complete=self.__on_subscriptions_confirmed,
        )

        self.__network_manager.set_on_connect_callback(self.__handle_connect)
        self.__wire_flap_tip_correlation()

    def __wire_flap_tip_correlation(self) -> None:
        """
        Bind tip-seq sampling for post-flap correlation when a journal is reachable.

        Complements BB-B5-A1 write-liveness; tip-stale heal callback is wired by
        the collector entrypoint (``set_on_post_flap_tip_stale``).
        """
        set_provider = getattr(self.__network_manager, "set_tip_seq_provider", None)
        if not callable(set_provider):
            return
        provider = self.__resolve_tip_seq_provider()
        if provider is not None:
            set_provider(provider)

    def __resolve_tip_seq_provider(self):
        """Return a tip-seq callable from a journal-backed dispatch strategy, if any."""
        journal = getattr(self.__dispatch_strategy, "journal", None)
        if journal is None:
            return None
        latest_seq = getattr(journal, "latest_seq", None)
        if not callable(latest_seq):
            return None

        def _tip_seq_provider():
            value = latest_seq()
            if value is None:
                return None
            return int(value)

        return _tip_seq_provider

    def __on_subscriptions_confirmed(self, confirmed: Set[str]) -> None:
        """Log full ack, then arm journal write-liveness (first boot + reconnect).

        Day20 WATCH #3: never skip connect_generation=1 (post-OS tip mute).
        """
        connect_generation = self.__connect_generation
        first_boot = connect_generation <= 1
        logger.info(
            "Abonnements confirmés après reconnect: demandés=%s, confirmés=%s "
            "connect_generation=%s first_boot=%s",
            sorted(confirmed),
            sorted(confirmed),
            connect_generation,
            first_boot,
        )
        if self.__write_liveness_guard is not None:
            # Always arm — including first confirm after OS restart / fresh collector.
            self.__write_liveness_guard.arm_after_subscriptions_confirmed(
                set(confirmed),
                connect_generation=max(connect_generation, 1),
            )

    async def __handle_connect(self) -> None:
        """Notifies reconnect listeners before resubscribing so backfill session state is reset first."""
        self.__connect_generation += 1
        if self.__write_liveness_guard is not None:
            # Drop any prior write deadline; re-arm only after the new confirms
            # (including the first confirm of this connect generation).
            self.__write_liveness_guard.cancel()
        for callback in list(self.__on_reconnect_callbacks):
            try:
                await callback()
            except Exception as exc:
                logger.error(
                    "Error in stream on_reconnect callback %r: %s",
                    callback,
                    exc,
                    exc_info=True,
                )
        await self._resubscribe_all()

    def register_on_reconnect(self, callback: Callable[[], Awaitable[None]]) -> None:
        """
        Registers an async callback invoked after each successful WS connect/resubscribe cycle.
        """
        if callback is None or not callable(callback):
            raise TypeError("callback must be a callable awaitable")
        if not inspect.iscoroutinefunction(callback):
            raise TypeError("callback must be an async function")
        if callback not in self.__on_reconnect_callbacks:
            self.__on_reconnect_callbacks.append(callback)

    def unregister_on_reconnect(self, callback: Callable[[], Awaitable[None]]) -> None:
        """Unregisters a callback previously added via register_on_reconnect."""
        if callback is None or not callable(callback):
            raise TypeError("callback must be a callable awaitable")
        if callback in self.__on_reconnect_callbacks:
            self.__on_reconnect_callbacks.remove(callback)

    @property
    def registry(self) -> SubscriptionRegistry:
        """Return the subscription registry."""
        return self.__registry

    @property
    def subscription_strategy(self) -> ISubscriptionStrategy:
        """Return the subscription strategy."""
        return self.__subscription_strategy

    @property
    def parsing_strategy(self) -> IParsingStrategy:
        """Return the parsing strategy."""
        return self.__parsing_strategy

    @property
    def dispatch_strategy(self) -> IDispatchStrategy:
        """Return the dispatch strategy."""
        return self.__dispatch_strategy

    @property
    def network_manager(self) -> ReconnectingWebSocketManager:
        """Return the network manager."""
        return self.__network_manager

    @property
    def observers(self) -> List[IPriceObserver]:
        """Return a copy of the attached observers list."""
        return list(self.__observers)

    @property
    def confirmation_tracker(self) -> SubscriptionConfirmationTracker:
        """Return the subscription confirmation tracker (reconnect acks)."""
        return self.__confirmation_tracker

    @property
    def write_liveness_guard(self) -> Optional[PostReconnectWriteLivenessGuard]:
        """Return the optional post-reconnect journal write-liveness guard."""
        return self.__write_liveness_guard

    @property
    def connect_generation(self) -> int:
        """Return 1-based WS connect count (0 before first connect)."""
        return self.__connect_generation

    # Pattern: Observer (Observable part)
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

    async def __notify_observers(self, tick: TradeTick) -> None:
        for observer in list(self.__observers):
            try:
                await observer.on_price_update(tick)
            except Exception as exc:
                logger.error(
                    "Error in stream price observer %r: %s",
                    observer,
                    exc,
                    exc_info=True,
                )

    async def wait_for_next_tick(self) -> TradeTick:
        return await self.__dispatch_strategy.wait_for_next_tick()

    def mark_tick_as_processed(self) -> None:
        self.__dispatch_strategy.mark_tick_as_processed()

    def is_stopped(self) -> bool:
        return self.__network_manager.is_stopped()

    def is_connected(self) -> bool:
        return self.__network_manager.is_connected()

    def get_active_symbols(self) -> List[str]:
        return self.__registry.get_all()

    async def __aiter__(self) -> AsyncIterator[TradeTick]:
        while not self.is_stopped():
            try:
                yield await self.wait_for_next_tick()
            except Exception:
                if self.is_stopped():  # pragma: no cover
                    break
                raise

    async def start_streaming(self) -> None:
        """Pattern: Template Method - The algorithm skeleton."""
        logger.info("Démarrage du flux %s.", self.__class__.__name__)
        async for message in self.__network_manager.start_connection_and_listen():
            try:
                parsed_message = self.__parsing_strategy.parse(message)
                if parsed_message is None:
                    continue

                if isinstance(parsed_message, TradeMessage):
                    for tick in parsed_message.ticks:
                        await self.__dispatch_strategy.dispatch(tick)
                        await self.__notify_observers(tick)

                elif isinstance(parsed_message, SystemMessage):
                    if parsed_message.event == "subscribe" and parsed_message.symbol:
                        self.__confirmation_tracker.record_confirmation(
                            parsed_message.symbol
                        )
                    if parsed_message.event != "pong":
                        logger.info(parsed_message.msg)

                elif isinstance(parsed_message, ErrorMessage):
                    logger.error(parsed_message.msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error(
                    "%s: failed to process websocket message",
                    self.__class__.__name__,
                    exc_info=True,
                )
                continue

    async def stop(self) -> None:
        self.__confirmation_tracker.cancel()
        if self.__write_liveness_guard is not None:
            self.__write_liveness_guard.cancel()
        await self.__network_manager.stop()

    async def _resubscribe_all(self) -> None:
        """Pattern: Template Method hook — resubscribe all active symbols after reconnect."""
        symbols = self.get_active_symbols()
        if symbols:
            await self.__send_subscribe_payload(symbols)
            requested = sorted(symbols)
            logger.info(
                "Requête globale d'abonnement envoyée pour %s: symboles demandés=%s",
                self.__class__.__name__,
                requested,
            )
            self.__confirmation_tracker.begin_expectation(requested)
        else:
            self.__confirmation_tracker.cancel()

    async def __send_subscribe_payload(self, symbols: List[str]) -> None:
        payload = self.__subscription_strategy.format_subscribe(symbols)
        await self.__network_manager.send(payload)

    async def __send_unsubscribe_payload(self, symbols: List[str]) -> None:
        payload = self.__subscription_strategy.format_unsubscribe(symbols)
        await self.__network_manager.send(payload)

    async def subscribe_symbol(self, symbol: str) -> None:
        _validate_symbol(symbol)
        if self.__registry.add(symbol):
            await self.__send_subscribe_payload([symbol])
            logger.info("Abonnement dynamique : %s", symbol)

    async def subscribe_symbols(self, symbols: List[str]) -> None:
        _validate_symbols_list(symbols)
        to_add = [symbol for symbol in symbols if self.__registry.add(symbol)]
        if to_add:
            await self.__send_subscribe_payload(to_add)
            logger.info("Abonnements par lot : %s", to_add)

    async def unsubscribe_symbol(self, symbol: str) -> None:
        _validate_symbol(symbol)
        if self.__registry.remove(symbol):
            await self.__send_unsubscribe_payload([symbol])
            logger.info("Désabonnement dynamique : %s", symbol)

    async def unsubscribe_symbols(self, symbols: List[str]) -> None:
        _validate_symbols_list(symbols)
        to_remove = [symbol for symbol in symbols if self.__registry.remove(symbol)]
        if to_remove:
            await self.__send_unsubscribe_payload(to_remove)
            logger.info("Désabonnements par lot : %s", to_remove)

    async def wait_until_connected(self) -> None:
        while not self.is_connected():
            if self.is_stopped():
                raise ConnectionError("Le flux a été arrêté avant de pouvoir se connecter.")
            await asyncio.sleep(0.1)
