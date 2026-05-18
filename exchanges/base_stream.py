import abc
import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional, List, AsyncIterator

from core.models.trade_tick import TradeTick
from core.models.messages import TradeMessage, SystemMessage, ErrorMessage
from core.state.subscription_registry import SubscriptionRegistry
from core.network.reconnecting_ws_manager import ReconnectingWebSocketManager
from core.interfaces.base import (
    IExchangeStream, 
    ISubscriptionStrategy, 
    IParsingStrategy, 
    IDispatchStrategy,
    IPriceObserver
)

logger = logging.getLogger(__name__)

# Pattern: Template Method
# Base class defining the skeleton of the streaming algorithm.
# Subclasses only need to implement the initialization and resubscription.

class BaseExchangeStream(IExchangeStream, ABC):
    def __init__(
        self, 
        network_manager: ReconnectingWebSocketManager,
        subscription_strategy: ISubscriptionStrategy,
        parsing_strategy: IParsingStrategy,
        dispatch_strategy: IDispatchStrategy,
        symbols: Optional[List[str]] = None
    ):
        if not isinstance(network_manager, ReconnectingWebSocketManager):
            raise TypeError("network_manager must be a ReconnectingWebSocketManager instance")
        if not isinstance(subscription_strategy, ISubscriptionStrategy):
            raise TypeError("subscription_strategy must be a ISubscriptionStrategy instance")
        if not isinstance(parsing_strategy, IParsingStrategy):
            raise TypeError("parsing_strategy must be a IParsingStrategy instance")
        if not isinstance(dispatch_strategy, IDispatchStrategy):
            raise TypeError("dispatch_strategy must be a IDispatchStrategy instance")

        self._registry = SubscriptionRegistry(initial_symbols=symbols)
        self._subscription_strategy = subscription_strategy
        self._parsing_strategy = parsing_strategy
        self._dispatch_strategy = dispatch_strategy
        self._network_manager = network_manager
        self._observers: List[IPriceObserver] = []
        
        self._network_manager.set_on_connect_callback(self._resubscribe_all)

    @property
    def registry(self) -> SubscriptionRegistry:
        """[Completeness] Return the subscription registry."""
        return self._registry

    @property
    def subscription_strategy(self) -> ISubscriptionStrategy:
        """[Completeness] Return the subscription strategy."""
        return self._subscription_strategy

    @property
    def parsing_strategy(self) -> IParsingStrategy:
        """[Completeness] Return the parsing strategy."""
        return self._parsing_strategy

    @property
    def dispatch_strategy(self) -> IDispatchStrategy:
        """[Completeness] Return the dispatch strategy."""
        return self._dispatch_strategy

    @property
    def network_manager(self) -> ReconnectingWebSocketManager:
        """[Completeness] Return the network manager."""
        return self._network_manager

    @property
    def observers(self) -> List[IPriceObserver]:
        """[Completeness] Return a copy of the attached observers list."""
        return list(self._observers)

    # Pattern: Observer (Observable part)
    def attach_observer(self, observer: IPriceObserver) -> None:
        if not isinstance(observer, IPriceObserver):
            raise TypeError("observer must be an IPriceObserver instance")
        if observer not in self._observers:
            self._observers.append(observer)

    def detach_observer(self, observer: IPriceObserver) -> None:
        if not isinstance(observer, IPriceObserver):
            raise TypeError("observer must be an IPriceObserver instance")
        if observer in self._observers:
            self._observers.remove(observer)

    async def _notify_observers(self, tick: TradeTick) -> None:
        for observer in self._observers:
            await observer.on_price_update(tick)

    async def wait_for_next_tick(self) -> TradeTick:
        return await self._dispatch_strategy.wait_for_next_data()

    def mark_tick_as_processed(self) -> None:
        self._dispatch_strategy.task_done()

    def is_stopped(self) -> bool:
        return self._network_manager.is_stopped()

    def is_connected(self) -> bool:
        return self._network_manager.is_connected()

    def get_active_symbols(self) -> List[str]:
        return self._registry.get_all()

    async def __aiter__(self) -> AsyncIterator[TradeTick]:
        while not self.is_stopped():
            try:
                yield await self.wait_for_next_tick()
            except Exception:
                if self.is_stopped(): # pragma: no cover
                    break
                raise

    async def start_streaming(self) -> None:
        """Pattern: Template Method - The algorithm skeleton."""
        logger.info(f"Démarrage du flux {self.__class__.__name__}.")
        async for message in self._network_manager.start_connection_and_listen():
            parsed_message = self._parsing_strategy.parse(message)
            
            if isinstance(parsed_message, TradeMessage):
                for tick in parsed_message.ticks:
                    await self._dispatch_strategy.dispatch(tick)
                    await self._notify_observers(tick)
                        
            elif isinstance(parsed_message, SystemMessage):
                if parsed_message.event != "pong":
                    logger.info(parsed_message.msg)
                    
            elif isinstance(parsed_message, ErrorMessage):
                logger.error(parsed_message.msg)

    async def stop(self) -> None:
        await self._network_manager.stop()

    @abstractmethod
    async def _resubscribe_all(self) -> None:
        pass # pragma: no cover

    async def subscribe_symbol(self, symbol: str) -> None:
        if symbol is None:
            raise ValueError("symbol cannot be empty")
        if not isinstance(symbol, str):
            raise TypeError("symbol must be a string")
        if not symbol:
            raise ValueError("symbol cannot be empty")
            
        if self._registry.add(symbol):
            payload = self._subscription_strategy.format_subscribe([symbol])
            await self._network_manager.send(payload)
            logger.info(f"Abonnement dynamique : {symbol}")

    async def subscribe_symbols(self, symbols: List[str]) -> None:
        if symbols is None:
            raise ValueError("symbols list cannot be empty")
        if not isinstance(symbols, list):
            raise TypeError("symbols must be a list")
        if not symbols:
            raise ValueError("symbols list cannot be empty")
            
        for s in symbols:
            if not isinstance(s, str):
                raise TypeError("symbols must be strings")
            if not s:
                raise ValueError("symbols must be non-empty strings")
            
        to_add = [s for s in symbols if self._registry.add(s)]
        if to_add:
            payload = self._subscription_strategy.format_subscribe(to_add)
            await self._network_manager.send(payload)
            logger.info(f"Abonnements par lot : {to_add}")

    async def unsubscribe_symbol(self, symbol: str) -> None:
        if symbol is None:
            raise ValueError("symbol cannot be empty")
        if not isinstance(symbol, str):
            raise TypeError("symbol must be a string")
        if not symbol:
            raise ValueError("symbol cannot be empty")
            
        if self._registry.remove(symbol):
            payload = self._subscription_strategy.format_unsubscribe([symbol])
            await self._network_manager.send(payload)
            logger.info(f"Désabonnement dynamique : {symbol}")

    async def unsubscribe_symbols(self, symbols: List[str]) -> None:
        if symbols is None:
            raise ValueError("symbols list cannot be empty")
        if not isinstance(symbols, list):
            raise TypeError("symbols must be a list")
        if not symbols:
            raise ValueError("symbols list cannot be empty")
            
        for s in symbols:
            if not isinstance(s, str):
                raise TypeError("symbols must be strings")
            if not s:
                raise ValueError("symbols must be non-empty strings")
            
        to_remove = [s for s in symbols if self._registry.remove(s)]
        if to_remove:
            payload = self._subscription_strategy.format_unsubscribe(to_remove)
            await self._network_manager.send(payload)
            logger.info(f"Désabonnements par lot : {to_remove}")

    async def wait_until_connected(self) -> None:
        while not self.is_connected():
            if self.is_stopped():
                raise ConnectionError("Le flux a été arrêté avant de pouvoir se connecter.")
            await asyncio.sleep(0.1)
