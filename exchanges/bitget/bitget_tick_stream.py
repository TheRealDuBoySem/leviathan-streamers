import logging
from typing import Optional, List
from exchanges.base_stream import BaseExchangeStream
from core.network.reconnecting_ws_manager import ReconnectingWebSocketManager
from core.interfaces.base import ISubscriptionStrategy, IParsingStrategy, IDispatchStrategy

logger = logging.getLogger(__name__)

# Pattern: Strategy & Template Method (Concrete Implementation)
# Specializes the BaseExchangeStream for Bitget-specific resubscription.

class BitgetTickStream(BaseExchangeStream):
    """
    Concrete implementation of the Bitget trade data ingestion.
    """
    def __init__(
        self, 
        network_manager: ReconnectingWebSocketManager,
        subscription_strategy: ISubscriptionStrategy,
        parsing_strategy: IParsingStrategy,
        dispatch_strategy: IDispatchStrategy,
        symbols: Optional[List[str]] = None
    ):
        super().__init__(
            network_manager=network_manager,
            subscription_strategy=subscription_strategy,
            parsing_strategy=parsing_strategy,
            dispatch_strategy=dispatch_strategy,
            symbols=symbols
        )

    async def _resubscribe_all(self) -> None:
        """Pattern: Implementation of Template Method's abstract hook."""
        symbols = self._registry.get_all()
        if symbols: # pragma: no cover
            payload = self._subscription_strategy.format_subscribe(symbols)
            await self._network_manager.send(payload)
            logger.info("Requête globale d'abonnement envoyée pour Bitget.") # pragma: no cover


