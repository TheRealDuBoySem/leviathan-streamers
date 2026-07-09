import logging
from typing import Optional, List
from exchanges.base_stream import BaseExchangeStream
from core.network.reconnecting_ws_manager import ReconnectingWebSocketManager
from core.interfaces.base import ISubscriptionStrategy, IParsingStrategy, IDispatchStrategy

logger = logging.getLogger(__name__)

# Pattern: Strategy & Template Method (Concrete Implementation)
# Specializes the BaseExchangeStream for Bitget-specific configuration.


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

