import logging
from typing import Optional, List

from core.network.reconnecting_ws_manager import ReconnectingWebSocketManager
from core.routing.async_queue_dispatcher import AsyncQueueDispatcher
from exchanges.bitget.bitget_subscription_protocol import BitgetSubscriptionProtocol
from exchanges.bitget.parsing.bitget_message_parser import BitgetMessageParser
from exchanges.bitget.bitget_tick_stream import BitgetTickStream

logger = logging.getLogger(__name__)

# Pattern: Factory
# Centralizes the creation of complex Bitget stream objects.

class BitgetStreamFactory:
    
    # [Completeness] Centralized default instrument type constant
    DEFAULT_INST_TYPE = "USDT-FUTURES"
    
    @staticmethod
    def create_stream(
        url: str, 
        symbols: Optional[List[str]] = None,
        inst_type: str = DEFAULT_INST_TYPE
    ) -> BitgetTickStream:
        """
        Factory method to create a fully configured BitgetTickStream.
        
        Preconditions:
            - url must be a non-empty string.
            - inst_type must be a non-empty string.
            - symbols, if provided, must be a list of non-empty strings.
        """
        if not isinstance(url, str):
            raise TypeError("url must be a string")
        if not url:
            raise ValueError("url cannot be empty")
            
        if not isinstance(inst_type, str):
            raise TypeError("inst_type must be a string")
        if not inst_type:
            raise ValueError("inst_type cannot be empty")
            
        if symbols is not None:
            if not isinstance(symbols, list):
                raise TypeError("symbols must be a list")
            for s in symbols:
                if not isinstance(s, str):
                    raise TypeError("symbols must be strings")
                if not s:
                    raise ValueError("symbols must be non-empty strings")
        
        network_manager = ReconnectingWebSocketManager.create_default(url)
        subscription_strategy = BitgetSubscriptionProtocol(inst_type=inst_type)
        parsing_strategy = BitgetMessageParser.create_default()
        dispatch_strategy = AsyncQueueDispatcher()
        
        return BitgetTickStream(
            network_manager=network_manager,
            subscription_strategy=subscription_strategy,
            parsing_strategy=parsing_strategy,
            dispatch_strategy=dispatch_strategy,
            symbols=symbols
        )
