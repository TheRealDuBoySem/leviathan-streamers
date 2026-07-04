from typing import Optional, List

from core.network.reconnecting_ws_manager import ReconnectingWebSocketManager
from core.routing.async_queue_dispatcher import AsyncQueueDispatcher
from exchanges.bitget.bitget_subscription_protocol import BitgetSubscriptionProtocol
from exchanges.bitget.parsing.bitget_message_parser import BitgetMessageParser
from exchanges.bitget.bitget_tick_stream import BitgetTickStream

# Pattern: Factory
# Centralizes the creation of complex Bitget stream objects.


class BitgetStreamFactory:
    """Factory for fully configured BitgetTickStream instances."""

    DEFAULT_INST_TYPE = "USDT-FUTURES"

    @classmethod
    def create_default(
        cls,
        url: str,
        symbols: Optional[List[str]] = None,
        inst_type: str = DEFAULT_INST_TYPE,
    ) -> BitgetTickStream:
        """
        Create a BitgetTickStream with standard resilient network defaults.

        Preconditions:
            - url must be a non-empty string.
            - inst_type must be a non-empty string.
            - symbols, if provided, must be a list of non-empty strings.
        """
        return cls.create_stream(url=url, symbols=symbols, inst_type=inst_type)

    @staticmethod
    def create_stream(
        url: str,
        symbols: Optional[List[str]] = None,
        inst_type: str = DEFAULT_INST_TYPE,
        max_retries: Optional[int] = None,
        timeout_seconds: int = 60,
        keep_alive_interval: int = 30,
        keep_alive_payload: str = "ping",
        connect_timeout: float = 10.0,
    ) -> BitgetTickStream:
        """
        Create a fully configured BitgetTickStream.

        Bitget-specific parameters (inst_type, symbols) are validated here.
        Network resilience parameters are delegated to
        ReconnectingWebSocketManager.create_default.

        Preconditions:
            - url must be a non-empty string.
            - inst_type must be a non-empty string.
            - symbols, if provided, must be a list of non-empty strings.

        Postconditions:
            - Returned stream uses default Bitget parsing and dispatch strategies.
        """
        if not isinstance(inst_type, str):
            raise TypeError("inst_type must be a string")
        if not inst_type:
            raise ValueError("inst_type cannot be empty")

        if symbols is not None:
            if not isinstance(symbols, list):
                raise TypeError("symbols must be a list")
            for symbol in symbols:
                if not isinstance(symbol, str):
                    raise TypeError("symbols must be strings")
                if not symbol:
                    raise ValueError("symbols must be non-empty strings")

        network_manager = ReconnectingWebSocketManager.create_default(
            url=url,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
            keep_alive_interval=keep_alive_interval,
            keep_alive_payload=keep_alive_payload,
            connect_timeout=connect_timeout,
        )
        subscription_strategy = BitgetSubscriptionProtocol(inst_type=inst_type)
        parsing_strategy = BitgetMessageParser.create_default()
        dispatch_strategy = AsyncQueueDispatcher()

        return BitgetTickStream(
            network_manager=network_manager,
            subscription_strategy=subscription_strategy,
            parsing_strategy=parsing_strategy,
            dispatch_strategy=dispatch_strategy,
            symbols=symbols,
        )
