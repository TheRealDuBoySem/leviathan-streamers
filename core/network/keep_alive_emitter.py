import asyncio
import logging
from typing import Callable, Awaitable
from core.interfaces.base import IHeartbeat

logger = logging.getLogger(__name__)

class KeepAliveEmitter(IHeartbeat):
    """
    Periodically sends a keep-alive message through a provided send function.
    
    Invariants:
        - interval_seconds must be a positive number.
    """
    def __init__(self, interval_seconds: float = 30.0):
        """
        Initialize the emitter.
        
        Preconditions:
            - interval_seconds must be positive.
        """
        if not isinstance(interval_seconds, (int, float)):
            raise TypeError("interval_seconds must be a number (int or float)")
        if interval_seconds <= 0:
            raise ValueError(f"interval_seconds must be positive, got {interval_seconds}")
            
        self.__interval = interval_seconds

    @property
    def interval_seconds(self) -> int:
        """
        [Completeness] Return the configured keep-alive interval in seconds.
        """
        return self.__interval

    async def run(self, send_func: Callable[[str], Awaitable[None]], payload: str = "ping") -> None:
        """
        Start the keep-alive loop.
        
        Preconditions:
            - send_func must be a callable that accepts a string and returns an awaitable.
            - payload must be a non-empty string.
        """
        if not callable(send_func):
            raise TypeError("send_func must be callable")
        if not isinstance(payload, str):
            raise TypeError("payload must be a string")
        if not payload:
            raise ValueError("payload cannot be empty")
            
        try:
            while True:
                await asyncio.sleep(self.__interval)
                logger.debug("Envoi du Keep-Alive...")
                await send_func(payload)
        except asyncio.CancelledError:
            pass
