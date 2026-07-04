import asyncio
import logging
from typing import Callable, Awaitable, Optional
from core.interfaces.base import IHeartbeat

logger = logging.getLogger(__name__)


class KeepAliveEmitter(IHeartbeat):
    """
    Periodically sends a keep-alive message through a provided send function.

    Pattern: Strategy (IHeartbeat) — interchangeable heartbeat loop for resilient transports.

    Invariants:
        - interval_seconds is a positive number.
        - payload is a non-empty string.
    """
    def __init__(self, interval_seconds: float = 30.0, payload: str = "ping"):
        """
        Initialize the emitter.
        
        Preconditions:
            - interval_seconds must be positive.
            - payload must be a non-empty string.
        """
        if not isinstance(interval_seconds, (int, float)):
            raise TypeError("interval_seconds must be a number (int or float)")
        if interval_seconds <= 0:
            raise ValueError(f"interval_seconds must be positive, got {interval_seconds}")
        if not isinstance(payload, str):
            raise TypeError("payload must be a string")
        if not payload:
            raise ValueError("payload cannot be empty")
            
        self.__interval = interval_seconds
        self.__payload = payload

    @property
    def interval_seconds(self) -> float:
        """
        [Completeness] Return the configured keep-alive interval in seconds.
        """
        return self.__interval

    @property
    def payload(self) -> str:
        """
        Return the configured keep-alive payload.
        """
        return self.__payload

    async def run(
        self,
        send_func: Callable[[str], Awaitable[None]],
        payload: Optional[str] = None,
    ) -> None:
        """
        Start the keep-alive loop. Designed to run as a background asyncio task.

        The first emission occurs after one full interval; cancel the task to stop.

        Preconditions:
            - send_func must be a callable that accepts a string and returns an awaitable.
            - payload must be a non-empty string if provided.

        Postconditions:
            - On cancellation, the loop exits and CancelledError is re-raised.
            - On send_func failure, the exception propagates and the loop stops.
        """
        actual_payload = payload if payload is not None else self.__payload
        if not callable(send_func):
            raise TypeError("send_func must be callable")
        if not isinstance(actual_payload, str):
            raise TypeError("payload must be a string")
        if not actual_payload:
            raise ValueError("payload cannot be empty")

        try:
            while True:
                await asyncio.sleep(self.__interval)
                logger.debug(
                    "Envoi du Keep-Alive (intervalle=%ss, payload=%r)...",
                    self.__interval,
                    actual_payload,
                )
                await send_func(actual_payload)
        except asyncio.CancelledError:
            logger.debug("Keep-Alive arrêté (tâche annulée).")
            raise
