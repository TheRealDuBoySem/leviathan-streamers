import asyncio
import logging
import websockets
from websockets.exceptions import ConnectionClosed
from typing import Optional, AsyncGenerator, Callable, Awaitable
from core.interfaces.base import IRetryPolicy, IWatchdog, IHeartbeat

logger = logging.getLogger(__name__)

class MaxRetriesExceededError(Exception):
    """Raised when the maximum number of retry attempts is exceeded."""
    pass

class ReconnectingWebSocketManager:
    """
    Manages a resilient WebSocket connection with automatic reconnection.
    
    Invariants:
        - retry_policy, watchdog, and keep_alive are non-null.
    """
    @classmethod
    def create_default(
        cls, 
        url: str, 
        max_retries: Optional[int] = None, 
        timeout_seconds: int = 60, 
        keep_alive_interval: int = 30
    ) -> "ReconnectingWebSocketManager":
        """Factory method to create a manager with standard resilient configuration."""
        if not isinstance(url, str):
            raise TypeError("url must be a string")
        if max_retries is not None:
            if not isinstance(max_retries, int):
                raise TypeError("max_retries must be an integer")
        if not isinstance(timeout_seconds, int):
            raise TypeError("timeout_seconds must be an integer")
        if not isinstance(keep_alive_interval, int):
            raise TypeError("keep_alive_interval must be an integer")

        from core.network.retry_policy import RetryPolicy
        from core.network.silence_watchdog import SilenceWatchdog
        from core.network.keep_alive_emitter import KeepAliveEmitter
        
        return cls(
            url=url,
            retry_policy=RetryPolicy(max_retries=max_retries),
            watchdog=SilenceWatchdog(timeout_seconds=timeout_seconds),
            keep_alive=KeepAliveEmitter(interval_seconds=keep_alive_interval)
        )

    def __init__(self, url: str, retry_policy: IRetryPolicy, watchdog: IWatchdog, keep_alive: IHeartbeat):
        """
        Initialize the manager.
        
        Preconditions:
            - url must be a valid non-empty string.
            - retry_policy, watchdog, and keep_alive must be valid instances.
        """
        if not isinstance(url, str):
            raise TypeError("url must be a string")
        if not url:
            raise ValueError("url cannot be empty")
        if not isinstance(retry_policy, IRetryPolicy):
            raise TypeError("retry_policy must be a IRetryPolicy instance")
        if not isinstance(watchdog, IWatchdog):
            raise TypeError("watchdog must be a IWatchdog instance")
        if not isinstance(keep_alive, IHeartbeat):
            raise TypeError("keep_alive must be a IHeartbeat instance")
            
        self.__url = url
        self.__retry_policy = retry_policy
        self.__watchdog = watchdog
        self.__keep_alive = keep_alive
        self.__ws: Optional[websockets.WebSocketClientProtocol] = None
        self.__stop_event = asyncio.Event()
        self.__on_connect_callback = None

    @property
    def url(self) -> str:
        """[Completeness] Return the configured WebSocket URL."""
        return self.__url

    @property
    def retry_policy(self) -> IRetryPolicy:
        """[Completeness] Return the injected retry policy."""
        return self.__retry_policy

    @property
    def watchdog(self) -> IWatchdog:
        """[Completeness] Return the injected silence watchdog."""
        return self.__watchdog

    @property
    def keep_alive(self) -> IHeartbeat:
        """[Completeness] Return the injected keep alive emitter."""
        return self.__keep_alive

    def set_on_connect_callback(self, callback: Callable[[], Awaitable[None]]) -> None:
        """
        Set a callback to be executed upon successful connection.
        
        Preconditions:
            - callback must be callable.
        """
        if not callable(callback):
            raise TypeError("callback must be callable")
        self.__on_connect_callback = callback

    def is_stopped(self) -> bool:
        """Return True if the manager has been stopped."""
        return self.__stop_event.is_set()

    def is_connected(self) -> bool:
        """Return True if the WebSocket connection is currently active."""
        if self.__ws is None:
            return False
        if hasattr(self.__ws, "state"):
            from websockets.protocol import State
            return self.__ws.state == State.OPEN
        if hasattr(self.__ws, "open"):
            return bool(self.__ws.open)
        if hasattr(self.__ws, "closed"):
            return not self.__ws.closed
        return False

    async def send(self, message: str) -> None:
        """
        Send a message over the WebSocket.
        
        Preconditions:
            - message must be a non-empty string.
        """
        if not isinstance(message, str):
            raise TypeError("message must be a string")
        if not message:
            raise ValueError("message cannot be empty")
            
        if self.__ws:
            await self.__ws.send(message)

    async def disconnect(self) -> None:
        """Gracefully close the current connection."""
        if self.__ws:
            await self.__ws.close()

    async def stop(self) -> None:
        """Stop the manager and close any active connection."""
        self.__stop_event.set()
        await self.disconnect()

    async def _health_loop(self) -> None:
        """Internal loop to monitor connection health via the watchdog."""
        try:
            while not self.__stop_event.is_set(): # pragma: no cover
                await asyncio.sleep(5)
                if not self.__watchdog.check_health(): # pragma: no cover
                    logger.error("Watchdog: Délai dépassé. Coupure de la connexion forcée.")
                    await self.disconnect()
                    break
        except asyncio.CancelledError: # pragma: no cover
            pass

    async def start_connection_and_listen(self) -> AsyncGenerator[str, None]:
        """
        Connect to the WebSocket and yield incoming messages.
        
        This method handles reconnection logic according to the retry policy.
        """
        attempt = 0

        while not self.__stop_event.is_set():
            if not self.__retry_policy.can_retry(attempt):
                logger.error("Échec critique: Limite de reconnexions atteinte.")
                self.__stop_event.set()
                raise MaxRetriesExceededError("Connexion impossible.")

            logger.info(f"Connexion à {self.__url} (Tentative {attempt + 1})...")
            health_task = None
            keep_alive_task = None

            try:
                async with websockets.connect(self.__url, ping_interval=None) as ws:
                    self.__ws = ws
                    logger.info("WebSocket connecté avec succès.")
                    attempt = 0
                    self.__watchdog.ping()
                    
                    health_task = asyncio.create_task(self._health_loop())
                    keep_alive_task = asyncio.create_task(self.__keep_alive.run(self.send))
                    
                    if self.__on_connect_callback:
                        await self.__on_connect_callback()
                    
                    async for message in ws:
                        self.__watchdog.ping()
                        yield message
                        
            except ConnectionClosed as e:
                close_code = e.rcvd.code if getattr(e, "rcvd", None) else "Unknown"
                logger.warning(f"WebSocket fermé ({close_code}).") # pragma: no cover
            except Exception as e:
                logger.error(f"Erreur réseau: {e}")
            finally:
                self.__ws = None
                if health_task: health_task.cancel()
                if keep_alive_task: keep_alive_task.cancel()

            if not self.__stop_event.is_set():
                delay = self.__retry_policy.get_delay(attempt)
                logger.info(f"Reconnexion dans {delay}s...")
                try:
                    await asyncio.wait_for(self.__stop_event.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
                attempt += 1
