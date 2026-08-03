from __future__ import annotations

import asyncio
import inspect
import logging
import sys
import time
from typing import Any, Optional, AsyncGenerator, Callable, Awaitable, Dict

import websockets
from websockets.exceptions import ConnectionClosed

from core.interfaces.base import IRetryPolicy, IWatchdog, IHeartbeat
from core.network.post_flap_tip_correlation import (
    PostFlapTipCorrelationMonitor,
    TipSeqProvider,
)

logger = logging.getLogger(__name__)

_HEALTH_CHECK_INTERVAL_SECONDS = 5.0
_ABSENT_CLOSE_DIAGNOSTIC = "close code/reason absent"


def _normalize_close_reason(reason: Any) -> str:
    """Normalize a WebSocket close reason to a UTF-8 string."""
    if reason is None:
        return ""
    if isinstance(reason, bytes):
        return reason.decode("utf-8", errors="replace")
    return str(reason)


def _format_close_frame(frame: Any, *, source: str) -> str:
    """Format a single close frame (rcvd or sent) for diagnostics."""
    code = getattr(frame, "code", None)
    reason = _normalize_close_reason(getattr(frame, "reason", None))
    code_label = str(code) if code is not None else "absent"
    return f"code={code_label}, reason={reason!r}, frame={source}"


def format_websocket_close_diagnostic(exc: Any) -> str:
    """
    Build a clear close diagnostic from a ConnectionClosed-like exception.

    Prefers the received close frame, then the sent close frame.
    When neither frame is available, returns an explicit absence label
    instead of an opaque sentinel such as ``Unknown``.
    """
    rcvd = getattr(exc, "rcvd", None)
    if rcvd is not None:
        return _format_close_frame(rcvd, source="rcvd")
    sent = getattr(exc, "sent", None)
    if sent is not None:
        return _format_close_frame(sent, source="sent")
    return _ABSENT_CLOSE_DIAGNOSTIC


def log_reconnect_countdown(
    delay: float,
    reconnect_seq: int,
    last_close_mono_ms: Optional[int],
    *,
    now_monotonic_ms: Callable[[], int],
) -> None:
    """
    Log the reconnect delay with a stable close→reconnect chronology.

    When ``last_close_mono_ms`` is known (ConnectionClosed path), include
    ``since_close_ms`` derived from a monotonic clock so wall-clock skew cannot
    invert close/reconnect ordering in the logs.
    """
    if last_close_mono_ms is not None:
        since_close_ms = now_monotonic_ms() - last_close_mono_ms
        logger.info(
            "Reconnexion dans %ss... seq=%s since_close_ms=%s",
            delay,
            reconnect_seq,
            since_close_ms,
        )
        return
    logger.info("Reconnexion dans %ss... seq=%s", delay, reconnect_seq)


class MaxRetriesExceededError(Exception):
    """Raised when the maximum number of retry attempts is exceeded."""

    pass


class ReconnectingWebSocketManager:
    """
    Manages a resilient WebSocket connection with automatic reconnection.

    Pattern: Factory (create_default) + Strategy (IRetryPolicy, IWatchdog, IHeartbeat).

    Invariants:
        - retry_policy, watchdog, and keep_alive are non-null.
    """

    @classmethod
    def create_default(
        cls,
        url: str,
        max_retries: Optional[int] = None,
        timeout_seconds: float = 60.0,
        keep_alive_interval: float = 30.0,
        keep_alive_payload: str = "ping",
        connect_timeout: float = 10.0,
    ) -> "ReconnectingWebSocketManager":
        """Factory method to create a manager with standard resilient configuration."""
        if not isinstance(url, str):
            raise TypeError("url must be a string")
        if not url:
            raise ValueError("url cannot be empty")
        if max_retries is not None:
            if not isinstance(max_retries, int):
                raise TypeError("max_retries must be an integer")
            if max_retries < 0:
                raise ValueError("max_retries must be >= 0")
        if not isinstance(timeout_seconds, (int, float)):
            raise TypeError("timeout_seconds must be a number (int or float)")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be strictly positive")
        if not isinstance(keep_alive_interval, (int, float)):
            raise TypeError("keep_alive_interval must be a number (int or float)")
        if keep_alive_interval <= 0:
            raise ValueError("keep_alive_interval must be strictly positive")
        if not isinstance(keep_alive_payload, str):
            raise TypeError("keep_alive_payload must be a string")
        if not keep_alive_payload:
            raise ValueError("keep_alive_payload cannot be empty")
        if not isinstance(connect_timeout, (int, float)):
            raise TypeError("connect_timeout must be a float or integer")
        if connect_timeout <= 0:
            raise ValueError("connect_timeout must be strictly positive")

        from core.network.retry_policy import RetryPolicy
        from core.network.silence_watchdog import SilenceWatchdog
        from core.network.keep_alive_emitter import KeepAliveEmitter

        return cls(
            url=url,
            retry_policy=RetryPolicy(max_retries=max_retries),
            watchdog=SilenceWatchdog(timeout_seconds=timeout_seconds),
            keep_alive=KeepAliveEmitter(
                interval_seconds=keep_alive_interval,
                payload=keep_alive_payload,
            ),
            connect_timeout=connect_timeout,
        )

    def __init__(
        self,
        url: str,
        retry_policy: IRetryPolicy,
        watchdog: IWatchdog,
        keep_alive: IHeartbeat,
        connect_timeout: float = 10.0,
    ):
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
        if not isinstance(connect_timeout, (int, float)):
            raise TypeError("connect_timeout must be a float or integer")
        if connect_timeout <= 0:
            raise ValueError("connect_timeout must be strictly positive")

        self.__url = url
        self.__retry_policy = retry_policy
        self.__watchdog = watchdog
        self.__keep_alive = keep_alive
        self.__connect_timeout = float(connect_timeout)
        self.__ws: Optional[Any] = None
        self.__stop_event = asyncio.Event()
        self.__on_connect_callback: Optional[Callable[[], Awaitable[None]]] = None
        self.__health_task: Optional[asyncio.Task] = None
        self.__keep_alive_task: Optional[asyncio.Task] = None
        self.__log_seq = 0
        self.__flap_tip_monitor = PostFlapTipCorrelationMonitor(
            url=self.__url,
            now_wall_ms=self.__now_wall_ms,
            now_mono_ms=self.__now_monotonic_ms,
        )

    def __next_log_seq(self) -> int:
        self.__log_seq += 1
        return self.__log_seq

    def __now_wall_ms(self) -> int:
        return time.time_ns() // 1_000_000

    def __now_monotonic_ms(self) -> int:
        return time.monotonic_ns() // 1_000_000

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

    @property
    def connect_timeout(self) -> float:
        """[Completeness] Return the configured connection timeout in seconds."""
        return self.__connect_timeout

    def set_on_connect_callback(self, callback: Callable[[], Awaitable[None]]) -> None:
        """
        Set a callback to be executed upon successful connection.

        Preconditions:
            - callback must be an async callable.
        """
        if not callable(callback):
            raise TypeError("callback must be callable")
        if not inspect.iscoroutinefunction(callback):
            raise TypeError("callback must be an async function")
        self.__on_connect_callback = callback

    def set_tip_seq_provider(self, provider: Optional[TipSeqProvider]) -> None:
        """
        Bind an optional tip-seq sampler for post-flap correlation telemetry.

        When set, public WS flaps log tip_seq before/after reconnect and may emit
        ``post_flap_tip_stale`` if the tip does not advance in the observation window.
        """
        self.__flap_tip_monitor.set_tip_seq_provider(provider)

    @property
    def flap_tip_monitor(self) -> PostFlapTipCorrelationMonitor:
        """Return the post-flap tip correlation monitor (WATCH Day19 #2)."""
        return self.__flap_tip_monitor

    def is_stopped(self) -> bool:
        """Return True if the manager has been stopped."""
        return self.__stop_event.is_set()

    def is_connected(self) -> bool:
        """Return True if the WebSocket connection is currently active."""
        if self.__ws is None:
            return False
        return self.__is_websocket_open(self.__ws)

    def get_status_report(self) -> Dict[str, object]:
        """Return a snapshot of the manager connection state."""
        return {
            "url": self.__url,
            "is_connected": self.is_connected(),
            "is_stopped": self.is_stopped(),
            "connect_timeout": self.__connect_timeout,
        }

    async def wait_until_connected(self, poll_interval: float = 0.1) -> None:
        """
        Wait until the WebSocket connection is active.

        Preconditions:
            - poll_interval must be strictly positive.

        Raises:
            ConnectionError: if the manager stops before a connection is established.
        """
        if not isinstance(poll_interval, (int, float)):
            raise TypeError("poll_interval must be a number (int or float)")
        if poll_interval <= 0:
            raise ValueError("poll_interval must be strictly positive")

        while not self.is_connected():
            if self.is_stopped():
                raise ConnectionError(
                    "Manager stopped before connection was established."
                )
            await asyncio.sleep(poll_interval)

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
        if not self.is_connected():
            raise ConnectionError("WebSocket is not connected")

        await self.__ws.send(message)

    async def disconnect(self) -> None:
        """Gracefully close the current connection."""
        if self.__ws is not None:
            await self.__ws.close()
            self.__ws = None

    async def stop(self) -> None:
        """Stop the manager, cancel background tasks, and close any active connection."""
        self.__stop_event.set()
        self.__flap_tip_monitor.cancel()
        await self.__cancel_background_task(self.__health_task)
        await self.__cancel_background_task(self.__keep_alive_task)
        self.__health_task = None
        self.__keep_alive_task = None
        await self.disconnect()

    def __is_websocket_open(self, ws: Any) -> bool:
        """Evaluate whether a websocket client reports an open session."""
        if hasattr(ws, "state"):
            from websockets.protocol import State

            return ws.state == State.OPEN
        if hasattr(ws, "open"):
            return bool(ws.open)
        if hasattr(ws, "closed"):
            return not ws.closed
        return False

    async def __open_websocket_connection(
        self,
    ) -> tuple[Any, Any]:
        """Open a WebSocket session, applying connect_timeout to the handshake."""
        connect_ctx = websockets.connect(self.__url, ping_interval=None)
        try:
            ws = await asyncio.wait_for(
                connect_ctx.__aenter__(),
                timeout=self.__connect_timeout,
            )
        except Exception:
            await connect_ctx.__aexit__(*sys.exc_info())
            raise
        return connect_ctx, ws

    async def __close_websocket_connection(self, connect_ctx: Any) -> None:
        """Exit a websockets.connect context manager."""
        await connect_ctx.__aexit__(None, None, None)

    async def __health_loop(self) -> None:
        """Internal loop to monitor connection health via the watchdog."""
        try:
            while not self.__stop_event.is_set():  # pragma: no cover
                await asyncio.sleep(_HEALTH_CHECK_INTERVAL_SECONDS)
                if not self.__watchdog.check_health():  # pragma: no cover
                    logger.error(
                        "Watchdog: Délai dépassé. Coupure de la connexion forcée."
                    )
                    await self.__cancel_background_task(self.__keep_alive_task)
                    await self.disconnect()
                    break
        except asyncio.CancelledError:  # pragma: no cover
            pass

    async def __cancel_background_task(self, task: Optional[asyncio.Task]) -> None:
        """Cancel and await a background task, suppressing expected teardown errors."""
        if task is None:
            return
        if not task.done():
            task.cancel()
        try:
            await task
        except (asyncio.CancelledError, ConnectionError):
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

            attempt_seq = self.__next_log_seq()
            logger.info(
                "Connexion à %s (Tentative %s) seq=%s event_ts_ms=%s event_mono_ms=%s",
                self.__url,
                attempt + 1,
                attempt_seq,
                self.__now_wall_ms(),
                self.__now_monotonic_ms(),
            )
            connect_ctx = None
            health_task = None
            keep_alive_task = None
            last_close_wall_ms: Optional[int] = None
            last_close_mono_ms: Optional[int] = None

            try:
                connect_ctx, ws = await self.__open_websocket_connection()
                try:
                    self.__ws = ws
                    connected_seq = self.__next_log_seq()
                    reconnect_wall_ms = self.__now_wall_ms()
                    reconnect_mono_ms = self.__now_monotonic_ms()
                    logger.info(
                        "WebSocket connecté avec succès seq=%s event_ts_ms=%s event_mono_ms=%s",
                        connected_seq,
                        reconnect_wall_ms,
                        reconnect_mono_ms,
                    )
                    logger.debug(
                        "WebSocket session started url=%s",
                        self.__url,
                    )
                    attempt = 0
                    self.__watchdog.ping()
                    self.__flap_tip_monitor.note_connection_restored(
                        reconnect_wall_ms=reconnect_wall_ms,
                        reconnect_mono_ms=reconnect_mono_ms,
                    )

                    health_task = asyncio.create_task(self.__health_loop())
                    keep_alive_task = asyncio.create_task(
                        self.__keep_alive.run(self.send)
                    )
                    self.__health_task = health_task
                    self.__keep_alive_task = keep_alive_task

                    if self.__on_connect_callback:
                        await self.__on_connect_callback()

                    try:
                        async for message in ws:
                            self.__watchdog.ping()
                            yield message
                    except ConnectionClosed as e:
                        last_close_wall_ms = self.__now_wall_ms()
                        last_close_mono_ms = self.__now_monotonic_ms()
                        close_seq = self.__next_log_seq()
                        diagnostic = format_websocket_close_diagnostic(e)
                        logger.warning(
                            "WebSocket fermé seq=%s event_ts_ms=%s event_mono_ms=%s (%s).",
                            close_seq,
                            last_close_wall_ms,
                            last_close_mono_ms,
                            diagnostic,
                        )
                        self.__flap_tip_monitor.note_connection_closed(
                            close_wall_ms=last_close_wall_ms,
                            close_mono_ms=last_close_mono_ms,
                        )
                        preparing_seq = self.__next_log_seq()
                        logger.debug(
                            "WebSocket connection closed; preparing reconnect seq=%s close_event_mono_ms=%s",
                            preparing_seq,
                            last_close_mono_ms,
                        )
                finally:
                    await self.__close_websocket_connection(connect_ctx)
            except Exception as e:
                logger.error(f"Erreur réseau: {e}")
            finally:
                await self.__cancel_background_task(health_task)
                await self.__cancel_background_task(keep_alive_task)
                self.__health_task = None
                self.__keep_alive_task = None
                self.__ws = None

            if not self.__stop_event.is_set():
                delay = self.__retry_policy.get_delay(attempt)
                reconnect_seq = self.__next_log_seq()
                log_reconnect_countdown(
                    delay,
                    reconnect_seq,
                    last_close_mono_ms,
                    now_monotonic_ms=self.__now_monotonic_ms,
                )
                try:
                    await asyncio.wait_for(self.__stop_event.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
                attempt += 1
