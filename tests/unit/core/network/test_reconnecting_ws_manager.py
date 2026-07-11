import pytest
import asyncio
from unittest.mock import AsyncMock, patch

_original_sleep = asyncio.sleep

async def mock_sleep_yield(delay):
    await _original_sleep(0)

from core.network.reconnecting_ws_manager import ReconnectingWebSocketManager, MaxRetriesExceededError
from core.network.retry_policy import RetryPolicy
from core.network.silence_watchdog import SilenceWatchdog
from core.network.keep_alive_emitter import KeepAliveEmitter

@pytest.mark.asyncio
async def test_manager_stop():
    mgr = ReconnectingWebSocketManager("ws://t", RetryPolicy(), SilenceWatchdog(), KeepAliveEmitter())
    assert not mgr.is_connected()
    await mgr.stop()
    assert mgr.is_stopped()

@pytest.mark.asyncio
async def test_connect_fail():
    mgr = ReconnectingWebSocketManager("ws://t", RetryPolicy(max_retries=0), SilenceWatchdog(), KeepAliveEmitter())
    with pytest.raises(MaxRetriesExceededError):
        async for _ in mgr.start_connection_and_listen(): pass

def test_reconnecting_ws_manager_factory():
    """Test the factory method create_default."""
    mgr = ReconnectingWebSocketManager.create_default(
        url="ws://test",
        max_retries=10,
        timeout_seconds=45,
        keep_alive_interval=15,
        keep_alive_payload="custom_heartbeat"
    )
    assert isinstance(mgr, ReconnectingWebSocketManager)
    # Check new public inspectable properties
    assert mgr.url == "ws://test"
    assert mgr.retry_policy.max_retries == 10
    assert mgr.watchdog.timeout_seconds == 45
    assert mgr.keep_alive.interval_seconds == 15
    assert mgr.keep_alive.payload == "custom_heartbeat"
    assert mgr.connect_timeout == 10.0

    # Check new default of max_retries=None
    mgr_default = ReconnectingWebSocketManager.create_default(url="ws://test")
    assert mgr_default.retry_policy.max_retries is None
    assert mgr_default.keep_alive.payload == "ping"

    # Check keep_alive_payload type check
    with pytest.raises(TypeError, match="keep_alive_payload must be a string"):
        ReconnectingWebSocketManager.create_default(url="ws://test", keep_alive_payload=123)

    with pytest.raises(ValueError, match="url cannot be empty"):
        ReconnectingWebSocketManager.create_default(url="")

    with pytest.raises(ValueError, match="connect_timeout must be strictly positive"):
        ReconnectingWebSocketManager.create_default(url="ws://test", connect_timeout=0)


    # Check that they are read-only
    with pytest.raises(AttributeError):
        mgr.url = "new_url"
    with pytest.raises(AttributeError):
        mgr.retry_policy = None
    with pytest.raises(AttributeError):
        mgr.watchdog = None
    with pytest.raises(AttributeError):
        mgr.keep_alive = None


class BaseMockWS:
    def __init__(self):
        self.send = AsyncMock()
        self.close = AsyncMock(side_effect=self._set_closed)
        self.closed = False
    async def _set_closed(self, *args, **kwargs):
        self.closed = True
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    async def __aiter__(self):
        yield "msg1"
        while not self.closed:
            await asyncio.sleep(0.01)

@pytest.mark.asyncio
async def test_listen_loop_success():
    mgr = ReconnectingWebSocketManager("ws://t", RetryPolicy(max_retries=1), SilenceWatchdog(), KeepAliveEmitter())
    mock_ws = BaseMockWS()
    
    with patch("websockets.connect", return_value=mock_ws):
        async for msg in mgr.start_connection_and_listen():
            assert msg == "msg1"
            await mgr.stop()
            break

@pytest.mark.asyncio
async def test_listen_loop_retry_on_error():
    # Attempt 0 fails (Exception), Attempt 1 succeeds. max_retries=2 means attempt < 2 is OK (0, 1).
    mgr = ReconnectingWebSocketManager("ws://t", RetryPolicy(max_retries=2), SilenceWatchdog(), KeepAliveEmitter())
    with patch("websockets.connect", side_effect=[Exception("net error"), BaseMockWS()]):
        with patch("asyncio.sleep", mock_sleep_yield):
            async for msg in mgr.start_connection_and_listen():
                if msg == "msg1":
                    await mgr.stop()
                    break

@pytest.mark.asyncio
async def test_on_connect_callback():
    mgr = ReconnectingWebSocketManager("ws://t", RetryPolicy(max_retries=1), SilenceWatchdog(), KeepAliveEmitter())
    callback = AsyncMock()
    mgr.set_on_connect_callback(callback)
    with patch("websockets.connect", return_value=BaseMockWS()):
        async for _ in mgr.start_connection_and_listen():
            callback.assert_awaited_once()
            await mgr.stop()
            break

@pytest.mark.asyncio
async def test_send_and_disconnect():
    mgr = ReconnectingWebSocketManager("ws://t", RetryPolicy(max_retries=1), SilenceWatchdog(), KeepAliveEmitter())
    mock_ws = BaseMockWS()
    with patch("websockets.connect", return_value=mock_ws):
        async for _ in mgr.start_connection_and_listen():
            assert mgr.is_connected()
            await mgr.send("hello")
            mock_ws.send.assert_awaited_with("hello")
            await mgr.disconnect()
            assert not mgr.is_connected()
            await mgr.stop()
            break

@pytest.mark.asyncio
async def test_is_connected_variations():
    from websockets.protocol import State

    class StateMockWS(BaseMockWS):
        def __init__(self):
            super().__init__()
            self.state = State.OPEN

        async def close(self, *args, **kwargs):
            self.state = State.CLOSED
            await super().close(*args, **kwargs)

    class OpenAttrMockWS(BaseMockWS):
        def __init__(self):
            super().__init__()
            self.open = True

        async def close(self, *args, **kwargs):
            self.open = False
            await super().close(*args, **kwargs)

    class ClosedAttrMockWS(BaseMockWS):
        pass

    class UnknownStateMockWS:
        def __init__(self):
            self.send = AsyncMock()
            self.close = AsyncMock()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def __aiter__(self):
            yield "msg"

    for mock_ws in (StateMockWS(), OpenAttrMockWS(), ClosedAttrMockWS()):
        mgr = ReconnectingWebSocketManager(
            "ws://t", RetryPolicy(max_retries=1), SilenceWatchdog(), KeepAliveEmitter()
        )
        assert mgr.is_connected() is False
        with patch("websockets.connect", return_value=mock_ws):
            async for _ in mgr.start_connection_and_listen():
                assert mgr.is_connected() is True
                await mgr.disconnect()
                assert mgr.is_connected() is False
                await mgr.stop()
                break

    unknown_mgr = ReconnectingWebSocketManager(
        "ws://t", RetryPolicy(max_retries=1), SilenceWatchdog(), KeepAliveEmitter()
    )
    with patch("websockets.connect", return_value=UnknownStateMockWS()):
        async for _ in unknown_mgr.start_connection_and_listen():
            assert unknown_mgr.is_connected() is False
            await unknown_mgr.stop()
            break

def test_contracts():
    mgr = ReconnectingWebSocketManager("ws://t", RetryPolicy(), SilenceWatchdog(), KeepAliveEmitter())
    
    # 1. Type validation tests for constructor
    with pytest.raises(TypeError, match="url must be a string"):
        ReconnectingWebSocketManager(123, RetryPolicy(), SilenceWatchdog(), KeepAliveEmitter())
    with pytest.raises(ValueError, match="url cannot be empty"):
        ReconnectingWebSocketManager("", RetryPolicy(), SilenceWatchdog(), KeepAliveEmitter())
    with pytest.raises(TypeError, match="retry_policy must be a IRetryPolicy instance"):
        ReconnectingWebSocketManager("ws://t", None, SilenceWatchdog(), KeepAliveEmitter())
    with pytest.raises(TypeError, match="watchdog must be a IWatchdog instance"):
        ReconnectingWebSocketManager("ws://t", RetryPolicy(), None, KeepAliveEmitter())
    with pytest.raises(TypeError, match="keep_alive must be a IHeartbeat instance"):
        ReconnectingWebSocketManager("ws://t", RetryPolicy(), SilenceWatchdog(), None)
        
    # 2. Type validation tests for callback and send
    with pytest.raises(TypeError, match="callback must be callable"):
        mgr.set_on_connect_callback("not a callback")
    with pytest.raises(TypeError, match="callback must be an async function"):
        mgr.set_on_connect_callback(lambda: None)
    with pytest.raises(TypeError, match="message must be a string"):
        asyncio.run(mgr.send(123))
    with pytest.raises(ValueError, match="message cannot be empty"):
        asyncio.run(mgr.send(""))
    with pytest.raises(ConnectionError, match="WebSocket is not connected"):
        asyncio.run(mgr.send("hello"))

    # 3. Type validation tests for create_default factory method
    with pytest.raises(TypeError, match="url must be a string"):
        ReconnectingWebSocketManager.create_default(url=123)
    with pytest.raises(TypeError, match="max_retries must be an integer"):
        ReconnectingWebSocketManager.create_default(url="ws://t", max_retries="5")
    with pytest.raises(TypeError, match="timeout_seconds must be a number"):
        ReconnectingWebSocketManager.create_default(url="ws://t", timeout_seconds="60")
    with pytest.raises(ValueError, match="timeout_seconds must be strictly positive"):
        ReconnectingWebSocketManager.create_default(url="ws://t", timeout_seconds=0)
    with pytest.raises(TypeError, match="keep_alive_interval must be a number"):
        ReconnectingWebSocketManager.create_default(url="ws://t", keep_alive_interval="30")
    with pytest.raises(ValueError, match="keep_alive_interval must be strictly positive"):
        ReconnectingWebSocketManager.create_default(url="ws://t", keep_alive_interval=0)
    with pytest.raises(ValueError, match="max_retries must be >= 0"):
        ReconnectingWebSocketManager.create_default(url="ws://t", max_retries=-1)
    with pytest.raises(ValueError, match="keep_alive_payload cannot be empty"):
        ReconnectingWebSocketManager.create_default(url="ws://t", keep_alive_payload="")


@pytest.mark.asyncio
async def test_health_loop_watchdog_failure():
    from core.interfaces.base import IWatchdog
    class FailingWatchdog(IWatchdog):
        def ping(self): pass
        def check_health(self): return False
    
    mock_ws = BaseMockWS()
    mgr = ReconnectingWebSocketManager("ws://t", RetryPolicy(max_retries=2), FailingWatchdog(), KeepAliveEmitter())
    
    with patch("websockets.connect", return_value=mock_ws):
        with patch("asyncio.sleep", mock_sleep_yield):
            async for _ in mgr.start_connection_and_listen():
                if mock_ws.closed:
                    await mgr.stop()
                    break
    
    assert mock_ws.closed is True

@pytest.mark.asyncio
async def test_listen_loop_connection_closed_exception():
    from websockets.exceptions import ConnectionClosed
    # Use max_retries=2 to allow attempt 0 and attempt 1.
    mgr = ReconnectingWebSocketManager("ws://t", RetryPolicy(max_retries=2), SilenceWatchdog(), KeepAliveEmitter())
    
    class ErrorMockWS(BaseMockWS):
        async def __aiter__(self):
            yield "msg"
            raise ConnectionClosed(None, None)

    with patch("websockets.connect", side_effect=[ErrorMockWS(), BaseMockWS()]):
        with patch("asyncio.sleep", mock_sleep_yield):
            async for msg in mgr.start_connection_and_listen():
                if msg == "msg1":
                    await mgr.stop()
                    break
    
    assert mgr.is_stopped() is True


@pytest.mark.asyncio
async def test_reconnecting_ws_manager_connection_closed_code_and_await_cancel():
    from websockets.exceptions import ConnectionClosed
    mgr = ReconnectingWebSocketManager("ws://t", RetryPolicy(max_retries=1), SilenceWatchdog(), KeepAliveEmitter())
    
    class FakeWS(BaseMockWS):
        async def __aiter__(self):
            raise ConnectionClosed(None, None)
            yield  # pragma: no cover - makes this an async generator, not a coroutine

    with patch("websockets.connect", return_value=FakeWS()):
        try:
            async for _ in mgr.start_connection_and_listen():
                pass
        except Exception:
            pass

    assert mgr.is_connected() is False


@pytest.mark.asyncio
async def test_reconnecting_ws_manager_connect_timeout_retry():
    mgr = ReconnectingWebSocketManager(
        url="ws://t",
        retry_policy=RetryPolicy(max_retries=1),
        watchdog=SilenceWatchdog(),
        keep_alive=KeepAliveEmitter(),
        connect_timeout=0.01
    )
    
    class SlowConnect(BaseMockWS):
        async def __aenter__(self):
            await _original_sleep(1.0)
            return self  # pragma: no cover - cancelled by connect_timeout before returning

    with patch("websockets.connect", return_value=SlowConnect()):
        with pytest.raises(MaxRetriesExceededError):
            async for _ in mgr.start_connection_and_listen():
                pass  # pragma: no cover - connection never succeeds


def test_get_status_report():
    mgr = ReconnectingWebSocketManager("ws://t", RetryPolicy(), SilenceWatchdog(), KeepAliveEmitter())
    report = mgr.get_status_report()
    assert report == {
        "url": "ws://t",
        "is_connected": False,
        "is_stopped": False,
        "connect_timeout": 10.0,
    }


@pytest.mark.asyncio
async def test_wait_until_connected_success():
    mgr = ReconnectingWebSocketManager("ws://t", RetryPolicy(max_retries=1), SilenceWatchdog(), KeepAliveEmitter())
    with patch("websockets.connect", return_value=BaseMockWS()):
        listen_task = asyncio.create_task(
            _run_listen_until_stopped(mgr),
            name="listen-task",
        )
        await asyncio.sleep(0)
        await mgr.wait_until_connected(poll_interval=0.01)
        assert mgr.is_connected()
        await mgr.stop()
        await listen_task


async def _run_listen_until_stopped(mgr):
    async for _ in mgr.start_connection_and_listen():
        if mgr.is_stopped():
            break


@pytest.mark.asyncio
async def test_wait_until_connected_stopped_raises():
    mgr = ReconnectingWebSocketManager("ws://t", RetryPolicy(max_retries=0), SilenceWatchdog(), KeepAliveEmitter())
    await mgr.stop()
    with pytest.raises(ConnectionError, match="Manager stopped before connection was established"):
        await mgr.wait_until_connected(poll_interval=0.01)


def test_wait_until_connected_invalid_poll_interval():
    mgr = ReconnectingWebSocketManager("ws://t", RetryPolicy(), SilenceWatchdog(), KeepAliveEmitter())
    with pytest.raises(ValueError, match="poll_interval must be strictly positive"):
        asyncio.run(mgr.wait_until_connected(poll_interval=0))


@pytest.mark.asyncio
async def test_wait_until_connected_rejects_non_numeric_poll_interval():
    mgr = ReconnectingWebSocketManager("ws://t", RetryPolicy(), SilenceWatchdog(), KeepAliveEmitter())
    with pytest.raises(TypeError, match="poll_interval must be a number"):
        await mgr.wait_until_connected(poll_interval="0.1")  # type: ignore[arg-type]


def test_reconnecting_ws_manager_invalid_connect_timeout():
    with pytest.raises(TypeError, match="connect_timeout must be a float or integer"):
        ReconnectingWebSocketManager("ws://t", RetryPolicy(), SilenceWatchdog(), KeepAliveEmitter(), connect_timeout="invalid")
        
    with pytest.raises(ValueError, match="connect_timeout must be strictly positive"):
        ReconnectingWebSocketManager("ws://t", RetryPolicy(), SilenceWatchdog(), KeepAliveEmitter(), connect_timeout=0)
        
    with pytest.raises(TypeError, match="connect_timeout must be a float or integer"):
        ReconnectingWebSocketManager.create_default(url="ws://t", connect_timeout="invalid")

