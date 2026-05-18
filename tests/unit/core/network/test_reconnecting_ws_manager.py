import pytest
import asyncio
from unittest.mock import AsyncMock, patch
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
        keep_alive_interval=15
    )
    assert isinstance(mgr, ReconnectingWebSocketManager)
    # Check new public inspectable properties
    assert mgr.url == "ws://test"
    assert mgr.retry_policy.max_retries == 10
    assert mgr.watchdog.timeout_seconds == 45
    assert mgr.keep_alive.interval_seconds == 15

    # Check new default of max_retries=None
    mgr_default = ReconnectingWebSocketManager.create_default(url="ws://test")
    assert mgr_default.retry_policy.max_retries is None


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
        with patch("asyncio.sleep", return_value=None):
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

def test_is_connected_variations():
    mgr = ReconnectingWebSocketManager("ws://t", RetryPolicy(), SilenceWatchdog(), KeepAliveEmitter())
    
    # 1. Test when __ws has state attribute
    from websockets.protocol import State
    class MockWSWithState:
        def __init__(self, state):
            self.state = state
            
    mgr._ReconnectingWebSocketManager__ws = MockWSWithState(State.OPEN)
    assert mgr.is_connected() is True
    mgr._ReconnectingWebSocketManager__ws = MockWSWithState(State.CLOSED)
    assert mgr.is_connected() is False
    
    # 2. Test when __ws has legacy open attribute
    class MockWSWithOpen:
        def __init__(self, open_val):
            self.open = open_val
            
    mgr._ReconnectingWebSocketManager__ws = MockWSWithOpen(True)
    assert mgr.is_connected() is True
    mgr._ReconnectingWebSocketManager__ws = MockWSWithOpen(False)
    assert mgr.is_connected() is False
    
    # 3. Test fallback returning False
    class MockWSNoAttr:
        pass
        
    mgr._ReconnectingWebSocketManager__ws = MockWSNoAttr()
    assert mgr.is_connected() is False

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
    with pytest.raises(TypeError, match="message must be a string"):
        asyncio.run(mgr.send(123))
    with pytest.raises(ValueError, match="message cannot be empty"):
        asyncio.run(mgr.send(""))

    # 3. Type validation tests for create_default factory method
    with pytest.raises(TypeError, match="url must be a string"):
        ReconnectingWebSocketManager.create_default(url=123)
    with pytest.raises(TypeError, match="max_retries must be an integer"):
        ReconnectingWebSocketManager.create_default(url="ws://t", max_retries="5")
    with pytest.raises(TypeError, match="timeout_seconds must be an integer"):
        ReconnectingWebSocketManager.create_default(url="ws://t", timeout_seconds="60")
    with pytest.raises(TypeError, match="keep_alive_interval must be an integer"):
        ReconnectingWebSocketManager.create_default(url="ws://t", keep_alive_interval="30")


@pytest.mark.asyncio
async def test_health_loop_watchdog_failure():
    from core.interfaces.base import IWatchdog
    class FailingWatchdog(IWatchdog):
        def ping(self): pass
        def check_health(self): return False
    
    mock_ws = BaseMockWS()
    mgr = ReconnectingWebSocketManager("ws://t", RetryPolicy(max_retries=2), FailingWatchdog(), KeepAliveEmitter())
    
    async def fast_health_loop():
        await mgr.disconnect()

    with patch("websockets.connect", return_value=mock_ws):
        with patch.object(mgr, "_health_loop", side_effect=fast_health_loop):
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
        with patch("asyncio.sleep", return_value=None):
            async for msg in mgr.start_connection_and_listen():
                if msg == "msg1":
                    await mgr.stop()
                    break
    
    assert mgr.is_stopped() is True
