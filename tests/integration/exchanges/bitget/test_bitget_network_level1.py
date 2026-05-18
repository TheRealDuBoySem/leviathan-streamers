import pytest
from core.network.reconnecting_ws_manager import ReconnectingWebSocketManager, MaxRetriesExceededError
from core.network.retry_policy import RetryPolicy
from core.network.silence_watchdog import SilenceWatchdog
from core.network.keep_alive_emitter import KeepAliveEmitter
from unittest.mock import patch

@pytest.fixture
def real_network_manager():
    return ReconnectingWebSocketManager(
        url="ws://fake",
        retry_policy=RetryPolicy(max_retries=1),
        watchdog=SilenceWatchdog(),
        keep_alive=KeepAliveEmitter()
    )

@pytest.mark.asyncio
async def test_bottom_up_level1_network_integration(real_network_manager):
    """
    Test de niveau 1 : ReconnectingWebSocketManager avec ses VRAIS composants de bas niveau.
    - RetryPolicy
    - SilenceWatchdog
    - KeepAliveEmitter
    """
    class MockWS:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def __aiter__(self):
            yield '{"action":"pong"}'
            from websockets.exceptions import ConnectionClosedOK
            raise ConnectionClosedOK(None, None)
        async def send(self, data): pass
        async def recv(self): pass
        async def close(self): pass

    with patch("websockets.connect", return_value=MockWS()):
        try:
            async for msg in real_network_manager.start_connection_and_listen():
                assert msg == '{"action":"pong"}'
        except MaxRetriesExceededError:
            pass
        
    assert real_network_manager.is_connected() is False
    # Vérifie que le watchdog réel a bien été pingué par le manager réel
    assert real_network_manager._ReconnectingWebSocketManager__watchdog.check_health() is True
