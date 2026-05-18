import asyncio
import pytest
from core.models.trade_tick import TradeTick
from core.routing.async_queue_dispatcher import AsyncQueueDispatcher
from exchanges.bitget.bitget_tick_stream import BitgetTickStream
from exchanges.bitget.parsing.bitget_message_parser import BitgetMessageParser
from exchanges.bitget.bitget_subscription_protocol import BitgetSubscriptionProtocol
from core.network.reconnecting_ws_manager import ReconnectingWebSocketManager
from core.network.retry_policy import RetryPolicy
from core.network.silence_watchdog import SilenceWatchdog
from core.network.keep_alive_emitter import KeepAliveEmitter
from unittest.mock import patch

@pytest.fixture
def stream():
    # Niveau 2 : On utilise TOUS les composants réels
    network_manager = ReconnectingWebSocketManager(
        url="ws://fake",
        retry_policy=RetryPolicy(max_retries=1),
        watchdog=SilenceWatchdog(),
        keep_alive=KeepAliveEmitter()
    )
    return BitgetTickStream(
        network_manager=network_manager,
        subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
        parsing_strategy=BitgetMessageParser.create_default(),
        dispatch_strategy=AsyncQueueDispatcher(),
        symbols=["BTCUSDT"]
    )

@pytest.mark.asyncio
async def test_bottom_up_level2_bitget_stream_integration(stream):
    """
    Test de niveau 2 : BitgetTickStream intégrant tous les composants réels.
    Vérifie la chaîne : Socket -> NetworkMgr -> Parser -> Dispatcher -> Tick
    """
    class MockWS:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def __aiter__(self):
            # Message Trade Bitget réel
            yield '{"action":"snapshot","arg":{"channel":"trade","instId":"BTCUSDT"},"data":[{"ts":1710000000000,"price":"70000","size":"1","side":"buy","tradeId":"1"}]}'
            from websockets.exceptions import ConnectionClosedOK
            raise ConnectionClosedOK(None, None)
        async def send(self, data): pass
        async def recv(self): pass
        async def close(self): pass

    with patch("websockets.connect", return_value=MockWS()):
        task = asyncio.create_task(stream.start_streaming())
        
        # Consommation via le dispatcher réel
        tick = await stream.wait_for_next_tick()
        
        assert isinstance(tick, TradeTick)
        assert tick.inst_id == "BTCUSDT"
        assert tick.price == 70000.0
        
        await stream.stop()
        await task
