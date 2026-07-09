import pytest
from unittest.mock import AsyncMock, patch
from exchanges.bitget.bitget_tick_stream import BitgetTickStream
from core.network.reconnecting_ws_manager import ReconnectingWebSocketManager
from core.network.retry_policy import RetryPolicy
from core.network.silence_watchdog import SilenceWatchdog
from core.network.keep_alive_emitter import KeepAliveEmitter
from core.interfaces.base import (
    ISubscriptionStrategy, 
    IParsingStrategy, 
    IDispatchStrategy,
    IPriceObserver
)
from core.serialization.json_deserializer import JsonDeserializer
from core.routing.async_queue_dispatcher import AsyncQueueDispatcher
from exchanges.bitget.bitget_subscription_protocol import BitgetSubscriptionProtocol
from exchanges.bitget.parsing.bitget_message_parser import BitgetMessageParser
from exchanges.bitget.parsing.bitget_event_classifier import BitgetEventClassifier
from exchanges.bitget.parsing.bitget_trade_mapper import BitgetTradeMapper


def _capture_on_connect_callback(ws_manager):
    """Capture the on-connect callback registered during stream construction."""
    captured = []
    original = ws_manager.set_on_connect_callback

    def wrapper(callback):
        captured.append(callback)
        return original(callback)

    ws_manager.set_on_connect_callback = wrapper
    return captured, original


@pytest.fixture
def mock_ws_manager():
    return ReconnectingWebSocketManager(
        url="ws://test",
        retry_policy=RetryPolicy(),
        watchdog=SilenceWatchdog(),
        keep_alive=KeepAliveEmitter()
    )

@pytest.fixture
def mock_parser():
    return BitgetMessageParser(
        deserializer=JsonDeserializer(),
        classifier=BitgetEventClassifier(),
        trade_mapper=BitgetTradeMapper()
    )

@pytest.fixture
def stream(mock_ws_manager, mock_parser):
    return BitgetTickStream(
        network_manager=mock_ws_manager,
        subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
        parsing_strategy=mock_parser,
        dispatch_strategy=AsyncQueueDispatcher()
    )

@pytest.mark.asyncio
async def test_sub_methods(mocker, stream):
    mock_send = mocker.patch('core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.send', new_callable=AsyncMock)
    # Individual calls
    await stream.subscribe_symbol("ETH")
    assert mock_send.call_count == 1
    
    # Batch calls
    await stream.subscribe_symbols(["LTC", "XRP"])
    assert mock_send.call_count == 2
    assert set(stream.get_active_symbols()) == {"ETH", "LTC", "XRP"}
    
    await stream.unsubscribe_symbols(["ETH", "LTC"])
    assert mock_send.call_count == 3
    assert set(stream.get_active_symbols()) == {"XRP"}
    
    await stream.unsubscribe_symbol("XRP")
    assert mock_send.call_count == 4
    assert set(stream.get_active_symbols()) == set()

@pytest.mark.asyncio
async def test_batch_sub_methods(mocker, stream):
    """Test batch subscribe and unsubscribe methods."""
    mock_send = mocker.patch('core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.send', new_callable=AsyncMock)
    
    # Test batch subscribe
    await stream.subscribe_symbols(["BTC", "ETH"])
    assert mock_send.call_count == 1
    assert set(stream.get_active_symbols()) == {"BTC", "ETH"}
    
    # Test batch subscribe with duplicates (should only send unique new ones)
    await stream.subscribe_symbols(["BTC", "LTC"])
    assert mock_send.call_count == 2
    assert set(stream.get_active_symbols()) == {"BTC", "ETH", "LTC"}
    
    # Test batch unsubscribe
    await stream.unsubscribe_symbols(["BTC", "ETH"])
    assert mock_send.call_count == 3
    assert set(stream.get_active_symbols()) == {"LTC"}
    
    # Test batch unsubscribe non-existent
    await stream.unsubscribe_symbols(["XRP"])
    assert mock_send.call_count == 3
    assert set(stream.get_active_symbols()) == {"LTC"}

@pytest.mark.asyncio
async def test_resubscribe_empty(mocker, mock_ws_manager, mock_parser):
    captured, original = _capture_on_connect_callback(mock_ws_manager)
    stream = BitgetTickStream(
        network_manager=mock_ws_manager,
        subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
        parsing_strategy=mock_parser,
        dispatch_strategy=AsyncQueueDispatcher(),
    )
    mock_ws_manager.set_on_connect_callback = original

    mock_send = mocker.patch(
        "core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.send",
        new_callable=AsyncMock,
    )
    await captured[0]()
    assert mock_send.call_count == 0


@pytest.mark.asyncio
async def test_handle_connect_notifies_listeners_before_resubscribe(mocker, mock_ws_manager, mock_parser):
    captured, original = _capture_on_connect_callback(mock_ws_manager)
    stream = BitgetTickStream(
        network_manager=mock_ws_manager,
        subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
        parsing_strategy=mock_parser,
        dispatch_strategy=AsyncQueueDispatcher(),
        symbols=["BTC"],
    )
    mock_ws_manager.set_on_connect_callback = original

    call_order: list[str] = []

    async def on_reconnect() -> None:
        call_order.append("listener")

    stream.register_on_reconnect(on_reconnect)

    async def record_send(*_args, **_kwargs) -> None:
        call_order.append("resubscribe")

    mocker.patch(
        "core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.send",
        side_effect=record_send,
    )
    await captured[0]()
    assert call_order == ["listener", "resubscribe"]


@pytest.mark.asyncio
async def test_handle_connect_resubscribes_and_notifies_listeners(mocker, mock_ws_manager, mock_parser):
    captured, original = _capture_on_connect_callback(mock_ws_manager)
    stream = BitgetTickStream(
        network_manager=mock_ws_manager,
        subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
        parsing_strategy=mock_parser,
        dispatch_strategy=AsyncQueueDispatcher(),
        symbols=["BTC"],
    )
    mock_ws_manager.set_on_connect_callback = original

    mock_send = mocker.patch(
        "core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.send",
        new_callable=AsyncMock,
    )
    callback = AsyncMock()
    stream.register_on_reconnect(callback)
    await captured[0]()
    mock_send.assert_awaited_once()
    callback.assert_awaited_once()


def test_register_on_reconnect_validates_callback(stream):
    with pytest.raises(TypeError, match="callback must be a callable awaitable"):
        stream.register_on_reconnect(None)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="callback must be an async function"):
        stream.register_on_reconnect(lambda: None)  # type: ignore[arg-type]

@pytest.mark.asyncio
async def test_unsubscribe_method(mocker, mock_ws_manager, mock_parser):
    s = BitgetTickStream(
        network_manager=mock_ws_manager,
        subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
        parsing_strategy=mock_parser,
        dispatch_strategy=AsyncQueueDispatcher(),
        symbols=["BTC"]
    )
    mock_send = mocker.patch('core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.send', new_callable=AsyncMock)
    
    await s.unsubscribe_symbol("ETH") # Not in set
    assert mock_send.call_count == 0
    
    await s.unsubscribe_symbol("BTC") # In set
    assert mock_send.call_count == 1

@pytest.mark.asyncio
async def test_connect_and_stop(mocker, stream):
    async def mock_listen():
        yield "trade_msg"
        yield "sys_msg"
        yield "err_msg"
        pass
        
    mocker.patch('core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.start_connection_and_listen', side_effect=mock_listen)
    
    def mock_parse(self, m):
        from core.models.messages import TradeMessage, SystemMessage, ErrorMessage
        from core.models.trade_tick import TradeTick
        if m == "trade_msg": 
            tick = TradeTick(inst_id="BTC", ts=1, price=1.0, size=1.0, side="buy", trade_id="1")
            return TradeMessage(ticks=[tick])
        elif m == "sys_msg": return SystemMessage(event="info", msg="s")
        elif m == "err_msg": return ErrorMessage(msg="e")
        return None
        
    mocker.patch('exchanges.bitget.parsing.bitget_message_parser.BitgetMessageParser.parse', mock_parse)
    mock_dispatch = mocker.patch('core.routing.async_queue_dispatcher.AsyncQueueDispatcher.dispatch', new_callable=AsyncMock)
    
    await stream.start_streaming()
    assert mock_dispatch.call_count == 1
    
    mock_stop = mocker.patch('core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.stop', new_callable=AsyncMock)
    await stream.stop()
    assert mock_stop.call_count == 1

@pytest.mark.asyncio
async def test_delegated_methods(mocker, stream):
    mock_get = mocker.patch('core.routing.async_queue_dispatcher.AsyncQueueDispatcher.wait_for_next_tick', new_callable=AsyncMock)
    await stream.wait_for_next_tick()
    assert mock_get.call_count == 1
    
    mock_mark = mocker.patch('core.routing.async_queue_dispatcher.AsyncQueueDispatcher.mark_tick_as_processed')
    stream.mark_tick_as_processed()
    assert mock_mark.call_count == 1
    
    mocker.patch('core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.is_stopped', return_value=True)
    assert stream.is_stopped() is True

    mocker.patch('core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.is_connected', return_value=True)
    assert stream.is_connected() is True

    assert stream.get_active_symbols() == []

@pytest.mark.asyncio
async def test_bitget_tick_stream_aiter(mocker, mock_ws_manager, mock_parser):
    """Test the AsyncIterator interface of BitgetTickStream."""
    dispatcher = AsyncQueueDispatcher()
    stream = BitgetTickStream(mock_ws_manager, BitgetSubscriptionProtocol("mc"), mock_parser, dispatcher)
    
    from core.models.trade_tick import TradeTick
    tick = TradeTick(inst_id="BTC", ts=1, price=1.0, size=1.0, side="buy", trade_id="1")
    await dispatcher.dispatch(tick)
    
    mocker.patch('core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.is_stopped', side_effect=[False, True])
    
    ticks = []
    async for t in stream:
        ticks.append(t)
    
    assert len(ticks) == 1
    assert ticks[0].inst_id == "BTC"

@pytest.mark.asyncio
async def test_bitget_tick_stream_aiter_exception(mocker, mock_ws_manager, mock_parser):
    """Test exception handling in __aiter__."""
    dispatcher = AsyncQueueDispatcher()
    stream = BitgetTickStream(mock_ws_manager, BitgetSubscriptionProtocol("mc"), mock_parser, dispatcher)
    
    # Case 1: Exception while NOT stopped (should raise)
    mocker.patch.object(dispatcher, 'wait_for_next_tick', side_effect=RuntimeError("test error"))
    mocker.patch.object(stream, 'is_stopped', return_value=False)
    
    with pytest.raises(RuntimeError, match="test error"):
        async for _ in stream:
            pass
            
    # Case 2: Exception while STOPPED (should break)
    mocker.patch.object(stream, 'is_stopped', side_effect=[False, True])
    # The first call to __aiter__ checks is_stopped (False), then enters loop, then calls get_next_tick
    # We want it to fail, then check is_stopped (True) and break.
    
    ticks = []
    async for t in stream:
        ticks.append(t)
    assert len(ticks) == 0

@pytest.mark.asyncio
async def test_base_exchange_stream_observers(mocker, stream):
    """Test the Observer pattern in BaseExchangeStream via start_streaming."""
    observer = mocker.Mock(spec=IPriceObserver)
    observer.on_price_update = AsyncMock()
    stream.attach_observer(observer)

    from core.models.trade_tick import TradeTick
    from core.models.messages import TradeMessage

    tick = TradeTick(inst_id="BTC", ts=1, price=1.0, size=1.0, side="buy", trade_id="1")

    async def mock_listen():
        yield "trade_msg"

    mocker.patch.object(
        stream.network_manager,
        "start_connection_and_listen",
        side_effect=mock_listen,
    )
    mocker.patch.object(
        stream.parsing_strategy,
        "parse",
        return_value=TradeMessage(ticks=[tick]),
    )

    await stream.start_streaming()
    observer.on_price_update.assert_awaited_once_with(tick)

    stream.detach_observer(observer)
    observer.on_price_update.reset_mock()
    await stream.start_streaming()
    assert observer.on_price_update.call_count == 0

@pytest.mark.asyncio
async def test_start_streaming_logic(mocker, stream):
    """Test start_streaming logic for system and error messages."""
    from core.models.messages import SystemMessage, ErrorMessage
    
    async def mock_listen():
        yield "sys"
        yield "err"
        
    mocker.patch.object(
        stream.network_manager,
        "start_connection_and_listen",
        side_effect=mock_listen,
    )

    def mock_parse(msg):
        if msg == "sys":
            return SystemMessage(event="info", msg="sys_msg")
        if msg == "err":
            return ErrorMessage(msg="err_msg")
        return None

    mocker.patch.object(stream.parsing_strategy, "parse", side_effect=mock_parse)
    
    with patch('exchanges.base_stream.logger') as mock_logger:
        await stream.start_streaming()
        # Verify logger calls for system and error messages
        mock_logger.info.assert_any_call("sys_msg")
        mock_logger.error.assert_any_call("err_msg")

@pytest.mark.asyncio
async def test_bitget_tick_stream_contracts(mock_ws_manager, mock_parser):
    """Verify Design by Contract preconditions for BitgetTickStream."""
    from core.interfaces.base import ISubscriptionStrategy, IParsingStrategy, IDispatchStrategy
    from core.network.reconnecting_ws_manager import ReconnectingWebSocketManager
    
    with pytest.raises(TypeError, match="network_manager"):
        BitgetTickStream(None, BitgetSubscriptionProtocol("mc"), mock_parser, AsyncQueueDispatcher())
    with pytest.raises(TypeError, match="subscription_strategy"):
        BitgetTickStream(mock_ws_manager, None, mock_parser, AsyncQueueDispatcher())
    with pytest.raises(TypeError, match="parsing_strategy"):
        BitgetTickStream(mock_ws_manager, BitgetSubscriptionProtocol("mc"), None, AsyncQueueDispatcher())
    with pytest.raises(TypeError, match="dispatch_strategy"):
        BitgetTickStream(mock_ws_manager, BitgetSubscriptionProtocol("mc"), mock_parser, None)

    with pytest.raises(TypeError, match="symbols must be a list"):
        BitgetTickStream(
            mock_ws_manager,
            BitgetSubscriptionProtocol("mc"),
            mock_parser,
            AsyncQueueDispatcher(),
            symbols="BTC",
        )
    
    s = BitgetTickStream(mock_ws_manager, BitgetSubscriptionProtocol("mc"), mock_parser, AsyncQueueDispatcher())
    with pytest.raises(ValueError, match="symbol cannot be empty"):
        await s.subscribe_symbol("")
    with pytest.raises(ValueError, match="symbol cannot be empty"):
        await s.unsubscribe_symbol("")

@pytest.mark.asyncio
async def test_base_stream_more_contracts(stream):
    """Verify more advanced Design by Contract validations for BaseExchangeStream."""
    # Observers validation
    with pytest.raises(TypeError, match="observer must be an IPriceObserver instance"):
        stream.attach_observer("not an observer")
    with pytest.raises(TypeError, match="observer must be an IPriceObserver instance"):
        stream.detach_observer("not an observer")
        
    # Subscribe single validation
    with pytest.raises(TypeError, match="symbol must be a string"):
        await stream.subscribe_symbol(123)
    with pytest.raises(TypeError, match="symbol must be a string"):
        await stream.unsubscribe_symbol(123)
    with pytest.raises(ValueError, match="symbol cannot be empty"):
        await stream.subscribe_symbol(None)
    with pytest.raises(ValueError, match="symbol cannot be empty"):
        await stream.unsubscribe_symbol(None)
        
    # Subscribe batch validation
    with pytest.raises(ValueError, match="symbols list cannot be empty"):
        await stream.subscribe_symbols(None)
    with pytest.raises(TypeError, match="symbols must be a list"):
        await stream.subscribe_symbols("not a list")
    with pytest.raises(ValueError, match="symbols list cannot be empty"):
        await stream.subscribe_symbols([])
    with pytest.raises(TypeError, match="symbols must be strings"):
        await stream.subscribe_symbols(["BTC", 123])
    with pytest.raises(ValueError, match="symbols must be non-empty strings"):
        await stream.subscribe_symbols(["BTC", ""])
        
    with pytest.raises(ValueError, match="symbols list cannot be empty"):
        await stream.unsubscribe_symbols(None)
    with pytest.raises(TypeError, match="symbols must be a list"):
        await stream.unsubscribe_symbols("not a list")
    with pytest.raises(ValueError, match="symbols list cannot be empty"):
        await stream.unsubscribe_symbols([])
    with pytest.raises(TypeError, match="symbols must be strings"):
        await stream.unsubscribe_symbols(["BTC", 123])
    with pytest.raises(ValueError, match="symbols must be non-empty strings"):
        await stream.unsubscribe_symbols(["BTC", ""])

def test_base_stream_properties(stream):
    """Verify read-only properties of BaseExchangeStream."""
    assert stream.registry is not None
    assert stream.subscription_strategy is not None
    assert stream.parsing_strategy is not None
    assert stream.dispatch_strategy is not None
    assert stream.network_manager is not None
    assert isinstance(stream.observers, list)
    
    with pytest.raises(AttributeError):
        stream.registry = None
    with pytest.raises(AttributeError):
        stream.subscription_strategy = None
    with pytest.raises(AttributeError):
        stream.parsing_strategy = None
    with pytest.raises(AttributeError):
        stream.dispatch_strategy = None
    with pytest.raises(AttributeError):
        stream.network_manager = None
    with pytest.raises(AttributeError):
        stream.observers = None

@pytest.mark.asyncio
async def test_wait_until_connected(mocker, stream):
    """Verify wait_until_connected method of BaseExchangeStream."""
    # Case 1: Already connected
    mocker.patch.object(stream, 'is_connected', return_value=True)
    await stream.wait_until_connected() # Should return immediately
    
    # Case 2: Connects after a short sleep
    mocker.patch.object(stream, 'is_connected', side_effect=[False, True])
    await stream.wait_until_connected()
    
    # Case 3: Stopped before connection (should raise ConnectionError)
    mocker.patch.object(stream, 'is_connected', return_value=False)
    mocker.patch.object(stream, 'is_stopped', return_value=True)
    with pytest.raises(ConnectionError, match="Le flux a été arrêté avant de pouvoir se connecter."):
        await stream.wait_until_connected()


