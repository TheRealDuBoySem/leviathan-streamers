import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
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


def test_unregister_on_reconnect_validates_and_removes_callback(stream):
    async def on_reconnect() -> None:
        return None

    stream.register_on_reconnect(on_reconnect)
    with pytest.raises(TypeError, match="callback must be a callable awaitable"):
        stream.unregister_on_reconnect(None)  # type: ignore[arg-type]
    stream.unregister_on_reconnect(on_reconnect)


def test_initial_symbols_validation_rejects_invalid_entries(mock_ws_manager, mock_parser):
    with pytest.raises(TypeError, match="symbols must be strings"):
        BitgetTickStream(
            network_manager=mock_ws_manager,
            subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
            parsing_strategy=mock_parser,
            dispatch_strategy=AsyncQueueDispatcher(),
            symbols=["BTC", 123],
        )
    with pytest.raises(ValueError, match="symbols must be non-empty strings"):
        BitgetTickStream(
            network_manager=mock_ws_manager,
            subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
            parsing_strategy=mock_parser,
            dispatch_strategy=AsyncQueueDispatcher(),
            symbols=[""],
        )


@pytest.mark.asyncio
async def test_handle_connect_logs_callback_errors(mocker, mock_ws_manager, mock_parser, caplog):
    import logging

    captured, original = _capture_on_connect_callback(mock_ws_manager)
    stream = BitgetTickStream(
        network_manager=mock_ws_manager,
        subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
        parsing_strategy=mock_parser,
        dispatch_strategy=AsyncQueueDispatcher(),
        symbols=["BTC"],
    )
    mock_ws_manager.set_on_connect_callback = original
    handle_connect = captured[0]

    async def failing_callback() -> None:
        raise RuntimeError("boom")

    stream.register_on_reconnect(failing_callback)
    mocker.patch.object(stream, "_resubscribe_all", new_callable=AsyncMock)

    with caplog.at_level(logging.ERROR):
        await handle_connect()

    assert any("Error in stream on_reconnect callback" in record.message for record in caplog.records)

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
    assert stream.confirmation_tracker is not None
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
    with pytest.raises(AttributeError):
        stream.confirmation_tracker = None

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


@pytest.mark.asyncio
async def test_start_streaming_ignores_none_parse_result(mocker, stream):
    """Parse returning None is a valid no-op per IParsingStrategy contract."""
    async def mock_listen():
        yield "ignored"

    mocker.patch.object(
        stream.network_manager,
        "start_connection_and_listen",
        side_effect=mock_listen,
    )
    mocker.patch.object(stream.parsing_strategy, "parse", return_value=None)
    mock_dispatch = mocker.patch.object(
        stream.dispatch_strategy, "dispatch", new_callable=AsyncMock
    )

    await stream.start_streaming()
    mock_dispatch.assert_not_awaited()


@pytest.mark.asyncio
async def test_start_streaming_continues_after_parse_error(mocker, stream, caplog):
    """A single malformed message must not stop the streaming loop."""
    import logging

    async def mock_listen():
        yield "bad"
        yield "good"

    mocker.patch.object(
        stream.network_manager,
        "start_connection_and_listen",
        side_effect=mock_listen,
    )

    from core.models.trade_tick import TradeTick
    from core.models.messages import TradeMessage

    tick = TradeTick(inst_id="BTC", ts=1, price=1.0, size=1.0, side="buy", trade_id="1")

    def mock_parse(msg):
        if msg == "bad":
            raise ValueError("malformed")
        return TradeMessage(ticks=[tick])

    mocker.patch.object(stream.parsing_strategy, "parse", side_effect=mock_parse)
    mock_dispatch = mocker.patch.object(
        stream.dispatch_strategy, "dispatch", new_callable=AsyncMock
    )

    with caplog.at_level(logging.ERROR):
        await stream.start_streaming()

    mock_dispatch.assert_awaited_once()
    assert any("failed to process websocket message" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_start_streaming_propagates_cancelled_error(mocker, stream):
    """CancelledError must propagate for graceful task shutdown."""
    async def mock_listen():
        yield "msg"
        raise asyncio.CancelledError()

    mocker.patch.object(
        stream.network_manager,
        "start_connection_and_listen",
        side_effect=mock_listen,
    )
    mocker.patch.object(stream.parsing_strategy, "parse", return_value=None)

    with pytest.raises(asyncio.CancelledError):
        await stream.start_streaming()


@pytest.mark.asyncio
async def test_notify_observers_isolates_observer_failures(mocker, stream, caplog):
    """A failing observer must not prevent other observers or dispatch from running."""
    import logging

    healthy = mocker.Mock(spec=IPriceObserver)
    healthy.on_price_update = AsyncMock()
    failing = mocker.Mock(spec=IPriceObserver)
    failing.on_price_update = AsyncMock(side_effect=RuntimeError("observer boom"))

    stream.attach_observer(failing)
    stream.attach_observer(healthy)

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
    mock_dispatch = mocker.patch.object(
        stream.dispatch_strategy, "dispatch", new_callable=AsyncMock
    )

    with caplog.at_level(logging.ERROR):
        await stream.start_streaming()

    mock_dispatch.assert_awaited_once_with(tick)
    healthy.on_price_update.assert_awaited_once_with(tick)
    assert any("Error in stream price observer" in record.message for record in caplog.records)


def test_network_manager_duck_typing_accepts_compatible_manager(mock_parser):
    """Network manager validation depends on behavior, not concrete class type."""
    class CompatibleNetworkManager:
        def set_on_connect_callback(self, callback):
            self._callback = callback

        async def start_connection_and_listen(self):
            if False:
                yield ""

        async def send(self, message):
            return None

        async def stop(self):
            return None

        def is_stopped(self):
            return False

        def is_connected(self):
            return False

    manager = CompatibleNetworkManager()
    stream = BitgetTickStream(
        network_manager=manager,
        subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
        parsing_strategy=mock_parser,
        dispatch_strategy=AsyncQueueDispatcher(),
    )
    assert stream.network_manager is manager


def test_bitget_stream_rejects_network_manager_without_streaming_contract(mock_parser):
    class IncompleteManager:
        def start_connection_and_listen(self):
            return None

    with pytest.raises(TypeError, match="network_manager must provide a callable"):
        BitgetTickStream(
            IncompleteManager(),  # type: ignore[arg-type]
            subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
            parsing_strategy=mock_parser,
            dispatch_strategy=AsyncQueueDispatcher(),
        )


@pytest.mark.asyncio
async def test_resubscribe_logs_requested_symbols_and_tracks_confirmations(
    mocker, mock_ws_manager, mock_parser, caplog
):
    """After reconnect, requested symbols are logged and confirmations tracked."""
    import logging

    captured, original = _capture_on_connect_callback(mock_ws_manager)
    stream = BitgetTickStream(
        network_manager=mock_ws_manager,
        subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
        parsing_strategy=mock_parser,
        dispatch_strategy=AsyncQueueDispatcher(),
        symbols=["BTCUSDT", "ETHUSDT", "XRPUSDT"],
        confirmation_timeout_seconds=0.05,
    )
    mock_ws_manager.set_on_connect_callback = original
    mocker.patch(
        "core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.send",
        new_callable=AsyncMock,
    )

    with caplog.at_level(logging.INFO):
        await captured[0]()

    assert any(
        "Requête globale d'abonnement" in record.message
        and "BTCUSDT" in record.message
        and "ETHUSDT" in record.message
        and "XRPUSDT" in record.message
        for record in caplog.records
    )
    assert set(stream.confirmation_tracker.get_expected_symbols()) == {
        "BTCUSDT",
        "ETHUSDT",
        "XRPUSDT",
    }


@pytest.mark.asyncio
async def test_start_streaming_records_subscribe_acks_and_warns_on_partial(
    mocker, mock_ws_manager, mock_parser, caplog
):
    """Partial confirmations after reconnect produce an explicit WARNING."""
    import logging

    captured, original = _capture_on_connect_callback(mock_ws_manager)
    stream = BitgetTickStream(
        network_manager=mock_ws_manager,
        subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
        parsing_strategy=mock_parser,
        dispatch_strategy=AsyncQueueDispatcher(),
        symbols=["BTCUSDT", "ETHUSDT", "XRPUSDT"],
        confirmation_timeout_seconds=0.08,
    )
    mock_ws_manager.set_on_connect_callback = original
    mocker.patch(
        "core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.send",
        new_callable=AsyncMock,
    )
    await captured[0]()

    async def mock_listen():
        yield orjson_subscribe_ack("XRPUSDT")
        await asyncio.sleep(0.12)

    mocker.patch.object(
        stream.network_manager,
        "start_connection_and_listen",
        side_effect=mock_listen,
    )

    with caplog.at_level(logging.WARNING):
        await stream.start_streaming()

    assert any(
        "Confirmation partielle d'abonnement" in record.message
        and "XRPUSDT" in record.message
        and "BTCUSDT" in record.message
        and "ETHUSDT" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_start_streaming_logs_full_confirmation_after_reconnect(
    mocker, mock_ws_manager, mock_parser, caplog
):
    """All acks before timeout log requested vs confirmed as complete."""
    import logging

    captured, original = _capture_on_connect_callback(mock_ws_manager)
    stream = BitgetTickStream(
        network_manager=mock_ws_manager,
        subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
        parsing_strategy=mock_parser,
        dispatch_strategy=AsyncQueueDispatcher(),
        symbols=["BTCUSDT", "XRPUSDT"],
        confirmation_timeout_seconds=1.0,
    )
    mock_ws_manager.set_on_connect_callback = original
    mocker.patch(
        "core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.send",
        new_callable=AsyncMock,
    )
    await captured[0]()

    async def mock_listen():
        yield orjson_subscribe_ack("BTCUSDT")
        yield orjson_subscribe_ack("XRPUSDT")

    mocker.patch.object(
        stream.network_manager,
        "start_connection_and_listen",
        side_effect=mock_listen,
    )

    with caplog.at_level(logging.INFO):
        await stream.start_streaming()

    assert any(
        "Abonnements confirmés après reconnect" in record.message
        and "BTCUSDT" in record.message
        and "XRPUSDT" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_full_confirmation_arms_write_liveness_then_stale_without_writes(
    mocker, mock_ws_manager, mock_parser, caplog
):
    """BB-B5-A1: sub OK after reconnect but no journal write → write-liveness stale."""
    import logging

    from core.state.post_reconnect_write_liveness import PostReconnectWriteLivenessGuard

    stale_calls: list[set[str]] = []
    guard = PostReconnectWriteLivenessGuard(
        timeout_seconds=0.05,
        min_heal_interval_seconds=0.0,
        on_stale=lambda symbols, _elapsed: stale_calls.append(set(symbols)),
    )

    captured, original = _capture_on_connect_callback(mock_ws_manager)
    stream = BitgetTickStream(
        network_manager=mock_ws_manager,
        subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
        parsing_strategy=mock_parser,
        dispatch_strategy=AsyncQueueDispatcher(),
        symbols=["XRPUSDT"],
        confirmation_timeout_seconds=1.0,
        write_liveness_guard=guard,
    )
    mock_ws_manager.set_on_connect_callback = original
    mocker.patch(
        "core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.send",
        new_callable=AsyncMock,
    )
    await captured[0]()

    async def mock_listen():
        yield orjson_subscribe_ack("XRPUSDT")
        await asyncio.sleep(0.15)

    mocker.patch.object(
        stream.network_manager,
        "start_connection_and_listen",
        side_effect=mock_listen,
    )

    with caplog.at_level(logging.CRITICAL):
        await stream.start_streaming()

    assert stale_calls == [{"XRPUSDT"}]
    assert any("BB-B5-A1" in record.message for record in caplog.records)
    assert stream.write_liveness_guard is guard


@pytest.mark.asyncio
async def test_j21_h20_flap_confirm_tip_stagnant_triggers_dual_heal(
    mocker, mock_ws_manager, mock_parser, caplog, tmp_path
):
    """J21 H20 BB-B5-A1 regression: flap→~1.25s reconnect→sub XRPUSDT OK→tip frozen.

    Chronology (beta 2026-08-01 @20:15):
      WS public close → reconnect ~1.25s → sub XRPUSDT confirmed → tip stays
      ``1684815``, 0 ticks. Collector must CRITICAL + self-heal on both paths:
      post-flap tip correlation AND post-confirm write-liveness.
    """
    import logging

    from core.journal.journal_dispatch_decorator import JournalDispatchDecorator
    from core.journal.tick_journal import TickJournal
    from core.routing.sink_dispatch_strategy import SinkDispatchStrategy
    from core.state.post_reconnect_write_liveness import (
        COLLECTOR_WRITE_LIVENESS_EXIT_CODE,
        PostReconnectWriteLivenessGuard,
    )

    tip_seq = 1_684_815
    write_stale: list[set[str]] = []
    flap_stale: list[tuple[int, int, float]] = []

    guard = PostReconnectWriteLivenessGuard(
        timeout_seconds=0.05,
        min_heal_interval_seconds=0.0,
        on_stale=lambda symbols, _elapsed: write_stale.append(set(symbols)),
    )
    # Real journal-backed dispatch (as in run_collector); tip seq pinned below to
    # the J21 frozen value without writing 1.6M ticks.
    journal = TickJournal(str(tmp_path))
    dispatch = JournalDispatchDecorator(
        SinkDispatchStrategy(),
        journal,
        write_liveness_guard=guard,
    )

    captured, original = _capture_on_connect_callback(mock_ws_manager)
    stream = BitgetTickStream(
        network_manager=mock_ws_manager,
        subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
        parsing_strategy=mock_parser,
        dispatch_strategy=dispatch,
        symbols=["XRPUSDT"],
        confirmation_timeout_seconds=1.0,
        write_liveness_guard=guard,
    )
    mock_ws_manager.set_on_connect_callback = original
    # Pin tip sampler to J21 frozen seq (journal may be empty in unit test).
    mock_ws_manager.set_tip_seq_provider(lambda: tip_seq)
    mock_ws_manager.set_on_post_flap_tip_stale(
        lambda before, now, elapsed: flap_stale.append((before, now, elapsed))
    )
    mock_ws_manager.flap_tip_monitor.set_stale_window_seconds(0.05)
    mock_ws_manager.flap_tip_monitor.set_min_heal_interval_seconds(0.0)

    mocker.patch(
        "core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.send",
        new_callable=AsyncMock,
    )

    # Boot connect (gen=1) — satisfy first write-liveness so only the post-flap
    # confirm window is under test (mirrors long-lived collector before H20 flap).
    await captured[0]()
    assert stream.connect_generation == 1
    guard.arm_after_subscriptions_confirmed({"XRPUSDT"}, connect_generation=1)
    guard.record_journal_write()
    assert guard.is_awaiting_write() is False

    async def mock_listen():
        yield orjson_subscribe_ack("XRPUSDT")
        await asyncio.sleep(0.18)

    mocker.patch.object(
        stream.network_manager,
        "start_connection_and_listen",
        side_effect=mock_listen,
    )

    with caplog.at_level(logging.INFO):
        # J21 H20 flap: close → reconnect ~1.25s, tip unchanged at 1684815.
        mock_ws_manager.flap_tip_monitor.note_connection_closed(
            close_wall_ms=1_000, close_mono_ms=100
        )
        mock_ws_manager.flap_tip_monitor.note_connection_restored(
            reconnect_wall_ms=2_250, reconnect_mono_ms=1_350
        )

        # Post-flap connect (gen=2) cancels any prior write window; re-arm after confirm.
        await captured[0]()
        assert stream.connect_generation == 2
        await stream.start_streaming()

    assert write_stale == [{"XRPUSDT"}]
    assert len(flap_stale) == 1
    assert flap_stale[0][:2] == (tip_seq, tip_seq)
    assert any(
        "post_flap_correlation" in r.message
        and "tip_seq_before=1684815" in r.message
        and "since_close_ms=1250" in r.message
        for r in caplog.records
    )
    assert any(
        "post_flap_tip_stale" in r.message and r.levelno >= logging.CRITICAL
        for r in caplog.records
    )
    assert any(
        r.levelno >= logging.CRITICAL and "BB-B5-A1" in r.message
        for r in caplog.records
    )
    assert COLLECTOR_WRITE_LIVENESS_EXIT_CODE == 1


@pytest.mark.asyncio
async def test_first_boot_confirm_arms_write_liveness_mute_tip_heal(
    mocker, mock_ws_manager, mock_parser, caplog
):
    """Day20 WATCH #3: first connect post-OS (connect_generation=1) must arm + heal.

    Mirrors H06: WS public UP + sub confirmed, tip frozen → collector self-heal.
    """
    import logging

    from core.state.post_reconnect_write_liveness import PostReconnectWriteLivenessGuard

    stale_calls: list[set[str]] = []
    guard = PostReconnectWriteLivenessGuard(
        timeout_seconds=0.05,
        min_heal_interval_seconds=0.0,
        on_stale=lambda symbols, _elapsed: stale_calls.append(set(symbols)),
    )

    captured, original = _capture_on_connect_callback(mock_ws_manager)
    stream = BitgetTickStream(
        network_manager=mock_ws_manager,
        subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
        parsing_strategy=mock_parser,
        dispatch_strategy=AsyncQueueDispatcher(),
        symbols=["XRPUSDT"],
        confirmation_timeout_seconds=1.0,
        write_liveness_guard=guard,
    )
    mock_ws_manager.set_on_connect_callback = original
    mocker.patch(
        "core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.send",
        new_callable=AsyncMock,
    )
    # First boot only — no prior reconnect cycle.
    assert stream.connect_generation == 0
    await captured[0]()
    assert stream.connect_generation == 1

    async def mock_listen():
        yield orjson_subscribe_ack("XRPUSDT")
        await asyncio.sleep(0.15)

    mocker.patch.object(
        stream.network_manager,
        "start_connection_and_listen",
        side_effect=mock_listen,
    )

    with caplog.at_level(logging.INFO):
        await stream.start_streaming()

    assert guard.arm_count == 1
    assert stale_calls == [{"XRPUSDT"}]
    assert any(
        "first_boot=True" in record.message or "connect_generation=1" in record.message
        for record in caplog.records
    )
    assert any(
        record.levelno >= logging.CRITICAL and "BB-B5-A1" in record.message
        for record in caplog.records
    )


def test_bitget_tick_stream_rejects_invalid_write_liveness_guard(
    mock_ws_manager, mock_parser
):
    with pytest.raises(TypeError, match="write_liveness_guard must be a PostReconnectWriteLivenessGuard"):
        BitgetTickStream(
            network_manager=mock_ws_manager,
            subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
            parsing_strategy=mock_parser,
            dispatch_strategy=AsyncQueueDispatcher(),
            symbols=["XRPUSDT"],
            write_liveness_guard=object(),  # type: ignore[arg-type]
        )


def test_tip_seq_provider_skips_non_callable_latest_seq(
    mocker, mock_ws_manager, mock_parser, tmp_path
):
    from core.journal.journal_dispatch_decorator import JournalDispatchDecorator
    from core.journal.tick_journal import TickJournal
    from core.routing.sink_dispatch_strategy import SinkDispatchStrategy

    journal = TickJournal(str(tmp_path))
    journal.latest_seq = 42  # shadow method with non-callable attribute
    dispatch = JournalDispatchDecorator(SinkDispatchStrategy(), journal)

    mgr = mock_ws_manager
    set_provider = mocker.MagicMock()
    mgr.set_tip_seq_provider = set_provider
    BitgetTickStream(
        network_manager=mgr,
        subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
        parsing_strategy=mock_parser,
        dispatch_strategy=dispatch,
        symbols=["XRPUSDT"],
    )
    set_provider.assert_not_called()


def test_tip_seq_provider_returns_none_when_journal_latest_is_none(
    mocker, mock_ws_manager, mock_parser, tmp_path
):
    from core.journal.journal_dispatch_decorator import JournalDispatchDecorator
    from core.journal.tick_journal import TickJournal
    from core.routing.sink_dispatch_strategy import SinkDispatchStrategy

    journal = TickJournal(str(tmp_path))
    mocker.patch.object(journal, "latest_seq", return_value=None)
    dispatch = JournalDispatchDecorator(SinkDispatchStrategy(), journal)
    mgr = mock_ws_manager
    set_provider = mocker.MagicMock()
    mgr.set_tip_seq_provider = set_provider
    BitgetTickStream(
        network_manager=mgr,
        subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
        parsing_strategy=mock_parser,
        dispatch_strategy=dispatch,
        symbols=["XRPUSDT"],
    )
    set_provider.assert_called_once()
    assert set_provider.call_args.args[0]() is None


@pytest.mark.asyncio
async def test_stop_cancels_write_liveness_guard(mocker, mock_ws_manager, mock_parser):
    from core.state.post_reconnect_write_liveness import PostReconnectWriteLivenessGuard

    guard = PostReconnectWriteLivenessGuard(timeout_seconds=5.0)
    cancel = mocker.spy(guard, "cancel")
    stream = BitgetTickStream(
        network_manager=mock_ws_manager,
        subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
        parsing_strategy=mock_parser,
        dispatch_strategy=AsyncQueueDispatcher(),
        symbols=["XRPUSDT"],
        write_liveness_guard=guard,
    )
    mocker.patch.object(mock_ws_manager, "stop", new_callable=AsyncMock)
    await stream.stop()
    cancel.assert_called()


@pytest.mark.asyncio
async def test_stop_cancels_pending_confirmation_watchdog(
    mocker, mock_ws_manager, mock_parser, caplog
):
    import logging

    captured, original = _capture_on_connect_callback(mock_ws_manager)
    stream = BitgetTickStream(
        network_manager=mock_ws_manager,
        subscription_strategy=BitgetSubscriptionProtocol(inst_type="mc"),
        parsing_strategy=mock_parser,
        dispatch_strategy=AsyncQueueDispatcher(),
        symbols=["BTCUSDT"],
        confirmation_timeout_seconds=0.2,
    )
    mock_ws_manager.set_on_connect_callback = original
    mocker.patch(
        "core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.send",
        new_callable=AsyncMock,
    )
    mock_stop = mocker.patch(
        "core.network.reconnecting_ws_manager.ReconnectingWebSocketManager.stop",
        new_callable=AsyncMock,
    )
    await captured[0]()
    assert stream.confirmation_tracker.is_expectation_active()

    with caplog.at_level(logging.WARNING):
        await stream.stop()
        await asyncio.sleep(0.25)

    mock_stop.assert_awaited_once()
    assert not any(
        "Confirmation partielle d'abonnement" in record.message
        for record in caplog.records
    )
    assert stream.confirmation_tracker.is_expectation_active() is False


def orjson_subscribe_ack(symbol: str) -> str:
    import orjson

    return orjson.dumps(
        {
            "event": "subscribe",
            "arg": {"instType": "mc", "channel": "trade", "instId": symbol},
        }
    ).decode("utf-8")


@pytest.mark.asyncio
async def test_start_streaming_propagates_cancelled_error_from_parse(mocker, stream):
    async def mock_listen():
        yield "msg"

    mocker.patch.object(
        stream.network_manager,
        "start_connection_and_listen",
        side_effect=mock_listen,
    )
    mocker.patch.object(stream.parsing_strategy, "parse", side_effect=asyncio.CancelledError())

    with pytest.raises(asyncio.CancelledError):
        await stream.start_streaming()
