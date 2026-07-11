import pytest
from exchanges.bitget.bitget_stream_factory import BitgetStreamFactory
from exchanges.bitget.bitget_tick_stream import BitgetTickStream
from core.network.reconnecting_ws_manager import ReconnectingWebSocketManager
from core.routing.async_queue_dispatcher import AsyncQueueDispatcher
from exchanges.bitget.bitget_subscription_protocol import BitgetSubscriptionProtocol
from exchanges.bitget.parsing.bitget_message_parser import BitgetMessageParser


def test_bitget_stream_factory_create():
    """Test that the factory creates a correctly configured stream."""
    stream = BitgetStreamFactory.create_stream(
        url="ws://test",
        symbols=["BTCUSDT"],
        inst_type="mc",
    )

    assert isinstance(stream, BitgetTickStream)
    assert isinstance(stream.network_manager, ReconnectingWebSocketManager)
    assert isinstance(stream.subscription_strategy, BitgetSubscriptionProtocol)
    assert isinstance(stream.parsing_strategy, BitgetMessageParser)
    assert isinstance(stream.dispatch_strategy, AsyncQueueDispatcher)
    assert stream.get_active_symbols() == ["BTCUSDT"]
    assert stream.subscription_strategy.inst_type == "mc"


def test_bitget_stream_factory_create_default():
    """Test the convenience factory with standard network defaults."""
    stream = BitgetStreamFactory.create_default(
        url="ws://test",
        symbols=["ETHUSDT"],
    )

    assert isinstance(stream, BitgetTickStream)
    assert stream.get_active_symbols() == ["ETHUSDT"]
    assert stream.subscription_strategy.inst_type == BitgetStreamFactory.DEFAULT_INST_TYPE
    assert stream.network_manager.connect_timeout == 10.0


def test_bitget_stream_factory_contracts():
    """Verify Design by Contract preconditions for BitgetStreamFactory."""
    with pytest.raises(TypeError, match="url must be a string"):
        BitgetStreamFactory.create_stream(url=123)
    with pytest.raises(ValueError, match="url cannot be empty"):
        BitgetStreamFactory.create_stream(url="")

    with pytest.raises(TypeError, match="inst_type must be a string"):
        BitgetStreamFactory.create_stream(url="ws://test", inst_type=123)
    with pytest.raises(ValueError, match="inst_type cannot be empty"):
        BitgetStreamFactory.create_stream(url="ws://test", inst_type="")

    with pytest.raises(TypeError, match="symbols must be a list"):
        BitgetStreamFactory.create_stream(url="ws://test", symbols="not a list")
    with pytest.raises(TypeError, match="symbols must be strings"):
        BitgetStreamFactory.create_stream(url="ws://test", symbols=["BTC", 123])
    with pytest.raises(ValueError, match="symbols must be non-empty strings"):
        BitgetStreamFactory.create_stream(url="ws://test", symbols=["BTC", ""])


def test_bitget_stream_factory_constants():
    """Verify class-level constants of BitgetStreamFactory."""
    assert BitgetStreamFactory.DEFAULT_INST_TYPE == "USDT-FUTURES"

    stream = BitgetStreamFactory.create_stream(url="ws://test")
    payload = stream.subscription_strategy.format_subscribe(["BTC"])
    assert BitgetStreamFactory.DEFAULT_INST_TYPE in payload


def test_bitget_stream_factory_dispatch_strategy():
    """Verify custom dispatch strategy injection and contract validation."""
    custom_dispatcher = AsyncQueueDispatcher()
    stream = BitgetStreamFactory.create_stream(
        url="ws://test",
        dispatch_strategy=custom_dispatcher,
    )
    assert stream.dispatch_strategy is custom_dispatcher

    with pytest.raises(TypeError, match="dispatch_strategy must be a IDispatchStrategy instance"):
        BitgetStreamFactory.create_stream(url="ws://test", dispatch_strategy=object())


def test_bitget_stream_factory_timeout_propagation():
    """Verify that timeout/keep_alive/connect parameters propagate correctly."""
    stream = BitgetStreamFactory.create_stream(
        url="ws://test",
        symbols=["BTCUSDT"],
        max_retries=5,
        timeout_seconds=45,
        keep_alive_interval=25,
        keep_alive_payload="custom_ping",
        connect_timeout=8.0,
    )
    mgr = stream.network_manager
    assert mgr.connect_timeout == 8.0
    assert mgr.watchdog.timeout_seconds == 45
    assert mgr.keep_alive.interval_seconds == 25
    assert mgr.keep_alive.payload == "custom_ping"
    assert mgr.retry_policy.max_retries == 5

    with pytest.raises(TypeError, match="max_retries must be an integer"):
        BitgetStreamFactory.create_stream(url="ws://test", max_retries="5")
    with pytest.raises(TypeError, match="timeout_seconds must be an integer"):
        BitgetStreamFactory.create_stream(url="ws://test", timeout_seconds="45")
    with pytest.raises(TypeError, match="keep_alive_interval must be an integer"):
        BitgetStreamFactory.create_stream(url="ws://test", keep_alive_interval="25")
    with pytest.raises(TypeError, match="keep_alive_payload must be a string"):
        BitgetStreamFactory.create_stream(url="ws://test", keep_alive_payload=123)
    with pytest.raises(TypeError, match="connect_timeout must be a float or integer"):
        BitgetStreamFactory.create_stream(url="ws://test", connect_timeout="8.0")
    with pytest.raises(ValueError, match="connect_timeout must be strictly positive"):
        BitgetStreamFactory.create_stream(url="ws://test", connect_timeout=0)
