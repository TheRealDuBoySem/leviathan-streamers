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
        inst_type="mc"
    )
    
    assert isinstance(stream, BitgetTickStream)
    # Check dependencies (using private attributes for verification)
    assert isinstance(stream._network_manager, ReconnectingWebSocketManager)
    assert isinstance(stream._subscription_strategy, BitgetSubscriptionProtocol)
    assert isinstance(stream._parsing_strategy, BitgetMessageParser)
    assert isinstance(stream._dispatch_strategy, AsyncQueueDispatcher)
    assert stream.get_active_symbols() == ["BTCUSDT"]

def test_bitget_stream_factory_contracts():
    """Verify Design by Contract preconditions for BitgetStreamFactory."""
    # URL validations
    with pytest.raises(TypeError, match="url must be a string"):
        BitgetStreamFactory.create_stream(url=123)
    with pytest.raises(ValueError, match="url cannot be empty"):
        BitgetStreamFactory.create_stream(url="")
        
    # inst_type validations
    with pytest.raises(TypeError, match="inst_type must be a string"):
        BitgetStreamFactory.create_stream(url="ws://test", inst_type=123)
    with pytest.raises(ValueError, match="inst_type cannot be empty"):
        BitgetStreamFactory.create_stream(url="ws://test", inst_type="")
        
    # symbols validations
    with pytest.raises(TypeError, match="symbols must be a list"):
        BitgetStreamFactory.create_stream(url="ws://test", symbols="not a list")
    with pytest.raises(TypeError, match="symbols must be strings"):
        BitgetStreamFactory.create_stream(url="ws://test", symbols=["BTC", 123])
    with pytest.raises(ValueError, match="symbols must be non-empty strings"):
        BitgetStreamFactory.create_stream(url="ws://test", symbols=["BTC", ""])

def test_bitget_stream_factory_constants():
    """Verify class-level constants of BitgetStreamFactory."""
    assert BitgetStreamFactory.DEFAULT_INST_TYPE == "USDT-FUTURES"
    
    # Test fallback to DEFAULT_INST_TYPE
    stream = BitgetStreamFactory.create_stream(url="ws://test")
    payload = stream._subscription_strategy.format_subscribe(["BTC"])
    assert BitgetStreamFactory.DEFAULT_INST_TYPE in payload

