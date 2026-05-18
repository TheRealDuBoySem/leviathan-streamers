import pytest
from exchanges.bitget.parsing.bitget_trade_mapper import BitgetTradeMapper

def test_mapper():
    data = {"arg": {"instId": "BTC"}, "data": [{"ts": 1, "price": "1", "size": "1", "side": "buy", "tradeId": "1"}]}
    ticks = BitgetTradeMapper.map(data)
    assert ticks[0].inst_id == "BTC"
    assert ticks[0].side == "buy"

def test_mapper_exception():
    # ts is not an int
    data = {"arg": {"instId": "BTC"}, "data": [{"ts": "invalid", "price": "1", "size": "1", "side": "buy", "tradeId": "1"}]}
    ticks = BitgetTradeMapper.map(data)
    assert len(ticks) == 0

def test_mapper_contracts():
    """Verify Design by Contract preconditions for BitgetTradeMapper."""
    with pytest.raises(TypeError, match="Expected dict"):
        BitgetTradeMapper.map(None)
    
    # Test invalid trades list
    assert BitgetTradeMapper.map({"data": "not a list"}) == []
    
    # Test validation error on tick
    data = {"data": [{"side": "invalid"}]}
    assert BitgetTradeMapper.map(data) == []

def test_mapper_generic_exception():
    # Trigger Exception branch by passing something that causes an unexpected error in TradeTick instantiation
    # e.g. trade is None, so trade.get raises AttributeError
    data = {"data": [None]}
    assert BitgetTradeMapper.map(data) == []

def test_mapper_constants():
    """Verify class-level constants of BitgetTradeMapper."""
    assert BitgetTradeMapper.DEFAULT_INST_ID == "UNKNOWN"
    
    # Test fallback to DEFAULT_INST_ID
    data = {"data": [{"ts": 1, "price": "1", "size": "1", "side": "buy", "tradeId": "1"}]}
    ticks = BitgetTradeMapper.map(data)
    assert len(ticks) == 1
    assert ticks[0].inst_id == BitgetTradeMapper.DEFAULT_INST_ID

