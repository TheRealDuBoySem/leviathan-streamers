import pytest
from core.models.trade_tick import TradeTick

def test_trade_tick_contracts():
    """Verify Design by Contract preconditions for TradeTick."""
    with pytest.raises(ValueError, match="inst_id cannot be empty"):
        TradeTick(inst_id="", ts=1, price=1.0, size=1.0, side="buy", trade_id="1")
    with pytest.raises(ValueError, match="ts must be positive"):
        TradeTick(inst_id="BTC", ts=0, price=1.0, size=1.0, side="buy", trade_id="1")
    with pytest.raises(ValueError, match="price must be positive"):
        TradeTick(inst_id="BTC", ts=1, price=-1.0, size=1.0, side="buy", trade_id="1")
    with pytest.raises(ValueError, match="size must be positive"):
        TradeTick(inst_id="BTC", ts=1, price=1.0, size=0, side="buy", trade_id="1")
    with pytest.raises(ValueError, match="side must be 'buy' or 'sell'"):
        TradeTick(inst_id="BTC", ts=1, price=1.0, size=1.0, side="invalid", trade_id="1")
    with pytest.raises(ValueError, match="trade_id cannot be empty"):
        TradeTick(inst_id="BTC", ts=1, price=1.0, size=1.0, side="buy", trade_id="")

def test_trade_tick_types():
    """Verify Type contract preconditions for TradeTick."""
    with pytest.raises(TypeError, match="inst_id must be a string"):
        TradeTick(inst_id=123, ts=1, price=1.0, size=1.0, side="buy", trade_id="1")
    with pytest.raises(TypeError, match="ts must be an integer"):
        TradeTick(inst_id="BTC", ts=1.5, price=1.0, size=1.0, side="buy", trade_id="1")
    with pytest.raises(TypeError, match="price must be a number"):
        TradeTick(inst_id="BTC", ts=1, price="1.0", size=1.0, side="buy", trade_id="1")
    with pytest.raises(TypeError, match="size must be a number"):
        TradeTick(inst_id="BTC", ts=1, price=1.0, size="1.0", side="buy", trade_id="1")
    with pytest.raises(TypeError, match="side must be a string"):
        TradeTick(inst_id="BTC", ts=1, price=1.0, size=1.0, side=123, trade_id="1")
    with pytest.raises(TypeError, match="trade_id must be a string"):
        TradeTick(inst_id="BTC", ts=1, price=1.0, size=1.0, side="buy", trade_id=123)

def test_trade_tick_notional():
    """Verify the notional property calculation."""
    tick = TradeTick(inst_id="BTC", ts=1, price=50000.0, size=0.5, side="buy", trade_id="1")
    assert tick.notional == 25000.0

