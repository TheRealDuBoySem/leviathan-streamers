import pytest
from core.models.messages import TradeMessage, SystemMessage, ErrorMessage
from core.models.trade_tick import TradeTick

def test_trade_message_invariants():
    # Valid trade message
    tick = TradeTick(inst_id="BTC", ts=1, price=1.0, size=1.0, side="buy", trade_id="1")
    msg = TradeMessage(ticks=[tick])
    assert msg.ticks == [tick]

    # Type error for ticks list
    with pytest.raises(TypeError, match="ticks doit être une liste"):
        TradeMessage(ticks="not a list")

    # Value error for empty ticks list
    with pytest.raises(ValueError, match="ticks ne peut pas être vide"):
        TradeMessage(ticks=[])

    # Type error for element in ticks list
    with pytest.raises(TypeError, match="Tous les éléments de ticks doivent être des instances de TradeTick"):
        TradeMessage(ticks=["not a tick"])

def test_system_message_invariants():
    # Valid system message
    msg = SystemMessage(event="subscribe", msg="Success")
    assert msg.event == "subscribe"
    assert msg.msg == "Success"
    assert msg.symbol is None

    msg_with_symbol = SystemMessage(event="subscribe", msg="ok", symbol="XRPUSDT")
    assert msg_with_symbol.symbol == "XRPUSDT"

    # Value error for empty event
    with pytest.raises(ValueError, match="event ne peut pas être vide"):
        SystemMessage(event="", msg="Success")

    # Type error for non-string msg
    with pytest.raises(TypeError, match="msg doit être une chaîne de caractères"):
        SystemMessage(event="subscribe", msg=123)

    with pytest.raises(TypeError, match="symbol must be a string when provided"):
        SystemMessage(event="subscribe", msg="ok", symbol=123)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="symbol cannot be empty when provided"):
        SystemMessage(event="subscribe", msg="ok", symbol="")

def test_error_message_invariants():
    # Valid error message
    msg = ErrorMessage(msg="Failed connection")
    assert msg.msg == "Failed connection"

    # Value error for empty msg
    with pytest.raises(ValueError, match="msg ne peut pas être vide"):
        ErrorMessage(msg="")
