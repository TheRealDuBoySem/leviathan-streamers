import pytest
import orjson
from exchanges.bitget.bitget_subscription_protocol import BitgetSubscriptionProtocol

def test_protocol():
    p = BitgetSubscriptionProtocol("F")
    res = orjson.loads(p.format_subscribe(["BTC"]))
    assert res["op"] == "subscribe"

def test_protocol_unsubscribe():
    p = BitgetSubscriptionProtocol("F")
    res = orjson.loads(p.format_unsubscribe(["BTC"]))
    assert res["op"] == "unsubscribe"

def test_bitget_protocol_contracts():
    """Verify Design by Contract preconditions for BitgetSubscriptionProtocol."""
    with pytest.raises(ValueError, match="inst_type must be a non-empty string"):
        BitgetSubscriptionProtocol(inst_type="")
    with pytest.raises(ValueError, match="inst_type must be a non-empty string"):
        BitgetSubscriptionProtocol(inst_type=None)
    with pytest.raises(TypeError, match="inst_type must be a string"):
        BitgetSubscriptionProtocol(inst_type=123)
    
    p = BitgetSubscriptionProtocol(inst_type="mc")
    with pytest.raises(ValueError, match="symbols list cannot be empty"):
        p.format_subscribe([])
    with pytest.raises(ValueError, match="symbols list cannot be empty"):
        p.format_unsubscribe([])
    with pytest.raises(ValueError, match="symbols list cannot be empty"):
        p.format_subscribe(None)
    with pytest.raises(ValueError, match="symbols list cannot be empty"):
        p.format_unsubscribe(None)
        
    # Non-list validation
    with pytest.raises(TypeError, match="symbols must be a list"):
        p.format_subscribe("not a list")
    with pytest.raises(TypeError, match="symbols must be a list"):
        p.format_unsubscribe("not a list")
         
    # Symbol type validation
    with pytest.raises(TypeError, match="symbols must be strings"):
        p.format_subscribe(["BTC", 123])
    with pytest.raises(TypeError, match="symbols must be strings"):
        p.format_unsubscribe(["BTC", 123])
    with pytest.raises(ValueError, match="symbols must be non-empty strings"):
        p.format_subscribe(["BTC", ""])
    with pytest.raises(ValueError, match="symbols must be non-empty strings"):
        p.format_unsubscribe(["BTC", ""])


def test_bitget_protocol_properties():
    """Verify read-only properties of BitgetSubscriptionProtocol."""
    p = BitgetSubscriptionProtocol(inst_type="mc")
    assert p.inst_type == "mc"
    
    with pytest.raises(AttributeError):
        p.inst_type = "other"

