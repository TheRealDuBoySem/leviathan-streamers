import pytest
from exchanges.bitget.parsing.bitget_event_classifier import BitgetEventClassifier

def test_classifier():
    c = BitgetEventClassifier.classify
    assert c({"action": "snapshot", "arg": {"channel": "trade"}}) == BitgetEventClassifier.TRADE
    assert c({"event": "subscribe"}) == BitgetEventClassifier.SUBSCRIBE
    assert c({"error": "x"}) == BitgetEventClassifier.ERROR
    assert c({"other": "y"}) == BitgetEventClassifier.UNKNOWN

def test_classifier_contracts():
    """Verify Design by Contract preconditions for BitgetEventClassifier."""
    with pytest.raises(TypeError, match="Expected dict"):
        BitgetEventClassifier.classify(None)

def test_classifier_constants():
    """Verify the definition of classifier constants."""
    assert BitgetEventClassifier.TRADE == "trade"
    assert BitgetEventClassifier.SUBSCRIBE == "subscribe"
    assert BitgetEventClassifier.ERROR == "error"
    assert BitgetEventClassifier.UNKNOWN == "unknown"

