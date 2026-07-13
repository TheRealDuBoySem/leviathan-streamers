import pytest
from exchanges.bitget.parsing.bitget_message_parser import BitgetMessageParser
from core.models.messages import SystemMessage
from core.serialization.json_deserializer import JsonDeserializer
from exchanges.bitget.parsing.bitget_event_classifier import BitgetEventClassifier
from exchanges.bitget.parsing.bitget_trade_mapper import BitgetTradeMapper

@pytest.fixture
def parser():
    return BitgetMessageParser.create_default()

def test_bitget_parser_factory():
    """Test the factory method create_default."""
    parser = BitgetMessageParser.create_default()
    assert isinstance(parser, BitgetMessageParser)

def test_parse_pong(parser):
    res = parser.parse("pong")
    assert isinstance(res, SystemMessage)

def test_parse_subscribe(mocker, parser):
    mocker.patch(
        'core.serialization.json_deserializer.JsonDeserializer.deserialize',
        return_value={"arg": {"instType": "mc", "channel": "trade", "instId": "XRPUSDT"}},
    )
    mocker.patch('exchanges.bitget.parsing.bitget_event_classifier.BitgetEventClassifier.classify', return_value="subscribe")
    res = parser.parse("msg")
    assert res.event == "subscribe"
    assert res.symbol == "XRPUSDT"
    assert "XRPUSDT" in res.msg


def test_parse_subscribe_without_inst_id(mocker, parser):
    mocker.patch(
        'core.serialization.json_deserializer.JsonDeserializer.deserialize',
        return_value={"arg": "BTC"},
    )
    mocker.patch('exchanges.bitget.parsing.bitget_event_classifier.BitgetEventClassifier.classify', return_value="subscribe")
    res = parser.parse("msg")
    assert res.event == "subscribe"
    assert res.symbol is None

def test_parse_error(mocker, parser):
    mocker.patch('core.serialization.json_deserializer.JsonDeserializer.deserialize', return_value={})
    mocker.patch('exchanges.bitget.parsing.bitget_event_classifier.BitgetEventClassifier.classify', return_value="error")
    res = parser.parse("msg")
    assert res.msg.startswith("Erreur")

def test_parse_unknown(mocker, parser):
    mocker.patch('core.serialization.json_deserializer.JsonDeserializer.deserialize', return_value={})
    mocker.patch('exchanges.bitget.parsing.bitget_event_classifier.BitgetEventClassifier.classify', return_value="unknown")
    res = parser.parse("msg")
    assert res is None

def test_parse_decode_error(mocker, parser):
    import orjson
    mocker.patch('core.serialization.json_deserializer.JsonDeserializer.deserialize', side_effect=orjson.JSONDecodeError("msg", "doc", 0))
    res = parser.parse("msg")
    assert res is None

def test_parse_generic_error(mocker, parser):
    mocker.patch('core.serialization.json_deserializer.JsonDeserializer.deserialize', side_effect=Exception("msg"))
    res = parser.parse("msg")
    assert res is None

def test_parser_contracts(parser):
    """Verify Design by Contract preconditions for BitgetMessageParser."""
    with pytest.raises(ValueError, match="message must be a non-empty string"):
        parser.parse("")
    with pytest.raises(ValueError, match="message must be a non-empty string"):
        parser.parse(None)
    with pytest.raises(TypeError, match="message must be a string"):
        parser.parse(123)
    
    with pytest.raises(TypeError, match="deserializer must be a JsonDeserializer instance"):
        BitgetMessageParser(None, BitgetEventClassifier(), BitgetTradeMapper())
    with pytest.raises(TypeError, match="classifier must be a BitgetEventClassifier instance"):
        BitgetMessageParser(JsonDeserializer(), None, BitgetTradeMapper())
    with pytest.raises(TypeError, match="trade_mapper must be a BitgetTradeMapper instance"):
        BitgetMessageParser(JsonDeserializer(), BitgetEventClassifier(), None)

def test_parser_properties(parser):
    """Verify read-only properties of BitgetMessageParser."""
    assert isinstance(parser.deserializer, JsonDeserializer)
    assert isinstance(parser.classifier, BitgetEventClassifier)
    assert isinstance(parser.trade_mapper, BitgetTradeMapper)
    
    with pytest.raises(AttributeError):
        parser.deserializer = None
    with pytest.raises(AttributeError):
        parser.classifier = None
    with pytest.raises(AttributeError):
        parser.trade_mapper = None


def test_parse_re_raise_validation_error(mocker, parser):
    """Ensure that contract violations in sub-components are re-raised."""
    mocker.patch('core.serialization.json_deserializer.JsonDeserializer.deserialize', side_effect=ValueError("Contract violation"))
    with pytest.raises(ValueError, match="Contract violation"):
        parser.parse("msg")
