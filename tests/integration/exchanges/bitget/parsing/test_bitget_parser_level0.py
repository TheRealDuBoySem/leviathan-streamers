from exchanges.bitget.parsing.bitget_message_parser import BitgetMessageParser
from core.models.messages import TradeMessage
from core.serialization.json_deserializer import JsonDeserializer
from exchanges.bitget.parsing.bitget_event_classifier import BitgetEventClassifier
from exchanges.bitget.parsing.bitget_trade_mapper import BitgetTradeMapper

def test_parser_integration():
    parser = BitgetMessageParser(
        deserializer=JsonDeserializer(),
        classifier=BitgetEventClassifier(),
        trade_mapper=BitgetTradeMapper()
    )
    json_str = '{"action":"snapshot","arg":{"channel":"trade","instId":"BTCUSDT"},"data":[{"ts":1710000000000,"price":"70000","size":"1","side":"buy","tradeId":"1"}]}'
    result = parser.parse(json_str)
    assert isinstance(result, TradeMessage)
    assert result.ticks[0].inst_id == "BTCUSDT"
