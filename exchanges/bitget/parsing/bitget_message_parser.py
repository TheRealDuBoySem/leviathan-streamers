import logging
import orjson
from typing import Optional
from core.models.messages import ParsedMessage, SystemMessage, TradeMessage, ErrorMessage
from core.interfaces.base import IParsingStrategy
from core.serialization.json_deserializer import JsonDeserializer
from exchanges.bitget.parsing.bitget_event_classifier import BitgetEventClassifier
from exchanges.bitget.parsing.bitget_trade_mapper import BitgetTradeMapper

logger = logging.getLogger(__name__)

class BitgetMessageParser(IParsingStrategy):
    """
    Parses raw Bitget messages into standardized ParsedMessage objects.
    
    Pattern: Strategy (Concrete Implementation)
    """
    
    @classmethod
    def create_default(cls) -> "BitgetMessageParser":
        """Factory method to create a parser with default dependencies."""
        return cls(
            deserializer=JsonDeserializer(),
            classifier=BitgetEventClassifier(),
            trade_mapper=BitgetTradeMapper()
        )

    def __init__(self, deserializer: JsonDeserializer, classifier: BitgetEventClassifier, trade_mapper: BitgetTradeMapper):
        """
        Initialize the parser with injected dependencies.
        
        Preconditions:
            - deserializer, classifier, and trade_mapper must be valid instances.
        """
        if not isinstance(deserializer, JsonDeserializer):
            raise TypeError("deserializer must be a JsonDeserializer instance")
        if not isinstance(classifier, BitgetEventClassifier):
            raise TypeError("classifier must be a BitgetEventClassifier instance")
        if not isinstance(trade_mapper, BitgetTradeMapper):
            raise TypeError("trade_mapper must be a BitgetTradeMapper instance")
            
        self.__deserializer = deserializer
        self.__classifier = classifier
        self.__trade_mapper = trade_mapper

    @property
    def deserializer(self) -> JsonDeserializer:
        """[Completeness] Return the injected JSON deserializer."""
        return self.__deserializer

    @property
    def classifier(self) -> BitgetEventClassifier:
        """[Completeness] Return the injected event classifier."""
        return self.__classifier

    @property
    def trade_mapper(self) -> BitgetTradeMapper:
        """[Completeness] Return the injected trade mapper."""
        return self.__trade_mapper

    def parse(self, message: str) -> Optional[ParsedMessage]:
        """
        Parse a raw WebSocket message.
        
        Preconditions:
            - message must be a non-empty string.
            
        Postconditions:
            - Returns a ParsedMessage subclass or None if parsing fails.
        """
        if message is None:
            raise ValueError("message must be a non-empty string")
        if not isinstance(message, str):
            raise TypeError("message must be a string")
        if not message:
            raise ValueError("message must be a non-empty string")
            
        if message == "pong":
            return SystemMessage(event="pong", msg="Pong reçu.")

        try:
            data = self.__deserializer.deserialize(message)
            event_type = self.__classifier.classify(data)

            if event_type == BitgetEventClassifier.TRADE:
                return TradeMessage(ticks=self.__trade_mapper.map(data)) # pragma: no cover
            elif event_type == BitgetEventClassifier.SUBSCRIBE:
                return SystemMessage(event="subscribe", msg=f"Abonnement confirmé: {data.get('arg')}")
            elif event_type == BitgetEventClassifier.ERROR:
                return ErrorMessage(msg=f"Erreur Bitget: {data}")
            
        except orjson.JSONDecodeError:
            logger.warning(f"Impossible de décoder le message JSON: {message}")
        except (ValueError, TypeError) as e:
            # Re-raise contract violations for fail-fast
            raise
        except Exception as e:
            logger.error(f"Erreur inattendue de parsing: {e}")
        return None
