from typing import Dict, Any

class BitgetEventClassifier:
    """Classifies Bitget WebSocket messages into known event types."""
    
    TRADE = "trade"
    SUBSCRIBE = "subscribe"
    ERROR = "error"
    UNKNOWN = "unknown"
    
    @staticmethod
    def classify(data: Dict[str, Any]) -> str:
        """
        Identify the event type of a Bitget message.
        
        Preconditions:
            - data must be a dictionary.
            
        Postconditions:
            - Returns a string indicating the event type ('trade', 'subscribe', 'error', or 'unknown').
        """
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data).__name__}")
            
        arg_data = data.get("arg", {})
        if data.get("action") in ["snapshot", "update"] and arg_data.get("channel") == "trade":
            return BitgetEventClassifier.TRADE
        if data.get("event") == "subscribe":
            return BitgetEventClassifier.SUBSCRIBE
        if data.get("event") == "error" or "error" in data:
            return BitgetEventClassifier.ERROR
            
        return BitgetEventClassifier.UNKNOWN
