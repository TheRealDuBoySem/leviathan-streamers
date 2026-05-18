import orjson
from typing import Dict, Any

class JsonDeserializer:
    """Provides JSON deserialization services."""
    
    @staticmethod
    def deserialize(message: str) -> Dict[str, Any]:
        """
        Deserialize a JSON string into a dictionary.
        
        Preconditions:
            - message must be a non-empty string.
            
        Postconditions:
            - Returns a dictionary representing the JSON data.
            
        Raises:
            - TypeError: If message is not a string, or if result is not a dictionary.
            - ValueError: If message is empty.
            - orjson.JSONDecodeError: If JSON is invalid.
        """
        if not isinstance(message, str):
            raise TypeError("message must be a string")
        if not message:
            raise ValueError("message must be a non-empty string")
            
        data = orjson.loads(message)
        
        if not isinstance(data, dict):
            raise TypeError(f"Expected JSON object (dict), got {type(data).__name__}")
            
        return data
