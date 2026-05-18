import orjson
from typing import List
from core.interfaces.base import ISubscriptionStrategy

class BitgetSubscriptionProtocol(ISubscriptionStrategy):
    """
    Handles formatting of subscription messages for Bitget.
    
    Pattern: Strategy (Concrete Implementation)
    """
    def __init__(self, inst_type: str):
        """
        Initialize the protocol handler.
        
        Preconditions:
            - inst_type must be a non-empty string (e.g., 'mc', 'spot').
        """
        if inst_type is None:
            raise ValueError("inst_type must be a non-empty string")
        if not isinstance(inst_type, str):
            raise TypeError("inst_type must be a string")
        if not inst_type:
            raise ValueError("inst_type must be a non-empty string")
            
        self.__inst_type = inst_type

    @property
    def inst_type(self) -> str:
        """[Completeness] Return the instrument type configured for this protocol."""
        return self.__inst_type

    def format_subscribe(self, symbols: List[str]) -> str:
        """
        Format a subscription message.
        
        Preconditions:
            - symbols must be a non-empty list of strings.
        """
        if symbols is None:
            raise ValueError("symbols list cannot be empty")
        if not isinstance(symbols, list):
            raise TypeError("symbols must be a list")
        if not symbols:
            raise ValueError("symbols list cannot be empty")
            
        for s in symbols:
            if not isinstance(s, str):
                raise TypeError("symbols must be strings")
            if not s:
                raise ValueError("symbols must be non-empty strings")
            
        args = [{"instType": self.__inst_type, "channel": "trade", "instId": s} for s in symbols]
        return orjson.dumps({"op": "subscribe", "args": args}).decode("utf-8")

    def format_unsubscribe(self, symbols: List[str]) -> str:
        """
        Format an unsubscription message.
        
        Preconditions:
            - symbols must be a non-empty list of strings.
        """
        if symbols is None:
            raise ValueError("symbols list cannot be empty")
        if not isinstance(symbols, list):
            raise TypeError("symbols must be a list")
        if not symbols:
            raise ValueError("symbols list cannot be empty")
            
        for s in symbols:
            if not isinstance(s, str):
                raise TypeError("symbols must be strings")
            if not s:
                raise ValueError("symbols must be non-empty strings")
            
        args = [{"instType": self.__inst_type, "channel": "trade", "instId": s} for s in symbols]
        return orjson.dumps({"op": "unsubscribe", "args": args}).decode("utf-8")
