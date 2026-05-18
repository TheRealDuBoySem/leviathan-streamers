from typing import Optional, List, Set

class SubscriptionRegistry:
    """
    Manages a set of unique symbols for subscription.
    
    Invariants:
        - The internal set contains only non-empty strings.
    """
    @classmethod
    def create_empty(cls) -> "SubscriptionRegistry":
        """Factory method to create an empty registry."""
        return cls()

    def __init__(self, initial_symbols: Optional[List[str]] = None):
        """
        Initialize the registry.
        
        Preconditions:
            - initial_symbols, if provided, must be a list of non-empty strings.
        """
        if initial_symbols is not None:
            if not isinstance(initial_symbols, list):
                raise TypeError("initial_symbols must be a list")
            for s in initial_symbols:
                if not isinstance(s, str):
                    raise TypeError("symbols must be strings")
                if not s:
                    raise ValueError("symbols must be non-empty strings")
                    
        self.__symbols: Set[str] = set(initial_symbols) if initial_symbols else set()

    def add(self, symbol: str) -> bool:
        """
        Add a symbol to the registry.
        
        Preconditions:
            - symbol must be a non-empty string.
            
        Postconditions:
            - Returns True if symbol was added, False if it was already present.
        """
        if not isinstance(symbol, str):
            raise TypeError("symbol must be a string")
        if not symbol:
            raise ValueError("symbol must be a non-empty string")
            
        if symbol not in self.__symbols:
            self.__symbols.add(symbol)
            return True
        return False

    def add_many(self, symbols: List[str]) -> int:
        """
        Add multiple symbols to the registry.
        
        Preconditions:
            - symbols must be a list of non-empty strings.
            
        Postconditions:
            - Returns the number of symbols actually added.
        """
        if not isinstance(symbols, list):
            raise TypeError("symbols must be a list")
            
        added_count = 0
        for s in symbols:
            if self.add(s):
                added_count += 1
        return added_count

    def remove(self, symbol: str) -> bool:
        """
        Remove a symbol from the registry.
        
        Preconditions:
            - symbol must be a non-empty string.
            
        Postconditions:
            - Returns True if symbol was removed, False if it was not found.
        """
        if not isinstance(symbol, str):
            raise TypeError("symbol must be a string")
        if not symbol:
            raise ValueError("symbol must be a non-empty string")
            
        if symbol in self.__symbols:
            self.__symbols.remove(symbol)
            return True
        return False

    def get_all(self) -> List[str]:
        """
        Return a list of all registered symbols.
        
        Postconditions:
            - Returns a list of strings.
        """
        return list(self.__symbols)

    def clear(self) -> None:
        """Remove all symbols from the registry."""
        self.__symbols.clear()

    def __len__(self) -> int:
        """Return the number of symbols in the registry."""
        return len(self.__symbols)

    def __contains__(self, symbol: str) -> bool:
        """
        [Completeness] Check if a symbol is in the registry using the native 'in' operator.
        """
        if not isinstance(symbol, str):
            raise TypeError("symbol must be a string")
        return symbol in self.__symbols

    def __iter__(self):
        """
        [Completeness] Allow direct iteration over the subscription registry.
        """
        return iter(self.__symbols)
