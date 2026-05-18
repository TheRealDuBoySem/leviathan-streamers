from dataclasses import dataclass

@dataclass(frozen=True)
class TradeTick:
    """
    Represents a single trade tick from an exchange.
    
    Invariants:
        - inst_id is a non-empty string.
        - ts is a positive integer (timestamp).
        - price is a positive float.
        - size is a positive float.
        - side is either 'buy' or 'sell'.
        - trade_id is a non-empty string.
    """
    inst_id: str
    ts: int
    price: float
    size: float
    side: str
    trade_id: str

    def __post_init__(self):
        # 1. Validation of types at runtime
        if not isinstance(self.inst_id, str):
            raise TypeError("inst_id must be a string")
        if not isinstance(self.ts, int):
            raise TypeError("ts must be an integer")
        if not isinstance(self.price, (int, float)):
            raise TypeError("price must be a number (int or float)")
        if not isinstance(self.size, (int, float)):
            raise TypeError("size must be a number (int or float)")
        if not isinstance(self.side, str):
            raise TypeError("side must be a string")
        if not isinstance(self.trade_id, str):
            raise TypeError("trade_id must be a string")

        # 2. Validation of invariants values
        if not self.inst_id:
            raise ValueError("inst_id cannot be empty")
        if self.ts <= 0:
            raise ValueError(f"ts must be positive, got {self.ts}")
        if self.price <= 0:
            raise ValueError(f"price must be positive, got {self.price}")
        if self.size <= 0:
            raise ValueError(f"size must be positive, got {self.size}")
        if self.side not in ("buy", "sell"):
            raise ValueError(f"side must be 'buy' or 'sell', got {self.side}")
        if not self.trade_id:
            raise ValueError("trade_id cannot be empty")

    @property
    def notional(self) -> float:
        """
        Calculates the total financial (notional) value of the tick (price * size).
        
        Postconditions:
            - The result is guaranteed to be strictly positive (> 0).
        """
        return float(self.price * self.size)
