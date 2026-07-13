from typing import List, Optional
from dataclasses import dataclass
from core.models.trade_tick import TradeTick

class ParsedMessage:
    """Classe de base représentant un message réseau parsé."""
    pass

@dataclass(frozen=True)
class TradeMessage(ParsedMessage):
    """
    Message contenant un lot de ticks de trade.

    Invariants:
        - ticks est une liste non vide.
        - Tous les éléments de ticks sont des instances de TradeTick.
    """
    ticks: List[TradeTick]

    def __post_init__(self):
        if not isinstance(self.ticks, list):
            raise TypeError("ticks doit être une liste")
        if not self.ticks:
            raise ValueError("ticks ne peut pas être vide")
        for tick in self.ticks:
            if not isinstance(tick, TradeTick):
                raise TypeError("Tous les éléments de ticks doivent être des instances de TradeTick")

@dataclass(frozen=True)
class SystemMessage(ParsedMessage):
    """
    Message système ou technique envoyé par l'exchange (ex: pong, welcome).

    Invariants:
        - event est une chaîne non vide.
        - msg est une chaîne (éventuellement vide).
        - symbol, when set, is a non-empty string (subscribe/unsubscribe ack).
    """
    event: str
    msg: str
    symbol: Optional[str] = None

    def __post_init__(self):
        if not self.event:
            raise ValueError("event ne peut pas être vide")
        if not isinstance(self.msg, str):
            raise TypeError("msg doit être une chaîne de caractères")
        if self.symbol is not None:
            if not isinstance(self.symbol, str):
                raise TypeError("symbol must be a string when provided")
            if not self.symbol:
                raise ValueError("symbol cannot be empty when provided")

@dataclass(frozen=True)
class ErrorMessage(ParsedMessage):
    """
    Message d'erreur envoyé par l'exchange ou capturé lors d'un échec technique.

    Invariants:
        - msg est une chaîne non vide.
    """
    msg: str

    def __post_init__(self):
        if not self.msg:
            raise ValueError("msg ne peut pas être vide")
