"""
Interface de parsing des messages bruts du flux exchange.

Pattern: Strategy — IParsingStrategy.
"""

from abc import ABC, abstractmethod
from typing import Optional

from core.models.messages import ParsedMessage

__all__ = ["IParsingStrategy"]


class IParsingStrategy(ABC):
    @abstractmethod
    def parse(self, message: str) -> Optional[ParsedMessage]:
        """
        Parse un message brut en provenance du flux de l'exchange.

        Préconditions:
            - message: chaîne de caractères non vide.

        Postconditions:
            - Retourne un ParsedMessage (TradeMessage, SystemMessage ou ErrorMessage).
            - Retourne None uniquement si le message est valide mais sans intérêt métier.

        Exceptions:
            - ValueError: si le message est vide ou malformé.
            - TypeError: si message n'est pas une chaîne de caractères.
        """
        pass  # pragma: no cover
