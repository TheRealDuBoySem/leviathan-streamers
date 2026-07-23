"""
Interface de formatage des charges utiles d'abonnement / désabonnement.

Pattern: Strategy — ISubscriptionStrategy.
"""

from abc import ABC, abstractmethod
from typing import List

__all__ = ["ISubscriptionStrategy"]


class ISubscriptionStrategy(ABC):
    @abstractmethod
    def format_subscribe(self, symbols: List[str]) -> str:
        """
        Formate la charge utile JSON d'abonnement pour l'exchange.

        Préconditions:
            - symbols: liste non vide de symboles (ex: ["BTCUSDT"]).

        Postconditions:
            - Retourne une chaîne JSON prête à l'envoi.

        Exceptions:
            - ValueError: si symbols est vide.
            - TypeError: si symbols n'est pas une liste ou contient des éléments non-string.
        """
        pass  # pragma: no cover

    @abstractmethod
    def format_unsubscribe(self, symbols: List[str]) -> str:
        """
        Formate la charge utile JSON de désabonnement pour l'exchange.

        Préconditions:
            - symbols: liste non vide de symboles.

        Postconditions:
            - Retourne une chaîne JSON prête à l'envoi.

        Exceptions:
            - ValueError: si symbols est vide.
            - TypeError: si symbols n'est pas une liste ou contient des éléments non-string.
        """
        pass  # pragma: no cover
