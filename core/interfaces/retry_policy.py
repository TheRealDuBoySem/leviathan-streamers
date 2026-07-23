"""
Interface de politique de reconnexion.

Pattern: Strategy — IRetryPolicy.
"""

from abc import ABC, abstractmethod

__all__ = ["IRetryPolicy"]


class IRetryPolicy(ABC):
    @abstractmethod
    def can_retry(self, attempt: int) -> bool:
        """
        Détermine si une nouvelle tentative de reconnexion est autorisée.

        Préconditions:
            - attempt >= 0

        Exceptions:
            - ValueError: si attempt < 0.
            - TypeError: si attempt n'est pas un entier.
        """
        pass  # pragma: no cover

    @abstractmethod
    def get_delay(self, attempt: int) -> int:
        """
        Calcule le délai (en secondes) à respecter avant la tentative suivante.

        Préconditions:
            - attempt >= 0

        Postconditions:
            - Retourne un entier >= 0 représentant le délai en secondes.

        Exceptions:
            - ValueError: si attempt < 0.
            - TypeError: si attempt n'est pas un entier.
        """
        pass  # pragma: no cover
