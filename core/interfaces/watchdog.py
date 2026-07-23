"""
Interface de surveillance de santé (silence / activité).

Pattern: Strategy — IWatchdog.
"""

from abc import ABC, abstractmethod

__all__ = ["IWatchdog"]


class IWatchdog(ABC):
    @abstractmethod
    def ping(self) -> None:
        """
        Enregistre un signe de vie (activité) pour réinitialiser le minuteur de santé.

        Postconditions:
            - Le minuteur de silence est réinitialisé.
        """
        pass  # pragma: no cover

    @abstractmethod
    def check_health(self) -> bool:
        """
        Évalue la santé du système. Retourne False si aucune activité n'a été détectée.

        Postconditions:
            - Retourne True tant que l'activité est dans les limites acceptables.
            - Retourne False si le timeout de silence est dépassé.
        """
        pass  # pragma: no cover
