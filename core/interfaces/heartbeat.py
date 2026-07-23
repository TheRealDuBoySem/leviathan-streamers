"""
Interface d'émission périodique de battements de cœur (keepalive).

Pattern: Strategy — IHeartbeat.
"""

from abc import ABC, abstractmethod
from typing import Callable, Awaitable

__all__ = ["IHeartbeat"]


class IHeartbeat(ABC):
    @abstractmethod
    async def run(
        self,
        send_func: Callable[[str], Awaitable[None]],
        payload: str = "ping",
    ) -> None:
        """
        Exécute la boucle périodique d'émission de battements de cœur (heartbeats).

        Préconditions:
            - send_func: callable acceptant une chaîne et retournant un awaitable.
            - payload: chaîne non vide (défaut: "ping").

        Postconditions:
            - La boucle s'exécute jusqu'à annulation de la tâche asyncio.

        Exceptions:
            - TypeError: si send_func n'est pas callable ou payload invalide.
            - ValueError: si payload est vide.
        """
        pass  # pragma: no cover
