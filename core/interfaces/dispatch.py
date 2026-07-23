"""
Interface d'acheminement des ticks de trade.

Pattern: Strategy — IDispatchStrategy.
"""

from abc import ABC, abstractmethod

from leviathan_common.models.trade_tick import TradeTick

__all__ = ["IDispatchStrategy"]


class IDispatchStrategy(ABC):
    @abstractmethod
    async def dispatch(self, tick: TradeTick) -> None:
        """
        Achemine un tick de trade vers la file d'attente ou les observateurs.

        Préconditions:
            - tick: instance valide de TradeTick.

        Postconditions:
            - Le tick est disponible pour consommation via wait_for_next_tick().

        Exceptions:
            - TypeError: si tick n'est pas une instance de TradeTick.
        """
        pass  # pragma: no cover

    @abstractmethod
    async def wait_for_next_tick(self) -> TradeTick:
        """
        Récupère le prochain tick disponible de manière asynchrone.

        Postconditions:
            - Retourne une instance valide de TradeTick.
        """
        pass  # pragma: no cover

    @abstractmethod
    def mark_tick_as_processed(self) -> None:
        """
        Signale que le traitement du dernier tick récupéré est terminé.

        Préconditions:
            - Un tick a été récupéré via wait_for_next_tick() sans appel intermédiaire
              à mark_tick_as_processed().
        """
        pass  # pragma: no cover
