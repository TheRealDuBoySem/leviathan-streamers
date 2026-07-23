"""
Interface façade du flux de données d'un exchange.

Pattern: Observer — attach/detach d'IPriceObserver.
"""

from abc import ABC, abstractmethod
from typing import List, Callable, Awaitable, AsyncIterator

from leviathan_common.models.trade_tick import TradeTick
from leviathan_common.interfaces.base import IPriceObserver

__all__ = ["IExchangeStream"]


class IExchangeStream(ABC):
    @abstractmethod
    async def start_streaming(self) -> None:
        """
        Démarre la connexion et le flux de données en tâche de fond.

        Postconditions:
            - Le flux écoute les messages réseau jusqu'à un appel à stop().
        """
        pass  # pragma: no cover

    @abstractmethod
    async def stop(self) -> None:
        """
        Arrête proprement le streaming et ferme les connexions actives.

        Postconditions:
            - is_stopped() retourne True.
        """
        pass  # pragma: no cover

    @abstractmethod
    async def subscribe_symbol(self, symbol: str) -> None:
        """
        Abonne le flux à un symbole unique.

        Préconditions:
            - symbol: chaîne non vide.

        Exceptions:
            - ValueError: si symbol est vide.
            - TypeError: si symbol n'est pas une chaîne de caractères.
        """
        pass  # pragma: no cover

    @abstractmethod
    async def subscribe_symbols(self, symbols: List[str]) -> None:
        """
        Abonne le flux à plusieurs symboles simultanément.

        Préconditions:
            - symbols: liste non vide de chaînes non vides.

        Exceptions:
            - ValueError: si symbols est vide ou contient des chaînes vides.
            - TypeError: si symbols n'est pas une liste ou contient des éléments non-string.
        """
        pass  # pragma: no cover

    @abstractmethod
    async def unsubscribe_symbol(self, symbol: str) -> None:
        """
        Désabonne le flux d'un symbole unique.

        Préconditions:
            - symbol: chaîne non vide.

        Exceptions:
            - ValueError: si symbol est vide.
            - TypeError: si symbol n'est pas une chaîne de caractères.
        """
        pass  # pragma: no cover

    @abstractmethod
    async def unsubscribe_symbols(self, symbols: List[str]) -> None:
        """
        Désabonne le flux de plusieurs symboles simultanément.

        Préconditions:
            - symbols: liste non vide de chaînes non vides.

        Exceptions:
            - ValueError: si symbols est vide ou contient des chaînes vides.
            - TypeError: si symbols n'est pas une liste ou contient des éléments non-string.
        """
        pass  # pragma: no cover

    @abstractmethod
    def is_stopped(self) -> bool:
        """
        Retourne True si le flux est arrêté.
        """
        pass  # pragma: no cover

    @abstractmethod
    def is_connected(self) -> bool:
        """
        Retourne True si la connexion active est établie.
        """
        pass  # pragma: no cover

    @abstractmethod
    async def wait_until_connected(self) -> None:
        """
        Attend asynchronement que la connexion soit pleinement établie.

        Exceptions:
            - ConnectionError: si le flux est arrêté avant l'établissement de la connexion.
        """
        pass  # pragma: no cover

    @abstractmethod
    def register_on_reconnect(self, callback: Callable[[], Awaitable[None]]) -> None:
        """
        Enregistre un callback asynchrone invoqué après chaque connexion WS réussie
        (y compris après resubscription post-reconnexion).

        Préconditions:
            - callback: callable awaitable sans argument.

        Exceptions:
            - TypeError: si callback n'est pas un callable awaitable.
        """
        pass  # pragma: no cover

    @abstractmethod
    def unregister_on_reconnect(self, callback: Callable[[], Awaitable[None]]) -> None:
        """
        Désenregistre un callback précédemment ajouté via register_on_reconnect.

        Préconditions:
            - callback: le même callable enregistré auparavant.

        Exceptions:
            - TypeError: si callback n'est pas un callable awaitable.
        """
        pass  # pragma: no cover

    @abstractmethod
    def get_active_symbols(self) -> List[str]:
        """
        Retourne la liste des symboles actuellement abonnés.

        Postconditions:
            - Retourne une copie de la liste des symboles actifs.
        """
        pass  # pragma: no cover

    @abstractmethod
    async def wait_for_next_tick(self) -> TradeTick:
        """
        Récupère le prochain tick de trade disponible de manière asynchrone.

        Postconditions:
            - Retourne une instance valide de TradeTick.
        """
        pass  # pragma: no cover

    @abstractmethod
    def mark_tick_as_processed(self) -> None:
        """
        Signale que le traitement du dernier tick récupéré est terminé.

        Préconditions:
            - Un tick a été récupéré via wait_for_next_tick() ou __aiter__()
              sans appel intermédiaire à mark_tick_as_processed().
        """
        pass  # pragma: no cover

    @abstractmethod
    def attach_observer(self, observer: IPriceObserver) -> None:
        """
        Attache un observateur de prix au flux.

        Préconditions:
            - observer: instance implémentant IPriceObserver.

        Exceptions:
            - TypeError: si observer n'implémente pas IPriceObserver.
        """
        pass  # pragma: no cover

    @abstractmethod
    def detach_observer(self, observer: IPriceObserver) -> None:
        """
        Détache un observateur de prix précédemment attaché.

        Préconditions:
            - observer: instance implémentant IPriceObserver.

        Exceptions:
            - TypeError: si observer n'implémente pas IPriceObserver.
        """
        pass  # pragma: no cover

    @property
    @abstractmethod
    def observers(self) -> List[IPriceObserver]:
        """
        Retourne une copie de la liste des observateurs attachés.
        """
        pass  # pragma: no cover

    @abstractmethod
    def __aiter__(self) -> AsyncIterator[TradeTick]:
        """
        Permet de consommer les TradeTicks sous forme d'itérateur asynchrone.
        """
        pass  # pragma: no cover
