from abc import ABC, abstractmethod
from typing import Optional, List, Any, Callable, Awaitable, AsyncIterator
from leviathan_common.models.trade_tick import TradeTick

# Pattern: Strategy
# Used to encapsulate parsing, protocol and dispatch algorithms,
# allowing them to be swapped independently of the exchange stream.

class IParsingStrategy(ABC):
    @abstractmethod
    def parse(self, message: str) -> Optional[Any]:
        """
        Parse un message brut en provenance du flux de l'exchange.

        Préconditions:
            - message: Doit être une chaîne de caractères non vide.

        Postconditions:
            - Retourne l'objet métier parsé (ex: TradeTick).
            - Retourne None uniquement si le message est valide mais n'a pas d'intérêt métier (ex: ping/pong).

        Exceptions:
            - ValueError: Si le message est vide ou malformé.
        """
        pass # pragma: no cover

class ISubscriptionStrategy(ABC):
    @abstractmethod
    def format_subscribe(self, symbols: List[str]) -> str:
        """
        Formate la charge utile JSON d'abonnement pour l'exchange.

        Préconditions:
            - symbols: Liste de symboles non vide (ex: ["BTCUSDT"]).
        """
        pass # pragma: no cover
    
    @abstractmethod
    def format_unsubscribe(self, symbols: List[str]) -> str:
        """
        Formate la charge utile JSON de désabonnement pour l'exchange.

        Préconditions:
            - symbols: Liste de symboles non vide.
        """
        pass # pragma: no cover

class IDispatchStrategy(ABC):
    @abstractmethod
    async def dispatch(self, data: Any) -> None:
        """
        Achemine les données reçues vers la file d'attente ou les observateurs.
        """
        pass # pragma: no cover
    
    @abstractmethod
    async def wait_for_next_data(self) -> Any:
        """
        Récupère la prochaine donnée disponible de manière asynchrone.
        """
        pass # pragma: no cover
    
    @abstractmethod
    def task_done(self) -> None:
        """
        Signale que la tâche de traitement de la dernière donnée récupérée est terminée.
        """
        pass # pragma: no cover

# Pattern: Observer
# Allows multiple components to react to new trade ticks without
# the stream orchestrator knowing about them.

from leviathan_common.interfaces.base import IPriceObserver

class IExchangeStream(ABC):
    @abstractmethod
    async def start_streaming(self) -> None:
        """
        Démarre la connexion et le flux de données en tâche de fond.
        """
        pass # pragma: no cover
    
    @abstractmethod
    async def stop(self) -> None:
        """
        Arrête proprement le streaming et ferme les connexions actives.
        """
        pass # pragma: no cover
    
    @abstractmethod
    async def subscribe_symbol(self, symbol: str) -> None:
        """
        Abonne le flux à un symbole unique.
        """
        pass # pragma: no cover

    @abstractmethod
    async def subscribe_symbols(self, symbols: List[str]) -> None:
        """
        [Convenance] Abonne le flux à plusieurs symboles simultanément.
        """
        pass # pragma: no cover
    
    @abstractmethod
    async def unsubscribe_symbol(self, symbol: str) -> None:
        """
        Désabonne le flux d'un symbole unique.
        """
        pass # pragma: no cover

    @abstractmethod
    async def unsubscribe_symbols(self, symbols: List[str]) -> None:
        """
        [Convenance] Désabonne le flux de plusieurs symboles simultanément.
        """
        pass # pragma: no cover

    @abstractmethod
    def is_stopped(self) -> bool:
        """
        Retourne True si le flux est arrêté.
        """
        pass # pragma: no cover
        
    @abstractmethod
    def is_connected(self) -> bool:
        """
        Retourne True si la connexion active est établie.
        """
        pass # pragma: no cover

    @abstractmethod
    async def wait_until_connected(self) -> None:
        """
        [Complétude] Attend asynchronement que la connexion soit pleinement établie.
        """
        pass # pragma: no cover
        
    @abstractmethod
    def get_active_symbols(self) -> List[str]:
        """
        Retourne la liste des symboles actuellement abonnés.
        """
        pass # pragma: no cover

    @abstractmethod
    def __aiter__(self) -> AsyncIterator[TradeTick]:
        """
        Permet de consommer les TradeTicks sous forme d'itérateur asynchrone.
        """
        pass # pragma: no cover

# Utility Strategy Interfaces
class IRetryPolicy(ABC):
    @abstractmethod
    def can_retry(self, attempt: int) -> bool:
        """
        Détermine si une nouvelle tentative de reconnexion est autorisée.

        Préconditions:
            - attempt >= 0
        """
        pass # pragma: no cover
    
    @abstractmethod
    def get_delay(self, attempt: int) -> int:
        """
        Calcule le délai (en secondes) à respecter avant la tentative suivante.

        Préconditions:
            - attempt >= 0
        """
        pass # pragma: no cover

class IWatchdog(ABC):
    @abstractmethod
    def ping(self) -> None:
        """
        Enregistre un signe de vie (activité) pour réinitialiser le minuteur de santé.
        """
        pass # pragma: no cover
    
    @abstractmethod
    def check_health(self) -> bool:
        """
        Évalue la santé du système. Retourne False si aucune activité n'a été détectée.
        """
        pass # pragma: no cover

class IHeartbeat(ABC):
    @abstractmethod
    async def run(self, send_func: Callable[[str], Awaitable[None]], payload: str = "ping") -> None:
        """
        Exécute la boucle périodique d'émission de battements de cœur (heartbeats).
        """
        pass # pragma: no cover
