"""
Interfaces du sous-système leviathan_streamers.

Pattern: Strategy — parsing, subscription, dispatch, retry, watchdog, heartbeat.
Pattern: Observer — IPriceObserver, IExchangeStream (attach/detach).
"""

from leviathan_common.interfaces.base import IPriceObserver

from core.interfaces.parsing import IParsingStrategy
from core.interfaces.subscription import ISubscriptionStrategy
from core.interfaces.dispatch import IDispatchStrategy
from core.interfaces.exchange_stream import IExchangeStream
from core.interfaces.retry_policy import IRetryPolicy
from core.interfaces.watchdog import IWatchdog
from core.interfaces.heartbeat import IHeartbeat

__all__ = [
    "IParsingStrategy",
    "ISubscriptionStrategy",
    "IDispatchStrategy",
    "IExchangeStream",
    "IPriceObserver",
    "IRetryPolicy",
    "IWatchdog",
    "IHeartbeat",
]
