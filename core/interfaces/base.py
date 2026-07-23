"""
Barrel de rétrocompatibilité pour `core.interfaces.base`.

Préférer les imports ciblés (`core.interfaces.parsing`, etc.) ou
`from core.interfaces import ...`.
"""

from core.interfaces import (
    IParsingStrategy,
    ISubscriptionStrategy,
    IDispatchStrategy,
    IExchangeStream,
    IPriceObserver,
    IRetryPolicy,
    IWatchdog,
    IHeartbeat,
)

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
