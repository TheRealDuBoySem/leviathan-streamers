import pytest

from core.interfaces.base import (
    IParsingStrategy,
    ISubscriptionStrategy,
    IDispatchStrategy,
    IExchangeStream,
    IPriceObserver,
    IRetryPolicy,
    IWatchdog,
    IHeartbeat,
)


@pytest.mark.parametrize(
    "interface_cls",
    [
        IParsingStrategy,
        ISubscriptionStrategy,
        IDispatchStrategy,
        IExchangeStream,
        IPriceObserver,
        IRetryPolicy,
        IWatchdog,
        IHeartbeat,
    ],
)
def test_base_reexports_are_abstract(interface_cls):
    with pytest.raises(TypeError):
        interface_cls()


def test_iprice_observer_reexported():
    from leviathan_common.interfaces.base import IPriceObserver as CommonObserver

    assert IPriceObserver is CommonObserver
