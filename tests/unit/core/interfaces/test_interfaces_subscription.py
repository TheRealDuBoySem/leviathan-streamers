import pytest

from core.interfaces.subscription import ISubscriptionStrategy


def test_isubscription_strategy_cannot_be_instantiated():
    with pytest.raises(TypeError):
        ISubscriptionStrategy()


def test_isubscription_strategy_required_methods():
    assert hasattr(ISubscriptionStrategy, "format_subscribe")
    assert hasattr(ISubscriptionStrategy, "format_unsubscribe")
