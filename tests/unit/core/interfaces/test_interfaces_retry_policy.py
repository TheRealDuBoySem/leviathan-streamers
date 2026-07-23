import pytest

from core.interfaces.retry_policy import IRetryPolicy


def test_iretry_policy_cannot_be_instantiated():
    with pytest.raises(TypeError):
        IRetryPolicy()


def test_iretry_policy_required_methods():
    assert hasattr(IRetryPolicy, "can_retry")
    assert hasattr(IRetryPolicy, "get_delay")
