import pytest

from core.interfaces.watchdog import IWatchdog


def test_iwatchdog_cannot_be_instantiated():
    with pytest.raises(TypeError):
        IWatchdog()


def test_iwatchdog_required_methods():
    assert hasattr(IWatchdog, "ping")
    assert hasattr(IWatchdog, "check_health")
