import pytest

from core.interfaces.heartbeat import IHeartbeat


def test_iheartbeat_cannot_be_instantiated():
    with pytest.raises(TypeError):
        IHeartbeat()


def test_iheartbeat_required_methods():
    assert hasattr(IHeartbeat, "run")
