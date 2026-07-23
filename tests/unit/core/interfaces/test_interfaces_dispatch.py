import inspect
import pytest

from core.interfaces.dispatch import IDispatchStrategy


def test_idispatch_strategy_cannot_be_instantiated():
    with pytest.raises(TypeError):
        IDispatchStrategy()


def test_idispatch_strategy_tick_methods():
    assert hasattr(IDispatchStrategy, "wait_for_next_tick")
    assert hasattr(IDispatchStrategy, "mark_tick_as_processed")
    assert not hasattr(IDispatchStrategy, "wait_for_next_data")
    assert not hasattr(IDispatchStrategy, "task_done")

    dispatch_sig = inspect.signature(IDispatchStrategy.dispatch)
    assert "tick" in dispatch_sig.parameters
