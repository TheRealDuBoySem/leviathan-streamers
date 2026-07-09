import inspect
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
def test_interface_cannot_be_instantiated(interface_cls):
    with pytest.raises(TypeError):
        interface_cls()


def test_iparsing_strategy_parse_signature():
    sig = inspect.signature(IParsingStrategy.parse)
    assert sig.return_annotation is not inspect.Signature.empty
    assert "ParsedMessage" in str(sig.return_annotation)


def test_idispatch_strategy_tick_methods():
    assert hasattr(IDispatchStrategy, "wait_for_next_tick")
    assert hasattr(IDispatchStrategy, "mark_tick_as_processed")
    assert not hasattr(IDispatchStrategy, "wait_for_next_data")
    assert not hasattr(IDispatchStrategy, "task_done")

    dispatch_sig = inspect.signature(IDispatchStrategy.dispatch)
    assert "tick" in dispatch_sig.parameters


def test_iexchange_stream_completeness():
    required_methods = {
        "start_streaming",
        "stop",
        "subscribe_symbol",
        "subscribe_symbols",
        "unsubscribe_symbol",
        "unsubscribe_symbols",
        "is_stopped",
        "is_connected",
        "wait_until_connected",
        "register_on_reconnect",
        "unregister_on_reconnect",
        "get_active_symbols",
        "wait_for_next_tick",
        "mark_tick_as_processed",
        "attach_observer",
        "detach_observer",
        "observers",
        "__aiter__",
    }
    abstract_methods = {
        name
        for name, value in inspect.getmembers(IExchangeStream)
        if getattr(value, "__isabstractmethod__", False)
    }
    assert required_methods == abstract_methods


def test_iexchange_stream_implements_async_iterator():
    assert hasattr(IExchangeStream, "__aiter__")


def test_iprice_observer_reexported():
    from leviathan_common.interfaces.base import IPriceObserver as CommonObserver

    assert IPriceObserver is CommonObserver
