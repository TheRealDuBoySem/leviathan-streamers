import inspect
import pytest

from core.interfaces.exchange_stream import IExchangeStream


def test_iexchange_stream_cannot_be_instantiated():
    with pytest.raises(TypeError):
        IExchangeStream()


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
