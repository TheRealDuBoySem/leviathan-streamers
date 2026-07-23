import inspect
import pytest

from core.interfaces.parsing import IParsingStrategy


def test_iparsing_strategy_cannot_be_instantiated():
    with pytest.raises(TypeError):
        IParsingStrategy()


def test_iparsing_strategy_parse_signature():
    sig = inspect.signature(IParsingStrategy.parse)
    assert sig.return_annotation is not inspect.Signature.empty
    assert "ParsedMessage" in str(sig.return_annotation)
