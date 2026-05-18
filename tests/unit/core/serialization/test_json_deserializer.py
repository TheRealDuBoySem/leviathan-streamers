import pytest
import orjson
from core.serialization.json_deserializer import JsonDeserializer

def test_deserialize_valid_json():
    assert JsonDeserializer.deserialize('{"k": "v"}') == {"k": "v"}

def test_deserialize_invalid_json():
    with pytest.raises(orjson.JSONDecodeError):
        JsonDeserializer.deserialize('invalid')

def test_json_deserializer_contracts():
    """Verify Design by Contract preconditions for JsonDeserializer."""
    with pytest.raises(TypeError, match="message must be a string"):
        JsonDeserializer.deserialize(123)
    with pytest.raises(ValueError, match="message must be a non-empty string"):
        JsonDeserializer.deserialize("")
    with pytest.raises(TypeError, match="Expected JSON object"):
        JsonDeserializer.deserialize("[]")

