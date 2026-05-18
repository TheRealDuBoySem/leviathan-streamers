import pytest
from core.state.subscription_registry import SubscriptionRegistry

def test_add_remove():
    reg = SubscriptionRegistry.create_empty()
    assert len(reg) == 0
    assert reg.add("BTC") is True
    assert len(reg) == 1
    assert reg.add("BTC") is False
    assert reg.get_all() == ["BTC"]
    
    assert reg.add_many(["ETH", "LTC", "BTC"]) == 2
    assert len(reg) == 3
    
    reg.clear()
    assert len(reg) == 0
    reg.add("ETH")
    assert reg.remove("ETH") is True
    assert reg.remove("ETH") is False

def test_registry_contracts():
    """Verify Design by Contract preconditions for SubscriptionRegistry."""
    with pytest.raises(TypeError, match="initial_symbols must be a list"):
        SubscriptionRegistry(initial_symbols="not a list")
    with pytest.raises(ValueError, match="symbols must be non-empty strings"):
        SubscriptionRegistry(initial_symbols=[""])
    
    r = SubscriptionRegistry()
    with pytest.raises(ValueError, match="symbol must be a non-empty string"):
        r.add("")
    with pytest.raises(ValueError, match="symbol must be a non-empty string"):
        r.remove("")
    with pytest.raises(TypeError, match="symbols must be a list"):
        r.add_many("not a list")

def test_registry_types():
    """Verify Type contract preconditions for SubscriptionRegistry."""
    with pytest.raises(TypeError, match="symbols must be strings"):
        SubscriptionRegistry(initial_symbols=["BTC", 123])
    
    r = SubscriptionRegistry()
    with pytest.raises(TypeError, match="symbol must be a string"):
        r.add(123)
    with pytest.raises(TypeError, match="symbol must be a string"):
        r.remove(123)
    with pytest.raises(TypeError, match="symbol must be a string"):
        123 in r

def test_registry_magic_methods():
    """Verify standard collection magic methods of SubscriptionRegistry."""
    reg = SubscriptionRegistry(initial_symbols=["BTC", "ETH"])
    
    # Verify membership (__contains__)
    assert "BTC" in reg
    assert "ETH" in reg
    assert "LTC" not in reg
    
    # Verify iteration (__iter__)
    symbols_list = list(reg)
    assert len(symbols_list) == 2
    assert "BTC" in symbols_list
    assert "ETH" in symbols_list

