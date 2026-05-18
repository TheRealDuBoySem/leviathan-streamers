import pytest
from core.network.retry_policy import RetryPolicy

def test_policy():
    p = RetryPolicy(max_retries=2, max_delay=5)
    assert p.get_delay(0) == 1
    assert p.get_delay(2) == 4
    assert p.get_delay(5) == 5
    assert p.can_retry(1) is True
    assert p.can_retry(2) is False

    # Test infinite retries
    p_inf = RetryPolicy(max_retries=None)
    assert p_inf.max_retries is None
    assert p_inf.can_retry(0) is True
    assert p_inf.can_retry(999999) is True


def test_retry_policy_contracts():
    """Verify Design by Contract preconditions for RetryPolicy."""
    with pytest.raises(ValueError, match="max_retries must be >= 0"):
        RetryPolicy(max_retries=-1)
    with pytest.raises(ValueError, match="max_delay must be >= 0"):
        RetryPolicy(max_delay=-1)
    
    p = RetryPolicy()
    with pytest.raises(ValueError, match="attempt must be >= 0"):
        p.get_delay(-1)
    with pytest.raises(ValueError, match="attempt must be >= 0"):
        p.can_retry(-1)

def test_retry_policy_types():
    """Verify Type contract preconditions for RetryPolicy."""
    with pytest.raises(TypeError, match="max_retries must be an integer"):
        RetryPolicy(max_retries="5")
    with pytest.raises(TypeError, match="max_delay must be an integer"):
        RetryPolicy(max_delay="30")
        
    p = RetryPolicy()
    with pytest.raises(TypeError, match="attempt must be an integer"):
        p.get_delay(1.5)
    with pytest.raises(TypeError, match="attempt must be an integer"):
        p.can_retry("1")

def test_retry_policy_properties():
    """Verify the properties of RetryPolicy."""
    p = RetryPolicy(max_retries=10, max_delay=60)
    assert p.max_retries == 10
    assert p.max_delay == 60
    
    # Verify that properties are read-only
    with pytest.raises(AttributeError):
        p.max_retries = 5
    with pytest.raises(AttributeError):
        p.max_delay = 30

