import pytest
import time
from core.network.silence_watchdog import SilenceWatchdog

def test_watchdog(mocker):
    w = SilenceWatchdog(timeout_seconds=2)
    assert w.check_health() is True
    
    current_time = time.time()
    # Advance time by 3 seconds
    mocker.patch('time.time', return_value=current_time + 3)
    assert w.check_health() is False
    
    # Ping updates last_activity to current_time + 3
    w.ping()
    assert w.check_health() is True

    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        SilenceWatchdog(timeout_seconds=0)

def test_watchdog_types():
    """Verify Type contract preconditions for SilenceWatchdog."""
    with pytest.raises(TypeError, match="timeout_seconds must be a number"):
        SilenceWatchdog(timeout_seconds="60")

def test_watchdog_properties(mocker):
    """Verify the properties of SilenceWatchdog."""
    current_time = time.time()
    mocker.patch('time.time', return_value=current_time)
    
    w = SilenceWatchdog(timeout_seconds=10)
    assert w.timeout_seconds == 10
    assert w.last_activity == current_time
    assert w.elapsed_silence == 0.0
    
    # Advance time and verify elapsed_silence increases
    mocker.patch('time.time', return_value=current_time + 4.5)
    assert w.elapsed_silence == 4.5
    
    # Verify that properties are read-only
    with pytest.raises(AttributeError):
        w.timeout_seconds = 20
    with pytest.raises(AttributeError):
        w.last_activity = current_time + 10

