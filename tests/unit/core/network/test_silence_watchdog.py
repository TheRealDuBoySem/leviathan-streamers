import math

import pytest
import time

from core.network.silence_watchdog import SilenceWatchdog


def test_watchdog(mocker):
    current_time = time.monotonic()
    mocker.patch("time.monotonic", return_value=current_time)
    w = SilenceWatchdog(timeout_seconds=2)
    assert w.check_health() is True

    # Advance time by 3 seconds
    mocker.patch("time.monotonic", return_value=current_time + 3)
    assert w.check_health() is False

    # Ping updates last_activity to current_time + 3
    w.ping()
    assert w.check_health() is True


def test_watchdog_contracts():
    """Verify Design by Contract preconditions for SilenceWatchdog."""
    with pytest.raises(ValueError, match="timeout_seconds must be a finite positive number"):
        SilenceWatchdog(timeout_seconds=0)
    with pytest.raises(ValueError, match="timeout_seconds must be a finite positive number"):
        SilenceWatchdog(timeout_seconds=-1)
    with pytest.raises(ValueError, match="timeout_seconds must be a finite positive number"):
        SilenceWatchdog(timeout_seconds=math.nan)
    with pytest.raises(ValueError, match="timeout_seconds must be a finite positive number"):
        SilenceWatchdog(timeout_seconds=math.inf)


def test_watchdog_types():
    """Verify Type contract preconditions for SilenceWatchdog."""
    with pytest.raises(TypeError, match="timeout_seconds must be a number"):
        SilenceWatchdog(timeout_seconds="60")

def test_watchdog_properties(mocker):
    """Verify the properties of SilenceWatchdog."""
    current_time = time.monotonic()
    mocker.patch("time.monotonic", return_value=current_time)

    w = SilenceWatchdog(timeout_seconds=10)
    assert w.timeout_seconds == 10
    assert w.last_activity == current_time
    assert w.elapsed_silence == 0.0

    # Advance time and verify elapsed_silence increases. Use approx because monotonic()
    # returns a large float, so (current_time + 4.5) - current_time carries FP rounding noise.
    mocker.patch("time.monotonic", return_value=current_time + 4.5)
    assert w.elapsed_silence == pytest.approx(4.5)

    # Verify that properties are read-only
    with pytest.raises(AttributeError):
        w.timeout_seconds = 20
    with pytest.raises(AttributeError):
        w.last_activity = current_time + 10


def test_watchdog_timeout_boundary(mocker):
    """Health remains True at the exact timeout, False immediately after."""
    current_time = time.monotonic()
    mocker.patch("time.monotonic", return_value=current_time)
    w = SilenceWatchdog(timeout_seconds=2)

    mocker.patch("time.monotonic", return_value=current_time + 2)
    assert w.check_health() is True

    mocker.patch("time.monotonic", return_value=current_time + 2.001)
    assert w.check_health() is False

