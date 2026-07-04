import math
import time

from core.interfaces.base import IWatchdog


class SilenceWatchdog(IWatchdog):
    """
    Monitors activity and reports if a silence timeout has been reached.

    Pattern: Strategy (IWatchdog) — interchangeable silence monitor for resilient transports.

    Invariants:
        - timeout_seconds is a finite positive number.
    """

    def __init__(self, timeout_seconds: float = 60.0):
        """
        Initialize the watchdog.

        Preconditions:
            - timeout_seconds must be a finite positive number.
        """
        if not isinstance(timeout_seconds, (int, float)):
            raise TypeError("timeout_seconds must be a number (int or float)")
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError(
                f"timeout_seconds must be a finite positive number, got {timeout_seconds}"
            )

        self.__timeout = float(timeout_seconds)
        self.__last_activity = time.monotonic()

    @property
    def timeout_seconds(self) -> float:
        """
        [Completeness] Return the configured silence timeout in seconds.
        """
        return self.__timeout

    @property
    def last_activity(self) -> float:
        """
        [Completeness] Return the monotonic timestamp of the last recorded activity.
        """
        return self.__last_activity

    @property
    def elapsed_silence(self) -> float:
        """
        [Completeness] Return the elapsed silence duration in seconds since the last activity.
        """
        return float(time.monotonic() - self.__last_activity)

    def ping(self) -> None:
        """
        Reset the watchdog timer.

        Postconditions:
            - last_activity is updated to the current monotonic time.
        """
        self.__last_activity = time.monotonic()

    def check_health(self) -> bool:
        """
        Verify if the silence timeout has been reached.

        Postconditions:
            - Returns True while elapsed silence is less than or equal to the timeout.
            - Returns False once elapsed silence exceeds the timeout.
        """
        return self.elapsed_silence <= self.__timeout
