import time
from core.interfaces.base import IWatchdog

class SilenceWatchdog(IWatchdog):
    """
    Monitors activity and reports if a silence timeout has been reached.
    
    Invariants:
        - timeout_seconds must be a positive number.
    """
    def __init__(self, timeout_seconds: float = 60.0):
        """
        Initialize the watchdog.
        
        Preconditions:
            - timeout_seconds must be positive.
        """
        if not isinstance(timeout_seconds, (int, float)):
            raise TypeError("timeout_seconds must be a number (int or float)")
        if timeout_seconds <= 0:
            raise ValueError(f"timeout_seconds must be positive, got {timeout_seconds}")
            
        self.__timeout = timeout_seconds
        self.__last_activity = time.time()

    @property
    def timeout_seconds(self) -> float:
        """
        [Completeness] Return the configured silence timeout in seconds.
        """
        return self.__timeout

    @property
    def last_activity(self) -> float:
        """
        [Completeness] Return the absolute timestamp of the last recorded activity.
        """
        return self.__last_activity

    @property
    def elapsed_silence(self) -> float:
        """
        [Completeness] Return the elapsed silence duration in seconds since the last activity.
        """
        return float(time.time() - self.__last_activity)

    def ping(self) -> None:
        """
        Reset the watchdog timer.
        
        Postconditions:
            - last_activity is updated to current time.
        """
        self.__last_activity = time.time()

    def check_health(self) -> bool:
        """
        Verify if the silence timeout has been reached.
        
        Postconditions:
            - Returns True if healthy (within timeout), False otherwise.
        """
        return self.elapsed_silence <= self.__timeout
