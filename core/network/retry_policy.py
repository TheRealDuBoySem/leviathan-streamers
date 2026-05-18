from typing import Optional
from core.interfaces.base import IRetryPolicy


class RetryPolicy(IRetryPolicy):
    """
    Defines the retry strategy for network operations.
    
    Invariants:
        - max_retries >= 0
        - max_delay >= 0
    """
    def __init__(self, max_retries: Optional[int] = 5, max_delay: int = 30):
        """
        Initialize the retry policy.
        
        Preconditions:
            - max_retries must be a non-negative integer or None (infinite).
            - max_delay must be a non-negative integer.
        """
        if max_retries is not None:
            if not isinstance(max_retries, int):
                raise TypeError("max_retries must be an integer")
            if max_retries < 0:
                raise ValueError(f"max_retries must be >= 0, got {max_retries}")
                
        if not isinstance(max_delay, int):
            raise TypeError("max_delay must be an integer")
        if max_delay < 0:
            raise ValueError(f"max_delay must be >= 0, got {max_delay}")
            
        self.__max_retries = max_retries
        self.__max_delay = max_delay

    @property
    def max_retries(self) -> Optional[int]:
        """
        [Completeness] Return the maximum connection retry attempts, or None for infinite.
        """
        return self.__max_retries

    @property
    def max_delay(self) -> int:
        """
        [Completeness] Return the maximum retry delay in seconds.
        """
        return self.__max_delay

    def get_delay(self, attempt: int) -> int:
        """
        Calculate the delay for a given attempt using exponential backoff.
        
        Preconditions:
            - attempt must be a non-negative integer.
        
        Postconditions:
            - Returned delay is between 0 and max_delay.
        """
        if not isinstance(attempt, int):
            raise TypeError("attempt must be an integer")
        if attempt < 0:
            raise ValueError(f"attempt must be >= 0, got {attempt}")
            
        delay = min((2 ** attempt), self.__max_delay)
        return delay

    def can_retry(self, attempt: int) -> bool:
        """
        Check if another retry attempt is allowed.
        
        Preconditions:
            - attempt must be a non-negative integer.
        """
        if not isinstance(attempt, int):
            raise TypeError("attempt must be an integer")
        if attempt < 0:
            raise ValueError(f"attempt must be >= 0, got {attempt}")
            
        if self.__max_retries is None:
            return True
        return attempt < self.__max_retries
