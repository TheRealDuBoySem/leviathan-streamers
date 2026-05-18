import asyncio
import logging
from core.models.trade_tick import TradeTick
from core.interfaces.base import IDispatchStrategy

logger = logging.getLogger(__name__)

class AsyncQueueDispatcher(IDispatchStrategy):
    """
    Dispatches data via an asynchronous queue.
    
    Pattern: Strategy (Concrete Implementation)
    """
    def __init__(self, maxsize: int = 10000):
        """
        Initialize the dispatcher.
        
        Preconditions:
            - maxsize must be a positive integer.
        """
        if not isinstance(maxsize, int):
            raise TypeError("maxsize must be an integer")
        if maxsize <= 0:
            raise ValueError(f"maxsize must be positive, got {maxsize}")
            
        self.__queue: asyncio.Queue[TradeTick] = asyncio.Queue(maxsize=maxsize)

    @property
    def maxsize(self) -> int:
        """
        [Completeness] Return the maximum size of the queue.
        """
        return self.__queue.maxsize

    @property
    def full(self) -> bool:
        """
        [Completeness] Return True if the queue is full.
        """
        return self.__queue.full()

    async def dispatch(self, data: TradeTick) -> None:
        """
        Enqueue data for processing.
        
        Preconditions:
            - data must be an instance of TradeTick.
        """
        if not isinstance(data, TradeTick):
            raise TypeError(f"Expected TradeTick, got {type(data).__name__}")
            
        try:
            self.__queue.put_nowait(data)
        except asyncio.QueueFull:
            logger.error(f"ALERTE: Consumer trop lent ! Tick jeté ({data.inst_id}).")

    async def wait_for_next_data(self) -> TradeTick:
        """
        Wait for and return the next trade tick from the queue.
        
        Postconditions:
            - Returns a valid TradeTick instance.
        """
        tick = await self.__queue.get()
        assert isinstance(tick, TradeTick), "Invariant violation: non-TradeTick object in queue"
        return tick

    def task_done(self) -> None:
        """Notify that a previously enqueued task is complete."""
        self.__queue.task_done()

    def qsize(self) -> int:
        """Return the current size of the queue."""
        return self.__queue.qsize()

    def empty(self) -> bool:
        """Check if the queue is empty."""
        return self.__queue.empty()
