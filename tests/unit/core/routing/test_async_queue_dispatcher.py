import pytest
from core.routing.async_queue_dispatcher import AsyncQueueDispatcher
from core.models.trade_tick import TradeTick

@pytest.mark.asyncio
async def test_dispatcher():
    d = AsyncQueueDispatcher(maxsize=1)
    t = TradeTick("B", 1, 1.0, 1.0, "buy", "1")
    await d.dispatch(t)
    assert d.qsize() == 1
    await d.dispatch(t) # Fails silently due to maxsize=1 (logs error)
    assert d.qsize() == 1
    
    # Test wait_for_next_data and task_done
    retrieved = await d.wait_for_next_data()
    assert retrieved == t
    d.task_done()
    assert d.empty()

@pytest.mark.asyncio
async def test_dispatcher_contracts():
    """Verify Design by Contract preconditions for AsyncQueueDispatcher."""
    with pytest.raises(ValueError, match="maxsize must be positive"):
        AsyncQueueDispatcher(maxsize=0)
    
    d = AsyncQueueDispatcher()
    with pytest.raises(TypeError, match="Expected TradeTick"):
        await d.dispatch("not a tick")

def test_dispatcher_types():
    """Verify Type contract preconditions for AsyncQueueDispatcher."""
    with pytest.raises(TypeError, match="maxsize must be an integer"):
        AsyncQueueDispatcher(maxsize="10")

def test_dispatcher_properties():
    """Verify the properties of AsyncQueueDispatcher."""
    d = AsyncQueueDispatcher(maxsize=5)
    assert d.maxsize == 5
    assert d.full is False
    
    # Verify that properties are read-only
    with pytest.raises(AttributeError):
        d.maxsize = 20
    with pytest.raises(AttributeError):
        d.full = True

