import pytest
import asyncio
from core.network.keep_alive_emitter import KeepAliveEmitter

@pytest.mark.asyncio
async def test_keep_alive():
    emitter = KeepAliveEmitter(interval_seconds=0.01)
    called = []
    async def mock_send(p): called.append(p)
    task = asyncio.create_task(emitter.run(mock_send, "p"))
    await asyncio.sleep(0.02)
    task.cancel()
    assert len(called) >= 1

@pytest.mark.asyncio
async def test_keep_alive_contracts():
    """Verify Design by Contract preconditions for KeepAliveEmitter."""
    with pytest.raises(ValueError, match="interval_seconds must be positive"):
        KeepAliveEmitter(interval_seconds=0)
    
    e = KeepAliveEmitter()
    with pytest.raises(TypeError, match="send_func must be callable"):
        await e.run(None)
    with pytest.raises(ValueError, match="payload cannot be empty"):
        await e.run(lambda x: x, payload="")

@pytest.mark.asyncio
async def test_keep_alive_emitter_types():
    """Verify Type contract preconditions for KeepAliveEmitter."""
    with pytest.raises(TypeError, match="interval_seconds must be a number"):
        KeepAliveEmitter(interval_seconds="30")
    
    e = KeepAliveEmitter()
    with pytest.raises(TypeError, match="payload must be a string"):
        await e.run(lambda x: x, payload=123)

def test_keep_alive_emitter_property():
    """Verify the interval_seconds read-only property."""
    e = KeepAliveEmitter(interval_seconds=42)
    assert e.interval_seconds == 42
    
    # Verify that it is read-only (no setter)
    with pytest.raises(AttributeError):
        e.interval_seconds = 10

