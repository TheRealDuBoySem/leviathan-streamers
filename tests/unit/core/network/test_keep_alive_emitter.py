import asyncio

import pytest

from core.network.keep_alive_emitter import KeepAliveEmitter


async def _run_until_first_send(emitter, mock_send, monkeypatch, *, run_payload=None):
    """Drive KeepAliveEmitter without depending on wall-clock timing."""
    sent = asyncio.Event()
    real_sleep = asyncio.sleep
    task_holder: dict[str, asyncio.Task] = {}

    async def tracking_send(payload: str) -> None:
        await mock_send(payload)
        if not sent.is_set():
            sent.set()
            task = task_holder.get("task")
            if task is not None:
                task.cancel()

    async def immediate_sleep(_seconds: float) -> None:
        # Yield once without waiting the keep-alive interval.
        await real_sleep(0)

    monkeypatch.setattr(
        "core.network.keep_alive_emitter.asyncio.sleep",
        immediate_sleep,
    )
    task = asyncio.create_task(emitter.run(tracking_send, run_payload))
    task_holder["task"] = task
    await asyncio.wait_for(sent.wait(), timeout=1.0)
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_keep_alive(monkeypatch):
    emitter = KeepAliveEmitter(interval_seconds=0.01)
    called = []

    async def mock_send(p):
        called.append(p)

    await _run_until_first_send(emitter, mock_send, monkeypatch, run_payload="p")
    assert called == ["p"]


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


def test_keep_alive_emitter_properties():
    """Verify properties of KeepAliveEmitter."""
    e = KeepAliveEmitter(interval_seconds=42, payload="custom_ping")
    assert e.interval_seconds == 42
    assert e.payload == "custom_ping"

    e_float = KeepAliveEmitter(interval_seconds=0.5)
    assert e_float.interval_seconds == 0.5

    with pytest.raises(AttributeError):
        e.interval_seconds = 10
    with pytest.raises(AttributeError):
        e.payload = "new_ping"

    # Precondition validations on constructor
    with pytest.raises(TypeError, match="payload must be a string"):
        KeepAliveEmitter(payload=123)
    with pytest.raises(ValueError, match="payload cannot be empty"):
        KeepAliveEmitter(payload="")


@pytest.mark.asyncio
async def test_keep_alive_cancellation_propagates():
    """Verify CancelledError is re-raised when the loop task is cancelled."""
    emitter = KeepAliveEmitter(interval_seconds=10)
    task = asyncio.create_task(emitter.run(lambda _: asyncio.sleep(0)))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_keep_alive_default_payload(monkeypatch):
    emitter = KeepAliveEmitter(interval_seconds=0.01, payload="configured_ping")
    called = []

    async def mock_send(p):
        called.append(p)

    await _run_until_first_send(emitter, mock_send, monkeypatch)
    assert called == ["configured_ping"]
