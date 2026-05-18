import pytest
import asyncio
import websockets
import json
from main import start_app

@pytest.mark.asyncio
async def test_system_end_to_end():
    async def mock_bitget_server(websocket):
        await websocket.send(json.dumps({"event": "subscribe", "arg": {"instId": "BTCUSDT"}}))
        await asyncio.sleep(0.1)
        trade_data = {"action": "snapshot", "arg": {"channel": "trade", "instId": "BTCUSDT"}, "data": [{"ts": 1, "price": "1000", "size": "1", "side": "buy", "tradeId": "abc"}]}
        await websocket.send(json.dumps(trade_data))
        await asyncio.sleep(2)
        
    async with websockets.serve(mock_bitget_server, "localhost", 8765):
        messages_processed = await asyncio.wait_for(start_app("ws://localhost:8765", ["BTCUSDT"], max_messages=1), timeout=5.0)
        assert messages_processed == 1

@pytest.mark.asyncio
async def test_system_contracts():
    """Verify Design by Contract preconditions for start_app."""
    # URL validations
    with pytest.raises(TypeError, match="url must be a string"):
        await start_app(123, ["BTCUSDT"])
    with pytest.raises(ValueError, match="url cannot be empty"):
        await start_app("", ["BTCUSDT"])
        
    # symbols validations
    with pytest.raises(TypeError, match="symbols must be a list"):
        await start_app("ws://localhost", "not a list")
    with pytest.raises(TypeError, match="symbols must be strings"):
        await start_app("ws://localhost", ["BTC", 123])
    with pytest.raises(ValueError, match="symbols must be non-empty strings"):
        await start_app("ws://localhost", ["BTC", ""])
        
    # max_messages validations
    with pytest.raises(TypeError, match="max_messages must be an integer"):
        await start_app("ws://localhost", ["BTC"], max_messages="not an int")
    with pytest.raises(ValueError, match="max_messages cannot be negative"):
        await start_app("ws://localhost", ["BTC"], max_messages=-1)

