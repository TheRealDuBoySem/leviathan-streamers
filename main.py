import asyncio
import logging
import os
from exchanges.bitget.bitget_stream_factory import BitgetStreamFactory

logger = logging.getLogger(__name__)

async def start_app(url: str, symbols: list, max_messages: int = 0):
    """
    Start the application and orchestrate network ingestion and consumption.
    
    Preconditions:
        - url must be a non-empty string.
        - symbols must be a list of non-empty strings.
        - max_messages must be a non-negative integer.
    """
    # [DbC] Preconditions validations
    if not isinstance(url, str):
        raise TypeError("url must be a string")
    if not url:
        raise ValueError("url cannot be empty")
        
    if not isinstance(symbols, list):
        raise TypeError("symbols must be a list")
    for s in symbols:
        if not isinstance(s, str):
            raise TypeError("symbols must be strings")
        if not s:
            raise ValueError("symbols must be non-empty strings")
            
    if not isinstance(max_messages, int):
        raise TypeError("max_messages must be an integer")
    if max_messages < 0:
        raise ValueError("max_messages cannot be negative")

    # Composition Root via Pattern: Factory
    streamer = BitgetStreamFactory.create_stream(
        url=url,
        symbols=symbols
    )
    
    connect_task = asyncio.create_task(streamer.start_streaming())
    messages_received = 0
    
    async def consumer():
        nonlocal messages_received
        logger.info("Consumer démarré.")
        async for tick in streamer:
            try:
                logger.info(f"CONSUMER | {tick.inst_id} | TS: {tick.ts} | Prix: {tick.price} | Qty: {tick.size} | Côté: {tick.side}")
                messages_received += 1
                if max_messages > 0 and messages_received >= max_messages:
                    logger.info(f"Max messages ({max_messages}) atteints. Arrêt propre du mini-système.")
                    await streamer.stop()
            finally:
                streamer.mark_tick_as_processed()
                
    consumer_task = asyncio.create_task(consumer())
    
    try:
        await connect_task
    except asyncio.CancelledError:
        pass
    finally:
        await streamer.stop()
        consumer_task.cancel()
        
    return messages_received

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    url = os.getenv("WS_URL", "wss://ws.bitget.com/v2/ws/public")
    max_msgs = int(os.getenv("MAX_MESSAGES", "0"))
    
    try:
        asyncio.run(start_app(url, ["BTCUSDT", "ETHUSDT"], max_messages=max_msgs))
    except KeyboardInterrupt:
        print("Interruption (Ctrl+C). Arrêt propre.")

if __name__ == "__main__":
    main()
