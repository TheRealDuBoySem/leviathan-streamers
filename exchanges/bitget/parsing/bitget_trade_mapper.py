import logging
from typing import Dict, Any, List
from core.models.trade_tick import TradeTick

logger = logging.getLogger(__name__)

class BitgetTradeMapper:
    """Maps raw Bitget data into standardized TradeTick objects."""
    
    DEFAULT_INST_ID = "UNKNOWN"
    
    @staticmethod
    def map(data: Dict[str, Any]) -> List[TradeTick]:
        """
        Map a Bitget trade message to a list of TradeTick objects.
        
        Preconditions:
            - data must be a dictionary containing Bitget trade information.
            
        Postconditions:
            - Returns a list of TradeTick instances.
        """
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data).__name__}")
            
        inst_id = data.get("arg", {}).get("instId", BitgetTradeMapper.DEFAULT_INST_ID)
        trades = data.get("data", [])
        
        if not isinstance(trades, list):
            logger.warning(f"Expected list of trades, got {type(trades).__name__}")
            return []
            
        ticks = []
        for trade in trades:
            try:
                ticks.append(TradeTick(
                    inst_id=inst_id,
                    ts=int(trade.get("ts", 0)),
                    price=float(trade.get("price", 0.0)),
                    size=float(trade.get("size", 0.0)),
                    side=trade.get("side", ""),
                    trade_id=trade.get("tradeId", "")
                ))
            except (ValueError, TypeError) as e:
                # Design by Contract: TradeTick itself validates its invariants
                logger.error(f"Erreur de validation sur un tick : {e}")
            except Exception as e:
                logger.error(f"Erreur de mapping sur un tick isolé : {e}")
        return ticks
