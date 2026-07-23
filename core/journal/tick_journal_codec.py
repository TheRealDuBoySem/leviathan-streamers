"""
Serialize and deserialize TradeTick records for the durable tick journal.

Pattern: Codec — pure mapping between TradeTick and journal JSON payloads.
"""

from __future__ import annotations

from leviathan_common.models.trade_tick import TradeTick

_TICK_REQUIRED_FIELDS = ("inst_id", "ts", "price", "size", "side", "trade_id")


def tick_to_dict(tick: TradeTick) -> dict:
    return {
        "inst_id": tick.inst_id,
        "ts": tick.ts,
        "price": tick.price,
        "size": tick.size,
        "side": tick.side,
        "trade_id": tick.trade_id,
    }


def tick_from_dict(data: dict) -> TradeTick:
    if not isinstance(data, dict):
        raise TypeError("tick record must be a dictionary")
    for field in _TICK_REQUIRED_FIELDS:
        if field not in data:
            raise ValueError(f"tick record missing required field '{field}'")
    return TradeTick(
        inst_id=str(data["inst_id"]),
        ts=int(data["ts"]),
        price=float(data["price"]),
        size=float(data["size"]),
        side=str(data["side"]),
        trade_id=str(data["trade_id"]),
    )
