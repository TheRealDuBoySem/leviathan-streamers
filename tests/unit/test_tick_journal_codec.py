"""Unit tests for tick journal TradeTick codec."""

import pytest

from core.journal.tick_journal_codec import tick_from_dict, tick_to_dict
from leviathan_common.models.trade_tick import TradeTick


def test_tick_from_dict_rejects_missing_field():
    with pytest.raises(ValueError, match="missing required field 'trade_id'"):
        tick_from_dict(
            {"inst_id": "BTCUSDT", "ts": 1, "price": 1.0, "size": 1.0, "side": "buy"}
        )


def test_tick_from_dict_rejects_non_dict():
    with pytest.raises(TypeError, match="must be a dictionary"):
        tick_from_dict([])  # type: ignore[arg-type]


def test_tick_codec_round_trip():
    tick = TradeTick("BTCUSDT", 1000, 100.0, 1.0, "buy", "t1")
    restored = tick_from_dict(tick_to_dict(tick))
    assert restored.inst_id == tick.inst_id
    assert restored.ts == tick.ts
    assert restored.price == tick.price
    assert restored.size == tick.size
    assert restored.side == tick.side
    assert restored.trade_id == tick.trade_id
