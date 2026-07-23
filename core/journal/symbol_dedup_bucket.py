"""
Bounded per-symbol trade_id deduplication window.

Pattern: Bounded Set — FIFO eviction once capacity is reached.
"""

from __future__ import annotations

from collections import deque


class SymbolDedupBucket:
    """Bounded FIFO dedup set for one symbol."""

    def __init__(self, max_size: int) -> None:
        self.__max_size = max_size
        self.__order: deque[str] = deque()
        self.__seen: set[str] = set()

    def contains(self, trade_id: str) -> bool:
        return trade_id in self.__seen

    def add(self, trade_id: str) -> None:
        if trade_id in self.__seen:
            return
        if len(self.__order) >= self.__max_size:
            oldest = self.__order.popleft()
            self.__seen.discard(oldest)
        self.__order.append(trade_id)
        self.__seen.add(trade_id)

    def to_list(self) -> list[str]:
        return list(self.__order)

    @classmethod
    def from_list(cls, trade_ids: list[str], max_size: int) -> "SymbolDedupBucket":
        bucket = cls(max_size)
        for trade_id in trade_ids[-max_size:]:
            bucket.add(str(trade_id))
        return bucket
