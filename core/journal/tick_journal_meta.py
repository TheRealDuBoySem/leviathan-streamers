"""
Persisted tick-journal metadata (latest_seq, dedup windows, seq_index).

Pattern: Repository — owns meta JSON load/persist and in-memory dedup buckets.
"""

from __future__ import annotations

import json
import os

from core.journal.journal_io import atomic_write_json
from core.journal.symbol_dedup_bucket import SymbolDedupBucket


class TickJournalMetaStore:
    """
    In-memory + on-disk store for tick journal metadata.

    Invariants:
        - latest_seq is monotonically non-decreasing while this process appends.
        - seen_trade_ids buckets respect the configured dedup_window capacity.
    """

    def __init__(self, meta_path: str, *, dedup_window: int) -> None:
        if not isinstance(meta_path, str) or not meta_path.strip():
            raise ValueError("meta_path must be a non-empty string")
        if dedup_window <= 0:
            raise ValueError("dedup_window must be positive")
        self.__meta_path = meta_path.strip()
        self.__dedup_window = dedup_window
        self.__payload = self.load_payload(self.__meta_path)
        self.__dedup_buckets = self.__hydrate_dedup_buckets()

    @property
    def meta_path(self) -> str:
        return self.__meta_path

    @property
    def dedup_window(self) -> int:
        return self.__dedup_window

    @staticmethod
    def load_payload(meta_path: str) -> dict:
        if not os.path.exists(meta_path):
            return {"latest_seq": 0, "seen_trade_ids": {}, "seq_index": [[0, 0]]}
        with open(meta_path, "r", encoding="utf-8") as handle:
            loaded = json.load(handle)
        if not isinstance(loaded, dict):
            raise ValueError("tick journal meta must be a JSON object")
        loaded.setdefault("latest_seq", 0)
        loaded.setdefault("seen_trade_ids", {})
        loaded.setdefault("seq_index", [[0, 0]])
        return loaded

    def __hydrate_dedup_buckets(self) -> dict[str, SymbolDedupBucket]:
        buckets: dict[str, SymbolDedupBucket] = {}
        seen_raw = self.__payload.get("seen_trade_ids", {})
        if not isinstance(seen_raw, dict):
            return buckets
        for symbol, trade_ids in seen_raw.items():
            if isinstance(trade_ids, list):
                buckets[str(symbol).upper()] = SymbolDedupBucket.from_list(
                    [str(item) for item in trade_ids],
                    self.__dedup_window,
                )
        return buckets

    def __serialize_dedup_buckets(self) -> dict[str, list[str]]:
        return {
            symbol: bucket.to_list()
            for symbol, bucket in self.__dedup_buckets.items()
        }

    def latest_seq(self) -> int:
        return int(self.__payload.get("latest_seq", 0))

    def set_latest_seq(self, seq: int) -> None:
        if not isinstance(seq, int) or seq < 0:
            raise ValueError("seq must be a non-negative integer")
        self.__payload["latest_seq"] = seq

    def seq_index(self) -> list:
        index = self.__payload.setdefault("seq_index", [[0, 0]])
        if not isinstance(index, list):
            self.__payload["seq_index"] = [[0, 0]]
            return self.__payload["seq_index"]
        return index

    def replace_seq_index(self, index: list) -> None:
        if not isinstance(index, list):
            raise TypeError("seq_index must be a list")
        self.__payload["seq_index"] = index

    def get_or_create_bucket(self, symbol: str) -> SymbolDedupBucket:
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("symbol must be a non-empty string")
        key = symbol.strip().upper()
        return self.__dedup_buckets.setdefault(
            key,
            SymbolDedupBucket(self.__dedup_window),
        )

    def persist(self) -> None:
        payload = dict(self.__payload)
        payload["seen_trade_ids"] = self.__serialize_dedup_buckets()
        atomic_write_json(self.__meta_path, payload)

    def read_latest_seq_from_disk(self) -> int:
        """
        Return latest_seq from persisted meta without mutating in-memory state.

        Falls back to the in-memory latest_seq when disk meta is unreadable.
        """
        try:
            meta = self.load_payload(self.__meta_path)
        except (OSError, json.JSONDecodeError, ValueError):
            return self.latest_seq()
        return int(meta.get("latest_seq", 0))

    def reload_from_disk(self) -> None:
        """Replace in-memory payload and dedup buckets from disk."""
        self.__payload = self.load_payload(self.__meta_path)
        self.__dedup_buckets = self.__hydrate_dedup_buckets()

    def reload_seq_index_from_disk(self) -> None:
        """
        Reload sparse seq_index from persisted meta (D4-01).

        Compaction in another process rewrites byte offsets; a live reader's
        in-memory index must not keep pre-rewrite hints that point past EOF.
        """
        try:
            loaded = self.load_payload(self.__meta_path)
        except (OSError, json.JSONDecodeError, ValueError):
            return
        index = loaded.get("seq_index", [[0, 0]])
        if not isinstance(index, list):
            return
        self.__payload["seq_index"] = index
