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
        # BB-D23-02 / J25–J31: coherent tip high-water — never report a
        # phantom rewind below a previously observed durable disk tip
        # (J24 latest=1790634; J25/J26 H12; J27 H02/H23; J28 H03/H07/H09/
        # H11/H13/H17/H21/H22 soft-stale sticky latest=1810648 while cursor
        # ~2.13M–~2.17M — ABSENT H02/H23 on J28 ≠ J27; J29 H02 ×3 same
        # 1810648 + post-respawn sticky 2182116 = H03 heal tip baseline,
        # multi-h H04/H08/H10–11/H13–14/H16/H19/H23; J30 same 2182116
        # @H02/H06/H19–H21/H23A while 1810648 ABSENT day-wide; J31 sticky
        # Forced to_seq=2319820 H04–H23 under v0.18.35 — clamp still applies;
        # force_rewind must also receive coherent tip_seq).
        self.__disk_tip_high_water = int(self.__payload.get("latest_seq", 0))

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

        Falls back to the coherent high-water (max of in-memory tip and last
        successful disk observation) when disk meta is unreadable.

        BB-D23-02 / J25–J31: rejects phantom tip rewinds below the observed
        high-water (J24 sticky ``latest=1790634``; J25/J26 H12; J27 H02
        cursor~2034155 / H23 cursor~2131704; J28 H03 cursor~2138203 /
        H13~2158220 / H17~2167213 / H22~2173206 soft-stale sticky
        ``latest=1810648``; J29 H02 cursor~2179706 sticky ``1810648``;
        J29 post-heal sticky ``2182116`` H04 cursor~2182642 /
        H23~2216660; J30 same ``2182116`` H02 cursor~2238113 /
        H06~2249611 / H19~2297597 / H20~2298589 / H21~2300067 /
        H23A~2313486 — ``1810648`` ABSENT; J31 sticky ``2319820`` while
        live ~2.32M–2.38M). High-water advances only on successful disk
        reads — not on unpersisted in-memory ``set_latest_seq`` — so
        META_PERSIST lag stays honest.
        """
        try:
            meta = self.load_payload(self.__meta_path)
        except (OSError, json.JSONDecodeError, ValueError):
            return max(int(self.latest_seq()), int(self.__disk_tip_high_water))
        raw = int(meta.get("latest_seq", 0))
        if raw < 0:
            raw = 0
        if raw < self.__disk_tip_high_water:
            return int(self.__disk_tip_high_water)
        self.__disk_tip_high_water = raw
        return raw

    def reload_from_disk(self) -> None:
        """
        Replace in-memory payload and dedup buckets from disk.

        BB-D23-02 / J25–J31: ``latest_seq`` is clamped to the coherent
        high-water so a phantom disk rewind (e.g. ``1810648`` after ~2.03M
        J27 H02 / ~2.13M H23 / J28 H03~2.138M / H22~2.173M / J29 H02
        ~2.179M; or post-respawn ``2182116`` after J29 H04~2.182M /
        H23~2.216M / J30 H02~2.238M / H06~2.249M / H19–H21~2.297M–
        2.300M / H23A~2.313M; or J31 sticky ``2319820`` after H04~2.321M /
        H15~2.371M) cannot poison ``latest_seq()`` / soft-stale
        diagnostics that bypass ``read_latest_seq_from_disk``. Dedup
        buckets and seq_index still reload from disk as written.
        """
        self.__payload = self.load_payload(self.__meta_path)
        self.__dedup_buckets = self.__hydrate_dedup_buckets()
        loaded_tip = int(self.__payload.get("latest_seq", 0))
        if loaded_tip < 0:
            loaded_tip = 0
        if loaded_tip < self.__disk_tip_high_water:
            loaded_tip = int(self.__disk_tip_high_water)
        else:
            self.__disk_tip_high_water = loaded_tip
        self.__payload["latest_seq"] = loaded_tip

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
