"""
Append-only durable tick journal for beta supervised restarts.

Pattern: Repository — persists TradeTick records with monotonic sequence numbers.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Iterator

from core.journal.journal_file_lock import JournalFileLock
from core.journal.journal_incremental_reader import (
    DEFAULT_INCOMPLETE_RECORD_MAX_WAIT_SECONDS,
    JournalIncrementalReader,
)
from core.journal.journal_io import (
    atomic_write_json,
    preview_journal_line,
    should_log_invalid_line,
)
from core.journal.symbol_dedup_bucket import SymbolDedupBucket
from core.journal.tick_journal_codec import tick_from_dict, tick_to_dict
from core.journal.tick_journal_cursor import TickJournalCursor
from leviathan_common.models.trade_tick import TradeTick

# Re-exports retained for existing `from core.journal.tick_journal import ...` sites.
__all__ = [
    "COMPACT_MIN_LAG_SEQ",
    "DEFAULT_DEDUP_WINDOW",
    "DEFAULT_EMPTY_POLL_DIAGNOSTIC_SECONDS",
    "DEFAULT_INCOMPLETE_RECORD_MAX_WAIT_SECONDS",
    "JournalIncrementalReader",
    "META_PERSIST_INTERVAL",
    "SEQ_INDEX_INTERVAL",
    "TickJournal",
    "TickJournalCursor",
    "tick_from_dict",
    "tick_to_dict",
]

logger = logging.getLogger(__name__)

DEFAULT_DEDUP_WINDOW = 10_000
SEQ_INDEX_INTERVAL = 500
META_PERSIST_INTERVAL = 50
COMPACT_MIN_LAG_SEQ = 5_000
# D4-04: emit unread lag diagnostics while cold-start / tail-follow yields nothing.
DEFAULT_EMPTY_POLL_DIAGNOSTIC_SECONDS = 5.0

_TICK_JOURNAL_FILE = "tick_journal.jsonl"
_TICK_JOURNAL_META_FILE = "tick_journal.meta.json"
_TICK_JOURNAL_CURSOR_FILE = "tick_journal.cursor.json"
_TICK_JOURNAL_LOCK_FILE = "tick_journal.lock"
_TICK_JOURNAL_QUARANTINE_FILE = "tick_journal.quarantine.jsonl"


class TickJournal:
    """
    Append-only JSONL journal with (inst_id, trade_id) deduplication.

    Invariants:
        - latest_seq is monotonically non-decreasing.
        - Each seq maps to at most one tick line in the journal file.
    """

    def __init__(
        self,
        checkpoint_dir: str,
        *,
        dedup_window: int = DEFAULT_DEDUP_WINDOW,
        seq_index_interval: int = SEQ_INDEX_INTERVAL,
    ) -> None:
        if not isinstance(checkpoint_dir, str) or not checkpoint_dir.strip():
            raise ValueError("checkpoint_dir must be a non-empty string")
        if dedup_window <= 0:
            raise ValueError("dedup_window must be positive")
        if seq_index_interval <= 0:
            raise ValueError("seq_index_interval must be positive")
        normalized_dir = checkpoint_dir.strip()
        self.__checkpoint_dir = normalized_dir
        self.__journal_path = os.path.join(normalized_dir, _TICK_JOURNAL_FILE)
        self.__meta_path = os.path.join(normalized_dir, _TICK_JOURNAL_META_FILE)
        self.__cursor_path = os.path.join(normalized_dir, _TICK_JOURNAL_CURSOR_FILE)
        self.__lock_path = os.path.join(normalized_dir, _TICK_JOURNAL_LOCK_FILE)
        self.__quarantine_path = os.path.join(normalized_dir, _TICK_JOURNAL_QUARANTINE_FILE)
        self.__dedup_window = dedup_window
        self.__seq_index_interval = seq_index_interval
        self.__thread_lock = threading.Lock()
        self.__append_counter = 0
        os.makedirs(normalized_dir, exist_ok=True)
        self.__meta = self.__load_meta()
        self.__dedup_buckets = self.__hydrate_dedup_buckets()

    @property
    def journal_path(self) -> str:
        return self.__journal_path

    @property
    def cursor_path(self) -> str:
        return self.__cursor_path

    @property
    def quarantine_path(self) -> str:
        return self.__quarantine_path

    def create_incremental_reader(self, **reader_kwargs) -> JournalIncrementalReader:
        return JournalIncrementalReader(self, **reader_kwargs)

    def quarantine_line(self, line: str, *, reason: str) -> None:
        """Append a rejected journal line for forensics without blocking the reader."""
        if not isinstance(line, str):
            raise TypeError("line must be a string")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")
        payload = {
            "ts_ms": int(time.time() * 1000),
            "reason": reason.strip(),
            "line": line,
        }
        encoded = json.dumps(payload, separators=(",", ":")) + "\n"
        try:
            with open(self.__quarantine_path, "a", encoding="utf-8") as handle:
                handle.write(encoded)
                handle.flush()
        except OSError as exc:
            logger.warning(
                "Failed to quarantine invalid journal line (%s): %s",
                reason.strip(),
                exc,
            )

    def __load_meta(self) -> dict:
        if not os.path.exists(self.__meta_path):
            return {"latest_seq": 0, "seen_trade_ids": {}, "seq_index": [[0, 0]]}
        with open(self.__meta_path, "r", encoding="utf-8") as handle:
            loaded = json.load(handle)
        if not isinstance(loaded, dict):
            raise ValueError("tick journal meta must be a JSON object")
        loaded.setdefault("latest_seq", 0)
        loaded.setdefault("seen_trade_ids", {})
        loaded.setdefault("seq_index", [[0, 0]])
        return loaded

    def __hydrate_dedup_buckets(self) -> dict[str, SymbolDedupBucket]:
        buckets: dict[str, SymbolDedupBucket] = {}
        seen_raw = self.__meta.get("seen_trade_ids", {})
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

    def __persist_meta(self) -> None:
        payload = dict(self.__meta)
        payload["seen_trade_ids"] = self.__serialize_dedup_buckets()
        atomic_write_json(self.__meta_path, payload)

    def __record_seq_index(self, seq: int, byte_offset: int) -> None:
        index: list = self.__meta.setdefault("seq_index", [[0, 0]])
        if not index:
            index.append([0, 0])
        last_seq = int(index[-1][0])
        if seq == 0 or (seq % self.__seq_index_interval == 0 and seq > last_seq):
            index.append([seq, byte_offset])
        if len(index) > 256:
            self.__meta["seq_index"] = index[-256:]

    def byte_offset_for_seq(self, start_seq: int) -> int:
        """
        Return the BOL byte offset where incremental reads should begin for
        ``start_seq``.

        Uses the sparse ``seq_index`` as a hint, then walks forward to the first
        complete line whose seq is >= start_seq. Invalid lines are skipped
        silently during resolution (no quarantine) so restore/reset cannot
        re-enter already-passed poison just to locate the tip.
        """
        if not isinstance(start_seq, int) or start_seq < 0:
            raise ValueError("start_seq must be a non-negative integer")
        if start_seq <= 1:
            return 0
        return self.__resolve_byte_offset_for_seq(
            start_seq,
            self.__indexed_byte_offset_hint(start_seq),
        )

    def __indexed_byte_offset_hint(self, start_seq: int) -> int:
        index = self.__meta.get("seq_index", [[0, 0]])
        chosen = [0, 0]
        for entry in index:
            if not isinstance(entry, list) or len(entry) != 2:
                continue
            seq_value = int(entry[0])
            if seq_value <= start_seq:
                chosen = [seq_value, int(entry[1])]
            else:
                break
        return int(chosen[1])

    def reload_seq_index_from_disk(self) -> None:
        """
        Reload sparse ``seq_index`` from persisted meta (D4-01).

        Compaction in another process rewrites byte offsets; a live reader's
        in-memory index must not keep pre-rewrite hints that point past EOF.
        """
        try:
            loaded = self.__load_meta()
        except (OSError, json.JSONDecodeError, ValueError):
            return
        index = loaded.get("seq_index", [[0, 0]])
        if not isinstance(index, list):
            return
        with self.__thread_lock:
            self.__meta["seq_index"] = index

    def __resolve_byte_offset_for_seq(self, start_seq: int, indexed_offset: int) -> int:
        if not os.path.exists(self.__journal_path):
            return indexed_offset
        try:
            with open(self.__journal_path, "rb") as handle:
                handle.seek(0, os.SEEK_END)
                file_size = handle.tell()
                # D4-01: a stale sparse hint from a pre-rewrite generation is
                # larger than the new file. Clamping to EOF would skip every
                # retained record; restart the walk from BOL instead.
                if indexed_offset > file_size:
                    offset = 0
                else:
                    offset = min(max(0, indexed_offset), file_size)
                if offset > 0:
                    handle.seek(offset - 1)
                    if handle.read(1) != b"\n":
                        handle.seek(offset)
                        handle.readline()
                        offset = handle.tell()
                handle.seek(offset)
                while True:
                    line_start = handle.tell()
                    line = handle.readline()
                    if not line:
                        return line_start
                    if not line.endswith(b"\n"):
                        return line_start
                    stripped = line.strip()
                    if not stripped:
                        continue
                    try:
                        record = json.loads(stripped.decode("utf-8"))
                        seq = int(record["seq"])
                    except (
                        UnicodeDecodeError,
                        json.JSONDecodeError,
                        KeyError,
                        TypeError,
                        ValueError,
                    ):
                        continue
                    if seq >= start_seq:
                        return line_start
        except OSError:
            return indexed_offset

    def latest_seq(self) -> int:
        with self.__thread_lock:
            return int(self.__meta.get("latest_seq", 0))

    def read_latest_seq_from_disk(self) -> int:
        """
        Return latest_seq from persisted meta without mutating in-memory state.

        Used by the supervisor during collector handoff when another process
        appends ticks to the journal.
        """
        try:
            meta = self.__load_meta()
        except (OSError, json.JSONDecodeError, ValueError):
            return self.latest_seq()
        return int(meta.get("latest_seq", 0))

    def load_cursor(self) -> TickJournalCursor:
        if not os.path.exists(self.__cursor_path):
            return TickJournalCursor()
        try:
            with open(self.__cursor_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"tick journal cursor is not valid JSON: {self.__cursor_path}"
            ) from exc
        return TickJournalCursor.from_dict(payload)

    def save_cursor(self, cursor: TickJournalCursor) -> None:
        if not isinstance(cursor, TickJournalCursor):
            raise TypeError("cursor must be a TickJournalCursor instance")
        atomic_write_json(self.__cursor_path, cursor.to_dict())

    def append(self, tick: TradeTick) -> int:
        """
        Append a tick and return its assigned sequence number.

        Duplicate (inst_id, trade_id) pairs inside the dedup window return the
        current latest_seq without writing a new journal line.
        """
        if not isinstance(tick, TradeTick):
            raise TypeError("tick must be a TradeTick instance")
        with JournalFileLock(self.__lock_path):
            with self.__thread_lock:
                symbol = tick.inst_id.upper()
                bucket = self.__dedup_buckets.setdefault(
                    symbol,
                    SymbolDedupBucket(self.__dedup_window),
                )
                dedup_key = tick.trade_id
                if bucket.contains(dedup_key):
                    return int(self.__meta.get("latest_seq", 0))
                next_seq = int(self.__meta.get("latest_seq", 0)) + 1
                record = {"seq": next_seq, "tick": tick_to_dict(tick)}
                encoded = json.dumps(record, separators=(",", ":")) + "\n"
                with open(self.__journal_path, "a", encoding="utf-8") as handle:
                    byte_offset = handle.tell()
                    handle.write(encoded)
                    handle.flush()
                    os.fsync(handle.fileno())
                bucket.add(dedup_key)
                self.__meta["latest_seq"] = next_seq
                self.__record_seq_index(next_seq, byte_offset)
                self.__append_counter += 1
                if self.__append_counter % META_PERSIST_INTERVAL == 0:
                    self.__persist_meta()
                return next_seq

    def flush_meta(self) -> None:
        with JournalFileLock(self.__lock_path):
            with self.__thread_lock:
                self.__persist_meta()

    def append_supervisor_handoff_pulse(self, symbol: str) -> int:
        """Append a synthetic tick so overlap handoff can detect a new collector process."""
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("symbol must be a non-empty string")
        now_ms = int(time.time() * 1000)
        pulse = TradeTick(
            symbol.strip().upper(),
            now_ms,
            1.0,
            0.0001,
            "buy",
            f"LEV-HANDOFF-{now_ms}",
        )
        return self.append(pulse)

    def tail_from(self, start_seq: int) -> Iterator[tuple[int, TradeTick]]:
        if not isinstance(start_seq, int) or start_seq < 0:
            raise ValueError("start_seq must be a non-negative integer")
        reader = JournalIncrementalReader(self)
        current_seq = start_seq
        while True:
            batch = reader.poll(current_seq)
            if not batch:
                break
            for seq, tick in batch:
                yield seq, tick
                current_seq = seq + 1

    def compact_before_seq(self, min_retain_seq: int) -> int:
        """Rewrite the journal keeping records with seq >= min_retain_seq."""
        if min_retain_seq <= 1:
            return 0
        with JournalFileLock(self.__lock_path):
            with self.__thread_lock:
                if not os.path.exists(self.__journal_path):
                    return 0
                retained: list[str] = []
                removed = 0
                skipped_invalid = 0
                with open(self.__journal_path, "r", encoding="utf-8") as handle:
                    for line in handle:
                        stripped = line.strip()
                        if not stripped:
                            continue
                        try:
                            record = json.loads(stripped)
                            seq = int(record["seq"])
                        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                            skipped_invalid += 1
                            self.quarantine_line(stripped, reason=f"compact_skip: {exc}")
                            if should_log_invalid_line(skipped_invalid):
                                logger.warning(
                                    "TickJournal compact skipped invalid journal line "
                                    "(skipped_total=%s): %s",
                                    skipped_invalid,
                                    preview_journal_line(stripped),
                                )
                            continue
                        if seq < min_retain_seq:
                            removed += 1
                            continue
                        retained.append(json.dumps(record, separators=(",", ":")))
                if removed == 0 and skipped_invalid == 0:
                    return 0
                temp_path = f"{self.__journal_path}.compact.tmp"
                with open(temp_path, "w", encoding="utf-8") as handle:
                    byte_offset = 0
                    new_index = [[0, 0]]
                    for index, line in enumerate(retained):
                        record = json.loads(line)
                        seq = int(record["seq"])
                        if index == 0 or seq % self.__seq_index_interval == 0:
                            new_index.append([seq, byte_offset])
                        handle.write(line + "\n")
                        byte_offset = handle.tell()
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temp_path, self.__journal_path)
                self.__meta["seq_index"] = new_index[-256:]
                self.__persist_meta()
                return removed

    def maybe_compact(self, *, lag_seq: int = COMPACT_MIN_LAG_SEQ) -> int:
        """
        Drop records older than ``cursor.last_processed_seq - lag_seq``.

        Must not be invoked from the engine stop/shutdown path (D4-10): a
        rewrite immediately before the next process starts can leave readers
        with byte offsets past the new file size (feeds D4-01). Prefer an
        offline or collector-owned compaction window instead.
        """
        if lag_seq <= 0:
            raise ValueError("lag_seq must be positive")
        cursor = self.load_cursor()
        min_retain = max(1, cursor.last_processed_seq - lag_seq)
        if cursor.last_processed_seq <= lag_seq:
            return 0
        return self.compact_before_seq(min_retain)
