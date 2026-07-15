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
from collections import deque
from dataclasses import dataclass
from typing import Iterator, Optional

from core.journal.journal_file_lock import JournalFileLock
from leviathan_common.models.trade_tick import TradeTick

logger = logging.getLogger(__name__)

DEFAULT_DEDUP_WINDOW = 10_000
SEQ_INDEX_INTERVAL = 500
META_PERSIST_INTERVAL = 50
COMPACT_MIN_LAG_SEQ = 5_000
_INVALID_LINE_PREVIEW_CHARS = 120

_TICK_JOURNAL_FILE = "tick_journal.jsonl"
_TICK_JOURNAL_META_FILE = "tick_journal.meta.json"
_TICK_JOURNAL_CURSOR_FILE = "tick_journal.cursor.json"
_TICK_JOURNAL_LOCK_FILE = "tick_journal.lock"
_TICK_JOURNAL_QUARANTINE_FILE = "tick_journal.quarantine.jsonl"
_TICK_REQUIRED_FIELDS = ("inst_id", "ts", "price", "size", "side", "trade_id")


def _should_log_invalid_line(skipped_count: int) -> bool:
    """Rate-limit invalid-line warnings while keeping a rising counter visible."""
    if skipped_count <= 3:
        return True
    if skipped_count in (10, 50, 100):
        return True
    return skipped_count % 500 == 0


def _preview_journal_line(line: str) -> str:
    preview = line.replace("\n", "\\n")
    if len(preview) > _INVALID_LINE_PREVIEW_CHARS:
        return preview[:_INVALID_LINE_PREVIEW_CHARS] + "..."
    return preview


def _atomic_write_json(path: str, payload: dict) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, separators=(",", ":"))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp_path, path)


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


@dataclass(frozen=True)
class TickJournalCursor:
    last_processed_seq: int = 0

    def to_dict(self) -> dict:
        return {"last_processed_seq": self.last_processed_seq}

    @staticmethod
    def from_dict(data: dict) -> "TickJournalCursor":
        if not isinstance(data, dict):
            raise TypeError("cursor data must be a dictionary")
        seq = data.get("last_processed_seq", 0)
        if not isinstance(seq, int) or seq < 0:
            raise ValueError("last_processed_seq must be a non-negative integer")
        return TickJournalCursor(last_processed_seq=seq)


class _SymbolDedupBucket:
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
    def from_list(cls, trade_ids: list[str], max_size: int) -> "_SymbolDedupBucket":
        bucket = cls(max_size)
        for trade_id in trade_ids[-max_size:]:
            bucket.add(str(trade_id))
        return bucket


class JournalIncrementalReader:
    """Reads new journal records incrementally without rescanning the full file.

    Incomplete trailing lines (no newline yet) are left unread until complete.
    Complete but invalid lines are quarantined and skipped so they cannot poison
    the tail-follow loop or respawn retries. Recovery from a skip episode is
    logged once (edge-triggered) when a valid JSON record is read again.
    """

    def __init__(self, journal: "TickJournal") -> None:
        self.__journal = journal
        self.__read_offset = 0
        self.__next_seq = 1
        self.__skipped_invalid_lines = 0
        self.__consecutive_parse_failures = 0
        self.__last_skip_reason: Optional[str] = None

    def reset_from_seq(self, start_seq: int) -> None:
        if not isinstance(start_seq, int) or start_seq < 0:
            raise ValueError("start_seq must be a non-negative integer")
        self.__read_offset = self.__journal.byte_offset_for_seq(start_seq)
        self.__next_seq = start_seq
        # Seek starts a new read position; do not emit a false recovery signal.
        self.__consecutive_parse_failures = 0
        self.__last_skip_reason = None

    def get_invalid_line_skip_count(self) -> int:
        """Return lifetime count of complete lines quarantined/skipped as invalid."""
        return self.__skipped_invalid_lines

    def get_consecutive_parse_failures(self) -> int:
        """Return current streak of skipped invalid lines since the last valid record."""
        return self.__consecutive_parse_failures

    def poll(self, start_seq: int) -> list[tuple[int, TradeTick]]:
        if start_seq != self.__next_seq:
            self.reset_from_seq(start_seq)
        return self.__read_new_records()

    def __log_recovery_if_needed(self) -> None:
        if self.__consecutive_parse_failures <= 0:
            return
        logger.info(
            "JournalIncrementalReader recovered after %s consecutive parse failures "
            "/ skipped invalid lines (last reason=%s)",
            self.__consecutive_parse_failures,
            self.__last_skip_reason or "unknown",
        )
        self.__consecutive_parse_failures = 0
        self.__last_skip_reason = None

    def __quarantine_invalid_line(self, line: str, reason: str) -> None:
        self.__skipped_invalid_lines += 1
        self.__consecutive_parse_failures += 1
        self.__last_skip_reason = reason
        count = self.__skipped_invalid_lines
        self.__journal.quarantine_line(line, reason=reason)
        if _should_log_invalid_line(count):
            logger.warning(
                "JournalIncrementalReader skipped invalid journal line "
                "(skipped_total=%s, offset=%s, reason=%s): %s",
                count,
                self.__read_offset,
                reason,
                _preview_journal_line(line),
            )

    def __parse_complete_line(self, line: str) -> Optional[tuple[int, TradeTick]]:
        stripped = line.strip()
        if not stripped:
            return None
        try:
            record = json.loads(stripped)
        except json.JSONDecodeError as exc:
            self.__quarantine_invalid_line(stripped, reason=str(exc))
            return None
        if not isinstance(record, dict):
            self.__quarantine_invalid_line(stripped, reason="record is not a JSON object")
            return None
        try:
            seq = int(record["seq"])
            tick = tick_from_dict(record["tick"])
        except (KeyError, TypeError, ValueError) as exc:
            self.__quarantine_invalid_line(stripped, reason=str(exc))
            return None
        self.__log_recovery_if_needed()
        return seq, tick

    def __resync_if_journal_rewritten(self, journal_path: str) -> None:
        """
        Rebind the byte cursor after a concurrent compact/rewrite.

        ``compact_before_seq`` replaces the journal file via ``os.replace``. A
        live reader that still holds a pre-rewrite offset seeks past EOF and
        starves forever (NEW-01). Detect that and resync from the logical seq.
        """
        try:
            file_size = os.path.getsize(journal_path)
        except OSError:
            return
        if self.__read_offset <= file_size:
            return
        logger.warning(
            "JournalIncrementalReader offset past journal size after rewrite "
            "(offset=%s size=%s); resyncing from seq=%s",
            self.__read_offset,
            file_size,
            self.__next_seq,
        )
        self.reset_from_seq(self.__next_seq)

    def __read_new_records(self) -> list[tuple[int, TradeTick]]:
        journal_path = self.__journal.journal_path
        if not os.path.exists(journal_path):
            return []
        self.__resync_if_journal_rewritten(journal_path)
        records: list[tuple[int, TradeTick]] = []
        with open(journal_path, "r", encoding="utf-8") as handle:
            handle.seek(self.__read_offset)
            while True:
                line = handle.readline()
                if not line:
                    break
                # Incomplete trailing write: wait for a terminating newline.
                if not line.endswith("\n"):
                    break
                parsed = self.__parse_complete_line(line)
                self.__read_offset = handle.tell()
                if parsed is None:
                    continue
                seq, tick = parsed
                if seq < self.__next_seq:
                    continue
                records.append((seq, tick))
                self.__next_seq = seq + 1
        return records


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

    def create_incremental_reader(self) -> JournalIncrementalReader:
        return JournalIncrementalReader(self)

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

    def __hydrate_dedup_buckets(self) -> dict[str, _SymbolDedupBucket]:
        buckets: dict[str, _SymbolDedupBucket] = {}
        seen_raw = self.__meta.get("seen_trade_ids", {})
        if not isinstance(seen_raw, dict):
            return buckets
        for symbol, trade_ids in seen_raw.items():
            if isinstance(trade_ids, list):
                buckets[str(symbol).upper()] = _SymbolDedupBucket.from_list(
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
        _atomic_write_json(self.__meta_path, payload)

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
        if not isinstance(start_seq, int) or start_seq < 0:
            raise ValueError("start_seq must be a non-negative integer")
        if start_seq <= 1:
            return 0
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
        _atomic_write_json(self.__cursor_path, cursor.to_dict())

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
                    _SymbolDedupBucket(self.__dedup_window),
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
                            if _should_log_invalid_line(skipped_invalid):
                                logger.warning(
                                    "TickJournal compact skipped invalid journal line "
                                    "(skipped_total=%s): %s",
                                    skipped_invalid,
                                    _preview_journal_line(stripped),
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
        if lag_seq <= 0:
            raise ValueError("lag_seq must be positive")
        cursor = self.load_cursor()
        min_retain = max(1, cursor.last_processed_seq - lag_seq)
        if cursor.last_processed_seq <= lag_seq:
            return 0
        return self.compact_before_seq(min_retain)
