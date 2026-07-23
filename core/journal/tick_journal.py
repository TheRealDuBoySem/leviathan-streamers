"""
Append-only durable tick journal for beta supervised restarts.

Pattern: Facade / Repository — coordinates meta, seq index, compaction, and appends.
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Iterator

from core.journal.journal_file_lock import JournalFileLock
from core.journal.journal_incremental_reader import (
    DEFAULT_INCOMPLETE_RECORD_MAX_WAIT_SECONDS,
    JournalIncrementalReader,
)
from core.journal.journal_io import atomic_write_json
from core.journal.journal_quarantine import append_quarantine_line
from core.journal.tick_journal_codec import tick_from_dict, tick_to_dict
from core.journal.tick_journal_compactor import COMPACT_MIN_LAG_SEQ, TickJournalCompactor
from core.journal.tick_journal_cursor import TickJournalCursor
from core.journal.tick_journal_meta import TickJournalMetaStore
from core.journal.tick_journal_seq_index import SEQ_INDEX_INTERVAL, TickJournalSeqIndex
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

DEFAULT_DEDUP_WINDOW = 10_000
META_PERSIST_INTERVAL = 50
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
        self.__thread_lock = threading.Lock()
        self.__append_counter = 0
        os.makedirs(normalized_dir, exist_ok=True)
        self.__meta_store = TickJournalMetaStore(
            self.__meta_path,
            dedup_window=dedup_window,
        )
        self.__seq_index = TickJournalSeqIndex(
            journal_path=self.__journal_path,
            meta_store=self.__meta_store,
            seq_index_interval=seq_index_interval,
            thread_lock=self.__thread_lock,
        )
        self.__compactor = TickJournalCompactor(
            journal_path=self.__journal_path,
            lock_path=self.__lock_path,
            quarantine_path=self.__quarantine_path,
            meta_store=self.__meta_store,
            seq_index=self.__seq_index,
            thread_lock=self.__thread_lock,
        )

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
        append_quarantine_line(self.__quarantine_path, line, reason=reason)

    def byte_offset_for_seq(self, start_seq: int) -> int:
        return self.__seq_index.byte_offset_for_seq(start_seq)

    def reload_seq_index_from_disk(self) -> None:
        """
        Reload sparse ``seq_index`` from persisted meta (D4-01).

        Compaction in another process rewrites byte offsets; a live reader's
        in-memory index must not keep pre-rewrite hints that point past EOF.
        """
        self.__seq_index.reload_from_disk()

    def reload_meta_from_disk(self) -> None:
        """Reload latest_seq, seq_index, and dedup buckets from persisted meta."""
        with self.__thread_lock:
            self.__meta_store.reload_from_disk()

    def latest_seq(self) -> int:
        with self.__thread_lock:
            return self.__meta_store.latest_seq()

    def read_latest_seq_from_disk(self) -> int:
        """
        Return latest_seq from persisted meta without mutating in-memory state.

        Used by the supervisor during collector handoff when another process
        appends ticks to the journal.
        """
        return self.__meta_store.read_latest_seq_from_disk()

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
                bucket = self.__meta_store.get_or_create_bucket(symbol)
                dedup_key = tick.trade_id
                if bucket.contains(dedup_key):
                    return self.__meta_store.latest_seq()
                next_seq = self.__meta_store.latest_seq() + 1
                record = {"seq": next_seq, "tick": tick_to_dict(tick)}
                encoded = json.dumps(record, separators=(",", ":")) + "\n"
                with open(self.__journal_path, "a", encoding="utf-8") as handle:
                    byte_offset = handle.tell()
                    handle.write(encoded)
                    handle.flush()
                    os.fsync(handle.fileno())
                bucket.add(dedup_key)
                self.__meta_store.set_latest_seq(next_seq)
                self.__seq_index.record(next_seq, byte_offset)
                self.__append_counter += 1
                if self.__append_counter % META_PERSIST_INTERVAL == 0:
                    self.__meta_store.persist()
                return next_seq

    def flush_meta(self) -> None:
        with JournalFileLock(self.__lock_path):
            with self.__thread_lock:
                self.__meta_store.persist()

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
        return self.__compactor.compact_before_seq(min_retain_seq)

    def maybe_compact(self, *, lag_seq: int = COMPACT_MIN_LAG_SEQ) -> int:
        """
        Drop records older than ``cursor.last_processed_seq - lag_seq``.

        Must not be invoked from the engine stop/shutdown path (D4-10): a
        rewrite immediately before the next process starts can leave readers
        with byte offsets past the new file size (feeds D4-01). Prefer an
        offline or collector-owned compaction window instead.
        """
        cursor = self.load_cursor()
        return self.__compactor.maybe_compact(
            last_processed_seq=cursor.last_processed_seq,
            lag_seq=lag_seq,
        )
