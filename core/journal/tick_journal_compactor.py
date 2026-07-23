"""
Append-only journal compaction (prefix drop + seq_index rebuild).

Pattern: Command — rewrites the JSONL journal retaining seq >= min_retain_seq.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from core.journal.journal_file_lock import JournalFileLock
from core.journal.journal_io import preview_journal_line, should_log_invalid_line
from core.journal.journal_quarantine import append_quarantine_line
from core.journal.tick_journal_meta import TickJournalMetaStore
from core.journal.tick_journal_seq_index import TickJournalSeqIndex

logger = logging.getLogger(__name__)

COMPACT_MIN_LAG_SEQ = 5_000


class TickJournalCompactor:
    """
    Rewrites tick_journal.jsonl, dropping records older than a retain watermark.

    Must not be invoked from the engine stop/shutdown path (D4-10): a rewrite
    immediately before the next process starts can leave readers with byte
    offsets past the new file size (feeds D4-01). Prefer an offline or
    collector-owned compaction window instead.
    """

    def __init__(
        self,
        *,
        journal_path: str,
        lock_path: str,
        quarantine_path: str,
        meta_store: TickJournalMetaStore,
        seq_index: TickJournalSeqIndex,
        thread_lock: Any,
    ) -> None:
        if not isinstance(journal_path, str) or not journal_path.strip():
            raise ValueError("journal_path must be a non-empty string")
        if not isinstance(lock_path, str) or not lock_path.strip():
            raise ValueError("lock_path must be a non-empty string")
        if not isinstance(quarantine_path, str) or not quarantine_path.strip():
            raise ValueError("quarantine_path must be a non-empty string")
        if not isinstance(meta_store, TickJournalMetaStore):
            raise TypeError("meta_store must be a TickJournalMetaStore instance")
        if not isinstance(seq_index, TickJournalSeqIndex):
            raise TypeError("seq_index must be a TickJournalSeqIndex instance")
        if thread_lock is None or not callable(getattr(thread_lock, "acquire", None)):
            raise TypeError("thread_lock must be a threading.Lock-like object")
        self.__journal_path = journal_path.strip()
        self.__lock_path = lock_path.strip()
        self.__quarantine_path = quarantine_path.strip()
        self.__meta_store = meta_store
        self.__seq_index = seq_index
        self.__thread_lock = thread_lock

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
                            append_quarantine_line(
                                self.__quarantine_path,
                                stripped,
                                reason=f"compact_skip: {exc}",
                            )
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
                interval = self.__seq_index.seq_index_interval
                with open(temp_path, "w", encoding="utf-8") as handle:
                    byte_offset = 0
                    new_index = [[0, 0]]
                    for index, line in enumerate(retained):
                        record = json.loads(line)
                        seq = int(record["seq"])
                        if index == 0 or seq % interval == 0:
                            new_index.append([seq, byte_offset])
                        handle.write(line + "\n")
                        byte_offset = handle.tell()
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temp_path, self.__journal_path)
                self.__seq_index.replace_and_trim(new_index)
                self.__meta_store.persist()
                return removed

    def maybe_compact(
        self,
        *,
        last_processed_seq: int,
        lag_seq: int = COMPACT_MIN_LAG_SEQ,
    ) -> int:
        """
        Drop records older than ``last_processed_seq - lag_seq``.

        Preconditions:
            - lag_seq must be positive.
            - last_processed_seq must be a non-negative integer.
        """
        if lag_seq <= 0:
            raise ValueError("lag_seq must be positive")
        if not isinstance(last_processed_seq, int) or last_processed_seq < 0:
            raise ValueError("last_processed_seq must be a non-negative integer")
        min_retain = max(1, last_processed_seq - lag_seq)
        if last_processed_seq <= lag_seq:
            return 0
        return self.compact_before_seq(min_retain)
