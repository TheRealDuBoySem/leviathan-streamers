"""
Sparse seq→byte-offset index and journal seek resolution.

Pattern: Index — maintains sparse checkpoints and resolves BOL offsets for a seq.
"""

from __future__ import annotations

import json
import os
import threading

from core.journal.tick_journal_meta import TickJournalMetaStore

SEQ_INDEX_INTERVAL = 500
_SEQ_INDEX_MAX_ENTRIES = 256


class TickJournalSeqIndex:
    """
    Sparse sequence index backed by TickJournalMetaStore.

    Responsibility: record checkpoints and resolve the byte offset where an
    incremental read for ``start_seq`` should begin.
    """

    def __init__(
        self,
        *,
        journal_path: str,
        meta_store: TickJournalMetaStore,
        seq_index_interval: int = SEQ_INDEX_INTERVAL,
        thread_lock: threading.Lock | None = None,
    ) -> None:
        if not isinstance(journal_path, str) or not journal_path.strip():
            raise ValueError("journal_path must be a non-empty string")
        if not isinstance(meta_store, TickJournalMetaStore):
            raise TypeError("meta_store must be a TickJournalMetaStore instance")
        if seq_index_interval <= 0:
            raise ValueError("seq_index_interval must be positive")
        self.__journal_path = journal_path.strip()
        self.__meta_store = meta_store
        self.__seq_index_interval = seq_index_interval
        self.__thread_lock = thread_lock

    @property
    def seq_index_interval(self) -> int:
        return self.__seq_index_interval

    def record(self, seq: int, byte_offset: int) -> None:
        if not isinstance(seq, int) or seq < 0:
            raise ValueError("seq must be a non-negative integer")
        if not isinstance(byte_offset, int) or byte_offset < 0:
            raise ValueError("byte_offset must be a non-negative integer")
        index = self.__meta_store.seq_index()
        if not index:
            index.append([0, 0])
        last_seq = int(index[-1][0])
        if seq == 0 or (seq % self.__seq_index_interval == 0 and seq > last_seq):
            index.append([seq, byte_offset])
        if len(index) > _SEQ_INDEX_MAX_ENTRIES:
            self.__meta_store.replace_seq_index(index[-_SEQ_INDEX_MAX_ENTRIES:])

    def indexed_hint(self, start_seq: int) -> int:
        index = self.__meta_store.seq_index()
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

    def byte_offset_for_seq(self, start_seq: int) -> int:
        """
        Return the BOL byte offset where incremental reads should begin for
        ``start_seq``.

        Uses the sparse seq_index as a hint, then walks forward to the first
        complete line whose seq is >= start_seq. Invalid lines are skipped
        silently during resolution (no quarantine) so restore/reset cannot
        re-enter already-passed poison just to locate the tip.
        """
        if not isinstance(start_seq, int) or start_seq < 0:
            raise ValueError("start_seq must be a non-negative integer")
        if start_seq <= 1:
            return 0
        return self.resolve(start_seq, self.indexed_hint(start_seq))

    def resolve(self, start_seq: int, indexed_offset: int) -> int:
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

    def reload_from_disk(self) -> None:
        if self.__thread_lock is None:
            self.__meta_store.reload_seq_index_from_disk()
            return
        with self.__thread_lock:
            self.__meta_store.reload_seq_index_from_disk()

    def replace_and_trim(self, index: list) -> None:
        """Replace the index (e.g. after compaction) keeping at most max entries."""
        if not isinstance(index, list):
            raise TypeError("index must be a list")
        self.__meta_store.replace_seq_index(index[-_SEQ_INDEX_MAX_ENTRIES:])
