"""
Durable meta-tip publication policy (interval persist + idle flush).

Pattern: Strategy — decides when in-memory tip becomes durable on disk.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Optional

from core.journal.journal_file_lock import JournalFileLock
from core.journal.tick_journal_meta import TickJournalMetaStore

logger = logging.getLogger(__name__)

META_PERSIST_INTERVAL = 50
# J33 H22 / F-J33-03: after a partial burst (< META_PERSIST_INTERVAL appends),
# publish durable tip once the writer goes quiet so disk tip cannot freeze
# mid-stream while journal body / engine cursor already advanced (H22 Δ+23
# disk=2518909 cursor=2518932 → false disk_tip_frozen_famine). Keep well below
# soft-stale (~30s) and tick_stall (~120s). 0 disables idle flush (tests).
META_IDLE_FLUSH_SECONDS = 2.0


class TickJournalMetaDurability:
    """
    Publish meta tip every N appends, or after a quiet gap when dirty.

    Invariants:
        - flush() rewrites disk meta only when this instance dirtied tip
          (J34: engine reader must not clobber collector tip).
        - persist_locked() may run only while the caller holds the journal
          file lock and the shared thread lock.
    """

    def __init__(
        self,
        *,
        meta_store: TickJournalMetaStore,
        lock_path: str,
        thread_lock: Any,
        persist_interval: int = META_PERSIST_INTERVAL,
        idle_flush_seconds: float = META_IDLE_FLUSH_SECONDS,
    ) -> None:
        if not isinstance(meta_store, TickJournalMetaStore):
            raise TypeError("meta_store must be a TickJournalMetaStore instance")
        if not isinstance(lock_path, str) or not lock_path.strip():
            raise ValueError("lock_path must be a non-empty string")
        if thread_lock is None or not callable(getattr(thread_lock, "acquire", None)):
            raise TypeError("thread_lock must be a threading.Lock-like object")
        if isinstance(persist_interval, bool) or not isinstance(persist_interval, int):
            raise TypeError("persist_interval must be an integer")
        if persist_interval <= 0:
            raise ValueError("persist_interval must be positive")
        if isinstance(idle_flush_seconds, bool) or not isinstance(
            idle_flush_seconds, (int, float)
        ):
            raise TypeError("idle_flush_seconds must be a number")
        if float(idle_flush_seconds) < 0:
            raise ValueError("idle_flush_seconds must be >= 0")
        self.__meta_store = meta_store
        self.__lock_path = lock_path.strip()
        self.__thread_lock = thread_lock
        self.__persist_interval = persist_interval
        self.__idle_flush_seconds = float(idle_flush_seconds)
        self.__append_counter = 0
        self.__meta_dirty = False
        self.__idle_flush_timer: Optional[threading.Timer] = None

    def has_unpersisted_meta(self) -> bool:
        """Return True when in-memory tip advances are not yet durable on disk."""
        with self.__thread_lock:
            return bool(self.__meta_dirty)

    def note_append_locked(self) -> None:
        """
        Account for one journal append and persist or mark dirty.

        Caller must hold the journal file lock and the shared thread lock.
        """
        self.__append_counter += 1
        if self.__append_counter % self.__persist_interval == 0:
            self.persist_locked()
        else:
            self.__mark_dirty_locked()

    def persist_locked(self) -> None:
        """Persist meta and clear dirty / idle-flush timer (caller holds locks)."""
        self.__cancel_idle_flush_timer_locked()
        self.__meta_store.persist()
        self.__meta_dirty = False

    def flush(self) -> bool:
        """
        Persist meta when this writer has unpersisted tip advances.

        J34 / F-J34-01: a clean (non-writer) durability — e.g. the engine's
        checkpoint-attached TickJournal — must not rewrite durable meta.
        Doing so clobbers the collector tip with a stale in-memory snapshot
        (H17 ahead=369 / H20 ahead=508) while journal body stays ahead.

        Returns:
            True when a dirty tip was flushed, else False.
        """
        with self.__thread_lock:
            dirty = bool(self.__meta_dirty)
        if not dirty:
            return False
        with JournalFileLock(self.__lock_path):
            with self.__thread_lock:
                if not self.__meta_dirty:
                    return False
                self.persist_locked()
        return True

    def __mark_dirty_locked(self) -> None:
        """Mark tip dirty and (re)arm idle flush so quiet gaps publish tip."""
        self.__meta_dirty = True
        if self.__idle_flush_seconds <= 0:
            return
        self.__cancel_idle_flush_timer_locked()
        timer = threading.Timer(
            self.__idle_flush_seconds,
            self.__idle_flush_callback,
        )
        timer.daemon = True
        self.__idle_flush_timer = timer
        timer.start()

    def __cancel_idle_flush_timer_locked(self) -> None:
        timer = self.__idle_flush_timer
        self.__idle_flush_timer = None
        if timer is not None:
            timer.cancel()

    def __idle_flush_callback(self) -> None:
        """Timer target: publish durable tip after a quiet gap (J33 H22)."""
        try:
            self.flush()
        except Exception as exc:  # pragma: no cover - defensive I/O path
            logger.warning(
                "TickJournalMetaDurability: idle meta tip flush failed "
                "(will retry on next append): %s",
                exc,
            )
