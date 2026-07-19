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
from typing import Callable, Iterator, Optional

from core.journal.journal_file_lock import JournalFileLock
from leviathan_common.models.trade_tick import TradeTick

logger = logging.getLogger(__name__)

DEFAULT_DEDUP_WINDOW = 10_000
SEQ_INDEX_INTERVAL = 500
META_PERSIST_INTERVAL = 50
COMPACT_MIN_LAG_SEQ = 5_000
_INVALID_LINE_PREVIEW_CHARS = 120
# D4-09: bound wait for a `{`-prefixed incomplete trailing write before skipping.
DEFAULT_INCOMPLETE_RECORD_MAX_WAIT_SECONDS = 2.0
# D4-04: emit unread lag diagnostics while cold-start / tail-follow yields nothing.
DEFAULT_EMPTY_POLL_DIAGNOSTIC_SECONDS = 5.0

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


def _is_in_progress_journal_fragment(fragment: str) -> bool:
    """
    Return True when an incomplete trailing fragment may still become a valid
    JSONL record (writer mid-append). Torn suffixes that do not start with ``{``
    are never valid journal objects and must not block the reader (D4-09).
    """
    return fragment.lstrip().startswith("{")


class JournalIncrementalReader:
    """Reads new journal records incrementally without rescanning the full file.

    Incomplete trailing lines that look like an in-progress JSON object
    (leading ``{``) are left unread until complete, up to
    ``incomplete_record_max_wait_seconds``. Torn suffixes that cannot be valid
    journal objects are quarantined immediately so spawn is not gated for tens
    of seconds waiting for the next writer newline (D4-09).

    On ``reset_from_seq`` / checkpoint cold-attach (D4-04), any incomplete tip is
    abandoned immediately so a new engine gen does not park for the full wait
    window (or until a later append merges into poison).

    Complete but invalid lines are quarantined and skipped so they cannot poison
    the tail-follow loop or respawn retries. Recovery from a skip episode is
    logged once (edge-triggered) when a valid JSON record is read again.

    After seek/restore the byte cursor is always line-aligned (BOL). Each poll
    also re-aligns before reading so a sticky offset that lands mid-object after
    a rewrite/grow (D4-03) cannot wait forever on a torn remnant. A redundant
    ``reset_from_seq`` for the same logical ``start_seq`` does not rewind into
    already-consumed byte ranges when the current offset is still inside the
    journal file (bug #5 / checkpoint continue).
    """

    def __init__(
        self,
        journal: "TickJournal",
        *,
        incomplete_record_max_wait_seconds: float = DEFAULT_INCOMPLETE_RECORD_MAX_WAIT_SECONDS,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        if incomplete_record_max_wait_seconds <= 0:
            raise ValueError("incomplete_record_max_wait_seconds must be positive")
        if clock is not None and not callable(clock):
            raise TypeError("clock must be callable")
        self.__journal = journal
        self.__read_offset = 0
        self.__next_seq = 1
        self.__skipped_invalid_lines = 0
        self.__consecutive_parse_failures = 0
        self.__last_skip_reason: Optional[str] = None
        self.__last_known_size: Optional[int] = None
        self.__past_eof_resync_logged = False
        self.__incomplete_record_max_wait_seconds = float(
            incomplete_record_max_wait_seconds
        )
        # Resolve monotonic at init so tests can monkeypatch time.monotonic.
        self.__clock: Callable[[], float] = clock if clock is not None else time.monotonic
        # After skipping a torn EOF suffix, the cursor is already at the next
        # record BOL even when the previous byte is not ``\n`` (D4-09). Tie the
        # mark to the offset so a later mid-line sticky seek (D4-03) still aligns.
        self.__logical_bol_offset: Optional[int] = 0
        self.__pending_incomplete_offset: Optional[int] = None
        self.__pending_incomplete_started_at: Optional[float] = None
        self.__pending_incomplete_length: Optional[int] = None

    def reset_from_seq(self, start_seq: int) -> None:
        if not isinstance(start_seq, int) or start_seq < 0:
            raise ValueError("start_seq must be a non-negative integer")
        indexed_offset = self.__journal.byte_offset_for_seq(start_seq)
        self.__read_offset = self.__choose_reset_offset(
            start_seq=start_seq,
            indexed_offset=indexed_offset,
        )
        self.__align_read_offset_to_line_boundary()
        # D4-04: cold-start attach must not park on a torn tip. Abandon marks
        # __logical_bol_offset so the next align does not readline-skip the
        # first complete record after a non-``\n`` predecessor.
        self.__abandon_incomplete_tip_at_cursor(force=True)
        self.__logical_bol_offset = self.__read_offset
        self.__next_seq = start_seq
        # Seek starts a new read position; do not emit a false recovery signal.
        self.__consecutive_parse_failures = 0
        self.__last_skip_reason = None

    def force_rebind_from_seq(self, start_seq: int) -> None:
        """
        Force a byte-cursor rebind from the sparse index (D7 proactive recovery).

        Unlike ``reset_from_seq``, ignores the sticky high-water mark so a
        mid-line / incomplete tip that parks empty polls can be abandoned and
        re-aligned even when ``start_seq`` equals the current ``__next_seq``.
        """
        if not isinstance(start_seq, int) or start_seq < 0:
            raise ValueError("start_seq must be a non-negative integer")
        indexed_offset = self.__journal.byte_offset_for_seq(start_seq)
        try:
            file_size = os.path.getsize(self.__journal.journal_path)
        except OSError:
            file_size = None
        if file_size is not None and indexed_offset > file_size:
            indexed_offset = file_size
        self.__read_offset = indexed_offset
        self.__clear_incomplete_wait_state()
        self.__logical_bol_offset = None
        self.__align_read_offset_to_line_boundary()
        self.__abandon_incomplete_tip_at_cursor(force=True)
        self.__logical_bol_offset = self.__read_offset
        self.__next_seq = start_seq
        self.__consecutive_parse_failures = 0
        self.__last_skip_reason = None
        self.__past_eof_resync_logged = False

    def get_read_offset(self) -> int:
        """Return the current byte cursor used for incremental reads."""
        return self.__read_offset

    def get_invalid_line_skip_count(self) -> int:
        """Return lifetime count of complete lines quarantined/skipped as invalid."""
        return self.__skipped_invalid_lines

    def get_consecutive_parse_failures(self) -> int:
        """Return current streak of skipped invalid lines since the last valid record."""
        return self.__consecutive_parse_failures

    def get_read_progress_snapshot(self) -> dict:
        """
        Return offset/size/seq lag for cold-start observability (D4-04).

        ``latest_seq`` is ``max(disk meta, reader-observed floor)`` so a stale
        meta watermark (``META_PERSIST_INTERVAL``) cannot report
        ``next_seq >> latest_seq`` after the reader has already consumed those
        records from the journal file.

        ``lag_seq`` is how many journal seqs are at or beyond ``next_seq``
        according to that effective tip (0 when caught up). When the byte
        cursor is behind EOF (or an incomplete tip is pending) but meta still
        looks caught up, ``lag_seq`` is at least 1 so callers do not treat
        unread bytes as idle EOF.
        """
        try:
            journal_size = os.path.getsize(self.__journal.journal_path)
        except OSError:
            journal_size = 0
        disk_latest = self.__journal.read_latest_seq_from_disk()
        # Records already consumed imply tip >= next_seq - 1 even if meta lags.
        observed_floor = max(0, self.__next_seq - 1)
        latest_seq = max(int(disk_latest), observed_floor)
        if latest_seq >= self.__next_seq:
            lag_seq = latest_seq - self.__next_seq + 1
        else:
            lag_seq = 0
        incomplete_stuck = self.__pending_incomplete_offset is not None
        byte_unread = self.__read_offset < journal_size
        if lag_seq == 0 and (byte_unread or incomplete_stuck):
            lag_seq = 1
        return {
            "read_offset": self.__read_offset,
            "journal_size": journal_size,
            "next_seq": self.__next_seq,
            "latest_seq": latest_seq,
            "lag_seq": lag_seq,
            "incomplete_stuck": incomplete_stuck,
        }

    def poll(self, start_seq: int) -> list[tuple[int, TradeTick]]:
        if start_seq != self.__next_seq:
            self.reset_from_seq(start_seq)
        return self.__read_new_records()

    def __choose_reset_offset(self, *, start_seq: int, indexed_offset: int) -> int:
        """
        Pick the byte cursor for ``reset_from_seq``.

        When the logical next seq is unchanged and the live cursor already sits
        past the sparse index hint (and still inside the file), keep the
        high-water mark so checkpoint continue cannot re-enter poison that was
        already scanned. Past-EOF / shrunk-file cursors fall back to the index
        and are never left past EOF (D4-01).
        """
        journal_path = self.__journal.journal_path
        try:
            file_size = os.path.getsize(journal_path)
        except OSError:
            file_size = None

        safe_indexed = indexed_offset
        if file_size is not None and indexed_offset > file_size:
            # byte_offset_for_seq should already clamp; belt-and-suspenders.
            safe_indexed = file_size

        if start_seq != self.__next_seq:
            return safe_indexed
        if self.__read_offset <= safe_indexed:
            return safe_indexed
        if file_size is None or self.__read_offset > file_size:
            return safe_indexed
        return self.__read_offset

    def __align_read_offset_to_line_boundary(self) -> None:
        """
        Advance ``__read_offset`` to the next BOL when the cursor is mid-line.

        After rewrite/grow a sticky offset may land inside an object (including
        on a nested ``{``). Advancing past the torn remnant — even when it runs
        to EOF without a newline — prevents a false in-progress wait (D4-03).

        Offsets marked in ``__logical_bol_offset`` (after skipping an incomplete
        trailing poison) are trusted as BOL so a subsequent append is not
        discarded (complements D4-09). A different sticky mid-line offset still
        aligns.
        """
        if self.__read_offset <= 0:
            self.__logical_bol_offset = 0
            return
        if self.__logical_bol_offset == self.__read_offset:
            return
        journal_path = self.__journal.journal_path
        if not os.path.exists(journal_path):
            return
        before = self.__read_offset
        try:
            with open(journal_path, "rb") as handle:
                handle.seek(0, os.SEEK_END)
                file_size = handle.tell()
                if self.__read_offset >= file_size:
                    self.__read_offset = file_size
                else:
                    handle.seek(self.__read_offset - 1)
                    previous_byte = handle.read(1)
                    if previous_byte != b"\n":
                        handle.seek(self.__read_offset)
                        handle.readline()
                        self.__read_offset = handle.tell()
        except OSError:
            return
        if self.__read_offset != before:
            self.__clear_incomplete_wait_state()
            self.__logical_bol_offset = self.__read_offset

    def __mark_logical_bol(self) -> None:
        self.__logical_bol_offset = self.__read_offset

    def __clear_incomplete_wait_state(self) -> None:
        self.__pending_incomplete_offset = None
        self.__pending_incomplete_started_at = None
        self.__pending_incomplete_length = None

    def __abandon_incomplete_tip_at_cursor(self, *, force: bool) -> None:
        """
        If the byte cursor sits on an incomplete trailing fragment, skip it.

        ``force=True`` (cold-start ``reset_from_seq``) abandons immediately —
        including `{`-prefixed tips — so attach never parks for the live wait
        window (D4-04). ``force=False`` uses the same skip policy as poll.
        """
        journal_path = self.__journal.journal_path
        if not os.path.exists(journal_path):
            self.__clear_incomplete_wait_state()
            return
        try:
            with open(journal_path, "r", encoding="utf-8") as handle:
                handle.seek(self.__read_offset)
                line_start = handle.tell()
                line = handle.readline()
                if not line or line.endswith("\n"):
                    self.__clear_incomplete_wait_state()
                    return
                if force:
                    self.__read_offset = line_start
                    self.__skip_incomplete_trailing_fragment(
                        line,
                        reason="incomplete_trailing_cold_attach",
                    )
                    self.__read_offset = handle.tell()
                    self.__mark_logical_bol()
                    return
                if self.__should_skip_incomplete_trailing_fragment(line):
                    self.__read_offset = line_start
                    self.__skip_incomplete_trailing_fragment(line)
                    self.__read_offset = handle.tell()
                    self.__mark_logical_bol()
        except OSError:
            return

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

    def __should_skip_incomplete_trailing_fragment(self, fragment: str) -> bool:
        """
        Decide whether an EOF fragment without newline must be skipped now.

        Postconditions:
            Returns True for torn non-JSON suffixes, or for `{`-prefixed
            fragments that have remained incomplete past the configured wait.
        """
        if not fragment:
            return False
        if not _is_in_progress_journal_fragment(fragment):
            return True
        now = self.__clock()
        fragment_len = len(fragment)
        if (
            self.__pending_incomplete_offset != self.__read_offset
            or self.__pending_incomplete_length != fragment_len
            or self.__pending_incomplete_started_at is None
        ):
            self.__pending_incomplete_offset = self.__read_offset
            self.__pending_incomplete_started_at = now
            self.__pending_incomplete_length = fragment_len
            return False
        elapsed = now - self.__pending_incomplete_started_at
        return elapsed >= self.__incomplete_record_max_wait_seconds

    def __skip_incomplete_trailing_fragment(
        self,
        fragment: str,
        *,
        reason: Optional[str] = None,
    ) -> None:
        if reason is None:
            if _is_in_progress_journal_fragment(fragment):
                reason = "incomplete_trailing_stale"
            else:
                reason = "incomplete_trailing_poison"
        if reason == "incomplete_trailing_cold_attach":
            logger.warning(
                "JournalIncrementalReader abandoned incomplete journal tip on attach "
                "(offset=%s, reason=%s): %s",
                self.__read_offset,
                reason,
                _preview_journal_line(fragment),
            )
        self.__quarantine_invalid_line(fragment, reason=reason)
        self.__clear_incomplete_wait_state()

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
        starves forever (NEW-01 / D4-01). Recovery must advance or clamp the
        cursor (never leave offset stuck past EOF) and must not WARN-flood on
        every empty poll. A shrink that leaves the sticky offset numerically
        in-range can still land mid-line (D4-03) — clear incomplete-wait state
        so the subsequent BOL align can advance.
        """
        try:
            file_size = os.path.getsize(journal_path)
        except OSError:
            return
        previous_size = self.__last_known_size
        shrunk = previous_size is not None and file_size < previous_size
        if self.__read_offset > file_size:
            previous_offset = self.__read_offset
            # Another process may have rebuilt seq_index on disk after compact.
            self.__journal.reload_seq_index_from_disk()
            self.reset_from_seq(self.__next_seq)
            if self.__read_offset > file_size:
                self.__read_offset = file_size
                self.__clear_incomplete_wait_state()
            if not self.__past_eof_resync_logged:
                logger.warning(
                    "JournalIncrementalReader offset past journal size after rewrite "
                    "(offset=%s size=%s); resyncing from seq=%s",
                    previous_offset,
                    file_size,
                    self.__next_seq,
                )
                self.__past_eof_resync_logged = True
        elif shrunk:
            logger.warning(
                "JournalIncrementalReader journal shrunk under sticky offset "
                "(offset=%s size=%s previous_size=%s); aligning to line boundary",
                self.__read_offset,
                file_size,
                previous_size,
            )
            self.__clear_incomplete_wait_state()
            self.__align_read_offset_to_line_boundary()
            self.__past_eof_resync_logged = False
        else:
            self.__past_eof_resync_logged = False
        self.__last_known_size = file_size

    def __read_new_records(self) -> list[tuple[int, TradeTick]]:
        journal_path = self.__journal.journal_path
        if not os.path.exists(journal_path):
            return []
        self.__resync_if_journal_rewritten(journal_path)
        # D4-03: sticky offset after rewrite/grow may sit mid-object (even on a
        # nested '{'); align before parsing so torn remnants never stall.
        self.__align_read_offset_to_line_boundary()
        records: list[tuple[int, TradeTick]] = []
        with open(journal_path, "r", encoding="utf-8") as handle:
            handle.seek(self.__read_offset)
            while True:
                line_start = handle.tell()
                line = handle.readline()
                if not line:
                    break
                # Incomplete trailing write: skip torn poison immediately; wait
                # briefly for a `{`-prefixed in-progress append (D4-09).
                if not line.endswith("\n"):
                    if self.__should_skip_incomplete_trailing_fragment(line):
                        self.__read_offset = line_start
                        self.__skip_incomplete_trailing_fragment(line)
                        self.__read_offset = handle.tell()
                        self.__mark_logical_bol()
                        continue
                    break
                self.__clear_incomplete_wait_state()
                parsed = self.__parse_complete_line(line)
                self.__read_offset = handle.tell()
                self.__mark_logical_bol()
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
