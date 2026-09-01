"""
Incremental JSONL reader for the durable tick journal.

Pattern: Iterator / Cursor — polls new records without rescanning the full file.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Callable, Optional

from core.journal.journal_incomplete_fragment import (
    IncompleteTrailingFragmentPolicy,
    incomplete_trailing_skip_reason,
    is_in_progress_journal_fragment,
)
from core.journal.journal_io import preview_journal_line, should_log_invalid_line
from core.journal.journal_read_progress import compute_read_progress_snapshot
from core.journal.journal_record_line_parser import (
    JournalRecordParseError,
    parse_journal_record_line,
)
from leviathan_common.models.trade_tick import TradeTick

if TYPE_CHECKING:
    from core.journal.tick_journal import TickJournal

logger = logging.getLogger(__name__)

# D4-09: bound wait for a `{`-prefixed incomplete trailing write before skipping.
DEFAULT_INCOMPLETE_RECORD_MAX_WAIT_SECONDS = 2.0

__all__ = [
    "DEFAULT_INCOMPLETE_RECORD_MAX_WAIT_SECONDS",
    "JournalIncrementalReader",
    "is_in_progress_journal_fragment",
]


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
        self.__journal = journal
        self.__read_offset = 0
        self.__next_seq = 1
        self.__skipped_invalid_lines = 0
        self.__consecutive_parse_failures = 0
        self.__last_skip_reason: Optional[str] = None
        self.__last_known_size: Optional[int] = None
        self.__past_eof_resync_logged = False
        self.__incomplete_policy = IncompleteTrailingFragmentPolicy(
            incomplete_record_max_wait_seconds=incomplete_record_max_wait_seconds,
            clock=clock,
        )
        # After skipping a torn EOF suffix, the cursor is already at the next
        # record BOL even when the previous byte is not ``\n`` (D4-09). Tie the
        # mark to the offset so a later mid-line sticky seek (D4-03) still aligns.
        self.__logical_bol_offset: Optional[int] = 0

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
        """Return offset/size/seq lag for cold-start observability (D4-04)."""
        try:
            journal_size = os.path.getsize(self.__journal.journal_path)
        except OSError:
            journal_size = 0
        disk_latest = self.__journal.read_latest_seq_from_disk()
        return compute_read_progress_snapshot(
            read_offset=self.__read_offset,
            journal_size=journal_size,
            next_seq=self.__next_seq,
            disk_latest_seq=disk_latest,
            incomplete_stuck=self.__incomplete_policy.is_stuck,
        )

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
        self.__incomplete_policy.clear()

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
        if should_log_invalid_line(count):
            logger.warning(
                "JournalIncrementalReader skipped invalid journal line "
                "(skipped_total=%s, offset=%s, reason=%s): %s",
                count,
                self.__read_offset,
                reason,
                preview_journal_line(line),
            )

    def __should_skip_incomplete_trailing_fragment(self, fragment: str) -> bool:
        return self.__incomplete_policy.should_skip_now(
            offset=self.__read_offset,
            fragment=fragment,
        )

    def __skip_incomplete_trailing_fragment(
        self,
        fragment: str,
        *,
        reason: Optional[str] = None,
    ) -> None:
        if reason is None:
            reason = incomplete_trailing_skip_reason(fragment)
        if reason == "incomplete_trailing_cold_attach":
            logger.warning(
                "JournalIncrementalReader abandoned incomplete journal tip on attach "
                "(offset=%s, reason=%s): %s",
                self.__read_offset,
                reason,
                preview_journal_line(fragment),
            )
        self.__quarantine_invalid_line(fragment, reason=reason)
        self.__clear_incomplete_wait_state()

    def __parse_complete_line(self, line: str) -> Optional[tuple[int, TradeTick]]:
        parsed = parse_journal_record_line(line)
        if parsed is None:
            return None
        if isinstance(parsed, JournalRecordParseError):
            self.__quarantine_invalid_line(parsed.line, reason=parsed.reason)
            return None
        self.__log_recovery_if_needed()
        return parsed

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
