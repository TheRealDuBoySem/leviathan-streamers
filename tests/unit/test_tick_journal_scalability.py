"""Scalability / integration scenarios for TickJournal compaction + readers."""

import asyncio

import pytest

from core.journal.journal_tick_stream import JournalTickStream
from core.journal.tick_journal import (
    COMPACT_MIN_LAG_SEQ,
    DEFAULT_DEDUP_WINDOW,
    TickJournal,
)
from core.journal.tick_journal_cursor import TickJournalCursor
from leviathan_common.models.trade_tick import TradeTick


def _tick(trade_id: str, ts: int = 1000) -> TradeTick:
    return TradeTick("BTCUSDT", ts, 100.0, 1.0, "buy", trade_id)


def test_dedup_window_is_bounded(tmp_path):
    journal = TickJournal(str(tmp_path), dedup_window=3)
    for index in range(5):
        journal.append(_tick(f"t{index}"))
    journal.flush_meta()
    with open(journal.journal_path, "r", encoding="utf-8") as handle:
        assert len(handle.readlines()) == 5
    seq = journal.append(_tick("t0"))
    assert seq == 6


def test_incremental_reader_recovers_after_compact_rewrites_file(tmp_path):
    """
    NEW-01: compact rewrites tick_journal.jsonl via os.replace.

    An active incremental reader's byte offset must not starve forever after the
    rewrite — polls must resume from the logical next seq.
    """
    journal = TickJournal(str(tmp_path))
    total = COMPACT_MIN_LAG_SEQ + 100
    for index in range(total):
        journal.append(_tick(f"t{index}", ts=1000 + index))
    cursor_seq = total - 10
    journal.save_cursor(TickJournalCursor(last_processed_seq=cursor_seq))

    reader = journal.create_incremental_reader()
    next_seq = cursor_seq + 1
    assert len(reader.poll(next_seq)) == 10
    next_seq = total + 1

    for index in range(total, total + 5):
        journal.append(_tick(f"t{index}", ts=1000 + index))
    assert len(reader.poll(next_seq)) == 5
    next_seq = total + 6

    removed = journal.maybe_compact()
    assert removed > 0

    for index in range(total + 5, total + 10):
        journal.append(_tick(f"t{index}", ts=1000 + index))

    resumed = reader.poll(next_seq)
    assert len(resumed) == 5
    assert [seq for seq, _ in resumed] == list(range(total + 6, total + 11))
    assert resumed[0][1].trade_id == f"t{total + 5}"
    assert resumed[-1][1].trade_id == f"t{total + 9}"


@pytest.mark.asyncio
async def test_journal_tick_stream_survives_live_maybe_compact(tmp_path):
    """Live JournalTickStream must keep yielding ticks after maybe_compact."""
    journal = TickJournal(str(tmp_path), dedup_window=DEFAULT_DEDUP_WINDOW)
    total = COMPACT_MIN_LAG_SEQ + 50
    for index in range(total):
        journal.append(_tick(f"t{index}", ts=1000 + index))
    journal.save_cursor(TickJournalCursor(last_processed_seq=total))

    stream = JournalTickStream(journal, poll_interval_seconds=0.01)
    stream_task = asyncio.create_task(stream.start_streaming())
    try:
        # Prime the reader at the live tip so its byte offset is near EOF of the
        # pre-compact file — the NEW-01 failure mode without resync.
        journal.append(_tick(f"t{total}", ts=1000 + total))
        primed = await asyncio.wait_for(stream.wait_for_next_tick(), timeout=2.0)
        assert primed.trade_id == f"t{total}"
        stream.mark_tick_as_processed()

        removed = journal.maybe_compact()
        assert removed > 0

        journal.append(_tick(f"t{total + 1}", ts=1000 + total + 1))
        resumed = await asyncio.wait_for(stream.wait_for_next_tick(), timeout=2.0)
        assert resumed.trade_id == f"t{total + 1}"
        stream.mark_tick_as_processed()
    finally:
        await stream.stop()
        stream_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stream_task
