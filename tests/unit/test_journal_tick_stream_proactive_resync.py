"""
D7: proactive journal tail resync when unread lag is detected.

Prevents soft-stale → idle → CRITICAL stall when the tip advances but the
incremental reader is stuck (sticky incomplete tip / mid-line offset).
"""

from __future__ import annotations

import json
import logging
import os

import pytest

from core.journal.journal_incremental_reader import JournalIncrementalReader
from core.journal.journal_tick_stream import JournalTickStream
from core.journal.tick_journal import TickJournal
from core.journal.tick_journal_codec import tick_to_dict
from core.journal.tick_journal_cursor import TickJournalCursor
from leviathan_common.models.trade_tick import TradeTick


def _tick(trade_id: str, ts: int = 1000) -> TradeTick:
    return TradeTick("BTCUSDT", ts, 100.0, 1.0, "buy", trade_id)


def _record_line(seq: int, trade_id: str, ts: int = 1000) -> str:
    return json.dumps(
        {"seq": seq, "tick": tick_to_dict(_tick(trade_id, ts=ts))},
        separators=(",", ":"),
    )


def test_force_rebind_from_seq_ignores_sticky_high_water(tmp_path):
    """D7: force_rebind must re-seek from index, not keep a sticky mid-file offset."""
    journal = TickJournal(str(tmp_path), seq_index_interval=100)
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        for seq in range(1, 4):
            handle.write(_record_line(seq, f"t{seq}") + "\n")
    reader = JournalIncrementalReader(journal)
    assert len(reader.poll(1)) == 3
    sticky = reader.get_read_offset()
    # Corrupt sticky offset into the middle of the last line (simulates D4-03 land).
    reader._JournalIncrementalReader__read_offset = sticky - 5
    reader._JournalIncrementalReader__logical_bol_offset = None
    reader._JournalIncrementalReader__pending_incomplete_offset = sticky - 5
    reader._JournalIncrementalReader__pending_incomplete_started_at = 1.0
    reader._JournalIncrementalReader__pending_incomplete_length = 10

    reader.force_rebind_from_seq(4)

    assert reader.get_read_offset() == sticky
    snap = reader.get_read_progress_snapshot()
    assert snap["incomplete_stuck"] is False
    assert snap["next_seq"] == 4


def test_force_tail_resync_if_unread_returns_false_when_eof_caught_up(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("t1"))
    journal.flush_meta()
    stream = JournalTickStream(journal, poll_interval_seconds=0.01)
    stream.set_cursor(TickJournalCursor(last_processed_seq=1))
    assert stream.force_tail_resync_if_unread() is False


def test_force_tail_resync_if_unread_clears_incomplete_tip_and_reads_next(
    tmp_path, caplog
):
    """
    Sticky incomplete tip must be abandoned by proactive resync so a subsequent
    complete append is delivered (D7 / complements D4-04).
    """
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("kept"))
    journal.flush_meta()

    stream = JournalTickStream(
        journal,
        poll_interval_seconds=0.01,
        incomplete_record_max_wait_seconds=60.0,
    )
    stream.set_cursor(TickJournalCursor(last_processed_seq=1))

    # Append a `{`-prefixed incomplete tip after attach (live writer tear).
    with open(journal.journal_path, "a", encoding="utf-8") as handle:
        handle.write(_record_line(2, "torn")[:40])

    reader = stream._JournalTickStream__incremental_reader
    assert reader.poll(2) == []
    assert reader.get_read_progress_snapshot()["incomplete_stuck"] is True

    with caplog.at_level(logging.WARNING):
        assert stream.force_tail_resync_if_unread() is True
    assert any("forced tail resync" in r.message.lower() for r in caplog.records)
    assert reader.get_read_progress_snapshot()["incomplete_stuck"] is False

    with open(journal.journal_path, "a", encoding="utf-8") as handle:
        # Finish a fresh complete record after the abandoned tear (new line).
        handle.write("\n" + _record_line(2, "fresh", ts=2000) + "\n")
    journal._TickJournal__meta["latest_seq"] = 2
    journal.flush_meta()

    records = reader.poll(2)
    assert len(records) == 1
    assert records[0][0] == 2
    assert records[0][1].trade_id == "fresh"


def test_set_cursor_drains_pending_queue(tmp_path):
    """Catch-up set_cursor must not leave stale buffered ticks for the consumer."""
    journal = TickJournal(str(tmp_path))
    for i in range(3):
        journal.append(_tick(f"t{i}", ts=1000 + i))
    journal.flush_meta()
    stream = JournalTickStream(journal, poll_interval_seconds=0.01)
    stream._JournalTickStream__queue.put_nowait((1, _tick("stale1")))
    stream._JournalTickStream__queue.put_nowait((2, _tick("stale2")))
    stream._JournalTickStream__pending_seq = 1
    assert stream.pending_buffered_tick_count() == 2

    stream.set_cursor(TickJournalCursor(last_processed_seq=3))
    assert stream.pending_buffered_tick_count() == 0
    assert stream.cursor.last_processed_seq == 3


def test_pending_buffered_tick_count_empty(tmp_path):
    stream = JournalTickStream(TickJournal(str(tmp_path)))
    assert stream.pending_buffered_tick_count() == 0


def test_drain_pending_tick_queue_ignores_task_done_value_error(tmp_path):
    """Coverage: task_done without matching unfinished counter must not raise."""
    stream = JournalTickStream(TickJournal(str(tmp_path)))
    queue = stream._JournalTickStream__queue
    queue.put_nowait((1, _tick("stale")))
    queue.task_done()  # already balanced — next task_done in drain hits ValueError
    stream._JournalTickStream__drain_pending_tick_queue()
    assert stream.pending_buffered_tick_count() == 0


def test_force_rebind_from_seq_rejects_negative_and_clamps_past_eof(tmp_path, monkeypatch):
    """Coverage: ValueError on bad seq; OSError on getsize; clamp indexed_offset > size."""
    journal = TickJournal(str(tmp_path), seq_index_interval=1)
    journal.append(_tick("t1"))
    journal.append(_tick("t2"))
    journal.flush_meta()
    reader = JournalIncrementalReader(journal)

    with pytest.raises(ValueError, match="non-negative"):
        reader.force_rebind_from_seq(-1)

    def boom_getsize(_path):
        raise OSError("gone")

    monkeypatch.setattr(os.path, "getsize", boom_getsize)
    reader.force_rebind_from_seq(1)
    monkeypatch.undo()

    # Force index to claim an offset past the physical file size.
    past_eof = os.path.getsize(journal.journal_path) + 50
    monkeypatch.setattr(
        journal,
        "byte_offset_for_seq",
        lambda _seq: past_eof,
    )
    reader.force_rebind_from_seq(1)
    assert reader.get_read_offset() == os.path.getsize(journal.journal_path)
