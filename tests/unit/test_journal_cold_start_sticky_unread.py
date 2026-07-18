"""
TDD: D4-04 — Silent cold-start / sticky unread on torn journal tip.

Engine gens sat 120s in waiting_first_tick while the reader parked on an
incomplete trailing line; unlock often waited for a later append to merge into
poison. Cold attach must abandon a stuck tip promptly and emit lag diagnostics.

Complements D4-09 (poll-time incomplete skip) with reset_from_seq force-abandon
and JournalTickStream unread-lag observability.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

import pytest

from core.journal.journal_tick_stream import JournalTickStream
from core.journal.tick_journal import (
    JournalIncrementalReader,
    TickJournal,
    TickJournalCursor,
    tick_to_dict,
)
from leviathan_common.models.trade_tick import TradeTick


def _tick(trade_id: str, ts: int = 1000) -> TradeTick:
    return TradeTick("XRPUSDT", ts, 1.0, 1.0, "buy", trade_id)


def _record_line(seq: int, trade_id: str, ts: int = 1000) -> str:
    return json.dumps(
        {"seq": seq, "tick": tick_to_dict(_tick(trade_id, ts=ts))},
        separators=(",", ":"),
    )


class _FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += float(seconds)


def test_reset_from_seq_abandons_incomplete_tip_so_next_append_is_clean(tmp_path, caplog):
    """
    Cold-start attach (set_cursor / reset_from_seq) must not park on a torn EOF
    tip — including `{`-prefixed fragments — so the next collector append is a
    fresh complete line (D4-04).
    """
    journal = TickJournal(str(tmp_path))
    complete = _record_line(1, "kept") + "\n"
    torn = _record_line(2, "torn")[:50]  # `{`-prefixed incomplete JSON
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(complete)
        handle.write(torn)
    journal._TickJournal__meta["latest_seq"] = 1
    journal.flush_meta()

    reader = JournalIncrementalReader(
        journal,
        incomplete_record_max_wait_seconds=30.0,
    )
    with caplog.at_level(logging.WARNING):
        reader.reset_from_seq(2)

    assert reader.get_read_offset() == os.path.getsize(journal.journal_path)
    assert reader.get_invalid_line_skip_count() == 1
    assert any("abandoned incomplete" in r.message.lower() for r in caplog.records)
    assert any("incomplete_trailing_cold_attach" in r.message for r in caplog.records)

    with open(journal.journal_path, "a", encoding="utf-8") as handle:
        handle.write(_record_line(2, "live", ts=1100) + "\n")

    records = reader.poll(2)
    assert len(records) == 1
    assert records[0][1].trade_id == "live"


def test_reader_abandons_stuck_incomplete_after_timeout_without_waiting_for_poison(
    tmp_path, caplog
):
    """Live tail-follow: sticky `{`-prefixed incomplete tip clears after max wait."""
    journal = TickJournal(str(tmp_path))
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(_record_line(1, "ok") + "\n")
        handle.write(_record_line(2, "partial")[:40])

    clock = _FakeClock()
    reader = JournalIncrementalReader(
        journal,
        incomplete_record_max_wait_seconds=1.0,
        clock=clock,
    )
    assert reader.poll(1)[0][1].trade_id == "ok"
    assert reader.poll(2) == []
    assert reader.get_invalid_line_skip_count() == 0

    clock.advance(1.5)
    with caplog.at_level(logging.WARNING):
        assert reader.poll(2) == []

    assert reader.get_invalid_line_skip_count() == 1
    assert any("incomplete_trailing_stale" in r.message for r in caplog.records)
    tip = os.path.getsize(journal.journal_path)
    assert reader.get_read_offset() == tip
    snapshot = reader.get_read_progress_snapshot()
    assert snapshot["incomplete_stuck"] is False

    # Append a complete record after the abandoned tip (collector path).
    journal._TickJournal__meta["latest_seq"] = 1
    seq = journal.append(_tick("fresh", ts=1200))
    assert seq == 2

    records = reader.poll(2)
    assert [t.trade_id for _, t in records] == ["fresh"]


def test_reader_still_waits_for_partial_line_within_timeout(tmp_path):
    """Legitimate in-flight writes must still complete within the timeout window."""
    journal = TickJournal(str(tmp_path))
    partial = _record_line(1, "partial")[:40]
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(partial)

    clock = _FakeClock()
    reader = JournalIncrementalReader(
        journal,
        incomplete_record_max_wait_seconds=2.0,
        clock=clock,
    )
    assert reader.poll(1) == []

    clock.advance(0.5)
    with open(journal.journal_path, "a", encoding="utf-8") as handle:
        handle.write(_record_line(1, "partial")[40:] + "\n")

    records = reader.poll(1)
    assert [t.trade_id for _, t in records] == ["partial"]


def test_read_progress_snapshot_exposes_offset_size_and_lag(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("a"))
    journal.append(_tick("b"))
    journal.flush_meta()
    reader = JournalIncrementalReader(journal)
    reader.reset_from_seq(2)
    snapshot = reader.get_read_progress_snapshot()
    assert snapshot["next_seq"] == 2
    assert snapshot["read_offset"] >= 0
    assert snapshot["journal_size"] >= snapshot["read_offset"]
    assert snapshot["latest_seq"] == 2
    assert snapshot["lag_seq"] == 1
    assert "incomplete_stuck" in snapshot


def test_read_progress_snapshot_latest_seq_not_stale_vs_reader_progress(tmp_path):
    """
    Meta may lag (META_PERSIST_INTERVAL); snapshot latest_seq must not stay
    below records the reader has already consumed (next_seq >> disk watermark).
    """
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("a"))
    journal.append(_tick("b"))
    journal.append(_tick("c"))
    # Persist a stale watermark while the file already holds seq 1..3.
    journal._TickJournal__meta["latest_seq"] = 1
    journal.flush_meta()

    reader = JournalIncrementalReader(journal)
    assert [seq for seq, _ in reader.poll(1)] == [1, 2, 3]
    snapshot = reader.get_read_progress_snapshot()
    assert snapshot["next_seq"] == 4
    assert snapshot["latest_seq"] == 3
    assert snapshot["lag_seq"] == 0
    assert snapshot["read_offset"] == snapshot["journal_size"]


def test_read_progress_snapshot_coerces_production_stale_meta_watermark(tmp_path):
    """
    D6-A04 / D5-08: reproduce H00-A03 pattern — disk meta frozen at 545482 while
    the reader has already consumed far ahead (next_seq ~552k). Snapshot must
    never report latest_seq << next_seq - 1 when those seqs were consumed.
    """
    stale_watermark = 545_482
    consumed_through = 552_039
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("seed"))
    # Force the exact stale disk tip seen in 2026-07-17 logs.
    journal._TickJournal__meta["latest_seq"] = stale_watermark
    journal.flush_meta()
    assert journal.read_latest_seq_from_disk() == stale_watermark

    reader = JournalIncrementalReader(journal)
    # Simulate a caught-up reader that already walked past the stale meta tip.
    reader._JournalIncrementalReader__next_seq = consumed_through + 1
    try:
        reader._JournalIncrementalReader__read_offset = os.path.getsize(
            journal.journal_path
        )
    except OSError:
        reader._JournalIncrementalReader__read_offset = 0

    snapshot = reader.get_read_progress_snapshot()
    assert snapshot["next_seq"] == consumed_through + 1
    assert snapshot["latest_seq"] == consumed_through
    assert snapshot["latest_seq"] >= snapshot["next_seq"] - 1
    assert snapshot["latest_seq"] > stale_watermark
    assert snapshot["lag_seq"] == 0


@pytest.mark.asyncio
async def test_journal_tick_stream_eof_wait_is_not_warning_unread_lag(tmp_path, caplog):
    """
    P2: at EOF with lag_seq=0 the reader waits for new producer writes —
    that must not be logged as WARNING 'unread lag'.
    """
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("prior"))
    journal.save_cursor(TickJournalCursor(last_processed_seq=1))

    clock = _FakeClock()
    stream = JournalTickStream(
        journal,
        poll_interval_seconds=0.01,
        empty_poll_diagnostic_seconds=0.05,
        clock=clock,
    )
    stream_task = asyncio.create_task(stream.start_streaming())
    try:
        with caplog.at_level(logging.DEBUG):
            for _ in range(30):
                clock.advance(0.02)
                await asyncio.sleep(0.01)
                if any(
                    "waiting for new journal" in r.message.lower()
                    or "eof" in r.message.lower()
                    for r in caplog.records
                    if r.name.endswith("journal_tick_stream")
                    or "JournalTickStream" in r.message
                ):
                    break
        eof_records = [
            r
            for r in caplog.records
            if "JournalTickStream" in r.message
            and (
                "waiting for new journal" in r.message.lower()
                or "caught up at eof" in r.message.lower()
            )
        ]
        assert eof_records, "expected EOF-wait diagnostic log"
        assert all(r.levelno < logging.WARNING for r in eof_records)
        assert not any(
            "journal unread lag" in r.message.lower() and r.levelno >= logging.WARNING
            for r in caplog.records
        )
        assert any("offset=" in r.message for r in eof_records)
        assert any("size=" in r.message for r in eof_records)
    finally:
        await stream.stop()
        stream_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stream_task


@pytest.mark.asyncio
async def test_journal_tick_stream_logs_warning_unread_lag_when_behind(tmp_path, caplog):
    """Real unread lag (journal tip ahead of reader) stays WARNING."""
    journal = TickJournal(str(tmp_path))
    for i in range(5):
        journal.append(_tick(f"t{i}", ts=1000 + i))
    journal.flush_meta()
    journal.save_cursor(TickJournalCursor(last_processed_seq=0))

    # Park the stream cursor at seq 1 while meta tip is 5 → lag_seq > 0.
    # Use a reader that never advances past an incomplete tip to force empty polls.
    with open(journal.journal_path, "a", encoding="utf-8") as handle:
        handle.write('{"seq":6,"tick":{')  # incomplete → poll yields [] after replay?

    clock = _FakeClock()
    stream = JournalTickStream(
        journal,
        poll_interval_seconds=0.01,
        empty_poll_diagnostic_seconds=0.05,
        incomplete_record_max_wait_seconds=30.0,
        clock=clock,
    )
    # Force reader to sit behind tip: reset after consuming nothing by leaving
    # next_seq at 1 while meta latest is 5 — first polls will drain then stick.
    stream_task = asyncio.create_task(stream.start_streaming())
    try:
        # Drain queued ticks without marking so we focus on empty-poll diagnostics
        # after catch-up; instead stop draining and wait for sticky incomplete tip.
        await asyncio.sleep(0.05)
        with caplog.at_level(logging.WARNING):
            for _ in range(40):
                clock.advance(0.02)
                await asyncio.sleep(0.01)
                if any("journal unread lag" in r.message.lower() for r in caplog.records):
                    break
        # After draining complete records, sticky incomplete tip should warn.
        assert any(
            "journal unread lag" in r.message.lower() and r.levelno >= logging.WARNING
            for r in caplog.records
        )
    finally:
        await stream.stop()
        stream_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stream_task


@pytest.mark.asyncio
async def test_set_cursor_cold_attach_consumes_new_ticks_despite_torn_tip(tmp_path):
    """End-to-end: checkpoint restore must not sticky-hang on a torn journal tip."""
    journal = TickJournal(str(tmp_path))
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(_record_line(1, "prior") + "\n")
        # Prod-shaped torn suffix without newline (hour-00 gen195 unlock shape).
        handle.write('96960"}}')
    journal._TickJournal__meta["latest_seq"] = 1
    journal.flush_meta()

    stream = JournalTickStream(journal, poll_interval_seconds=0.01)
    stream.set_cursor(TickJournalCursor(last_processed_seq=1))
    assert stream.get_invalid_line_skip_count() >= 1

    stream_task = asyncio.create_task(stream.start_streaming())
    try:
        journal.append(_tick("after-attach", ts=2000))
        tick = await asyncio.wait_for(stream.wait_for_next_tick(), timeout=2.0)
        assert tick.trade_id == "after-attach"
    finally:
        await stream.stop()
        stream_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stream_task
