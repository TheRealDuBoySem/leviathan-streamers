"""
TDD: JournalIncrementalReader must tolerate empty, partial, and concatenated lines
without raising JSONDecodeError or poisoning respawn loops.
"""

import asyncio
import json
import logging
import os

import pytest

from core.journal.journal_tick_stream import JournalTickStream
from core.journal.tick_journal import (
    JournalIncrementalReader,
    TickJournal,
    _should_log_invalid_line,
    tick_to_dict,
)
from leviathan_common.models.trade_tick import TradeTick


def _tick(trade_id: str, ts: int = 1000) -> TradeTick:
    return TradeTick("BTCUSDT", ts, 100.0, 1.0, "buy", trade_id)


def _record_line(seq: int, trade_id: str, ts: int = 1000) -> str:
    return json.dumps(
        {"seq": seq, "tick": tick_to_dict(_tick(trade_id, ts=ts))},
        separators=(",", ":"),
    )


def test_reader_skips_empty_lines_without_raising(tmp_path):
    journal = TickJournal(str(tmp_path))
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write("\n")
        handle.write("   \n")
        handle.write(_record_line(1, "ok") + "\n")

    reader = JournalIncrementalReader(journal)
    records = reader.poll(1)

    assert len(records) == 1
    assert records[0][1].trade_id == "ok"


def test_reader_waits_on_partial_line_then_resumes(tmp_path):
    journal = TickJournal(str(tmp_path))
    partial = _record_line(1, "partial")[:40]
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(partial)

    reader = JournalIncrementalReader(journal)
    assert reader.poll(1) == []

    with open(journal.journal_path, "a", encoding="utf-8") as handle:
        handle.write(_record_line(1, "partial")[40:] + "\n")
        handle.write(_record_line(2, "next", ts=1100) + "\n")

    records = reader.poll(1)
    assert [tick.trade_id for _, tick in records] == ["partial", "next"]


def test_reader_skips_concatenated_json_line_and_continues(tmp_path, caplog):
    journal = TickJournal(str(tmp_path))
    concatenated = _record_line(1, "a") + _record_line(2, "b")
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(concatenated + "\n")
        handle.write(_record_line(3, "c", ts=1200) + "\n")

    reader = JournalIncrementalReader(journal)
    with caplog.at_level(logging.WARNING):
        records = reader.poll(1)

    assert len(records) == 1
    assert records[0][0] == 3
    assert records[0][1].trade_id == "c"
    assert any("skipped invalid" in record.message.lower() for record in caplog.records)


def test_reader_skips_malformed_line_and_advances_offset(tmp_path, caplog):
    journal = TickJournal(str(tmp_path))
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(_record_line(1, "first") + "\n")
        handle.write("{not-valid-json\n")
        handle.write(_record_line(2, "second", ts=1100) + "\n")

    reader = JournalIncrementalReader(journal)
    with caplog.at_level(logging.WARNING):
        first_batch = reader.poll(1)

    assert [tick.trade_id for _, tick in first_batch] == ["first", "second"]

    # Poison pill must not reappear on the next poll / respawn.
    second_batch = reader.poll(3)
    assert second_batch == []
    assert sum(1 for r in caplog.records if "skipped invalid" in r.message.lower()) >= 1
    assert os.path.exists(journal.quarantine_path)
    with open(journal.quarantine_path, "r", encoding="utf-8") as handle:
        quarantine = handle.read()
    assert "{not-valid-json" in quarantine


def test_reader_resume_after_skip_does_not_reparse_poison(tmp_path):
    journal = TickJournal(str(tmp_path))
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write("Expecting value char 0 poison\n")
        handle.write(_record_line(1, "survivor") + "\n")

    reader = JournalIncrementalReader(journal)
    first = reader.poll(1)
    assert len(first) == 1
    assert first[0][1].trade_id == "survivor"

    assert reader.poll(2) == []

    fresh = JournalIncrementalReader(journal)
    # Fresh reader restarts from byte 0 when asked for seq 1; poison skipped again
    # without raising, still yields survivor.
    revived = fresh.poll(1)
    assert len(revived) == 1
    assert revived[0][1].trade_id == "survivor"


def test_resync_swallows_getsize_oserror(tmp_path, monkeypatch):
    """Unable to stat the journal must not raise during the rewrite resync check."""
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("t1"))
    reader = JournalIncrementalReader(journal)
    assert reader.poll(1)

    def _boom(_path):
        raise OSError("stat failed")

    monkeypatch.setattr(os.path, "getsize", _boom)
    # Exists() still true; resync path hits getsize OSError and returns quietly.
    assert reader.poll(2) == []


def test_invalid_line_warning_rate_limit_avoids_flood():
    assert _should_log_invalid_line(1) is True
    assert _should_log_invalid_line(3) is True
    assert _should_log_invalid_line(4) is False
    assert _should_log_invalid_line(10) is True
    assert _should_log_invalid_line(11) is False
    assert _should_log_invalid_line(500) is True


def _recovery_log_records(caplog):
    return [
        record
        for record in caplog.records
        if "recovered after" in record.message.lower()
    ]


def test_reader_logs_recovery_once_after_poison_then_valid(tmp_path, caplog):
    journal = TickJournal(str(tmp_path))
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write("{not-valid-json\n")
        handle.write("{also-broken\n")
        handle.write(_record_line(1, "healed") + "\n")

    reader = JournalIncrementalReader(journal)
    with caplog.at_level(logging.INFO):
        records = reader.poll(1)

    assert [tick.trade_id for _, tick in records] == ["healed"]
    assert reader.get_invalid_line_skip_count() == 2
    assert reader.get_consecutive_parse_failures() == 0
    recovery = _recovery_log_records(caplog)
    assert len(recovery) == 1
    assert "2 consecutive" in recovery[0].message
    assert "last reason=" in recovery[0].message.lower()


def test_reader_recovery_log_is_edge_triggered_not_spam(tmp_path, caplog):
    journal = TickJournal(str(tmp_path))
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write("{poison\n")
        handle.write(_record_line(1, "a") + "\n")
        handle.write(_record_line(2, "b", ts=1100) + "\n")

    reader = JournalIncrementalReader(journal)
    with caplog.at_level(logging.INFO):
        first = reader.poll(1)
        second = reader.poll(3)

    assert [tick.trade_id for _, tick in first] == ["a", "b"]
    assert second == []
    assert len(_recovery_log_records(caplog)) == 1


def test_reader_logs_recovery_once_per_episode(tmp_path, caplog):
    journal = TickJournal(str(tmp_path))
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write("{ep1\n")
        handle.write(_record_line(1, "ok1") + "\n")

    reader = JournalIncrementalReader(journal)
    with caplog.at_level(logging.INFO):
        assert reader.poll(1)[0][1].trade_id == "ok1"
        with open(journal.journal_path, "a", encoding="utf-8") as handle:
            handle.write("{ep2\n")
            handle.write(_record_line(2, "ok2", ts=1100) + "\n")
        assert reader.poll(2)[0][1].trade_id == "ok2"

    assert reader.get_invalid_line_skip_count() == 2
    assert len(_recovery_log_records(caplog)) == 2


def test_reader_no_recovery_log_when_never_failed(tmp_path, caplog):
    journal = TickJournal(str(tmp_path))
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(_record_line(1, "clean") + "\n")

    reader = JournalIncrementalReader(journal)
    with caplog.at_level(logging.INFO):
        records = reader.poll(1)

    assert len(records) == 1
    assert reader.get_invalid_line_skip_count() == 0
    assert reader.get_consecutive_parse_failures() == 0
    assert _recovery_log_records(caplog) == []


def test_reader_consecutive_failures_while_still_poisoned(tmp_path):
    journal = TickJournal(str(tmp_path))
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write("{a\n")
        handle.write("{b\n")

    reader = JournalIncrementalReader(journal)
    assert reader.poll(1) == []
    assert reader.get_invalid_line_skip_count() == 2
    assert reader.get_consecutive_parse_failures() == 2


@pytest.mark.asyncio
async def test_journal_tick_stream_exposes_parse_failure_health_and_recovery(
    tmp_path, caplog
):
    journal = TickJournal(str(tmp_path))
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write("{stream-poison\n")
        handle.write(_record_line(1, "alive") + "\n")

    stream = JournalTickStream(journal, poll_interval_seconds=0.01)
    with caplog.at_level(logging.INFO):
        stream_task = asyncio.create_task(stream.start_streaming())
        try:
            tick = await asyncio.wait_for(stream.wait_for_next_tick(), timeout=1.0)
            assert tick.trade_id == "alive"
            stream.mark_tick_as_processed()
            assert stream.get_invalid_line_skip_count() >= 1
            assert stream.get_consecutive_parse_failures() == 0
            assert len(_recovery_log_records(caplog)) == 1
        finally:
            await stream.stop()
            stream_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await stream_task


def test_quarantine_line_rejects_blank_reason(tmp_path):
    journal = TickJournal(str(tmp_path))
    with pytest.raises(ValueError, match="reason must be a non-empty string"):
        journal.quarantine_line("bad", reason="  ")


def test_quarantine_line_rejects_non_string_and_swallows_oserror(tmp_path, monkeypatch, caplog):
    journal = TickJournal(str(tmp_path))
    with pytest.raises(TypeError, match="line must be a string"):
        journal.quarantine_line(123, reason="bad")  # type: ignore[arg-type]

    import builtins

    real_open = builtins.open

    def _selective_open(path, *args, **kwargs):
        if str(path).endswith("quarantine.jsonl"):
            raise OSError("disk full")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr("builtins.open", _selective_open)
    with caplog.at_level("WARNING"):
        journal.quarantine_line("{}", reason="poison")
    assert any("Failed to quarantine" in r.message for r in caplog.records)


def test_reader_quarantines_non_object_and_bad_tick_records(tmp_path):
    journal = TickJournal(str(tmp_path))
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write("123\n")
        handle.write('{"seq":"x","tick":{}}\n')
        handle.write(_record_line(1, "ok") + "\n")

    records = journal.create_incremental_reader().poll(1)
    assert len(records) == 1
    assert records[0][1].trade_id == "ok"
    with open(journal.quarantine_path, "r", encoding="utf-8") as handle:
        quarantine = handle.read()
    assert quarantine


def test_compact_quarantines_invalid_lines(tmp_path, caplog):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("old"))
    journal.append(_tick("keep"))
    with open(journal.journal_path, "a", encoding="utf-8") as handle:
        handle.write("{broken\n")
    with caplog.at_level("WARNING"):
        removed = journal.compact_before_seq(2)
    assert removed >= 1
    assert os.path.exists(journal.quarantine_path)
    assert any("compact skipped invalid" in r.message for r in caplog.records)

@pytest.mark.asyncio
async def test_journal_tick_stream_survives_poison_line(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("live"))
    with open(journal.journal_path, "r", encoding="utf-8") as handle:
        valid_content = handle.read()
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write("{corrupt\n")
        handle.write(valid_content)

    stream = JournalTickStream(journal, poll_interval_seconds=0.01)
    stream_task = asyncio.create_task(stream.start_streaming())
    try:
        tick = await asyncio.wait_for(stream.wait_for_next_tick(), timeout=1.0)
        assert tick.trade_id == "live"
        stream.mark_tick_as_processed()
    finally:
        await stream.stop()
        stream_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stream_task


def test_reader_aligns_mid_line_seek_before_parsing(tmp_path, caplog):
    """
    Bug #5: after restore/seek, landing mid-line must advance to the next newline
    before parsing — never treat a torn suffix as a complete invalid line.
    """
    journal = TickJournal(str(tmp_path), seq_index_interval=1)
    line1 = _record_line(1, "first") + "\n"
    line2 = _record_line(2, "second", ts=1100) + "\n"
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(line1)
        handle.write(line2)
    mid_line_offset = len(line1.encode("utf-8")) // 2
    assert 0 < mid_line_offset < len(line1.encode("utf-8"))
    # byte_offset_for_seq(1) always returns 0; use start_seq>=2 so the sparse
    # index mid-line offset is actually used on seek.
    journal._TickJournal__meta["latest_seq"] = 2
    journal._TickJournal__meta["seq_index"] = [[0, 0], [2, mid_line_offset]]
    journal.flush_meta()

    reader = JournalIncrementalReader(journal)
    with caplog.at_level(logging.WARNING):
        records = reader.poll(2)

    assert [tick.trade_id for _, tick in records] == ["second"]
    assert reader.get_invalid_line_skip_count() == 0
    assert not any("skipped invalid" in r.message.lower() for r in caplog.records)


def test_fresh_reset_to_tip_does_not_requarantine_prefix_poison(tmp_path):
    """
    Bug #5 cold restore: seeking to a high start_seq via sparse index must not
    re-quarantine already-passed poison while resolving the byte cursor.
    """
    journal = TickJournal(str(tmp_path), seq_index_interval=100)
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write("{prefix-poison\n")
        for seq in range(1, 6):
            handle.write(_record_line(seq, f"t{seq}", ts=1000 + seq) + "\n")
    journal._TickJournal__meta["latest_seq"] = 5
    journal._TickJournal__meta["seq_index"] = [[0, 0]]
    journal.flush_meta()

    reader = JournalIncrementalReader(journal)
    assert reader.poll(6) == []
    assert reader.get_invalid_line_skip_count() == 0
    assert reader.get_read_offset() > 0


def test_reset_same_seq_does_not_rewind_into_already_scanned_poison(tmp_path):
    """
    Bug #5: sparse-index reset_from_seq must not pull the byte cursor backwards
    into already-consumed poison when the logical next seq is unchanged
    (checkpoint continue / redundant restore).
    """
    journal = TickJournal(str(tmp_path), seq_index_interval=100)
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write("{poison-prefix\n")
        for seq in range(1, 6):
            handle.write(_record_line(seq, f"t{seq}", ts=1000 + seq) + "\n")
    journal._TickJournal__meta["latest_seq"] = 5
    journal._TickJournal__meta["seq_index"] = [[0, 0]]
    journal.flush_meta()

    reader = JournalIncrementalReader(journal)
    first = reader.poll(1)
    assert [tick.trade_id for _, tick in first] == [f"t{i}" for i in range(1, 6)]
    assert reader.get_invalid_line_skip_count() == 1
    tip_offset = reader.get_read_offset()
    assert tip_offset > 0

    reader.reset_from_seq(6)

    assert reader.get_read_offset() >= tip_offset
    assert reader.poll(6) == []
    assert reader.get_invalid_line_skip_count() == 1


def test_reset_same_seq_does_not_emit_false_recovery_after_poison(tmp_path, caplog):
    """Redundant reset at the tip must not re-scan poison and log a fake recovery."""
    journal = TickJournal(str(tmp_path), seq_index_interval=100)
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write("{poison\n")
        handle.write(_record_line(1, "ok") + "\n")
    journal._TickJournal__meta["latest_seq"] = 1
    journal._TickJournal__meta["seq_index"] = [[0, 0]]
    journal.flush_meta()

    reader = JournalIncrementalReader(journal)
    with caplog.at_level(logging.INFO):
        assert reader.poll(1)[0][1].trade_id == "ok"
        assert len(_recovery_log_records(caplog)) == 1
        reader.reset_from_seq(2)
        assert reader.poll(2) == []

    assert len(_recovery_log_records(caplog)) == 1
    assert reader.get_invalid_line_skip_count() == 1


def test_reset_backward_start_seq_rescans_but_line_aligns(tmp_path):
    """A true rewind (lower start_seq) may rescan; mid-line seeks still align."""
    journal = TickJournal(str(tmp_path), seq_index_interval=1)
    line1 = _record_line(1, "a") + "\n"
    line2 = _record_line(2, "b", ts=1100) + "\n"
    line3 = _record_line(3, "c", ts=1200) + "\n"
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(line1)
        handle.write(line2)
        handle.write(line3)
    mid = len(line1.encode("utf-8")) // 2
    line1_len = len(line1.encode("utf-8"))
    journal._TickJournal__meta["latest_seq"] = 3
    journal._TickJournal__meta["seq_index"] = [
        [0, 0],
        [2, mid],
        [3, line1_len + len(line2.encode("utf-8"))],
    ]
    journal.flush_meta()

    reader = JournalIncrementalReader(journal)
    assert [t.trade_id for _, t in reader.poll(3)] == ["c"]
    reader.reset_from_seq(2)
    # Mid-line index for seq 2 → align past torn line1 → read line2+.
    assert [t.trade_id for _, t in reader.poll(2)] == ["b", "c"]
    assert reader.get_invalid_line_skip_count() == 0


@pytest.mark.asyncio
async def test_journal_tick_stream_set_cursor_does_not_replay_poison(tmp_path):
    """Checkpoint restore via set_cursor must not re-quarantine already-passed poison."""
    journal = TickJournal(str(tmp_path), seq_index_interval=100)
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write("{early-poison\n")
        handle.write(_record_line(1, "live") + "\n")
    journal._TickJournal__meta["latest_seq"] = 1
    journal._TickJournal__meta["seq_index"] = [[0, 0]]
    journal.flush_meta()

    stream = JournalTickStream(journal, poll_interval_seconds=0.01)
    stream_task = asyncio.create_task(stream.start_streaming())
    try:
        tick = await asyncio.wait_for(stream.wait_for_next_tick(), timeout=1.0)
        assert tick.trade_id == "live"
        stream.mark_tick_as_processed()
        skips_after_first = stream.get_invalid_line_skip_count()
        assert skips_after_first >= 1

        from core.journal.tick_journal import TickJournalCursor

        stream.set_cursor(TickJournalCursor(last_processed_seq=1))
        await asyncio.sleep(0.05)
        assert stream.get_invalid_line_skip_count() == skips_after_first
    finally:
        await stream.stop()
        stream_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stream_task


def test_choose_reset_offset_keeps_high_water_when_index_lags(tmp_path):
    """Same-seq reset must keep tip offset when sparse index points earlier."""
    journal = TickJournal(str(tmp_path), seq_index_interval=100)
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        for seq in range(1, 4):
            handle.write(_record_line(seq, f"t{seq}", ts=1000 + seq) + "\n")
    reader = JournalIncrementalReader(journal)
    assert len(reader.poll(1)) == 3
    tip = reader.get_read_offset()
    # Force a stale sparse hint far behind the live tip.
    kept = reader._JournalIncrementalReader__choose_reset_offset(
        start_seq=4,
        indexed_offset=0,
    )
    assert kept == tip


def test_choose_reset_offset_falls_back_on_getsize_oserror(tmp_path, monkeypatch):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("t1"))
    reader = JournalIncrementalReader(journal)
    assert reader.poll(1)
    tip = reader.get_read_offset()
    reader._JournalIncrementalReader__next_seq = 2
    reader._JournalIncrementalReader__read_offset = tip

    def _boom(_path):
        raise OSError("stat failed")

    monkeypatch.setattr(os.path, "getsize", _boom)
    assert (
        reader._JournalIncrementalReader__choose_reset_offset(
            start_seq=2,
            indexed_offset=0,
        )
        == 0
    )


def test_choose_reset_offset_falls_back_when_cursor_past_eof(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("t1"))
    reader = JournalIncrementalReader(journal)
    assert reader.poll(1)
    reader._JournalIncrementalReader__next_seq = 2
    reader._JournalIncrementalReader__read_offset = 10**9
    assert (
        reader._JournalIncrementalReader__choose_reset_offset(
            start_seq=2,
            indexed_offset=7,
        )
        == 7
    )


def test_align_read_offset_noop_when_journal_missing(tmp_path):
    journal = TickJournal(str(tmp_path))
    missing = str(tmp_path / "missing_journal.jsonl")
    journal._TickJournal__journal_path = missing
    reader = JournalIncrementalReader(journal)
    reader._JournalIncrementalReader__read_offset = 12
    assert not os.path.exists(missing)
    reader._JournalIncrementalReader__align_read_offset_to_line_boundary()
    assert reader.get_read_offset() == 12


def test_align_read_offset_advances_past_mid_line(tmp_path):
    journal = TickJournal(str(tmp_path))
    line1 = _record_line(1, "a") + "\n"
    line2 = _record_line(2, "b", ts=1100) + "\n"
    with open(journal.journal_path, "wb") as handle:
        handle.write(line1.encode("utf-8"))
        handle.write(line2.encode("utf-8"))
    mid = len(line1.encode("utf-8")) // 2
    reader = JournalIncrementalReader(journal)
    reader._JournalIncrementalReader__read_offset = mid
    reader._JournalIncrementalReader__align_read_offset_to_line_boundary()
    assert reader.get_read_offset() == len(line1.encode("utf-8"))


def test_align_read_offset_swallows_oserror(tmp_path, monkeypatch):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("t1"))
    reader = JournalIncrementalReader(journal)
    reader._JournalIncrementalReader__read_offset = 1

    import builtins

    real_open = builtins.open

    def _boom(path, *args, **kwargs):
        mode = args[0] if args else kwargs.get("mode", "r")
        if str(path) == journal.journal_path and mode == "rb":
            raise OSError("read failed")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", _boom)
    reader._JournalIncrementalReader__align_read_offset_to_line_boundary()
    assert reader.get_read_offset() == 1


def test_resolve_byte_offset_handles_blank_and_partial_trailing_line(tmp_path):
    journal = TickJournal(str(tmp_path), seq_index_interval=100)
    line1 = (_record_line(1, "a") + "\n").encode("utf-8")
    blank = b"\n"
    partial = _record_line(2, "partial")[:30].encode("utf-8")
    with open(journal.journal_path, "wb") as handle:
        handle.write(line1)
        handle.write(blank)
        handle.write(partial)
    # Walk: seq1 (<2) → blank continue → partial without newline → line_start of partial.
    offset = journal._TickJournal__resolve_byte_offset_for_seq(2, 0)
    assert offset == len(line1) + len(blank)


def test_resolve_byte_offset_falls_back_on_open_oserror(tmp_path, monkeypatch):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("t1"))

    import builtins

    real_open = builtins.open

    def _boom(path, *args, **kwargs):
        mode = args[0] if args else kwargs.get("mode", "r")
        if str(path) == journal.journal_path and mode == "rb":
            raise OSError("open failed")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", _boom)
    assert journal._TickJournal__resolve_byte_offset_for_seq(1, 42) == 42


# --- D4-01: sticky offset past EOF after rewrite / stale seq_index ---


def _rewrite_warn_records(caplog):
    return [
        record
        for record in caplog.records
        if "offset past journal size after rewrite" in record.getMessage()
    ]


def test_d4_01_stale_seq_index_after_external_rewrite_recovers_without_spam(
    tmp_path, caplog
):
    """
    D4-01: after compact/rewrite, in-memory sparse seq_index can still point past
    the new file size. Resync must rebind the cursor, resume ticks, and not flood
    WARN on every empty poll with a stuck offset/seq.
    """
    journal = TickJournal(str(tmp_path), seq_index_interval=10)
    for index in range(50):
        journal.append(_tick(f"t{index}", ts=1000 + index))

    reader = JournalIncrementalReader(journal)
    assert len(reader.poll(1)) == 50
    tip_offset = reader.get_read_offset()
    assert tip_offset > 0

    with open(journal.journal_path, "r", encoding="utf-8") as handle:
        retained = [line for line in handle if line.strip()][-5:]
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.writelines(retained)
    rewritten_size = os.path.getsize(journal.journal_path)
    assert tip_offset > rewritten_size

    # Leave in-memory seq_index stale (other-process / pre-reload compact).
    journal.append(_tick("post-rewrite", ts=2000))

    with caplog.at_level(logging.WARNING):
        recovered = reader.poll(51)
        for _ in range(24):
            if recovered:
                break
            recovered = reader.poll(51)

    assert recovered, "reader must recover ticks after rewrite with stale seq_index"
    assert [seq for seq, _ in recovered] == [51]
    assert recovered[0][1].trade_id == "post-rewrite"
    assert reader.get_read_offset() <= os.path.getsize(journal.journal_path)
    assert reader.get_read_offset() != tip_offset
    assert len(_rewrite_warn_records(caplog)) <= 2


def test_d4_01_past_eof_resync_clamps_and_advances_on_subsequent_append(
    tmp_path, caplog
):
    """
    D4-01: live tip cursor past EOF after shrink must resync once, then consume
    the next append without a WARN storm at a frozen offset.
    """
    journal = TickJournal(str(tmp_path))
    for index in range(5):
        journal.append(_tick(f"t{index}", ts=1000 + index))
    reader = JournalIncrementalReader(journal)
    assert len(reader.poll(1)) == 5
    pre_offset = reader.get_read_offset()

    with open(journal.journal_path, "r", encoding="utf-8") as handle:
        last_line = [line for line in handle if line.strip()][-1]
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(last_line)
    assert pre_offset > os.path.getsize(journal.journal_path)

    with caplog.at_level(logging.WARNING):
        assert reader.poll(6) == []
        offsets = [reader.get_read_offset()]
        for _ in range(20):
            assert reader.poll(6) == []
            offsets.append(reader.get_read_offset())

        journal.append(_tick("live", ts=3000))
        resumed = reader.poll(6)

    assert [seq for seq, _ in resumed] == [6]
    assert resumed[0][1].trade_id == "live"
    assert all(offset <= os.path.getsize(journal.journal_path) for offset in offsets)
    assert pre_offset not in offsets
    assert len(_rewrite_warn_records(caplog)) <= 2


# --- D4-03: torn / partial journal line at sticky rewrite boundary ---


def test_d4_03_sticky_mid_line_after_grow_past_eof_advances_without_reread_loop(
    tmp_path, caplog
):
    """
    D4-03 (hour-22): sticky offset sits past EOF until the file grows; cursor
    then lands mid-object (e.g. ``k\":{…``). Reader must advance past the torn
    region, recover at most once, and never re-parse the same remnant.
    """
    journal = TickJournal(str(tmp_path), seq_index_interval=100)
    line1 = _record_line(1, "pad") + "\n"
    # Incident-shaped torn suffix (complete line with newline, starts mid-object).
    torn = (
        'k":{"inst_id":"XRPUSDT","ts":1784154630404,"price":1.118,'
        '"size":18.0,"side":"buy","trade_id":"1461415930925686784"}}\n'
    )
    healed = _record_line(2, "healed", ts=1100) + "\n"
    sticky = len(line1.encode("utf-8")) + 2  # mid-torn, not BOL

    # Phase 1: small file so sticky is past EOF (D4-01 neighbour).
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(line1)
    journal._TickJournal__meta["latest_seq"] = 1
    journal._TickJournal__meta["seq_index"] = [[0, 0]]
    journal.flush_meta()

    reader = JournalIncrementalReader(journal)
    assert reader.poll(1)[0][1].trade_id == "pad"
    reader._JournalIncrementalReader__read_offset = sticky
    reader._JournalIncrementalReader__next_seq = 2
    assert reader.get_read_offset() > os.path.getsize(journal.journal_path)
    assert reader.poll(2) == []  # still past EOF / waiting for grow

    # Phase 2: file grows past sticky with torn mid-line + valid record.
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(line1)
        handle.write(torn)
        handle.write(healed)
    journal._TickJournal__meta["latest_seq"] = 2
    journal.flush_meta()

    with caplog.at_level(logging.INFO):
        first = reader.poll(2)
        second = reader.poll(3)

    assert [tick.trade_id for _, tick in first] == ["healed"]
    assert second == []
    # Offset advanced past torn+healed; repeated polls must not re-hit torn.
    assert reader.get_read_offset() == os.path.getsize(journal.journal_path)
    assert reader.get_invalid_line_skip_count() <= 1
    assert len(_recovery_log_records(caplog)) <= 1


def test_d4_03_sticky_mid_line_truncated_eof_after_rewrite_does_not_stall(tmp_path):
    """
    D4-03: after rewrite, sticky offset mid-line and remnant runs to EOF without
    a newline. Even when the remnant starts with a nested ``{`` (looks like an
    in-progress object to D4-09), mid-line align must advance to EOF — not wait.
    """
    journal = TickJournal(str(tmp_path))
    line1 = _record_line(1, "keep") + "\n"
    # Mid offset deliberately lands on nested '{' so incomplete-wait would stall
    # without BOL align (hour-22 sticky mid-object shape).
    torn_no_nl = (
        'k":{"inst_id":"XRPUSDT","ts":1,"price":1.0,"size":1.0,'
        '"side":"buy","trade_id":"torn"}}'
    )
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(line1)
        handle.write(torn_no_nl)

    reader = JournalIncrementalReader(journal)
    assert reader.poll(1)[0][1].trade_id == "keep"
    mid = len(line1.encode("utf-8")) + 3  # points at '{'
    assert torn_no_nl[3:4] == "{"
    reader._JournalIncrementalReader__read_offset = mid
    reader._JournalIncrementalReader__next_seq = 2

    assert reader.poll(2) == []
    assert reader.get_read_offset() == os.path.getsize(journal.journal_path)

    with open(journal.journal_path, "a", encoding="utf-8") as handle:
        handle.write("\n")
        handle.write(_record_line(2, "after-rewrite", ts=1100) + "\n")

    records = reader.poll(2)
    assert [tick.trade_id for _, tick in records] == ["after-rewrite"]
    assert reader.poll(3) == []


def test_d4_03_file_shrink_under_sticky_offset_realigns_and_recovers_once(
    tmp_path, caplog
):
    """
    D4-03: journal shrink/rewrite while sticky offset remains numerically inside
    the new file can land mid-line. Align must advance; no re-read loop.
    """
    journal = TickJournal(str(tmp_path), seq_index_interval=100)
    for seq in range(1, 40):
        journal.append(_tick(f"old{seq}", ts=1000 + seq))

    reader = JournalIncrementalReader(journal)
    assert len(reader.poll(1)) == 39
    pre_rewrite_size = reader.get_read_offset()
    assert reader.poll(40) == []

    prefix = "".join(
        _record_line(seq, f"new{seq}", ts=2000 + seq) + "\n" for seq in range(1, 6)
    )
    mid_line = _record_line(99, "discard-mid", ts=2099) + "\n"
    suffix = "".join(
        _record_line(seq, f"new{seq}", ts=2000 + seq) + "\n" for seq in range(6, 12)
    )
    content = prefix + mid_line + suffix
    raw = content.encode("utf-8")
    sticky = len(prefix.encode("utf-8")) + len(mid_line.encode("utf-8")) // 2
    assert sticky < len(raw) < pre_rewrite_size
    assert raw[sticky - 1 : sticky] != b"\n"

    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(content)
    journal._TickJournal__meta["latest_seq"] = 11
    journal._TickJournal__meta["seq_index"] = [[0, 0]]
    journal.flush_meta()

    reader._JournalIncrementalReader__next_seq = 6
    reader._JournalIncrementalReader__read_offset = sticky

    with caplog.at_level(logging.INFO):
        records = reader.poll(6)
        tip_after = reader.get_read_offset()
        again = reader.poll(12)

    assert tip_after > sticky
    assert [s for s, _ in records] == list(range(6, 12))
    assert [t.trade_id for _, t in records] == [f"new{s}" for s in range(6, 12)]
    assert again == []
    assert len(_recovery_log_records(caplog)) <= 1
    assert any("journal shrunk under sticky offset" in r.message for r in caplog.records)


def test_d4_03_newline_terminated_torn_suffix_skips_once_then_recovers(
    tmp_path, caplog
):
    """Complete torn suffix at BOL uses Day-1 quarantine; recovery logs once."""
    journal = TickJournal(str(tmp_path))
    torn = (
        'k":{"inst_id":"XRPUSDT","ts":1784154630404,"price":1.118,'
        '"size":18.0,"side":"buy","trade_id":"1461415930925686784"}}\n'
    )
    valid = _record_line(1, "stream-ok") + "\n"
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(torn)
        handle.write(valid)

    reader = JournalIncrementalReader(journal)
    with caplog.at_level(logging.INFO):
        records = reader.poll(1)
        assert reader.poll(2) == []

    assert [tick.trade_id for _, tick in records] == ["stream-ok"]
    assert reader.get_invalid_line_skip_count() == 1
    assert reader.get_consecutive_parse_failures() == 0
    assert len(_recovery_log_records(caplog)) == 1
    assert os.path.exists(journal.quarantine_path)
    with open(journal.quarantine_path, "r", encoding="utf-8") as handle:
        quarantine = handle.read()
    assert "XRPUSDT" in quarantine
    assert "1461415930925686784" in quarantine


def test_reader_rejects_non_positive_incomplete_max_wait(tmp_path):
    journal = TickJournal(str(tmp_path))
    with pytest.raises(ValueError, match="incomplete_record_max_wait_seconds must be positive"):
        JournalIncrementalReader(journal, incomplete_record_max_wait_seconds=0)


# --- D4-09: poison-gated first-tick delay (incomplete trailing torn suffix) ---


def test_reader_skips_incomplete_trailing_poison_suffix_immediately(tmp_path, caplog):
    """
    D4-09: torn journal suffixes that cannot be an in-progress JSON object
    (no leading '{') must be quarantined on the first poll — not parked until
    the next writer supplies a newline (observed ~33–56s startup delay).
    """
    journal = TickJournal(str(tmp_path))
    poison = (
        'id":"XRPUSDT","ts":1784147002135,"price":1.1125,'
        '"size":22.0,"side":"buy","trade_id":"1461383935646507016"}}'
    )
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(poison)

    reader = JournalIncrementalReader(journal)
    with caplog.at_level(logging.WARNING):
        assert reader.poll(1) == []

    assert reader.get_invalid_line_skip_count() == 1
    assert reader.get_read_offset() == len(poison.encode("utf-8"))
    assert any("incomplete_trailing_poison" in r.message for r in caplog.records)
    with open(journal.quarantine_path, "r", encoding="utf-8") as handle:
        quarantine = json.loads(handle.readline())
    assert quarantine["reason"] == "incomplete_trailing_poison"
    assert quarantine["line"] == poison


def test_reader_reaches_valid_tick_after_trailing_poison_without_waiting_for_newline(
    tmp_path,
):
    """After skipping EOF poison, a subsequent complete record is visible immediately."""
    journal = TickJournal(str(tmp_path))
    poison = 'id":"XRPUSDT","ts":1,"price":1.0,"size":1.0,"side":"buy","trade_id":"t"}}'
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(poison)

    reader = JournalIncrementalReader(journal)
    assert reader.poll(1) == []
    assert reader.get_invalid_line_skip_count() == 1

    with open(journal.journal_path, "a", encoding="utf-8") as handle:
        handle.write(_record_line(1, "survivor") + "\n")

    records = reader.poll(1)
    assert len(records) == 1
    assert records[0][1].trade_id == "survivor"


def test_reader_skips_stale_incomplete_json_object_after_max_wait(tmp_path):
    """
    D4-09: a `{`-prefixed incomplete write that never completes must not gate
    startup forever — skip after incomplete_record_max_wait_seconds.
    """
    journal = TickJournal(str(tmp_path))
    partial = _record_line(1, "stuck")[:40]
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(partial)

    clock = {"now": 1000.0}
    reader = JournalIncrementalReader(
        journal,
        incomplete_record_max_wait_seconds=0.5,
        clock=lambda: clock["now"],
    )
    assert reader.poll(1) == []
    assert reader.get_invalid_line_skip_count() == 0

    clock["now"] = 1000.6
    assert reader.poll(1) == []
    assert reader.get_invalid_line_skip_count() == 1
    assert reader.get_read_offset() == len(partial.encode("utf-8"))


@pytest.mark.asyncio
async def test_journal_tick_stream_bounds_time_to_first_tick_with_trailing_poison(
    tmp_path,
):
    """
    D4-09: spawn-path latency — trailing torn poison (no newline) must be
    skipped promptly so the first valid append is not gated for tens of seconds.
    """
    journal = TickJournal(str(tmp_path))
    poison = (
        'st_id":"XRPUSDT","ts":1784119124623,"price":1.1108,'
        '"size":137.0,"side":"buy","trade_id":"1461267008886415360"}}'
    )
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(poison)

    stream = JournalTickStream(journal, poll_interval_seconds=0.01)
    stream_task = asyncio.create_task(stream.start_streaming())
    try:
        started = asyncio.get_running_loop().time()
        for _ in range(50):
            if stream.get_invalid_line_skip_count() >= 1:
                break
            await asyncio.sleep(0.01)
        assert stream.get_invalid_line_skip_count() >= 1
        assert asyncio.get_running_loop().time() - started < 0.5

        with open(journal.journal_path, "a", encoding="utf-8") as handle:
            handle.write(_record_line(1, "first-live") + "\n")

        tick = await asyncio.wait_for(stream.wait_for_next_tick(), timeout=1.0)
        elapsed = asyncio.get_running_loop().time() - started
        assert tick.trade_id == "first-live"
        assert elapsed < 1.0
        stream.mark_tick_as_processed()
    finally:
        await stream.stop()
        stream_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stream_task

def test_reader_rejects_non_callable_clock(tmp_path):
    journal = TickJournal(str(tmp_path))
    with pytest.raises(TypeError, match="clock must be callable"):
        JournalIncrementalReader(journal, clock=123)  # type: ignore[arg-type]


def test_read_progress_snapshot_handles_missing_journal_file(tmp_path, mocker):
    journal = TickJournal(str(tmp_path))
    journal.append(
        TradeTick("XRPUSDT", 1000, 0.5, 1.0, "buy", "a")
    )
    reader = JournalIncrementalReader(journal)
    mocker.patch("os.path.getsize", side_effect=OSError("gone"))
    snapshot = reader.get_read_progress_snapshot()
    assert snapshot["journal_size"] == 0


def test_choose_reset_offset_clamps_indexed_hint_past_eof(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.append(
        TradeTick("XRPUSDT", 1000, 0.5, 1.0, "buy", "a")
    )
    reader = JournalIncrementalReader(journal)
    reader.reset_from_seq(1)
    file_size = os.path.getsize(journal.journal_path)
    chosen = reader._JournalIncrementalReader__choose_reset_offset(
        start_seq=2,
        indexed_offset=file_size + 50,
    )
    assert chosen == file_size


def test_should_skip_incomplete_empty_fragment_is_false(tmp_path):
    journal = TickJournal(str(tmp_path))
    reader = JournalIncrementalReader(journal)
    assert (
        reader._JournalIncrementalReader__should_skip_incomplete_trailing_fragment("")
        is False
    )


def test_reload_seq_index_from_disk_tolerates_bad_meta(tmp_path, mocker):
    journal = TickJournal(str(tmp_path))
    journal.append(
        TradeTick("XRPUSDT", 1000, 0.5, 1.0, "buy", "a")
    )
    mocker.patch.object(
        journal,
        "_TickJournal__load_meta",
        side_effect=OSError("meta missing"),
    )
    journal.reload_seq_index_from_disk()  # must not raise

    mocker.patch.object(
        journal,
        "_TickJournal__load_meta",
        return_value={"seq_index": "not-a-list", "latest_seq": 1},
    )
    journal.reload_seq_index_from_disk()  # non-list index ignored


def test_resync_past_eof_clamps_when_reset_still_past_size(tmp_path, mocker):
    journal = TickJournal(str(tmp_path))
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write('{"seq":1,"tick":{"inst_id":"XRPUSDT","ts":1,"price":1.0,"size":1.0,"side":"buy","trade_id":"a"}}\n')
    journal._TickJournal__meta["latest_seq"] = 1
    journal._TickJournal__meta["seq_index"] = [[0, 0]]
    journal.flush_meta()

    reader = JournalIncrementalReader(journal)
    reader._JournalIncrementalReader__next_seq = 1
    reader._JournalIncrementalReader__read_offset = 10_000
    reader._JournalIncrementalReader__past_eof_resync_logged = False

    def fake_reset(start_seq: int) -> None:
        reader._JournalIncrementalReader__read_offset = 20_000

    mocker.patch.object(reader, "reset_from_seq", side_effect=fake_reset)
    reader._JournalIncrementalReader__resync_if_journal_rewritten(
        journal.journal_path
    )
    assert reader._JournalIncrementalReader__read_offset == os.path.getsize(
        journal.journal_path
    )


def test_abandon_incomplete_tip_force_false_skips_after_wait(tmp_path):
    """Cover force=False branch of __abandon_incomplete_tip_at_cursor (D4-09)."""
    journal = TickJournal(str(tmp_path))
    complete = _record_line(1, "ok") + "\n"
    torn = '{"seq":2,"tick":{"inst_id":"BTCUSDT"'
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(complete)
        handle.write(torn)

    with open(journal.journal_path, "r", encoding="utf-8") as handle:
        handle.readline()
        tip_offset = handle.tell()

    clock = {"t": 100.0}

    def now() -> float:
        return clock["t"]

    reader = JournalIncrementalReader(
        journal,
        incomplete_record_max_wait_seconds=1.0,
        clock=now,
    )
    reader._JournalIncrementalReader__read_offset = tip_offset
    # First call arms the wait window without quarantining.
    reader._JournalIncrementalReader__abandon_incomplete_tip_at_cursor(force=False)
    assert reader.get_invalid_line_skip_count() == 0
    assert reader._JournalIncrementalReader__pending_incomplete_offset == tip_offset
    clock["t"] = 102.0
    reader._JournalIncrementalReader__abandon_incomplete_tip_at_cursor(force=False)
    assert reader.get_invalid_line_skip_count() == 1
    assert reader._JournalIncrementalReader__pending_incomplete_offset is None


def test_abandon_incomplete_tip_swallows_oserror(tmp_path, mocker):
    journal = TickJournal(str(tmp_path))
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(_record_line(1, "ok") + "\n")
    reader = JournalIncrementalReader(journal)
    mocker.patch("builtins.open", side_effect=OSError("io fail"))
    reader._JournalIncrementalReader__abandon_incomplete_tip_at_cursor(force=False)


def test_reload_seq_index_from_disk_tolerates_json_decode_error(tmp_path, mocker):
    journal = TickJournal(str(tmp_path))
    mocker.patch.object(
        journal,
        "_TickJournal__load_meta",
        side_effect=json.JSONDecodeError("bad", "doc", 0),
    )
    journal.reload_seq_index_from_disk()
