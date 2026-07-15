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
