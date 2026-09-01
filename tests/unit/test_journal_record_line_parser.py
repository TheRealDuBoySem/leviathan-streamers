"""Unit tests for journal record line parsing."""

from __future__ import annotations

import json
import logging

from core.journal.journal_record_line_parser import (
    JournalRecordParseError,
    parse_journal_record_line,
)
from core.journal.journal_incremental_reader import JournalIncrementalReader
from core.journal.tick_journal import TickJournal
from core.journal.tick_journal_codec import tick_to_dict
from leviathan_common.models.trade_tick import TradeTick


def _tick(trade_id: str, ts: int = 1000) -> TradeTick:
    return TradeTick("BTCUSDT", ts, 100.0, 1.0, "buy", trade_id)


def _record_line(seq: int, trade_id: str, ts: int = 1000) -> str:
    return json.dumps(
        {"seq": seq, "tick": tick_to_dict(_tick(trade_id, ts=ts))},
        separators=(",", ":"),
    )


def test_parse_journal_record_line_returns_none_for_blank():
    assert parse_journal_record_line("\n") is None
    assert parse_journal_record_line("   \n") is None


def test_parse_journal_record_line_valid_record():
    line = _record_line(3, "ok", ts=1100) + "\n"
    parsed = parse_journal_record_line(line)
    assert parsed is not None
    assert not isinstance(parsed, JournalRecordParseError)
    seq, tick = parsed
    assert seq == 3
    assert tick.trade_id == "ok"


def test_parse_journal_record_line_json_decode_error():
    result = parse_journal_record_line("{not-valid-json\n")
    assert isinstance(result, JournalRecordParseError)
    assert "not-valid-json" in result.line


def test_parse_journal_record_line_non_object():
    result = parse_journal_record_line("123\n")
    assert isinstance(result, JournalRecordParseError)
    assert result.reason == "record is not a JSON object"


def test_parse_journal_record_line_bad_tick_shape():
    result = parse_journal_record_line('{"seq":"x","tick":{}}\n')
    assert isinstance(result, JournalRecordParseError)


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
    second_batch = reader.poll(3)
    assert second_batch == []
    assert sum(1 for r in caplog.records if "skipped invalid" in r.message.lower()) >= 1
