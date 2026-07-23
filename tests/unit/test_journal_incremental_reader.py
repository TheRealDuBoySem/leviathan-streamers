"""Unit tests for JournalIncrementalReader basic behaviour."""

import json
import os

import pytest

from core.journal.journal_incremental_reader import JournalIncrementalReader
from core.journal.tick_journal import TickJournal
from core.journal.tick_journal_codec import tick_to_dict
from leviathan_common.models.trade_tick import TradeTick


def _tick(trade_id: str, ts: int = 1000) -> TradeTick:
    return TradeTick("BTCUSDT", ts, 100.0, 1.0, "buy", trade_id)


def test_journal_incremental_reader_skips_blank_and_stale_lines(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("keep"))
    with open(journal.journal_path, "a", encoding="utf-8") as handle:
        handle.write("\n")
        stale = {"seq": 0, "tick": tick_to_dict(_tick("stale"))}
        handle.write(json.dumps(stale) + "\n")
    reader = JournalIncrementalReader(journal)
    records = reader.poll(1)
    assert len(records) == 1
    assert records[0][1].trade_id == "keep"


def test_journal_incremental_reader_missing_journal_returns_empty(tmp_path):
    journal = TickJournal(str(tmp_path))
    if os.path.exists(journal.journal_path):
        os.remove(journal.journal_path)
    reader = JournalIncrementalReader(journal)
    assert reader.poll(1) == []


def test_journal_incremental_reader_reset_from_seq_validation(tmp_path):
    journal = TickJournal(str(tmp_path))
    reader = journal.create_incremental_reader()
    with pytest.raises(ValueError, match="start_seq must be a non-negative integer"):
        reader.reset_from_seq(-1)


def test_incremental_reader_does_not_rescan_whole_file(tmp_path):
    journal = TickJournal(str(tmp_path))
    for index in range(20):
        journal.append(_tick(f"t{index}", ts=1000 + index))
    reader = journal.create_incremental_reader()
    first_batch = reader.poll(1)
    assert len(first_batch) == 20
    journal.append(_tick("t20", ts=1020))
    second_batch = reader.poll(21)
    assert len(second_batch) == 1
    assert second_batch[0][1].trade_id == "t20"
