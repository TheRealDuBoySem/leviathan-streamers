import os

import pytest

from leviathan_common.models.trade_tick import TradeTick
from core.journal.tick_journal import TickJournal, TickJournalCursor, tick_from_dict


def _tick(trade_id: str, ts: int = 1000) -> TradeTick:
    return TradeTick(
        inst_id="BTCUSDT",
        ts=ts,
        price=100.0,
        size=1.0,
        side="buy",
        trade_id=trade_id,
    )


def test_tick_journal_read_latest_seq_from_disk_sees_other_process_appends(tmp_path):
    writer = TickJournal(str(tmp_path))
    reader = TickJournal(str(tmp_path))
    assert reader.read_latest_seq_from_disk() == 0
    writer.append(_tick("t1"))
    writer.flush_meta()
    assert reader.latest_seq() == 0
    assert reader.read_latest_seq_from_disk() == 1


def test_tick_journal_append_and_replay(tmp_path):
    journal = TickJournal(str(tmp_path))
    seq1 = journal.append(_tick("t1"))
    seq2 = journal.append(_tick("t2", ts=1100))
    assert seq1 == 1
    assert seq2 == 2
    assert journal.latest_seq() == 2

    replay = list(journal.tail_from(1))
    assert len(replay) == 2
    assert replay[0][0] == 1
    assert replay[1][1].trade_id == "t2"


def test_tick_journal_deduplicates_trade_id(tmp_path):
    journal = TickJournal(str(tmp_path))
    first = journal.append(_tick("dup"))
    second = journal.append(_tick("dup", ts=1200))
    assert first == 1
    assert second == 1
    assert journal.latest_seq() == 1


def test_tick_journal_cursor_round_trip(tmp_path):
    journal = TickJournal(str(tmp_path))
    cursor = TickJournalCursor(last_processed_seq=7)
    journal.save_cursor(cursor)
    loaded = journal.load_cursor()
    assert loaded.last_processed_seq == 7


def test_tick_journal_strips_checkpoint_dir(tmp_path):
    journal = TickJournal(f"  {tmp_path}  ")
    assert journal.journal_path == os.path.join(str(tmp_path), "tick_journal.jsonl")


def test_tick_from_dict_rejects_missing_field():
    with pytest.raises(ValueError, match="missing required field 'trade_id'"):
        tick_from_dict({"inst_id": "BTCUSDT", "ts": 1, "price": 1.0, "size": 1.0, "side": "buy"})


def test_tick_journal_load_cursor_rejects_invalid_json(tmp_path):
    journal = TickJournal(str(tmp_path))
    with open(journal.cursor_path, "w", encoding="utf-8") as handle:
        handle.write("{not-json")
    with pytest.raises(ValueError, match="not valid JSON"):
        journal.load_cursor()


def test_tick_journal_constructor_rejects_invalid_dedup_window(tmp_path):
    with pytest.raises(ValueError, match="dedup_window must be positive"):
        TickJournal(str(tmp_path), dedup_window=0)


def test_tick_journal_tail_from_rejects_negative_start_seq(tmp_path):
    journal = TickJournal(str(tmp_path))
    with pytest.raises(ValueError, match="start_seq must be a non-negative integer"):
        list(journal.tail_from(-1))


def test_tick_journal_read_latest_seq_from_disk_falls_back_on_corrupt_meta(tmp_path, mocker):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("t1"))
    mocker.patch.object(journal, "_TickJournal__load_meta", side_effect=OSError("bad meta"))
    assert journal.read_latest_seq_from_disk() == journal.latest_seq()


def test_tick_journal_append_supervisor_handoff_pulse(tmp_path):
    journal = TickJournal(str(tmp_path))
    seq = journal.append_supervisor_handoff_pulse("btcusdt")
    assert seq == 1
    replay = list(journal.tail_from(1))
    assert replay[0][1].inst_id == "BTCUSDT"
    assert replay[0][1].trade_id.startswith("LEV-HANDOFF-")


def test_tick_journal_append_supervisor_handoff_pulse_rejects_empty_symbol(tmp_path):
    journal = TickJournal(str(tmp_path))
    with pytest.raises(ValueError, match="symbol must be a non-empty string"):
        journal.append_supervisor_handoff_pulse("   ")
