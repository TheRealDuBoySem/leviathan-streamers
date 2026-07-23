"""Additional TickJournal coverage."""

import json

import pytest

from core.journal.tick_journal import META_PERSIST_INTERVAL, TickJournal
from core.journal.tick_journal_cursor import TickJournalCursor
from leviathan_common.models.trade_tick import TradeTick


def _tick(trade_id: str, ts: int = 1000) -> TradeTick:
    return TradeTick("BTCUSDT", ts, 100.0, 1.0, "buy", trade_id)


def test_tick_journal_rejects_invalid_seq_index_interval(tmp_path):
    with pytest.raises(ValueError, match="seq_index_interval must be positive"):
        TickJournal(str(tmp_path), seq_index_interval=0)


def test_tick_journal_append_rejects_non_tick(tmp_path):
    journal = TickJournal(str(tmp_path))
    with pytest.raises(TypeError, match="tick must be a TradeTick"):
        journal.append({"bad": True})  # type: ignore[arg-type]


def test_tick_journal_save_cursor_rejects_invalid_type(tmp_path):
    journal = TickJournal(str(tmp_path))
    with pytest.raises(TypeError, match="TickJournalCursor"):
        journal.save_cursor({"bad": 1})  # type: ignore[arg-type]


def test_tick_journal_meta_invalid_root_raises(tmp_path):
    meta_path = tmp_path / "tick_journal.meta.json"
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump([1, 2, 3], handle)
    with pytest.raises(ValueError, match="meta must be a JSON object"):
        TickJournal(str(tmp_path))


def test_tick_journal_hydrate_ignores_invalid_seen_trade_ids(tmp_path):
    meta_path = tmp_path / "tick_journal.meta.json"
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump({"latest_seq": 0, "seen_trade_ids": "bad", "seq_index": [[0, 0]]}, handle)
    journal = TickJournal(str(tmp_path))
    assert journal.latest_seq() == 0


def test_tick_journal_dedup_bucket_eviction(tmp_path):
    journal = TickJournal(str(tmp_path), dedup_window=2)
    journal.append(_tick("a"))
    journal.append(_tick("b"))
    journal.append(_tick("c"))
    assert journal.latest_seq() == 3
    replay = {trade_id for _, tick in journal.tail_from(1) for trade_id in [tick.trade_id]}
    assert replay == {"a", "b", "c"}


def test_tick_journal_byte_offset_for_seq_uses_index(tmp_path):
    journal = TickJournal(str(tmp_path), seq_index_interval=1)
    for index in range(3):
        journal.append(_tick(f"t{index}", ts=1000 + index))
    journal.flush_meta()
    assert journal.byte_offset_for_seq(2) >= 0


def test_tick_journal_compact_before_seq(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("old"))
    journal.append(_tick("new", ts=1100))
    removed = journal.compact_before_seq(2)
    assert removed == 1
    replay = list(journal.tail_from(1))
    assert len(replay) == 1
    assert replay[0][1].trade_id == "new"


def test_tick_journal_compact_returns_zero_when_nothing_removed(tmp_path):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("only"))
    assert journal.compact_before_seq(1) == 0


def test_tick_journal_maybe_compact_runs(tmp_path):
    journal = TickJournal(str(tmp_path))
    for index in range(5):
        journal.append(_tick(f"t{index}", ts=1000 + index))
    journal.save_cursor(TickJournalCursor(last_processed_seq=5))
    assert journal.maybe_compact(lag_seq=1) >= 0


def test_tick_journal_rejects_blank_checkpoint_dir():
    with pytest.raises(ValueError, match="checkpoint_dir must be a non-empty string"):
        TickJournal("   ")


def test_tick_journal_hydrates_seen_trade_ids_lists(tmp_path):
    meta_path = tmp_path / "tick_journal.meta.json"
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": 0, "seen_trade_ids": {"BTCUSDT": ["a", "b"]}, "seq_index": [[0, 0]]},
            handle,
        )
    journal = TickJournal(str(tmp_path))
    assert journal.latest_seq() == 0


def test_tick_journal_byte_offset_rejects_invalid_start_seq_type(tmp_path):
    journal = TickJournal(str(tmp_path))
    with pytest.raises(ValueError, match="start_seq must be a non-negative integer"):
        journal.byte_offset_for_seq("bad")  # type: ignore[arg-type]
    journal = TickJournal(str(tmp_path))
    assert journal.byte_offset_for_seq(1) == 0
    assert journal.byte_offset_for_seq(0) == 0


def test_tick_journal_record_seq_index_handles_empty_index(tmp_path):
    meta_path = tmp_path / "tick_journal.meta.json"
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump({"latest_seq": 0, "seen_trade_ids": {}, "seq_index": []}, handle)
    journal = TickJournal(str(tmp_path), seq_index_interval=1)
    journal.append(_tick("indexed"))
    assert journal._TickJournal__meta["seq_index"]


def test_tick_journal_byte_offset_handles_malformed_index(tmp_path):
    meta_path = tmp_path / "tick_journal.meta.json"
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "latest_seq": 0,
                "seen_trade_ids": {},
                "seq_index": [[], "bad", [2, 10], [5, 25]],
            },
            handle,
        )
    journal = TickJournal(str(tmp_path))
    assert journal.byte_offset_for_seq(4) == 10


def test_tick_journal_auto_persists_meta_on_append_interval(tmp_path, mocker):
    journal = TickJournal(str(tmp_path), seq_index_interval=1)
    persist_mock = mocker.patch.object(journal, "_TickJournal__persist_meta")
    for index in range(META_PERSIST_INTERVAL):
        journal.append(_tick(f"t{index}", ts=1000 + index))
    persist_mock.assert_called()


def test_tick_journal_compact_returns_zero_when_journal_missing(tmp_path):
    journal = TickJournal(str(tmp_path))
    assert journal.compact_before_seq(2) == 0


def test_tick_journal_compact_skips_blank_lines_and_keeps_all_records(tmp_path):
    journal = TickJournal(str(tmp_path))
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write("\n\n")
    assert journal.compact_before_seq(2) == 0


def test_tick_journal_seq_index_truncates_after_many_appends(tmp_path):
    journal = TickJournal(str(tmp_path), seq_index_interval=1)
    for index in range(300):
        journal.append(_tick(f"t{index}", ts=1000 + index))
    index = journal._TickJournal__meta["seq_index"]
    assert len(index) <= 256
