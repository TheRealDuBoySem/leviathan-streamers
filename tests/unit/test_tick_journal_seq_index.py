"""Unit tests for TickJournalSeqIndex."""

import json
import os

import pytest

from core.journal.journal_io import atomic_write_json
from core.journal.tick_journal import TickJournal
from core.journal.tick_journal_codec import tick_to_dict
from core.journal.tick_journal_meta import TickJournalMetaStore
from core.journal.tick_journal_seq_index import TickJournalSeqIndex
from leviathan_common.models.trade_tick import TradeTick


def _tick(trade_id: str, ts: int = 1000) -> TradeTick:
    return TradeTick("BTCUSDT", ts, 100.0, 1.0, "buy", trade_id)


def _record_line(seq: int, trade_id: str, ts: int = 1000) -> str:
    return json.dumps(
        {"seq": seq, "tick": tick_to_dict(_tick(trade_id, ts=ts))},
        separators=(",", ":"),
    )


def _seed_meta(tmp_path, *, latest_seq: int = 0, seq_index=None) -> None:
    atomic_write_json(
        str(tmp_path / "tick_journal.meta.json"),
        {
            "latest_seq": latest_seq,
            "seen_trade_ids": {},
            "seq_index": seq_index if seq_index is not None else [[0, 0]],
        },
    )


def test_seq_index_rejects_invalid_interval(tmp_path):
    store = TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=10)
    with pytest.raises(ValueError, match="seq_index_interval must be positive"):
        TickJournalSeqIndex(
            journal_path=str(tmp_path / "j.jsonl"),
            meta_store=store,
            seq_index_interval=0,
        )


def test_seq_index_byte_offset_for_seq_uses_index(tmp_path):
    journal = TickJournal(str(tmp_path), seq_index_interval=1)
    for index in range(3):
        journal.append(_tick(f"t{index}", ts=1000 + index))
    journal.flush_meta()
    assert journal.byte_offset_for_seq(2) >= 0


def test_seq_index_byte_offset_rejects_invalid_start_seq_type(tmp_path):
    store = TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=10)
    index = TickJournalSeqIndex(
        journal_path=str(tmp_path / "j.jsonl"),
        meta_store=store,
        seq_index_interval=1,
    )
    with pytest.raises(ValueError, match="start_seq must be a non-negative integer"):
        index.byte_offset_for_seq("bad")  # type: ignore[arg-type]
    assert index.byte_offset_for_seq(1) == 0
    assert index.byte_offset_for_seq(0) == 0


def test_seq_index_record_handles_empty_index(tmp_path):
    _seed_meta(tmp_path, seq_index=[])
    journal = TickJournal(str(tmp_path), seq_index_interval=1)
    journal.append(_tick("indexed"))
    journal.flush_meta()
    with open(tmp_path / "tick_journal.meta.json", "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    assert payload["seq_index"]


def test_seq_index_byte_offset_handles_malformed_index(tmp_path):
    _seed_meta(
        tmp_path,
        seq_index=[[], "bad", [2, 10], [5, 25]],
    )
    journal = TickJournal(str(tmp_path))
    assert journal.byte_offset_for_seq(4) == 10


def test_seq_index_truncates_after_many_appends(tmp_path):
    journal = TickJournal(str(tmp_path), seq_index_interval=1)
    for index in range(300):
        journal.append(_tick(f"t{index}", ts=1000 + index))
    journal.flush_meta()
    with open(tmp_path / "tick_journal.meta.json", "r", encoding="utf-8") as handle:
        index = json.load(handle)["seq_index"]
    assert len(index) <= 256


def test_seq_index_resolve_handles_blank_and_partial_trailing_line(tmp_path):
    store = TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=10)
    journal_path = str(tmp_path / "j.jsonl")
    index = TickJournalSeqIndex(
        journal_path=journal_path,
        meta_store=store,
        seq_index_interval=100,
    )
    line1 = (_record_line(1, "a") + "\n").encode("utf-8")
    blank = b"\n"
    partial = _record_line(2, "partial")[:30].encode("utf-8")
    with open(journal_path, "wb") as handle:
        handle.write(line1)
        handle.write(blank)
        handle.write(partial)
    offset = index.resolve(2, 0)
    assert offset == len(line1) + len(blank)


def test_seq_index_resolve_falls_back_on_open_oserror(tmp_path, monkeypatch):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("t1"))
    store = TickJournalMetaStore(str(tmp_path / "tick_journal.meta.json"), dedup_window=10_000)
    index = TickJournalSeqIndex(
        journal_path=journal.journal_path,
        meta_store=store,
        seq_index_interval=100,
    )

    import builtins

    real_open = builtins.open

    def _boom(path, *args, **kwargs):
        mode = args[0] if args else kwargs.get("mode", "r")
        if str(path) == journal.journal_path and mode == "rb":
            raise OSError("open failed")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", _boom)
    assert index.resolve(1, 42) == 42


def test_tick_journal_reload_seq_index_from_disk_delegates(tmp_path, mocker):
    journal = TickJournal(str(tmp_path))
    journal.append(_tick("a"))
    mocker.patch.object(
        TickJournalMetaStore,
        "load_payload",
        side_effect=OSError("meta missing"),
    )
    journal.reload_seq_index_from_disk()


def test_seq_index_rejects_blank_journal_path(tmp_path):
    store = TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=10)
    with pytest.raises(ValueError, match="journal_path must be a non-empty string"):
        TickJournalSeqIndex(journal_path="  ", meta_store=store)


def test_seq_index_rejects_invalid_meta_store(tmp_path):
    with pytest.raises(TypeError, match="meta_store must be a TickJournalMetaStore"):
        TickJournalSeqIndex(
            journal_path=str(tmp_path / "j.jsonl"),
            meta_store=object(),  # type: ignore[arg-type]
        )


def test_seq_index_record_rejects_invalid_args(tmp_path):
    store = TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=10)
    index = TickJournalSeqIndex(
        journal_path=str(tmp_path / "j.jsonl"),
        meta_store=store,
        seq_index_interval=1,
    )
    with pytest.raises(ValueError, match="seq must be a non-negative integer"):
        index.record(-1, 0)
    with pytest.raises(ValueError, match="byte_offset must be a non-negative integer"):
        index.record(1, -1)


def test_seq_index_reload_from_disk_without_thread_lock(tmp_path):
    store = TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=10)
    store.replace_seq_index([[0, 0], [5, 50]])
    store.persist()
    index = TickJournalSeqIndex(
        journal_path=str(tmp_path / "j.jsonl"),
        meta_store=store,
        thread_lock=None,
    )
    store.replace_seq_index([[0, 0]])
    index.reload_from_disk()
    assert store.seq_index() == [[0, 0], [5, 50]]


def test_seq_index_replace_and_trim_rejects_non_list(tmp_path):
    store = TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=10)
    index = TickJournalSeqIndex(
        journal_path=str(tmp_path / "j.jsonl"),
        meta_store=store,
    )
    with pytest.raises(TypeError, match="index must be a list"):
        index.replace_and_trim("bad")  # type: ignore[arg-type]
