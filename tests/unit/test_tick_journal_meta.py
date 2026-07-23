"""Unit tests for TickJournalMetaStore."""

import json
import os

import pytest

from core.journal.tick_journal import META_PERSIST_INTERVAL, TickJournal
from core.journal.tick_journal_meta import TickJournalMetaStore
from leviathan_common.models.trade_tick import TradeTick


def _tick(trade_id: str, ts: int = 1000) -> TradeTick:
    return TradeTick("BTCUSDT", ts, 100.0, 1.0, "buy", trade_id)


def test_meta_store_rejects_blank_path():
    with pytest.raises(ValueError, match="meta_path must be a non-empty string"):
        TickJournalMetaStore("  ", dedup_window=10)


def test_meta_store_rejects_non_positive_dedup_window(tmp_path):
    with pytest.raises(ValueError, match="dedup_window must be positive"):
        TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=0)


def test_meta_store_invalid_root_raises(tmp_path):
    meta_path = tmp_path / "tick_journal.meta.json"
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump([1, 2, 3], handle)
    with pytest.raises(ValueError, match="meta must be a JSON object"):
        TickJournalMetaStore(str(meta_path), dedup_window=10)


def test_meta_store_hydrate_ignores_invalid_seen_trade_ids(tmp_path):
    meta_path = tmp_path / "tick_journal.meta.json"
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump({"latest_seq": 0, "seen_trade_ids": "bad", "seq_index": [[0, 0]]}, handle)
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.latest_seq() == 0


def test_meta_store_hydrates_seen_trade_ids_lists(tmp_path):
    meta_path = tmp_path / "tick_journal.meta.json"
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": 0, "seen_trade_ids": {"BTCUSDT": ["a", "b"]}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    bucket = store.get_or_create_bucket("btcusdt")
    assert bucket.contains("a")
    assert bucket.contains("b")


def test_meta_store_read_latest_seq_from_disk_falls_back_on_corrupt_meta(tmp_path, mocker):
    store = TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=10)
    store.set_latest_seq(3)
    mocker.patch.object(
        TickJournalMetaStore,
        "load_payload",
        side_effect=OSError("bad meta"),
    )
    assert store.read_latest_seq_from_disk() == 3


def test_meta_store_reload_seq_index_tolerates_bad_meta(tmp_path, mocker):
    store = TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=10)
    store.replace_seq_index([[0, 0], [1, 10]])
    mocker.patch.object(
        TickJournalMetaStore,
        "load_payload",
        side_effect=OSError("meta missing"),
    )
    store.reload_seq_index_from_disk()
    assert store.seq_index() == [[0, 0], [1, 10]]

    mocker.patch.object(
        TickJournalMetaStore,
        "load_payload",
        return_value={"seq_index": "not-a-list", "latest_seq": 1},
    )
    store.reload_seq_index_from_disk()
    assert store.seq_index() == [[0, 0], [1, 10]]


def test_meta_store_reload_seq_index_tolerates_json_decode_error(tmp_path, mocker):
    store = TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=10)
    mocker.patch.object(
        TickJournalMetaStore,
        "load_payload",
        side_effect=json.JSONDecodeError("bad", "doc", 0),
    )
    store.reload_seq_index_from_disk()


def test_tick_journal_auto_persists_meta_on_append_interval(tmp_path):
    journal = TickJournal(str(tmp_path), seq_index_interval=1)
    for index in range(META_PERSIST_INTERVAL):
        journal.append(_tick(f"t{index}", ts=1000 + index))
    assert journal.read_latest_seq_from_disk() == META_PERSIST_INTERVAL
    assert os.path.exists(tmp_path / "tick_journal.meta.json")


def test_tick_journal_reload_meta_from_disk(tmp_path):
    writer = TickJournal(str(tmp_path))
    writer.append(_tick("a"))
    writer.flush_meta()
    reader = TickJournal(str(tmp_path))
    assert reader.latest_seq() == 1
    writer.append(_tick("b"))
    writer.flush_meta()
    reader.reload_meta_from_disk()
    assert reader.latest_seq() == 2


def test_meta_store_exposes_path_and_dedup_window(tmp_path):
    path = str(tmp_path / "m.json")
    store = TickJournalMetaStore(path, dedup_window=12)
    assert store.meta_path == path
    assert store.dedup_window == 12


def test_meta_store_set_latest_seq_rejects_invalid(tmp_path):
    store = TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=10)
    with pytest.raises(ValueError, match="seq must be a non-negative integer"):
        store.set_latest_seq(-1)
    with pytest.raises(ValueError, match="seq must be a non-negative integer"):
        store.set_latest_seq("1")  # type: ignore[arg-type]


def test_meta_store_seq_index_recovers_non_list_payload(tmp_path):
    meta_path = tmp_path / "m.json"
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": 0, "seen_trade_ids": {}, "seq_index": "corrupt"},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.seq_index() == [[0, 0]]


def test_meta_store_replace_seq_index_rejects_non_list(tmp_path):
    store = TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=10)
    with pytest.raises(TypeError, match="seq_index must be a list"):
        store.replace_seq_index("bad")  # type: ignore[arg-type]


def test_meta_store_get_or_create_bucket_rejects_blank_symbol(tmp_path):
    store = TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=10)
    with pytest.raises(ValueError, match="symbol must be a non-empty string"):
        store.get_or_create_bucket("  ")
