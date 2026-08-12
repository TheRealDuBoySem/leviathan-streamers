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


def test_meta_store_read_latest_seq_rejects_phantom_disk_rewind(tmp_path):
    """
    BB-D23-02: after observing a high disk tip, a stale/boot-era tip must not
    surface (J24 phantom latest=1790634 while live ~1810k).
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": 1_810_634, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == 1_810_634

    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": 1_790_634, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    assert store.read_latest_seq_from_disk() == 1_810_634


def test_j25_read_latest_seq_rejects_phantom_1810648(tmp_path):
    """
    J25 WATCH / BB-D23-02: soft-stale dumps showed sticky latest_seq=1810648
    while live tip was ~1.85M–1.91M (H04–H06, H15, H17–H20, H22). After a
    coherent ~1.9M disk observation, raw 1810648 must not surface as tip.
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    high_water = 1_900_000
    phantom = 1_810_648
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": high_water, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == high_water

    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": phantom, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    assert store.read_latest_seq_from_disk() == high_water
    assert store.read_latest_seq_from_disk() != phantom


def test_j25_reload_from_disk_rejects_phantom_1810648(tmp_path):
    """
    J25: reload_meta must not ingest a phantom tip rewind into in-memory
    latest_seq (bypass of read_latest_seq_from_disk high-water).
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    high_water = 1_900_000
    phantom = 1_810_648
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": high_water, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == high_water

    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "latest_seq": phantom,
                "seen_trade_ids": {"BTCUSDT": ["t1"]},
                "seq_index": [[0, 0], [phantom, 99]],
            },
            handle,
        )
    store.reload_from_disk()
    assert store.latest_seq() == high_water
    assert store.read_latest_seq_from_disk() == high_water
    # Dedup / seq_index still come from disk (tip alone is clamped).
    assert store.get_or_create_bucket("BTCUSDT").contains("t1")
    assert store.seq_index() == [[0, 0], [phantom, 99]]


def test_j25_reload_from_disk_clamps_negative_latest_seq_to_zero(tmp_path):
    """reload_from_disk: negative disk tip is normalized to 0 before high-water clamp."""
    meta_path = tmp_path / "tick_journal.meta.json"
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": 0, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)

    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": -5, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store.reload_from_disk()
    assert store.latest_seq() == 0


def test_j26_h12_soft_stale_rejects_phantom_tip_1810648(tmp_path):
    """
    F-J26-03 / J26 H12 @12:42: soft-stale logged latest_seq=1810648 (phantom)
    while cursor_seq=1975156 after coherent tips ~1974328 / 1974528.

    Soft-stale diagnostics read tip via read_latest_seq_from_disk (and may
    reload_meta). After observing the pre-rewind high-water, neither path may
    surface the phantom tip.
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    # Last coherent soft-stale tip before the H12 phantom (12:28 latest=1974528).
    high_water = 1_974_528
    phantom = 1_810_648
    cursor_seq = 1_975_156  # H12 soft-stale cursor; must stay above tip
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": high_water, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == high_water
    assert cursor_seq > high_water

    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "latest_seq": phantom,
                "seen_trade_ids": {},
                "seq_index": [[0, 0], [phantom, 42]],
            },
            handle,
        )
    # Soft-stale tip probe must not echo the rewind fantôme.
    assert store.read_latest_seq_from_disk() == high_water
    assert store.read_latest_seq_from_disk() != phantom

    store.reload_from_disk()
    assert store.latest_seq() == high_water
    assert store.read_latest_seq_from_disk() == high_water
    assert store.seq_index() == [[0, 0], [phantom, 42]]
    # Cursor overhang vs clamped tip remains tip-split sticky, not a rewind.
    assert cursor_seq > store.latest_seq()


def test_j27_h02_soft_stale_rejects_phantom_tip_1810648(tmp_path):
    """
    F-J27-03 / J27 H02 @02:54:23: soft-stale logged latest_seq=1810648 (phantom)
    while cursor_seq=2034155 after coherent soft-stale #1 tip=2028328 (@02:07).

    v0.18.35 high-water clamp (J25/J26) already closes the report path; this
    locks the J27 H02 numbers so a future clamp regression fails loudly.
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    # Soft-stale #1 @02:07:47 coherent tip before the @02:54 fantôme.
    high_water = 2_028_328
    phantom = 1_810_648
    cursor_seq = 2_034_155
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": high_water, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == high_water
    assert cursor_seq > high_water

    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "latest_seq": phantom,
                "seen_trade_ids": {},
                "seq_index": [[0, 0], [phantom, 54]],
            },
            handle,
        )
    assert store.read_latest_seq_from_disk() == high_water
    assert store.read_latest_seq_from_disk() != phantom

    store.reload_from_disk()
    assert store.latest_seq() == high_water
    assert store.read_latest_seq_from_disk() == high_water
    assert store.seq_index() == [[0, 0], [phantom, 54]]
    assert cursor_seq > store.latest_seq()


def test_j27_h23_soft_stale_rejects_phantom_tip_1810648(tmp_path):
    """
    F-J27-03 / J27 H23 @23:38:49: soft-stale logged latest_seq=1810648 (phantom)
    while cursor_seq=2131704 after coherent tips ~2131578 (idle/soft-stale window).

    Same clamp path as H02; distinct high-water / cursor lock the end-of-day
    recurrence so both J27 evidence points stay covered.
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    # Last coherent tip before the H23 fantôme (approaching-idle @23:26 tip=2131578).
    high_water = 2_131_578
    phantom = 1_810_648
    cursor_seq = 2_131_704
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": high_water, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == high_water
    assert cursor_seq > high_water

    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "latest_seq": phantom,
                "seen_trade_ids": {},
                "seq_index": [[0, 0], [phantom, 23]],
            },
            handle,
        )
    assert store.read_latest_seq_from_disk() == high_water
    assert store.read_latest_seq_from_disk() != phantom

    store.reload_from_disk()
    assert store.latest_seq() == high_water
    assert store.read_latest_seq_from_disk() == high_water
    assert store.seq_index() == [[0, 0], [phantom, 23]]
    assert cursor_seq > store.latest_seq()


def test_j28_h03_soft_stale_rejects_phantom_tip_1810648(tmp_path):
    """
    F-J28-03 / J28 H03 @03:23:02: soft-stale logged latest_seq=1810648 (phantom)
    while cursor_seq=2138203 after coherent soft-stale @03:22 tip=2138178.

    v0.18.36 high-water clamp (J25–J27) already closes the report path; this
    locks the J28 H03 numbers (ABSENT on J28 H02 — ≠ J27 H02) so a future
    clamp regression fails loudly on the daytime recurrence window.
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    # Soft-stale @03:22:22 coherent tip before the @03:23 fantôme.
    high_water = 2_138_178
    phantom = 1_810_648
    cursor_seq = 2_138_203
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": high_water, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == high_water
    assert cursor_seq > high_water

    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "latest_seq": phantom,
                "seen_trade_ids": {},
                "seq_index": [[0, 0], [phantom, 3]],
            },
            handle,
        )
    assert store.read_latest_seq_from_disk() == high_water
    assert store.read_latest_seq_from_disk() != phantom

    store.reload_from_disk()
    assert store.latest_seq() == high_water
    assert store.read_latest_seq_from_disk() == high_water
    assert store.seq_index() == [[0, 0], [phantom, 3]]
    assert cursor_seq > store.latest_seq()


def test_j28_h22_soft_stale_rejects_phantom_tip_1810648(tmp_path):
    """
    F-J28-03 / J28 H22 @22:22:52: soft-stale logged latest_seq=1810648 (phantom)
    while cursor_seq=2173206 after coherent soft-stale @22:18 tip=2173128;
    fantôme sticky ×3 (~65s) then catch-up tip=2173228 @22:27.

    Same clamp path as H03; distinct high-water / cursor lock the evening
    J28 evidence (ABSENT H23 on J28 — ≠ J27 H23).
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    # Soft-stale @22:18:47 coherent tip before the @22:22 fantôme.
    high_water = 2_173_128
    phantom = 1_810_648
    cursor_seq = 2_173_206
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": high_water, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == high_water
    assert cursor_seq > high_water

    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "latest_seq": phantom,
                "seen_trade_ids": {},
                "seq_index": [[0, 0], [phantom, 22]],
            },
            handle,
        )
    assert store.read_latest_seq_from_disk() == high_water
    assert store.read_latest_seq_from_disk() != phantom

    store.reload_from_disk()
    assert store.latest_seq() == high_water
    assert store.read_latest_seq_from_disk() == high_water
    assert store.seq_index() == [[0, 0], [phantom, 22]]
    assert cursor_seq > store.latest_seq()


def test_meta_store_read_latest_seq_fallback_keeps_disk_high_water(tmp_path, mocker):
    """
    Transient meta I/O must not fall back to a boot-era in-memory tip below the
    last coherent disk high-water (H11 stall precursor).
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": 1_810_647, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == 1_810_647
    # Simulate engine boot-era in-memory tip (deploy watermark).
    store.set_latest_seq(1_790_634)
    mocker.patch.object(
        TickJournalMetaStore,
        "load_payload",
        side_effect=OSError("race with collector persist"),
    )
    assert store.read_latest_seq_from_disk() == 1_810_647


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


def test_meta_store_read_latest_seq_clamps_negative_disk_tip(tmp_path):
    """BB-D23-02: negative latest_seq on disk is clamped to 0 before high-water."""
    meta_path = tmp_path / "tick_journal.meta.json"
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump({"latest_seq": -7, "seen_trade_ids": {}, "seq_index": []}, handle)
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == 0
