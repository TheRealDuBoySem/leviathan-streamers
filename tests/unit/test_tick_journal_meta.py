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


def test_j29_h02_soft_stale_rejects_phantom_tip_1810648(tmp_path):
    """
    F-J29-05 / J29 H02 @02:36–02:38: soft-stale/approaching logged
    latest_seq=1810648 (phantom) ×3 while cursor_seq~2179706 after coherent
    soft-stale @02:34 tip=2179628; catch-up tip=2179728 @02:42.

    v0.18.36 high-water clamp (J25–J28) already closes the report path; this
    locks the J29 H02 numbers (présent H02 J29 — ABSENT H02 J28) so a future
    clamp regression fails loudly on the pre-respawn recurrence.
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    # Soft-stale @02:34:36 coherent tip before the @02:36 fantôme.
    high_water = 2_179_628
    phantom = 1_810_648
    cursor_seq = 2_179_706
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
                "seq_index": [[0, 0], [phantom, 2]],
            },
            handle,
        )
    assert store.read_latest_seq_from_disk() == high_water
    assert store.read_latest_seq_from_disk() != phantom

    store.reload_from_disk()
    assert store.latest_seq() == high_water
    assert store.read_latest_seq_from_disk() == high_water
    assert store.seq_index() == [[0, 0], [phantom, 2]]
    assert cursor_seq > store.latest_seq()


def test_j29_h04_soft_stale_rejects_phantom_tip_2182116(tmp_path):
    """
    F-J29-05 / J29 H04 @04:28:58: soft-stale logged latest_seq=2182116
    (post-respawn phantom = H03 heal tip baseline 2182115→2182116) while
    cursor_seq=2182642 after coherent tip=2182565 @04:27; catch-up
    tip=2182665 @04:30. Gap pic 526 — même classe que 1810648, seq ≠.

    Clamp high-water already covers any rewind below observed tip; this
    locks the first post-heal fantôme so a future clamp regression fails
    on the new sticky seq (not only the classic 1810648).
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    # Soft-stale / approaching window @04:27 coherent tip before fantôme.
    high_water = 2_182_565
    phantom = 2_182_116  # H03 heal latest after trivial delta=1
    cursor_seq = 2_182_642
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": high_water, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == high_water
    assert cursor_seq > high_water
    assert high_water > phantom

    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "latest_seq": phantom,
                "seen_trade_ids": {},
                "seq_index": [[0, 0], [phantom, 4]],
            },
            handle,
        )
    assert store.read_latest_seq_from_disk() == high_water
    assert store.read_latest_seq_from_disk() != phantom

    store.reload_from_disk()
    assert store.latest_seq() == high_water
    assert store.read_latest_seq_from_disk() == high_water
    assert store.seq_index() == [[0, 0], [phantom, 4]]
    assert cursor_seq > store.latest_seq()


def test_j29_h23_soft_stale_rejects_phantom_tip_2182116(tmp_path):
    """
    F-J29-05 / J29 H23 @23:12: soft-stale/approaching logged latest_seq=2182116
    (same post-respawn fantôme sticky multi-h) while cursor_seq=2216660 after
    coherent tip=2216315 @23:02; catch-up tip=2216815 @23:17.

    Distinct end-of-day high-water / cursor lock the multi-hour recurrence
    (H04→H23) so both first-apparition and late-day evidence stay covered.
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    # Soft-stale @23:02:50 coherent tip before the @23:12 fantôme.
    high_water = 2_216_315
    phantom = 2_182_116
    cursor_seq = 2_216_660
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": high_water, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == high_water
    assert cursor_seq > high_water
    assert high_water > phantom

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


def test_j30_h02_soft_stale_rejects_phantom_tip_2182116(tmp_path):
    """
    F-J30-03 / J30 H02 @02:54:48: soft-stale logged latest_seq=2182116
    (post-heal fantôme) while cursor_seq=2238113 after coherent soft-stale
    @02:35 tip=2236665; tip cohérent rétabli 2238215 @02:59.

    Contraste J29 H02 : tip rewind 1810648 ABSENT J30 ; même clamp path
    couvre la sticky seq post-respawn. Locks the J30 morning numbers.
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    high_water = 2_236_665
    phantom = 2_182_116
    cursor_seq = 2_238_113
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": high_water, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == high_water
    assert cursor_seq > high_water
    assert high_water > phantom

    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "latest_seq": phantom,
                "seen_trade_ids": {},
                "seq_index": [[0, 0], [phantom, 2]],
            },
            handle,
        )
    assert store.read_latest_seq_from_disk() == high_water
    assert store.read_latest_seq_from_disk() != phantom

    store.reload_from_disk()
    assert store.latest_seq() == high_water
    assert store.read_latest_seq_from_disk() == high_water
    assert store.seq_index() == [[0, 0], [phantom, 2]]
    assert cursor_seq > store.latest_seq()


def test_j30_h06_soft_stale_rejects_phantom_tip_2182116(tmp_path):
    """
    F-J30-03 / J30 H06 @06:24:20: soft-stale logged latest_seq=2182116
    while cursor_seq=2249611 (gap≈67k); catch-up tip=2249815 @06:31.

    No soft-stale tip in-hour before the fantôme — high-water locks the
    last coherent H05 soft tip=2247515 @05:47 (same process lifetime
    gen=3) so a future clamp regression fails on the H06 window.
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    # H05 @05:47:54 last coherent soft tip before H06 @06:24 fantôme.
    high_water = 2_247_515
    phantom = 2_182_116
    cursor_seq = 2_249_611
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": high_water, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == high_water
    assert cursor_seq > high_water
    assert high_water > phantom

    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "latest_seq": phantom,
                "seen_trade_ids": {},
                "seq_index": [[0, 0], [phantom, 6]],
            },
            handle,
        )
    assert store.read_latest_seq_from_disk() == high_water
    assert store.read_latest_seq_from_disk() != phantom

    store.reload_from_disk()
    assert store.latest_seq() == high_water
    assert store.read_latest_seq_from_disk() == high_water
    assert store.seq_index() == [[0, 0], [phantom, 6]]
    assert cursor_seq > store.latest_seq()


def test_j30_h19_soft_stale_rejects_phantom_tip_2182116(tmp_path):
    """
    F-J30-03 / J30 H19 @19:46:16: soft-stale logged latest_seq=2182116
    while cursor_seq=2297597 after coherent soft-stale @19:28 tip=2296715;
    tip disk redevient cohérent ~2297k post-recovery.

    Distinct evening trading-hour lock (D6-B03 same heure, unrelated).
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    high_water = 2_296_715
    phantom = 2_182_116
    cursor_seq = 2_297_597
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": high_water, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == high_water
    assert cursor_seq > high_water
    assert high_water > phantom

    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "latest_seq": phantom,
                "seen_trade_ids": {},
                "seq_index": [[0, 0], [phantom, 19]],
            },
            handle,
        )
    assert store.read_latest_seq_from_disk() == high_water
    assert store.read_latest_seq_from_disk() != phantom

    store.reload_from_disk()
    assert store.latest_seq() == high_water
    assert store.read_latest_seq_from_disk() == high_water
    assert store.seq_index() == [[0, 0], [phantom, 19]]
    assert cursor_seq > store.latest_seq()


def test_j30_h20_soft_stale_rejects_phantom_tip_2182116(tmp_path):
    """
    F-J30-03 / J30 H20 @20:36:27: soft-stale logged latest_seq=2182116
    while cursor_seq=2298589 after coherent soft tip plateau 2298515
    @20:31–33; soft-stale suivants revoient tip live 2298665+.
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    high_water = 2_298_515
    phantom = 2_182_116
    cursor_seq = 2_298_589
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": high_water, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == high_water
    assert cursor_seq > high_water
    assert high_water > phantom

    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "latest_seq": phantom,
                "seen_trade_ids": {},
                "seq_index": [[0, 0], [phantom, 20]],
            },
            handle,
        )
    assert store.read_latest_seq_from_disk() == high_water
    assert store.read_latest_seq_from_disk() != phantom

    store.reload_from_disk()
    assert store.latest_seq() == high_water
    assert store.read_latest_seq_from_disk() == high_water
    assert store.seq_index() == [[0, 0], [phantom, 20]]
    assert cursor_seq > store.latest_seq()


def test_j30_h21_soft_stale_rejects_phantom_tip_2182116(tmp_path):
    """
    F-J30-03 / J30 H21 @21:46:40: soft-stale logged latest_seq=2182116
    while cursor_seq=2300067 after coherent tip plateau 2299965
    @21:40–42; tip live rétabli 2300515 @21:53.
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    high_water = 2_299_965
    phantom = 2_182_116
    cursor_seq = 2_300_067
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": high_water, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == high_water
    assert cursor_seq > high_water
    assert high_water > phantom

    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "latest_seq": phantom,
                "seen_trade_ids": {},
                "seq_index": [[0, 0], [phantom, 21]],
            },
            handle,
        )
    assert store.read_latest_seq_from_disk() == high_water
    assert store.read_latest_seq_from_disk() != phantom

    store.reload_from_disk()
    assert store.latest_seq() == high_water
    assert store.read_latest_seq_from_disk() == high_water
    assert store.seq_index() == [[0, 0], [phantom, 21]]
    assert cursor_seq > store.latest_seq()


def test_j30_h23a_soft_stale_rejects_phantom_tip_2182116(tmp_path):
    """
    F-J30-03 / J30 H23A @23:35–23:36 (pré-deploy v0.18.32): soft-stale
    logged latest_seq=2182116 ×2 while cursor_seq=2313486 / 2313506 after
    coherent soft-stale @23:32 tip=2313415. Fantôme ABSENT Phase B
    post-v0.18.33 (~12 min early).

    Distinct end-of-day pre-stop lock so H23A evidence stays covered
    independently of J29 H23 (~2.216M).
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    high_water = 2_313_415
    phantom = 2_182_116
    cursor_seq = 2_313_486
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": high_water, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == high_water
    assert cursor_seq > high_water
    assert high_water > phantom

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


def test_j31_h15_soft_stale_rejects_phantom_tip_2319820(tmp_path):
    """
    F-J31-01 / J31 H15: Forced rewind sticky ``to_seq=2319820`` while live
    cursor ~2371k–2379k (pic ×7 under v0.18.35). Same high-water clamp class
    as ``1810648`` / ``2182116`` — lock the J31 sticky seq so a clamp
    regression fails loudly. Early J30 H23B tip-aligned window is invalidé.
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    high_water = 2_371_720
    phantom = 2_319_820
    cursor_seq = 2_371_814  # ahead≈94 vs coherent tip (H15 band)
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": high_water, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == high_water
    assert cursor_seq > high_water
    assert high_water > phantom

    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "latest_seq": phantom,
                "seen_trade_ids": {},
                "seq_index": [[0, 0], [phantom, 15]],
            },
            handle,
        )
    assert store.read_latest_seq_from_disk() == high_water
    assert store.read_latest_seq_from_disk() != phantom

    store.reload_from_disk()
    assert store.latest_seq() == high_water
    assert store.read_latest_seq_from_disk() == high_water
    assert store.seq_index() == [[0, 0], [phantom, 15]]
    assert cursor_seq > store.latest_seq()


def test_j31_h04_soft_stale_rejects_phantom_tip_2319820(tmp_path):
    """
    F-J31-01 / J31 H04: Forced rewind sticky ``2319820`` while live tip
    ~2321k–2322k (×3 clusters under v0.18.35). Distinct early-day lock from H15.
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    high_water = 2_321_720
    phantom = 2_319_820
    cursor_seq = 2_321_790
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
                "seq_index": [[0, 0], [phantom, 4]],
            },
            handle,
        )
    assert store.read_latest_seq_from_disk() == high_water
    assert store.read_latest_seq_from_disk() != phantom

    store.reload_from_disk()
    assert store.latest_seq() == high_water
    assert store.read_latest_seq_from_disk() == high_water
    assert store.seq_index() == [[0, 0], [phantom, 4]]


def test_j32_h13_persist_never_rewrites_phantom_tip_2319820(tmp_path):
    """
    F-J32-02 / J32 H13: once a coherent tip (~2.45M) is observed, persist must
    not rewrite sticky fantôme ``2319820`` onto disk (root of Forced→2319820).
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    coherent = 2_450_636  # H13 soft-stale aligned tip @13:20
    phantom = 2_319_820
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": coherent, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == coherent

    # Simulate a poisoned in-memory tip (reload race / stale writer) then persist.
    store.set_latest_seq(phantom)
    store.persist()
    reloaded = json.loads(meta_path.read_text(encoding="utf-8"))
    assert int(reloaded["latest_seq"]) == coherent
    assert int(reloaded["latest_seq"]) != phantom
    assert store.read_latest_seq_from_disk() == coherent


def test_j32_h23_persist_never_rewrites_phantom_tip_2454018(tmp_path):
    """
    F-J32-02 / J32 H22–H23: post-recovery sticky ``2454018`` must not be
    re-persisted once a higher coherent tip was observed.
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    coherent = 2_456_168
    phantom = 2_454_018
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": coherent, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == coherent

    store.set_latest_seq(phantom)
    store.persist()
    reloaded = json.loads(meta_path.read_text(encoding="utf-8"))
    assert int(reloaded["latest_seq"]) == coherent
    assert int(reloaded["latest_seq"]) != phantom


def test_j32_ensure_latest_seq_at_least_repairs_sticky_disk_tip(tmp_path):
    """
    F-J32-02: Forced/heal path must be able to bump disk tip past sticky
    fantômes so a fresh TickJournal cannot re-seed ``2319820`` / ``2454018``.
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    phantom = 2_454_018
    coherent = 2_456_168
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"latest_seq": phantom, "seen_trade_ids": {}, "seq_index": [[0, 0]]},
            handle,
        )
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert store.read_latest_seq_from_disk() == phantom

    assert store.ensure_latest_seq_at_least(coherent) is True
    store.persist()
    assert store.latest_seq() == coherent
    assert store.read_latest_seq_from_disk() == coherent
    fresh = TickJournalMetaStore(str(meta_path), dedup_window=10)
    assert fresh.read_latest_seq_from_disk() == coherent
    assert fresh.read_latest_seq_from_disk() != phantom
    assert store.ensure_latest_seq_at_least(coherent) is False
    assert store.ensure_latest_seq_at_least(phantom) is False


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


def test_max_seq_from_seq_index_skips_non_integer_entries():
    """J32: sparse seq_index may contain non-int heads — skip without raising."""
    from core.journal.tick_journal_meta import _max_seq_from_seq_index

    assert _max_seq_from_seq_index("not-a-list") == 0
    assert _max_seq_from_seq_index([]) == 0
    assert (
        _max_seq_from_seq_index(
            [["bad", 0], None, [], [None], ["12x", 1], [10, 99], [3, 0]]
        )
        == 10
    )


def test_j32_set_latest_seq_tolerates_negative_payload_current(tmp_path):
    """
    J32 BB-D23-02: poisoned negative latest_seq in payload must not break
    monotonic set_latest_seq (clamp current to 0 before max).
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump({"latest_seq": -7, "seen_trade_ids": {}, "seq_index": [[0, 0]]}, handle)
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    store.set_latest_seq(2_454_018)
    assert store.latest_seq() == 2_454_018


def test_j32_ensure_latest_seq_at_least_rejects_invalid(tmp_path):
    store = TickJournalMetaStore(str(tmp_path / "m.json"), dedup_window=10)
    with pytest.raises(ValueError, match="seq must be a non-negative integer"):
        store.ensure_latest_seq_at_least(-1)
    with pytest.raises(ValueError, match="seq must be a non-negative integer"):
        store.ensure_latest_seq_at_least("1")  # type: ignore[arg-type]


def test_j32_persist_clamps_negative_tip_defense(tmp_path):
    """
    J32 BB-D23-02 defensive: if both payload tip and high-water are negative,
    persist must rewrite tip to 0 (never leave a negative fantôme on disk).
    """
    meta_path = tmp_path / "tick_journal.meta.json"
    store = TickJournalMetaStore(str(meta_path), dedup_window=10)
    store._TickJournalMetaStore__payload["latest_seq"] = -3
    store._TickJournalMetaStore__disk_tip_high_water = -1
    store.persist()
    reloaded = json.loads(meta_path.read_text(encoding="utf-8"))
    assert int(reloaded["latest_seq"]) == 0
    assert store.latest_seq() == 0
