"""
Read-progress observability for incremental journal tail-follow.

Pattern: Pure functions — lag/tip metrics from known cursor and disk watermarks,
plus classifiers that interpret a progress snapshot (EOF idle vs real lag vs
mid-append trailing bytes).
"""

from __future__ import annotations

# Align with TickJournal.META_PERSIST_INTERVAL — tip inflate past disk only within
# this window; larger overhang is sticky cursor (BB-D23-02).
TIP_META_LAG_TOLERANCE_SEQ = 50


def compute_read_progress_snapshot(
    *,
    read_offset: int,
    journal_size: int,
    next_seq: int,
    disk_latest_seq: int,
    incomplete_stuck: bool,
    meta_lag_tolerance_seq: int = TIP_META_LAG_TOLERANCE_SEQ,
) -> dict:
    """
    Compute offset/size/seq lag for cold-start observability (D4-04).

    ``latest_seq`` is ``max(disk meta, reader-observed floor)`` so a stale
    meta watermark (``META_PERSIST_INTERVAL``) cannot report
    ``next_seq >> latest_seq`` after the reader has already consumed those
    records from the journal file.

    ``lag_seq`` is how many journal seqs are at or beyond ``next_seq``
    according to that effective tip (0 when caught up). A sticky
    incomplete tip still bumps ``lag_seq`` to at least 1 so callers do
    not treat a torn line as idle EOF. Mere trailing bytes while the seq
    cursor is already past ``latest_seq`` (mid-append / not-yet-polled tip,
    J27 H07 / J32 H01–H02) must **not** invent ``lag_seq=1`` — that FP
    mis-labeled soft-stale as ``journal_lag`` and spuriously forced
    tail resync under v0.18.35.
    """
    disk_latest = int(disk_latest_seq)
    # Records already consumed imply tip >= next_seq - 1 even if meta lags —
    # but only within META_PERSIST lag. A checkpoint cursor past the live
    # disk tip (BB-D23-02 sticky watermark) must not invent a phantom tip.
    observed_floor = max(0, next_seq - 1)
    overhang = observed_floor - disk_latest
    cursor_ahead_of_tip = overhang > meta_lag_tolerance_seq
    if cursor_ahead_of_tip:
        latest_seq = disk_latest
    else:
        latest_seq = max(disk_latest, observed_floor)
    if latest_seq >= next_seq:
        lag_seq = latest_seq - next_seq + 1
    else:
        lag_seq = 0
    # J32 root: only incomplete tip invents lag when seq looks caught up.
    # Trailing unread bytes alone are owned by the poll / H07 gate.
    if lag_seq == 0 and incomplete_stuck:
        lag_seq = 1
    return {
        "read_offset": read_offset,
        "journal_size": journal_size,
        "next_seq": next_seq,
        "latest_seq": latest_seq,
        "lag_seq": lag_seq,
        "incomplete_stuck": incomplete_stuck,
        "cursor_ahead_of_tip": cursor_ahead_of_tip,
    }


def is_eof_caught_up_progress_snapshot(snapshot: dict) -> bool:
    """
    Return True when an empty poll is idle EOF wait, not unread lag (D5-07 / D6-A03).

    The D6 pre-restart storm logged WARNING while already showing
    ``offset==size``, ``lag_seq=0``, ``incomplete_stuck=False`` (sometimes with
    stale ``latest_seq < next_seq``). That signature must never be WARNING.
    """
    try:
        read_offset = int(snapshot["read_offset"])
        journal_size = int(snapshot["journal_size"])
        lag_seq = int(snapshot["lag_seq"])
        incomplete_stuck = bool(snapshot["incomplete_stuck"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(
            "progress snapshot must expose read_offset, journal_size, "
            f"lag_seq, incomplete_stuck as numeric/bool fields: {exc}"
        ) from exc
    return read_offset >= journal_size and lag_seq == 0 and not incomplete_stuck


def is_seq_caught_up_trailing_byte_lag_snapshot(snapshot: dict) -> bool:
    """
    Return True for J27 H07 / J32 H01–H02 false-positive soft-stale resync.

    Evidence H07 @07:54:20 (and J32 soft-stale): ``offset < size``,
    ``next_seq > latest_seq``, ``incomplete_stuck=False``. Historical logs may
    still show ``lag_seq=1`` from the pre-J32 artificial unread-byte bump;
    post-root, trailing mid-append keeps ``lag_seq=0``. Seq cursor is caught
    up; trailing bytes are a mid-append / not-yet-polled tip. Force-rebind here
    abandons an in-flight write and WARN-spams without healing real seq lag —
    tail-follow poll owns the incomplete-wait window instead.
    """
    try:
        read_offset = int(snapshot["read_offset"])
        journal_size = int(snapshot["journal_size"])
        next_seq = int(snapshot["next_seq"])
        latest_seq = int(snapshot["latest_seq"])
        incomplete_stuck = bool(snapshot["incomplete_stuck"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(
            "progress snapshot must expose read_offset, journal_size, "
            f"next_seq, latest_seq, incomplete_stuck as numeric/bool fields: {exc}"
        ) from exc
    return (
        not incomplete_stuck
        and read_offset < journal_size
        and next_seq > latest_seq
    )
