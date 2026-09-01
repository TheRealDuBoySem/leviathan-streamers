"""
Read-progress observability for incremental journal tail-follow.

Pattern: Pure function — lag/tip metrics from known cursor and disk watermarks.
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
