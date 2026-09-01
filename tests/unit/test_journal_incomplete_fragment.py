"""Unit tests for incomplete trailing journal fragment policy (D4-09)."""

from __future__ import annotations

import json
import logging

import pytest

from core.journal.journal_incomplete_fragment import (
    IncompleteTrailingFragmentPolicy,
    incomplete_trailing_skip_reason,
    is_in_progress_journal_fragment,
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


def test_is_in_progress_journal_fragment():
    assert is_in_progress_journal_fragment('{"seq":1')
    assert not is_in_progress_journal_fragment('id":"XRPUSDT"')
    assert not is_in_progress_journal_fragment("")


def test_incomplete_trailing_skip_reason():
    assert incomplete_trailing_skip_reason('{"partial"') == "incomplete_trailing_stale"
    assert incomplete_trailing_skip_reason('suffix"}}') == "incomplete_trailing_poison"


def test_policy_rejects_non_positive_max_wait():
    with pytest.raises(ValueError, match="incomplete_record_max_wait_seconds must be positive"):
        IncompleteTrailingFragmentPolicy(incomplete_record_max_wait_seconds=0)


def test_policy_rejects_non_callable_clock():
    with pytest.raises(TypeError, match="clock must be callable"):
        IncompleteTrailingFragmentPolicy(
            incomplete_record_max_wait_seconds=1.0,
            clock=123,  # type: ignore[arg-type]
        )


def test_should_skip_empty_fragment_is_false():
    policy = IncompleteTrailingFragmentPolicy(incomplete_record_max_wait_seconds=1.0)
    assert policy.should_skip_now(offset=0, fragment="") is False


def test_policy_skips_poison_suffix_immediately():
    poison = 'id":"XRPUSDT","ts":1,"price":1.0,"size":1.0,"side":"buy","trade_id":"t"}}'
    policy = IncompleteTrailingFragmentPolicy(incomplete_record_max_wait_seconds=2.0)
    assert policy.should_skip_now(offset=0, fragment=poison) is True
    assert policy.is_stuck is False


def test_policy_waits_then_skips_stale_json_object():
    partial = _record_line(1, "stuck")[:40]
    clock = {"now": 1000.0}
    policy = IncompleteTrailingFragmentPolicy(
        incomplete_record_max_wait_seconds=0.5,
        clock=lambda: clock["now"],
    )
    assert policy.should_skip_now(offset=10, fragment=partial) is False
    assert policy.is_stuck is True
    assert policy.should_skip_now(offset=10, fragment=partial) is False
    clock["now"] = 1000.6
    assert policy.should_skip_now(offset=10, fragment=partial) is True
    assert policy.is_stuck is True


def test_policy_clear_resets_stuck_state():
    partial = _record_line(1, "stuck")[:40]
    policy = IncompleteTrailingFragmentPolicy(incomplete_record_max_wait_seconds=2.0)
    assert policy.should_skip_now(offset=0, fragment=partial) is False
    assert policy.is_stuck is True
    policy.clear()
    assert policy.is_stuck is False


def test_reader_skips_incomplete_trailing_poison_suffix_immediately(tmp_path, caplog):
    """
    D4-09: torn journal suffixes that cannot be an in-progress JSON object
    (no leading '{') must be quarantined on the first poll — not parked until
    the next writer supplies a newline (observed ~33–56s startup delay).
    """
    journal = TickJournal(str(tmp_path))
    poison = (
        'id":"XRPUSDT","ts":1784147002135,"price":1.1125,'
        '"size":22.0,"side":"buy","trade_id":"1461383935646507016"}}'
    )
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(poison)

    reader = JournalIncrementalReader(journal)
    with caplog.at_level(logging.WARNING):
        assert reader.poll(1) == []

    assert reader.get_invalid_line_skip_count() == 1
    assert reader.get_read_offset() == len(poison.encode("utf-8"))
    assert any("incomplete_trailing_poison" in r.message for r in caplog.records)
    with open(journal.quarantine_path, "r", encoding="utf-8") as handle:
        quarantine = json.loads(handle.readline())
    assert quarantine["reason"] == "incomplete_trailing_poison"
    assert quarantine["line"] == poison


def test_reader_rejects_non_positive_incomplete_max_wait(tmp_path):
    journal = TickJournal(str(tmp_path))
    with pytest.raises(ValueError, match="incomplete_record_max_wait_seconds must be positive"):
        JournalIncrementalReader(journal, incomplete_record_max_wait_seconds=0)


def test_reader_rejects_non_callable_clock(tmp_path):
    journal = TickJournal(str(tmp_path))
    with pytest.raises(TypeError, match="clock must be callable"):
        JournalIncrementalReader(journal, clock=123)  # type: ignore[arg-type]


def test_reader_skips_stale_incomplete_json_object_after_max_wait(tmp_path):
    journal = TickJournal(str(tmp_path))
    partial = _record_line(1, "stuck")[:40]
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(partial)

    clock = {"now": 1000.0}
    reader = JournalIncrementalReader(
        journal,
        incomplete_record_max_wait_seconds=0.5,
        clock=lambda: clock["now"],
    )
    assert reader.poll(1) == []
    assert reader.get_invalid_line_skip_count() == 0

    clock["now"] = 1000.6
    assert reader.poll(1) == []
    assert reader.get_invalid_line_skip_count() == 1
    assert reader.get_read_offset() == len(partial.encode("utf-8"))


def test_abandon_incomplete_tip_force_false_skips_after_wait(tmp_path):
    """Cover force=False branch of abandon-incomplete-tip (D4-09)."""
    journal = TickJournal(str(tmp_path))
    complete = _record_line(1, "ok") + "\n"
    torn = '{"seq":2,"tick":{"inst_id":"BTCUSDT"'
    with open(journal.journal_path, "w", encoding="utf-8") as handle:
        handle.write(complete)
        handle.write(torn)

    with open(journal.journal_path, "r", encoding="utf-8") as handle:
        handle.readline()
        tip_offset = handle.tell()

    clock = {"t": 100.0}
    reader = JournalIncrementalReader(
        journal,
        incomplete_record_max_wait_seconds=1.0,
        clock=lambda: clock["t"],
    )
    reader._JournalIncrementalReader__read_offset = tip_offset
    reader._JournalIncrementalReader__abandon_incomplete_tip_at_cursor(force=False)
    assert reader.get_invalid_line_skip_count() == 0
    policy = reader._JournalIncrementalReader__incomplete_policy
    assert policy.is_stuck is True
    clock["t"] = 102.0
    reader._JournalIncrementalReader__abandon_incomplete_tip_at_cursor(force=False)
    assert reader.get_invalid_line_skip_count() == 1
    assert policy.is_stuck is False
