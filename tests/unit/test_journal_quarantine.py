"""Unit tests for journal_quarantine helpers."""

import logging

import pytest

from core.journal.journal_quarantine import append_quarantine_line


def test_append_quarantine_line_writes_payload(tmp_path):
    path = str(tmp_path / "tick_journal.quarantine.jsonl")
    append_quarantine_line(path, '{"bad":1}', reason="poison")
    with open(path, "r", encoding="utf-8") as handle:
        content = handle.read()
    assert "poison" in content
    assert "bad" in content


def test_append_quarantine_line_rejects_blank_reason(tmp_path):
    path = str(tmp_path / "q.jsonl")
    with pytest.raises(ValueError, match="reason must be a non-empty string"):
        append_quarantine_line(path, "bad", reason="  ")


def test_append_quarantine_line_rejects_non_string_line(tmp_path):
    path = str(tmp_path / "q.jsonl")
    with pytest.raises(TypeError, match="line must be a string"):
        append_quarantine_line(path, 123, reason="bad")  # type: ignore[arg-type]


def test_append_quarantine_line_rejects_blank_path():
    with pytest.raises(ValueError, match="quarantine_path must be a non-empty string"):
        append_quarantine_line("  ", "{}", reason="x")


def test_append_quarantine_line_swallows_oserror(tmp_path, monkeypatch, caplog):
    path = str(tmp_path / "q.jsonl")
    import builtins

    real_open = builtins.open

    def _selective_open(open_path, *args, **kwargs):
        if str(open_path) == path:
            raise OSError("disk full")
        return real_open(open_path, *args, **kwargs)

    monkeypatch.setattr("builtins.open", _selective_open)
    with caplog.at_level(logging.WARNING):
        append_quarantine_line(path, "{}", reason="poison")
    assert any("Failed to quarantine" in record.message for record in caplog.records)
