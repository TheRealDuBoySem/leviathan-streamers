"""Unit tests for journal_io helpers."""

import json
import os

from core.journal.journal_io import (
    atomic_write_json,
    preview_journal_line,
    should_log_invalid_line,
)


def test_should_log_invalid_line_rate_limit_avoids_flood():
    assert should_log_invalid_line(1) is True
    assert should_log_invalid_line(3) is True
    assert should_log_invalid_line(4) is False
    assert should_log_invalid_line(10) is True
    assert should_log_invalid_line(11) is False
    assert should_log_invalid_line(500) is True


def test_preview_journal_line_truncates_long_payload():
    long_line = "x" * 200
    preview = preview_journal_line(long_line)
    assert preview.endswith("...")
    assert len(preview) == 123


def test_preview_journal_line_escapes_newlines():
    assert preview_journal_line("a\nb") == "a\\nb"


def test_atomic_write_json_replaces_target(tmp_path):
    path = str(tmp_path / "meta.json")
    atomic_write_json(path, {"latest_seq": 3})
    with open(path, "r", encoding="utf-8") as handle:
        assert json.load(handle) == {"latest_seq": 3}
    assert not os.path.exists(f"{path}.tmp")
