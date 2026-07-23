"""
Shared journal I/O helpers (atomic JSON writes, line preview, log rate-limit).

Pattern: Utility — pure helpers with no journal lifecycle state.
"""

from __future__ import annotations

import json
import os

_INVALID_LINE_PREVIEW_CHARS = 120


def should_log_invalid_line(skipped_count: int) -> bool:
    """Rate-limit invalid-line warnings while keeping a rising counter visible."""
    if skipped_count <= 3:
        return True
    if skipped_count in (10, 50, 100):
        return True
    return skipped_count % 500 == 0


def preview_journal_line(line: str) -> str:
    preview = line.replace("\n", "\\n")
    if len(preview) > _INVALID_LINE_PREVIEW_CHARS:
        return preview[:_INVALID_LINE_PREVIEW_CHARS] + "..."
    return preview


def atomic_write_json(path: str, payload: dict) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, separators=(",", ":"))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp_path, path)
