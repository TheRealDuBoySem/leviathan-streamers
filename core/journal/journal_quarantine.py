"""
Forensic quarantine writer for rejected journal lines.

Pattern: Utility — append-only poison-line sink with no journal lifecycle state.
"""

from __future__ import annotations

import json
import logging
import time

logger = logging.getLogger(__name__)


def append_quarantine_line(quarantine_path: str, line: str, *, reason: str) -> None:
    """
    Append a rejected journal line for forensics without blocking the caller.

    Preconditions:
        - quarantine_path is a non-empty string.
        - line is a string.
        - reason is a non-empty string.
    """
    if not isinstance(quarantine_path, str) or not quarantine_path.strip():
        raise ValueError("quarantine_path must be a non-empty string")
    if not isinstance(line, str):
        raise TypeError("line must be a string")
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("reason must be a non-empty string")
    payload = {
        "ts_ms": int(time.time() * 1000),
        "reason": reason.strip(),
        "line": line,
    }
    encoded = json.dumps(payload, separators=(",", ":")) + "\n"
    try:
        with open(quarantine_path, "a", encoding="utf-8") as handle:
            handle.write(encoded)
            handle.flush()
    except OSError as exc:
        logger.warning(
            "Failed to quarantine invalid journal line (%s): %s",
            reason.strip(),
            exc,
        )
