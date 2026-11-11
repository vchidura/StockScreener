"""Fault records for the fault journal (G102 R21, Nov 11).

Every service reports a fault it absorbed or could not handle as one line on stderr: ``FAULT {json}``. One collector
(``runner/fault_collector.py`` on the host today; the platform's log shipping on servers later) reads every container's
log and is the only writer of the journal files, so 23 services never append to one shared file. A record carries what an
operator needs to address it offline: the service, what happened, whether it was recovered and how, and bounded
context. Emitting never raises and never blocks the caller.

Severities:
- FATAL: the process is about to exit, or a stage failed with no recovery in this process.
- ERROR: a unit of work failed (a window, a batch) and was not recovered.
- DEGRADED: work went on with something left out or labelled (INV-7/INV-8: an item rejected, a lane not evaluated).
- RECOVERED: a transient error was retried, or a bad item was isolated, and the work completed.
- WOULD_ACT: the self-heal check found something it could fix but runs record-only (the user's Nov 11 rule: record-only
  for the first sessions, then act). The record names the action it would have taken.
- ACTED: the self-heal check changed something (only actions the user has switched on).

FUTURE (servers): keep this format; ship stderr with the platform's log agent and point alerts at FATAL and at
fingerprints whose count grows. A database fault table is optional then; it is not used now (no migration).
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone

FAULT_PREFIX = "FAULT "
FAULT_SCHEMA = "realtime-fault-v1"
SEVERITIES = ("FATAL", "ERROR", "DEGRADED", "RECOVERED", "WOULD_ACT", "ACTED")
MESSAGE_LIMIT = 500
TRACEBACK_LIMIT = 4000
CONTEXT_VALUE_LIMIT = 300

_REDACTIONS = (
    (re.compile(r"(postgres(?:ql)?://)[^\s'\"]+", re.IGNORECASE), r"\1***"),
    (re.compile(r"\b(password|passwd|pwd|secret|token|api[_-]?key|apikey)\b(\s*[=:]\s*)[^\s&'\",;]+", re.IGNORECASE),
     r"\1\2***"),
    (re.compile(r"(Authorization:\s*Bearer\s+)\S+", re.IGNORECASE), r"\1***"),
    (re.compile(r"([?&](?:apiKey|api_key|token)=)[^&\s'\"]+", re.IGNORECASE), r"\1***"),
)


def redact(text: object) -> str:
    """Connection strings, keys and tokens out of a message or traceback (the repository and the journal are not
    secret stores). Hashes and identifiers stay: they are what an operator searches for."""
    value = "" if text is None else str(text)
    for pattern, replacement in _REDACTIONS:
        value = pattern.sub(replacement, value)
    return value


def fingerprint(service: str, kind: str, key: str = "") -> str:
    """Stable identity of a fault so repeats fold into one journal entry with a count (a crash loop is one entry)."""
    normalized = re.sub(r"\d+", "#", f"{service}|{kind}|{key}")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def _bounded(value, limit=CONTEXT_VALUE_LIMIT):
    if isinstance(value, (bool, int, float)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(key)[:80]: _bounded(item, limit) for key, item in list(value.items())[:30]}
    if isinstance(value, (list, tuple, set)):
        return [_bounded(item, limit) for item in list(value)[:30]]
    return redact(value)[:limit]


def fault_record(
    *,
    kind: str,
    severity: str,
    message: str,
    recoverable: bool,
    action: str = "NONE",
    service: str | None = None,
    key: str = "",
    context: dict | None = None,
    traceback_text: str | None = None,
    occurred_at: datetime | None = None,
) -> dict:
    severity = str(severity).upper()
    if severity not in SEVERITIES:
        severity = "ERROR"
    name = service or os.environ.get("REALTIME_FAULT_SERVICE") or os.environ.get("HOSTNAME") or "unknown"
    at = (occurred_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    record = {
        "schema": FAULT_SCHEMA,
        "occurred_at": at.isoformat(),
        "service": str(name)[:120],
        "kind": str(kind).upper()[:60],
        "severity": severity,
        "recoverable": bool(recoverable),
        "action": str(action).upper()[:60],
        "message": redact(message)[:MESSAGE_LIMIT],
        "fingerprint": fingerprint(str(name), str(kind).upper(), key or str(message)[:120]),
        "context": _bounded(context or {}),
    }
    if traceback_text:
        record["traceback"] = redact(traceback_text)[-TRACEBACK_LIMIT:]
    return record


def emit_fault(**kwargs) -> dict | None:
    """Write one ``FAULT {json}`` line to stderr. Never raises: a fault report must not become a second fault."""
    try:
        record = fault_record(**kwargs)
        sys.stderr.write(FAULT_PREFIX + json.dumps(record, sort_keys=True, default=str) + "\n")
        sys.stderr.flush()
        return record
    except Exception:  # noqa: BLE001
        return None


def parse_fault_line(line: str) -> dict | None:
    """The record in a log line (Docker may prefix a timestamp), or None."""
    index = line.find(FAULT_PREFIX + "{")
    if index < 0:
        return None
    try:
        record = json.loads(line[index + len(FAULT_PREFIX):])
    except (TypeError, ValueError):
        return None
    if not isinstance(record, dict) or record.get("schema") != FAULT_SCHEMA:
        return None
    return record
