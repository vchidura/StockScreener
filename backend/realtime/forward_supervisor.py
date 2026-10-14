from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping

from .contracts import canonical_sha256
from .forward_runtime import DeveloperForwardWindow


SUPERVISOR_VERSION = "developer_forward_supervisor_v1"
TERMINAL_WINDOW_STATES = frozenset({"COMPLETE", "MISSED", "FAILED"})


def window_key(window: DeveloperForwardWindow) -> str:
    return window.cutoff.astimezone(timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class DeveloperForwardWindowDecision:
    status: str
    missed: tuple[DeveloperForwardWindow, ...]
    due: DeveloperForwardWindow | None
    next_window: DeveloperForwardWindow | None


def decide_developer_forward_window(windows, states: Mapping[str, str], *, now: datetime):
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("supervisor clock must be timezone-aware")
    current = now.astimezone(timezone.utc)
    ordered = tuple(windows)
    if (not ordered or tuple(sorted(ordered, key=lambda row: row.cutoff)) != ordered
            or len({window_key(row) for row in ordered}) != len(ordered)):
        raise ValueError("supervisor requires unique ordered session windows")
    unknown = set(states.values()) - TERMINAL_WINDOW_STATES - {"RUNNING"}
    if unknown:
        raise ValueError("supervisor state contains an unknown window status")
    missed = []
    due = None
    next_window = None
    for window in ordered:
        key = window_key(window)
        if states.get(key) in TERMINAL_WINDOW_STATES:
            continue
        if states.get(key) == "RUNNING":
            return DeveloperForwardWindowDecision("BLOCKED_RUNNING_INSPECTION_REQUIRED", tuple(missed), None, window)
        if current >= window.deadline:
            missed.append(window)
            continue
        if window.available_after <= current < window.deadline:
            due = window
            break
        if current < window.available_after:
            next_window = window
            break
    if due is not None:
        status = "DUE"
    elif next_window is not None:
        status = "WAITING"
    elif len(states) + len(missed) >= len(ordered):
        status = "SESSION_COMPLETE"
    else:
        status = "IDLE"
    return DeveloperForwardWindowDecision(status, tuple(missed), due, next_window)


def supervisor_identity(**values) -> str:
    return canonical_sha256({"version": SUPERVISOR_VERSION, **values})


def validate_monitoring_acceptance(payload, *, cohort_id, configuration_sha256):
    expected = {
        "version", "status", "cohort_id", "configuration_sha256",
        "accepted_session", "approved_at", "manual_ui_data_review",
    }
    if (not isinstance(payload, dict) or set(payload) != expected
            or payload["version"] != "developer_forward_monitoring_acceptance_v1"
            or payload["status"] != "ACCEPTED"
            or payload["cohort_id"] != str(cohort_id)
            or payload["configuration_sha256"] != configuration_sha256
            or payload["manual_ui_data_review"] is not True):
        raise ValueError("continuous supervisor monitoring acceptance is absent or drifted")
    accepted = datetime.fromisoformat(payload["approved_at"])
    if accepted.tzinfo is None or accepted.utcoffset() is None:
        raise ValueError("continuous supervisor acceptance clock must be aware")
    return payload