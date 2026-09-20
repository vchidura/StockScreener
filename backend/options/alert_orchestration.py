from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import datetime
from typing import Mapping

from options.alert_plans import _canonical
from options.alert_qualification import retained_time
from options.repositories.alert_publications import publication_exposure_key


DRY_RUN_VERSION = "option_alert_dry_run_v2"


def dry_run_exposure_keys(rows: list[dict]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(
        publication_exposure_key(row["preview"]["plan_preview"]["plan"])
        for row in rows if row["status"] == "INDICATIVE_PLAN_VALID" and not row["blockers"]
    ))


def finalize_alert_dry_run(
    rows: list[dict], *, policy: Mapping[str, object], assessed_at: datetime,
    completed_at: datetime, expected_slot: datetime | None, final_slot: datetime | None,
    publication_state: Mapping[str, object],
) -> dict[str, object]:
    if not 1 <= len(rows) <= 20 or len({row["candidate_id"] for row in rows}) != len(rows):
        raise ValueError("dry-run requires 1-20 distinct candidates")
    assessed_at, completed_at = retained_time(assessed_at), retained_time(completed_at)
    if completed_at < assessed_at:
        raise ValueError("dry-run completion cannot precede assessment")
    detached = json.loads(_canonical(rows))
    states = {row["exposure_key"]: row for row in publication_state.get("rows", ())}
    seen: dict[str, str] = {}
    for row in detached:
        if row["status"] != "INDICATIVE_PLAN_VALID" or row["blockers"]:
            row["status"] = "NOT_APPLICABLE" if row["status"] == "NOT_APPLICABLE" else "BLOCKED"
            continue
        plan = row["preview"]["plan_preview"]["plan"]
        exposure_key = publication_exposure_key(plan)
        row["exposure_key"] = exposure_key
        state = states.get(exposure_key)
        row["publication_state"] = state
        if expected_slot is None or final_slot != expected_slot:
            row["blockers"].append({"code": "SOURCE_SLOT_CHANGED_DURING_DRY_RUN"})
        if retained_time(plan["entry_deadline"]) <= completed_at:
            row["blockers"].append({"code": "ENTRY_WINDOW_ELAPSED_DURING_DRY_RUN"})
        if not publication_state.get("available"):
            row["blockers"].append({"code": "PUBLICATION_STATE_UNAVAILABLE"})
            row["recurrence"] = "UNAVAILABLE"
        elif state and state["active_plan_count"]:
            row["blockers"].append({"code": "ACTIVE_EXPOSURE_ALREADY_PUBLISHED", "plan_id": state["active_plan_id"]})
            row["recurrence"] = "ACTIVE_EXPOSURE"
        elif state and state["published_plan_count"]:
            row["recurrence"] = "PREVIOUSLY_TERMINATED_EXPOSURE"
        else:
            row["recurrence"] = "NEW_EXPOSURE"
        if exposure_key in seen:
            row["blockers"].append({"code": "DUPLICATE_EXPOSURE_IN_BATCH", "candidate_id": seen[exposure_key]})
        if row["blockers"]:
            row["status"] = "BLOCKED"
        else:
            row["status"] = "WOULD_PUBLISH_INDICATIVE"
            seen[exposure_key] = row["candidate_id"]
    return json.loads(_canonical({
        "version": DRY_RUN_VERSION, "assessment_only": True, "dry_run": True,
        "assessed_at": assessed_at, "completed_at": completed_at, "expected_delayed_slot": expected_slot,
        "policy": policy, "policy_sha256": hashlib.sha256(_canonical(policy).encode("ascii")).hexdigest(),
        "selection_order": "EXPLICIT_REQUEST_ORDER", "rows": detached,
        "counts": dict(Counter(row["status"] for row in detached)),
        "publication_state_available": publication_state.get("available"),
        "publication_state_checked_at": publication_state.get("checked_at"),
        "publication_state_reason": publication_state.get("reason"),
        "persisted": False, "publication_permission": False, "execution_permission": False,
        "paper_position_created": False, "reservations_created": False,
        "limitations": ["POINT_IN_TIME_DRY_RUN_NOT_A_RESERVATION", "PUBLICATION_MUST_RECHECK_ATOMICALLY",
                        "EXPLICIT_REQUESTS_NOT_UNIVERSE_COVERAGE", "INDICATIVE_MARKS_NOT_EXECUTABLE_QUOTES",
                        "NO_CALIBRATED_MANAGEMENT_OR_PAPER_FILL"],
    }))