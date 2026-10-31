"""Durable calendar plan and reconciliation for unattended Advanced V3 days."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from options.calendar import OptionExchangeCalendar

from .advanced_options_v3_history import (
    ADVANCED_OPTIONS_V3_HISTORY_EVIDENCE_SCHEMA,
    advanced_options_v3_history_source_configuration,
)
from .advanced_realtime_session_scheduler import build_advanced_session_windows
from .contracts import canonical_sha256


UTC = timezone.utc
DAILY_SUPERVISOR_PLAN_SCHEMA = "advanced-realtime-v3-daily-supervisor-plan-v1"
DAILY_SUPERVISOR_STATE_SCHEMA = "advanced-realtime-v3-daily-supervisor-state-v1"
DAILY_SUPERVISOR_ACTION_SCHEMA = "advanced-realtime-v3-daily-supervisor-action-v1"
COORDINATOR_IDENTITY_SCHEMA = "advanced-realtime-v3-coordinator-identity-v1"
EASTERN = ZoneInfo("America/New_York")
REQUIRED_DAILY_ARTIFACTS = (
    "authorization",
    "freeze_report",
    "package",
    "bundle",
    "acquisition_binding",
    "history_preflight",
    "env_file",
    "secret_directory",
)
DAILY_ARTIFACT_FILENAMES = {
    "authorization": "authorization.json",
    "freeze_report": "freeze-report.json",
    "package": "session-package.json",
    "bundle": "session-bundle.json",
    "acquisition_binding": "acquisition-binding.json",
    "history_preflight": "history-preflight.json",
    "env_file": "runtime.env",
    "secret_directory": "secrets",
}


def build_daily_supervisor_plan(
    session_date: date,
    *,
    release_source_sha256: str,
    release_image_id: str,
) -> dict[str, object]:
    calendar = OptionExchangeCalendar("XNYS")
    if calendar.session_on_or_after(session_date) != session_date:
        raise ValueError("daily supervisor target must be an XNYS session")
    windows = build_advanced_session_windows(session_date, calendar=calendar)
    session_open = calendar.session_open(session_date)
    session_close = calendar.session_close(session_date)
    value = {
        "schema": DAILY_SUPERVISOR_PLAN_SCHEMA,
        "session_date": session_date.isoformat(),
        "previous_session": calendar.previous_session_date(
            session_date
        ).isoformat(),
        "next_session": calendar.next_session_date(session_date).isoformat(),
        "preparation_deadline_utc": (
            session_open - timedelta(minutes=20)
        ).isoformat(),
        "acquisition_start_utc": (
            session_open - timedelta(minutes=10)
        ).isoformat(),
        "session_open_utc": session_open.isoformat(),
        "market_capture_stop_utc": session_close.isoformat(),
        "decision_stop_utc": windows[-1].latest_options_cutoff.isoformat(),
        "window_count": len(windows),
        "history_required_through": calendar.previous_session_date(
            session_date
        ).isoformat(),
        "release": {
            "source_sha256": release_source_sha256,
            "image_id": release_image_id,
            "date_coupled": False,
        },
        "required_daily_artifacts": list(REQUIRED_DAILY_ARTIFACTS),
    }
    value["plan_sha256"] = canonical_sha256(value)
    return value


def reconcile_daily_supervisor(
    plan: Mapping[str, object],
    *,
    artifacts: Mapping[str, object],
    prior_state: Mapping[str, object] | None,
    observed_at: datetime,
) -> dict[str, object]:
    if plan.get("schema") != DAILY_SUPERVISOR_PLAN_SCHEMA:
        raise ValueError("daily supervisor plan schema changed")
    unsigned = dict(plan)
    digest = unsigned.pop("plan_sha256", None)
    if digest != canonical_sha256(unsigned):
        raise ValueError("daily supervisor plan identity changed")
    missing = [
        name for name in REQUIRED_DAILY_ARTIFACTS
        if not artifacts.get(name)
    ]
    if prior_state is not None and (
        prior_state.get("schema") != DAILY_SUPERVISOR_STATE_SCHEMA
        or prior_state.get("plan_sha256") != digest
        or prior_state.get("session_date") != plan.get("session_date")
    ):
        raise RuntimeError("daily supervisor durable state diverged")
    state = "PREPARATION_REQUIRED" if missing else "READY_TO_LAUNCH"
    value = {
        "schema": DAILY_SUPERVISOR_STATE_SCHEMA,
        "session_date": plan["session_date"],
        "plan_sha256": digest,
        "state": state,
        "missing_artifacts": missing,
        "observed_at_utc": observed_at.astimezone(UTC).isoformat(),
        "release": dict(plan["release"]),
        "window_count": plan["window_count"],
        "effects": {
            "provider_requests": 0,
            "service_actions": 0,
            "production_database_writes": 0,
            "schedule_changes": 0,
        },
    }
    value["semantic_sha256"] = canonical_sha256(value)
    return value


def artifacts_from_paths(values: Mapping[str, Path]) -> dict[str, bool]:
    return {
        name: (
            path.is_dir() if name == "secret_directory" else path.is_file()
        )
        for name, path in values.items()
    }


def validated_daily_artifacts(
    plan: Mapping[str, object],
    values: Mapping[str, Path],
) -> dict[str, bool]:
    """Validate the history gate not otherwise consumed by the scheduler."""
    availability = artifacts_from_paths(values)
    if not availability.get("history_preflight"):
        return availability
    path = values["history_preflight"]
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError("daily history preflight is unreadable") from error
    unsigned = dict(report)
    semantic = unsigned.pop("semantic_sha256", None)
    evidence = report.get("evidence")
    if not isinstance(evidence, dict):
        raise RuntimeError("daily history preflight evidence is absent")
    evidence_unsigned = dict(evidence)
    evidence_sha256 = evidence_unsigned.pop("evidence_sha256", None)
    readiness = evidence.get("readiness")
    expected_source = advanced_options_v3_history_source_configuration()
    effects = report.get("effects")
    if (
        report.get("contract")
        != "advanced-options-v3-history-preflight-v1"
        or report.get("status") != "PASS"
        or semantic != canonical_sha256(unsigned)
        or evidence.get("schema")
        != ADVANCED_OPTIONS_V3_HISTORY_EVIDENCE_SCHEMA
        or evidence_sha256 != canonical_sha256(evidence_unsigned)
        or evidence.get("session_date") != plan.get("session_date")
        or not isinstance(readiness, dict)
        or readiness.get("status") != "READY"
        or readiness.get("required_through_session")
        != plan.get("history_required_through")
        or readiness.get("latest_covered_session")
        != plan.get("history_required_through")
        or report.get("source_configuration") != expected_source
        or evidence.get("source_configuration") != expected_source
        or not isinstance(effects, dict)
        or any(value != 0 for value in effects.values())
    ):
        raise RuntimeError("daily history preflight identity/readiness changed")
    binding = report.get("binding")
    if (
        not isinstance(binding, dict)
        or binding.get("evidence_sha256") != evidence_sha256
    ):
        raise RuntimeError("daily history preflight binding changed")
    return availability


def session_for_observation(observed_at: datetime) -> date:
    """Return the session an always-on supervisor should reconcile."""
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("supervisor observation must be timezone-aware")
    calendar = OptionExchangeCalendar("XNYS")
    eastern_date = observed_at.astimezone(EASTERN).date()
    session_date = calendar.session_on_or_after(eastern_date)
    if (
        session_date == eastern_date
        and observed_at.astimezone(UTC)
        > build_advanced_session_windows(
            session_date,
            calendar=calendar,
        )[-1].latest_options_cutoff
    ):
        return calendar.next_session_date(session_date)
    return session_date


def daily_artifact_paths(
    session_root: Path,
    session_date: date,
) -> dict[str, Path]:
    directory = session_root / session_date.isoformat()
    return {
        name: directory / filename
        for name, filename in DAILY_ARTIFACT_FILENAMES.items()
    }


def daily_supervisor_action(
    plan: Mapping[str, object],
    *,
    artifacts: Mapping[str, object],
    coordinator_directory: Path,
    observed_at: datetime,
) -> dict[str, object]:
    """Choose one fail-closed, restart-stable lifecycle action."""
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("supervisor observation must be timezone-aware")
    missing = [
        name for name in REQUIRED_DAILY_ARTIFACTS
        if not artifacts.get(name)
    ]
    cleanup = coordinator_directory / "CLEANUP.json"
    identity = coordinator_directory / "COORDINATOR.json"
    if coordinator_directory.exists():
        if not coordinator_directory.is_dir() or not identity.is_file():
            raise RuntimeError("daily coordinator state is partial")
        coordinator = _read_coordinator_identity(identity)
        if coordinator.get("session_date") != plan.get("session_date"):
            raise RuntimeError("daily coordinator session identity changed")
        if cleanup.exists():
            action = "AWAIT_NEXT_SESSION"
        elif missing:
            raise RuntimeError(
                "daily coordinator resume artifacts are missing: "
                + ", ".join(missing)
            )
        else:
            action = "RESUME_SCHEDULER"
    elif missing:
        deadline = datetime.fromisoformat(str(plan["preparation_deadline_utc"]))
        action = (
            "MISSED_PREPARATION_DEADLINE"
            if observed_at.astimezone(UTC) > deadline
            else "WAIT_FOR_PREPARATION"
        )
    else:
        start = datetime.fromisoformat(str(plan["acquisition_start_utc"]))
        action = (
            "MISSED_LAUNCH_DEADLINE"
            if observed_at.astimezone(UTC) > start
            else "LAUNCH_SCHEDULER"
        )
    value = {
        "schema": DAILY_SUPERVISOR_ACTION_SCHEMA,
        "session_date": plan["session_date"],
        "plan_sha256": plan["plan_sha256"],
        "action": action,
        "missing_artifacts": missing,
        "observed_at_utc": observed_at.astimezone(UTC).isoformat(),
        "coordinator_directory": str(coordinator_directory.resolve()),
        "effects": {
            "provider_requests": 0,
            "service_actions": 0,
            "production_database_writes": 0,
            "schedule_changes": 0,
        },
    }
    value["semantic_sha256"] = canonical_sha256(value)
    return value


def scheduler_argv(
    *,
    python_executable: Path,
    scheduler_script: Path,
    database: str,
    artifact_paths: Mapping[str, Path],
    docker_executable: Path,
    coordinator_directory: Path,
) -> list[str]:
    """Build the exact production scheduler invocation for one daily plan."""
    missing_keys = set(REQUIRED_DAILY_ARTIFACTS) - set(artifact_paths)
    if missing_keys:
        raise ValueError(
            "daily scheduler artifact paths are incomplete: "
            + ", ".join(sorted(missing_keys))
        )
    argv = [
        str(python_executable.resolve()),
        str(scheduler_script.resolve()),
        "run",
        "--database", database,
        "--confirm-database-name", database,
        "--authorization", str(artifact_paths["authorization"].resolve()),
        "--package", str(artifact_paths["package"].resolve()),
        "--bundle", str(artifact_paths["bundle"].resolve()),
        "--acquisition-binding",
        str(artifact_paths["acquisition_binding"].resolve()),
        "--freeze-report", str(artifact_paths["freeze_report"].resolve()),
        "--env-file", str(artifact_paths["env_file"].resolve()),
        "--secret-directory",
        str(artifact_paths["secret_directory"].resolve()),
        "--docker-executable", str(docker_executable.resolve()),
        "--output-directory", str(coordinator_directory.resolve()),
        "--poll-seconds", "1",
        "--monitor-seconds", "15",
        "--continuous",
        "--execute",
    ]
    if coordinator_directory.exists():
        if not (coordinator_directory / "COORDINATOR.json").is_file():
            raise RuntimeError("daily coordinator state is partial")
        _read_coordinator_identity(
            coordinator_directory / "COORDINATOR.json"
        )
        if (coordinator_directory / "CLEANUP.json").exists():
            raise RuntimeError("completed daily coordinator cannot resume")
        argv.append("--resume")
    return argv


def _read_coordinator_identity(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError("daily coordinator identity is unreadable") from error
    unsigned = dict(value)
    semantic = unsigned.pop("semantic_sha256", None)
    if (
        value.get("schema") != COORDINATOR_IDENTITY_SCHEMA
        or semantic != canonical_sha256(unsigned)
    ):
        raise RuntimeError("daily coordinator identity changed")
    return value
