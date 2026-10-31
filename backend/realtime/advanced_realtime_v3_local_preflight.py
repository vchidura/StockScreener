from __future__ import annotations

from datetime import date
from typing import Mapping, Sequence

from options.calendar import OptionExchangeCalendar
from realtime.contracts import canonical_sha256


LOCAL_PREFLIGHT_SCHEMA = "advanced-realtime-v3-local-preflight-v1"
COORDINATOR_MANIFEST_SCHEMA = (
    "advanced-realtime-v3-preopen-coordinator-manifest-v1"
)


def authorization_clock_plan(
    session_date: date,
    *,
    calendar: OptionExchangeCalendar | None = None,
) -> dict[str, object]:
    if not isinstance(session_date, date):
        raise TypeError("session_date must be a date")
    exchange = calendar or OptionExchangeCalendar("XNYS")
    prior_session = exchange.previous_session_date(session_date)
    if exchange.next_session_date(prior_session) != session_date:
        raise ValueError(
            "target session is not the next XNYS session after its parent"
        )
    return {
        "parent_valid_from": prior_session.isoformat(),
        "parent_valid_through": prior_session.isoformat(),
        "child_valid_from": session_date.isoformat(),
        "child_valid_through": session_date.isoformat(),
    }


def _command(manifest: Mapping[str, object], command_id: str):
    gates = manifest.get("gates")
    if not isinstance(gates, Sequence) or isinstance(gates, (str, bytes)):
        raise ValueError("manifest gates are unavailable")
    matches = [
        command
        for gate in gates
        if isinstance(gate, Mapping)
        for command in gate.get("commands", ())
        if (
            isinstance(command, Mapping)
            and command.get("id") == command_id
        )
    ]
    if len(matches) != 1:
        raise ValueError(
            f"manifest must define exactly one {command_id} command"
        )
    return matches[0]


def _flag_values(argv: object) -> dict[str, list[str]]:
    if (
        not isinstance(argv, Sequence)
        or isinstance(argv, (str, bytes))
        or not all(isinstance(value, str) for value in argv)
    ):
        raise ValueError("manifest command argv is invalid")
    values: dict[str, list[str]] = {}
    index = 0
    while index < len(argv):
        name = argv[index]
        if not name.startswith("--"):
            index += 1
            continue
        if index + 1 >= len(argv) or argv[index + 1].startswith("--"):
            values.setdefault(name, []).append("")
            index += 1
            continue
        values.setdefault(name, []).append(argv[index + 1])
        index += 2
    return values


def _one(values: Mapping[str, list[str]], name: str) -> str:
    matches = values.get(name, ())
    if len(matches) != 1 or not matches[0]:
        raise ValueError(f"manifest command requires one {name}")
    return matches[0]


def validate_manifest_authorization_clocks(
    manifest: Mapping[str, object],
    *,
    calendar: OptionExchangeCalendar | None = None,
) -> dict[str, object]:
    if manifest.get("schema") != COORDINATOR_MANIFEST_SCHEMA:
        raise ValueError("coordinator manifest schema changed")
    try:
        session_date = date.fromisoformat(str(manifest["session_date"]))
    except (KeyError, ValueError) as error:
        raise ValueError("manifest session_date is invalid") from error
    expected = authorization_clock_plan(
        session_date,
        calendar=calendar,
    )
    root = _flag_values(
        _command(manifest, "authorize_equity_root").get("argv")
    )
    child = _flag_values(
        _command(manifest, "authorize_equity_target9").get("argv")
    )
    actual = {
        "parent_valid_from": _one(root, "--valid-from"),
        "parent_valid_through": _one(root, "--valid-through"),
        "child_valid_from": _one(child, "--valid-from"),
        "child_valid_through": _one(child, "--valid-through"),
    }
    if _one(root, "--universe-target") != "1":
        raise ValueError("parent authorization must be exact target-1")
    if _one(child, "--universe-target") != "9":
        raise ValueError("child authorization must be exact target-9")
    if (
        _one(child, "--parent-authorization-id")
        != "{binding:equity_root_authorization_id}"
    ):
        raise ValueError("child authorization is not bound to its parent")
    if actual != expected:
        raise ValueError(
            "authorization promotion clocks are invalid: "
            f"expected={expected}, actual={actual}"
        )
    report = {
        "schema": LOCAL_PREFLIGHT_SCHEMA,
        "status": "PASS",
        "mode": "MANIFEST_VALIDATION",
        "session_date": session_date.isoformat(),
        "authorization_clocks": expected,
        "checks": {
            "parent_target": 1,
            "child_target": 9,
            "parent_binding": "equity_root_authorization_id",
            "next_session_promotion": True,
        },
        "effects": {
            "provider_requests": 0,
            "production_database_writes": 0,
            "production_service_actions": 0,
            "publication": 0,
            "pointer_changes": 0,
            "read_pin_changes": 0,
        },
    }
    report["semantic_sha256"] = canonical_sha256(report)
    return report


def build_authorization_clock_report(
    session_date: date,
    *,
    calendar: OptionExchangeCalendar | None = None,
) -> dict[str, object]:
    report = {
        "schema": LOCAL_PREFLIGHT_SCHEMA,
        "status": "PASS",
        "mode": "CLOCK_PLAN",
        "session_date": session_date.isoformat(),
        "authorization_clocks": authorization_clock_plan(
            session_date,
            calendar=calendar,
        ),
        "effects": {
            "provider_requests": 0,
            "production_database_writes": 0,
            "production_service_actions": 0,
            "publication": 0,
            "pointer_changes": 0,
            "read_pin_changes": 0,
        },
    }
    report["semantic_sha256"] = canonical_sha256(report)
    return report
