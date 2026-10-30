#!/usr/bin/env python3
"""Assemble and validate an immutable Advanced Realtime V3 pre-open freeze."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_realtime_session_operations import (  # noqa: E402
    build_preopen_freeze_report,
    preopen_freeze_template,
)


INPUT_SCHEMA = "advanced-realtime-v3-freeze-inputs-v1"
SECTIONS = (
    "normalization",
    "finalization",
    "provider_budgets",
    "identities",
)


def _read(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid freeze input document: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError("freeze input document must be an object")
    return value


def _write(path: Path, value: dict) -> bytes:
    encoded = (
        json.dumps(value, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
    return encoded


def _merge(target: dict, patch: dict, path: str) -> None:
    unknown = set(patch) - set(target)
    if unknown:
        raise ValueError(
            f"freeze inputs contain unknown {path} fields: {sorted(unknown)}"
        )
    for name, value in patch.items():
        current = target[name]
        if isinstance(current, dict):
            if not isinstance(value, dict):
                raise ValueError(f"freeze input {path}.{name} must be an object")
            _merge(current, value, f"{path}.{name}")
        else:
            target[name] = value


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--operator", required=True)
    parser.add_argument("--approver", required=True)
    parser.add_argument(
        "--freeze-completed-at",
        type=datetime.fromisoformat,
        required=True,
    )
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--freeze-record", type=Path, required=True)
    parser.add_argument("--freeze-report", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    for name in ("inputs", "freeze_record", "freeze_report"):
        setattr(args, name, getattr(args, name).resolve())
    if (
        not args.operator.strip()
        or not args.approver.strip()
        or args.freeze_completed_at.tzinfo is None
        or args.freeze_completed_at.utcoffset() is None
        or not args.inputs.is_file()
        or args.freeze_record.exists()
        or args.freeze_report.exists()
        or args.freeze_record.parent != args.freeze_report.parent
        or not args.freeze_record.parent.is_dir()
        or not args.execute
    ):
        parser.error(
            "freeze assembly requires explicit owners, UTC time, existing inputs, "
            "new colocated outputs and --execute"
        )
    return args


def execute(args) -> dict:
    inputs = _read(args.inputs)
    if (
        inputs.get("schema") != INPUT_SCHEMA
        or inputs.get("session_date") != args.session_date.isoformat()
        or set(inputs.get("sections", {})) != set(SECTIONS)
    ):
        raise ValueError("freeze input identity or sections changed")
    freeze = preopen_freeze_template(args.session_date)
    freeze.update({
        "operator": args.operator.strip(),
        "approver": args.approver.strip(),
        "freeze_completed_at_utc": (
            args.freeze_completed_at.isoformat()
        ),
    })
    for name in SECTIONS:
        section = inputs["sections"][name]
        if not isinstance(section, dict):
            raise ValueError(f"freeze input section {name} must be an object")
        _merge(freeze[name], section, name)
    provisional = build_preopen_freeze_report(freeze)
    freeze["provider_budgets"]["equity"]["scope_sha256"] = (
        provisional["computed_budget_scope_sha256"]["equity"]
    )
    freeze["provider_budgets"]["options"]["scope_sha256"] = (
        provisional["computed_budget_scope_sha256"]["options"]
    )
    freeze["identities"]["equity_authority_scope_sha256"] = (
        provisional["computed_budget_scope_sha256"]["equity"]
    )
    report = build_preopen_freeze_report(freeze)
    record_bytes = _write(args.freeze_record, freeze)
    report_bytes = _write(args.freeze_report, report)
    return {
        "status": "FREEZE_ASSEMBLED",
        "decision": report["decision"],
        "freeze_sha256": report["freeze_sha256"],
        "freeze_record_file_sha256": hashlib.sha256(
            record_bytes
        ).hexdigest(),
        "freeze_report_semantic_sha256": report["semantic_sha256"],
        "freeze_report_file_sha256": hashlib.sha256(
            report_bytes
        ).hexdigest(),
        "freeze_record": str(args.freeze_record),
        "freeze_report": str(args.freeze_report),
        "provider_effects": 0,
        "production_effects": 0,
    }


def main(argv=None) -> int:
    result = execute(parse_args(argv))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["decision"] == "GO" else 1


if __name__ == "__main__":
    raise SystemExit(main())
