#!/usr/bin/env python3
"""Aggregate exact Advanced Realtime V3 validation reports without side effects."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
WORKSPACE_DIR = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import canonical_sha256  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402
from scripts.realtime_v2_audit_support import (  # noqa: E402
    require_new_workspace_report,
)


def _load(path: Path):
    encoded = path.read_bytes()
    return json.loads(encoded), hashlib.sha256(encoded).hexdigest()


def _zero_effects(effects) -> bool:
    return isinstance(effects, dict) and all(
        value == 0
        for value in effects.values()
    )


def _treasury_available(report) -> bool:
    buckets = report.get("gap_report", {}).get("buckets", {})
    return any(
        row.get("fact") == "treasury_curve"
        and row.get("status") == "AVAILABLE"
        and row.get("evidence", {}).get("publication_causal") is True
        for row in buckets.get("AVAILABLE", ())
    )


def _target9_bar_history_covered(report) -> bool:
    per_ticker = report.get("per_ticker")
    if not isinstance(per_ticker, list) or len(per_ticker) != 9:
        return False
    for row in per_ticker:
        gaps = row.get("bar_gaps", {})
        if not gaps or any(
            value.get("missing_session_count") != 0
            or value.get("incomplete_sessions")
            for value in gaps.values()
        ):
            return False
    return not report.get("global_references", {}).get(
        "downstream_blockers"
    )


def evaluate(
    qualification,
    replay,
    history,
    scanner,
    treasury_reports,
    readiness,
):
    checks = {
        "qualification": (
            qualification.get("verdict") == "PASS"
            and _zero_effects(qualification.get("effects"))
        ),
        "retained_replay": (
            replay.get("status") == "COMPLETE"
            and replay.get("provider_requests") == 0
            and replay.get("provider_transport_calls") == 0
            and replay.get("release_advanced") is False
        ),
        "target9_bar_history": _target9_bar_history_covered(history),
        "scanner": (
            scanner.get("verdict") == "PASS"
            and not scanner.get("failures")
            and scanner.get("controls", {}).get("provider_requests") == 0
            and scanner.get("controls", {}).get("database_writes") == 0
            and scanner.get("controls", {}).get(
                "transaction_read_only"
            ) == "on"
        ),
        "treasury": bool(treasury_reports) and all(
            _treasury_available(report)
            and _zero_effects(report.get("scope", {}).get("effects"))
            for report in treasury_reports
        ),
        "readiness": (
            readiness.get("verdict") == "GO_PROVIDER_AUTHORITY_REQUIRED"
            and all(
                not readiness.get("gap_report", {})
                .get("buckets", {})
                .get(name)
                for name in (
                    "MISSING_STORED",
                    "STALE",
                    "AMBIGUOUS",
                    "REQUIRES_PROVIDER",
                )
            )
            and _zero_effects(readiness.get("scope", {}).get("effects"))
        ),
    }
    return checks


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--qualification-report", type=Path, required=True)
    parser.add_argument("--replay-report", type=Path, required=True)
    parser.add_argument("--history-report", type=Path, required=True)
    parser.add_argument("--scanner-report", type=Path, required=True)
    parser.add_argument(
        "--treasury-report",
        type=Path,
        action="append",
        required=True,
    )
    parser.add_argument("--readiness-report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    args.output = require_new_workspace_report(args.output, WORKSPACE_DIR)
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    sources = {}
    reports = {}
    for name, path in (
        ("qualification", args.qualification_report),
        ("replay", args.replay_report),
        ("history", args.history_report),
        ("scanner", args.scanner_report),
        ("readiness", args.readiness_report),
    ):
        reports[name], file_sha256 = _load(path.resolve())
        sources[name] = {
            "path": str(path.resolve()),
            "file_sha256": file_sha256,
        }
    treasury_reports = []
    for index, path in enumerate(args.treasury_report, start=1):
        report, file_sha256 = _load(path.resolve())
        treasury_reports.append(report)
        sources[f"treasury_{index}"] = {
            "path": str(path.resolve()),
            "file_sha256": file_sha256,
        }
    checks = evaluate(
        reports["qualification"],
        reports["replay"],
        reports["history"],
        reports["scanner"],
        tuple(treasury_reports),
        reports["readiness"],
    )
    report = {
        "contract": "advanced-realtime-v3-holistic-validation-v1",
        "verdict": "PASS" if all(checks.values()) else "BLOCKED",
        "checks": checks,
        "sources": sources,
        "effects": {
            "provider_requests": 0,
            "database_writes": 0,
            "service_actions": 0,
            "publication": 0,
        },
    }
    report["semantic_sha256"] = canonical_sha256(report)
    write_report(args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0 if report["verdict"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
