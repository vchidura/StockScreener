#!/usr/bin/env python3
"""Build disabled V3 operations artifacts from immutable provider-free evidence."""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_v3_deployment import (  # noqa: E402
    advanced_options_v3_package_plan,
    advanced_options_v3_package_status,
    build_disabled_advanced_options_v3_package,
    provider_free_advanced_options_v3_preflight,
)
from realtime.contracts import canonical_sha256  # noqa: E402


UTC = timezone.utc
ZERO_EFFECTS = {
    "provider_rest": 0,
    "provider_websocket": 0,
    "production_database_writes": 0,
    "production_service_actions": 0,
    "production_migrations": 0,
    "repair_backfill": 0,
    "publication": 0,
    "pointer_changes": 0,
    "stored_read_changes": 0,
    "retention_actions": 0,
    "delivery": 0,
    "trading": 0,
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--production-preflight", type=Path, required=True)
    parser.add_argument("--qualification-report", type=Path, required=True)
    parser.add_argument("--docker-rehearsal-report", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--image-id", required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    args = parser.parse_args(argv)
    args.production_preflight = args.production_preflight.resolve()
    args.qualification_report = args.qualification_report.resolve()
    args.docker_rehearsal_report = args.docker_rehearsal_report.resolve()
    args.output_directory = args.output_directory.resolve()
    if args.output_directory.exists():
        parser.error("--output-directory must be new")
    if (
        len(args.source_commit) != 40
        or any(
            character not in "0123456789abcdef"
            for character in args.source_commit
        )
    ):
        parser.error("--source-commit must be a full Git commit")
    if (
        not args.image_id.startswith("sha256:")
        or len(args.image_id) != 71
    ):
        parser.error("--image-id must be an immutable local image ID")
    return args


def _load(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain an object")
    return value


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write(path: Path, value) -> dict[str, str]:
    payload = dict(value)
    payload["semantic_sha256"] = canonical_sha256(payload)
    encoded = (
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
    return {
        "path": str(path),
        "semantic_sha256": payload["semantic_sha256"],
        "file_sha256": hashlib.sha256(encoded).hexdigest(),
    }


def _docker_ps():
    result = subprocess.run(
        ("docker", "ps", "--no-trunc", "--format", "{{json .}}"),
        cwd=REPOSITORY_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Docker service observation failed: "
            + result.stderr.decode("utf-8", errors="replace")[:500]
        )
    return tuple(
        json.loads(line)
        for line in result.stdout.decode("utf-8").splitlines()
        if line.strip()
    )


def _find_fact(preflight, fact):
    for rows in preflight["gap_report"]["buckets"].values():
        for row in rows:
            if row["fact"] == fact:
                return row["evidence"]
    raise KeyError(fact)


def _compact_gap(preflight):
    buckets = preflight["gap_report"]["buckets"]
    return {
        "contract": "advanced-options-v3-compact-gap-report-v1",
        "verdict": preflight["verdict"],
        "production_preflight_semantic_sha256": preflight[
            "semantic_sha256"
        ],
        "session_date": preflight["scope"]["session_date"],
        **{
            name: buckets[name]
            for name in (
                "AVAILABLE",
                "MISSING_STORED",
                "STALE",
                "AMBIGUOUS",
                "REQUIRES_PROVIDER",
            )
        },
        "effects": dict(ZERO_EFFECTS),
    }


def _repair_plan(preflight):
    history = preflight["stored_data"]["history_coverage"][0]
    requests = []
    by_session = {}
    for symbol in ("QQQ", "SPY"):
        for session in history["symbols"][symbol]["missing_sessions"]:
            by_session.setdefault(session, []).append(symbol)
    for session in sorted(by_session):
        requests.append({
            "owner": (
                "backend/scripts/repair_realtime_v2_adjusted_daily.py"
            ),
            "endpoint": (
                "/v2/aggs/grouped/locale/us/market/stocks/"
                f"{session}"
            ),
            "parameters": {
                "adjusted": True,
                "include_otc": False,
            },
            "session": session,
            "symbols": sorted(by_session[session]),
            "request_ceiling": 1,
            "response_byte_limit": 3_000_000,
            "maximum_response_bytes": 3_000_001,
        })
    return {
        "contract": "advanced-options-v3-bounded-repair-plan-v1",
        "state": "DISABLED_NOT_AUTHORIZED",
        "executable": False,
        "database": {
            "name": "stocks_realtime_v2",
            "history_cohort_id": history["cohort_id"],
            "write_role": "stocks_realtime_v2_app",
            "production_ddl": False,
        },
        "adjusted_daily": {
            "requests": requests,
            "request_ceiling": len(requests),
            "response_byte_limit_each": 3_000_000,
            "total_byte_ceiling": 12_000_000,
            "sessions": sorted(by_session),
            "symbols": ["QQQ", "SPY"],
            "adjusted": True,
            "raw_daily_requests": 0,
        },
        "optional_research_context": {
            "free_float_required": False,
            "short_interest_percent_required": False,
            "accepted_lanes": ["O1", "O3"],
            "provider_requests": 0,
        },
        "corporate_actions": {
            "owner": "backend/scripts/backfill_corporate_actions.py",
            "state": "BLOCKED_OWNER_BYTE_GATES_REQUIRED",
            "symbols": ["QQQ", "SPY"],
            "window": ["2026-10-23", "2026-10-26"],
            "endpoints": [
                "/stocks/v1/splits",
                "/stocks/v1/dividends",
            ],
            "request_ceiling": 2,
            "response_byte_limit_each": 5_000_000,
            "total_byte_ceiling": 10_000_000,
        },
        "treasury": {
            "owner": "backend/scripts/backfill_option_model_inputs.py",
            "state": "BLOCKED_OWNER_DATE_AND_BYTE_GATES_REQUIRED",
            "canonical_source": "us_treasury_par_yield_v1",
            "required_rate_date_lte": "2026-10-23",
            "maximum_age_seconds_at_reference_freeze": 3600,
            "request_ceiling": 1,
            "response_byte_limit": 2_000_000,
        },
        "pre_session_not_historical_repair": {
            "owner": (
                "backend/realtime/"
                "advanced_options_p8_selection_source.py"
            ),
            "endpoint": "/v3/snapshot/options/{underlyingAsset}",
            "underlyings": ["QQQ", "SPY"],
            "expiration_date": "2026-10-23",
            "request_ceiling": 2,
            "maximum_results_per_underlying": 250,
            "pagination": 0,
            "decoded_byte_ceiling": 100_000_000,
            "selected_contracts": (
                "NOT_KNOWN_UNTIL_AUTHORIZED_PROSPECTIVE_SELECTION"
            ),
        },
        "rollback": {
            "publication": False,
            "release_or_pointer_change": False,
            "stored_read_change": False,
            "delete_original_bars": False,
            "on_failure": (
                "retain request receipts and new immutable revisions; "
                "do not advance any pointer"
            ),
        },
        "cleanup": {
            "temporary_files": "exact run-owned files only",
            "services": "none",
            "containers": "none",
            "retention": "none",
        },
        "effects": dict(ZERO_EFFECTS),
    }


def main(argv=None) -> int:
    args = parse_args(argv)
    preflight = _load(args.production_preflight)
    qualification = _load(args.qualification_report)
    docker_rehearsal = _load(args.docker_rehearsal_report)
    unsigned_qualification = dict(qualification)
    qualification_semantic_sha256 = unsigned_qualification.pop(
        "semantic_sha256",
        None,
    )
    if (
        preflight.get("contract")
        != "advanced-options-v3-operations-preflight-v1"
        or qualification.get("verdict") != "PASS"
        or qualification_semantic_sha256
        != canonical_sha256(unsigned_qualification)
        or docker_rehearsal.get("verdict") != "PASS"
        or docker_rehearsal.get("scope", {}).get("image_id")
        != args.image_id
    ):
        raise ValueError("V3 preparation evidence identity changed")
    args.output_directory.mkdir()
    artifacts = {}
    gap = _compact_gap(preflight)
    artifacts["gap_report"] = _write(
        args.output_directory / "GAP.json",
        gap,
    )
    repair = _repair_plan(preflight)
    repair["gap_report_semantic_sha256"] = artifacts["gap_report"][
        "semantic_sha256"
    ]
    artifacts["repair_plan"] = _write(
        args.output_directory / "REPAIR-PLAN.json",
        repair,
    )
    package = build_disabled_advanced_options_v3_package(
        repository_root=REPOSITORY_ROOT,
        source_commit=args.source_commit,
        backend_image=args.image_id,
        qualification_semantic_sha256=qualification[
            "semantic_sha256"
        ],
        qualification_file_sha256=_file_sha256(
            args.qualification_report
        ),
    )
    package_path = args.output_directory / "PACKAGE.json"
    package_path.write_text(
        json.dumps(package, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    artifacts["package"] = {
        "path": str(package_path),
        "semantic_sha256": package["package_sha256"],
        "file_sha256": _file_sha256(package_path),
    }
    for name, value in (
        ("PLAN", advanced_options_v3_package_plan(package)),
        (
            "PREFLIGHT",
            provider_free_advanced_options_v3_preflight(
                package,
                repository_root=REPOSITORY_ROOT,
            ),
        ),
        ("STATUS", advanced_options_v3_package_status(package)),
    ):
        artifacts[name.casefold()] = _write(
            args.output_directory / f"{name}.json",
            value,
        )
    service_observation = {
        "contract": "advanced-options-v3-service-observation-v1",
        "observed_at_utc": datetime.now(UTC),
        "running_containers": _docker_ps(),
        "production_actions": 0,
    }
    artifacts["service_observation"] = _write(
        args.output_directory / "SERVICE-OBSERVATION.json",
        service_observation,
    )
    rehearsal = {
        "contract": "advanced-options-v3-docker-rehearsal-status-v1",
        "verdict": "PASS",
        "image_id": args.image_id,
        "profile": "realtime-v3-options",
        "execution_flag": "--no-build",
        "source_report_semantic_sha256": docker_rehearsal[
            "semantic_sha256"
        ],
        "source_report_file_sha256": _file_sha256(
            args.docker_rehearsal_report
        ),
        "verified": {
            "four_role_health": all(
                row["health"] == "healthy"
                for row in docker_rehearsal["services"]
            ),
            "durable_database_barrier": True,
            "v5_window_completion": (
                docker_rehearsal["durable_database_barrier"]["window"][
                    "state"
                ] == "COMPLETE"
            ),
        },
        "cleanup": docker_rehearsal["cleanup"],
        "effects": dict(ZERO_EFFECTS),
    }
    artifacts["rehearsal"] = _write(
        args.output_directory / "REHEARSAL.json",
        rehearsal,
    )
    operations_status = {
        "contract": "advanced-options-v3-operations-status-v1",
        "verdict": "BLOCKED",
        "session_date": "2026-10-23",
        "qualification": {
            "semantic_sha256": qualification["semantic_sha256"],
            "file_sha256": _file_sha256(args.qualification_report),
        },
        "production_preflight": {
            "semantic_sha256": preflight["semantic_sha256"],
            "file_sha256": _file_sha256(args.production_preflight),
        },
        "image_id": args.image_id,
        "package_sha256": package["package_sha256"],
        "activation": package["activation"],
        "blockers": [
            *[
                f"{status}:{row['fact']}"
                for status in (
                    "MISSING_STORED",
                    "STALE",
                    "AMBIGUOUS",
                    "REQUIRES_PROVIDER",
                )
                for row in preflight["gap_report"]["buckets"][status]
            ],
            "SESSION_ACQUISITION_NOT_PREPARED",
            "WINDOW_CYCLE_NOT_PREPARED",
            "CANONICAL_PUBLICATION_NOT_PREPARED",
            "STORED_READ_NOT_AUTHORIZED",
        ],
        "nine_underlying_options_expansion_authorized": False,
        "equity_target": 9,
        "effects": dict(ZERO_EFFECTS),
    }
    artifacts["operations_status"] = _write(
        args.output_directory / "OPERATIONS-STATUS.json",
        operations_status,
    )
    manifest = {
        "contract": "advanced-options-v3-operations-artifact-manifest-v1",
        "artifacts": artifacts,
        "effects": dict(ZERO_EFFECTS),
    }
    manifest_info = _write(
        args.output_directory / "MANIFEST.json",
        manifest,
    )
    print(json.dumps({
        "verdict": "BLOCKED",
        "manifest": manifest_info,
        "package_sha256": package["package_sha256"],
        "artifact_count": len(artifacts),
        "effects": ZERO_EFFECTS,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
