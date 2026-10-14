#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys

from dotenv import load_dotenv
from psycopg2 import pool


BACKEND_DIR = Path(__file__).resolve().parent.parent
WORKSPACE_DIR = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.database_access import (
    RealtimeDatabaseRole,
    RealtimePoolBudget,
    RealtimeProcessDatabasePlan,
)
from realtime.historical_reference_backfill import (
    HistoricalReferenceBackfillPlan,
    acquire_historical_reference_backfill,
    decode_historical_reference_artifact,
    encode_historical_reference_artifact,
    historical_fact_counts,
    historical_reference_blockers,
    mark_historical_earnings_unavailable,
    persist_historical_reference_backfill_cursor,
)
from realtime.migrations import validate_target_database
from realtime.security import build_role_names
from realtime.worker_database import RealtimeProcessDatabase


REALTIME_ENV_PATH = BACKEND_DIR / ".env"
DEFAULT_ARTIFACT_PATH = (
    BACKEND_DIR / ".realtime-canary" /
    "advanced-realtime-h1-backfill-20261009.artifact.json"
)
DEFAULT_REPORT_PATH = (
    BACKEND_DIR / ".realtime-canary" /
    "advanced-realtime-h1-backfill-20261009.json"
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Acquire or apply the bounded Advanced Realtime H1 reference backfill."
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--role-prefix", default="stocks_realtime")
    parser.add_argument("--env-file", type=Path, default=REALTIME_ENV_PATH)
    parser.add_argument("--artifact-path", type=Path, default=DEFAULT_ARTIFACT_PATH)
    parser.add_argument("--report-path", type=Path, default=DEFAULT_REPORT_PATH)
    operations = parser.add_mutually_exclusive_group()
    operations.add_argument("--acquire", action="store_true")
    operations.add_argument("--apply", action="store_true")
    operations.add_argument("--verify-acquired", action="store_true")
    parser.add_argument("--allow-unavailable-historical-earnings", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database(args.database, args.confirm_database_name)
    build_role_names(args.role_prefix)
    for path in (args.artifact_path.resolve(), args.report_path.resolve()):
        if not path.is_relative_to(WORKSPACE_DIR):
            parser.error("H1 evidence paths must remain inside the workspace")
    return args


def _settings(env_file: Path, *, acquire: bool, apply: bool) -> dict[str, str]:
    required = []
    if acquire:
        required.append("FINNHUB_API_KEY")
    if apply:
        required.append("REALTIME_RUNTIME_DSN")
    if not required:
        return {}
    if not env_file.is_file():
        raise FileNotFoundError("H1 environment file is missing")
    load_dotenv(env_file, override=True)
    settings = {name: os.getenv(name, "").strip() for name in required}
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError("H1 requires: " + ", ".join(missing))
    return settings


def _write_atomic(path: Path, payload: str, *, exclusive=False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if exclusive and path.exists():
        raise FileExistsError(f"H1 evidence path already exists: {path.name}")
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("x" if exclusive else "w", encoding="utf-8", newline="\n") as output:
        output.write(payload)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)


def _write_report(path: Path, report) -> None:
    _write_atomic(
        path,
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
    )


def _plan_report(args) -> dict:
    plan = HistoricalReferenceBackfillPlan()
    return {
        "state": "PLAN",
        "database": args.database,
        "tickers": plan.tickers,
        "start_date": plan.start_date,
        "end_date": plan.end_date,
        "plan_sha256": plan.sha256,
        "finnhub_windows": plan.finnhub_windows,
        "provider_request_ceiling": plan.maximum_provider_requests,
        "response_byte_ceiling": plan.maximum_total_bytes,
        "response_byte_ceiling_each": plan.maximum_response_bytes,
        "row_ceiling": plan.maximum_rows,
        "target_relations": (
            "ref.market_event_revisions",
            "ref.market_event_coverage",
            "ref.risk_free_rate_observations",
        ),
        "seed_promotion": False,
        "provider_requests": 0,
        "database_writes": 0,
    }


def _redacted_error(error, settings) -> str:
    message = str(error)
    for value in settings.values():
        if value:
            message = message.replace(value, "[REDACTED]")
    return message


def _acquire(args, settings):
    artifact_path = args.artifact_path.resolve()
    report_path = args.report_path.resolve()
    if artifact_path.exists() or report_path.exists():
        raise FileExistsError("H1 acquire requires new artifact and report paths")
    plan = HistoricalReferenceBackfillPlan()
    started_at = datetime.now(timezone.utc)
    _write_report(report_path, {
        **_plan_report(args),
        "state": "ACQUIRING",
        "started_at": started_at,
    })
    try:
        result = acquire_historical_reference_backfill(
            plan,
            finnhub_api_key=settings["FINNHUB_API_KEY"],
        )
        artifact_text = encode_historical_reference_artifact(result)
        decoded = decode_historical_reference_artifact(artifact_text)
        if decoded != result:
            raise ValueError("H1 artifact verification changed acquired facts")
        _write_atomic(artifact_path, artifact_text, exclusive=True)
        report = {
            **_plan_report(args),
            "state": "ACQUIRED",
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc),
            "artifact_path": str(artifact_path.relative_to(WORKSPACE_DIR)),
            "artifact_sha256": hashlib.sha256(artifact_text.encode("utf-8")).hexdigest(),
            "acquired_at": result.acquired_at,
            "provider_requests": result.provider_requests,
            "response_bytes": result.response_bytes,
            "fact_counts": historical_fact_counts(result.facts),
            "receipts": tuple({
                "host": row.host,
                "path": row.path,
                "parameters": row.parameters,
                "status_code": row.status_code,
                "response_bytes": row.response_bytes,
                "response_sha256": row.response_sha256,
                "received_at": row.received_at,
            } for row in result.receipts),
            "database_writes": 0,
            "values_exposed": False,
        }
        _write_report(report_path, report)
        return report
    except Exception as error:
        _write_report(report_path, {
            **_plan_report(args),
            "state": "ACQUIRE_FAILED",
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc),
            "error_type": type(error).__name__,
            "error_message": _redacted_error(error, settings),
            "database_writes": 0,
            "values_exposed": False,
        })
        raise


def _apply(args, settings):
    artifact_path = args.artifact_path.resolve()
    report_path = args.report_path.resolve()
    if not artifact_path.is_file() or not report_path.is_file():
        raise FileNotFoundError("H1 apply requires retained acquisition evidence")
    retained = json.loads(report_path.read_text(encoding="utf-8"))
    if retained.get("state") not in {"ACQUIRED", "ACQUIRED_BLOCKED"}:
        raise ValueError("H1 apply requires retained acquisition evidence")
    artifact_text = artifact_path.read_text(encoding="utf-8")
    artifact_sha256 = hashlib.sha256(artifact_text.encode("utf-8")).hexdigest()
    if artifact_sha256 != retained.get("artifact_sha256"):
        raise ValueError("H1 artifact file hash drifted")
    result = decode_historical_reference_artifact(artifact_text)
    blockers = historical_reference_blockers(result)
    unavailable_tickers = ()
    if blockers and args.allow_unavailable_historical_earnings:
        result, unavailable_tickers = mark_historical_earnings_unavailable(result)
        blockers = historical_reference_blockers(result)
    if blockers:
        raise ValueError("H1 acquired artifact is blocked: " + ";".join(blockers))
    apply_policy = {
        "historical_earnings": (
            "UNAVAILABLE"
            if args.allow_unavailable_historical_earnings
            else "REQUIRE_EVENTS"
        ),
        "unavailable_tickers": unavailable_tickers,
        "known_no_event_inferred": False,
    }
    roles = build_role_names(args.role_prefix)
    principal_pool = pool.SimpleConnectionPool(
        1,
        1,
        dsn=settings["REALTIME_RUNTIME_DSN"],
    )
    database = None
    started_at = datetime.now(timezone.utc)
    _write_report(report_path, {
        **retained,
        "state": "APPLYING",
        "apply_started_at": started_at,
        "apply_policy": apply_policy,
    })
    try:
        process_plan = RealtimeProcessDatabasePlan(
            "maintenance:historical_reference_h1",
            RealtimeDatabaseRole.REFERENCES,
            RealtimePoolBudget(1, 1),
        )
        database = RealtimeProcessDatabase.wire(
            process_plan,
            principal_pool=principal_pool,
            role_name=roles.references,
            statement_timeout_ms=10_000,
        )
        with database.transaction(isolation_level="SERIALIZABLE") as cursor:
            applied = persist_historical_reference_backfill_cursor(
                cursor,
                result,
                database=args.database,
                role_name=roles.references,
            )
        report = {
            **retained,
            "state": "COMPLETE",
            "apply_started_at": started_at,
            "apply_finished_at": datetime.now(timezone.utc),
            "apply": applied,
            "apply_policy": apply_policy,
            "acquisition_blockers": tuple(retained.get("blockers", ())),
            "blockers": (),
            "database_writes": sum(applied["fact_counts"].values()),
            "values_exposed": False,
        }
        _write_report(report_path, report)
        return report
    except Exception as error:
        _write_report(report_path, {
            **retained,
            "state": "APPLY_FAILED",
            "apply_started_at": started_at,
            "apply_finished_at": datetime.now(timezone.utc),
            "error_type": type(error).__name__,
            "error_message": _redacted_error(error, settings),
            "apply_policy": apply_policy,
            "values_exposed": False,
        })
        raise
    finally:
        principal_pool.closeall()


def _verify_acquired(args):
    artifact_path = args.artifact_path.resolve()
    report_path = args.report_path.resolve()
    if not artifact_path.is_file() or not report_path.is_file():
        raise FileNotFoundError("H1 verification requires retained acquisition evidence")
    retained = json.loads(report_path.read_text(encoding="utf-8"))
    if retained.get("state") not in {"ACQUIRED", "ACQUIRED_BLOCKED"}:
        raise ValueError("H1 verification requires an acquired report")
    artifact_text = artifact_path.read_text(encoding="utf-8")
    if hashlib.sha256(artifact_text.encode("utf-8")).hexdigest() != retained.get("artifact_sha256"):
        raise ValueError("H1 artifact file hash drifted")
    result = decode_historical_reference_artifact(artifact_text)
    blockers = historical_reference_blockers(result)
    state = "ACQUIRED_BLOCKED" if blockers else "ACQUIRED_VERIFIED"
    report = {
        **retained,
        "state": state,
        "verified_at": datetime.now(timezone.utc),
        "blockers": blockers,
        "database_writes": 0,
        "values_exposed": False,
    }
    _write_report(report_path, report)
    return report


def main(argv=None):
    args = parse_args(argv)
    if not (args.acquire or args.apply or args.verify_acquired):
        print(json.dumps(_plan_report(args), indent=2, sort_keys=True, default=str))
        print("No provider requests or database writes performed.")
        return 0
    settings = _settings(args.env_file, acquire=args.acquire, apply=args.apply)
    if args.acquire:
        report = _acquire(args, settings)
    elif args.apply:
        report = _apply(args, settings)
    else:
        report = _verify_acquired(args)
    print(json.dumps({
        "state": report["state"],
        "provider_requests": report.get("provider_requests", 0),
        "response_bytes": report.get("response_bytes", 0),
        "database_writes": report.get("database_writes", 0),
        "report_path": str(args.report_path.resolve().relative_to(WORKSPACE_DIR)),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())