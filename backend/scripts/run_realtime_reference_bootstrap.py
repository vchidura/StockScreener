#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import dotenv_values, load_dotenv
from psycopg2 import pool, sql


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import CANARY_UNIVERSE
from realtime.contracts import canonical_sha256
from realtime.bootstrap_manifest import load_realtime_bootstrap_manifest
from realtime.database_access import verify_realtime_pool_target
from realtime.migrations import validate_target_database
from realtime.reference_repository import persist_reference_result
from realtime.reference_seed import SeedStatus
from realtime.reference_seed import ReferenceKind
from realtime.reference_service import ReferenceSeedRequest
from realtime.reference_transports import (
    ReferenceTransportLimits,
    build_live_reference_service,
)
from realtime.security import build_role_names


MAXIMUM_REFERENCE_REQUESTS = 68
MAXIMUM_REFERENCE_REQUESTS_WITH_FUNDAMENTALS = 104
ETF_TICKERS = frozenset({"SPY", "QQQ", "IWM"})
DEFAULT_REPORT_PATH = BACKEND_DIR / ".realtime-canary" / "reference-bootstrap.json"
REALTIME_ENV_PATH = BACKEND_DIR / ".env"


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bootstrap fresh references for the isolated realtime canary."
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--role-prefix", default="stocks_realtime")
    parser.add_argument("--report-path", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--manifest")
    parser.add_argument("--env-file", type=Path, default=REALTIME_ENV_PATH)
    parser.add_argument("--enable-fundamentals", action="store_true")
    operations = parser.add_mutually_exclusive_group()
    operations.add_argument("--execute", action="store_true")
    operations.add_argument("--status", action="store_true")
    operations.add_argument("--check-environment", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database(args.database, args.confirm_database_name)
    build_role_names(args.role_prefix)
    return args


def _load_environment(*, require_provider_keys: bool, env_file: Path | None = None) -> dict[str, str]:
    env_file = env_file or REALTIME_ENV_PATH
    if not env_file.is_file():
        raise FileNotFoundError("realtime reference environment file is missing")
    load_dotenv(env_file, override=True)
    names = ["REALTIME_RUNTIME_DSN"]
    if require_provider_keys:
        names.extend(("POLYGON_API_KEY", "FINNHUB_API_KEY"))
    settings = {name: os.getenv(name, "").strip() for name in names}
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError(
            "realtime reference bootstrap requires: " + ", ".join(missing)
        )
    return settings


def _environment_status(env_file: Path | None = None) -> dict[str, object]:
    env_file = env_file or REALTIME_ENV_PATH
    values = dotenv_values(env_file)
    placeholders = {
        "",
        "changeme",
        "replace_me",
        "your_api_key_here",
        "your_finnhub_api_key_here",
        "your_polygon_api_key_here",
    }
    checks = {}
    for name in ("POLYGON_API_KEY", "FINNHUB_API_KEY"):
        value = str(values.get(name) or "").strip()
        checks[name] = {
            "present": bool(value),
            "non_placeholder": value.lower() not in placeholders,
        }
    return {
        "status": (
            "CONFIGURED"
            if all(row["present"] and row["non_placeholder"] for row in checks.values())
            else "INCOMPLETE"
        ),
        "provider_credentials": checks,
        "values_exposed": False,
        "provider_requests": 0,
        "database_connections": 0,
    }


def _write_report(path: Path, report: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(report, indent=2, sort_keys=True) + "\n").encode("utf-8")
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("wb") as output:
        output.write(payload)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)


def _pool(dsn: str):
    return pool.SimpleConnectionPool(1, 1, dsn=dsn)


def _status(principal_pool, role_name: str, universe=CANARY_UNIVERSE) -> dict[str, object]:
    verify_realtime_pool_target(principal_pool, role_name=role_name)
    connection = principal_pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
            )
            cursor.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role_name)))
            cursor.execute("SET LOCAL statement_timeout = '10s'")
            cursor.execute(
                "SELECT current.generation, manifest.status, manifest.member_count, "
                "manifest.seed_sha256 FROM rt.seed_current AS current "
                "JOIN rt.seed_manifests AS manifest ON manifest.seed_id=current.seed_id "
                "WHERE current.scope='MARKET_REFERENCES'"
            )
            seed = cursor.fetchone()
            cursor.execute(
                "SELECT ARRAY_AGG(DISTINCT ticker ORDER BY ticker) "
                "FROM ref.security_revisions WHERE ticker = ANY(%s)",
                (list(universe.equity_tickers),),
            )
            equity_tickers = tuple(cursor.fetchone()[0] or ())
            cursor.execute(
                "SELECT ARRAY_AGG(DISTINCT revision.ticker ORDER BY revision.ticker) "
                "FROM ref.option_contracts AS contract "
                "JOIN ref.security_revisions AS revision "
                "ON revision.security_id=contract.underlying_security_id "
                "WHERE revision.ticker = ANY(%s)",
                (list(universe.option_underlyings),),
            )
            option_underlyings = tuple(cursor.fetchone()[0] or ())
        connection.rollback()
    finally:
        principal_pool.putconn(connection, close=bool(connection.closed))
    return {
        "database": "stocks_realtime_v1",
        "seed": (
            None
            if seed is None
            else {
                "generation": seed[0],
                "status": seed[1],
                "member_count": seed[2],
                "seed_sha256": seed[3],
            }
        ),
        "equity_tickers": equity_tickers,
        "option_underlyings": option_underlyings,
    }


def _expected_seed_members(universe, include_fundamentals=False) -> int:
    per_equity = 4 if include_fundamentals else 3
    return len(universe.equity_tickers) * per_equity + len(universe.option_underlyings) * 2 + 2


def _execute(settings: dict[str, str], role_name: str, universe=CANARY_UNIVERSE,
             include_fundamentals=False) -> dict[str, object]:
    principal_pool = _pool(settings["REALTIME_RUNTIME_DSN"])
    try:
        verify_realtime_pool_target(principal_pool, role_name=role_name)
        connection = principal_pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role_name)))
                cursor.execute(
                    "SELECT COALESCE(MAX(generation), 0) + 1 FROM rt.seed_current "
                    "WHERE scope='MARKET_REFERENCES'"
                )
                generation = int(cursor.fetchone()[0])
            connection.rollback()
        finally:
            principal_pool.putconn(connection, close=bool(connection.closed))
        if generation <= 0:
            raise RuntimeError("reference bootstrap generation is invalid")
        now = datetime.now(timezone.utc)
        service = build_live_reference_service(
            massive_api_key=settings["POLYGON_API_KEY"],
            finnhub_api_key=settings["FINNHUB_API_KEY"],
            etf_tickers=ETF_TICKERS,
            massive_base_url="https://api.massive.com",
            limits=ReferenceTransportLimits(
                maximum_requests=(100 if include_fundamentals else 64),
                maximum_response_bytes=2_000_000,
                maximum_total_bytes=16_000_000,
                maximum_rows=50_000,
                maximum_pages=16,
            ),
            include_fundamentals=include_fundamentals,
        )
        result = service.run_once(
            ReferenceSeedRequest(
                universe,
                now.date(),
                now,
                now + timedelta(minutes=1),
                now + timedelta(hours=6),
            )
        )
        maximum_requests = (MAXIMUM_REFERENCE_REQUESTS_WITH_FUNDAMENTALS
            if include_fundamentals else MAXIMUM_REFERENCE_REQUESTS)
        if result.provider_requests > maximum_requests:
            raise RuntimeError("reference bootstrap exceeded its request authorization")
        if result.manifest.status is not SeedStatus.READY:
            raise RuntimeError(
                "reference bootstrap is not ready: "
                + ", ".join(result.manifest.reason_codes)
            )
        if len(result.manifest.members) != _expected_seed_members(universe, include_fundamentals):
            raise RuntimeError("reference bootstrap member count changed")
        connection = principal_pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role_name)))
            persist_reference_result(connection, result, generation=generation)
        finally:
            principal_pool.putconn(connection, close=bool(connection.closed))
        return {
            "status": "REFERENCE_BOOTSTRAP_COMPLETE",
            "generation": generation,
            "seed_sha256": result.manifest.seed_sha256,
            "member_count": len(result.manifest.members),
            "provider_requests": result.provider_requests,
            "response_bytes": result.response_bytes,
            "reference_sha256": result.manifest.seed_sha256,
            "action_sha256": canonical_sha256({"version": "realtime-reference-actions-v1",
                "members": tuple(member for member in result.manifest.members if member.kind in {
                    ReferenceKind.CORPORATE_ACTION_COVERAGE, ReferenceKind.EARNINGS_COVERAGE,
                    ReferenceKind.FOMC_COVERAGE})}),
        }
    finally:
        principal_pool.closeall()


def main(argv=None) -> int:
    args = parse_args(argv)
    roles = build_role_names(args.role_prefix)
    universe = (
        load_realtime_bootstrap_manifest(Path(args.manifest).resolve()).universe
        if args.manifest else CANARY_UNIVERSE
    )
    if args.check_environment:
        print(json.dumps(_environment_status(args.env_file), indent=2))
        return 0
    if not (args.execute or args.status):
        print(
            json.dumps(
                {
                    "mode": "PLAN",
                    "database": args.database,
                    "role": roles.references,
                    "symbols": universe.equity_tickers,
                    "option_underlyings": universe.option_underlyings,
                    "fundamentals_enabled": args.enable_fundamentals,
                    "expected_seed_members": _expected_seed_members(universe, args.enable_fundamentals),
                    "sources": ("massive", "finnhub", "federal_reserve", "treasury"),
                    "maximum_provider_requests": (MAXIMUM_REFERENCE_REQUESTS_WITH_FUNDAMENTALS
                        if args.enable_fundamentals else MAXIMUM_REFERENCE_REQUESTS),
                    "maximum_pages_per_massive_request": 16,
                    "canonical_import": False,
                },
                indent=2,
            )
        )
        print("No provider requests or database writes performed.")
        return 0
    settings = _load_environment(require_provider_keys=args.execute, env_file=args.env_file)
    if args.status:
        principal_pool = _pool(settings["REALTIME_RUNTIME_DSN"])
        try:
            report = _status(principal_pool, roles.references, universe)
        finally:
            principal_pool.closeall()
    else:
        report_path = Path(args.report_path).resolve()
        started_at = datetime.now(timezone.utc)
        _write_report(
            report_path,
            {
                "state": "RUNNING",
                "database": args.database,
                "started_at": started_at.isoformat(),
                "maximum_provider_requests": (MAXIMUM_REFERENCE_REQUESTS_WITH_FUNDAMENTALS
                    if args.enable_fundamentals else MAXIMUM_REFERENCE_REQUESTS),
                "maximum_pages_per_massive_request": 16,
                "values_exposed": False,
            },
        )
        try:
            report = _execute(
                settings, roles.references, universe,
                include_fundamentals=args.enable_fundamentals,
            )
        except Exception as error:
            message = str(error)
            for name in ("POLYGON_API_KEY", "FINNHUB_API_KEY"):
                message = message.replace(settings[name], "[REDACTED]")
            _write_report(
                report_path,
                {
                    "state": "FAILED",
                    "database": args.database,
                    "started_at": started_at.isoformat(),
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "error_type": type(error).__name__,
                    "error_message": message,
                    "values_exposed": False,
                },
            )
            raise
        report = {
            "state": "COMPLETE",
            "database": args.database,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "values_exposed": False,
            **report,
        }
        _write_report(report_path, report)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())