#!/usr/bin/env python3
"""Plan or execute one bounded V2 adjusted grouped-daily session repair."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID

from dotenv import load_dotenv
from psycopg2.pool import ThreadedConnectionPool

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.data.polygon_http import RequestsPolygonHttpTransport  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    API_ROLE,
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_adjusted_daily_repair import (  # noqa: E402
    AdjustedDailyRepairPlan,
    execute_adjusted_daily_repair,
)
from realtime.equity_validation_source import EquityValidationIdentity  # noqa: E402
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from scripts.realtime_script_support import PacedPolygonTransport, write_report  # noqa: E402
from scripts.repair_realtime_v2_recent_sessions import (  # noqa: E402
    _load_scope_with_identity_continuity,
    _repair_identity_cutoff,
)
from scripts.run_realtime_v2_developer_window import _load_scope  # noqa: E402
from realtime.reference_adapters import _security_id  # noqa: E402
from realtime.ticker_projection import _security  # noqa: E402
from realtime.equity_recent_repair import AuthorizedNativeRepairTransport  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--authorized-until", type=datetime.fromisoformat, required=True)
    parser.add_argument("--maximum-provider-requests", type=int, required=True)
    parser.add_argument("--maximum-response-bytes", type=int, required=True)
    parser.add_argument("--response-byte-limit", type=int, required=True)
    parser.add_argument("--minimum-spacing-seconds", type=float, default=0.5)
    parser.add_argument("--timeout-seconds", type=float, default=30)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--provider-authorization-reference", required=True)
    parser.add_argument("--expected-cost", required=True)
    parser.add_argument("--ticker", action="append")
    parser.add_argument("--raw", action="store_true")
    parser.add_argument("--dated-reference-report", type=Path)
    parser.add_argument(
        "--allow-late-identity-continuity",
        action="store_true",
    )
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(args.database, args.confirm_database_name)
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("adjusted-daily repair targets only stocks_realtime_v2")
    if (
        args.authorized_until.tzinfo is None
        or args.authorized_until.utcoffset() is None
        or args.maximum_provider_requests != 1
        or args.maximum_response_bytes != args.response_byte_limit + 1
        or not 0.5 <= args.minimum_spacing_seconds <= 60
        or not 1 <= args.timeout_seconds <= 60
        or not all(value.strip() for value in (
            args.authorization_reference,
            args.provider_authorization_reference,
            args.expected_cost,
        ))
    ):
        parser.error("adjusted-daily scope, budget, pacing or authorization is invalid")
    args.authorized_until = args.authorized_until.astimezone(timezone.utc)
    args.tickers = tuple(sorted({
        value.strip().upper()
        for value in (args.ticker or ())
        if value.strip()
    }))
    if args.ticker and not args.tickers:
        parser.error("--ticker cannot be blank")
    args.report_path = args.report_path.resolve()
    if args.report_path.exists():
        parser.error("adjusted-daily report must be new")
    if args.dated_reference_report is not None:
        args.dated_reference_report = args.dated_reference_report.resolve()
        if not args.dated_reference_report.is_file() or len(args.tickers) != 1:
            parser.error(
                "--dated-reference-report requires one exact selected ticker"
            )
    return args


def _load_dated_identity(provider, args):
    path = args.dated_reference_report
    if path.stat().st_size > 1_000_000:
        raise ValueError("dated reference report exceeds its bound")
    body = path.read_bytes()
    report = json.loads(body)
    result = report.get("result")
    if (
        report.get("contract") not in {
            "v2-dated-spy-reference-v1",
            "v2-dated-equity-reference-v1",
        }
        or report.get("status") != "PASS"
        or report.get("ticker") != args.tickers[0]
        or report.get("source_as_of_date") != args.session_date.isoformat()
        or report.get("provider_requests") != 1
        or not isinstance(result, dict)
        or result.get("ticker") != args.tickers[0]
        or result.get("source_as_of_date") != args.session_date.isoformat()
    ):
        raise ValueError("dated reference report identity is invalid")
    observed_at = datetime.fromisoformat(report["observed_at"])
    expected_security_id = _security_id(args.tickers[0])
    with provider.transaction(
        APP_ROLE,
        read_only=True,
        dict_cursor=True,
    ) as unit:
        unit.cursor.execute(
            "SELECT member.instrument_id,instrument.security_id,"
            "current.*,current.revision_id AS security_revision_id "
            "FROM rt.cohorts AS cohort "
            "JOIN rt.subscription_members AS member USING(manifest_id) "
            "JOIN rt.instruments AS instrument USING(instrument_id) "
            "JOIN ref.security_revisions AS current USING(security_id) "
            "WHERE cohort.cohort_id=%s AND member.channel='AM' "
            "AND current.ticker=%s AND current.active "
            "ORDER BY current.observed_at DESC LIMIT 1",
            (str(args.cohort_id), args.tickers[0]),
        )
        member = unit.cursor.fetchone()
    if member is None or str(member["security_id"]) != str(
        expected_security_id
    ):
        raise ValueError(
            "dated reference does not match the retained security identity"
        )
    dated_cik = str(result.get("cik") or "").lstrip("0")
    current_cik = str(member.get("cik") or "").lstrip("0")
    same_figi = (
        result.get("composite_figi")
        and result.get("composite_figi")
        == member.get("composite_figi")
    )
    if not same_figi and (
        not dated_cik or not current_cik or dated_cik != current_cik
    ):
        raise ValueError(
            "dated reference FIGI/CIK conflicts with retained identity"
        )
    revision = _security(member)
    identity = EquityValidationIdentity(
        revision.ticker,
        revision,
        int(member["instrument_id"]),
    )
    evidence = ((
        identity.instrument_id,
        identity.ticker,
        "V2_TICKER_ID_WITH_DATED_FIGI_CIK_V1",
        args.session_date,
        observed_at,
        str(expected_security_id),
        result.get("composite_figi"),
        result.get("share_class_figi"),
        result.get("cik"),
        report["response_sha256"],
        hashlib.sha256(body).hexdigest(),
    ),)
    return (identity,), evidence


def main(argv=None):
    args = parse_args(argv)
    load_dotenv(BACKEND_DIR / ".env", override=True)
    role = APP_ROLE if args.execute else API_ROLE
    dsn = os.getenv(
        "REALTIME_V2_APP_DSN" if args.execute else "REALTIME_V2_API_DSN",
        "",
    ).strip()
    if not dsn:
        raise RuntimeError(f"adjusted-daily repair requires the {role} DSN")
    pool = ThreadedConnectionPool(1, 2, dsn=dsn, options="-c timezone=UTC")
    try:
        provider = V2ConnectionProvider(
            {role: pool},
            V2AccessConfig(
                security_mode="hardened",
                role_prefix=EXPECTED_V2_DATABASE_NAME,
            ),
        )
        provider.verify()
        if not args.execute:
            with provider.transaction(API_ROLE, read_only=True) as unit:
                unit.cursor.execute(
                    "SELECT manifest.stock_count,"
                    "array_agg(DISTINCT reference.ticker "
                    "ORDER BY reference.ticker) "
                    "FROM rt.cohorts cohort "
                    "JOIN rt.subscription_manifests manifest USING (manifest_id) "
                    "JOIN rt.subscription_members member USING(manifest_id) "
                    "JOIN rt.instruments instrument USING(instrument_id) "
                    "JOIN ref.security_revisions reference USING(security_id) "
                    "WHERE cohort.cohort_id=%s AND member.channel='AM' "
                    "AND reference.active GROUP BY manifest.stock_count",
                    (str(args.cohort_id),),
                )
                row = unit.cursor.fetchone()
            if row is None or (
                not args.tickers and int(row[0]) != 9
            ) or (
                args.tickers
                and not set(args.tickers).issubset(set(row[1] or ()))
            ):
                raise ValueError("adjusted-daily repair requires the exact nine-name cohort")
            report = {
                "mode": "PLAN",
                "session_date": args.session_date.isoformat(),
                "members": len(args.tickers) if args.tickers else 9,
                "tickers": args.tickers,
                "adjusted": not args.raw,
                "provider_requests_required": 1,
                "maximum_response_bytes": args.maximum_response_bytes,
                "provider_requests": 0,
                "database_writes": 0,
                "release_advanced": False,
            }
            write_report(args.report_path, report)
            print(json.dumps(report, sort_keys=True))
            return 0
        now = datetime.now(timezone.utc)
        cutoff = _repair_identity_cutoff(args.session_date)
        if args.dated_reference_report is not None:
            identities, identity_continuity = _load_dated_identity(
                provider,
                args,
            )
        else:
            try:
                identities, _ = _load_scope(
                    provider,
                    args.cohort_id,
                    cutoff,
                )
                identity_continuity = ()
            except ValueError:
                if not args.allow_late_identity_continuity:
                    raise
                identities, identity_continuity = (
                    _load_scope_with_identity_continuity(
                        provider,
                        args.cohort_id,
                        cutoff,
                        known_by=now,
                        selected_tickers=(
                            args.tickers if args.tickers else None
                        ),
                    )
                )
        if args.tickers:
            selected = set(args.tickers)
            if identity_continuity:
                pairs = tuple(
                    (identity, continuity)
                    for identity, continuity in zip(
                        identities,
                        identity_continuity,
                    )
                    if identity.ticker in selected
                )
                identities = tuple(row[0] for row in pairs)
                identity_continuity = tuple(row[1] for row in pairs)
            else:
                identities = tuple(
                    identity
                    for identity in identities
                    if identity.ticker in selected
                )
            if {identity.ticker for identity in identities} != selected:
                raise ValueError(
                    "daily repair selected ticker identities are unavailable"
                )
        plan = AdjustedDailyRepairPlan(
            args.cohort_id,
            identities,
            args.session_date,
            args.authorized_until,
            args.maximum_provider_requests,
            args.maximum_response_bytes,
            args.response_byte_limit,
            args.authorization_reference,
            args.provider_authorization_reference,
            args.expected_cost,
            not args.raw,
            bool(args.tickers),
            identity_continuity,
        )
        api_key = os.getenv("POLYGON_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("adjusted-daily repair requires POLYGON_API_KEY")
        transport = PacedPolygonTransport(
            AuthorizedNativeRepairTransport(
                RequestsPolygonHttpTransport(
                    api_key,
                    timeout_seconds=args.timeout_seconds,
                    maximum_response_bytes=args.response_byte_limit,
                ),
                plan.authorized_until,
                lambda: datetime.now(timezone.utc),
            ),
            minimum_interval_seconds=args.minimum_spacing_seconds,
        )
        report = execute_adjusted_daily_repair(
            provider,
            transport,
            plan,
            clock=lambda: datetime.now(timezone.utc),
        )
        write_report(args.report_path, report)
        print(json.dumps(report, sort_keys=True))
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
