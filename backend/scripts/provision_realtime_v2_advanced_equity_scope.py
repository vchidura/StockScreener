#!/usr/bin/env python3
"""Plan or persist the unpublished SPY/AM Advanced equity control plane."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_control_plane import (  # noqa: E402
    build_advanced_equity_control_plane_plan,
    persist_advanced_equity_control_plane_cursor,
)
from realtime.advanced_ingress_runtime_profile import (  # noqa: E402
    load_advanced_ingress_runtime_catalog,
)
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_pipeline_config import (  # noqa: E402
    EquityPipeline,
    OptionsPipeline,
    RealtimeV2PipelineConfig,
)
from realtime.equity_live024_compatibility import (  # noqa: E402
    LIVE024_SCHEMA_SHA256,
    load_live_preflight_report,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    V2_MIGRATIONS_DIR,
    load_realtime_migrations,
    schema_fingerprint,
    validate_target_database_v2,
)
from scripts.realtime_script_support import write_report  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--symbol", action="append", required=True)
    parser.add_argument("--entitlement-report", type=Path, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--expires-at", type=datetime.fromisoformat, required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--live024-preflight-report", type=Path)
    parser.add_argument("--live024-preflight-report-sha256")
    parser.add_argument("--live024-preflight-approval", type=Path)
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("Advanced scope targets only stocks_realtime_v2")
    if args.expires_at.tzinfo is None or args.expires_at.utcoffset() is None:
        parser.error("--expires-at must be timezone-aware")
    args.expires_at = args.expires_at.astimezone(timezone.utc)
    if not args.authorization_reference.strip():
        parser.error("--authorization-reference cannot be blank")
    args.symbols = tuple(sorted({
        value.strip().upper()
        for value in args.symbol
        if value.strip()
    }))
    if not args.symbols or len(args.symbols) != len(args.symbol):
        parser.error("--symbol values must be unique nonblank tickers")
    args.entitlement_report = args.entitlement_report.resolve()
    if not args.entitlement_report.is_file():
        parser.error("--entitlement-report must exist")
    args.report_path = args.report_path.resolve()
    if args.report_path.exists():
        parser.error("--report-path must be new")
    compatibility_values = (
        args.live024_preflight_report,
        args.live024_preflight_report_sha256,
        args.live024_preflight_approval,
    )
    if any(value is not None for value in compatibility_values) and any(
        value is None for value in compatibility_values
    ):
        parser.error(
            "live-024 report, SHA256 and approval must be supplied together"
        )
    if args.live024_preflight_report is not None:
        args.live024_preflight_report = (
            args.live024_preflight_report.resolve()
        )
        if not args.live024_preflight_report.is_file():
            parser.error("--live024-preflight-report must exist")
        args.live024_preflight_approval = (
            args.live024_preflight_approval.resolve()
        )
        if not args.live024_preflight_approval.is_file():
            parser.error("--live024-preflight-approval must exist")
    return args


def _load_entitlement(path, expected_symbols):
    if path.stat().st_size > 1024 * 1024:
        raise ValueError("entitlement report exceeds its verification bound")
    body = path.read_bytes()
    report = json.loads(body)
    scope = report.get("scope") or {}
    if (
        report.get("status") != "PASS"
        or tuple(sorted(scope.get("authorized_universe") or ()))
        != tuple(expected_symbols)
        or scope.get("authorized_channels") != ["AM"]
        or report.get("credentials_printed") is not False
    ):
        raise ValueError(
            "entitlement report is not exact selected-universe/AM PASS evidence"
        )
    return hashlib.sha256(body).hexdigest(), report["checked_at"]


def _provider(config):
    pool = ThreadedConnectionPool(
        1,
        2,
        dsn=config.app_dsn,
        options="-c timezone=UTC",
    )
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
            statement_timeout_ms=30_000,
        ),
    )
    provider.verify()
    return provider, pool


def _load_compatibility_preflight(args, checked_at):
    if args.live024_preflight_report is None:
        return None
    if args.symbols != ("SPY",):
        raise ValueError("live-024 compatibility is restricted to exact SPY")
    report, digest = load_live_preflight_report(
        args.live024_preflight_report,
        args.live024_preflight_report_sha256,
    )
    if args.live024_preflight_approval.stat().st_size > 1024 * 1024:
        raise ValueError("live-024 provisioning approval exceeds its bound")
    approval = json.loads(
        args.live024_preflight_approval.read_text(encoding="utf-8")
    )
    if (
        approval.get("owner_decision")
        != "approve_equity_live024_scope_provisioning"
        or approval.get("conditional_execution_authorized") is not True
        or approval.get("contract_id") != report["contract_id"]
        or approval.get("preflight_report_sha256") != digest
        or approval.get("qualification_report_sha256")
        != report["qualification_report_sha256"]
        or approval.get("manifest_id") != report.get("manifest_id")
        or approval.get("authorization_reference")
        != args.authorization_reference
    ):
        raise ValueError(
            "live-024 preflight lacks exact scope-provisioning approval"
        )
    observed = datetime.fromisoformat(report["checked_at"]).astimezone(
        timezone.utc
    )
    arm_deadline = datetime.fromisoformat(report["arm_deadline"]).astimezone(
        timezone.utc
    )
    if (
        report.get("database") != EXPECTED_V2_DATABASE_NAME
        or report.get("schema_sha256") != LIVE024_SCHEMA_SHA256
        or observed > checked_at
        or checked_at - observed > timedelta(minutes=10)
        or checked_at >= arm_deadline
    ):
        raise ValueError(
            "live-024 preflight is stale, out of scope or past its arm deadline"
        )
    return report, digest


def main(argv=None):
    args = parse_args(argv)
    config = RealtimeV2PipelineConfig.from_mapping(
        dotenv_values(args.env_file)
    )
    if (
        config.equity_pipeline is not EquityPipeline.MASSIVE_ADVANCED
        or config.options_pipeline is not OptionsPipeline.DISABLED
    ):
        raise RuntimeError(
            "Advanced scope requires Advanced equity and disabled options"
        )
    evidence_sha256, evidence_checked_at = _load_entitlement(
        args.entitlement_report,
        args.symbols,
    )
    evidence_checked = datetime.fromisoformat(
        evidence_checked_at
    ).astimezone(timezone.utc)
    checked = datetime.now(timezone.utc)
    if (
        checked < evidence_checked
        or checked - evidence_checked > timedelta(hours=4)
    ):
        raise ValueError("entitlement evidence is outside its four-hour TTL")
    runtime_catalog = load_advanced_ingress_runtime_catalog(
        config.advanced_ingress_policy_path,
        expected_version=config.advanced_ingress_policy_version,
    )
    runtime_profile = runtime_catalog.profile_for_target(len(args.symbols))
    runtime_policy_sha256 = runtime_catalog.scale_policy.sha256
    configuration_sha256 = runtime_profile.configuration_sha256(
        runtime_policy_sha256
    )
    provider, pool = _provider(config)
    try:
        compatibility = _load_compatibility_preflight(args, checked)
        with provider.transaction(APP_ROLE, read_only=True) as unit:
            cursor = unit.cursor
            schema_sha256 = (
                schema_fingerprint(load_realtime_migrations(V2_MIGRATIONS_DIR))
                if compatibility is None
                else compatibility[0]["schema_sha256"]
            )
            cursor.execute(
                (
                    "SELECT policy_sha256 "
                    "FROM rt.database_security_policies "
                    "WHERE policy_version='realtime_security_v3'"
                )
                if compatibility is not None
                else (
                    "SELECT security_policy_sha256 "
                    "FROM rt.capability_profiles "
                    "ORDER BY created_at DESC,profile_id DESC LIMIT 1"
                )
            )
            security = cursor.fetchone()
            if security is None:
                raise ValueError("V2 security policy identity is unavailable")
            if (
                compatibility is not None
                and security[0].strip()
                != compatibility[0]["security_policy_sha256"]
            ):
                raise ValueError(
                    "live security policy differs from compatibility preflight"
                )
            cursor.execute(
                "SELECT DISTINCT ON (ticker) ticker,security_id "
                "FROM ref.security_revisions "
                "WHERE ticker=ANY(%s) AND active "
                "AND effective_from<=%s "
                "AND (effective_to IS NULL OR effective_to>%s) "
                "AND observed_at<=%s "
                "ORDER BY ticker,observed_at DESC,revision_id DESC",
                (list(args.symbols), checked, checked, checked),
            )
            security_ids = {
                str(ticker): security_id
                for ticker, security_id in cursor.fetchall()
            }
            if tuple(sorted(security_ids)) != args.symbols:
                raise ValueError(
                    "selected Advanced security identities are unavailable"
                )
            cursor.execute(
                "SELECT status,configuration_sha256,"
                "runtime_policy_sha256 "
                "FROM rt.advanced_ingress_runtime_profiles "
                "WHERE runtime_profile_sha256=%s",
                (runtime_profile.sha256,),
            )
            stored_profile = cursor.fetchone()
            if (
                stored_profile is None
                or stored_profile[0] != "ACTIVE"
                or stored_profile[1] != configuration_sha256
                or stored_profile[2] != runtime_policy_sha256
            ):
                raise ValueError(
                    "selected universe runtime profile is not ACTIVE"
                )
            if (
                compatibility is not None
                and (
                    runtime_profile.sha256
                    != compatibility[0]["runtime_profile_sha256"]
                    or runtime_policy_sha256
                    != compatibility[0]["runtime_policy_sha256"]
                )
            ):
                raise ValueError(
                    "runtime profile differs from compatibility preflight"
                )
        plan = build_advanced_equity_control_plane_plan(
            session_date=args.session_date,
            security_ids=security_ids,
            entitlement_evidence_sha256=evidence_sha256,
            authorization_reference=args.authorization_reference,
            checked_at=checked,
            expires_at=args.expires_at,
            schema_sha256=schema_sha256,
            security_policy_sha256=security[0],
            configuration_sha256=configuration_sha256,
        )
        if args.execute:
            with provider.transaction(APP_ROLE) as unit:
                persist_advanced_equity_control_plane_cursor(
                    unit.cursor,
                    plan,
                )
        report = {
            "mode": "EXECUTE" if args.execute else "PLAN",
            "status": "READY" if args.execute else "PLANNED",
            "cohort_id": str(plan.cohort_id),
            "profile_id": str(plan.profile_id),
            "manifest_id": str(plan.manifest_id),
            "provider_tier": "MASSIVE_ADVANCED",
            "symbols": args.symbols,
            "channels": ("AM",),
            "options_pipeline": "DISABLED",
            "portal_activated": False,
            "publication_advanced": False,
            "provider_requests": 0,
            "credentials_exposed": False,
            "compatibility_contract": (
                None if compatibility is None else compatibility[0]["contract_id"]
            ),
            "compatibility_preflight_sha256": (
                None if compatibility is None else compatibility[1]
            ),
            "schema_sha256": schema_sha256,
        }
        write_report(args.report_path, report)
        print(json.dumps(report, indent=2))
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
