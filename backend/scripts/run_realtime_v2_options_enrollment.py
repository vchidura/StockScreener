#!/usr/bin/env python3
"""Run one bounded Developer Options V2 acquire/admission enrollment.

The script provisions only a current passive capability profile, then uses the existing staged
pre-session repositories to persist packages, catalog versions, a MARKET_OPTIONS cohort, its
PRE_SESSION slot set, admission rows and admitted OI baselines. It does not bind OPTIONS_CYCLE,
OPTIONS_FLOW, a websocket, or a continuous service.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

from dotenv import load_dotenv
from psycopg2.pool import ThreadedConnectionPool


WORKSPACE_DIR = Path(__file__).resolve().parents[2]
BACKEND_DIR = WORKSPACE_DIR / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.config import load_option_runtime_configuration  # noqa: E402
from realtime.contracts import DEVELOPER_FORWARD_UNIVERSE  # noqa: E402
from realtime.database_access import RealtimeDatabaseRole  # noqa: E402
from realtime.database_access_v2 import APP_ROLE, V2AccessConfig, V2ConnectionProvider  # noqa: E402
from realtime.forward_runtime import DeveloperForwardBudget, DeveloperForwardScheduler  # noqa: E402
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    SECURITY_MODE_HARDENED,
    V2_MIGRATIONS_DIR,
    load_realtime_migrations,
    schema_fingerprint,
    validate_target_database_v2,
)
from realtime.option_pre_session_runtime import (  # noqa: E402
    _load_scope,
    build_option_pre_session_runtime,
)
from realtime.option_pre_session_staging import StagedAccountScheduler  # noqa: E402
from realtime.option_pre_session_target import OptionPreSessionTargetPlan  # noqa: E402
from realtime.options_v2_runtime import OptionsPreSessionOperations  # noqa: E402
from realtime.preflight import (  # noqa: E402
    PassiveHostEvidence,
    PassivePreflightConfiguration,
    PassivePreflightEvidence,
    PassiveSchemaEvidence,
    ProviderTier,
    run_passive_preflight,
)
from realtime.preflight_repository import persist_preflight_profile_evidence_cursor  # noqa: E402
from realtime.relation_registry import security_policy_sha256  # noqa: E402
from realtime.stage_database_v2 import V2WorkerDatabase  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402
from scripts.run_realtime_option_pre_session import (  # noqa: E402
    _forward_nine_environment,
    build_developer_forward_process_plan,
)


MAXIMUM_PROVIDER_REQUESTS = 360
MAXIMUM_RESPONSE_BYTES = 67_108_864
MINIMUM_REQUEST_INTERVAL_SECONDS = 0.5
MAXIMUM_DURATION_SECONDS = 300


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--deadline", type=datetime.fromisoformat, required=True)
    parser.add_argument("--authorized-underlyings", nargs="+", required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode=SECURITY_MODE_HARDENED,
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("Options enrollment targets only stocks_realtime_v2")
    if args.deadline.tzinfo is None or args.deadline.utcoffset() is None:
        parser.error("--deadline must be timezone-aware")
    args.deadline = args.deadline.astimezone(timezone.utc)
    args.authorized_underlyings = tuple(
        symbol.strip().upper() for symbol in args.authorized_underlyings
    )
    if args.report_path.exists():
        parser.error("--report-path must be new")
    if not args.authorization_reference.strip():
        parser.error("--authorization-reference cannot be blank")
    return args


def _pool() -> ThreadedConnectionPool:
    load_dotenv(BACKEND_DIR / ".env", override=True)
    dsn = os.getenv("REALTIME_V2_APP_DSN", "").strip()
    if not dsn:
        raise RuntimeError("REALTIME_V2_APP_DSN is required")
    return ThreadedConnectionPool(1, 2, dsn=dsn)


def _persist_current_blocked_profile(database, checked_at: datetime):
    migrations = load_realtime_migrations(V2_MIGRATIONS_DIR)
    versions = tuple(migration.version for migration in migrations)
    schema_sha256 = schema_fingerprint(migrations)
    policy_sha256 = security_policy_sha256()
    configuration = PassivePreflightConfiguration(
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        account_alias="polygon-developer-shadow",
        universe=DEVELOPER_FORWARD_UNIVERSE,
        expected_schema_sha256=schema_sha256,
        expected_security_policy_sha256=policy_sha256,
    )
    evidence = PassivePreflightEvidence(
        schema=PassiveSchemaEvidence(
            EXPECTED_V2_DATABASE_NAME,
            versions,
            schema_sha256,
            policy_sha256,
            True,
        ),
        host=PassiveHostEvidence(
            local_filesystem=False,
            journal_mode="UNVERIFIED",
            journal_synchronous=0,
            planned_journal_bytes=1,
            usable_disk_bytes=1,
            clock_verified=False,
        ),
    )
    profile = run_passive_preflight(
        configuration,
        evidence,
        checked_at=checked_at,
        expected_database_name=EXPECTED_V2_DATABASE_NAME,
        expected_schema_versions=versions,
    )
    if (
        profile.provider_tier is not ProviderTier.MASSIVE_DEVELOPER
        or profile.universe_sha256 != DEVELOPER_FORWARD_UNIVERSE.sha256
        or profile.verdict.value not in {"FAIL", "UNAVAILABLE"}
    ):
        raise ValueError("Options V2 passive profile did not remain blocked Developer")
    with database.transaction() as cursor:
        persist_preflight_profile_evidence_cursor(cursor, profile)
    return profile


def _configuration_environment():
    arguments = SimpleNamespace(
        env_file=BACKEND_DIR / ".env",
        developer_policy="options/policies/developer_forward_liquidity_v2.json",
        valuation_policy="options/policies/valuation_zero_yield_research_v1.json",
        strategy_policy="options/policies/strategy_technical_forward_v2.json",
        research_variant="zero-yield-research-v1",
    )
    return _forward_nine_environment(arguments, execute=True)


def _report_base(args):
    return {
        "database": args.database,
        "session_date": args.session_date.isoformat(),
        "authorization_reference": args.authorization_reference,
        "authorized_underlyings": list(args.authorized_underlyings),
        "canonical_underlyings": list(DEVELOPER_FORWARD_UNIVERSE.option_underlyings),
        "maximum_provider_requests": MAXIMUM_PROVIDER_REQUESTS,
        "maximum_response_bytes": MAXIMUM_RESPONSE_BYTES,
        "minimum_request_interval_seconds": MINIMUM_REQUEST_INTERVAL_SECONDS,
        "maximum_duration_seconds": MAXIMUM_DURATION_SECONDS,
        "advanced_or_websocket_enabled": False,
        "provider_requests": 0,
        "response_bytes": 0,
        "database_writes": False,
    }


def main(argv=None) -> int:
    args = parse_args(argv)
    report = _report_base(args)
    if args.authorized_underlyings != DEVELOPER_FORWARD_UNIVERSE.option_underlyings:
        report.update(
            status="BLOCKED",
            reason_codes=["AUTHORIZED_UNIVERSE_IDENTITY_MISMATCH"],
        )
        write_report(args.report_path, report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 2
    if not args.execute:
        report.update(status="PLAN", reason_codes=[])
        write_report(args.report_path, report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    started_at = datetime.now(timezone.utc)
    if (
        started_at.date() != args.session_date
        or not started_at < args.deadline
        or (args.deadline - started_at).total_seconds() > MAXIMUM_DURATION_SECONDS
    ):
        report.update(status="BLOCKED", reason_codes=["AUTHORIZED_CLOCK_WINDOW_INVALID"])
        write_report(args.report_path, report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 2
    environment = _configuration_environment()
    configuration = load_option_runtime_configuration(environment, BACKEND_DIR)
    process_plan = build_developer_forward_process_plan(
        args.database,
        environ=environment,
        configuration=configuration,
        maximum_provider_requests=MAXIMUM_PROVIDER_REQUESTS,
        maximum_response_bytes=MAXIMUM_RESPONSE_BYTES,
        maximum_target_dte_deviation=4,
        target_dtes=(7, 21, 45),
        staged=True,
    )
    pool = _pool()
    runtime = None
    try:
        provider = V2ConnectionProvider(
            {APP_ROLE: pool},
            V2AccessConfig(
                security_mode=SECURITY_MODE_HARDENED,
                role_prefix="stocks_realtime_v2",
                statement_timeout_ms=120_000,
            ),
        )
        provider.verify()
        references = V2WorkerDatabase(provider, RealtimeDatabaseRole.REFERENCES)
        control = V2WorkerDatabase(provider, RealtimeDatabaseRole.CONTROL)
        profile = _persist_current_blocked_profile(references, started_at)
        scope = _load_scope(
            references, started_at, DEVELOPER_FORWARD_UNIVERSE
        )
        target_plan = OptionPreSessionTargetPlan(
            scope=scope,
            policy=process_plan.manifest_policy,
            session_date=args.session_date,
            scheduled_cycle=started_at,
            effective_at=started_at,
            maximum_provider_requests=MAXIMUM_PROVIDER_REQUESTS,
            maximum_response_bytes=MAXIMUM_RESPONSE_BYTES,
        )
        budget = DeveloperForwardBudget(
            maximum_requests=MAXIMUM_PROVIDER_REQUESTS,
            maximum_response_bytes=MAXIMUM_RESPONSE_BYTES,
            maximum_attempts=min(
                3,
                configuration.policy.provider_requests.maximum_rate_limit_retries + 1,
            ),
        )
        bootstrap_scheduler = DeveloperForwardScheduler(
            budget, clock=lambda: datetime.now(timezone.utc), wait=time.sleep
        )
        runtime = build_option_pre_session_runtime(
            process_plan,
            scheduled_cycle=started_at,
            effective_at=started_at,
            environ=environment,
            backend_dir=BACKEND_DIR,
            clock=lambda: datetime.now(timezone.utc),
            request_scheduler=bootstrap_scheduler,
            source_deadline=args.deadline,
            prepared_target_plan=target_plan,
            references_database=references,
            control_database=control,
        )
        operations = OptionsPreSessionOperations(
            process_plan.sha256,
            runtime.target_plan,
            runtime.runner.source,
            process_plan.cohort_binding,
            args.deadline,
            uuid4(),
            lambda: datetime.now(timezone.utc),
            scheduler_factory=lambda run_id: StagedAccountScheduler(
                references,
                run_id,
                budget,
                clock=lambda: datetime.now(timezone.utc),
                wait=time.sleep,
            ),
        )
        acquired = operations.acquire(references, control)
        report.update(
            run_id=str(acquired.run_id),
            completed_underlyings=list(acquired.completed_underlyings),
            provider_requests=acquired.provider_requests,
            response_bytes=acquired.response_bytes,
            database_writes=True,
            profile_id=str(profile.profile_id),
        )
        if not acquired.complete:
            report.update(status="FAILED", reason_codes=["OPTIONS_ACQUIRE_INCOMPLETE"])
            write_report(args.report_path, report)
            return 1
        admission = operations.admit(references, control)
        report.update(
            status="COMPLETE" if admission.status == "READY" else "BLOCKED",
            reason_codes=list(admission.reason_codes),
            selection_id=str(admission.selection_id),
            cohort_id=str(admission.cohort_id) if admission.cohort_id else None,
            slot_set_version=admission.slot_set_version,
            slot_contract_count=admission.slot_contract_count,
            elapsed_seconds=(
                datetime.now(timezone.utc) - started_at
            ).total_seconds(),
        )
        write_report(args.report_path, report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if admission.status == "READY" else 2
    except Exception as error:
        report.update(
            status="FAILED",
            error_type=type(error).__name__,
            error_reason=str(error)[:240] if isinstance(error, ValueError) else None,
            database_write_state="UNKNOWN_INSPECT_BEFORE_RETRY",
            elapsed_seconds=(
                datetime.now(timezone.utc) - started_at
            ).total_seconds(),
        )
        write_report(args.report_path, report)
        raise
    finally:
        if runtime is not None:
            runtime.close()
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
