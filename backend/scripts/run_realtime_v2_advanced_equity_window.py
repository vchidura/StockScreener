#!/usr/bin/env python3
"""Run one provider-free Advanced V2 closure window from durable journal bars."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import canonical_sha256  # noqa: E402
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
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.orchestration_repository import (  # noqa: E402
    PostgresOrchestrationRepository,
    WindowSpec,
)
from realtime.advanced_scheduler import (  # noqa: E402
    ADVANCED_WINDOW_BUILD_ID,
    discover_advanced_window_recovery_cursor,
)
from realtime.runner_environment import RunnerEnvironment  # noqa: E402
from realtime.stage_graph import (  # noqa: E402
    STAGES_V3,
    STAGE_CATALOG_VERSION_V3,
)
from realtime.stage_orchestrator import (  # noqa: E402
    StageOrchestrator,
    StageOutcome,
)
from realtime.stage_runners_v2 import build_runners  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402


BASE_STAGES = (
    "EQUITY_INPUT_GATE",
    "EQUITY_MATERIALIZE",
    "EQUITY_PROJECTION",
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--cutoff", type=datetime.fromisoformat, required=True)
    parser.add_argument("--finality-delay-seconds", type=int, required=True)
    parser.add_argument("--completion-budget-seconds", type=int, default=300)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--window-key-suffix", default="")
    parser.add_argument("--retry-release-now", action="store_true")
    parser.add_argument("--reuse-input-receipt", action="store_true")
    parser.add_argument("--reuse-portal-receipt", action="store_true")
    parser.add_argument("--include-alerts", action="store_true")
    parser.add_argument("--include-strategies", action="store_true")
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--publication-reference", default="")
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("Advanced windows target only stocks_realtime_v2")
    if args.cutoff.tzinfo is None or args.cutoff.utcoffset() is None:
        parser.error("--cutoff must be timezone-aware")
    args.cutoff = args.cutoff.astimezone(timezone.utc)
    if args.cutoff.date() != args.session_date:
        parser.error("--cutoff must belong to --session-date in UTC")
    if not 1 <= args.finality_delay_seconds <= 60:
        parser.error("--finality-delay-seconds must be in [1, 60]")
    if not 1 <= args.completion_budget_seconds <= 300:
        parser.error("--completion-budget-seconds must be in [1, 300]")
    if not args.authorization_reference.strip():
        parser.error("--authorization-reference cannot be blank")
    if args.window_key_suffix and (
        not args.window_key_suffix.startswith("-retry")
        or not args.window_key_suffix[len("-retry"):].isdigit()
    ):
        parser.error(
            "--window-key-suffix must be -retry<N>"
        )
    if args.retry_release_now and not args.window_key_suffix:
        parser.error("--retry-release-now requires a retry suffix")
    if (
        args.reuse_input_receipt or args.reuse_portal_receipt
    ) and not args.window_key_suffix:
        parser.error("receipt reuse requires a durable retry identity")
    if args.publish and not args.publication_reference.strip():
        parser.error("--publish requires --publication-reference")
    if not args.publish and args.publication_reference.strip():
        parser.error("--publication-reference requires --publish")
    stages = list(BASE_STAGES)
    if args.include_alerts:
        stages.append("STOCK_ALERTS")
    if args.include_strategies:
        stages.append("STRATEGY_PUBLISH")
    if args.publish:
        stages.append("PORTAL_PUBLISH")
    args.enabled_stages = tuple(stages)
    args.report_path = args.report_path.resolve()
    if args.report_path.exists():
        parser.error("--report-path must be new")
    if not args.execute:
        parser.error("Advanced windows mutate V2 state and require --execute")
    return args


def _provider(config):
    pool = ThreadedConnectionPool(
        1,
        8,
        dsn=config.app_dsn,
        options="-c timezone=UTC",
    )
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
            statement_timeout_ms=300_000,
        ),
    )
    provider.verify()
    return provider, pool


def _ensure_reference_readiness(provider, repository, args):
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        cursor = unit.cursor
        cursor.execute(
            "SELECT seed.seed_id,seed.status,seed.effective_at,"
            "seed.valid_until "
            "FROM rt.seed_current AS current "
            "JOIN rt.seed_manifests AS seed USING(seed_id) "
            "WHERE current.scope='ADVANCED_SPY_REFERENCES'"
        )
        seed = cursor.fetchone()
        cursor.execute(
            "SELECT manifest.stock_count,"
            "count(DISTINCT reference.ticker) "
            "FROM rt.cohorts AS cohort "
            "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
            "JOIN rt.subscription_members AS member USING(manifest_id) "
            "JOIN rt.instruments AS instrument USING(instrument_id) "
            "JOIN ref.security_revisions AS reference USING(security_id) "
            "WHERE cohort.cohort_id=%s AND member.channel='AM' "
            "AND reference.active AND reference.effective_from<=%s "
            "AND (reference.effective_to IS NULL "
            "OR reference.effective_to>%s) "
            "AND reference.observed_at<=%s "
            "GROUP BY manifest.stock_count",
            (
                str(args.cohort_id),
                args.cutoff,
                args.cutoff,
                args.cutoff,
            ),
        )
        reference_row = cursor.fetchone()
    if (
        seed is None
        or seed[1] != "READY"
        or not seed[2] <= args.cutoff < seed[3]
        or reference_row is None
        or int(reference_row[0]) != int(reference_row[1])
    ):
        raise ValueError("Advanced SPY reference readiness is unavailable")
    repository.upsert_readiness(
        args.cohort_id,
        "REFERENCE",
        STAGE_CATALOG_VERSION_V3,
        "READY",
        (),
        f"advanced-reference:{seed[0]}",
        datetime.now(timezone.utc),
    )


def main(argv=None) -> int:
    args = parse_args(argv)
    config = RealtimeV2PipelineConfig.from_mapping(
        dotenv_values(args.env_file)
    )
    if (
        config.equity_pipeline is not EquityPipeline.MASSIVE_ADVANCED
        or config.options_pipeline is not OptionsPipeline.DISABLED
    ):
        raise RuntimeError(
            "Advanced window requires Advanced equity and disabled options"
        )
    provider, pool = _provider(config)
    try:
        repository = PostgresOrchestrationRepository(provider)
        _ensure_reference_readiness(provider, repository, args)
        recovery = None
        if args.reuse_input_receipt or args.reuse_portal_receipt:
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                recovery = discover_advanced_window_recovery_cursor(
                    unit.cursor,
                    cohort_id=args.cohort_id,
                    cutoff=args.cutoff,
                    checked_at=datetime.now(timezone.utc),
                )
            if (
                args.reuse_input_receipt
                and recovery.input_receipt is None
            ):
                raise ValueError(
                    "durable INPUT_READY receipt is unavailable"
                )
            if (
                args.reuse_portal_receipt
                and recovery.portal_receipt is None
            ):
                raise ValueError(
                    "durable PORTAL_READY receipt is unavailable"
                )
        release_at = (
            datetime.now(timezone.utc)
            if args.retry_release_now
            else args.cutoff
            + timedelta(seconds=args.finality_delay_seconds)
        )
        deadline = release_at + timedelta(
            seconds=args.completion_budget_seconds
        )
        scope_reference = (
            f"{args.authorization_reference}:"
            f"{args.publication_reference or 'UNPUBLISHED'}"
        )
        config_sha256 = canonical_sha256(
            {
                "version": ADVANCED_WINDOW_BUILD_ID,
                "cohort_id": args.cohort_id,
                "session_date": args.session_date,
                "cutoff": args.cutoff,
                "window_key_suffix": args.window_key_suffix,
                "retry_release_now": args.retry_release_now,
                "enabled_stages": args.enabled_stages,
                "publication": args.publish,
                "scope_reference": scope_reference,
            }
        )
        spec = WindowSpec(
            args.cohort_id,
            "MASSIVE_ADVANCED",
            (
                f"advanced-{args.cutoff:%Y%m%dT%H%M%SZ}"
                f"{args.window_key_suffix}"
            ),
            release_at,
            args.cutoff,
            deadline,
        )
        existing = next(
            (
                row
                for row in repository.open_windows(args.cohort_id)
                if row.window_key == spec.window_key
            ),
            None,
        )
        if existing is None:
            plan_id = repository.create_plan(
                args.cohort_id,
                args.session_date,
                args.enabled_stages,
                config_sha256=config_sha256,
                deadline=deadline + timedelta(minutes=5),
                scope_reference=scope_reference,
                catalog_version=STAGE_CATALOG_VERSION_V3,
            )
            window = repository.ensure_window(plan_id, spec)
        else:
            plan_id = existing.plan_id
            window = existing
            retained_plan = repository.load_plan(plan_id)
            if (
                retained_plan.stage_catalog_version
                != STAGE_CATALOG_VERSION_V3
                or retained_plan.enabled_stages
                != frozenset(args.enabled_stages)
            ):
                raise ValueError(
                    "existing Advanced window plan identity changed"
                )
        environment = RunnerEnvironment(
            provider,
            build_id=ADVANCED_WINDOW_BUILD_ID,
            freeze_heap=True,
        )
        runners = build_runners(
            set(args.enabled_stages),
            provider,
            environment=environment,
        )
        if args.reuse_input_receipt:
            input_receipt = recovery.input_receipt
            runners["EQUITY_INPUT_GATE"] = (
                lambda context: StageOutcome.complete(
                    evidence_ref=(
                        "durable-input-gate:"
                        f"{context.window.cutoff.isoformat()}"
                    )
                )
            )
            runners["EQUITY_MATERIALIZE"] = (
                lambda _context: StageOutcome.complete(
                    receipt=input_receipt,
                    evidence_ref=(
                        "reused-input-ready:"
                        f"{input_receipt.input_sha256}"
                    ),
                )
            )
        if args.reuse_portal_receipt and "PORTAL_PUBLISH" in runners:
            portal_receipt = recovery.portal_receipt
            runners["PORTAL_PUBLISH"] = (
                lambda _context: StageOutcome.complete(
                    receipt=portal_receipt,
                    evidence_ref=(
                        "reused-portal-ready:"
                        f"{portal_receipt.input_sha256}"
                    ),
                )
            )
        orchestrator = StageOrchestrator(
            repository,
            runners,
            database=provider,
            stages=STAGES_V3,
            catalog_version=STAGE_CATALOG_VERSION_V3,
            max_lane_seconds=args.completion_budget_seconds,
        )
        orchestrator.start(plan_id)
        report = orchestrator.run_window(window.window_id)
        result = {
            "status": report.status,
            "window_state": report.window_state,
            "plan_id": str(plan_id),
            "window_id": str(window.window_id),
            "cohort_id": str(args.cohort_id),
            "cutoff": args.cutoff.isoformat(),
            "enabled_stages": args.enabled_stages,
            "publication_advanced": args.publish,
            "provider_requests": 0,
            "credentials_exposed": False,
            "executed": report.executed,
            "lane_errors": report.lane_errors,
        }
        write_report(args.report_path, result)
        print(json.dumps(result, indent=2))
        return 0 if report.window_state == "COMPLETE" else 2
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
