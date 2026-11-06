#!/usr/bin/env python3
"""Run one provider-free Advanced V2 closure window from durable journal bars."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import (  # noqa: E402
    EQUITY_VALIDATION_UNIVERSE,
    canonical_sha256,
)
from realtime.advanced_equity_publication_authority import (  # noqa: E402
    advanced_equity_publication_authority_from_payload,
    validate_advanced_equity_publication_authority,
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
    RealtimeV2Process,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.orchestration_repository import (  # noqa: E402
    PostgresOrchestrationRepository,
    WindowSpec,
)
from realtime.portal_projection_repository import (  # noqa: E402
    load_portal_release_pointer_cursor,
)
from realtime.preflight import ProviderTier  # noqa: E402
from realtime.advanced_options_validation_mode import (  # noqa: E402
    AdvancedOptionsValidationMode,
    advanced_options_projection_policy,
)
from realtime.advanced_scheduler import (  # noqa: E402
    ADVANCED_WINDOW_BUILD_ID,
    discover_advanced_window_recovery_cursor,
)
from realtime.runner_environment import RunnerEnvironment  # noqa: E402
from realtime.screening_delta_loader import ScreeningInputLoader  # noqa: E402
from realtime.ticker_projection import TickerProjectionState  # noqa: E402
from realtime.stage_graph import (  # noqa: E402
    STAGES_V3,
    STAGE_CATALOG_VERSION_V3,
)
from realtime.stage_orchestrator import (  # noqa: E402
    StageOrchestrator,
    StageOutcome,
)
from realtime.stage_runners_equity import (  # noqa: E402
    reuse_advanced_input_ready_runner,
    reuse_advanced_portal_ready_runner,
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
    parser.add_argument("--execution-deadline", type=datetime.fromisoformat)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--window-key-suffix", default="")
    parser.add_argument("--retry-release-now", action="store_true")
    parser.add_argument("--reuse-input-receipt", action="store_true")
    parser.add_argument("--reuse-portal-receipt", action="store_true")
    parser.add_argument("--include-alerts", action="store_true")
    parser.add_argument("--include-strategies", action="store_true")
    parser.add_argument("--history-binding-report", type=Path)
    parser.add_argument("--history-binding-file-sha256")
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--continuous-publication", action="store_true")
    parser.add_argument("--publication-reference", default="")
    parser.add_argument("--publication-authority", type=Path)
    parser.add_argument("--publication-authority-semantic-sha256")
    parser.add_argument("--publication-authority-file-sha256")
    parser.add_argument("--report-path", type=Path, required=True)
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
    if args.execution_deadline is not None:
        if (
            args.execution_deadline.tzinfo is None
            or args.execution_deadline.utcoffset() is None
        ):
            parser.error("--execution-deadline must be timezone-aware")
        if not args.retry_release_now:
            parser.error(
                "--execution-deadline requires --retry-release-now"
            )
        args.execution_deadline = args.execution_deadline.astimezone(
            timezone.utc
        )
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
    if args.publish and args.continuous_publication:
        parser.error(
            "--continuous-publication replaces the one-shot --publish path"
        )
    if args.publish and not args.publication_reference.strip():
        parser.error("--publish requires --publication-reference")
    if not args.publish and args.publication_reference.strip():
        parser.error("--publication-reference requires --publish")
    publication_values = (
        args.publication_authority,
        args.publication_authority_semantic_sha256,
        args.publication_authority_file_sha256,
    )
    if args.publish and any(value is None for value in publication_values):
        parser.error("--publish requires the exact publication authority")
    if not args.publish and any(
        value is not None for value in publication_values
    ):
        parser.error("publication authority requires --publish")
    if args.reuse_portal_receipt and not args.reuse_input_receipt:
        parser.error("PORTAL_READY reuse requires INPUT_READY reuse")
    history_values = (
        args.history_binding_report,
        args.history_binding_file_sha256,
    )
    if args.include_strategies and any(
        value is None for value in history_values
    ):
        parser.error(
            "Advanced strategies require exact bound history evidence"
        )
    if not args.include_strategies and any(
        value is not None for value in history_values
    ):
        parser.error(
            "history binding evidence requires --include-strategies"
        )
    if args.publish and (
        not args.retry_release_now
        or not args.reuse_input_receipt
        or not args.reuse_portal_receipt
    ):
        parser.error(
            "P11 publication requires an explicit durable receipt retry"
        )
    stages = list(BASE_STAGES)
    if args.include_alerts:
        stages.append("STOCK_ALERTS")
    if args.include_strategies:
        stages.append("STRATEGY_PUBLISH")
    if args.publish or args.continuous_publication:
        stages.append("PORTAL_PUBLISH")
    args.enabled_stages = tuple(stages)
    args.report_path = args.report_path.resolve()
    if args.history_binding_report is not None:
        args.history_binding_report = (
            args.history_binding_report.resolve()
        )
        if (
            not args.history_binding_report.is_file()
            or len(args.history_binding_file_sha256) != 64
            or any(
                value not in "0123456789abcdef"
                for value in args.history_binding_file_sha256
            )
        ):
            parser.error(
                "Advanced strategy history evidence is invalid"
            )
    if args.publication_authority is not None:
        args.publication_authority = (
            args.publication_authority.resolve()
        )
        if not args.publication_authority.is_file():
            parser.error("--publication-authority must exist")
        for name in (
            "publication_authority_semantic_sha256",
            "publication_authority_file_sha256",
        ):
            value = getattr(args, name)
            if len(value) != 64 or any(
                character not in "0123456789abcdef"
                for character in value
            ):
                parser.error(
                    f"--{name.replace('_', '-')} must be a SHA-256"
                )
    if args.report_path.exists():
        parser.error("--report-path must be new")
    if not args.execute:
        parser.error("Advanced windows mutate V2 state and require --execute")
    return args


def _validate_strategy_history_binding(args):
    if not args.include_strategies:
        return None
    body = args.history_binding_report.read_bytes()
    report = json.loads(body)
    coverage = report.get("coverage") or {}
    if (
        hashlib.sha256(body).hexdigest()
        != args.history_binding_file_sha256
        or report.get("mode") != "EXECUTE"
        or report.get("status") != "BOUND"
        or report.get("target_cohort_id") != str(args.cohort_id)
        or tuple(sorted(report.get("symbols") or ()))
        != tuple(sorted(EQUITY_VALIDATION_UNIVERSE.equity_tickers))
        or int(coverage.get("adjusted_daily_sessions") or 0) < 283
        or int(coverage.get("raw_daily_sessions") or 0) < 90
        or int(coverage.get("five_minute_bars") or 0) < 1
        or int(coverage.get("one_hour_bars") or 0) < 1
        or report.get("provider_requests") != 0
    ):
        raise ValueError(
            "Advanced strategy history binding is incomplete"
        )
    return report


def _load_publication_authority(
    args,
    *,
    recovery,
    prior_pointer,
    checked_at,
):
    body = args.publication_authority.read_bytes()
    document = json.loads(body)
    unsigned = dict(document)
    semantic_sha256 = unsigned.pop("semantic_sha256", None)
    if (
        hashlib.sha256(body).hexdigest()
        != args.publication_authority_file_sha256
        or semantic_sha256
        != args.publication_authority_semantic_sha256
        or canonical_sha256(unsigned) != semantic_sha256
        or document.get("contract")
        != "advanced-equity-p11-publication-authority-document-v1"
    ):
        raise ValueError("P11 publication authority identity changed")
    authority = advanced_equity_publication_authority_from_payload(
        document.get("authority")
    )
    if (
        authority.authorization_reference
        != args.publication_reference
    ):
        raise ValueError("P11 publication reference changed")
    validate_advanced_equity_publication_authority(
        authority,
        cohort_id=args.cohort_id,
        session_date=args.session_date,
        cutoff=args.cutoff,
        window_key_suffix=args.window_key_suffix,
        input_receipt=recovery.input_receipt,
        portal_receipt=recovery.portal_receipt,
        prior_release_id=(
            None
            if prior_pointer is None
            else prior_pointer.release_id
        ),
        prior_cutoff=(
            None if prior_pointer is None else prior_pointer.cutoff
        ),
        checked_at=checked_at,
    )
    return authority


def _validation_policy(args, environment):
    policy = advanced_options_projection_policy(environment)
    if (
        policy.validation_mode
        is AdvancedOptionsValidationMode.DEVELOPMENT_DIAGNOSTIC
        and (args.publish or args.continuous_publication)
    ):
        raise RuntimeError(
            "development diagnostic Equity windows cannot publish"
        )
    return policy


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


def _load_stock_alert_projection(provider, request):
    from realtime.canonical_stock_alert_repository import (
        load_canonical_stock_alert_projection_cursor,
    )

    with provider.transaction(
        APP_ROLE,
        read_only=True,
        dict_cursor=True,
        isolation_level="REPEATABLE READ",
    ) as unit:
        unit.cursor.execute(
            "SET LOCAL search_path TO rt,ref,pg_catalog"
        )
        return load_canonical_stock_alert_projection_cursor(
            unit.cursor,
            request,
        )


def _continuous_reference_evidence_cursor(cursor, args):
    """Prove a continuous desired-state cohort from its members' own facts.

    C4 owns these facts per member, while a global seed is a separate
    short-lived manual refresh that no continuous service renews. Returns None
    for any other cohort, which must still present a current global seed.
    """
    cursor.execute(
        "SELECT snapshot_sha256 "
        "FROM rt.realtime_desired_session_snapshots "
        "WHERE cohort_id=%s AND session_date=%s AND materialized_at<=%s",
        (str(args.cohort_id), args.session_date, args.cutoff),
    )
    snapshot = cursor.fetchone()
    if snapshot is None:
        return None
    cursor.execute(
        "SELECT manifest.stock_count,"
        "count(DISTINCT instrument.instrument_id),"
        "(SELECT count(*) FROM rt.subscription_members AS declared "
        "WHERE declared.manifest_id=manifest.manifest_id "
        "AND declared.channel='AM') "
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
        "AND NOT EXISTS ("
        "SELECT 1 FROM unnest(ARRAY['SPLIT','DIVIDEND']) "
        "AS required(action_type) "
        "WHERE NOT EXISTS ("
        "SELECT 1 FROM ref.corporate_action_coverage AS coverage "
        "WHERE coverage.security_id=reference.security_id "
        "AND coverage.action_type=required.action_type "
        "AND coverage.window_start<=%s AND coverage.window_end>=%s "
        "AND coverage.observed_at>=reference.effective_from "
        "AND coverage.received_at<=%s"
        ")) "
        "GROUP BY manifest.manifest_id,manifest.stock_count",
        (
            str(args.cohort_id),
            args.cutoff,
            args.cutoff,
            args.cutoff,
            args.session_date,
            args.session_date,
            args.cutoff,
        ),
    )
    members = cursor.fetchone()
    if members is None or len({int(value) for value in members}) != 1:
        raise ValueError(
            "Advanced continuous member references are unavailable"
        )
    return f"advanced-reference-c4:{snapshot[0]}"


def _seed_reference_evidence_cursor(cursor, args):
    cursor.execute(
        "SELECT seed.seed_id,seed.status,seed.effective_at,"
        "seed.valid_until "
        "FROM rt.seed_current AS current "
        "JOIN rt.seed_manifests AS seed USING(seed_id) "
        "WHERE current.scope IN ("
        "'EQUITY_VALIDATION_REFERENCES',"
        "'ADVANCED_SPY_REFERENCES') "
        "ORDER BY "
        "(current.scope='EQUITY_VALIDATION_REFERENCES') DESC "
        "LIMIT 1"
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
        raise ValueError("Advanced reference readiness is unavailable")
    return f"advanced-reference:{seed[0]}"


def _ensure_reference_readiness(provider, repository, args):
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        continuous_evidence = _continuous_reference_evidence_cursor(
            unit.cursor,
            args,
        )
        evidence_ref = (
            continuous_evidence
            or _seed_reference_evidence_cursor(unit.cursor, args)
        )
    if args.continuous_publication and continuous_evidence is None:
        raise ValueError(
            "continuous publication requires a desired-snapshot cohort"
        )
    repository.upsert_readiness(
        args.cohort_id,
        "REFERENCE",
        STAGE_CATALOG_VERSION_V3,
        "READY",
        (),
        evidence_ref,
        datetime.now(timezone.utc),
    )


class _WarmProjection:
    """G85: the incremental loader and projection state, kept between windows of one cohort in this process.

    The Advanced scheduler runs every window through `main()` in its own process. A new `RunnerEnvironment`
    per window used to start both empty, so each window re-read every retained bar and projected every ticker
    from scratch (Nov 5, 9 tickers: about 33 s cold against 3.6 s warm, identical output). A window gets the
    warm pair only when its cutoff is newer than the last one this pair served, for the same cohort; a retry or
    recovery of an older or equal cutoff gets a fresh pair and leaves the warm one alone, as rehydration does.
    A window that does not complete, or raises, drops the warm pair, so the next window starts cold."""

    def __init__(self):
        self.cohort_id = None
        self.cutoff = None
        self.state = None
        self.loader = None

    def take(self, cohort_id, cutoff):
        if self.cohort_id == cohort_id and self.cutoff is not None and cutoff > self.cutoff:
            return self.state, self.loader, True
        return TickerProjectionState(), ScreeningInputLoader(), False

    def settle(self, cohort_id, cutoff, state, loader, *, warm, complete):
        if warm:
            if complete:
                self.cutoff = cutoff
            else:
                self.drop()
            return
        # A fresh pair becomes the warm one only when it is now the newest for its cohort (first window after a
        # start, or a new cohort); an older or equal cutoff (retry, recovery) leaves the warm pair as it was.
        if complete and (self.cohort_id != cohort_id or self.cutoff is None or cutoff > self.cutoff):
            self.cohort_id, self.cutoff, self.state, self.loader = cohort_id, cutoff, state, loader

    def drop(self):
        self.cohort_id = self.cutoff = self.state = self.loader = None


_WARM = _WarmProjection()


def dashboard_due(cutoff, *, warm: bool) -> bool:
    """The Dashboard is rebuilt on a process's first window for a cohort (session start, restart, retry with a
    fresh pair) and hourly on the :35 windows; PORTAL_PUBLISH adds the session close. In between the previous one
    is carried forward. The user accepts a stale Dashboard (Nov 6): it is a sample page awaiting a redesign."""
    return not warm or cutoff.minute == 35


def main(argv=None) -> int:
    args = parse_args(argv)
    ticker_state, screening_loader, warm = _WARM.take(args.cohort_id, args.cutoff)
    complete = False
    try:
        code = _main(
            args, ticker_state=ticker_state, screening_loader=screening_loader,
            dashboard_due=dashboard_due(args.cutoff, warm=warm),
        )
        complete = code == 0
        return code
    finally:
        _WARM.settle(
            args.cohort_id, args.cutoff, ticker_state, screening_loader,
            warm=warm, complete=complete,
        )


def _main(args, *, ticker_state, screening_loader, dashboard_due=True) -> int:
    strategy_history = _validate_strategy_history_binding(args)
    validation_policy = _validation_policy(args, os.environ)
    config = RealtimeV2PipelineConfig.from_mapping(
        os.environ,
        process=RealtimeV2Process.APP_WORKER,
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
        recovery = None
        prior_pointer = None
        publication_authority = None
        if args.reuse_input_receipt or args.reuse_portal_receipt:
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                recovery = discover_advanced_window_recovery_cursor(
                    unit.cursor,
                    cohort_id=args.cohort_id,
                    cutoff=args.cutoff,
                    checked_at=datetime.now(timezone.utc),
                )
                if args.publish:
                    prior_pointer = load_portal_release_pointer_cursor(
                        unit.cursor,
                        cohort_id=args.cohort_id,
                        provider_tier=ProviderTier.MASSIVE_ADVANCED,
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
            if args.publish:
                publication_authority = _load_publication_authority(
                    args,
                    recovery=recovery,
                    prior_pointer=prior_pointer,
                    checked_at=datetime.now(timezone.utc),
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
        if args.execution_deadline is not None:
            deadline = min(deadline, args.execution_deadline)
            if deadline <= release_at:
                return 2
        _ensure_reference_readiness(provider, repository, args)
        scope_reference = (
            f"{args.authorization_reference}:"
            + (
                "CONTINUOUS-PUBLICATION"
                if args.continuous_publication
                else args.publication_reference or "UNPUBLISHED"
            )
        )
        config_sha256 = canonical_sha256(
            {
                "version": ADVANCED_WINDOW_BUILD_ID,
                "cohort_id": args.cohort_id,
                "session_date": args.session_date,
                "cutoff": args.cutoff,
                "window_key_suffix": args.window_key_suffix,
                "retry_release_now": args.retry_release_now,
                "execution_deadline": args.execution_deadline,
                "enabled_stages": args.enabled_stages,
                "validation_mode": (
                    validation_policy.validation_mode.value
                ),
                "publication": args.publish or args.continuous_publication,
                "publication_authority_sha256": (
                    None
                    if publication_authority is None
                    else publication_authority.authority_sha256
                ),
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
                args.continuous_publication
                and retained_plan.enabled_stages == frozenset(BASE_STAGES)
            ):
                # A window planned before publication finishes as planned;
                # the next window advances the pointer.
                args.continuous_publication = False
                args.enabled_stages = BASE_STAGES
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
            ticker_state=ticker_state,
            screening_loader=screening_loader,
            dashboard_due=dashboard_due,
            freeze_heap=True,
            stock_alert_projection_loader=(
                (
                    lambda request: _load_stock_alert_projection(
                        provider,
                        request,
                    )
                )
                if args.include_alerts
                else None
            ),
            input_known_by=(
                recovery.input_known_by
                if args.reuse_input_receipt
                else None
            ),
        )
        runners = build_runners(
            set(args.enabled_stages),
            provider,
            environment=environment,
        )
        if args.reuse_input_receipt:
            input_receipt = recovery.input_receipt
            if (
                recovery.input_known_by is None
                or recovery.input_gate_sha256 is None
            ):
                raise ValueError(
                    "durable INPUT_READY recovery scope is unavailable"
                )
            runners["EQUITY_INPUT_GATE"] = (
                lambda _context: StageOutcome.complete(
                    evidence_ref=(
                        "reused-input-gate:"
                        f"{recovery.input_gate_sha256}"
                    ),
                )
            )
            runners["EQUITY_MATERIALIZE"] = (
                reuse_advanced_input_ready_runner(
                    environment,
                    receipt=input_receipt,
                    input_known_by=recovery.input_known_by,
                    expected_gate_sha256=(
                        recovery.input_gate_sha256
                    ),
                )
            )
        if args.reuse_portal_receipt:
            portal_receipt = recovery.portal_receipt
            runners["EQUITY_PROJECTION"] = (
                reuse_advanced_portal_ready_runner(
                    environment,
                    receipt=portal_receipt,
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
        pointer = None
        pointer_advanced = False
        if args.publish:
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                pointer = load_portal_release_pointer_cursor(
                    unit.cursor,
                    cohort_id=args.cohort_id,
                    provider_tier=ProviderTier.MASSIVE_ADVANCED,
                )
            pointer_advanced = (
                pointer is not None
                and (
                    prior_pointer is None
                    or pointer.release_id != prior_pointer.release_id
                )
            )
            if (
                report.window_state == "COMPLETE"
                and not pointer_advanced
            ):
                raise RuntimeError(
                    "P11 publication did not advance one cohort-local pointer"
                )
            if (
                report.window_state != "COMPLETE"
                and pointer_advanced
            ):
                raise RuntimeError(
                    "P11 failed publication advanced the pointer"
                )
        elif args.continuous_publication:
            # A late backlog window is stored without moving the pointer, so
            # the pointer is reported here and never required to advance.
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                pointer = load_portal_release_pointer_cursor(
                    unit.cursor,
                    cohort_id=args.cohort_id,
                    provider_tier=ProviderTier.MASSIVE_ADVANCED,
                )
        result = {
            "status": report.status,
            "window_state": report.window_state,
            "plan_id": str(plan_id),
            "window_id": str(window.window_id),
            "cohort_id": str(args.cohort_id),
            "cutoff": args.cutoff.isoformat(),
            "enabled_stages": args.enabled_stages,
            "validation_mode": (
                validation_policy.validation_mode.value
            ),
            "terminal_receipt": (
                validation_policy.equity_terminal_receipt
            ),
            "portal_projection_complete": (
                report.window_state == "COMPLETE"
                and any(
                    stage_id == "EQUITY_PROJECTION"
                    and state == "COMPLETE"
                    for stage_id, state in report.executed
                )
            ),
            "publication_mode": (
                "COHORT_LOCAL_PORTAL_PUBLISH"
                if args.publish
                else "CONTINUOUS_PORTAL_PUBLISH"
                if args.continuous_publication
                else "PROJECTION_ONLY"
            ),
            "publication_advanced": args.publish,
            "publication_authority_id": (
                None
                if publication_authority is None
                else str(publication_authority.authority_id)
            ),
            "prior_release_id": (
                None
                if prior_pointer is None
                else str(prior_pointer.release_id)
            ),
            "current_release_id": (
                None if pointer is None else str(pointer.release_id)
            ),
            "pointer_advances": 1 if pointer_advanced else 0,
            "release_allowed_by_validation_mode": (
                validation_policy.release_allowed
            ),
            "pointer_allowed_by_validation_mode": (
                validation_policy.pointer_allowed
            ),
            "read_pin_allowed_by_validation_mode": (
                validation_policy.read_pin_allowed
            ),
            "normal_portal_serving_allowed_by_validation_mode": (
                validation_policy.normal_portal_serving_allowed
            ),
            "provider_requests": 0,
            "credentials_exposed": False,
            "strategy_history_binding_sha256": (
                None
                if strategy_history is None
                else strategy_history["binding_sha256"]
            ),
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
