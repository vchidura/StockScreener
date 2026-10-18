#!/usr/bin/env python3
"""Run one authorized Developer acquisition window or provider-free portal correction."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

import exchange_calendars
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.data.polygon_http import RequestsPolygonHttpTransport  # noqa: E402
from realtime.contracts import EQUITY_VALIDATION_UNIVERSE, canonical_sha256  # noqa: E402
from realtime.database_access import RealtimeDatabaseRole  # noqa: E402
from realtime.database_access_v2 import APP_ROLE, V2AccessConfig, V2ConnectionProvider  # noqa: E402
from realtime.equity_v2_backfill_source import AccountingPolygonTransport  # noqa: E402
from realtime.equity_v2_window_source import PolygonEquityV2WindowSource  # noqa: E402
from realtime.equity_bar_drain import drain_equity_bar_jobs  # noqa: E402
from realtime.equity_validation_source import (  # noqa: E402
    EquityValidationIdentity,
    EquityValidationSourceLimits,
    PolygonEquityValidationHistorySource,
)
from realtime.migrations import EXPECTED_V2_DATABASE_NAME, validate_target_database_v2  # noqa: E402
from realtime.orchestration_repository import (  # noqa: E402
    CycleReceiptSpec, EQUITY_WINDOW_BUILD_ID, PostgresOrchestrationRepository, WindowSpec,
    equity_window_recovery_cursor,
)
from realtime.reference_seed import SeedStatus  # noqa: E402
from realtime.runner_environment import RunnerEnvironment  # noqa: E402
from realtime.screening_delta_loader import ScreeningInputLoader  # noqa: E402
from realtime.stage_database_v2 import V2StageDatabase, V2WorkerDatabase  # noqa: E402
from realtime.stage_graph import STAGE_CATALOG_VERSION_V2, STAGES_V2  # noqa: E402
from realtime.stage_orchestrator import StageOrchestrator  # noqa: E402
from realtime.stage_orchestrator import StageOutcome  # noqa: E402
from realtime.stage_runners_equity import (  # noqa: E402
    INPUT_READY,
    input_ready_identity,
    portal_publish_runner,
    reference_generation,
)
from realtime.stage_runners_v2 import build_runners, unwired_stages, wired_stages  # noqa: E402
from realtime.stage_runners_ingest import EquityAcquisitionRequest, acquire_equity_bars  # noqa: E402
from realtime.ticker_projection import _security  # noqa: E402
from realtime.v2_service import V2Service  # noqa: E402
from scripts.realtime_script_support import PacedPolygonTransport, write_report  # noqa: E402

ENABLED_STAGES = tuple(
    stage for stage in STAGES_V2
    if not stage.startswith("OPTIONS_")
)
PORTAL_REPROJECTION_STAGES = (
    "REFERENCE",
    "EQUITY_INGEST",
    "EQUITY_MATERIALIZE",
    "EQUITY_PROJECTION",
    "PORTAL_PUBLISH",
)
PROJECTION_ONLY_STAGES = tuple(
    stage
    for stage in PORTAL_REPROJECTION_STAGES
    if stage != "PORTAL_PUBLISH"
)
PUBLICATION_ONLY_STAGES = ("PORTAL_PUBLISH",)
MINIMUM_REQUEST_INTERVAL_SECONDS = 0.5


class ExistingReferenceService:
    def __init__(self, manifest):
        self.manifest = manifest

    def run_once(self, request):
        if request.universe != EQUITY_VALIDATION_UNIVERSE:
            raise ValueError("existing reference service scope changed")
        return SimpleNamespace(manifest=self.manifest)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--window-start", type=datetime.fromisoformat, required=True)
    parser.add_argument("--cutoff", type=datetime.fromisoformat, required=True)
    parser.add_argument("--cache-directory", type=Path, required=True)
    parser.add_argument("--authorization-reference", default="")
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--window-key-suffix", default="")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--resume-prefill", action="store_true")
    parser.add_argument("--reuse-current-input", action="store_true")
    parser.add_argument("--portal-reprojection", action="store_true")
    parser.add_argument("--projection-only", action="store_true")
    parser.add_argument("--publication-only", action="store_true")
    parser.add_argument("--publication-source-window-id", type=UUID)
    parser.add_argument(
        "--allow-late-identity-continuity",
        action="store_true",
    )
    parser.add_argument("--serial-lanes", action="store_true")
    parser.add_argument("--wait-for-input-receipt", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(args.database, args.confirm_database_name)
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("the Developer window targets only stocks_realtime_v2")
    if args.cutoff.tzinfo is None or args.cutoff.utcoffset() is None:
        parser.error("--cutoff must be timezone-aware")
    if args.window_start.tzinfo is None or args.window_start.utcoffset() is None:
        parser.error("--window-start must be timezone-aware")
    args.window_start = args.window_start.astimezone(timezone.utc)
    args.cutoff = args.cutoff.astimezone(timezone.utc)
    if args.cutoff.minute % 15 or args.cutoff.second or args.cutoff.microsecond:
        parser.error("--cutoff must be a 15-minute boundary")
    if len(args.cutoff.isoformat() + args.window_key_suffix) > 64:
        parser.error("cutoff plus --window-key-suffix exceeds the durable window-key bound")
    if (
        args.window_start >= args.cutoff
        or int((args.cutoff - args.window_start).total_seconds()) % 300
    ):
        parser.error("--window-start must define complete five-minute windows")
    if args.report_path.exists():
        parser.error("--report-path must be new")
    if args.execute and not args.authorization_reference.strip():
        parser.error("--execute requires --authorization-reference")
    if args.window_key_suffix.startswith("-receipt-retry") and not args.reuse_current_input:
        parser.error("receipt retries must reuse sealed current input")
    if args.resume_prefill and args.reuse_current_input:
        parser.error("prefill cannot be combined with receipt-only recovery")
    if args.portal_reprojection:
        if args.resume_prefill or args.reuse_current_input:
            parser.error("portal reprojection cannot acquire or reuse input")
        if args.publication_only:
            if not args.window_key_suffix.startswith(
                "-portal-publish-recovery"
            ):
                parser.error(
                    "publication-only recovery requires a "
                    "-portal-publish-recovery suffix"
                )
        elif not args.window_key_suffix.startswith("-portal-correction"):
            parser.error(
                "portal reprojection requires a -portal-correction suffix"
            )
    if args.projection_only and not args.portal_reprojection:
        parser.error("--projection-only requires --portal-reprojection")
    if args.publication_only and (
        not args.portal_reprojection
        or args.projection_only
        or args.publication_source_window_id is None
    ):
        parser.error(
            "--publication-only requires --portal-reprojection, an exact "
            "--publication-source-window-id and no --projection-only"
        )
    if (
        not args.publication_only
        and args.publication_source_window_id is not None
    ):
        parser.error(
            "--publication-source-window-id requires --publication-only"
        )
    if (
        args.allow_late_identity_continuity
        and not args.portal_reprojection
    ):
        parser.error(
            "late identity continuity is limited to portal reprojection"
        )
    return args


def _load_historical_reference_manifest(provider, cutoff, known_by):
    with provider.transaction(read_only=True, dict_cursor=True) as unit:
        unit.cursor.execute(
            "SELECT seed.seed_id,seed.seed_sha256,seed.universe_sha256,"
            "seed.session_date,seed.status,seed.reason_codes,"
            "seed.member_count,seed.created_at,seed.effective_at,"
            "seed.valid_until,seed.source_generations "
            "FROM rt.seed_manifests AS seed "
            "WHERE seed.status='READY' AND seed.universe_sha256=%s "
            "AND seed.created_at<=%s AND seed.effective_at<=%s "
            "AND seed.valid_until>%s "
            "ORDER BY seed.created_at DESC,seed.seed_id DESC LIMIT 1",
            (
                EQUITY_VALIDATION_UNIVERSE.sha256,
                known_by,
                known_by,
                known_by,
            ),
        )
        seed = unit.cursor.fetchone()
        unit.cursor.execute(
            "SELECT seed_id FROM rt.seed_manifests "
            "WHERE status='READY' AND universe_sha256=%s "
            "AND created_at<=%s "
            "ORDER BY created_at DESC,seed_id DESC LIMIT 1",
            (EQUITY_VALIDATION_UNIVERSE.sha256, cutoff),
        )
        prior = unit.cursor.fetchone()
    if seed is None or prior is None or seed["created_at"] <= cutoff:
        raise ValueError(
            "Developer historical reference seed continuity is unavailable"
        )
    return SimpleNamespace(
        seed_id=seed["seed_id"],
        seed_sha256=seed["seed_sha256"],
        universe_sha256=seed["universe_sha256"],
        session_date=seed["session_date"],
        status=SeedStatus.READY,
        reason_codes=tuple(seed["reason_codes"]),
        member_count=seed["member_count"],
        created_at=seed["created_at"],
        effective_at=seed["effective_at"],
        valid_until=seed["valid_until"],
        source_generations=tuple(
            sorted(seed["source_generations"].items())
        ),
    )


def _pool(dsn):
    return ThreadedConnectionPool(1, 12, dsn=dsn)


def _load_scope(provider, cohort_id, cutoff):
    with provider.transaction(read_only=True, dict_cursor=True) as unit:
        cursor = unit.cursor
        cursor.execute(
            "SELECT DISTINCT ON (revision.ticker) revision.*,"
            "revision.revision_id AS security_revision_id,instrument.instrument_id "
            "FROM ref.security_revisions AS revision "
            "JOIN rt.instruments AS instrument USING (security_id) "
            "WHERE revision.ticker=ANY(%s) AND revision.active "
            "AND revision.effective_from<=%s AND revision.effective_to>%s "
            "AND revision.observed_at<=%s "
            "ORDER BY revision.ticker,revision.observed_at DESC,revision.revision_id DESC",
            (list(EQUITY_VALIDATION_UNIVERSE.equity_tickers), cutoff, cutoff, cutoff),
        )
        rows = tuple(cursor.fetchall())
        cursor.execute(
            "SELECT seed.seed_id,seed.seed_sha256,seed.universe_sha256,seed.session_date,"
            "seed.status,seed.reason_codes,seed.member_count,seed.created_at,seed.effective_at,"
            "seed.valid_until,seed.source_generations "
            "FROM rt.seed_current AS current JOIN rt.seed_manifests AS seed USING(seed_id) "
            "WHERE current.scope='EQUITY_VALIDATION_REFERENCES'"
        )
        seed = cursor.fetchone()
        cursor.execute(
            "SELECT manifest_id FROM rt.cohorts WHERE cohort_id=%s",
            (str(cohort_id),),
        )
        cohort = cursor.fetchone()
    identities = {
        row["ticker"]: EquityValidationIdentity(
            row["ticker"], _security(row), row["instrument_id"])
        for row in rows
    }
    if set(identities) != set(EQUITY_VALIDATION_UNIVERSE.equity_tickers):
        raise ValueError(
            "Developer window has incomplete current identities: "
            f"found={sorted(identities)} "
            f"expected={sorted(EQUITY_VALIDATION_UNIVERSE.equity_tickers)} "
            f"cutoff={cutoff.isoformat()}"
        )
    if (
        seed is None
        or seed["status"] != "READY"
        or seed["universe_sha256"] != EQUITY_VALIDATION_UNIVERSE.sha256
        or not seed["effective_at"] <= cutoff < seed["valid_until"]
        or cohort is None
    ):
        raise ValueError("Developer window current seed or cohort is invalid")
    manifest = SimpleNamespace(
        seed_id=seed["seed_id"],
        seed_sha256=seed["seed_sha256"],
        universe_sha256=seed["universe_sha256"],
        session_date=seed["session_date"],
        status=SeedStatus.READY,
        reason_codes=tuple(seed["reason_codes"]),
        member_count=seed["member_count"],
        created_at=seed["created_at"],
        effective_at=seed["effective_at"],
        valid_until=seed["valid_until"],
        source_generations=tuple(sorted(seed["source_generations"].items())),
    )
    return tuple(
        identities[ticker]
        for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers
    ), manifest


def _existing_revision_ids(provider, cohort_id, window_start, cutoff):
    with provider.transaction(read_only=True) as unit:
        unit.cursor.execute(
            "SELECT bar_revision_id FROM rt.bar_catalog "
            "WHERE cohort_id=%s AND interval IN ('1m','5m','15m','30m','1h') "
            "AND bar_start>=%s AND bar_end<=%s",
            (str(cohort_id), window_start, cutoff),
        )
        return frozenset(UUID(str(row[0])) for row in unit.cursor.fetchall())


def _existing_window_spec(provider, cohort_id, cutoff):
    key = cutoff.isoformat()
    with provider.transaction(read_only=True) as unit:
        unit.cursor.execute(
            "SELECT provider_tier,release_at,deadline FROM rt.window_runs "
            "WHERE cohort_id=%s AND window_key=%s",
            (str(cohort_id), key),
        )
        row = unit.cursor.fetchone()
    return None if row is None else WindowSpec(
        cohort_id, row[0], key, row[1], cutoff, row[2])


def _validate_cutoff(cutoff):
    calendar = exchange_calendars.get_calendar("XNYS")
    session = cutoff.date()
    if (
        not calendar.is_session(str(session))
        or not calendar.session_open(str(session)).to_pydatetime() < cutoff
        <= calendar.session_close(str(session)).to_pydatetime()
    ):
        raise ValueError("Developer window cutoff is outside the XNYS cash session")
    if datetime.now(timezone.utc) < cutoff + timedelta(minutes=15):
        raise ValueError("Developer window cutoff is not available under the tier delay")
    return session


def _publication_recovery_state(
    provider,
    *,
    cohort_id,
    cutoff,
    source_window_id,
):
    with provider.transaction(read_only=True, dict_cursor=True) as unit:
        cursor = unit.cursor
        cursor.execute(
            "SELECT window_run.window_id,window_run.window_key,"
            "window_run.state,window_run.provider_tier,window_run.cutoff,"
            "window_run.deadline,plan.plan_id,plan.session_date,"
            "plan.stage_catalog_version,plan.enabled_stages "
            "FROM rt.window_runs AS window_run "
            "JOIN rt.service_plans AS plan USING(plan_id) "
            "WHERE window_run.window_id=%s AND window_run.cohort_id=%s "
            "AND window_run.cutoff=%s",
            (str(source_window_id), str(cohort_id), cutoff),
        )
        source = cursor.fetchone()
        if source is None:
            raise ValueError(
                "publication source window does not match the exact cohort "
                "and cutoff"
            )
        source = dict(source)
        cursor.execute(
            "SELECT DISTINCT ON (stage_id) stage_id,state,attempt "
            "FROM rt.stage_runs WHERE window_id=%s "
            "ORDER BY stage_id,attempt DESC",
            (str(source_window_id),),
        )
        stage_runs = {
            row["stage_id"]: dict(row)
            for row in cursor.fetchall()
        }
        cursor.execute(
            "SELECT receipt_id,receipt_kind,cohort_id,cutoff,"
            "reference_generation,universe_sha256,input_sha256,build_id "
            "FROM rt.cycle_receipts WHERE window_id=%s "
            "AND receipt_kind=ANY(%s) ORDER BY receipt_kind",
            (
                str(source_window_id),
                [INPUT_READY, "PORTAL_READY"],
            ),
        )
        receipts = {
            row["receipt_kind"]: dict(row)
            for row in cursor.fetchall()
        }
        cursor.execute(
            "SELECT release_id,cutoff,session_date,updated_at "
            "FROM rt.portal_release_current WHERE cohort_id=%s "
            "AND provider_tier='MASSIVE_DEVELOPER'",
            (str(cohort_id),),
        )
        current_release = cursor.fetchone()
        cursor.execute(
            "SELECT scope,cohort_id,provider_tier,activation_id,"
            "effective_at,updated_at FROM rt.portal_cohort_current "
            "WHERE scope='PLATFORM'",
        )
        platform_pointer = cursor.fetchone()
        cursor.execute(
            "SELECT "
            "(SELECT COUNT(*) FROM rt.snapshot_catalog "
            "WHERE cohort_id=%s) AS snapshot_catalog,"
            "(SELECT COUNT(*) FROM rt.portal_snapshot_hot "
            "WHERE cohort_id=%s) AS snapshot_hot,"
            "(SELECT COUNT(*) FROM rt.portal_snapshot_kept "
            "WHERE cohort_id=%s) AS snapshot_kept,"
            "(SELECT COUNT(*) FROM rt.portal_releases "
            "WHERE cohort_id=%s) AS releases",
            tuple(str(cohort_id) for _ in range(4)),
        )
        publication_counts = dict(cursor.fetchone())
        cursor.execute(
            "SELECT COUNT(*) AS total_requests,"
            "COALESCE(SUM(response_bytes),0) AS total_response_bytes,"
            "COUNT(*) FILTER (WHERE cohort_id=%s) AS cohort_requests,"
            "COALESCE(SUM(response_bytes) FILTER (WHERE cohort_id=%s),0) "
            "AS cohort_response_bytes FROM rt.provider_request_ledger",
            (str(cohort_id), str(cohort_id)),
        )
        provider_ledger = dict(cursor.fetchone())
    if (
        source["state"] != "COMPLETE"
        or source["provider_tier"] != "MASSIVE_DEVELOPER"
        or source["cutoff"] != cutoff
        or source["session_date"] != cutoff.date()
        or source["stage_catalog_version"] != STAGE_CATALOG_VERSION_V2
        or set(source["enabled_stages"]) != set(PROJECTION_ONLY_STAGES)
    ):
        raise ValueError(
            "publication source is not the completed Developer "
            "projection-only window"
        )
    if any(
        stage_runs.get(stage, {}).get("state") != "COMPLETE"
        for stage in ("EQUITY_MATERIALIZE", "EQUITY_PROJECTION")
    ):
        raise ValueError(
            "publication source lacks completed materialization or projection"
        )
    if set(receipts) != {INPUT_READY, "PORTAL_READY"}:
        raise ValueError(
            "publication source lacks exact INPUT_READY and PORTAL_READY "
            "receipts"
        )
    input_receipt = receipts[INPUT_READY]
    portal_receipt = receipts["PORTAL_READY"]
    if any(
        receipt["cohort_id"] != cohort_id
        or receipt["cutoff"] != cutoff
        or receipt["reference_generation"] < 1
        or receipt["build_id"] != EQUITY_WINDOW_BUILD_ID
        or len(receipt["universe_sha256"]) != 64
        or len(receipt["input_sha256"]) != 64
        for receipt in receipts.values()
    ) or (
        input_receipt["reference_generation"]
        != portal_receipt["reference_generation"]
    ):
        raise ValueError(
            "publication source receipts disagree with the Developer "
            "recovery scope"
        )
    return {
        "source_window": source,
        "stage_runs": stage_runs,
        "receipts": receipts,
        "current_release": (
            dict(current_release)
            if current_release is not None
            else None
        ),
        "platform_pointer": (
            dict(platform_pointer)
            if platform_pointer is not None
            else None
        ),
        "publication_counts": publication_counts,
        "provider_ledger": provider_ledger,
    }


def _publication_effects(state, *, cutoff, session_close):
    return {
        "release_receipt_id": str(
            state["receipts"][INPUT_READY]["receipt_id"]
        ),
        "projection_receipt_id": str(
            state["receipts"]["PORTAL_READY"]["receipt_id"]
        ),
        "release_classes": [
            "INTRADAY",
            *(["SESSION_CLOSE"] if session_close else []),
        ],
        "cohort_current_pointer": {
            "table": "rt.portal_release_current",
            "before": state["current_release"],
            "after_cutoff": cutoff,
        },
        "platform_pointer": {
            "table": "rt.portal_cohort_current",
            "effect": "UNCHANGED",
            "identity": state["platform_pointer"],
        },
        "provider_effect": "ZERO_EXTERNAL_REQUESTS",
    }


def main(argv=None):
    args = parse_args(argv)
    session_date = _validate_cutoff(args.cutoff)
    enabled_stages = (
        PUBLICATION_ONLY_STAGES
        if args.publication_only
        else PROJECTION_ONLY_STAGES
        if args.projection_only
        else PORTAL_REPROJECTION_STAGES
        if args.portal_reprojection
        else ENABLED_STAGES
    )
    calendar = exchange_calendars.get_calendar("XNYS")
    if args.window_start != calendar.session_open(str(session_date)).to_pydatetime():
        raise ValueError("Developer replacement window must start at the XNYS open")
    plan = {
        "mode": "EXECUTE" if args.execute else "PLAN",
        "database": args.database,
        "cohort_id": str(args.cohort_id),
        "session_date": session_date.isoformat(),
        "window_start": args.window_start.isoformat(),
        "cutoff": args.cutoff.isoformat(),
        "window_key_suffix": args.window_key_suffix,
        "enabled_stages": sorted(enabled_stages),
        "provider_request_ceiling": 0 if args.portal_reprojection else 18,
        "minimum_request_interval_seconds": MINIMUM_REQUEST_INTERVAL_SECONDS,
    }
    if not args.execute:
        if not args.publication_only:
            print(json.dumps({
                **plan,
                "provider_requests": 0,
                "database_writes": 0,
            }, sort_keys=True))
            return 0
        load_dotenv(BACKEND_DIR / ".env", override=True)
        app_dsn = os.getenv("REALTIME_V2_APP_DSN", "").strip()
        if not app_dsn:
            raise RuntimeError(
                "Developer publication preview requires REALTIME_V2_APP_DSN"
            )
        preview_pool = _pool(app_dsn)
        try:
            preview_provider = V2ConnectionProvider(
                {APP_ROLE: preview_pool},
                V2AccessConfig(
                    security_mode="hardened",
                    role_prefix=EXPECTED_V2_DATABASE_NAME,
                    statement_timeout_ms=300_000,
                ),
            )
            preview_provider.verify()
            publication_state = _publication_recovery_state(
                preview_provider,
                cohort_id=args.cohort_id,
                cutoff=args.cutoff,
                source_window_id=args.publication_source_window_id,
            )
        finally:
            preview_pool.closeall()
        print(json.dumps({
            **plan,
            "publication_source": publication_state,
            "publication_effects": _publication_effects(
                publication_state,
                cutoff=args.cutoff,
                session_close=True,
            ),
            "provider_requests": 0,
            "database_writes": 0,
        }, default=str, sort_keys=True))
        return 0
    load_dotenv(BACKEND_DIR / ".env", override=True)
    app_dsn = os.getenv("REALTIME_V2_APP_DSN", "").strip()
    api_key = os.getenv("POLYGON_API_KEY", "").strip()
    if not app_dsn:
        raise RuntimeError("Developer window requires REALTIME_V2_APP_DSN")
    if not args.reuse_current_input and not args.portal_reprojection and not api_key:
        raise RuntimeError("Developer acquisition requires POLYGON_API_KEY")
    pool = _pool(app_dsn)
    started_at = datetime.now(timezone.utc)
    try:
        provider = V2ConnectionProvider(
            {APP_ROLE: pool},
            V2AccessConfig(
                security_mode="hardened",
                role_prefix=EXPECTED_V2_DATABASE_NAME,
                statement_timeout_ms=300_000,
            ),
        )
        provider.verify()
        publication_before = None
        if args.publication_only:
            publication_before = _publication_recovery_state(
                provider,
                cohort_id=args.cohort_id,
                cutoff=args.cutoff,
                source_window_id=args.publication_source_window_id,
            )
        if args.reuse_current_input:
            with provider.transaction(read_only=True) as unit:
                recovery = equity_window_recovery_cursor(
                    unit.cursor, args.cohort_id, args.cutoff
                )
            if not recovery["has_input_receipt"]:
                raise ValueError("current-input recovery requires a matching durable INPUT_READY receipt")
        if args.publication_only:
            identities, manifest = (), None
        elif (
            args.portal_reprojection
            and args.allow_late_identity_continuity
        ):
            identities = ()
            manifest = _load_historical_reference_manifest(
                provider,
                args.cutoff,
                datetime.now(timezone.utc),
            )
        else:
            identities, manifest = _load_scope(
                provider,
                args.cohort_id,
                args.cutoff,
            )
        transport = history_source = equity_source = None
        if not args.reuse_current_input and not args.portal_reprojection:
            existing_revision_ids = _existing_revision_ids(
                provider, args.cohort_id, args.window_start, args.cutoff)
            transport = AccountingPolygonTransport(
                PacedPolygonTransport(
                    RequestsPolygonHttpTransport(api_key, timeout_seconds=30),
                    minimum_interval_seconds=MINIMUM_REQUEST_INTERVAL_SECONDS,
                ),
                cohort_id=args.cohort_id,
                clock=lambda: datetime.now(timezone.utc),
                request_ceiling=18,
                cache_directory=args.cache_directory,
                request_identity_suffix=args.window_key_suffix or (
                    "resume-prefill-v2" if args.resume_prefill else ""),
            )
            history_source = PolygonEquityValidationHistorySource(
                transport, identities, cohort_id=args.cohort_id,
                limits=EquityValidationSourceLimits(
                    maximum_requests=18, maximum_pages_per_request=1,
                    maximum_retries_per_request=0, maximum_workers=1,
                ),
            )
            equity_source = PolygonEquityV2WindowSource(
                history_source, cutoff=args.cutoff, window_start=args.window_start,
                existing_revision_ids=existing_revision_ids,
                segment_identity_suffix=args.window_key_suffix or (
                    "resume-prefill-v2" if args.resume_prefill else ""),
            )
        prefill = None
        if args.resume_prefill:
            if equity_source is None:
                raise ValueError("prefill requires an acquisition source")
            acquisition = acquire_equity_bars(
                V2WorkerDatabase(provider, RealtimeDatabaseRole.INGRESS),
                equity_source,
                EquityAcquisitionRequest(
                    args.cohort_id,
                    EQUITY_VALIDATION_UNIVERSE,
                    args.cutoff,
                ),
            )
            drained = drain_equity_bar_jobs(
                V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE),
                cohort_id=args.cohort_id,
                lease_owner=uuid5(
                    NAMESPACE_URL,
                    f"v2-window-prefill:{args.cohort_id}:{args.cutoff.isoformat()}",
                ),
                max_jobs=len(acquisition.acquisition_batch_ids),
            )
            if drained.unsuccessful or drained.retried:
                raise RuntimeError("Developer window prefill did not materialize cleanly")
            prefill = {
                "facts": acquisition.fact_count,
                "batches": len(acquisition.acquisition_batch_ids),
                "bars": drained.bars,
            }
        environment = RunnerEnvironment(
            provider,
            build_id=EQUITY_WINDOW_BUILD_ID,
            reference_service=(
                ExistingReferenceService(manifest)
                if manifest is not None
                else None
            ),
            equity_source=equity_source,
            universe=EQUITY_VALIDATION_UNIVERSE,
            reference_scope="EQUITY_VALIDATION_REFERENCES",
            input_known_by=(
                publication_before["source_window"]["deadline"]
                if args.publication_only
                else recovery["input_known_by"]
                if args.reuse_current_input
                else None
            ),
            freeze_heap=True,
            screening_loader=(
                None
                if args.allow_late_identity_continuity
                else ScreeningInputLoader()
            ),
            allow_late_identity_continuity=(
                args.allow_late_identity_continuity
            ),
        )
        repository = PostgresOrchestrationRepository(provider)
        config_sha256 = canonical_sha256({
            "version": EQUITY_WINDOW_BUILD_ID,
            "cohort_id": args.cohort_id,
            "window_start": args.window_start,
            "cutoff": args.cutoff,
            "window_key_suffix": args.window_key_suffix,
            "enabled_stages": enabled_stages,
            "portal_reprojection": args.portal_reprojection,
            "projection_only": args.projection_only,
            "publication_only": args.publication_only,
            "publication_source_window_id": (
                str(args.publication_source_window_id)
                if args.publication_source_window_id is not None
                else None
            ),
            "allow_late_identity_continuity": (
                args.allow_late_identity_continuity
            ),
        })
        plan_id = repository.create_plan(
            args.cohort_id,
            session_date,
            enabled_stages,
            config_sha256=config_sha256,
            deadline=started_at + timedelta(minutes=30),
            scope_reference=args.authorization_reference,
            catalog_version=STAGE_CATALOG_VERSION_V2,
        )
        if unwired_stages(enabled_stages) or set(wired_stages(enabled_stages)) != set(enabled_stages):
            raise RuntimeError("Developer window stage wiring changed")
        runners = build_runners(enabled_stages, provider, environment=environment)
        if args.publication_only:
            runners["PORTAL_PUBLISH"] = portal_publish_runner(
                environment,
                receipt_window_id=args.publication_source_window_id,
            )
        if args.reuse_current_input:
            input_receipt = recovery["input_receipt"]
            if not isinstance(input_receipt, CycleReceiptSpec):
                raise ValueError("current-input recovery lacks an exact sealed INPUT_READY receipt")
            runners["EQUITY_INGEST"] = lambda context: StageOutcome.complete(
                evidence_ref=f"durable-current-input:{context.window.cutoff.isoformat()}")
            runners["EQUITY_MATERIALIZE"] = lambda _context: StageOutcome.complete(
                receipt=input_receipt,
                evidence_ref=f"reused-input-ready:{input_receipt.input_sha256}",
            )
        if args.portal_reprojection:
            runners["EQUITY_INGEST"] = lambda context: StageOutcome.complete(
                evidence_ref=f"durable-repaired-input:{context.window.cutoff.isoformat()}")

            def seal_repaired_input(context):
                compute = V2StageDatabase(
                    context,
                    RealtimeDatabaseRole.COMPUTE,
                )
                generation = reference_generation(
                    compute,
                    (
                        environment.known_by(context.window)
                        if environment.allow_late_identity_continuity
                        else context.window.cutoff
                    ),
                )
                if generation < 1:
                    return StageOutcome.blocked("REFERENCE_GENERATION_UNAVAILABLE")
                universe_sha256, input_sha256 = input_ready_identity(
                    compute,
                    cohort_id=context.plan.cohort_id,
                    cutoff=context.window.cutoff,
                    known_by=environment.known_by(context.window),
                    reference_generation=generation,
                    allow_late_identity_continuity=(
                        environment.allow_late_identity_continuity
                    ),
                )
                return StageOutcome.complete(receipt=CycleReceiptSpec(
                    INPUT_READY,
                    context.window.cutoff,
                    generation,
                    universe_sha256,
                    input_sha256,
                    environment.build_id,
                ))

            runners["EQUITY_MATERIALIZE"] = seal_repaired_input
        if args.wait_for_input_receipt:
            for stage_id in (
                set(enabled_stages)
                & {"STOCK_ALERTS", "EQUITY_PROJECTION", "STRATEGY_PUBLISH"}
            ):
                delegate = runners[stage_id]

                def wait_for_receipt(context, delegate=delegate):
                    deadline = time.monotonic() + 120
                    while time.monotonic() < deadline and not context.is_cancelled():
                        with provider.transaction(read_only=True) as unit:
                            unit.cursor.execute(
                                "SELECT 1 FROM rt.cycle_receipts "
                                "WHERE window_id=%s AND receipt_kind='INPUT_READY'",
                                (str(context.window.window_id),),
                            )
                            if unit.cursor.fetchone() is not None:
                                return delegate(context)
                        time.sleep(0.1)
                    return StageOutcome.failed("INPUT_READY_RECEIPT_MISSING")

                runners[stage_id] = wait_for_receipt
        orchestrator = StageOrchestrator(
            repository,
            runners,
            database=provider,
            stages=STAGES_V2,
            catalog_version=STAGE_CATALOG_VERSION_V2,
            max_lane_seconds=600,
            lane_grace_seconds=120,
            parallel_lanes=not args.serial_lanes,
        )
        orchestrator.start(plan_id)
        now = datetime.now(timezone.utc)
        window_key = args.cutoff.isoformat() + args.window_key_suffix
        spec = (
            _existing_window_spec(provider, args.cohort_id, args.cutoff)
            if not args.window_key_suffix else None
        ) or WindowSpec(
            args.cohort_id,
            "MASSIVE_DEVELOPER",
            window_key,
            now,
            args.cutoff,
            now + timedelta(minutes=10),
        )
        service = V2Service(orchestrator, repository, plan_id, (spec,))
        reference_state = (
            "NOT_RUN"
            if args.publication_only
            else service.tick_resident_once("REFERENCE")
        )
        poll = service.poll_windows()
        with provider.transaction(read_only=True) as unit:
            unit.cursor.execute(
                "SELECT state FROM rt.window_runs "
                "WHERE cohort_id=%s AND window_key=%s",
                (str(args.cohort_id), spec.window_key),
            )
            durable_window = unit.cursor.fetchone()
            unit.cursor.execute(
                "SELECT stage_id,state FROM rt.stage_runs "
                "WHERE window_id=(SELECT window_id FROM rt.window_runs "
                "WHERE cohort_id=%s AND window_key=%s) "
                "AND attempt=(SELECT MAX(latest.attempt) FROM rt.stage_runs AS latest "
                "WHERE latest.window_id=stage_runs.window_id "
                "AND latest.stage_id=stage_runs.stage_id) "
                "ORDER BY stage_id",
                (str(args.cohort_id), spec.window_key),
            )
            durable_stages = tuple(unit.cursor.fetchall())
        durable_complete = (
            durable_window == ("COMPLETE",)
            and len(durable_stages) == (
                len(enabled_stages)
                - int("REFERENCE" in enabled_stages)
            )
            and all(state == "COMPLETE" for _, state in durable_stages)
        )
        if (
            reference_state not in {"COMPLETE", "DEGRADED", "NOT_RUN"}
            or poll.window_errors
            or not durable_complete
            or not poll.windows
        ):
            raise RuntimeError("Developer window did not complete every bound stage")
        with provider.transaction(read_only=True, dict_cursor=True) as unit:
            unit.cursor.execute(
                "SELECT "
                "(SELECT COUNT(*) FROM rt.cycle_receipts WHERE cohort_id=%s AND cutoff=%s) AS receipts,"
                "(SELECT COUNT(*) FROM rt.screening_current WHERE cohort_id=%s) AS generations,"
                "(SELECT COUNT(*) FROM rt.portal_release_current WHERE cohort_id=%s) AS current_releases",
                (
                    str(args.cohort_id), args.cutoff,
                    str(args.cohort_id),
                    str(args.cohort_id),
                ),
            )
            persisted = dict(unit.cursor.fetchone())
        publication_after = (
            _publication_recovery_state(
                provider,
                cohort_id=args.cohort_id,
                cutoff=args.cutoff,
                source_window_id=args.publication_source_window_id,
            )
            if args.publication_only
            else None
        )
        completed_at = datetime.now(timezone.utc)
        report = {
            **plan,
            "status": "COMPLETE",
            "plan_id": str(plan_id),
            "reference_state": reference_state,
            "window": asdict(poll.windows[0]),
            "persisted": persisted,
            "prefill": prefill,
            "publication_before": publication_before,
            "publication_after": publication_after,
            "publication_effects": (
                _publication_effects(
                    publication_before,
                    cutoff=args.cutoff,
                    session_close=True,
                )
                if publication_before is not None
                else None
            ),
            "provider_requests": transport.external_requests if transport is not None else 0,
            "provider_cache_hits": transport.cache_hits if transport is not None else 0,
            "logical_provider_requests": history_source.provider_requests if history_source is not None else 0,
            "provider_response_bytes": history_source.response_bytes if history_source is not None else 0,
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "duration_seconds": (completed_at - started_at).total_seconds(),
        }
        write_report(args.report_path.resolve(), json.loads(json.dumps(report, default=str)))
        print(json.dumps({
            "state": "COMPLETE",
            "plan_id": str(plan_id),
            "window_status": poll.windows[0].status,
            "executed": poll.windows[0].executed,
            "persisted": persisted,
            "provider_requests": report["provider_requests"],
            "provider_cache_hits": report["provider_cache_hits"],
            "provider_response_bytes": report["provider_response_bytes"],
            "duration_seconds": report["duration_seconds"],
            "report_path": str(args.report_path.resolve()),
        }, sort_keys=True))
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
