#!/usr/bin/env python3
"""Plan or run disabled-by-default incremental V2 1m/5m equity acquisition."""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid5, NAMESPACE_URL

from dotenv import load_dotenv
from psycopg2.pool import ThreadedConnectionPool

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.data.polygon_http import RequestsPolygonHttpTransport  # noqa: E402
from realtime.acquisition_ledger import ProviderRequestLane  # noqa: E402
from realtime.contracts import UniverseManifest  # noqa: E402
from realtime.database_access import RealtimeDatabaseRole  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_bar_drain import drain_equity_bar_jobs  # noqa: E402
from realtime.equity_interval_derivation_worker import (  # noqa: E402
    enqueue_interval_derivation_jobs_cursor,
    run_interval_derivation_worker_once,
)
from realtime.equity_portal_projection_worker import (  # noqa: E402
    enqueue_portal_projection_job_cursor,
    run_portal_projection_worker_once,
)
from realtime.equity_worker_service import (  # noqa: E402
    EQUITY_INTERVAL_DERIVE_JOB,
    EQUITY_PORTAL_PROJECT_JOB,
    EquityWorkerConfig,
)
from realtime.equity_recent_repair import AuthorizedNativeRepairTransport  # noqa: E402
from realtime.incremental_acquisition import (  # noqa: E402
    IncrementalEquitySource,
    latest_qualified_cutoff,
    plan_incremental_requests_cursor,
)
from realtime.incremental_finalization import (  # noqa: E402
    finalize_incremental_five_minute_windows,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    SECURITY_MODE_HARDENED,
    validate_target_database_v2,
)
from realtime.provider_budget import ProviderBudgetScope  # noqa: E402
from realtime.stage_database_v2 import V2WorkerDatabase  # noqa: E402
from realtime.stage_runners_ingest import (  # noqa: E402
    EquityAcquisitionRequest,
    acquire_equity_bars,
)
from scripts.realtime_script_support import PacedPolygonTransport, write_report  # noqa: E402
from scripts.run_realtime_v2_developer_window import _load_scope  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--interval",
        action="append",
        choices=("1m", "5m"),
        dest="intervals",
    )
    parser.add_argument("--provider-delay-seconds", type=int, default=900)
    parser.add_argument("--response-byte-limit", type=int, required=True)
    parser.add_argument("--account-request-ceiling", type=int, required=True)
    parser.add_argument("--account-decoded-byte-ceiling", type=int, required=True)
    parser.add_argument("--authorized-until", type=datetime.fromisoformat, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--provider-authorization-reference", required=True)
    parser.add_argument("--expected-cost", required=True)
    parser.add_argument("--minimum-spacing-seconds", type=float, default=0.5)
    parser.add_argument("--timeout-seconds", type=float, default=30)
    parser.add_argument("--poll-seconds", type=float, default=5)
    parser.add_argument("--report-path", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--continuous", action="store_true")
    parser.add_argument("--execute-shadow", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode=SECURITY_MODE_HARDENED,
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("incremental equity targets only stocks_realtime_v2")
    args.intervals = tuple(dict.fromkeys(args.intervals or ("1m", "5m")))
    if args.authorized_until.tzinfo is None or args.authorized_until.utcoffset() is None:
        parser.error("--authorized-until must be timezone-aware")
    args.authorized_until = args.authorized_until.astimezone(timezone.utc)
    if (
        args.provider_delay_seconds < 0
        or args.response_byte_limit < 1_024
        or args.account_request_ceiling <= 0
        or args.account_decoded_byte_ceiling <= 0
        or args.minimum_spacing_seconds <= 0
        or not 1 <= args.timeout_seconds <= 60
        or args.poll_seconds <= 0
        or not all(value.strip() for value in (
            args.authorization_reference,
            args.provider_authorization_reference,
            args.expected_cost,
        ))
    ):
        parser.error("incremental budget, pacing or authorization is invalid")
    if args.execute_shadow and not (args.once or args.continuous):
        parser.error("--execute-shadow requires --once or --continuous")
    if not args.execute_shadow and (args.once or args.continuous):
        parser.error("--once/--continuous require --execute-shadow")
    if args.report_path is not None:
        args.report_path = args.report_path.resolve()
        if args.report_path.exists():
            parser.error("--report-path must be new")
    return args


def _provider(args):
    load_dotenv(BACKEND_DIR / ".env", override=True)
    dsn = os.getenv("REALTIME_V2_APP_DSN", "").strip()
    if not dsn:
        raise RuntimeError("incremental equity requires REALTIME_V2_APP_DSN")
    pool = ThreadedConnectionPool(1, 4, dsn=dsn, options="-c timezone=UTC")
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode=SECURITY_MODE_HARDENED,
            role_prefix=EXPECTED_V2_DATABASE_NAME,
            statement_timeout_ms=300_000,
        ),
    )
    provider.verify()
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
            "WHERE table_schema='rt' AND table_name='provider_request_ledger' "
            "AND column_name='reserved_response_bytes') "
            "AND EXISTS (SELECT 1 FROM information_schema.columns "
            "WHERE table_schema='rt' AND table_name='acquisition_watermarks' "
            "AND column_name='last_examined_bar_end')"
        )
        if not unit.cursor.fetchone()[0]:
            raise RuntimeError(
                "incremental shadow requires migrations 014 and 015"
            )
    return provider, pool


def _scope(args):
    return ProviderBudgetScope(
        account_alias="MASSIVE_DEVELOPER",
        session_date=args.session_date,
        authorization_reference=args.authorization_reference,
        provider_authorization_reference=args.provider_authorization_reference,
        expected_cost=args.expected_cost,
        request_ceiling=args.account_request_ceiling,
        decoded_byte_ceiling=args.account_decoded_byte_ceiling,
        lanes=frozenset({ProviderRequestLane.EQUITY_CURRENT}),
        authorized_until=args.authorized_until,
    )


def _plan(provider, args, now):
    rows = []
    for interval in args.intervals:
        cutoff = latest_qualified_cutoff(
            args.session_date,
            interval,
            known_at=now,
            provider_delay=timedelta(seconds=args.provider_delay_seconds),
        )
        if cutoff is None:
            rows.append({"interval": interval, "cutoff": None, "requests": ()})
            continue
        identities, _ = _load_scope(provider, args.cohort_id, cutoff)
        with provider.transaction(APP_ROLE, read_only=True) as unit:
            requests = plan_incremental_requests_cursor(
                unit.cursor,
                cohort_id=args.cohort_id,
                identities=identities,
                session_date=args.session_date,
                interval=interval,
                cutoff=cutoff,
            )
        rows.append({
            "interval": interval,
            "cutoff": cutoff,
            "identities": identities,
            "requests": requests,
        })
    return tuple(rows)


def _serialize_plan(rows):
    return [{
        "interval": row["interval"],
        "cutoff": (
            row["cutoff"].isoformat() if row["cutoff"] is not None else None
        ),
        "requests": [{
            "ticker": request.ticker,
            "requested_from": request.requested_from.isoformat(),
            "requested_to": request.requested_to.isoformat(),
            "expected_bars": request.expected_bars,
            "overlap_bars": request.overlap_bars,
            "prior_complete_bar_end": (
                request.prior_complete_bar_end.isoformat()
                if request.prior_complete_bar_end is not None else None
            ),
            "prior_examined_bar_end": (
                request.prior_examined_bar_end.isoformat()
                if request.prior_examined_bar_end is not None else None
            ),
            "prior_terminal_state": request.prior_terminal_state,
        } for request in row["requests"]],
    } for row in rows]


def _execute_cycle(provider, args, transport, budget_scope, now):
    reports = []
    for row in _plan(provider, args, now):
        requests = row["requests"]
        if not requests:
            reports.append({
                "interval": row["interval"],
                "cutoff": (
                    row["cutoff"].isoformat()
                    if row["cutoff"] is not None else None
                ),
                "status": "NO_WORK",
            })
            continue
        source = IncrementalEquitySource(
            provider,
            transport,
            budget_scope,
            row["identities"],
            requests,
            response_byte_limit=args.response_byte_limit,
            clock=lambda: datetime.now(timezone.utc),
        )
        universe = UniverseManifest(
            version=f"v2-incremental-{row['interval']}-v1",
            equity_tickers=tuple(item.ticker for item in row["identities"]),
            option_underlyings=(),
        )
        acquired = acquire_equity_bars(
            V2WorkerDatabase(provider, RealtimeDatabaseRole.INGRESS),
            source,
            EquityAcquisitionRequest(
                args.cohort_id,
                universe,
                row["cutoff"],
            ),
        )
        drained = drain_equity_bar_jobs(
            V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE),
            cohort_id=args.cohort_id,
            lease_owner=uuid5(
                NAMESPACE_URL,
                f"v2-incremental-drain:{args.cohort_id}:{row['interval']}:{row['cutoff'].isoformat()}",
            ),
            max_jobs=len(acquired.acquisition_batch_ids),
            acquisition_batch_ids=acquired.acquisition_batch_ids,
        )
        if drained.unsuccessful or drained.retried:
            raise RuntimeError(
                "incremental acquisition materialization did not complete cleanly"
            )
        derivation = None
        finalization = None
        technical_projection = None
        if row["interval"] == "5m":
            checked_at = datetime.now(timezone.utc)
            finalization = finalize_incremental_five_minute_windows(
                provider,
                cohort_id=args.cohort_id,
                session_date=args.session_date,
                instrument_ids=tuple(
                    item.instrument_id for item in row["identities"]
                ),
                through=row["cutoff"],
                checked_at=checked_at,
                authorization_reference=args.authorization_reference,
                plan_deadline=budget_scope.authorized_until,
            )
            with provider.transaction(APP_ROLE, dict_cursor=True) as unit:
                enqueue_interval_derivation_jobs_cursor(
                    unit.cursor,
                    cohort_id=args.cohort_id,
                    instrument_ids=tuple(
                        item.instrument_id for item in row["identities"]
                    ),
                    session_date=args.session_date,
                    checked_at=checked_at,
                )
            derived = run_interval_derivation_worker_once(
                V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE),
                EquityWorkerConfig(
                    cohort_id=args.cohort_id,
                    job_kind=EQUITY_INTERVAL_DERIVE_JOB,
                    lease_owner=uuid5(
                        NAMESPACE_URL,
                        "v2-incremental-derive:"
                        f"{args.cohort_id}:{row['cutoff'].isoformat()}",
                    ),
                    claim_limit=100,
                ),
                checked_at=checked_at,
            )
            derivation = {
                "claimed": derived.claimed,
                "completed": derived.completed,
                "retried": derived.retried,
                "failed": derived.failed,
                "blocked": derived.blocked,
                "bars": derived.output_count,
            }
            if row["cutoff"].minute % 15 == 0:
                with provider.transaction(APP_ROLE, dict_cursor=True) as unit:
                    projection_job_id = enqueue_portal_projection_job_cursor(
                        unit.cursor,
                        cohort_id=args.cohort_id,
                        cutoff=row["cutoff"],
                        available_at=datetime.now(timezone.utc),
                    )
                if projection_job_id is not None:
                    projected = run_portal_projection_worker_once(
                        V2WorkerDatabase(
                            provider,
                            RealtimeDatabaseRole.COMPUTE,
                        ),
                        EquityWorkerConfig(
                            cohort_id=args.cohort_id,
                            job_kind=EQUITY_PORTAL_PROJECT_JOB,
                            lease_owner=uuid5(
                                NAMESPACE_URL,
                                "v2-incremental-project:"
                                f"{args.cohort_id}:{row['cutoff'].isoformat()}",
                            ),
                            claim_limit=1,
                        ),
                        checked_at=datetime.now(timezone.utc),
                    )
                    technical_projection = {
                        "claimed": projected.claimed,
                        "completed": projected.completed,
                        "retried": projected.retried,
                        "failed": projected.failed,
                        "blocked": projected.blocked,
                        "snapshots": projected.output_count,
                    }
        reports.append({
            "interval": row["interval"],
            "cutoff": row["cutoff"].isoformat(),
            "status": "COMPLETE",
            "requests_planned": len(requests),
            "provider_requests": acquired.provider_requests,
            "facts": acquired.fact_count,
            "bars": drained.bars,
            "finalization": finalization,
            "derivation": derivation,
            "technical_projection": technical_projection,
        })
    return reports


def main(argv=None):
    args = parse_args(argv)
    provider, pool = _provider(args)
    try:
        now = datetime.now(timezone.utc)
        if not args.execute_shadow:
            report = {
                "mode": "PLAN",
                "shadow_enabled": False,
                "session_date": args.session_date.isoformat(),
                "budget_scope_sha256": _scope(args).sha256,
                "plan": _serialize_plan(_plan(provider, args, now)),
                "provider_requests": 0,
                "database_writes": 0,
            }
            print(json.dumps(report, indent=2, sort_keys=True))
            return 0
        api_key = os.getenv("POLYGON_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("incremental equity requires POLYGON_API_KEY")
        transport = PacedPolygonTransport(
            AuthorizedNativeRepairTransport(
                RequestsPolygonHttpTransport(
                    api_key,
                    timeout_seconds=args.timeout_seconds,
                    maximum_response_bytes=args.response_byte_limit,
                ),
                args.authorized_until,
                lambda: datetime.now(timezone.utc),
            ),
            minimum_interval_seconds=args.minimum_spacing_seconds,
        )
        stop = False

        def request_stop(*_):
            nonlocal stop
            stop = True

        signal.signal(signal.SIGINT, request_stop)
        signal.signal(signal.SIGTERM, request_stop)
        cycles = []
        while not stop:
            cycle = _execute_cycle(
                provider,
                args,
                transport,
                _scope(args),
                datetime.now(timezone.utc),
            )
            cycles.append(cycle)
            if args.once:
                break
            time.sleep(args.poll_seconds)
        report = {
            "mode": "EXECUTE_SHADOW",
            "session_date": args.session_date.isoformat(),
            "cycles": cycles,
            "publication_advanced": False,
            "finalization_enqueued": True,
        }
        serializable = json.loads(json.dumps(report, default=str))
        if args.report_path is not None:
            write_report(args.report_path, serializable)
        print(json.dumps(serializable, indent=2, sort_keys=True))
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
