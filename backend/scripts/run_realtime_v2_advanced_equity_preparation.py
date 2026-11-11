#!/usr/bin/env python3
"""Run durable off-hours preparation for desired Advanced Equity members."""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_preparation import (  # noqa: E402
    AdvancedEquityPreparationResident,
    BoundedPolygonEquityPreparationClient,
    EquityPreparationLimits,
    MassiveEquityPreparationSource,
    PREPARATION_STAGES,
)
from realtime.advanced_equity_bootstrap import (  # noqa: E402
    VerifiedEquityBootstrapHistory,
    plan_equity_bootstrap,
)
from realtime.advanced_equity_rollover import (  # noqa: E402
    AdvancedEquityRolloverResident,
)
from realtime.fundamentals_refresh import run_fundamentals_refresh  # noqa: E402
from realtime.earnings_calendar_refresh import (  # noqa: E402
    run_earnings_calendar_refresh,
)
from realtime.fed_calendar_refresh import (  # noqa: E402
    FED_CALENDAR_CHECK_INTERVAL,
    run_fed_calendar_refresh,
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
from scripts.realtime_script_support import write_report  # noqa: E402


UTC = timezone.utc
COMPLETED_CHUNK_PAUSE_SECONDS = 0.25
ROLLOVER_IDLE_POLL_SECONDS = 300


def _now() -> datetime:
    return datetime.now(UTC)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument(
        "--env-file",
        type=Path,
    )
    parser.add_argument("--poll-seconds", type=float, default=15.0)
    parser.add_argument("--maximum-concurrency", type=int, default=1)
    parser.add_argument(
        "--maximum-response-bytes-per-chunk",
        type=int,
        default=8 * 1024 * 1024,
    )
    parser.add_argument(
        "--maximum-response-bytes-per-member",
        type=int,
        default=2 * 1024 * 1024 * 1024,
    )
    parser.add_argument(
        "--maximum-requests-per-member",
        type=int,
        default=160,
    )
    parser.add_argument(
        "--maximum-account-requests",
        type=int,
        default=64_000,
    )
    parser.add_argument(
        "--maximum-account-response-bytes",
        type=int,
        default=512 * 1024 * 1024 * 1024,
    )
    parser.add_argument("--lease-seconds", type=int, default=300)
    parser.add_argument("--retry-initial-seconds", type=int, default=15)
    parser.add_argument("--retry-maximum-seconds", type=int, default=900)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--until-idle", action="store_true")
    parser.add_argument("--bootstrap-revision-id", type=UUID)
    parser.add_argument("--bootstrap-authorization-reference")
    parser.add_argument("--bootstrap-authorized-until", type=datetime.fromisoformat)
    parser.add_argument("--bootstrap-maximum-chunks", type=int)
    parser.add_argument("--bootstrap-report-path", type=Path)
    parser.add_argument("--bootstrap-plan", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if args.env_file is not None:
        args.env_file = args.env_file.resolve()
        if not args.env_file.is_file():
            parser.error("--env-file is unavailable")
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error(
            "Advanced Equity preparation targets only stocks_realtime_v2"
        )
    if args.poll_seconds <= 0 or args.poll_seconds > 300:
        parser.error("preparation poll interval must be in (0, 300]")
    try:
        args.limits = EquityPreparationLimits(
            maximum_concurrency=args.maximum_concurrency,
            maximum_response_bytes_per_chunk=(
                args.maximum_response_bytes_per_chunk
            ),
            maximum_response_bytes_per_member=(
                args.maximum_response_bytes_per_member
            ),
            maximum_requests_per_member=(
                args.maximum_requests_per_member
            ),
            maximum_account_requests=(
                args.maximum_account_requests
            ),
            maximum_account_response_bytes=(
                args.maximum_account_response_bytes
            ),
            lease_seconds=args.lease_seconds,
            retry_initial_seconds=args.retry_initial_seconds,
            retry_maximum_seconds=args.retry_maximum_seconds,
        )
    except ValueError as error:
        parser.error(str(error))
    if not args.execute and not args.bootstrap_plan:
        parser.error("Advanced Equity preparation requires --execute")
    if args.execute and args.bootstrap_plan:
        parser.error("--bootstrap-plan cannot execute preparation")
    bootstrap_arguments = (
        args.bootstrap_revision_id,
        args.bootstrap_authorization_reference,
        args.bootstrap_authorized_until,
        args.bootstrap_maximum_chunks,
        args.bootstrap_report_path,
    )
    if any(value is not None for value in bootstrap_arguments):
        if not all(value is not None for value in bootstrap_arguments) or not args.until_idle:
            parser.error("one-time bootstrap requires all --bootstrap-* arguments and --until-idle")
        if not args.bootstrap_authorization_reference.strip():
            parser.error("bootstrap authorization reference cannot be blank")
        deadline = args.bootstrap_authorized_until
        now = _now()
        if (
            deadline.tzinfo is None or deadline.utcoffset() is None
            or not now < deadline <= now + timedelta(hours=4)
        ):
            parser.error("bootstrap authorization must expire within four hours")
        if not 1 <= args.bootstrap_maximum_chunks <= 10_000:
            parser.error("bootstrap chunk bound must be within 1-10000")
        if args.bootstrap_report_path.exists():
            parser.error("bootstrap report path must be new")
        if (
            args.limits.maximum_requests_per_member > 160
            or args.limits.maximum_response_bytes_per_member > 2 * 1024**3
            or args.limits.maximum_account_requests > 64_000
            or args.limits.maximum_account_response_bytes > 512 * 1024**3
        ):
            parser.error("bootstrap cannot raise the existing member/account ceilings")
    if args.bootstrap_plan and args.bootstrap_revision_id is None:
        parser.error("--bootstrap-plan requires the complete bounded bootstrap scope")
    return args


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


def _payload(result) -> str:
    return json.dumps({
        "status": result.status,
        "checked_at": result.checked_at.isoformat(),
        "ticker": result.ticker,
        "stage": result.stage,
        "attempts": result.attempts,
        "next_retry_at": (
            None
            if result.next_retry_at is None
            else result.next_retry_at.isoformat()
        ),
        "reason": result.reason,
        "reconciliation_state": result.reconciliation_state,
        "provider_fetch_performed": result.provider_requests > 0,
        "provider_requests": result.provider_requests,
        "response_bytes": result.response_bytes,
        "stored_history_adopted": result.stored_history_adopted,
    }, sort_keys=True)


def _wait_seconds(result, *, poll_seconds: float, now: datetime) -> float:
    if result.status in {"PROGRESSED", "COMPLETE"}:
        return min(poll_seconds, COMPLETED_CHUNK_PAUSE_SECONDS)
    if result.next_retry_at is None:
        return poll_seconds
    return min(
        poll_seconds,
        max(0.0, (result.next_retry_at - now).total_seconds()),
    )


def _run_rollover(rollover) -> str:
    # The next session can still ingest without this, so it must never take
    # member preparation down with it.
    try:
        result = rollover.run_once()
    except Exception as error:
        print(json.dumps({
            "rollover_status": "FAILED",
            "reason": f"{type(error).__name__}:{str(error)[:160]}",
        }, sort_keys=True), flush=True)
        return "FAILED"
    if result.status not in {"IDLE", "LIVE_PRIORITY"}:
        print(json.dumps({
            "rollover_status": result.status,
            "action": result.action,
            "reason": result.reason,
            "remaining": result.remaining,
            "provider_requests": result.provider_requests,
            "response_bytes": result.response_bytes,
            "persisted_bars": result.persisted_bars,
            "persisted_reference_facts": result.persisted_reference_facts,
            "next_retry_at": (
                result.next_retry_at.isoformat() if result.next_retry_at is not None else None
            ),
        }, sort_keys=True), flush=True)
    return result.status


def _bootstrap_summary(provider, revision_id):
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT revision_id::text FROM rt.realtime_desired_current "
            "WHERE scope_key='ADVANCED_REALTIME'",
        )
        current = unit.cursor.fetchone()
        unit.cursor.execute(
            "SELECT count(*),count(*) FILTER (WHERE work.status='COMPLETE'),"
            "count(DISTINCT work.intent_id),"
            "count(DISTINCT work.intent_id) FILTER (WHERE readiness.status='READY'),"
            "COALESCE(sum(work.provider_request_count),0),"
            "COALESCE(sum(work.response_bytes),0) "
            "FROM rt.realtime_equity_preparation_work AS work "
            "JOIN rt.realtime_desired_intent_readiness AS readiness USING(intent_id) "
            "WHERE work.revision_id=%s",
            (str(revision_id),),
        )
        work, complete, members, ready, requests, response_bytes = (
            int(value) for value in unit.cursor.fetchone()
        )
    return {
        "revision_current": current is not None and current[0] == str(revision_id),
        "work_count": work,
        "complete_work_count": complete,
        "member_count": members,
        "ready_member_count": ready,
        "provider_requests_consumed": requests,
        "response_bytes_consumed": response_bytes,
        "preparation_complete": (
            members > 0 and ready == members and complete == work
            and work == members * len(PREPARATION_STAGES)
        ),
    }


def _run_bootstrap(args, provider, resident, stop):
    report = {
        "mode": "ONE_TIME_BOOTSTRAP",
        "status": "RUNNING",
        "authorization_reference": args.bootstrap_authorization_reference,
        "revision_id": str(args.bootstrap_revision_id),
        "started_at": _now().isoformat(),
        "authorized_until": args.bootstrap_authorized_until.isoformat(),
        "maximum_chunks": args.bootstrap_maximum_chunks,
        "cycles": 0,
        "outcomes": {},
        "consumer_acceptance": "NOT_CHECKED",
    }
    try:
        report["before"] = _bootstrap_summary(provider, args.bootstrap_revision_id)
        if not report["before"]["revision_current"]:
            raise RuntimeError("bootstrap desired revision is no longer current")
        for _cycle in range(args.bootstrap_maximum_chunks):
            if stop.is_set():
                report["status"] = "INTERRUPTED"
                return 2
            if _now() + timedelta(seconds=args.limits.lease_seconds) > args.bootstrap_authorized_until:
                report["status"] = "AUTHORIZATION_TIME_INSUFFICIENT"
                return 2
            result = resident.run_once()
            report["cycles"] += 1
            key = f"{result.stage}:{result.status}"
            report["outcomes"][key] = report["outcomes"].get(key, 0) + 1
            if result.status != "PROGRESSED":
                print(_payload(result), flush=True)
            if result.status in {"BLOCKED", "LIVE_PRIORITY"}:
                report["status"] = result.status
                report["reason"] = result.reason
                return 2
            if result.status == "IDLE":
                summary = _bootstrap_summary(provider, args.bootstrap_revision_id)
                report["after"] = summary
                report["status"] = (
                    "PREPARATION_COMPLETE"
                    if summary["preparation_complete"] and summary["revision_current"]
                    else "INCOMPLETE"
                )
                return 0 if report["status"] == "PREPARATION_COMPLETE" else 2
            stop.wait(COMPLETED_CHUNK_PAUSE_SECONDS)
        report["status"] = "CHUNK_BOUND_REACHED"
        return 2
    finally:
        if report["status"] == "RUNNING":
            report["status"] = "FAILED"
        report["finished_at"] = _now().isoformat()
        write_report(args.bootstrap_report_path, report)
        print(json.dumps({
            "bootstrap_status": report["status"],
            "cycles": report["cycles"],
            "report_path": str(args.bootstrap_report_path),
        }, sort_keys=True), flush=True)


def main(argv=None):
    args = parse_args(argv)
    env_file = args.env_file or BACKEND_DIR / ".env"
    file_environment = (
        {
            key: value
            for key, value in dotenv_values(env_file).items()
            if value is not None
        }
        if env_file.is_file()
        else {}
    )
    config = RealtimeV2PipelineConfig.from_mapping({
        **file_environment,
        **os.environ,
    },
        process=RealtimeV2Process.EQUITY_PREPARATION,
    )
    if (
        config.equity_pipeline is not EquityPipeline.MASSIVE_ADVANCED
        or config.options_pipeline is not OptionsPipeline.DISABLED
    ):
        raise RuntimeError(
            "Advanced Equity preparation requires Advanced equity "
            "and disabled options"
        )
    provider, pool = _provider(config)
    if args.bootstrap_plan:
        try:
            summary = _bootstrap_summary(provider, args.bootstrap_revision_id)
            if not summary["revision_current"]:
                raise RuntimeError("bootstrap desired revision is no longer current")
            report = plan_equity_bootstrap(
                provider, revision_id=args.bootstrap_revision_id,
                checked_at=_now(), limits=args.limits,
            )
            report["durable_preparation"] = summary
            write_report(args.bootstrap_report_path, report)
            print(json.dumps({
                "mode": report["mode"], "members": len(report["members"]),
                "estimated_chunks": sum(
                    member["estimated_stored_chunks"] + member["estimated_missing_chunks"]
                    for member in report["members"].values()
                ),
                "estimated_provider_requests": sum(
                    member["estimated_provider_requests"] for member in report["members"].values()
                ),
                "fits_remaining_request_budgets": report["fits_remaining_request_budgets"],
                "fits_remaining_response_budgets": report["fits_remaining_response_budgets"],
                "provider_requests": 0, "database_writes": 0,
                "report_path": str(args.bootstrap_report_path),
            }, sort_keys=True))
            return 0 if (
                report["fits_remaining_request_budgets"]
                and report["fits_remaining_response_budgets"]
            ) else 2
        finally:
            pool.closeall()
    source = MassiveEquityPreparationSource(
        BoundedPolygonEquityPreparationClient(
            config.massive_api_key
        )
    )
    resident = AdvancedEquityPreparationResident(
        provider,
        source,
        owner_id=uuid4(),
        limits=args.limits,
        clock=_now,
        bootstrap_history=(
            VerifiedEquityBootstrapHistory(provider, limits=args.limits)
            if args.bootstrap_revision_id is not None else None
        ),
        bootstrap_revision_id=args.bootstrap_revision_id,
    )
    rollover = AdvancedEquityRolloverResident(
        provider,
        source,
        owner_id=uuid4(),
        limits=args.limits,
        clock=_now,
    )
    next_rollover_at = _now()
    next_fed_calendar_at = _now()
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_args: stop.set())
    signal.signal(signal.SIGTERM, lambda *_args: stop.set())
    try:
        if args.bootstrap_revision_id is not None:
            return _run_bootstrap(args, provider, resident, stop)
        while not stop.is_set():
            result = resident.run_once()
            print(_payload(result))
            if args.once:
                return 0 if result.status != "BLOCKED" else 1
            if args.until_idle and result.status in {"IDLE", "BLOCKED"}:
                return 0 if result.status == "IDLE" else 1
            wait_seconds = _wait_seconds(
                result,
                poll_seconds=args.poll_seconds,
                now=_now(),
            )
            if result.status == "IDLE" and _now() >= next_rollover_at:
                rollover_result = _run_rollover(rollover)
                progressed = rollover_result == "PROGRESSED"
                next_rollover_at = _now() + timedelta(
                    seconds=(
                        COMPLETED_CHUNK_PAUSE_SECONDS
                        if progressed
                        else ROLLOVER_IDLE_POLL_SECONDS
                    )
                )
                if progressed:
                    wait_seconds = COMPLETED_CHUNK_PAUSE_SECONDS
            # The end-of-day maintenance slot. Idle only, so never during a session.
            if result.status == "IDLE" and _now() >= next_fed_calendar_at:
                print(json.dumps(
                    run_fed_calendar_refresh(provider, clock=_now),
                    sort_keys=True,
                ), flush=True)
                print(json.dumps(
                    run_earnings_calendar_refresh(provider, clock=_now),
                    sort_keys=True,
                ), flush=True)
                # G37 (Nov 10): the Financials tab's filed statements when due (R10: first idle slot after a start,
                # then every 30 minutes; due once per completed close), see fundamentals_refresh.py.
                print(json.dumps(
                    run_fundamentals_refresh(provider, clock=_now, api_key=config.massive_api_key),
                    sort_keys=True,
                ), flush=True)
                next_fed_calendar_at = _now() + FED_CALENDAR_CHECK_INTERVAL
            stop.wait(wait_seconds)
    finally:
        pool.closeall()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
