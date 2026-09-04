#!/usr/bin/env python3
"""Operate the versioned equity ingestion and analysis materialization pipeline."""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import requests


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv

load_dotenv(BACKEND_DIR / ".env")

from equity.domain import BarAvailabilityMode, DecisionWatermark
from equity.calendar import latest_expected_market_time
from equity.derivation import DERIVATION_SOURCES
from equity.leadership import try_advisory_leadership
from equity.orchestration import EquityMaterializationService
from equity.historical_research import composite_scanner_outcome_policies
from equity.polygon import PolygonEquityClient
from equity.repositories import (
    EquityAnalysisRepository,
    EquityBarRepository,
    EquityIngestionRepository,
    EquityReferenceRepository,
    EquityUniverseRepository,
)
from stock_screener.schema import BASELINE_VERSION, install_or_adopt_schema


WORKER_LOCK_NAME = os.getenv(
    "EQUITY_WORKER_LOCK_NAME", "stock-screener:equity-materialization-worker"
)
DEFAULT_NATIVE_FETCH_WORKERS = int(os.getenv("EQUITY_NATIVE_FETCH_WORKERS", "8"))


def apply_migration() -> str:
    import psycopg2

    connection = psycopg2.connect(
        dbname=os.getenv("DB_NAME", "stocks_db"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
    )
    try:
        return install_or_adopt_schema(connection)
    finally:
        connection.close()


def status() -> dict[str, int]:
    from database import get_db_cursor

    tables = (
        "schema_migrations",
        "equity_security_reference_revisions",
        "equity_fundamental_reports",
        "equity_universe_runs",
        "equity_universe_members",
        "equity_ingestion_segments",
        "equity_corporate_actions",
        "equity_bar_revisions",
        "equity_bar_publications",
        "equity_bar_publication_members",
        "equity_current_bar_projection",
        "equity_analysis_runs",
        "equity_analysis_members",
        "equity_evidence",
        "equity_context_snapshots",
        "equity_context_evidence",
        "equity_current_projection",
        "equity_outcome_policies",
        "equity_research_outcomes",
        "equity_qualification_revisions",
        "equity_portal_source_state",
        "equity_portal_snapshots",
        "equity_portal_current_projections",
    )
    counts = {}
    with get_db_cursor() as cursor:
        for table in tables:
            cursor.execute("SELECT to_regclass(%s) AS table_name", (f"public.{table}",))
            if cursor.fetchone()["table_name"] is None:
                counts[table] = -1
                continue
            cursor.execute(f"SELECT COUNT(*) AS count FROM {table}")
            counts[table] = int(cursor.fetchone()["count"])
    return counts


def selected_tickers(explicit: str | None) -> tuple[str, ...]:
    if explicit:
        return tuple(dict.fromkeys(
            value.strip().upper() for value in explicit.split(",") if value.strip()
        ))
    from database import get_selected_tickers

    return tuple(get_selected_tickers(active_only=True))


def bar_coverage_report(
    tickers: tuple[str, ...],
    *,
    interval: str,
    start: date,
    end: date,
    availability_mode: BarAvailabilityMode,
) -> dict[str, object]:
    from database import get_db_cursor

    with get_db_cursor() as cursor:
        cursor.execute(
            """
            WITH latest AS (
                SELECT DISTINCT ON (ticker, bar_start) ticker, bar_start, bar_end
                FROM equity_bar_revisions
                WHERE ticker = ANY(%s)
                  AND interval = %s
                  AND availability_mode = %s
                  AND session_date BETWEEN %s AND %s
                ORDER BY ticker, bar_start, system_observed_at DESC, created_at DESC
            ), watermarks AS (
                SELECT bar_end, COUNT(DISTINCT ticker) AS covered
                FROM latest
                GROUP BY bar_end
            )
            SELECT
                (SELECT COUNT(*) FROM latest) AS bars,
                (SELECT COUNT(DISTINCT ticker) FROM latest) AS observed_tickers,
                (SELECT MIN(bar_start) FROM latest) AS first_bar,
                (SELECT MAX(bar_end) FROM latest) AS last_bar,
                COUNT(*) AS watermarks,
                MIN(covered) AS minimum_covered,
                AVG(covered) AS average_covered,
                MAX(covered) AS maximum_covered,
                COUNT(*) FILTER (WHERE covered >= %s) AS complete_watermarks,
                COUNT(*) FILTER (WHERE covered >= %s AND covered < %s)
                    AS degraded_watermarks,
                COUNT(*) FILTER (WHERE covered < %s) AS failed_watermarks
            FROM watermarks
            """,
            (
                list(tickers), interval, availability_mode.value, start, end,
                math.ceil(len(tickers) * 0.95), math.ceil(len(tickers) * 0.90),
                math.ceil(len(tickers) * 0.95), math.ceil(len(tickers) * 0.90),
            ),
        )
        row = dict(cursor.fetchone())
        cursor.execute(
            """
            SELECT DISTINCT ticker
            FROM equity_bar_revisions
            WHERE ticker = ANY(%s)
              AND interval = %s
              AND availability_mode = %s
              AND session_date BETWEEN %s AND %s
            """,
            (list(tickers), interval, availability_mode.value, start, end),
        )
        observed = {row["ticker"] for row in cursor.fetchall()}
    average = row.get("average_covered")
    row["average_covered"] = float(average) if average is not None else None
    row.update({
        "interval": interval,
        "availability_mode": availability_mode.value,
        "expected_tickers": len(tickers),
        "missing_tickers": tuple(sorted(set(tickers) - observed)),
    })
    return row


def load_current_securities(
    tickers: tuple[str, ...], observed_at: datetime
):
    repository = EquityReferenceRepository()
    watermark = DecisionWatermark(observed_at, observed_at)
    return tuple(
        row
        for ticker in tickers
        if (row := repository.get_security_as_of(ticker, watermark)) is not None
    )


def run(args) -> int:
    if args.apply_migration:
        migration_result = apply_migration()
        print(f"{BASELINE_VERSION}: {migration_result}")
        if not any((args.coverage_report, args.recover_stale_runs, args.reference, args.fundamentals, args.bars, args.derive_bars, args.derive_history, args.reconcile, args.reconcile_monthly, args.publish_bars, args.analyze, args.outcomes, args.qualify, args.once, args.status)):
            return 0
    if args.status:
        for table, count in status().items():
            print(f"{table}: {'MISSING' if count < 0 else count}")
        if not any((args.coverage_report, args.recover_stale_runs, args.reference, args.fundamentals, args.bars, args.derive_bars, args.derive_history, args.reconcile, args.reconcile_monthly, args.publish_bars, args.analyze, args.outcomes, args.qualify, args.once)):
            return 0

    if args.recover_stale_runs:
        stale_after = timedelta(minutes=args.stale_after_minutes)
        recovered_runs = EquityAnalysisRepository().fail_stale_runs(
            stale_after=stale_after
        )
        recovered_segments = EquityIngestionRepository().fail_stale_segments(
            stale_after=stale_after
        )
        print({
            "terminal_failed_runs": len(recovered_runs),
            "terminal_failed_segments": len(recovered_segments),
            "runs": recovered_runs,
            "segments": recovered_segments,
        })
        if not any((args.reference, args.fundamentals, args.bars, args.derive_bars, args.derive_history, args.reconcile, args.reconcile_monthly, args.publish_bars, args.analyze, args.outcomes, args.qualify, args.once)):
            return 0

    tickers = selected_tickers(args.tickers)
    if not tickers:
        raise ValueError("no equity tickers are configured")
    observed_at = datetime.now(timezone.utc)
    requested_date = date.fromisoformat(args.date) if args.date else observed_at.date()
    requested_start = (
        date.fromisoformat(args.from_date) if args.from_date else requested_date
    )
    if requested_start > requested_date:
        raise ValueError("--from-date must be on or before --date")
    intervals = tuple(dict.fromkeys(args.interval or ["30m"]))
    if args.coverage_report:
        availability_mode = (
            BarAvailabilityMode.HISTORICAL_RECONSTRUCTED
            if args.replay else BarAvailabilityMode.LIVE_OBSERVED
        )
        for interval in intervals:
            print(bar_coverage_report(
                tickers,
                interval=interval,
                start=requested_start,
                end=requested_date,
                availability_mode=availability_mode,
            ))
        if not any((args.reference, args.fundamentals, args.bars, args.derive_bars, args.derive_history, args.reconcile, args.reconcile_monthly, args.publish_bars, args.analyze, args.outcomes, args.qualify, args.once)):
            return 0
    if args.fetch_workers <= 0:
        raise ValueError("--fetch-workers must be positive")
    client = PolygonEquityClient()
    pipeline = EquityMaterializationService(
        client, native_fetch_workers=args.fetch_workers
    )

    refresh_reference = args.reference or args.once
    if refresh_reference:
        reference = pipeline.refresh_reference(
            tickers,
            observed_at=observed_at,
            as_of_date=requested_date,
            include_float=not args.skip_float,
        )
        securities = reference.revisions
        universe_run_id = reference.universe_run_id
        print({
            "reference_revisions": len(securities),
            "missing_reference": reference.missing_tickers,
            "universe_run_id": str(universe_run_id),
        })
    else:
        securities = load_current_securities(tickers, observed_at)
        universe = EquityUniverseRepository().get_latest_as_of(
            DecisionWatermark(observed_at, observed_at)
        )
        if universe is None:
            raise ValueError("no published equity universe exists; run --reference first")
        universe_run_id = universe["universe_run_id"]
    if not securities:
        raise ValueError("no security reference revisions are available")

    fundamental_inputs_requested = (
        args.fundamentals or args.once
    ) and not args.skip_fundamentals
    fundamentals_available = False
    if fundamental_inputs_requested:
        fundamental_result = pipeline.refresh_fundamentals(
            securities, observed_at=observed_at
        )
        fundamentals_available = fundamental_result.available
        print({
            "fundamentals_available": fundamental_result.available,
            "fundamental_reports_written": fundamental_result.reports_written,
            "fundamental_reason": fundamental_result.reason,
        })

    for interval in intervals:
        if args.bars or args.once:
            if interval in DERIVATION_SOURCES:
                raise ValueError(
                    f"{interval} is internally derived; use --derive-bars"
                )
            ingestion = pipeline.ingest_native_interval(
                securities,
                interval=interval,
                start=requested_start,
                end=requested_date,
                observed_at=observed_at,
                availability_mode=(
                    BarAvailabilityMode.HISTORICAL_RECONSTRUCTED
                    if args.replay else BarAvailabilityMode.LIVE_OBSERVED
                ),
            )
            print({
                "interval": interval,
                "bars_received": ingestion.bar_count,
                "bars_inserted": ingestion.inserted_count,
                "missing_bars": ingestion.missing_tickers,
                "ingestion_segment_id": str(ingestion.ingestion_segment_id),
                "fetch_seconds": round(ingestion.fetch_seconds, 3),
                "persist_seconds": round(ingestion.persist_seconds, 3),
                "elapsed_seconds": round(ingestion.elapsed_seconds, 3),
            })
        if args.derive_bars or args.derive_history:
            if interval not in DERIVATION_SOURCES:
                raise ValueError(
                    f"{interval} is provider-native; use --bars"
                )
            requested_observation = (
                observed_at
                if args.date is None
                else datetime.combine(requested_date, time.max, tzinfo=timezone.utc)
            )
            derived_market_time = latest_expected_market_time(
                requested_observation, interval
            )
            print(pipeline.derive_interval(
                securities,
                target_interval=interval,
                watermark=DecisionWatermark(derived_market_time, observed_at),
                include_history=args.derive_history,
            ))
        if args.reconcile:
            print(pipeline.reconcile_stream_interval(
                interval=interval,
                observed_at=observed_at,
                limit=args.limit,
            ))
        if args.reconcile_monthly:
            if interval != "1mo":
                raise ValueError("--reconcile-monthly requires --interval 1mo")
            monthly_market_time = latest_expected_market_time(observed_at, "1mo")
            print(pipeline.reconcile_derived_monthly(
                securities,
                watermark=DecisionWatermark(monthly_market_time, observed_at),
            ))
        if args.publish_bars:
            market_time = EquityBarRepository().latest_common_market_time(
                tuple(row.ticker for row in securities),
                interval,
                observed_by=observed_at,
            )
            if market_time is None:
                raise ValueError(
                    f"no sufficiently covered finalized {interval} watermark is available"
                )
            print(pipeline.publish_canonical_interval(
                securities,
                interval=interval,
                watermark=DecisionWatermark(market_time, observed_at),
            ))
        if args.analyze or args.once:
            ratios = {}
            if not args.replay and fundamentals_available:
                try:
                    ratios = {
                        str(row["ticker"]).upper(): row
                        for row in client.fetch_ratios(
                            tuple(row.ticker for row in securities)
                        )
                    }
                except requests.HTTPError as exc:
                    if exc.response is None or exc.response.status_code != 403:
                        raise
                    print({"ratios_available": False, "reason": "POLYGON_RATIOS_ENTITLEMENT_UNAVAILABLE"})
            if args.replay:
                replay_results = pipeline.replay_interval_range(
                    securities,
                    universe_run_id=universe_run_id,
                    interval=interval,
                    start=datetime.combine(requested_start, time.min, tzinfo=timezone.utc),
                    end=datetime.combine(requested_date, time.max, tzinfo=timezone.utc),
                    available_by=observed_at,
                    max_runs=args.max_runs,
                )
                print({
                    "interval": interval,
                    "replay_runs": len(replay_results),
                    "complete": sum(row.status == "COMPLETE" for row in replay_results),
                    "degraded": sum(row.status == "DEGRADED" for row in replay_results),
                    "failed": sum(row.status == "FAILED" for row in replay_results),
                })
                continue
            market_time = EquityBarRepository().latest_common_market_time(
                tuple(row.ticker for row in securities),
                interval,
                observed_by=observed_at,
            )
            if market_time is None:
                raise ValueError(
                    f"no sufficiently covered finalized {interval} watermark is available"
                )
            analysis_watermark = DecisionWatermark(
                market_time,
                market_time if args.replay else observed_at,
            )
            analysis = pipeline.materialize_interval(
                securities,
                universe_run_id=universe_run_id,
                interval=interval,
                watermark=analysis_watermark,
                run_purpose="SHADOW" if args.shadow else "ORIGINAL",
                current_ratios=ratios,
            )
            print(analysis)
        if args.outcomes:
            policies = composite_scanner_outcome_policies(
                interval=interval,
                effective_from=datetime(2026, 8, 30, tzinfo=timezone.utc),
            )
            for policy in policies:
                for horizon_key in json.loads(policy.horizons_json):
                    print(pipeline.evaluate_directional_outcomes(
                        policy,
                        horizon_key,
                        available_by=observed_at,
                        finalize_unavailable=args.finalize_unavailable,
                    ))
    if args.qualify:
        for interval in intervals:
            print(pipeline.qualify_materialized_outcomes(
                available_by=observed_at,
                interval=interval,
                evaluation_version=args.evaluation_version,
            ))
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--apply-migration", action="store_true")
    result.add_argument("--status", action="store_true")
    result.add_argument("--coverage-report", action="store_true")
    result.add_argument("--recover-stale-runs", action="store_true")
    result.add_argument("--stale-after-minutes", type=int, default=60)
    result.add_argument("--reference", action="store_true")
    result.add_argument("--fundamentals", action="store_true")
    result.add_argument("--bars", action="store_true")
    result.add_argument("--derive-bars", action="store_true")
    result.add_argument("--derive-history", action="store_true")
    result.add_argument("--publish-bars", action="store_true")
    result.add_argument("--reconcile", action="store_true")
    result.add_argument("--reconcile-monthly", action="store_true")
    result.add_argument("--analyze", action="store_true")
    result.add_argument("--outcomes", action="store_true")
    result.add_argument("--qualify", action="store_true")
    result.add_argument("--finalize-unavailable", action="store_true")
    result.add_argument(
        "--evaluation-version", default="equity_qualification_v1"
    )
    result.add_argument("--once", action="store_true")
    result.add_argument("--replay", action="store_true")
    result.add_argument("--shadow", action="store_true")
    result.add_argument("--skip-float", action="store_true")
    result.add_argument("--skip-fundamentals", action="store_true")
    result.add_argument("--tickers", help="Comma-separated tickers; defaults to active universe")
    result.add_argument("--date", help="Session date YYYY-MM-DD; defaults to current UTC date")
    result.add_argument("--from-date", help="Optional bar-ingestion start date YYYY-MM-DD")
    result.add_argument("--max-runs", type=int, default=10000)
    result.add_argument("--limit", type=int, default=5000)
    result.add_argument(
        "--fetch-workers", type=int, default=DEFAULT_NATIVE_FETCH_WORKERS
    )
    result.add_argument(
        "--interval", action="append",
        choices=("1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"),
        help="Repeat for multiple intervals; defaults to 30m",
    )
    return result


def requires_leadership(args) -> bool:
    return any((
        args.apply_migration, args.recover_stale_runs, args.reference,
        args.fundamentals, args.bars, args.derive_bars, args.derive_history,
        args.reconcile, args.reconcile_monthly, args.publish_bars,
        args.analyze, args.outcomes,
        args.qualify, args.once,
    ))


def main() -> int:
    args = parser().parse_args()
    if args.shadow and args.replay:
        parser().error("--shadow and --replay are mutually exclusive")
    if args.shadow and not args.analyze:
        parser().error("--shadow requires --analyze")
    if not any((
        args.apply_migration, args.status, args.coverage_report,
        args.recover_stale_runs,
        args.reference, args.fundamentals,
        args.bars, args.derive_bars, args.derive_history, args.reconcile,
        args.reconcile_monthly,
        args.publish_bars, args.analyze,
        args.outcomes, args.qualify,
        args.once,
    )):
        parser().error("choose an operation")
    if requires_leadership(args):
        with try_advisory_leadership(WORKER_LOCK_NAME) as is_leader:
            if not is_leader:
                raise RuntimeError(
                    "equity worker is active; mutating operation requires leadership"
                )
            return run(args)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())