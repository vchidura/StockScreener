#!/usr/bin/env python3
"""Prepare signal-agnostic historical universes and canonical research bars."""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from time import sleep
from typing import Any, Callable
from uuid import NAMESPACE_URL, UUID, uuid5

import exchange_calendars
import pandas as pd
import requests
from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from equity.domain import BarAvailabilityMode, DecisionWatermark
from equity.historical_universe import (
    HistoricalUniversePolicy,
    grouped_daily_rows,
    historical_session_plan,
    historical_universe_run_id,
    select_historical_members,
)
from equity.leadership import try_advisory_leadership
from equity.polygon import (
    PolygonEquityClient,
    canonical_json,
    normalize_corporate_actions,
    normalize_grouped_daily_bars,
    normalize_security_reference,
    sha256_json,
)
from equity.repositories import (
    EquityBarRepository,
    EquityCorporateActionRepository,
    EquityIngestionRepository,
    EquityReferenceRepository,
    EquityUniverseRepository,
)
from research.gics_sectors import (
    ALTERNATE_MARKET_ETF,
    BROAD_MARKET_ETF,
    SECTOR_BENCHMARK_ETF,
)


HISTORICAL_BENCHMARK_TICKERS = frozenset({
    BROAD_MARKET_ETF,
    ALTERNATE_MARKET_ETF,
    *SECTOR_BENCHMARK_ETF.values(),
})


LOCK_NAME = os.getenv(
    "EQUITY_WORKER_LOCK_NAME", "stock-screener:equity-materialization-worker"
)


def _with_backoff(fetch, *, kind: str, key: str, attempts: int = 8):
    """Polygon's client raises on any non-200 and implements no backoff itself.

    Long runs also lose connections mid-stream, which surfaces as a transport
    error rather than a status code, so both are retried here.
    """
    delay = 2.0
    for attempt in range(1, attempts + 1):
        try:
            return fetch()
        except requests.RequestException as error:
            status = getattr(getattr(error, "response", None), "status_code", None)
            if status == 403:
                raise SystemExit(
                    f"Polygon returned 403 for {kind} {key}: the request predates "
                    "the account's entitlement window, which rolls forward five "
                    "years. Move --end later or reduce --sessions so the plan "
                    "starts inside the entitled range; retrying cannot fix this."
                ) from error
            retryable = (
                status in (429, 500, 502, 503, 504)
                or isinstance(error, (
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                ))
            )
            if not retryable or attempt == attempts:
                raise
            reason = f"HTTP {status}" if status else type(error).__name__
            print(
                f"  {kind} {key} {reason}; retry {attempt}/{attempts - 1} "
                f"in {delay:.0f}s",
                flush=True,
            )
            sleep(delay)
            delay = min(delay * 2, 60.0)
    raise RuntimeError("unreachable")


class ResponseCache:
    def __init__(self, directory: Path) -> None:
        self.directory = directory

    def get_or_fetch(
        self,
        kind: str,
        key: str,
        fetch: Callable[[], list[dict[str, Any]] | tuple[dict[str, Any], ...]],
    ) -> list[dict[str, Any]]:
        safe_name = "".join(
            character if character.isalnum() or character in ("-", "_") else "_"
            for character in f"{kind}_{key}"
        )
        path = self.directory / f"{safe_name}.json"
        if path.exists():
            document = json.loads(path.read_text(encoding="utf-8"))
            payload = document.get("payload")
            if not isinstance(payload, list) or document.get("sha256") != sha256_json(payload):
                raise ValueError(f"historical input cache checksum mismatch: {path}")
            return payload
        payload = [dict(row) for row in _with_backoff(fetch, kind=kind, key=key)]
        document = {"sha256": sha256_json(payload), "payload": payload}
        self.directory.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(canonical_json(document), encoding="utf-8")
        temporary.replace(path)
        return payload


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    operation = result.add_mutually_exclusive_group(required=True)
    operation.add_argument("--dry-run", action="store_true")
    operation.add_argument("--persist", action="store_true")
    operation.add_argument("--status", action="store_true")
    result.add_argument("--sessions", type=int, default=100)
    result.add_argument("--end", help="Last research session YYYY-MM-DD")
    result.add_argument("--calendar", default="XNYS")
    result.add_argument("--policy-version", default="liquid_us_common_stocks_v2")
    result.add_argument("--security-type", action="append", dest="security_types")
    result.add_argument("--lookback-sessions", type=int, default=20)
    result.add_argument(
        "--bar-warmup-sessions",
        type=int,
        help="Bar/action warm-up only; does not change universe liquidity policy",
    )
    result.add_argument("--minimum-coverage-ratio", type=float, default=0.90)
    result.add_argument("--minimum-price", type=Decimal, default=Decimal("5"))
    result.add_argument(
        "--minimum-median-dollar-volume",
        type=Decimal,
        default=Decimal("20000000"),
    )
    result.add_argument(
        "--cache-dir",
        type=Path,
        default=BACKEND_DIR / ".cache" / "historical-signal-research",
    )
    result.add_argument("--output", type=Path)
    result.add_argument("--no-resume", action="store_true")
    result.add_argument("--backfill-actions", action="store_true")
    result.add_argument("--backfill-bars", action="store_true")
    result.add_argument("--backfill-sector-references", action="store_true")
    result.add_argument(
        "--fetch-workers",
        type=int,
        default=int(os.getenv("EQUITY_NATIVE_FETCH_WORKERS", "8")),
    )
    return result


def status_report(policy_version: str) -> dict[str, Any]:
    from database import get_db_cursor

    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS runs,
                COUNT(*) FILTER (WHERE replay_available_at IS NULL
                    OR replay_available_at < effective_from
                    OR replay_available_at > observed_at
                    OR source_request_sha256 IS NULL) AS invalid_runs,
                MIN(effective_from) AS first_effective,
                MAX(effective_from) AS last_effective,
                MIN(admitted_members) AS minimum_members,
                MAX(admitted_members) AS maximum_members,
                MAX(policy_sha256) AS policy_sha256
            FROM equity_universe_runs
            WHERE availability_mode = 'HISTORICAL_RECONSTRUCTED'
              AND policy_version = %s
            """,
            (policy_version,),
        )
        universes = dict(cursor.fetchone())
        cursor.execute(
            """
            SELECT COUNT(*) AS memberships,
                   COUNT(DISTINCT member.ticker) AS distinct_tickers,
                   COUNT(*) FILTER (
                       WHERE reference.security_revision_id IS NULL
                   ) AS orphaned_members
            FROM equity_universe_members member
            JOIN equity_universe_runs run
              ON run.universe_run_id = member.universe_run_id
            LEFT JOIN equity_security_reference_revisions reference
              ON reference.security_revision_id = member.security_revision_id
            WHERE run.availability_mode = 'HISTORICAL_RECONSTRUCTED'
              AND run.policy_version = %s
            """,
            (policy_version,),
        )
        memberships = dict(cursor.fetchone())
        cursor.execute(
            """
            SELECT
                COUNT(*) AS actions,
                COUNT(*) FILTER (WHERE replay_available_at IS NULL
                    OR replay_available_at > first_observed_at) AS invalid_actions
            FROM equity_corporate_actions
            WHERE availability_mode = 'HISTORICAL_RECONSTRUCTED'
            """
        )
        actions = dict(cursor.fetchone())
        cursor.execute(
            """
            SELECT
                COUNT(*) AS bars,
                COUNT(DISTINCT ticker) AS distinct_tickers,
                MIN(session_date) AS first_session,
                MAX(session_date) AS last_session,
                COUNT(*) FILTER (WHERE replay_available_at IS NULL
                    OR replay_available_at <> bar_end) AS invalid_availability
            FROM equity_bar_revisions
            WHERE availability_mode = 'HISTORICAL_RECONSTRUCTED'
              AND interval = '1d'
              AND quality_codes @>
                  ARRAY['GROUPED_DAILY_EXACT_TICKER_V2']::TEXT[]
            """
        )
        bars = dict(cursor.fetchone())
        cursor.execute(
            """
            SELECT COUNT(*) AS reconstructed_current_pointers
            FROM equity_current_bar_projection projection
            JOIN equity_bar_revisions bar
              ON bar.bar_revision_id = projection.selected_bar_revision_id
            WHERE bar.availability_mode = 'HISTORICAL_RECONSTRUCTED'
            """
        )
        projections = dict(cursor.fetchone())
        cursor.execute(
            """
            SELECT status, record_count, checksum_sha256, completed_at,
                   gap_details
            FROM equity_ingestion_segments
            WHERE dataset = 'EQUITY_GROUPED_DAILY_BARS'
              AND gap_details->>'policy_sha256' = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (universes.get("policy_sha256"),),
        )
        segment_row = cursor.fetchone()
        cursor.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE status = 'QUARANTINED')
                    AS quarantined_segments,
                COUNT(*) FILTER (WHERE status <> 'QUARANTINED')
                    AS unquarantined_segments
            FROM equity_ingestion_segments
            WHERE dataset = 'EQUITY_GROUPED_DAILY_BARS'
              AND gap_details->>'policy_sha256' =
                  'dc7278014e15c18ee90b39a64fcb1619e99a62c0a2e1e5ce55fb43d16ad0b26c'
            """
        )
        superseded_v1 = dict(cursor.fetchone())
    failures = {
        "invalid_universe_runs": int(universes["invalid_runs"] or 0),
        "orphaned_universe_members": int(
            memberships["orphaned_members"] or 0
        ),
        "invalid_corporate_actions": int(actions["invalid_actions"] or 0),
        "invalid_daily_availability": int(bars["invalid_availability"] or 0),
        "reconstructed_current_pointers": int(
            projections["reconstructed_current_pointers"] or 0
        ),
        "unquarantined_v1_segments": int(
            superseded_v1["unquarantined_segments"] or 0
        ),
    }
    return {
        "status": "PASS" if not any(failures.values()) else "FAIL",
        "policy_version": policy_version,
        "failures": failures,
        "universes": universes,
        "memberships": memberships,
        "corporate_actions": actions,
        "daily_bars": bars,
        "latest_daily_segment": dict(segment_row) if segment_row else None,
        "superseded_v1": superseded_v1,
    }


def backfill_sector_references(
    *,
    client: PolygonEquityClient,
    cache: ResponseCache,
    policy_version: str,
    fetch_workers: int,
) -> dict[str, Any]:
    repository = EquityReferenceRepository()
    candidates = repository.list_historical_sector_candidates(policy_version)
    worker_state = threading.local()

    def fetch(candidate: dict[str, Any]):
        worker_client = getattr(worker_state, "client", None)
        if worker_client is None:
            worker_client = client.fork()
            worker_state.client = worker_client
        as_of_date = candidate["effective_from"].date()
        payloads = cache.get_or_fetch(
            "ticker-overview",
            f"{candidate['ticker']}-{as_of_date.isoformat()}",
            lambda: [payload] if (
                payload := worker_client.fetch_ticker_overview(
                    candidate["ticker"], as_of_date=as_of_date
                )
            ) else [],
        )
        if not payloads:
            return candidate, None
        revision = normalize_security_reference(
            payloads[0],
            observed_at=datetime.now(timezone.utc),
            source_as_of_date=as_of_date,
        )
        if revision.security_id != candidate["security_id"]:
            raise RuntimeError(
                f"historical sector identity changed for {candidate['ticker']}"
            )
        return candidate, revision

    with ThreadPoolExecutor(max_workers=fetch_workers) as pool:
        fetched = tuple(pool.map(fetch, candidates))
    classified = tuple(
        revision for _, revision in fetched
        if revision is not None and revision.sector is not None
    )
    unresolved = sorted(
        candidate["ticker"] for candidate, revision in fetched
        if revision is None or revision.sector is None
    )
    inserted = sum(
        repository.persist_security_revisions(classified[offset:offset + 500])
        for offset in range(0, len(classified), 500)
    )
    return {
        "candidates": len(candidates),
        "classified": len(classified),
        "inserted": inserted,
        "unresolved": unresolved,
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.sessions <= 0:
        raise ValueError("--sessions must be positive")
    if args.fetch_workers <= 0:
        raise ValueError("--fetch-workers must be positive")
    if args.bar_warmup_sessions is not None and args.bar_warmup_sessions < 0:
        raise ValueError("--bar-warmup-sessions cannot be negative")
    if args.backfill_bars and not args.persist:
        raise ValueError("--backfill-bars requires --persist")
    if args.backfill_actions and not args.persist:
        raise ValueError("--backfill-actions requires --persist")
    if args.backfill_sector_references and not args.persist:
        raise ValueError("--backfill-sector-references requires --persist")

    observed_at = datetime.now(timezone.utc)
    client = PolygonEquityClient()
    cache = ResponseCache(args.cache_dir)
    if args.backfill_sector_references:
        sector_result = backfill_sector_references(
            client=client,
            cache=cache,
            policy_version=args.policy_version,
            fetch_workers=args.fetch_workers,
        )
        if not args.backfill_actions and not args.backfill_bars:
            report = {
                "availability_mode": "HISTORICAL_RECONSTRUCTED",
                "cache_directory": str(args.cache_dir),
                "policy_version": args.policy_version,
                "sector_references": sector_result,
                "status": "PERSISTED",
            }
            serialized = json.dumps(report, indent=2, sort_keys=True)
            if args.output:
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(serialized + "\n", encoding="utf-8")
            print(serialized, flush=True)
            return report

    end_date = date.fromisoformat(args.end) if args.end else observed_at.date()
    security_types = tuple(args.security_types or ("CS",))
    policy = HistoricalUniversePolicy(
        policy_version=args.policy_version,
        security_types=security_types,
        lookback_sessions=args.lookback_sessions,
        minimum_coverage_ratio=args.minimum_coverage_ratio,
        minimum_price=args.minimum_price,
        minimum_median_dollar_volume=args.minimum_median_dollar_volume,
    )
    plan = historical_session_plan(
        end_date=end_date,
        research_sessions=args.sessions,
        warmup_sessions=max(
            policy.lookback_sessions, args.bar_warmup_sessions or 0
        ),
        calendar_name=args.calendar,
    )
    daily_history = {}
    for position, session_date in enumerate(plan.all_sessions, start=1):
        rows = cache.get_or_fetch(
            "grouped-unadjusted",
            session_date.isoformat(),
            lambda value=session_date: list(
                client.fetch_grouped_daily(value, adjusted=False)
            ),
        )
        daily_history[session_date] = grouped_daily_rows(rows)
        print(
            f"grouped {position}/{len(plan.all_sessions)} {session_date} "
            f"rows={len(daily_history[session_date])}",
            flush=True,
        )

    universe_repository = EquityUniverseRepository() if args.persist else None
    reference_repository = EquityReferenceRepository() if args.persist else None
    calendar = exchange_calendars.get_calendar(args.calendar)
    research_summaries = []
    eligible_union: set[str] = set()
    inserted_runs = resumed_runs = inserted_references = 0
    all_sessions = list(plan.all_sessions)

    for position, signal_date in enumerate(plan.research_sessions, start=1):
        effective_from = datetime.combine(signal_date, time.min, tzinfo=timezone.utc)
        existing = None
        if universe_repository is not None and not args.no_resume:
            existing = universe_repository.get_reconstructed_session(
                policy_sha256=policy.policy_sha256,
                effective_from=effective_from,
            )
        if existing is not None:
            tickers = universe_repository.member_tickers(existing["universe_run_id"])
            eligible_union.update(tickers)
            resumed_runs += 1
            research_summaries.append({
                "date": signal_date.isoformat(),
                "members": len(tickers),
                "status": "RESUMED",
                "universe_run_id": str(existing["universe_run_id"]),
            })
            print(
                f"universe {position}/{len(plan.research_sessions)} {signal_date} "
                f"members={len(tickers)} status=RESUMED",
                flush=True,
            )
            continue

        references = []
        for security_type in policy.security_types:
            references.extend(cache.get_or_fetch(
                f"tickers-{security_type}",
                signal_date.isoformat(),
                lambda value=signal_date, kind=security_type: list(
                    client.fetch_tickers_as_of(
                        value,
                        market=policy.market,
                        security_type=kind,
                        active=policy.active_only,
                    )
                ),
            ))
        signal_position = all_sessions.index(signal_date)
        prior_sessions = all_sessions[
            max(0, signal_position - policy.lookback_sessions):signal_position
        ]
        selection = select_historical_members(
            references,
            daily_history,
            signal_date=signal_date,
            prior_sessions=prior_sessions,
            policy=policy,
        )
        member_tickers = tuple(row.ticker for row in selection.members)
        eligible_union.update(member_tickers)
        source_request_sha256 = sha256_json(references)
        run_id = historical_universe_run_id(
            signal_date=signal_date,
            policy_sha256=policy.policy_sha256,
            source_request_sha256=source_request_sha256,
            member_tickers=member_tickers,
        )
        if universe_repository is not None and reference_repository is not None:
            revisions = tuple(
                normalize_security_reference(
                    dict(member.reference_payload),
                    observed_at=observed_at,
                    source_as_of_date=signal_date,
                )
                for member in selection.members
            )
            inserted_references += reference_repository.persist_security_revisions(revisions)
            session_open = calendar.session_open(pd.Timestamp(signal_date)).to_pydatetime()
            universe_repository.persist_complete_run(
                universe_run_id=run_id,
                source="POLYGON_HISTORICAL_REFERENCE",
                mode="RANKED",
                effective_from=effective_from,
                observed_at=observed_at,
                policy_version=policy.policy_version,
                policy_sha256=policy.policy_sha256,
                members=revisions,
                configuration={
                    "exclusion_counts": dict(selection.exclusion_counts),
                    "lookback_sessions": policy.lookback_sessions,
                    "minimum_coverage_ratio": policy.minimum_coverage_ratio,
                    "minimum_median_dollar_volume": str(
                        policy.minimum_median_dollar_volume
                    ),
                    "minimum_price": str(policy.minimum_price),
                    "reference_count": selection.reference_count,
                    "security_types": list(policy.security_types),
                },
                availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
                replay_available_at=session_open,
                source_request_sha256=source_request_sha256,
                member_metadata={
                    member.ticker: {
                        "score": float(member.median_dollar_volume),
                        "reasons": (
                            "ACTIVE_ON_SIGNAL_DATE",
                            "PRIOR_SESSION_PRICE_ELIGIBLE",
                            "PRIOR_SESSION_LIQUIDITY_ELIGIBLE",
                        ),
                    }
                    for member in selection.members
                },
            )
            inserted_runs += 1
        research_summaries.append({
            "date": signal_date.isoformat(),
            "exclusion_counts": dict(selection.exclusion_counts),
            "members": len(selection.members),
            "references": selection.reference_count,
            "status": "PERSISTED" if args.persist else "DRY_RUN",
            "universe_run_id": str(run_id),
        })
        print(
            f"universe {position}/{len(plan.research_sessions)} {signal_date} "
            f"references={selection.reference_count} members={len(selection.members)} "
            f"status={'PERSISTED' if args.persist else 'DRY_RUN'}",
            flush=True,
        )

    benchmark_tickers = HISTORICAL_BENCHMARK_TICKERS
    eligible_union.update(benchmark_tickers)

    action_result = None
    if args.backfill_actions and eligible_union:
        assert reference_repository is not None
        securities = reference_repository.list_securities_as_of(
            tuple(sorted(eligible_union)), DecisionWatermark(observed_at, observed_at)
        )
        security_ids = {row.ticker: row.security_id for row in securities}
        missing_reference_tickers = sorted(eligible_union - set(security_ids))
        if missing_reference_tickers:
            raise RuntimeError(
                "corporate-action backfill missing security references for "
                f"{len(missing_reference_tickers)} eligible tickers"
            )
        action_start = plan.all_sessions[0]
        action_end = plan.research_sessions[-1]
        cache_key = f"{action_start}_{action_end}"
        split_rows = cache.get_or_fetch(
            "splits",
            cache_key,
            lambda: list(client.fetch_splits(action_start, action_end)),
        )
        dividend_rows = cache.get_or_fetch(
            "dividends",
            cache_key,
            lambda: list(client.fetch_dividends(action_start, action_end)),
        )
        actions = (
            *normalize_corporate_actions(
                split_rows,
                security_ids=security_ids,
                action_type="SPLIT",
                observed_at=observed_at,
                availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
            ),
            *normalize_corporate_actions(
                dividend_rows,
                security_ids=security_ids,
                action_type="DIVIDEND",
                observed_at=observed_at,
                availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
            ),
        )
        inserted_actions = EquityCorporateActionRepository().persist(actions)
        action_result = {
            "dividends_fetched": len(dividend_rows),
            "eligible_actions": len(actions),
            "inserted_actions": inserted_actions,
            "splits_fetched": len(split_rows),
        }

    bar_result = None
    if args.backfill_bars and eligible_union:
        assert reference_repository is not None
        securities = reference_repository.list_securities_as_of(
            tuple(sorted(eligible_union)), DecisionWatermark(observed_at, observed_at)
        )
        security_ids = {row.ticker: row.security_id for row in securities}
        missing_reference_tickers = sorted(eligible_union - set(security_ids))
        if missing_reference_tickers:
            raise RuntimeError(
                "daily-bar backfill missing security references for "
                f"{len(missing_reference_tickers)} eligible tickers"
            )
        segment_id = uuid5(
            NAMESPACE_URL,
            "historical-grouped-daily:"
            + sha256_json({
                "end": plan.research_sessions[-1].isoformat(),
                "members": sorted(security_ids),
                "policy_sha256": policy.policy_sha256,
                "start": plan.all_sessions[0].isoformat(),
            }),
        )
        ingestion_repository = EquityIngestionRepository()
        bar_repository = EquityBarRepository()
        ingestion_repository.start_segment(
            ingestion_segment_id=segment_id,
            provider="polygon",
            provider_mode="REPLAY",
            dataset="EQUITY_GROUPED_DAILY_BARS",
            interval="1d",
            requested_from=datetime.combine(
                plan.all_sessions[0], time.min, tzinfo=timezone.utc
            ),
            requested_to=datetime.combine(
                plan.research_sessions[-1], time.max, tzinfo=timezone.utc
            ),
            observed_at=observed_at,
        )
        daily_bar_count = daily_inserted = 0
        tickers_with_bars = set()
        payload_manifest = []
        for session_date in plan.all_sessions:
            rows = cache.get_or_fetch(
                "grouped-unadjusted",
                session_date.isoformat(),
                lambda value=session_date: list(
                    client.fetch_grouped_daily(value, adjusted=False)
                ),
            )
            daily_bars = normalize_grouped_daily_bars(
                rows,
                session_date=session_date,
                security_ids=security_ids,
                observed_at=observed_at,
                ingestion_segment_id=segment_id,
                availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
                adjusted=False,
                calendar_name=args.calendar,
            )
            daily_bar_count += len(daily_bars)
            daily_inserted += bar_repository.persist(daily_bars)
            tickers_with_bars.update(row.ticker for row in daily_bars)
            payload_manifest.extend(
                (row.ticker, row.session_date.isoformat(), row.payload_sha256)
                for row in daily_bars
            )
        missing_daily_tickers = sorted(set(security_ids) - tickers_with_bars)
        ingestion_repository.complete_segment(
            segment_id,
            status="DEGRADED" if missing_daily_tickers else "COMPLETE",
            market_watermark=calendar.session_close(
                pd.Timestamp(plan.research_sessions[-1])
            ).to_pydatetime(),
            record_count=daily_bar_count,
            checksum_sha256=sha256_json(sorted(payload_manifest)),
            gap_details={
                "availability_mode": "HISTORICAL_RECONSTRUCTED",
                "missing_tickers": missing_daily_tickers,
                "policy_sha256": policy.policy_sha256,
                "source": "POLYGON_GROUPED_DAILY",
            },
            completed_at=datetime.now(timezone.utc),
        )
        bar_result = {
            "daily_bars": daily_bar_count,
            "daily_bars_inserted": daily_inserted,
            "ingestion_segment_id": str(segment_id),
            "missing_daily_tickers": missing_daily_tickers,
            "source": "POLYGON_GROUPED_DAILY",
        }

    report = {
        "availability_mode": "HISTORICAL_RECONSTRUCTED",
        "bars": bar_result,
        "benchmark_tickers": sorted(benchmark_tickers),
        "cache_directory": str(args.cache_dir),
        "eligible_union": len(eligible_union),
        "corporate_actions": action_result,
        "first_research_session": plan.research_sessions[0].isoformat(),
        "inserted_references": inserted_references,
        "inserted_runs": inserted_runs,
        "last_research_session": plan.research_sessions[-1].isoformat(),
        "policy_sha256": policy.policy_sha256,
        "policy_version": policy.policy_version,
        "research_sessions": len(plan.research_sessions),
        "resumed_runs": resumed_runs,
        "sessions": research_summaries,
        "status": "PERSISTED" if args.persist else "DRY_RUN",
        "warmup_sessions": len(plan.warmup_sessions),
        "universe_lookback_sessions": policy.lookback_sessions,
    }
    serialized = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialized + "\n", encoding="utf-8")
    print(serialized, flush=True)
    return report


def main() -> int:
    args = parser().parse_args()
    if args.status:
        print(json.dumps(status_report(args.policy_version), default=str, indent=2))
        return 0
    if args.persist:
        with try_advisory_leadership(LOCK_NAME) as is_leader:
            if not is_leader:
                raise RuntimeError(
                    "equity worker is active; historical input persistence requires leadership"
                )
            run(args)
    else:
        run(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())