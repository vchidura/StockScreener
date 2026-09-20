#!/usr/bin/env python3
"""Ingest a split-adjusted daily bar lineage from Polygon grouped daily.

Canonical daily bars are DERIVED from unadjusted 30m, so splits propagate as
false gaps into every higher interval. This writes an independent
`adjusted = true` daily lineage. `equity_canonical_bars` pins `adjusted = false`,
so the UI and options paths are unaffected; research reads it explicitly via
`list_final_after(..., adjusted=True)`.
"""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
import json
import sys
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import exchange_calendars
import pandas as pd
import requests
from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from equity.domain import BarAvailabilityMode, DecisionWatermark
from database import get_db_cursor
from equity.polygon import PolygonEquityClient, normalize_grouped_daily_bars, normalize_security_reference, sha256_json
from equity.repositories import (
    EquityBarRepository,
    EquityUniverseRepository,
)
from research.gics_sectors import (
    ALTERNATE_MARKET_ETF,
    BROAD_MARKET_ETF,
    SECTOR_BENCHMARK_ETF,
)
from scripts.prepare_historical_signal_research import ResponseCache

# Every outcome needs a market and a sector leg, so these must exist in the same
# lineage as the subjects even though they are never study subjects themselves.
BENCHMARK_TICKERS = frozenset({
    BROAD_MARKET_ETF, ALTERNATE_MARKET_ETF, *SECTOR_BENCHMARK_ETF.values(),
})

# equity_bar_revisions stores prices and volume as numeric(20,8).
NUMERIC_LIMIT = Decimal(10) ** 12
PRICE_FIELDS = ("open_price", "high_price", "low_price", "close_price", "volume", "vwap")


def storable(bar) -> bool:
    """Cumulative reverse splits can push back-adjusted prices past the column range.

    Mullen Automotive reaches ~1.7e13 per share in 2021 after roughly 1:1e12 of
    cumulative reverse splits. The series is arithmetically fine - returns are
    ratios - but it cannot be stored, so the affected bars are skipped and
    reported rather than silently rounded.
    """
    for field in PRICE_FIELDS:
        value = getattr(bar, field, None)
        if value is not None and abs(Decimal(value)) >= NUMERIC_LIMIT:
            return False
    return True


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--start", required=True, help="YYYY-MM-DD")
    result.add_argument("--end", required=True, help="YYYY-MM-DD")
    result.add_argument("--calendar", default="XNYS")
    result.add_argument(
        "--include-non-common", action="store_true",
        help="Keep ETFs and ETVs; excluded by default because their benchmarks "
             "are self-referential",
    )
    result.add_argument(
        "--include-non-common-ticker", action="append", default=[],
        help="Keep one explicitly selected ETF without admitting every non-common ticker; repeatable.",
    )
    result.add_argument(
        "--ticker", action="append", default=[],
        help="Restrict ingestion to selected universe tickers; repeatable and fail-closed.",
    )
    result.add_argument(
        "--skip-benchmarks", action="store_true",
        help="Do not add research benchmark ETFs; allowed only with explicit --ticker values.",
    )
    result.add_argument(
        "--security-type", action="append", choices=("CS", "ETF", "ETV"),
        help="Restrict dated reference types; repeatable. Defaults preserve existing behavior.",
    )
    result.add_argument(
        "--fetch-missing-reference-cache", action="store_true",
        help="Fetch only missing ticker-scoped dated references; cannot be combined with --apply.",
    )
    result.add_argument("--limit-sessions", type=int)
    result.add_argument(
        "--from-reconstructed-universes", action="store_true",
        help="Cover every member of every reconstructed universe run instead of "
             "the latest live universe. Required before a point-in-time study, "
             "whose cohort is far wider than the live universe.",
    )
    result.add_argument(
        "--policy-version", default="liquid_us_common_stocks_v2",
        help="Universe policy to draw members from with "
             "--from-reconstructed-universes",
    )
    result.add_argument("--apply", action="store_true")
    result.add_argument(
        "--reference-cache-dir", type=Path,
        default=BACKEND_DIR / ".cache" / "historical-signal-research",
        help="Checksum-verified tickers-CS/ETF (also ETV with --include-non-common) "
             "caches for every requested date; never filled from current references",
    )
    result.add_argument("--output", type=Path)
    return result


def reconstructed_union(policy_version: str) -> tuple[str, ...]:
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT member.ticker
            FROM equity_universe_members member
            JOIN equity_original_universe_runs run
              ON run.universe_run_id = member.universe_run_id
            WHERE run.availability_mode = 'HISTORICAL_RECONSTRUCTED'
              AND run.policy_version = %s
            ORDER BY member.ticker
            """,
            (policy_version,),
        )
        return tuple(row["ticker"] for row in cursor.fetchall())


@dataclass(frozen=True)
class DatedIdentityMap:
    security_ids: dict[str, UUID]
    excluded_tickers: frozenset[str]
    source_sha256: str


def dated_security_ids(cache_dir, session_date, tickers, *, observed_at,
                       security_types=("CS", "ETF"), include_non_common=False,
                       allowed_non_common_tickers=(), reference_fetcher=None,
                       cache_scope_tickers=()):

    cache = ResponseCache(cache_dir)
    selected = set(tickers)
    scoped_tickers = tuple(sorted({ticker.upper() for ticker in cache_scope_tickers}))
    allowed_non_common = {ticker.upper() for ticker in allowed_non_common_tickers}
    if not allowed_non_common <= selected:
        raise ValueError("explicit non-common tickers must be in the selected universe")
    identities = {}
    identity_tickers = {}
    source_hashes = {}
    for security_type in security_types:
        cache_kind = f"tickers-{security_type}"
        if scoped_tickers:
            cache_kind += "-subset-" + sha256_json(scoped_tickers)[:16]

        def missing_cache(current_type=security_type):
            if reference_fetcher is None:
                raise ValueError(f"DATED_REFERENCE_CACHE_MISSING: {cache_kind}_{session_date}")
            return reference_fetcher(current_type, session_date, scoped_tickers or tuple(sorted(selected)))

        references = cache.get_or_fetch(cache_kind, session_date.isoformat(), missing_cache)
        source_hashes[security_type] = sha256_json(references)
        for payload in references:
            if not isinstance(payload, dict) or payload.get("type") != security_type:
                raise ValueError(f"DATED_REFERENCE_TYPE_MISMATCH: {session_date}")
            ticker = payload.get("ticker")
            if ticker not in selected:
                continue
            cik = str(payload.get("cik") or "").strip()
            if not (payload.get("composite_figi") or payload.get("share_class_figi")
                    or (cik.isdigit() and cik.lstrip("0"))):
                raise ValueError(f"DATED_IDENTITY_EVIDENCE_MISSING: {session_date} {ticker}")
            revision = normalize_security_reference(
                payload, observed_at=observed_at, source_as_of_date=session_date,
            )
            identity = (revision.security_id, revision.cik, revision.share_class_figi, revision.security_type)
            if ticker in identities and identities[ticker] != identity:
                raise ValueError(f"AMBIGUOUS_DATED_IDENTITY: {session_date} {ticker}")
            other_ticker = identity_tickers.get(revision.security_id)
            if other_ticker is not None and other_ticker != ticker:
                raise ValueError(f"DATED_SECURITY_ID_COLLISION: {session_date} {other_ticker} {ticker}")
            identities[ticker] = identity
            identity_tickers[revision.security_id] = ticker
    excluded = frozenset(ticker for ticker, identity in identities.items()
                         if not include_non_common and identity[3] != "CS"
                         and ticker not in BENCHMARK_TICKERS and ticker not in allowed_non_common)
    return DatedIdentityMap(
        security_ids={ticker: identity[0] for ticker, identity in identities.items() if ticker not in excluded},
        excluded_tickers=excluded,
        source_sha256=sha256_json({"session_date": session_date.isoformat(), "sources": source_hashes}),
    )


def existing_identity_conflicts(cursor, session_date, security_ids):
    tickers = sorted(security_ids)
    cursor.execute(
        """
        WITH expected AS (
            SELECT * FROM unnest(%s::TEXT[], %s::UUID[]) AS identity(ticker, security_id)
        )
        SELECT bar.ticker, COUNT(*) AS revisions
        FROM equity_bar_revisions AS bar JOIN expected USING (ticker)
        WHERE bar.session_date = %s AND bar.interval = '1d'
          AND bar.session_scope = 'RTH' AND bar.adjusted = TRUE
          AND bar.availability_mode = 'HISTORICAL_RECONSTRUCTED'
          AND bar.security_id <> expected.security_id
        GROUP BY bar.ticker ORDER BY bar.ticker
        """, (tickers, [str(security_ids[ticker]) for ticker in tickers], session_date),
    )
    return [dict(row) for row in cursor.fetchall()]


def identity_preflight(sessions, tickers, *, cache_dir, observed_at, include_non_common=False,
                       allowed_non_common_tickers=(), required_benchmark_tickers=BENCHMARK_TICKERS,
                       security_types=None, reference_fetcher=None, cache_scope_tickers=()):
    plans = {}
    issues = []
    security_types = tuple(security_types or (("CS", "ETF", "ETV") if include_non_common else ("CS", "ETF")))
    for session in sessions:
        try:
            identity = dated_security_ids(
                cache_dir, session, tickers, observed_at=observed_at,
                security_types=security_types, include_non_common=include_non_common,
                allowed_non_common_tickers=allowed_non_common_tickers,
                reference_fetcher=reference_fetcher,
                cache_scope_tickers=cache_scope_tickers,
            )
        except (ValueError, OSError) as error:
            issues.append({"session": session.isoformat(), "reason": "DATED_REFERENCE_UNRESOLVED", "detail": str(error)})
            continue
        missing_benchmarks = sorted(set(required_benchmark_tickers) - set(identity.security_ids))
        if missing_benchmarks:
            issues.append({"session": session.isoformat(), "reason": "DATED_BENCHMARK_REFERENCE_MISSING",
                           "tickers": missing_benchmarks})
            continue
        plans[session] = identity
    if plans:
        with get_db_cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '180s'")
            for session, identity in plans.items():
                conflicts = existing_identity_conflicts(cursor, session, identity.security_ids)
                if conflicts:
                    issues.append({"session": session.isoformat(), "reason": "VERSIONED_IDENTITY_REPAIR_REQUIRED",
                                   "tickers": len(conflicts), "examples": conflicts[:20]})
    return plans, {
        "contract": "EXACT_DATE_CACHED_REFERENCE_V1",
        "status": "BLOCKED" if issues or not plans else "READY_FOR_PRICE_VALIDATION",
        "reference_sessions_resolved": len(plans),
        "existing_lineage_checked": bool(plans) and len(plans) == len(sessions),
        "existing_lineage_checked_sessions": len(plans),
        "issue_counts": dict(Counter(issue["reason"] for issue in issues)),
        "issue_examples": issues[:20],
        "source_sha256": sha256_json({session.isoformat(): identity.source_sha256 for session, identity in plans.items()}),
    }


def validate_price_identities(rows, tickers, identity, session_date):
    selected = set(tickers) - identity.excluded_tickers
    present = [str(row.get("T") or row.get("ticker") or "") for row in rows]
    missing = sorted(set(present).intersection(selected) - set(identity.security_ids))
    if missing:
        raise ValueError(f"PRICE_IDENTITY_UNRESOLVED: {session_date} {', '.join(missing[:20])}")
    duplicate = sorted(ticker for ticker, count in Counter(present).items() if ticker in selected and count > 1)
    if duplicate:
        raise ValueError(f"DUPLICATE_PRICE_TICKER: {session_date} {', '.join(duplicate[:20])}")
    return set(present).intersection(identity.security_ids)


def write_report(report, output):
    rendered = json.dumps(report, indent=2)
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered, flush=True)


def with_backoff(operation, *, attempts: int = 6):
    delay = 2.0
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except requests.HTTPError as error:
            status = getattr(error.response, "status_code", None)
            if status not in (429, 502, 503, 504) or attempt == attempts:
                raise
            print(f"    polygon {status}; retrying in {delay:.0f}s", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 60.0)
    raise RuntimeError("unreachable")


def main() -> int:
    arguments = parser().parse_args()
    start = date.fromisoformat(arguments.start)
    end = date.fromisoformat(arguments.end)
    if end < start or (arguments.limit_sessions is not None and arguments.limit_sessions <= 0):
        raise SystemExit("end must not precede start and limit-sessions must be positive")
    observed_at = datetime.now(timezone.utc)
    watermark = DecisionWatermark(observed_at, observed_at)

    universe_repository = EquityUniverseRepository()
    if arguments.from_reconstructed_universes:
        tickers = reconstructed_union(arguments.policy_version)
        if not tickers:
            raise SystemExit(
                "no reconstructed universe members found for "
                f"{arguments.policy_version}; run "
                "prepare_historical_signal_research.py --persist first"
            )
    else:
        universe = universe_repository.get_latest_as_of(watermark)
        if universe is None:
            raise SystemExit("no live universe run is available")
        tickers = universe_repository.member_tickers(universe["universe_run_id"])
    requested_tickers = tuple(dict.fromkeys(
        ticker.strip().upper() for ticker in arguments.ticker if ticker.strip()
    ))
    if arguments.skip_benchmarks and not requested_tickers:
        raise SystemExit("--skip-benchmarks requires at least one explicit --ticker")
    if requested_tickers:
        if not set(requested_tickers) <= set(tickers):
            raise SystemExit("explicit tickers must already belong to the selected universe")
        tickers = requested_tickers
    if arguments.fetch_missing_reference_cache and arguments.apply:
        raise SystemExit("reference-cache fetch and adjusted-price apply must be separate stages")
    calendar = exchange_calendars.get_calendar(arguments.calendar)
    sessions = [
        pd.Timestamp(value).date()
        for value in calendar.sessions_in_range(pd.Timestamp(start), pd.Timestamp(end))
    ]
    if arguments.limit_sessions:
        sessions = sessions[:arguments.limit_sessions]
    if not sessions:
        raise SystemExit("requested range has no exchange sessions")
    if any(calendar.session_close(pd.Timestamp(session)).to_pydatetime() > observed_at for session in sessions):
        raise SystemExit("requested range includes an unfinished exchange session")

    selected_tickers = tuple(sorted(
        set(tickers) if arguments.skip_benchmarks else set(tickers) | set(BENCHMARK_TICKERS)
    ))
    allowed_non_common_tickers = tuple(dict.fromkeys(
        ticker.strip().upper() for ticker in arguments.include_non_common_ticker if ticker.strip()
    ))
    if any(not ticker.replace(".", "").replace("-", "").isalnum() for ticker in allowed_non_common_tickers):
        raise SystemExit("explicit non-common tickers must be valid symbols")
    if not set(allowed_non_common_tickers) <= set(selected_tickers):
        raise SystemExit("explicit non-common tickers must already belong to the selected universe")
    security_types = tuple(dict.fromkeys(arguments.security_type or (
        ("CS", "ETF", "ETV") if arguments.include_non_common else ("CS", "ETF")
    )))
    reference_client = PolygonEquityClient() if arguments.fetch_missing_reference_cache else None

    def fetch_references(security_type, session_date, scoped_tickers):
        rows = []
        for ticker in scoped_tickers:
            payload = reference_client.fetch_ticker_overview(ticker, as_of_date=session_date)
            if payload is not None and payload.get("type") == security_type:
                rows.append(payload)
        return rows

    identity_plans, identity_report = identity_preflight(
        sessions, selected_tickers, cache_dir=arguments.reference_cache_dir,
        observed_at=observed_at, include_non_common=arguments.include_non_common,
        allowed_non_common_tickers=allowed_non_common_tickers,
        required_benchmark_tickers=() if arguments.skip_benchmarks else BENCHMARK_TICKERS,
        security_types=security_types,
        reference_fetcher=fetch_references if reference_client is not None else None,
        cache_scope_tickers=requested_tickers if arguments.skip_benchmarks else (),
    )

    report = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "sessions": len(sessions),
        "universe_members": len(tickers),
        "member_source": (
            "RECONSTRUCTED_UNIVERSES" if arguments.from_reconstructed_universes
            else "LATEST_LIVE_UNIVERSE"
        ),
        "securities_requested": len(selected_tickers),
        "explicit_tickers": list(requested_tickers),
        "benchmarks_included": not arguments.skip_benchmarks,
        "reference_cache_fetch_enabled": arguments.fetch_missing_reference_cache,
        "reference_security_types": list(security_types),
        "common_stock_only": not arguments.include_non_common,
        "explicit_non_common_tickers": list(allowed_non_common_tickers),
        "mode": "APPLY" if arguments.apply else "DRY_RUN",
        "identity_preflight": identity_report,
        "research_readiness": "NOT_CERTIFIED",
    }
    if identity_report["status"] == "BLOCKED":
        report["note"] = "nothing fetched or written; resolve dated identity evidence and use versioned repairs for existing conflicts"
        write_report(report, arguments.output)
        return 2
    if not arguments.apply:
        report["note"] = (
            "dated references cached; no adjusted prices fetched or database rows written"
            if arguments.fetch_missing_reference_cache
            else "nothing fetched or written; identity checks pass, actual provider prices still require validation"
        )
        write_report(report, arguments.output)
        return 0

    client = PolygonEquityClient()
    bar_repository = EquityBarRepository()
    normalized = inserted = empty_sessions = 0
    unentitled: list[str] = []
    out_of_range: dict[str, int] = {}
    for position, session_date in enumerate(sessions, 1):
        identity = identity_plans[session_date]
        try:
            rows = with_backoff(
                lambda: client.fetch_grouped_daily(session_date, adjusted=True)
            )
        except requests.HTTPError as error:
            if getattr(error.response, "status_code", None) != 403:
                raise
            unentitled.append(session_date.isoformat())
            continue
        if not rows:
            empty_sessions += 1
            continue
        expected_tickers = validate_price_identities(rows, selected_tickers, identity, session_date)
        bars = normalize_grouped_daily_bars(
            rows,
            session_date=session_date,
            security_ids=identity.security_ids,
            observed_at=observed_at,
            ingestion_segment_id=None,
            availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
            adjusted=True,
            calendar_name=arguments.calendar,
        )
        if expected_tickers != {row.ticker for row in bars}:
            raise ValueError(f"PRICE_NORMALIZATION_INCOMPLETE: {session_date}")
        normalized += len(bars)
        storable_bars = []
        for row in bars:
            if storable(row):
                storable_bars.append(row)
            else:
                out_of_range[row.ticker] = out_of_range.get(row.ticker, 0) + 1
        inserted += bar_repository.persist(tuple(storable_bars))
        if position % 50 == 0:
            print(f"  {position}/{len(sessions)} sessions, {inserted} bars inserted",
                  flush=True)

    report.update({
        "bars_normalized": normalized,
        "bars_inserted": inserted,
        "sessions_without_rows": empty_sessions,
        "sessions_not_entitled": len(unentitled),
        "bars_out_of_range": sum(out_of_range.values()),
        "tickers_out_of_range": dict(sorted(out_of_range.items())),
        "first_entitled_session": (
            sessions[len(unentitled)].isoformat()
            if unentitled and len(unentitled) < len(sessions) else None
        ),
    })
    write_report(report, arguments.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
