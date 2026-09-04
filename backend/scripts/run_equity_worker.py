#!/usr/bin/env python3
"""Run finalized canonical equity materialization slots."""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv
import requests

load_dotenv(BACKEND_DIR / ".env")

from database import get_selected_tickers
from equity.calendar import latest_expected_market_time
from equity.derivation import DERIVATION_SOURCES
from equity.domain import DecisionWatermark
from equity.historical_research import composite_scanner_outcome_policies
from equity.leadership import try_advisory_leadership
from equity.orchestration import (
    EquityMaterializationService,
    IngestionCoverageError,
    ReferenceRefreshResult,
)
from equity.polygon import PolygonEquityClient
from equity.repositories import (
    EquityAnalysisRepository,
    EquityIngestionRepository,
    EquityReferenceRepository,
    EquityUniverseRepository,
)


LOGGER = logging.getLogger("equity-worker")
POLL_SECONDS = int(os.getenv("EQUITY_WORKER_POLL_SECONDS", "15"))
HEARTBEAT_SECONDS = int(os.getenv("EQUITY_WORKER_HEARTBEAT_SECONDS", "300"))
STALE_RUN_MINUTES = int(os.getenv("EQUITY_STALE_RUN_MINUTES", "60"))
NATIVE_FETCH_WORKERS = int(os.getenv("EQUITY_NATIVE_FETCH_WORKERS", "8"))
PROVIDER_DELAY_MINUTES = int(os.getenv("EQUITY_PROVIDER_DELAY_MINUTES", "15"))
WORKER_LOCK_NAME = os.getenv(
    "EQUITY_WORKER_LOCK_NAME", "stock-screener:equity-materialization-worker"
)
INTERVALS = tuple(
    value.strip() for value in os.getenv(
        "EQUITY_MATERIALIZATION_INTERVALS", "5m,15m,30m,1h,1d,1wk,1mo"
    ).split(",")
    if value.strip()
)
INTERVAL_MINUTES = {"5m": 5, "15m": 15, "30m": 30}
SUPPORTED_INTERVALS = frozenset((*INTERVAL_MINUTES, *DERIVATION_SOURCES))
SCANNER_POLICY_EFFECTIVE_FROM = datetime(2026, 8, 30, tzinfo=timezone.utc)


def latest_completed_slot(now: datetime, interval: str) -> datetime | None:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if interval not in INTERVAL_MINUTES:
        raise ValueError(f"unsupported worker interval: {interval}")
    return latest_expected_market_time(now, interval)


def latest_due_slot(
    now: datetime,
    interval: str,
    *,
    provider_delay: timedelta = timedelta(0),
) -> datetime | None:
    if provider_delay < timedelta(0):
        raise ValueError("provider_delay must not be negative")
    observable_now = now - provider_delay
    if interval in INTERVAL_MINUTES:
        return latest_completed_slot(observable_now, interval)
    if interval in DERIVATION_SOURCES:
        return latest_expected_market_time(observable_now, interval)
    raise ValueError(f"unsupported worker interval: {interval}")


def load_or_refresh_reference(
    service: EquityMaterializationService,
    tickers: tuple[str, ...],
    now: datetime,
    session_date,
) -> ReferenceRefreshResult:
    watermark = DecisionWatermark(now, now)
    references = EquityReferenceRepository().list_securities_as_of(tickers, watermark)
    universe_repository = EquityUniverseRepository()
    universe = universe_repository.get_latest_as_of(watermark)
    universe_tickers = (
        universe_repository.member_tickers(universe["universe_run_id"])
        if universe is not None else frozenset()
    )
    reference_is_fresh = (
        len(references) == len(tickers)
        and all(now - row.observed_at <= timedelta(days=7) for row in references)
        and set(tickers).issubset(universe_tickers)
    )
    if reference_is_fresh and universe is not None:
        return ReferenceRefreshResult(
            universe_run_id=universe["universe_run_id"],
            revisions=references,
            missing_tickers=(),
        )
    return service.refresh_reference(
        tickers,
        observed_at=now,
        as_of_date=session_date,
        include_float=True,
    )


def mature_prospective_scanner_outcomes(
    service: EquityMaterializationService,
    interval: str,
    *,
    available_by: datetime,
) -> tuple[object, ...]:
    try:
        policies = composite_scanner_outcome_policies(
            interval=interval,
            effective_from=SCANNER_POLICY_EFFECTIVE_FROM,
        )
    except ValueError:
        return ()
    results = []
    for policy in policies:
        for horizon_key in json.loads(policy.horizons_json):
            results.append(service.evaluate_directional_outcomes(
                policy,
                horizon_key,
                available_by=available_by,
                prospective_only=True,
            ))
    return tuple(results)


def ingest_due_interval(
    service: EquityMaterializationService,
    revisions,
    *,
    interval: str,
    slot: datetime,
    observed_at: datetime,
    once: bool,
):
    try:
        if interval in DERIVATION_SOURCES:
            return service.derive_interval(
                revisions,
                target_interval=interval,
                watermark=DecisionWatermark(slot, observed_at),
            )
        return service.ingest_native_interval(
            revisions,
            interval=interval,
            start=slot.date(),
            end=slot.date(),
            observed_at=observed_at,
        )
    except (IngestionCoverageError, requests.RequestException):
        if once:
            raise
        LOGGER.exception(
            "transient ingestion failure interval=%s slot=%s; "
            "publication was skipped and the cycle will retry",
            interval,
            slot.isoformat(),
        )
        return None


def run_worker(*, once: bool = False) -> None:
    invalid = set(INTERVALS) - SUPPORTED_INTERVALS
    if invalid:
        raise ValueError(f"unsupported EQUITY_MATERIALIZATION_INTERVALS: {sorted(invalid)}")
    if NATIVE_FETCH_WORKERS <= 0:
        raise ValueError("EQUITY_NATIVE_FETCH_WORKERS must be positive")
    if HEARTBEAT_SECONDS <= 0:
        raise ValueError("EQUITY_WORKER_HEARTBEAT_SECONDS must be positive")
    if PROVIDER_DELAY_MINUTES < 0:
        raise ValueError("EQUITY_PROVIDER_DELAY_MINUTES must not be negative")
    tickers = tuple(get_selected_tickers(active_only=True))
    if not tickers:
        raise RuntimeError("no active selected_tickers are configured")
    stale_after = timedelta(minutes=STALE_RUN_MINUTES)
    analysis_repository = EquityAnalysisRepository()
    recovered_runs = analysis_repository.fail_stale_runs(
        stale_after=stale_after
    )
    recovered_segments = EquityIngestionRepository().fail_stale_segments(
        stale_after=stale_after
    )
    if recovered_runs or recovered_segments:
        LOGGER.warning(
            "terminal-failed stale analysis runs=%s ingestion segments=%s",
            len(recovered_runs), len(recovered_segments),
        )
    service = EquityMaterializationService(
        PolygonEquityClient(), native_fetch_workers=NATIVE_FETCH_WORKERS
    )
    reference = None
    reference_checked_at = None
    completed = analysis_repository.latest_published_market_times(INTERVALS)
    provider_delay = timedelta(minutes=PROVIDER_DELAY_MINUTES)
    last_heartbeat_at = datetime.now(timezone.utc)
    LOGGER.info(
        "worker started intervals=%s provider_delay_minutes=%s resumed_intervals=%s",
        ",".join(INTERVALS),
        PROVIDER_DELAY_MINUTES,
        ",".join(sorted(completed)),
    )
    while True:
        now = datetime.now(timezone.utc)
        for interval in INTERVALS:
            slot = latest_due_slot(now, interval, provider_delay=provider_delay)
            if slot is None or completed.get(interval) == slot:
                continue
            if (
                reference is None
                or reference_checked_at is None
                or now - reference_checked_at >= timedelta(days=7)
            ):
                reference = load_or_refresh_reference(
                    service, tickers, now, slot.date()
                )
                reference_checked_at = now
            LOGGER.info("materializing interval=%s slot=%s", interval, slot.isoformat())
            ingestion = ingest_due_interval(
                service,
                reference.revisions,
                interval=interval,
                slot=slot,
                observed_at=now,
                once=once,
            )
            if ingestion is None:
                break
            LOGGER.info(
                "ingestion complete interval=%s slot=%s bars=%s inserted=%s missing=%s",
                interval,
                slot.isoformat(),
                ingestion.bar_count,
                ingestion.inserted_count,
                len(ingestion.missing_tickers),
            )
            missing_fraction = (
                len(ingestion.missing_tickers) / len(reference.revisions)
                if reference.revisions else 1.0
            )
            if missing_fraction > 0.05:
                LOGGER.error(
                    "interval=%s slot=%s missing %.1f%% of tickers; analysis will fail closed",
                    interval,
                    slot.isoformat(),
                    missing_fraction * 100,
                )
            elif ingestion.missing_tickers:
                LOGGER.warning(
                    "interval=%s slot=%s missing tickers=%s",
                    interval,
                    slot.isoformat(),
                    ",".join(ingestion.missing_tickers[:20]),
                )
            publication = service.publish_canonical_interval(
                reference.revisions,
                interval=interval,
                watermark=DecisionWatermark(slot, now),
            )
            if publication.status == "FAILED":
                LOGGER.error(
                    "canonical publication failed interval=%s slot=%s "
                    "selected=%s missing=%s failed=%s",
                    interval,
                    slot.isoformat(),
                    publication.selected,
                    publication.missing,
                    publication.failed,
                )
                completed[interval] = slot
                continue
            LOGGER.info(
                "publication complete interval=%s slot=%s status=%s selected=%s missing=%s",
                interval,
                slot.isoformat(),
                publication.status,
                publication.selected,
                publication.missing,
            )
            LOGGER.info(
                "maturing outcomes interval=%s slot=%s",
                interval,
                slot.isoformat(),
            )
            outcome_runs = mature_prospective_scanner_outcomes(
                service,
                interval,
                available_by=now,
            )
            if outcome_runs:
                LOGGER.info(
                    "outcomes interval=%s due=%s persisted=%s pending=%s",
                    interval,
                    sum(row.due for row in outcome_runs),
                    sum(row.persisted for row in outcome_runs),
                    sum(row.pending for row in outcome_runs),
                )
            LOGGER.info(
                "materializing analysis interval=%s slot=%s members=%s",
                interval,
                slot.isoformat(),
                len(reference.revisions),
            )
            analysis = service.materialize_interval(
                reference.revisions,
                universe_run_id=reference.universe_run_id,
                interval=interval,
                watermark=DecisionWatermark(slot, now),
            )
            LOGGER.info(
                "ingestion=%s publication=%s analysis=%s",
                ingestion,
                publication,
                analysis,
            )
            completed[interval] = slot
        if once:
            return
        heartbeat_at = datetime.now(timezone.utc)
        if heartbeat_at - last_heartbeat_at >= timedelta(seconds=HEARTBEAT_SECONDS):
            watermarks = ",".join(
                f"{interval}:{completed[interval].isoformat()}"
                for interval in INTERVALS
                if interval in completed
            )
            LOGGER.info(
                "worker heartbeat state=waiting poll_seconds=%s watermarks=%s",
                POLL_SECONDS,
                watermarks,
            )
            last_heartbeat_at = heartbeat_at
        time.sleep(POLL_SECONDS)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--once",
        action="store_true",
        help="Process each configured interval once, then exit.",
    )
    return result


def main() -> int:
    args = parser().parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
    )
    with try_advisory_leadership(WORKER_LOCK_NAME) as is_leader:
        if not is_leader:
            LOGGER.error("another equity materialization worker holds leadership")
            return 2
        run_worker(once=args.once)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())