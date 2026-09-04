#!/usr/bin/env python3
"""Persist intraday scanner events, evaluate 30m outcomes, and qualify them.

Results remain fixed-cohort exploratory research. A passing revision here does not
authorize live recommendations or option conditioning.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pandas as pd
from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor
from equity.intraday_research import (
    INTRADAY_RESEARCH_COHORT,
    IntradayScannerEvent,
    intraday_event_evidence,
    intraday_event_evidence_id,
    intraday_policy_keys,
    intraday_scanner_outcome_policies,
)
from equity.leadership import try_advisory_leadership
from equity.orchestration import EquityMaterializationService
from equity.qualification import qualify_outcomes
from equity.repositories import (
    EquityEvidenceRepository,
    EquityOutcomeRepository,
    EquityUniverseRepository,
)
from research.gics_sectors import BROAD_MARKET_ETF
from research.intraday_scanners import (
    INTRADAY_DETECTOR_POLICY,
    INTRADAY_OUTCOME_HORIZONS,
    INTRADAY_SCANNER_REGISTRY,
)


LOCK_NAME = "stock-screener:intraday-scanner-outcomes"
EVIDENCE_BATCH = 5000
LIMITATIONS = (
    "FIXED_COHORT_EXPLORATORY",
    "SURVIVORSHIP_BIASED_UNIVERSE",
    "NO_POINT_IN_TIME_INTRADAY_MEMBERSHIP",
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--events", type=Path, required=True)
    result.add_argument("--persist-evidence", action="store_true")
    result.add_argument("--evaluate", action="store_true")
    result.add_argument("--qualify", action="store_true")
    result.add_argument("--all", action="store_true")
    result.add_argument("--interval", default="30m", choices=("30m",))
    result.add_argument("--round-trip-cost-bps", type=float, default=4.0)
    result.add_argument("--available-by", help="UTC ISO timestamp; defaults to now")
    result.add_argument(
        "--qualification-effective-from",
        help="Required for --qualify/--all; reviewed UTC ISO publication time",
    )
    result.add_argument(
        "--evaluation-version", default="intraday_scanner_qualification_v1"
    )
    result.add_argument("--minimum-events", type=int, default=100)
    result.add_argument("--minimum-independent-periods", type=int, default=40)
    result.add_argument("--output", type=Path)
    return result


def load_events(path: Path) -> tuple[IntradayScannerEvent, ...]:
    events = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            events.append(IntradayScannerEvent.from_dict(json.loads(line)))
        except Exception as exc:
            raise ValueError(f"invalid intraday event at line {line_number}") from exc
    events.sort(key=lambda row: (row.signal_time, row.ticker, row.event_id.hex))
    return tuple(events)


def eligible_events(
    events: tuple[IntradayScannerEvent, ...]
) -> tuple[IntradayScannerEvent, ...]:
    return tuple(row for row in events if row.payload.get("qualification_eligible"))


def _check_versions(events: tuple[IntradayScannerEvent, ...]) -> tuple[str, ...]:
    sources = tuple(sorted({row.source_name for row in events}))
    unknown = set(sources) - set(INTRADAY_SCANNER_REGISTRY)
    if unknown:
        raise ValueError(f"unregistered intraday sources: {sorted(unknown)}")
    for event in events:
        expected = INTRADAY_SCANNER_REGISTRY[event.source_name].source_version
        if event.source_version != expected:
            raise ValueError(
                f"intraday source version mismatch for {event.source_name}: "
                f"{event.source_version} != {expected}"
            )
    return sources


def persist_evidence(events: tuple[IntradayScannerEvent, ...]) -> dict[str, int]:
    universe_repository = EquityUniverseRepository()
    securities: dict[tuple[UUID, str], object] = {}
    tickers_by_run: dict[UUID, set[str]] = {}
    for event in events:
        tickers_by_run.setdefault(event.universe_run_id, set()).add(event.ticker)
    for universe_run_id in sorted(tickers_by_run, key=str):
        for security in universe_repository.members_for_replay(
            universe_run_id, sorted(tickers_by_run[universe_run_id])
        ):
            securities[(universe_run_id, security.ticker)] = security
    missing = [
        row for row in events
        if (row.universe_run_id, row.ticker) not in securities
    ]
    if missing:
        raise RuntimeError(
            f"{len(missing)} intraday events lack linked security revisions"
        )
    repository = EquityEvidenceRepository()
    inserted = 0
    for offset in range(0, len(events), EVIDENCE_BATCH):
        batch = events[offset:offset + EVIDENCE_BATCH]
        inserted += repository.persist(tuple(
            intraday_event_evidence(
                event, securities[(event.universe_run_id, event.ticker)]
            )
            for event in batch
        ))
    return {"events": len(events), "evidence_inserted": inserted}


def maturity_cutoff(interval: str, horizon_bars: int) -> datetime:
    """Latest signal bar end that still has a full entry-plus-horizon bar path."""
    if horizon_bars <= 0:
        raise ValueError("horizon_bars must be positive")
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT bar_end
            FROM equity_bar_revisions
            WHERE ticker = %s
              AND interval = %s
              AND session_scope = 'RTH'
              AND adjusted = FALSE
              AND is_final = TRUE
            ORDER BY bar_end DESC
            LIMIT %s
            """,
            (BROAD_MARKET_ETF, interval, horizon_bars + 1),
        )
        rows = [row["bar_end"] for row in cursor.fetchall()]
    if len(rows) <= horizon_bars:
        raise ValueError("insufficient intraday bars for the requested horizon")
    cutoff = rows[-1]
    return cutoff if cutoff.tzinfo else cutoff.replace(tzinfo=timezone.utc)


def evaluate(
    events: tuple[IntradayScannerEvent, ...],
    *,
    interval: str,
    round_trip_cost_bps: float,
    available_by: datetime,
) -> dict:
    eligible = eligible_events(events)
    if not eligible:
        raise ValueError("event input has no qualification-eligible subjects")
    sources = _check_versions(eligible)
    effective_from = min(row.signal_time for row in eligible)
    subject_ids_by_source = {
        source_name: tuple(
            intraday_event_evidence_id(row)
            for row in eligible if row.source_name == source_name
        )
        for source_name in sources
    }
    policies = intraday_scanner_outcome_policies(
        interval=interval,
        effective_from=effective_from,
        round_trip_cost_bps=round_trip_cost_bps,
        source_names=sources,
    )
    horizon_bars = tuple(INTRADAY_OUTCOME_HORIZONS[interval].values())
    cutoffs = {
        horizon_key: maturity_cutoff(interval, int(horizon_count))
        for horizon_key, horizon_count in INTRADAY_OUTCOME_HORIZONS[interval].items()
    }
    service = EquityMaterializationService(
        None,
        outcome_path_cache_size=100000,
        outcome_path_prefetch_limit=max(horizon_bars),
    )
    policies_by_source: dict[str, list] = {}
    for policy in policies:
        policies_by_source.setdefault(policy.source_name, []).append(policy)

    totals: dict[tuple[str, str, str, str], dict] = {}
    for source_name in sources:
        subject_ids = subject_ids_by_source[source_name]
        # Subjects outermost so one bar path serves every policy and horizon.
        for offset in range(0, len(subject_ids), EVIDENCE_BATCH):
            batch = subject_ids[offset:offset + EVIDENCE_BATCH]
            for policy in policies_by_source[source_name]:
                for horizon_key in json.loads(policy.horizons_json):
                    run = service.evaluate_directional_outcomes(
                        policy,
                        horizon_key,
                        available_by=available_by,
                        finalize_unavailable=True,
                        signal_observed_through=cutoffs[horizon_key],
                        subject_evidence_ids=batch,
                        limit=len(batch),
                    )
                    if run.due > 0 and run.persisted == 0:
                        raise RuntimeError(
                            f"outcome evaluation made no progress for "
                            f"{policy.policy_key} {horizon_key}"
                        )
                    key = (
                        source_name, policy.policy_key,
                        policy.policy_version, horizon_key,
                    )
                    entry = totals.setdefault(key, {
                        "source_name": source_name,
                        "policy_key": policy.policy_key,
                        "policy_version": policy.policy_version,
                        "horizon_key": horizon_key,
                        "maturity_cutoff": cutoffs[horizon_key].isoformat(),
                        "batches": 0, "due": 0, "persisted": 0, "pending": 0,
                    })
                    entry["batches"] += 1
                    entry["due"] += run.due
                    entry["persisted"] += run.persisted
                    entry["pending"] += run.pending
            service.clear_outcome_path_cache()
    return {"policies": [totals[key] for key in sorted(totals)]}


def qualify(
    events: tuple[IntradayScannerEvent, ...],
    *,
    interval: str,
    available_by: datetime,
    effective_from: datetime,
    evaluation_version: str,
    minimum_events: int,
    minimum_independent_periods: int,
) -> dict:
    eligible = eligible_events(events)
    sources = _check_versions(eligible)
    repository = EquityOutcomeRepository()
    subject_evidence_ids = tuple(
        intraday_event_evidence_id(event) for event in eligible
    )
    policy_keys = intraday_policy_keys(sources, interval)
    observations = repository.qualification_observations(
        available_by=available_by,
        interval=interval,
        source_names=sources,
        subject_evidence_ids=subject_evidence_ids,
        outcome_policy_keys=policy_keys,
    )
    revisions = qualify_outcomes(
        pd.DataFrame(observations),
        effective_from=effective_from,
        evaluation_version=evaluation_version,
        minimum_events=minimum_events,
        minimum_independent_periods=minimum_independent_periods,
        research_scope="EQUITY_SIGNAL",
        publication_metadata={
            "detector_policy_version": INTRADAY_DETECTOR_POLICY[
                "detector_policy_version"
            ],
            "limitations": list(LIMITATIONS),
            "research_cohort": INTRADAY_RESEARCH_COHORT,
        },
    )
    inserted = repository.persist_qualification_revisions(revisions)
    return {
        "observations": len(observations),
        "outcome_policy_keys": list(policy_keys),
        "revisions": len(revisions),
        "inserted_revisions": inserted,
        "report_identity": revisions[0].report_identity if revisions else None,
        "states": {
            state: sum(row.qualification_state == state for row in revisions)
            for state in ("ROBUST_PASS", "MONITOR_ONLY", "UNRANKED")
        },
        "results": [
            {
                "source_name": row.source_name,
                "source_version": row.source_version,
                "direction": row.direction,
                "horizon_key": row.horizon_key,
                "outcome_policy_key": row.outcome_policy_key,
                "sample_size": row.sample_size,
                "independent_periods": row.independent_periods,
                "mean_net_alpha": row.mean_net_alpha,
                "alpha_t_stat": row.alpha_t_stat,
                "alpha_fdr_q": row.alpha_fdr_q,
                "qualification_state": row.qualification_state,
                "metrics": json.loads(row.metrics_json),
            }
            for row in revisions
        ],
    }


def main(argv: list[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    steps = (
        ("persist_evidence", arguments.all or arguments.persist_evidence),
        ("evaluate", arguments.all or arguments.evaluate),
        ("qualify", arguments.all or arguments.qualify),
    )
    if not any(enabled for _, enabled in steps):
        raise SystemExit(
            "select --persist-evidence, --evaluate, --qualify, or --all"
        )
    available_by = (
        datetime.fromisoformat(arguments.available_by)
        if arguments.available_by else datetime.now(timezone.utc)
    )
    if available_by.tzinfo is None:
        raise SystemExit("--available-by must be timezone-aware")
    effective_from = None
    if arguments.all or arguments.qualify:
        if not arguments.qualification_effective_from:
            raise SystemExit("--qualification-effective-from is required to qualify")
        effective_from = datetime.fromisoformat(arguments.qualification_effective_from)
        if effective_from.tzinfo is None:
            raise SystemExit("--qualification-effective-from must be timezone-aware")

    events = load_events(arguments.events)
    if not events:
        raise SystemExit("event file is empty")

    report: dict = {
        "available_by": available_by.isoformat(),
        "events_file": str(arguments.events),
        "events_loaded": len(events),
        "interval": arguments.interval,
        "limitations": list(LIMITATIONS),
    }
    with try_advisory_leadership(LOCK_NAME) as acquired:
        if not acquired:
            raise SystemExit("another intraday outcome run holds the advisory lock")
        for name, enabled in steps:
            if not enabled:
                continue
            if name == "persist_evidence":
                report["persist_evidence"] = persist_evidence(eligible_events(events))
            elif name == "evaluate":
                report["evaluate"] = evaluate(
                    events,
                    interval=arguments.interval,
                    round_trip_cost_bps=arguments.round_trip_cost_bps,
                    available_by=available_by,
                )
            else:
                report["qualify"] = qualify(
                    events,
                    interval=arguments.interval,
                    available_by=available_by,
                    effective_from=effective_from,
                    evaluation_version=arguments.evaluation_version,
                    minimum_events=arguments.minimum_events,
                    minimum_independent_periods=arguments.minimum_independent_periods,
                )
    rendered = json.dumps(report, indent=2, sort_keys=True, default=str)
    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
