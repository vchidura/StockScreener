#!/usr/bin/env python3
"""Persist reconstructed signals, evaluate matured outcomes, and qualify them."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import exchange_calendars
import pandas as pd


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from database import get_db_cursor
from equity.historical_research import (
    GAP_PRIMARY_SOURCES,
    composite_scanner_outcome_policies,
    historical_event_evidence,
    historical_event_evidence_id,
    primary_gap_outcome_policies,
)
from equity.leadership import try_advisory_leadership
from equity.orchestration import EquityMaterializationService
from equity.outcomes import recommendation_plan_policy
from equity.qualification import qualify_outcomes
from equity.repositories import (
    EquityEvidenceRepository,
    EquityOutcomeRepository,
    EquityUniverseRepository,
)
from research.composite_scanners import (
    COMPOSITE_OUTCOME_HORIZONS,
    COMPOSITE_SCANNER_REGISTRY,
)
from research.historical_signal_replay import HistoricalSignalEvent


LOCK_NAME = "stock-screener:historical-signal-outcomes"


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--events", type=Path)
    result.add_argument("--persist-evidence", action="store_true")
    result.add_argument("--evaluate", action="store_true")
    result.add_argument("--qualify", action="store_true")
    result.add_argument("--all", action="store_true")
    result.add_argument("--status", action="store_true")
    result.add_argument(
        "--horizon-sessions", type=int, nargs="+", default=(5, 10, 21)
    )
    result.add_argument("--round-trip-cost-bps", type=float, default=4.0)
    result.add_argument("--available-by", help="UTC ISO timestamp; defaults to now")
    result.add_argument(
        "--qualification-effective-from",
        help="Required for --qualify/--all; reviewed UTC ISO publication time",
    )
    result.add_argument(
        "--evaluation-version", default="gap_formation_daily_qualification_v1"
    )
    result.add_argument("--output", type=Path)
    result.add_argument("--source-version", default="gap_formation_v1")
    result.add_argument(
        "--adjusted", action="store_true",
        help="Read outcome paths from the split-adjusted bar lineage. Required "
             "for daily studies; unadjusted paths carry split gaps as returns.",
    )
    return result


def load_events(path: Path) -> tuple[HistoricalSignalEvent, ...]:
    events = []
    # Streamed rather than read_text().splitlines(): study files reach hundreds of
    # megabytes and that materialises the whole file plus a list of every line.
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                events.append(HistoricalSignalEvent.from_dict(json.loads(line)))
            except Exception as exc:
                raise ValueError(
                    f"invalid historical event at line {line_number}"
                ) from exc
    events.sort(key=lambda row: (row.signal_time, row.ticker, row.event_id.hex))
    return tuple(events)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def persist_evidence(events: tuple[HistoricalSignalEvent, ...]) -> dict[str, int]:
    universe_repository = EquityUniverseRepository()
    securities = {}
    tickers_by_run = {}
    for event in events:
        tickers_by_run.setdefault(event.universe_run_id, set()).add(event.ticker)
    for universe_run_id in sorted(tickers_by_run, key=str):
        for security in universe_repository.members_for_replay(
            universe_run_id,
            sorted(tickers_by_run[universe_run_id]),
        ):
            securities[(universe_run_id, security.ticker)] = security
    missing = [
        row for row in events
        if (row.universe_run_id, row.ticker) not in securities
    ]
    if missing:
        raise RuntimeError(
            f"{len(missing)} historical events lack linked security revisions"
        )
    evidence = tuple(
        historical_event_evidence(
            event, securities[(event.universe_run_id, event.ticker)]
        )
        for event in events
    )
    inserted = EquityEvidenceRepository().persist(evidence)
    return {"events": len(events), "evidence_inserted": inserted}


def maturity_cutoff(horizon_sessions: int) -> datetime:
    if horizon_sessions <= 0:
        raise ValueError("--horizon-sessions must be positive")
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT session_date
            FROM equity_bar_revisions
            WHERE availability_mode = 'HISTORICAL_RECONSTRUCTED'
              AND interval = '1d'
              AND quality_codes @>
                  ARRAY['GROUPED_DAILY_EXACT_TICKER_V2']::TEXT[]
            ORDER BY session_date
            """
        )
        sessions = [row["session_date"] for row in cursor.fetchall()]
    if len(sessions) <= horizon_sessions:
        raise ValueError("insufficient daily sessions for requested outcome horizon")
    cutoff_date = sessions[-(horizon_sessions + 1)]
    calendar = exchange_calendars.get_calendar("XNYS")
    return calendar.session_close(pd.Timestamp(cutoff_date)).to_pydatetime()


def evaluate(
    events: tuple[HistoricalSignalEvent, ...],
    *,
    horizon_sessions: tuple[int, ...] | list[int],
    round_trip_cost_bps: float,
    available_by: datetime,
    adjusted: bool = False,
) -> dict:
    eligible = tuple(row for row in events if row.payload.get("qualification_eligible"))
    if not eligible:
        raise ValueError("event input has no qualification-eligible subjects")
    effective_from = min(row.signal_time for row in eligible)
    source_names = tuple(sorted({row.source_name for row in eligible}))
    versions_by_source = _versions_by_source(eligible)
    subject_ids_by_source = {
        source_name: tuple(
            historical_event_evidence_id(row)
            for row in eligible if row.source_name == source_name
        )
        for source_name in source_names
    }
    composite_sources = set(source_names) & set(COMPOSITE_SCANNER_REGISTRY)
    if composite_sources:
        if composite_sources != set(source_names):
            raise ValueError("composite event files cannot mix source families")
        for source_name in source_names:
            expected = COMPOSITE_SCANNER_REGISTRY[source_name].source_version
            if versions_by_source[source_name] != expected:
                raise ValueError(
                    f"composite source version mismatch: {source_name}"
                )
        expected_horizons = tuple(COMPOSITE_OUTCOME_HORIZONS["1d"].values())
        if tuple(horizon_sessions) != expected_horizons:
            raise ValueError(
                f"daily composite horizons must be {expected_horizons}"
            )
        policies = tuple(
            policy for policy in composite_scanner_outcome_policies(
                interval="1d",
                effective_from=effective_from,
                round_trip_cost_bps=round_trip_cost_bps,
            )
            if policy.source_name in composite_sources
        )
    else:
        source_version = _single(set(versions_by_source.values()), "source version")
        policies = primary_gap_outcome_policies(
            source_version=source_version,
            effective_from=effective_from,
            horizon_sessions=horizon_sessions,
            round_trip_cost_bps=round_trip_cost_bps,
            source_names=source_names,
        )
        named_horizons = {
            f"{int(value)}d": int(value) for value in horizon_sessions
        }
        policies = (*policies, *(
            recommendation_plan_policy(
                source_name=source_name,
                source_version=source_version,
                interval="1d",
                horizons=named_horizons,
                effective_from=effective_from,
                round_trip_cost_bps=round_trip_cost_bps,
                sector_benchmark=True,
                primary_benchmark="SECTOR",
                policy_version="recommendation_plan_sector_v1",
            )
            for source_name in source_names
            if source_name.startswith("GAP_")
        ))
    service = EquityMaterializationService(
        None,
        outcome_path_cache_size=100000,
        outcome_path_prefetch_limit=max(int(value) for value in horizon_sessions),
    )
    results = []
    for policy in policies:
        for horizon_key, horizon_count in json.loads(policy.horizons_json).items():
            cutoff = maturity_cutoff(int(horizon_count))
            due = persisted = pending = batches = 0
            source_subject_ids = subject_ids_by_source[policy.source_name]
            for offset in range(0, len(source_subject_ids), 5000):
                subject_batch = source_subject_ids[offset:offset + 5000]
                run = service.evaluate_directional_outcomes(
                    policy,
                    horizon_key,
                    available_by=available_by,
                    finalize_unavailable=True,
                    historical_reconstructed_only=True,
                    adjusted=adjusted,
                    signal_observed_through=cutoff,
                    subject_evidence_ids=subject_batch,
                    limit=len(subject_batch),
                )
                batches += 1
                due += run.due
                persisted += run.persisted
                pending += run.pending
                if run.due > 0 and run.persisted == 0:
                    raise RuntimeError(
                        f"outcome evaluation made no progress for "
                        f"{policy.policy_key} {horizon_key}"
                    )
            results.append({
                "source_name": policy.source_name,
                "policy_key": run.policy_key,
                "policy_version": policy.policy_version,
                "horizon_key": run.horizon_key,
                "maturity_cutoff": cutoff.isoformat(),
                "batches": batches,
                "due": due,
                "persisted": persisted,
                "pending": pending,
            })
    return {
        "policies": results,
    }


def qualify(
    events: tuple[HistoricalSignalEvent, ...],
    *,
    available_by: datetime,
    effective_from: datetime,
    evaluation_version: str,
    publication_metadata: dict | None = None,
) -> dict:
    repository = EquityOutcomeRepository()
    subject_evidence_ids = tuple(
        historical_event_evidence_id(event)
        for event in events
        if event.payload.get("qualification_eligible")
    )
    source_names = tuple(sorted({
        event.source_name
        for event in events
        if event.payload.get("qualification_eligible")
    }))
    eligible = tuple(
        event for event in events if event.payload.get("qualification_eligible")
    )
    versions_by_source = _versions_by_source(eligible)
    if set(source_names) & set(COMPOSITE_SCANNER_REGISTRY) \
            and evaluation_version.startswith("gap_"):
        raise ValueError(
            "composite qualification requires an explicit --evaluation-version; "
            f"{evaluation_version!r} is the gap-formation default and is stored "
            "verbatim in the retained qualification revision"
        )
    plan_sources = {
        event.source_name for event in eligible
        if event.source_name.startswith("GAP_")
        or "RECOMMENDATION_PLAN" in event.payload.get("outcome_modes", ())
    }
    policy_keys = tuple(sorted({
        f"{source_name}:{versions_by_source[source_name]}:1d:SIGNED:SECTOR_PRIMARY"
        for source_name in source_names
    } | {
        f"{source_name}:{versions_by_source[source_name]}:1d:RECOMMENDATION_PLAN:SECTOR_PRIMARY"
        for source_name in source_names
        if source_name in plan_sources
    }))
    observations = repository.qualification_observations(
        available_by=available_by,
        interval="1d",
        source_names=source_names,
        subject_evidence_ids=subject_evidence_ids,
        outcome_policy_keys=policy_keys,
    )
    revisions = qualify_outcomes(
        pd.DataFrame(observations),
        effective_from=effective_from,
        evaluation_version=evaluation_version,
        publication_metadata=publication_metadata,
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
                "direction": row.direction,
                "horizon_key": row.horizon_key,
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


def status(evaluation_version: str, source_version: str) -> dict:
    with get_db_cursor() as cursor:
        cursor.execute(
                        """
            SELECT source_name, COUNT(*) AS evidence
            FROM equity_evidence
            WHERE source_name = ANY(%s)
                            AND source_version = %s
              AND quality_codes @> ARRAY['HISTORICAL_RECONSTRUCTED']::TEXT[]
            GROUP BY source_name ORDER BY source_name
                        """,
                        ([*GAP_PRIMARY_SOURCES, "GAP_FORMATION_CONTROL"], source_version),
        )
        evidence = [dict(row) for row in cursor.fetchall()]
        cursor.execute(
             """
             SELECT DISTINCT ON (source_name)
                 source_name, policy_key, policy_version, interval,
                   horizons, cost_model, benchmark_policy
            FROM equity_outcome_policies
            WHERE source_name = ANY(%s)
            AND source_version = %s
             ORDER BY source_name, created_at DESC
            """,
             (list(GAP_PRIMARY_SOURCES), source_version),
        )
        policies = [dict(row) for row in cursor.fetchall()]
        cursor.execute(
                        """
                        WITH selected_policies AS (
                                SELECT DISTINCT ON (source_name)
                                             outcome_policy_id, source_name
                                FROM equity_outcome_policies
                                WHERE source_name = ANY(%s)
                                    AND source_version = %s
                                ORDER BY source_name, created_at DESC
                        )
            SELECT evidence.source_name, outcome.horizon_key,
                   outcome.entry_status, COUNT(*) AS outcomes
            FROM equity_research_outcomes outcome
            JOIN equity_evidence evidence
              ON evidence.evidence_id = outcome.subject_evidence_id
                        JOIN selected_policies policy
                            ON policy.outcome_policy_id = outcome.outcome_policy_id
            WHERE evidence.source_name = ANY(%s)
                            AND evidence.source_version = %s
                        GROUP BY evidence.source_name, outcome.horizon_key,
                     outcome.entry_status
            ORDER BY evidence.source_name, outcome.horizon_key,
                     outcome.entry_status
            """,
                        (
                                list(GAP_PRIMARY_SOURCES), source_version,
                                list(GAP_PRIMARY_SOURCES), source_version,
                        ),
        )
        outcomes = [dict(row) for row in cursor.fetchall()]
        cursor.execute(
                        """
                        WITH selected_policies AS (
                                SELECT DISTINCT ON (source_name)
                                             outcome_policy_id, source_name
                                FROM equity_outcome_policies
                                WHERE source_name = ANY(%s)
                                    AND source_version = %s
                                ORDER BY source_name, created_at DESC
                        )
                        SELECT evidence.source_name, evidence.direction, outcome.horizon_key,
                   COUNT(*) FILTER (
                       WHERE outcome.entry_status = 'ENTERED'
                   ) AS entered,
                   COUNT(*) FILTER (
                       WHERE outcome.entry_status = 'UNAVAILABLE'
                   ) AS unavailable,
                   AVG(outcome.net_return) FILTER (
                       WHERE outcome.entry_status = 'ENTERED'
                   ) AS mean_net_return,
                   AVG(outcome.net_alpha) FILTER (
                       WHERE outcome.entry_status = 'ENTERED'
                   ) AS mean_net_alpha,
                   AVG(outcome.mae_pct) FILTER (
                       WHERE outcome.entry_status = 'ENTERED'
                   ) AS mean_mae_pct,
                   AVG(outcome.mfe_pct) FILTER (
                       WHERE outcome.entry_status = 'ENTERED'
                   ) AS mean_mfe_pct,
                   AVG(outcome.mae_r) FILTER (
                       WHERE outcome.entry_status = 'ENTERED'
                   ) AS mean_mae_r,
                   AVG(outcome.mfe_r) FILTER (
                       WHERE outcome.entry_status = 'ENTERED'
                   ) AS mean_mfe_r,
                   (AVG((outcome.net_return > 0)::INTEGER) FILTER (
                       WHERE outcome.entry_status = 'ENTERED'
                   ))::DOUBLE PRECISION AS net_win_rate,
                   (AVG(outcome.stop_hit::INTEGER) FILTER (
                       WHERE outcome.entry_status = 'ENTERED'
                   ))::DOUBLE PRECISION AS stop_hit_rate,
                   (AVG(outcome.target_hit::INTEGER) FILTER (
                       WHERE outcome.entry_status = 'ENTERED'
                   ))::DOUBLE PRECISION AS target_hit_rate
            FROM equity_research_outcomes outcome
            JOIN equity_evidence evidence
              ON evidence.evidence_id = outcome.subject_evidence_id
                        JOIN selected_policies policy
                            ON policy.outcome_policy_id = outcome.outcome_policy_id
            WHERE evidence.source_name = ANY(%s)
                            AND evidence.source_version = %s
              AND outcome.outcome_revision = 1
                        GROUP BY evidence.source_name, evidence.direction, outcome.horizon_key
                        ORDER BY evidence.source_name, evidence.direction, outcome.horizon_key
            """,
                        (
                                list(GAP_PRIMARY_SOURCES), source_version,
                                list(GAP_PRIMARY_SOURCES), source_version,
                        ),
        )
        outcome_metrics = [dict(row) for row in cursor.fetchall()]
        cursor.execute(
                        """
            SELECT COUNT(*) AS control_outcomes
            FROM equity_research_outcomes outcome
            JOIN equity_evidence evidence
              ON evidence.evidence_id = outcome.subject_evidence_id
            WHERE evidence.source_name = 'GAP_FORMATION_CONTROL'
                            AND evidence.source_version = %s
                        """,
                        (source_version,),
        )
        control_outcomes = int(cursor.fetchone()["control_outcomes"] or 0)
        cursor.execute(
                        """
                        SELECT DISTINCT ON (source_name, direction, horizon_key)
                                     source_name, direction, horizon_key, qualification_state,
                                     sample_size, independent_periods, mean_net_alpha,
                                     alpha_t_stat, alpha_fdr_q,
                                     (metrics->>'early_alpha')::DOUBLE PRECISION AS early_alpha,
                                     (metrics->>'late_alpha')::DOUBLE PRECISION AS late_alpha,
                                     effective_from
            FROM equity_qualification_revisions
            WHERE source_name = ANY(%s)
                            AND source_version = %s
              AND evaluation_version = %s
                        ORDER BY source_name, direction, horizon_key, effective_from DESC,
                                         created_at DESC
            """,
                        (list(GAP_PRIMARY_SOURCES), source_version, evaluation_version),
        )
        qualifications = [dict(row) for row in cursor.fetchall()]
    failures = {
        "missing_primary_policies": max(3 - len(policies), 0),
        "control_outcomes": control_outcomes,
    }
    return {
        "status": "PASS" if not any(failures.values()) else "FAIL",
        "failures": failures,
        "evidence": evidence,
        "policies": policies,
        "outcomes": outcomes,
        "outcome_metrics": outcome_metrics,
        "qualifications": qualifications,
    }


def _utc(value: str | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _single(values: set[str], label: str) -> str:
    if len(values) != 1:
        raise ValueError(f"historical events contain multiple {label}s")
    return next(iter(values))


def _versions_by_source(
    events: tuple[HistoricalSignalEvent, ...],
) -> dict[str, str]:
    versions = {}
    for event in events:
        existing = versions.setdefault(event.source_name, event.source_version)
        if existing != event.source_version:
            raise ValueError(
                f"historical source contains multiple versions: {event.source_name}"
            )
    return versions


def publication_metadata(
    events: tuple[HistoricalSignalEvent, ...],
    *,
    events_path: Path,
    available_by: datetime,
    horizon_sessions: tuple[int, ...] | list[int],
    round_trip_cost_bps: float,
) -> dict:
    eligible = tuple(
        event for event in events if event.payload.get("qualification_eligible")
    )
    return {
        "available_by": available_by.isoformat(),
        "event_count": len(events),
        "event_file_sha256": _file_sha256(events_path),
        "eligible_event_count": len(eligible),
        "horizon_sessions": list(horizon_sessions),
        "maximum_signal_time": max(
            (event.signal_time for event in eligible), default=None
        ).isoformat() if eligible else None,
        "minimum_signal_time": min(
            (event.signal_time for event in eligible), default=None
        ).isoformat() if eligible else None,
        "round_trip_cost_bps": round_trip_cost_bps,
        "source_names": sorted({event.source_name for event in eligible}),
        "source_versions": sorted({event.source_version for event in eligible}),
        "universe_policy_versions": sorted({
            event.universe_policy_version for event in eligible
        }),
        "universe_run_ids_sha256": hashlib.sha256(
            "\n".join(sorted({
                str(event.universe_run_id) for event in eligible
            })).encode("ascii")
        ).hexdigest(),
    }


def main() -> int:
    args = parser().parse_args()
    if args.status:
        print(json.dumps(status(args.evaluation_version, args.source_version), default=str, indent=2))
        return 0
    operations = {
        "persist_evidence": args.persist_evidence or args.all,
        "evaluate": args.evaluate or args.all,
        "qualify": args.qualify or args.all,
    }
    if not any(operations.values()):
        parser().error("choose --persist-evidence, --evaluate, --qualify, or --all")
    if args.events is None:
        parser().error("--events is required for mutating operations")
    if operations["qualify"] and not args.qualification_effective_from:
        parser().error("--qualification-effective-from is required for qualification")
    events = load_events(args.events)
    available_by = _utc(args.available_by)
    report = {"events": len(events)}
    with try_advisory_leadership(LOCK_NAME) as is_leader:
        if not is_leader:
            raise RuntimeError("another historical outcome job owns leadership")
        if operations["persist_evidence"]:
            report["evidence"] = persist_evidence(events)
        if operations["evaluate"]:
            report["outcomes"] = evaluate(
                events,
                horizon_sessions=args.horizon_sessions,
                round_trip_cost_bps=args.round_trip_cost_bps,
                available_by=available_by,
                adjusted=args.adjusted,
            )
        if operations["qualify"]:
            report["qualification"] = qualify(
                events,
                available_by=available_by,
                effective_from=_utc(args.qualification_effective_from),
                evaluation_version=args.evaluation_version,
                publication_metadata=publication_metadata(
                    events,
                    events_path=args.events,
                    available_by=available_by,
                    horizon_sessions=args.horizon_sessions,
                    round_trip_cost_bps=args.round_trip_cost_bps,
                ),
            )
    serialized = json.dumps(report, default=str, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialized + "\n", encoding="utf-8")
    print(serialized)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())