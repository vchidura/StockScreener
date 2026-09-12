#!/usr/bin/env python3
"""Compare pre-registered Opportunity Board selector variants without publishing them."""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from options.analytics.board_selection import (  # noqa: E402
    BoardCandidateObservation,
    BoardSelectorVariant,
    BoardShadowMember,
    select_board_shadow,
)
from options.config import load_option_runtime_configuration  # noqa: E402
from options.repositories.board import BOARD_SELECTOR_SHA256  # noqa: E402


DEFAULT_OUTPUT = BACKEND_DIR.parent / "docs" / "option_board_selector_shadow.json"

SQL_CANDIDATES = """
SELECT candidate.candidate_id, candidate.matrix_id, candidate.underlying,
       candidate.strategy_name, candidate.candidate_kind,
       candidate.candidate_rank, candidate.market_data_time,
       candidate.observed_time,
       (candidate.market_data_time AT TIME ZONE 'America/New_York')::date
           AS session_date,
       run.scheduled_cycle,
       ARRAY(
           SELECT leg.contract_id
           FROM option_candidate_legs AS leg
           WHERE leg.candidate_id = candidate.candidate_id
           UNION
           SELECT (candidate.rank_components->>'contract_id')::BIGINT
           WHERE candidate.rank_components->>'contract_id' IS NOT NULL
           ORDER BY 1
       ) AS contract_ids
FROM option_strategy_candidates AS candidate
JOIN option_analysis_runs AS analysis USING (matrix_id)
JOIN option_ingestion_runs AS run USING (batch_id)
WHERE candidate.policy_sha256 = %s
  AND candidate.status = 'SELECTED'
  AND candidate.market_data_time <= %s
  AND candidate.observed_time <= %s
ORDER BY candidate.underlying, candidate.market_data_time,
         candidate.observed_time, candidate.matrix_id,
         candidate.strategy_name, candidate.candidate_kind,
         candidate.candidate_rank, candidate.candidate_id
"""

SQL_OUTCOMES = """
SELECT outcome.candidate_id, outcome.measurement_type, outcome.net_return,
       (outcome.market_time AT TIME ZONE 'America/New_York')::date AS session_date
FROM option_signal_decay_outcomes AS outcome
WHERE outcome.market_time >= %s
  AND outcome.valuation_policy_sha256 = %s
  AND outcome.net_return IS NOT NULL
"""

SQL_LATEST_PUBLICATION = """
SELECT publication_id, scheduled_cycle, selection_evidence
FROM option_board_publications
WHERE status = 'COMPLETE'
  AND strategy_policy_sha256 = %s
  AND selector_sha256 = %s
ORDER BY scheduled_cycle DESC, published_at DESC
LIMIT 1
"""

SQL_PUBLICATION_MEMBERS = """
SELECT candidate_id, board_position
FROM option_board_members
WHERE publication_id = %s
"""


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--days", type=int, default=30)
    result.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    result.add_argument("--no-write", action="store_true")
    return result


def percentile(values: list[int], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def member_summary(
    members: tuple[BoardShadowMember, ...],
    outcomes: dict[UUID, list[dict[str, Any]]],
) -> dict[str, Any]:
    position_one = tuple(member for member in members if member.board_position == 1)
    structured = tuple(
        member for member in position_one if member.candidate_kind != "RESEARCH_ONLY"
    )
    package_counts = Counter(
        (
            member.underlying,
            member.strategy_name,
            member.candidate_kind,
            member.contract_ids,
        )
        for member in members
    )
    repeated_occurrences = sum(count - 1 for count in package_counts.values())
    ranks = [member.raw_candidate_rank for member in position_one]
    by_strategy = Counter(member.strategy_name for member in position_one)
    by_kind = Counter(member.candidate_kind for member in position_one)
    outcome_rows = [
        row
        for member in structured
        for row in outcomes.get(member.candidate_id, ())
    ]
    outcome_by_horizon: dict[str, dict[str, Any]] = {}
    for horizon in sorted({row["measurement_type"] for row in outcome_rows}):
        horizon_rows = [row for row in outcome_rows if row["measurement_type"] == horizon]
        returns = [float(row["net_return"]) for row in horizon_rows]
        outcome_by_horizon[horizon] = {
            "outcomes": len(returns),
            "independent_sessions": len({row["session_date"] for row in horizon_rows}),
            "mean_net_return": statistics.fmean(returns) if returns else None,
            "positive_fraction": (
                sum(value > 0 for value in returns) / len(returns) if returns else None
            ),
            "inference": "DESCRIPTIVE_ONLY_NOT_BASELINE_ADJUSTED",
        }
    return {
        "member_occurrences": len(members),
        "position_one_members": len(position_one),
        "structured_position_one": len(structured),
        "research_position_one": len(position_one) - len(structured),
        "distinct_sessions": len({member.session_date for member in members}),
        "underlying_coverage": len({member.underlying for member in position_one}),
        "strategy_coverage": len({member.strategy_name for member in position_one}),
        "by_strategy": dict(sorted(by_strategy.items())),
        "by_candidate_kind": dict(sorted(by_kind.items())),
        "raw_rank": {
            "median": statistics.median(ranks) if ranks else None,
            "p90": percentile(ranks, 0.9),
            "maximum": max(ranks) if ranks else None,
            "above_one": sum(value > 1 for value in ranks),
        },
        "logical_packages": len(package_counts),
        "recurring_packages": sum(count > 1 for count in package_counts.values()),
        "repeated_occurrences": repeated_occurrences,
        "repeat_fraction": repeated_occurrences / len(members) if members else 0.0,
        "measured_structured_candidates": len(
            {member.candidate_id for member in structured if member.candidate_id in outcomes}
        ),
        "outcomes_by_horizon": outcome_by_horizon,
    }


def main() -> int:
    args = parser().parse_args()
    if args.days < 1:
        raise ValueError("days must be positive")
    configuration = load_option_runtime_configuration()
    now = datetime.now(timezone.utc)
    evaluation_start = now - timedelta(days=args.days)
    outcome_policy = configuration.valuation_policy
    with get_db_cursor() as cursor:
        cursor.execute("SET LOCAL statement_timeout = '120s'")
        cursor.execute(
            SQL_CANDIDATES,
            (configuration.strategy_policy_sha256, now, now),
        )
        raw_candidates = [dict(row) for row in cursor.fetchall()]
        cursor.execute(
            SQL_OUTCOMES,
            (evaluation_start, outcome_policy.policy_sha256),
        )
        outcome_rows = [dict(row) for row in cursor.fetchall()]
        cursor.execute(
            SQL_LATEST_PUBLICATION,
            (configuration.strategy_policy_sha256, BOARD_SELECTOR_SHA256),
        )
        publication = cursor.fetchone()
        publication_members: dict[UUID, int] = {}
        if publication:
            cursor.execute(SQL_PUBLICATION_MEMBERS, (publication["publication_id"],))
            publication_members = {
                row["candidate_id"]: int(row["board_position"])
                for row in cursor.fetchall()
            }

    observations = tuple(
        BoardCandidateObservation(
            candidate_id=row["candidate_id"],
            matrix_id=row["matrix_id"],
            underlying=row["underlying"],
            strategy_name=row["strategy_name"],
            candidate_kind=row["candidate_kind"],
            candidate_rank=int(row["candidate_rank"]),
            market_data_time=row["market_data_time"],
            observed_time=row["observed_time"],
            session_date=row["session_date"],
            contract_ids=tuple(int(value) for value in row["contract_ids"]),
        )
        for row in raw_candidates
        if row["contract_ids"]
    )
    cycle_by_candidate = {
        row["candidate_id"]: row["scheduled_cycle"] for row in raw_candidates
    }
    outcomes: dict[UUID, list[dict[str, Any]]] = defaultdict(list)
    for row in outcome_rows:
        outcomes[row["candidate_id"]].append(row)

    variants: dict[str, dict[str, Any]] = {}
    selected_by_variant: dict[BoardSelectorVariant, tuple[BoardShadowMember, ...]] = {}
    for variant in BoardSelectorVariant:
        result = select_board_shadow(
            observations,
            variant,
            maximum_position=3,
            evaluation_start=evaluation_start,
        )
        selected_by_variant[variant] = result.members
        variants[variant.value] = {
            "input_candidates": result.input_candidates,
            "excluded_candidates": result.excluded_candidates,
            "exclusion_fraction": (
                result.excluded_candidates / result.input_candidates
                if result.input_candidates
                else 0.0
            ),
            "surviving_candidates": result.surviving_candidates,
            **member_summary(result.members, outcomes),
        }

    publication_comparison = None
    if publication:
        cycle = publication["scheduled_cycle"]
        lifetime_members = {
            member.candidate_id: member.board_position
            for member in selected_by_variant[BoardSelectorVariant.LIFETIME_GLOBAL]
            if cycle_by_candidate.get(member.candidate_id) == cycle
        }
        exact_ids = set(publication_members)
        shadow_ids = set(lifetime_members)
        publication_comparison = {
            "publication_id": str(publication["publication_id"]),
            "scheduled_cycle": cycle,
            "persisted_members": len(exact_ids),
            "shadow_members": len(shadow_ids),
            "intersection": len(exact_ids & shadow_ids),
            "persisted_only": len(exact_ids - shadow_ids),
            "shadow_only": len(shadow_ids - exact_ids),
            "position_mismatches": sum(
                publication_members[candidate_id] != lifetime_members[candidate_id]
                for candidate_id in exact_ids & shadow_ids
            ),
        }

    payload = {
        "generated_at": now,
        "evaluation_start": evaluation_start,
        "days": args.days,
        "strategy_policy_sha256": configuration.strategy_policy_sha256,
        "valuation_policy_sha256": outcome_policy.policy_sha256,
        "candidate_observations_loaded": len(observations),
        "candidate_sessions": len({row.session_date for row in observations}),
        "variants": variants,
        "latest_publication_comparison": publication_comparison,
        "interpretation": {
            "selection_metrics": "Comparable across variants under identical candidate inputs.",
            "outcomes": (
                "Descriptive delayed-mark outcomes only. No selector may graduate without "
                "same-structure baseline adjustment and at least 40 independent periods."
            ),
            "recommended_action": "KEEP_SELECTOR_V1_AND_ACCUMULATE_PUBLICATIONS",
        },
    }
    encoded = json.dumps(payload, default=str, indent=2, allow_nan=False)
    print(encoded)
    if not args.no_write:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
