#!/usr/bin/env python3
"""Explain why a retained detector run produced no selected option alerts."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from uuid import UUID

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from options.calendar import OptionExchangeCalendar  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--run-id", type=UUID, required=True)
    args = parser.parse_args()
    if len(args.dataset_id) > 80:
        raise ValueError("dataset ID exceeds bound")
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='30s'")
        cursor.execute("""
            SELECT publication_id,scheduled_cycle,published_at,market_data_time,source_matrix_ids,
                   selection_evidence,covered_underlying_count,expected_underlying_count
            FROM option_board_publications
            WHERE publication_id=%s AND status='COMPLETE'
              AND selection_evidence->>'dataset_id'=%s
        """, (str(args.run_id), args.dataset_id))
        run = cursor.fetchone()
        if not run:
            raise ValueError("completed detector run is unavailable")
        matrix_ids = run["source_matrix_ids"]
        selected_at = run["published_at"]
        calendar = OptionExchangeCalendar()
        volume_session = calendar.session_for_slot(run["scheduled_cycle"])
        settlement_session = calendar.previous_session(volume_session)
        session_close = calendar.session_close(volume_session)
        cursor.execute("""
            SELECT strategy_name,structure_type,status,candidate_kind,
                   COUNT(*) AS candidates,
                   COUNT(*) FILTER (WHERE valid_until>%s) AS valid_at_selection
            FROM option_strategy_candidates
            WHERE matrix_id=ANY(%s::uuid[])
            GROUP BY strategy_name,structure_type,status,candidate_kind
            ORDER BY candidates DESC,strategy_name,structure_type
        """, (selected_at, matrix_ids))
        strategy_groups = [dict(row) for row in cursor.fetchall()]
        cursor.execute("""
            SELECT COUNT(*) AS retained_snapshots,
                   COUNT(*) FILTER (WHERE snapshot.day_volume IS NULL) AS missing_day_volume,
                   COUNT(*) FILTER (WHERE snapshot.open_interest IS NULL) AS missing_open_interest,
                   COUNT(*) FILTER (WHERE snapshot.open_interest=0) AS zero_open_interest,
                   COUNT(*) FILTER (WHERE snapshot.day_volume IS NOT NULL AND snapshot.open_interest>0) AS ratio_available,
                   COUNT(*) FILTER (WHERE snapshot.day_volume::numeric/NULLIF(snapshot.open_interest,0)>=3) AS ratio_at_least_3,
                   COUNT(*) FILTER (WHERE snapshot.day_volume::numeric/NULLIF(snapshot.open_interest,0)<3) AS ratio_below_3,
                   COUNT(*) FILTER (WHERE snapshot.day_volume::numeric/NULLIF(snapshot.open_interest,0)>=3
                       AND daily.open_interest=snapshot.open_interest
                       AND daily.open_interest_source='PROVIDER_CHAIN_SNAPSHOT'
                       AND daily.open_interest_observed_session=%s
                       AND daily.open_interest_observed_at<=snapshot.first_observed_at
                       AND daily.open_interest_revised_observed_at IS NULL) AS dated_ratio_at_least_3,
                   COUNT(*) FILTER (WHERE snapshot.day_volume::numeric/NULLIF(snapshot.open_interest,0)>=3
                       AND daily.open_interest=snapshot.open_interest
                       AND daily.open_interest_source='PROVIDER_CHAIN_SNAPSHOT'
                       AND daily.open_interest_observed_session=%s
                       AND daily.open_interest_observed_at<=snapshot.first_observed_at
                       AND daily.open_interest_revised_observed_at IS NULL
                       AND snapshot.market_data_time+INTERVAL '30 minutes'>%s
                       AND snapshot.expiration_cutoff>%s
                       AND %s>%s) AS fresh_dated_ratio_at_least_3
            FROM option_chain_snapshots AS snapshot
            JOIN option_analysis_runs AS analysis USING(batch_id)
            LEFT JOIN option_daily_contract_facts AS daily
              ON daily.contract_id=snapshot.contract_id AND daily.underlying=snapshot.underlying
             AND daily.settlement_session=%s
            WHERE analysis.matrix_id=ANY(%s::uuid[])
        """, (volume_session, volume_session, selected_at, selected_at,
              session_close, selected_at, settlement_session, matrix_ids))
        activity_funnel = dict(cursor.fetchone())
        cursor.execute("""
            WITH directional AS (
                SELECT candidate.*
                FROM option_strategy_candidates AS candidate
                WHERE candidate.matrix_id=ANY(%s::uuid[])
                  AND candidate.status='SELECTED'
                  AND candidate.candidate_kind IN ('SINGLE_CONTRACT','MULTI_LEG')
                  AND candidate.strategy_name IN ('DIRECTIONAL_LONG_PREMIUM','DIRECTIONAL_DEBIT_SPREAD')
            ), bought AS (
                SELECT candidate.candidate_id,candidate.underlying,candidate.strategy_name,
                       candidate.structure_type,candidate.valid_until,
                       leg.snapshot_id,leg.contract_id
                FROM directional AS candidate
                JOIN option_candidate_legs AS leg USING(candidate_id)
                WHERE leg.side='BUY'
            ), observations AS (
                SELECT (payload_text::jsonb->'observation'->>'snapshot_id')::uuid AS snapshot_id,
                       payload_text::jsonb->'observation'->>'baseline_disposition' AS baseline_disposition,
                       payload_text::jsonb->'observation'->>'underlyer' AS underlying
                FROM option_o1_indicator_observations
                WHERE dataset_id=%s AND run_id=%s
            ), packages AS (
                SELECT DISTINCT ON(candidate_id) candidate_id,assessment_status
                FROM option_package_assessments
                WHERE matrix_id=ANY(%s::uuid[]) AND assessed_at<=%s AND recorded_at<=%s
                ORDER BY candidate_id,assessed_at DESC,recorded_at DESC
            )
            SELECT COUNT(DISTINCT bought.candidate_id) AS directional_candidates,
                   COUNT(DISTINCT bought.candidate_id) FILTER (WHERE bought.valid_until>%s) AS valid_candidates,
                   COUNT(DISTINCT bought.candidate_id) FILTER (WHERE package.assessment_status='READY') AS ready_package_candidates,
                   COUNT(DISTINCT bought.candidate_id) FILTER (WHERE observation.snapshot_id IS NOT NULL) AS activity_matched_candidates,
                   COUNT(DISTINCT bought.candidate_id) FILTER (WHERE observation.baseline_disposition='CONFIRMED') AS confirmed_activity_candidates,
                   COUNT(DISTINCT bought.candidate_id) FILTER (WHERE bought.valid_until>%s AND observation.snapshot_id IS NOT NULL) AS valid_activity_matched_candidates,
                   COUNT(DISTINCT bought.candidate_id) FILTER (WHERE bought.valid_until>%s AND observation.baseline_disposition='CONFIRMED') AS valid_confirmed_activity_candidates,
                   COUNT(DISTINCT bought.underlying) AS directional_underlyings,
                   COUNT(DISTINCT observation.underlying) FILTER (WHERE observation.baseline_disposition='CONFIRMED') AS confirmed_underlyings
            FROM bought
            LEFT JOIN observations AS observation USING(snapshot_id)
            LEFT JOIN packages AS package USING(candidate_id)
          """, (matrix_ids, args.dataset_id, str(args.run_id), matrix_ids, selected_at,
              selected_at, selected_at, selected_at, selected_at))
        package_funnel = dict(cursor.fetchone())
        cursor.execute("""
            SELECT detector_id,selection_status,selection_reason,COUNT(*) AS occurrences
            FROM option_detector_evaluations
            WHERE dataset_id=%s AND run_id=%s
            GROUP BY detector_id,selection_status,selection_reason
            ORDER BY detector_id,selection_status,selection_reason
        """, (args.dataset_id, str(args.run_id)))
        evaluations = [dict(row) for row in cursor.fetchall()]
        cursor.execute("""
            SELECT payload_text::jsonb->'observation'->>'baseline_disposition' AS disposition,
                   COUNT(*) AS observations,
                   COUNT(DISTINCT payload_text::jsonb->'observation'->>'underlyer') AS underlyers
            FROM option_o1_indicator_observations
            WHERE dataset_id=%s AND run_id=%s
            GROUP BY disposition ORDER BY disposition
        """, (args.dataset_id, str(args.run_id)))
        o1_dispositions = [dict(row) for row in cursor.fetchall()]
    evidence = run["selection_evidence"]
    print(json.dumps({"version": "option_detector_contract_funnel_v1", "mode": "READ_ONLY",
        "dataset_id": args.dataset_id, "run_id": str(args.run_id),
        "scheduled_cycle": run["scheduled_cycle"], "selected_at": selected_at,
        "covered_underlyings": run["covered_underlying_count"],
        "expected_underlyings": run["expected_underlying_count"],
        "selection_counts": evidence.get("payload_text") if isinstance(evidence, dict) else None,
        "activity_funnel": activity_funnel,
        "strategy_groups": strategy_groups, "package_funnel": package_funnel,
        "o1_dispositions": o1_dispositions, "evaluations": evaluations,
        "source_writes": 0}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
