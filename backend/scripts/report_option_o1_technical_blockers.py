#!/usr/bin/env python3
"""Classify retained O1 technical-source blockers without writes."""
from __future__ import annotations

import argparse
from datetime import timedelta
from decimal import Decimal
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
from equity.materialization import SETUP_VERSION  # noqa: E402
from equity.polygon import sha256_json  # noqa: E402
from equity.repositories import EquityCorporateActionRepository  # noqa: E402

STRUCTURAL = ["Price Action", "Gap", "FVG", "Pattern", "Volume Pivot"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--run-id", type=UUID, required=True)
    args = parser.parse_args()
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='30s'")
        cursor.execute("""
            SELECT scheduled_cycle,published_at,market_data_time,source_matrix_ids
            FROM option_board_publications
            WHERE publication_id=%s AND status='COMPLETE'
              AND selection_evidence->>'dataset_id'=%s
        """, (str(args.run_id), args.dataset_id))
        run = cursor.fetchone()
        if not run:
            raise ValueError("completed detector run unavailable")
        cursor.execute("""
            WITH directional AS (
                SELECT candidate.* FROM option_strategy_candidates AS candidate
                WHERE candidate.matrix_id=ANY(%s::uuid[])
                  AND candidate.status='SELECTED' AND candidate.valid_until>%s
                  AND candidate.candidate_kind IN ('SINGLE_CONTRACT','MULTI_LEG')
                  AND candidate.strategy_name IN ('DIRECTIONAL_LONG_PREMIUM','DIRECTIONAL_DEBIT_SPREAD')
            ), bought AS (
                SELECT candidate.candidate_id,candidate.underlying,candidate.strategy_name,
                       candidate.structure_type,leg.snapshot_id,
                       CASE WHEN leg.contract_type='CALL' THEN 1 ELSE -1 END AS direction,leg.spot
                FROM directional AS candidate JOIN option_candidate_legs AS leg USING(candidate_id)
                WHERE leg.side='BUY'
            )
            SELECT bought.* FROM bought JOIN option_o1_indicator_observations AS evidence
              ON (evidence.payload_text::jsonb->'observation'->>'snapshot_id')::uuid=bought.snapshot_id
            WHERE evidence.dataset_id=%s AND evidence.run_id=%s
              AND evidence.payload_text::jsonb->'observation'->>'baseline_disposition'='CONFIRMED'
            ORDER BY bought.underlying,bought.direction,bought.candidate_id
        """, (run["source_matrix_ids"], run["published_at"], args.dataset_id, str(args.run_id)))
        candidates = [dict(row) for row in cursor.fetchall()]
        tickers = sorted({row["underlying"] for row in candidates})
        cursor.execute("""
            WITH eligible AS (
                SELECT setup.*,feature.source_revision_ids AS feature_bar_ids
                FROM equity_evidence AS setup JOIN equity_evidence AS feature
                  ON feature.evidence_id=(setup.payload->>'feature_evidence_id')::uuid
                WHERE setup.ticker=ANY(%s) AND setup.evidence_type='TRADE_SETUP'
                  AND setup.source_name='EQUITY_SETUP' AND setup.source_version=%s
                  AND setup.interval IN ('30m','1h')
                  AND setup.quality_state IN ('COMPLETE','RESEARCH_ONLY')
                  AND setup.lifecycle_status<>'CONFLICTED'
                  AND setup.market_time<=%s AND setup.observed_at<=%s
                  AND setup.created_at<=%s AND setup.valid_until>%s
                  AND feature.evidence_type='FEATURE_SNAPSHOT'
                  AND feature.security_id=setup.security_id AND feature.interval=setup.interval
                  AND feature.market_time=setup.market_time
                  AND feature.observed_at<=%s AND feature.created_at<=%s
            )
            SELECT DISTINCT ON(ticker,interval) * FROM eligible
            ORDER BY ticker,interval,market_time DESC,observed_at DESC
        """, (tickers, SETUP_VERSION, run["market_data_time"], run["published_at"],
              run["published_at"], run["published_at"], run["published_at"], run["published_at"]))
        setups = [dict(row) for row in cursor.fetchall()]
        feature_ids = sorted({identity for row in setups for identity in row["feature_bar_ids"]}, key=str)
        cursor.execute("""
            SELECT bar_revision_id,security_id,ticker,adjusted,bar_end,session_date
            FROM equity_bar_revisions
            WHERE bar_revision_id=ANY(%s::uuid[]) AND NOT adjusted AND is_final
              AND created_at<=%s AND system_observed_at<=%s
        """, (feature_ids, run["published_at"], run["published_at"]))
        bars = {row["bar_revision_id"]: dict(row) for row in cursor.fetchall()}
        cursor.execute("""
            SELECT DISTINCT ON(ticker) ticker,window_start,window_end,
                   first_observed_at,created_at,availability_mode
            FROM equity_corporate_action_coverage
            WHERE ticker=ANY(%s) AND action_type='SPLIT'
              AND first_observed_at<=%s AND first_observed_at>=%s
            ORDER BY ticker,first_observed_at DESC
        """, (tickers, run["published_at"], run["published_at"] - timedelta(hours=24)))
        latest_coverages = {row["ticker"]: dict(row) for row in cursor.fetchall()}

    action_repository = EquityCorporateActionRepository()
    results = []
    for candidate in candidates:
        current = sorted((row for row in setups if row["ticker"] == candidate["underlying"]
            and row["direction"] == candidate["direction"]),
            key=lambda row: (row["market_time"], row["observed_at"]), reverse=True)[:1]
        reasons = []
        feature_start = None
        if not current:
            reasons.append("CURRENT_READER_NO_DIRECTION_MATCH")
        for source in current:
            if sha256_json(source["payload"]) != source["payload_sha256"]:
                reasons.append("TECHNICAL_PAYLOAD_CHECKSUM_MISMATCH")
            selected = [bars[identity] for identity in source["feature_bar_ids"] if identity in bars]
            if (len(selected) != len(source["feature_bar_ids"])
                    or any(row["security_id"] != source["security_id"] or row["ticker"] != source["ticker"]
                        or row["adjusted"] or row["bar_end"] > source["market_time"] for row in selected)):
                reasons.append("FEATURE_HISTORY_UNAVAILABLE")
            if selected:
                feature_start = min(row["session_date"] for row in selected)
                coverage, actions = action_repository.latest_covered_actions(source["ticker"], "SPLIT",
                    window_start=feature_start, window_end=source["market_time"].date(),
                    observed_at=run["published_at"], maximum_age=timedelta(hours=24))
                if coverage is None:
                    reasons.append("SPLIT_COVERAGE_UNAVAILABLE")
                    latest = latest_coverages.get(source["ticker"])
                    if latest and latest["window_start"] > feature_start:
                        reasons.append("SPLIT_COVERAGE_WINDOW_TOO_NARROW")
                elif actions:
                    reasons.append("SPLIT_ACTION_PRESENT")
            stops = [level for level in source["payload"].get("stops", ())
                if level.get("source") in STRUCTURAL
                and candidate["direction"] * (candidate["spot"] - Decimal(str(level["price"]))) > 0]
            targets = [level for level in source["payload"].get("targets", ())
                if level.get("source") in STRUCTURAL
                and candidate["direction"] * (Decimal(str(level["price"])) - candidate["spot"]) > 0]
            if not stops:
                reasons.append("STRUCTURAL_STOP_UNAVAILABLE")
            if not targets:
                reasons.append("STRUCTURAL_TARGET_UNAVAILABLE")
        results.append(dict(candidate_id=str(candidate["candidate_id"]), underlying=candidate["underlying"],
            direction=candidate["direction"], strategy_name=candidate["strategy_name"],
            structure_type=candidate["structure_type"], spot=str(candidate["spot"]),
            feature_bar_count=len(current[0]["feature_bar_ids"]) if current else 0,
            feature_window_start=feature_start, latest_split_coverage=latest_coverages.get(candidate["underlying"]),
            reasons=sorted(set(reasons))))
    counts = {}
    for row in results:
        for reason in row["reasons"] or ["READY"]:
            counts[reason] = counts.get(reason, 0) + 1
    print(json.dumps(dict(version="option_o1_technical_blockers_v1", mode="READ_ONLY",
        dataset_id=args.dataset_id, run_id=str(args.run_id), scheduled_cycle=run["scheduled_cycle"],
        selected_at=run["published_at"], candidate_count=len(results), reason_counts=counts,
        candidates=results, source_writes=0), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
