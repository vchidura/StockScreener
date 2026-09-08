#!/usr/bin/env python3
"""Report read-only coverage of worker-materialized option current package marks."""
from __future__ import annotations

import json
import sys
from pathlib import Path

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from options.outcomes import delayed_proxy_commission_policy  # noqa: E402


def main() -> int:
    policy = delayed_proxy_commission_policy()
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) AS current_mark_count,
                   COUNT(DISTINCT candidate_id) AS current_candidate_count,
                   MIN(market_time) AS oldest_market_time,
                   MAX(market_time) AS newest_market_time
            FROM option_signal_current_marks
            WHERE valuation_policy_sha256 = %s
            """,
            (policy.policy_sha256,),
        )
        current_marks = dict(cursor.fetchone())
        cursor.execute(
            """
            SELECT COUNT(*) AS eligible_candidate_count,
                   COUNT(*) FILTER (WHERE current_mark.candidate_id IS NOT NULL)
                       AS marked_candidate_count,
                   COUNT(*) FILTER (WHERE current_mark.candidate_id IS NULL)
                       AS pending_candidate_count
            FROM option_strategy_candidates AS candidate
            LEFT JOIN option_signal_current_marks AS current_mark
              ON current_mark.candidate_id = candidate.candidate_id
             AND current_mark.valuation_policy_sha256 = %s
            WHERE candidate.status = 'SELECTED'
              AND candidate.candidate_kind IN ('SINGLE_CONTRACT', 'MULTI_LEG')
              AND candidate.capital_at_risk > 0
              AND candidate.market_data_time <= NOW()
              AND candidate.market_data_time > NOW() - INTERVAL '60 days'
              AND EXISTS (
                  SELECT 1 FROM option_candidate_legs AS leg
                  WHERE leg.candidate_id = candidate.candidate_id
                    AND leg.expiration_date >=
                        (NOW() AT TIME ZONE 'America/New_York')::DATE
              )
            """,
            (policy.policy_sha256,),
        )
        eligible = dict(cursor.fetchone())
        cursor.execute(
            """
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (
                       WHERE current_mark.market_time <= candidate.market_data_time
                   ) AS not_after_detection,
                   COUNT(*) FILTER (WHERE current_mark.gross_pnl = 0) AS zero_gross,
                   COUNT(*) FILTER (WHERE current_mark.gross_pnl <> 0) AS moved
            FROM option_signal_current_marks AS current_mark
            JOIN option_strategy_candidates AS candidate
              ON candidate.candidate_id = current_mark.candidate_id
            WHERE current_mark.valuation_policy_sha256 = %s
            """,
            (policy.policy_sha256,),
        )
        causality = dict(cursor.fetchone())
        cursor.execute(
            """
            SELECT candidate.underlying, candidate.strategy_name,
                   candidate.candidate_id, current_mark.market_time,
                   current_mark.observed_time, current_mark.net_pnl,
                   current_mark.net_return
            FROM option_strategy_candidates AS candidate
            JOIN option_signal_current_marks AS current_mark
              ON current_mark.candidate_id = candidate.candidate_id
             AND current_mark.valuation_policy_sha256 = %s
            ORDER BY current_mark.market_time DESC, candidate.candidate_id
            LIMIT 10
            """,
            (policy.policy_sha256,),
        )
        samples = [dict(row) for row in cursor.fetchall()]
    print(json.dumps(
        {
            "valuation_policy": policy.policy_version,
            "current_marks": current_marks,
            "causality": causality,
            "eligible_selected_packages": eligible,
            "sample_current_marks": samples,
        },
        default=str,
        sort_keys=True,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())