#!/usr/bin/env python3
"""Report whether policy-aligned settlement history can support IV context."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import sys
from pathlib import Path

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from options.calendar import OptionExchangeCalendar  # noqa: E402
from options.config import load_option_runtime_configuration  # noqa: E402


DEFAULT_OUTPUT = BACKEND_DIR.parent / "docs" / "option_iv_context_readiness.json"


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return result


def main() -> int:
    args = parser().parse_args()
    configuration = load_option_runtime_configuration()
    policy = configuration.settlement_valuation_policy
    now = datetime.now(timezone.utc)
    calendar = OptionExchangeCalendar()
    latest_completed_session = calendar.latest_completed_session(now)
    next_session = calendar.next_session_open(latest_completed_session).date()
    expected_sessions = calendar.trailing_sessions_before(
        next_session,
        policy.iv_lookback_sessions,
    )
    lookback_start = expected_sessions[0]
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            WITH legacy AS (
                SELECT underlying,
                       COUNT(*) FILTER (WHERE mark_close IS NOT NULL) AS all_marks,
                       COUNT(DISTINCT settlement_session) FILTER (
                           WHERE mark_close IS NOT NULL
                       ) AS all_sessions
                FROM option_daily_contract_facts
                WHERE underlying = ANY(%s)
                GROUP BY underlying
            ), policy AS (
                SELECT underlying, COUNT(*) AS policy_marks,
                       COUNT(DISTINCT settlement_session) AS policy_sessions,
                       MIN(settlement_session) AS policy_start,
                       MAX(settlement_session) AS policy_end
                FROM option_daily_contract_mark_revisions
                WHERE mark_adjusted = FALSE
                  AND mark_source = %s
                  AND valuation_policy_sha256 = %s
                  AND underlying = ANY(%s)
                                    AND settlement_session >= %s
                                    AND settlement_session <= %s
                GROUP BY underlying
            )
            SELECT COALESCE(legacy.underlying, policy.underlying) AS underlying,
                   legacy.all_marks, legacy.all_sessions,
                   policy.policy_marks, policy.policy_sessions,
                   policy.policy_start, policy.policy_end
            FROM legacy FULL OUTER JOIN policy USING (underlying)
            ORDER BY underlying
            """,
            (
                list(configuration.settings.underlyers),
                policy.mark_source,
                configuration.settlement_valuation_policy_sha256,
                list(configuration.settings.underlyers),
                lookback_start,
                latest_completed_session,
            ),
        )
        by_underlying = {row["underlying"]: dict(row) for row in cursor.fetchall()}
        cursor.execute(
            """
            WITH latest AS (
                SELECT DISTINCT ON (context.underlying, context.expiration_bucket)
                       context.*
                FROM option_iv_context_snapshots AS context
                JOIN option_analysis_runs AS analysis USING (matrix_id)
                JOIN option_ingestion_runs AS ingestion USING (batch_id)
                WHERE context.settlement_valuation_policy_sha256 = %s
                  AND context.calculation_version = %s
                  AND ingestion.configuration_sha256 = %s
                  AND analysis.policy_sha256 = %s
                  AND analysis.status = 'COMPLETE'
                ORDER BY context.underlying, context.expiration_bucket,
                         analysis.market_time DESC, analysis.observed_time DESC
            )
            SELECT COUNT(*) AS context_count,
                   COUNT(DISTINCT underlying) AS underlying_count,
                   COUNT(*) FILTER (
                       WHERE cardinality(null_reason_codes) = 0
                   ) AS ready_context_count,
                   COUNT(DISTINCT underlying) FILTER (
                       WHERE cardinality(null_reason_codes) = 0
                   ) AS ready_underlying_count
            FROM latest
            """,
            (
                configuration.settlement_valuation_policy_sha256,
                policy.iv_context_calculation_version,
                configuration.configuration_sha256,
                configuration.policy_sha256,
            ),
        )
        contexts = dict(cursor.fetchone())
        cursor.execute(
            """
            SELECT ticker,
                   BOOL_OR(
                       availability_mode = 'LIVE_OBSERVED'
                       AND first_observed_at >= %s
                       AND window_start <= CURRENT_DATE
                       AND window_end >= CURRENT_DATE + %s
                   ) AS live_ready,
                   BOOL_OR(
                       availability_mode = 'HISTORICAL_RECONSTRUCTED'
                       AND window_start <= CURRENT_DATE - %s
                       AND window_end >= CURRENT_DATE
                   ) AS historical_ready
            FROM equity_corporate_action_coverage
            WHERE LOWER(source) = %s
              AND action_type = 'DIVIDEND'
              AND ticker = ANY(%s)
            GROUP BY ticker
            """,
            (
                now - timedelta(
                    seconds=configuration.settings.dividend_input_max_age_seconds
                ),
                configuration.policy.contract_filter.maximum_dte,
                policy.iv_lookback_sessions * 2,
                configuration.settings.dividend_input_source or "",
                list(configuration.settings.underlyers),
            ),
        )
        dividend_coverage = {
            row["ticker"]: {
                "live": bool(row["live_ready"]),
                "historical": bool(row["historical_ready"]),
            }
            for row in cursor.fetchall()
        }
        cursor.execute(
            """
            SELECT COUNT(*) AS observation_count,
                   COUNT(DISTINCT rate_date) AS rate_dates,
                   MIN(rate_date) AS rate_start,
                   MAX(rate_date) AS rate_end
            FROM option_risk_free_rate_observations
            WHERE source = %s
              AND COALESCE(replay_available_at, first_observed_at) <= %s
            """,
            (
                configuration.settings.risk_free_rate_source,
                now,
            ),
        )
        rate_history = dict(cursor.fetchone())

    rows = []
    for underlying in configuration.settings.underlyers:
        values = by_underlying.get(underlying, {})
        sessions = int(values.get("policy_sessions") or 0)
        coverage = min(sessions / policy.iv_lookback_sessions, 1.0)
        rows.append({
            "underlying": underlying,
            "all_marks": int(values.get("all_marks") or 0),
            "all_sessions": int(values.get("all_sessions") or 0),
            "policy_marks": int(values.get("policy_marks") or 0),
            "policy_sessions": sessions,
            "policy_coverage_fraction": coverage,
            "policy_start": values.get("policy_start"),
            "policy_end": values.get("policy_end"),
            "mark_history_ready": (
                sessions >= policy.minimum_iv_sample_sessions
                and coverage >= policy.minimum_iv_coverage_fraction
            ),
        })
    missing = [
        row["underlying"] for row in rows if not row["mark_history_ready"]
    ]
    mark_history_ready = not missing
    dated_rates_ready = (
        configuration.settings.risk_free_rate_source != "manual_config_v1"
    )
    dated_dividends_ready = (
        configuration.settings.dividend_input_source is not None
        and all(
            dividend_coverage.get(underlying, {}).get("live", False)
            and dividend_coverage.get(underlying, {}).get("historical", False)
            for underlying in configuration.settings.underlyers
        )
    )
    expected_context_count = len(configuration.settings.underlyers) * 3
    comparable_iv_ready = (
        int(contexts["ready_context_count"] or 0) == expected_context_count
        and int(contexts["ready_underlying_count"] or 0)
        == len(configuration.settings.underlyers)
    )
    iv_context_ready = (
        mark_history_ready
        and dated_rates_ready
        and dated_dividends_ready
        and comparable_iv_ready
    )
    report = {
        "generated_at": now,
        "mark_history_ready": mark_history_ready,
        "iv_context_ready": iv_context_ready,
        "blockers": [
            *(
                []
                if mark_history_ready
                else ["INSUFFICIENT_POLICY_ALIGNED_SETTLEMENT_HISTORY"]
            ),
            *([] if dated_rates_ready else ["POINT_IN_TIME_RATES_NOT_CONFIGURED"]),
            *(
                []
                if dated_dividends_ready
                else ["POINT_IN_TIME_DIVIDENDS_NOT_CONFIGURED"]
            ),
            *(
                []
                if comparable_iv_ready
                else ["COMPARABLE_IV_SERIES_NOT_MATERIALIZED"]
            ),
        ],
        "settlement_valuation_policy_version": policy.policy_version,
        "settlement_valuation_policy_sha256": (
            configuration.settlement_valuation_policy_sha256
        ),
        "mark_source": policy.mark_source,
        "option_aggregates_adjusted": policy.option_aggregates_adjusted,
        "iv_context_calculation_version": policy.iv_context_calculation_version,
        "required_lookback_sessions": policy.iv_lookback_sessions,
        "lookback_start_session": lookback_start,
        "lookback_end_session": latest_completed_session,
        "minimum_sample_sessions": policy.minimum_iv_sample_sessions,
        "minimum_coverage_fraction": policy.minimum_iv_coverage_fraction,
        "dated_rates_ready": dated_rates_ready,
        "risk_free_rate_source": configuration.settings.risk_free_rate_source,
        "rate_history": rate_history,
        "dated_dividends_ready": dated_dividends_ready,
        "dividend_input_source": configuration.settings.dividend_input_source,
        "dividend_coverage": dividend_coverage,
        "missing_underlyings": missing,
        "underlyings": rows,
        "materialized_contexts": contexts,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, default=str, allow_nan=False),
        encoding="utf-8",
    )
    print(json.dumps(report, indent=2, default=str, allow_nan=False))
    return 0 if report["iv_context_ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())