#!/usr/bin/env python3
"""Materialize matched-horizon IV context for each latest configured matrix."""
from __future__ import annotations

import argparse
from bisect import bisect_right
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import sys
from uuid import NAMESPACE_URL, uuid5


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
from dotenv import load_dotenv
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from options.analytics.greeks import OptionValuationInput, solve_local_greeks  # noqa: E402
from options.analytics.iv_context import summarize_iv_context  # noqa: E402
from options.analytics.volatility_risk_premium import (  # noqa: E402
    ContractIvObservation,
    interpolate_total_variance_at_maturity,
    summarize_expiration_atm_iv,
)
from options.calendar import OptionExchangeCalendar  # noqa: E402
from options.config import load_option_runtime_configuration  # noqa: E402
from options.domain import ContractType  # noqa: E402
from options.model_inputs import (  # noqa: E402
    DividendCashFlow,
    TREASURY_CURVE_SOURCE,
    equivalent_continuous_dividend_yield,
    interpolate_rate,
)
from options.repositories.iv_context import (  # noqa: E402
    IvContextRecord,
    OptionIvContextRepository,
)
from options.repositories.model_inputs import OptionModelInputRepository  # noqa: E402


HORIZONS = {"7D": 7, "21D": 21, "45D": 45}
CALENDAR_DAYS_PER_YEAR = 365.0
DEFAULT_MAXIMUM_ABSOLUTE_LOG_MONEYNESS = 0.03
DEFAULT_OUTPUT = BACKEND_DIR.parent / "docs" / "option_iv_context_materialization.json"


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--apply",
        action="store_true",
        help="Persist immutable contexts; the default is a read-only dry run.",
    )
    result.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    result.add_argument(
        "--validation-latest-complete",
        action="store_true",
        help=(
            "Dry-run the latest complete cohort across prior configuration or "
            "market-policy identities."
        ),
    )
    result.add_argument(
        "--validation-maximum-log-moneyness",
        type=float,
        help="Dry-run sensitivity only; the persisted calculation remains fixed at 3%%.",
    )
    return result


def _validate_args(args: argparse.Namespace) -> None:
    if args.apply and (
        args.validation_latest_complete
        or args.validation_maximum_log_moneyness is not None
    ):
        raise ValueError("validation overrides cannot persist IV contexts")
    if (
        args.validation_maximum_log_moneyness is not None
        and not args.validation_latest_complete
    ):
        raise ValueError("moneyness sensitivity requires prior-cohort validation")
    if (
        args.validation_maximum_log_moneyness is not None
        and not 0 < args.validation_maximum_log_moneyness <= 0.20
    ):
        raise ValueError("validation moneyness must be in (0, 0.20]")

SQL_LATEST_MATRICES = """
SELECT DISTINCT ON (analysis.underlying)
       analysis.matrix_id, analysis.batch_id, analysis.underlying,
       analysis.market_time, analysis.observed_time,
    ingestion.configuration_sha256 AS source_configuration_sha256,
    analysis.policy_sha256 AS source_policy_sha256
FROM option_analysis_runs AS analysis
JOIN option_ingestion_runs AS ingestion USING (batch_id)
WHERE ingestion.configuration_sha256 = %s
  AND analysis.policy_sha256 = %s
  AND analysis.status = 'COMPLETE'
ORDER BY analysis.underlying, analysis.market_time DESC, analysis.observed_time DESC
"""

SQL_LATEST_MATRICES_ANY_CONFIGURATION = """
SELECT DISTINCT ON (analysis.underlying)
       analysis.matrix_id, analysis.batch_id, analysis.underlying,
       analysis.market_time, analysis.observed_time,
    ingestion.configuration_sha256 AS source_configuration_sha256,
    analysis.policy_sha256 AS source_policy_sha256
FROM option_analysis_runs AS analysis
JOIN option_ingestion_runs AS ingestion USING (batch_id)
WHERE analysis.status = 'COMPLETE'
ORDER BY analysis.underlying, analysis.market_time DESC, analysis.observed_time DESC
"""

SQL_CURRENT_IV = """
SELECT expiration_date, expiration_cutoff, contract_type,
       strike, spot, local_iv
FROM option_chain_snapshots
WHERE batch_id = %s AND local_iv IS NOT NULL AND iv_converged
"""

SQL_MARKS = """
SELECT mark.settlement_session, mark.mark_close,
       version.contract_type, version.strike, version.expiration_date
FROM option_daily_contract_mark_revisions AS mark
JOIN LATERAL (
    SELECT contract_type, strike, expiration_date
    FROM option_contract_catalog_versions
    WHERE contract_id = mark.contract_id AND contract_type IS NOT NULL
    ORDER BY
        CASE WHEN valid_from <= mark.settlement_session + INTERVAL '1 day'
             THEN 0 ELSE 1 END,
        valid_from DESC
    LIMIT 1
) AS version ON TRUE
WHERE mark.underlying = %s
  AND mark.valuation_policy_sha256 = %s
    AND mark.settlement_session >= %s
  AND mark.settlement_session < %s
ORDER BY mark.settlement_session, version.expiration_date, version.strike
"""

SQL_SPOTS = """
SELECT DISTINCT ON (session_date) session_date, close_price
FROM equity_bar_revisions
WHERE ticker = %s AND interval = '1d' AND is_final
  AND session_scope = 'RTH' AND adjusted = false
ORDER BY session_date,
         CASE source_kind WHEN 'RECONCILED' THEN 0 WHEN 'DERIVED' THEN 1
              WHEN 'NATIVE_REST' THEN 2 ELSE 4 END,
         COALESCE(replay_available_at, system_observed_at) DESC
"""

SQL_DIVIDEND_COVERAGE = """
SELECT * FROM equity_corporate_action_coverage
WHERE LOWER(source) = %s AND ticker = %s AND action_type = 'DIVIDEND'
  AND availability_mode = 'HISTORICAL_RECONSTRUCTED'
  AND window_start <= %s AND window_end >= %s
ORDER BY first_observed_at DESC LIMIT 1
"""

SQL_DIVIDENDS = """
SELECT action.corporate_action_id, action.ex_date, action.cash_amount
FROM equity_corporate_action_coverage_members AS member
JOIN equity_corporate_actions AS action USING (corporate_action_id)
WHERE member.coverage_id = %s
  AND action.cash_amount IS NOT NULL AND action.ex_date IS NOT NULL
ORDER BY action.ex_date, action.source_key
"""


def _expiration_points(
    rows,
    maximum_absolute_log_moneyness: float = (
        DEFAULT_MAXIMUM_ABSOLUTE_LOG_MONEYNESS
    ),
) -> tuple:
    grouped = defaultdict(list)
    for row in rows:
        grouped[row.expiration_date].append(row)
    return tuple(
        point for expiration in sorted(grouped)
        if (point := summarize_expiration_atm_iv(
            tuple(grouped[expiration]),
            maximum_absolute_log_moneyness=maximum_absolute_log_moneyness,
        ))
        is not None
    )


def _match_calendar_horizon(points, horizon_days: int):
    return interpolate_total_variance_at_maturity(
        points,
        horizon_days / CALENDAR_DAYS_PER_YEAR,
    )


def _current_observations(rows, matrix_market_time: datetime):
    observations = []
    for row in rows:
        maturity = (
            row["expiration_cutoff"] - matrix_market_time
        ).total_seconds() / (CALENDAR_DAYS_PER_YEAR * 24 * 3600)
        if maturity <= 0:
            continue
        observations.append(ContractIvObservation(
            expiration_date=row["expiration_date"],
            maturity_years=maturity,
            contract_type=ContractType(row["contract_type"]),
            strike=Decimal(str(row["strike"])),
            spot=Decimal(str(row["spot"])),
            implied_volatility=float(row["local_iv"]),
        ))
    return tuple(observations)


def _current_matched(
    batch_id,
    matrix_market_time: datetime,
    maximum_absolute_log_moneyness: float,
):
    with get_db_cursor() as cursor:
        cursor.execute(SQL_CURRENT_IV, (batch_id,))
        rows = cursor.fetchall()
    observations = _current_observations(rows, matrix_market_time)
    points = _expiration_points(observations, maximum_absolute_log_moneyness)
    return {
        bucket: _match_calendar_horizon(points, horizon_days)
        for bucket, horizon_days in HORIZONS.items()
    }


def _historical_matched(
    underlying,
    matrix_date,
    configuration,
    observed_at,
    maximum_absolute_log_moneyness: float,
):
    calendar = OptionExchangeCalendar()
    expected_sessions = calendar.trailing_sessions_before(
        matrix_date,
        configuration.settlement_valuation_policy.iv_lookback_sessions,
    )
    start = expected_sessions[0]
    with get_db_cursor() as cursor:
        cursor.execute(SQL_MARKS, (
            underlying,
            configuration.settlement_valuation_policy_sha256,
            start,
            matrix_date,
        ))
        marks = cursor.fetchall()
        coverage_end = max(
            (row["expiration_date"] for row in marks),
            default=matrix_date,
        )
        cursor.execute(SQL_SPOTS, (underlying,))
        spots = {
            row["session_date"]: Decimal(str(row["close_price"]))
            for row in cursor.fetchall()
        }
        cursor.execute(SQL_DIVIDEND_COVERAGE, (
            configuration.settings.dividend_input_source,
            underlying,
            start,
            coverage_end,
        ))
        dividend_coverage = cursor.fetchone()
        if dividend_coverage is None:
            raise RuntimeError(f"historical dividend coverage unavailable for {underlying}")
        cursor.execute(SQL_DIVIDENDS, (dividend_coverage["coverage_id"],))
        dividend_rows = cursor.fetchall()
    cash_flows = tuple(DividendCashFlow(
        ex_date=row["ex_date"], cash_amount=Decimal(str(row["cash_amount"]))
    ) for row in dividend_rows if Decimal(str(row["cash_amount"])) > 0)
    curves = OptionModelInputRepository().curves_between(
        source=TREASURY_CURVE_SOURCE,
        start=start,
        end=matrix_date,
        observed_at=observed_at,
    )
    by_curve_date = defaultdict(list)
    for row in curves:
        by_curve_date[row.rate_date].append(row)
    curve_dates = sorted(by_curve_date)
    if not curve_dates:
        raise RuntimeError("historical Treasury curves are unavailable")
    valuation_inputs = []
    metadata = []
    rate_ids_by_session = defaultdict(set)
    skipped_missing_spot = 0
    skipped_expired = 0
    skipped_missing_rate = 0
    for row in marks:
        session = row["settlement_session"]
        spot = spots.get(session)
        expiration = row["expiration_date"]
        if spot is None:
            skipped_missing_spot += 1
            continue
        if expiration <= session:
            skipped_expired += 1
            continue
        curve_index = bisect_right(curve_dates, session) - 1
        if curve_index < 0:
            skipped_missing_rate += 1
            continue
        curve = tuple(by_curve_date[curve_dates[curve_index]])
        maturity_days = (expiration - session).days
        rate = interpolate_rate(curve, maturity_days)
        dividend_yield = equivalent_continuous_dividend_yield(
            spot=spot,
            valuation_date=session,
            expiration_date=expiration,
            risk_free_rate=rate,
            cash_flows=cash_flows,
        )
        maturity = (
            calendar.expiration_cutoff(expiration) - calendar.session_close(session)
        ).total_seconds() / (365.0 * 24 * 3600)
        if maturity <= 0:
            continue
        valuation_inputs.append(OptionValuationInput(
            contract_type=ContractType(row["contract_type"]),
            spot=spot,
            strike=Decimal(str(row["strike"])),
            model_mark=Decimal(str(row["mark_close"])),
            time_to_expiration_years=maturity,
            risk_free_rate=rate,
            dividend_yield=dividend_yield,
        ))
        metadata.append((
            session, expiration, ContractType(row["contract_type"]),
            Decimal(str(row["strike"])), spot, maturity,
        ))
        rate_ids_by_session[session].update(item.rate_observation_id for item in curve)
    by_session = defaultdict(list)
    solved_rows = solve_local_greeks(tuple(valuation_inputs))
    solver_failures = defaultdict(int)
    for solved, meta in zip(solved_rows, metadata):
        if not solved.converged or solved.local_iv is None:
            reason = (
                solved.failure_reason.value
                if solved.failure_reason is not None
                else "UNKNOWN"
            )
            solver_failures[reason] += 1
            continue
        session, expiration, contract_type, strike, spot, maturity = meta
        by_session[session].append(ContractIvObservation(
            expiration_date=expiration,
            maturity_years=maturity,
            contract_type=contract_type,
            strike=strike,
            spot=spot,
            implied_volatility=solved.local_iv,
        ))
    history = {bucket: [] for bucket in HORIZONS}
    expiration_point_count = 0
    sessions_with_expiration_points = 0
    for session in sorted(by_session):
        points = _expiration_points(
            tuple(by_session[session]),
            maximum_absolute_log_moneyness,
        )
        expiration_point_count += len(points)
        sessions_with_expiration_points += bool(points)
        for bucket, horizon_days in HORIZONS.items():
            matched = _match_calendar_horizon(points, horizon_days)
            if matched is not None:
                history[bucket].append((session, matched.implied_volatility))
    diagnostics = {
        "mark_rows": len(marks),
        "valuation_input_rows": len(valuation_inputs),
        "skipped_missing_spot_rows": skipped_missing_spot,
        "skipped_expired_rows": skipped_expired,
        "skipped_missing_rate_rows": skipped_missing_rate,
        "solver_converged_rows": sum(row.converged for row in solved_rows),
        "solver_failure_rows": dict(sorted(solver_failures.items())),
        "sessions_with_solved_contracts": len(by_session),
        "sessions_with_expiration_points": sessions_with_expiration_points,
        "expiration_point_count": expiration_point_count,
        "matched_sessions": {
            bucket: len(values) for bucket, values in history.items()
        },
    }
    return (
        {bucket: tuple(values) for bucket, values in history.items()},
        rate_ids_by_session,
        dividend_coverage["coverage_id"],
        tuple(row["corporate_action_id"] for row in dividend_rows),
        expected_sessions,
        diagnostics,
    )


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    _validate_args(args)
    configuration = load_option_runtime_configuration()
    maximum_absolute_log_moneyness = (
        args.validation_maximum_log_moneyness
        or DEFAULT_MAXIMUM_ABSOLUTE_LOG_MONEYNESS
    )
    observed_at = datetime.now(timezone.utc)
    with get_db_cursor() as cursor:
        if args.validation_latest_complete:
            cursor.execute(SQL_LATEST_MATRICES_ANY_CONFIGURATION)
        else:
            cursor.execute(SQL_LATEST_MATRICES, (
                configuration.configuration_sha256,
                configuration.policy_sha256,
            ))
        matrices = cursor.fetchall()
    source_configuration_by_matrix = {
        matrix["matrix_id"]: matrix["source_configuration_sha256"]
        for matrix in matrices
    }
    source_policy_by_matrix = {
        matrix["matrix_id"]: matrix["source_policy_sha256"]
        for matrix in matrices
    }
    records = []
    historical_diagnostics = []
    for matrix in matrices:
        current = _current_matched(
            matrix["batch_id"],
            matrix["market_time"],
            maximum_absolute_log_moneyness,
        )
        (
            history,
            rate_ids_by_session,
            coverage_id,
            dividend_ids,
            expected_sessions,
            diagnostics,
        ) = _historical_matched(
            matrix["underlying"],
            matrix["market_time"].date(),
            configuration,
            observed_at,
            maximum_absolute_log_moneyness,
        )
        historical_diagnostics.append({
            "underlying": matrix["underlying"],
            **diagnostics,
        })
        for bucket in HORIZONS:
            current_match = current[bucket]
            historical = history[bucket]
            statistics = summarize_iv_context(
                current_match.implied_volatility if current_match else None,
                historical,
                lookback_sessions=(
                    configuration.settlement_valuation_policy.iv_lookback_sessions
                ),
                minimum_sample_sessions=(
                    configuration.settlement_valuation_policy.minimum_iv_sample_sessions
                ),
                minimum_coverage_fraction=(
                    configuration.settlement_valuation_policy.minimum_iv_coverage_fraction
                ),
                expected_sessions=expected_sessions,
            )
            used_sessions = [session for session, _ in historical][
                -configuration.settlement_valuation_policy.iv_lookback_sessions:
            ]
            rate_ids = tuple(sorted({
                value for session in used_sessions
                for value in rate_ids_by_session[session]
            }, key=str))
            identity = (
                f"{matrix['matrix_id']}:{bucket}:"
                f"{configuration.settlement_valuation_policy.iv_context_calculation_version}:"
                f"{configuration.settlement_valuation_policy_sha256}"
            )
            records.append(IvContextRecord(
                iv_context_id=uuid5(NAMESPACE_URL, identity),
                matrix_id=matrix["matrix_id"],
                underlying=matrix["underlying"],
                expiration_bucket=bucket,
                statistics=statistics,
                calculation_version=(
                    configuration.settlement_valuation_policy.iv_context_calculation_version
                ),
                first_observed_time=observed_at,
                settlement_valuation_policy_version=(
                    configuration.settlement_valuation_policy.policy_version
                ),
                settlement_valuation_policy_sha256=(
                    configuration.settlement_valuation_policy_sha256
                ),
                rate_source=configuration.settings.risk_free_rate_source,
                rate_observation_ids=rate_ids,
                dividend_source=configuration.settings.dividend_input_source or "",
                dividend_coverage_ids=(coverage_id,),
                dividend_action_ids=tuple(dividend_ids),
            ))
    inserted = OptionIvContextRepository().persist(records) if args.apply else 0
    report = {
        "generated_at": observed_at,
        "status": (
            "APPLIED"
            if args.apply
            else (
                "VALIDATION_PREVIOUS_COHORT"
                if args.validation_latest_complete
                else "DRY_RUN"
            )
        ),
        "active_configuration_sha256": configuration.configuration_sha256,
        "active_policy_sha256": configuration.policy_sha256,
        "maximum_absolute_log_moneyness": maximum_absolute_log_moneyness,
        "source_configuration_sha256s": sorted(
            set(source_configuration_by_matrix.values())
        ),
        "source_policy_sha256s": sorted(set(source_policy_by_matrix.values())),
        "matrices": len(matrices),
        "contexts": len(records),
        "inserted": inserted,
        "ready": sum(not row.statistics.null_reason_codes for row in records),
        "reasons": {
            reason: sum(reason in row.statistics.null_reason_codes for row in records)
            for reason in sorted({
                reason for row in records for reason in row.statistics.null_reason_codes
            })
        },
        "historical_diagnostics": historical_diagnostics,
        "underlyings": [
            {
                "underlying": row.underlying,
                "expiration_bucket": row.expiration_bucket,
                "source_configuration_sha256": (
                    source_configuration_by_matrix[row.matrix_id]
                ),
                "source_policy_sha256": source_policy_by_matrix[row.matrix_id],
                "current_comparable_iv": row.statistics.current_comparable_iv,
                "lookback_start_date": row.statistics.lookback_start_date,
                "lookback_end_date": row.statistics.lookback_end_date,
                "sample_count": row.statistics.sample_count,
                "coverage_fraction": row.statistics.coverage_fraction,
                "minimum_iv": row.statistics.minimum_iv,
                "maximum_iv": row.statistics.maximum_iv,
                "range_position_rank": row.statistics.range_position_rank,
                "empirical_percentile": row.statistics.empirical_percentile,
                "null_reason_codes": row.statistics.null_reason_codes,
                "rate_observation_count": len(row.rate_observation_ids),
                "dividend_coverage_count": len(row.dividend_coverage_ids),
                "dividend_action_count": len(row.dividend_action_ids),
            }
            for row in records
        ],
    }
    rendered = json.dumps(
        report,
        indent=2,
        sort_keys=True,
        default=str,
        allow_nan=False,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())