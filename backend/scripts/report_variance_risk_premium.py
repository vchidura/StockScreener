#!/usr/bin/env python3
"""Measure matched-horizon variance risk premium from retained daily option facts.

This is a research study, not a strategy backtest. It asks whether historical ATM implied
volatility can be matched to the existing physical-measure RV forecast and whether the
forecasted premium agrees with the subsequently realized premium. Strategy graduation
still requires structure-level P&L against a valid naive control.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from options.analytics.greeks import OptionValuationInput, solve_local_greeks  # noqa: E402
from options.analytics.realized_volatility import (  # noqa: E402
    DailyBar,
    build_bar_series,
    intraday_open_to_close_variance,
    walk_forward_har,
)
from options.analytics.volatility_risk_premium import (  # noqa: E402
    ContractIvObservation,
    calculate_variance_risk_premium,
    interpolate_total_variance,
    summarize_expiration_atm_iv,
)
from options.calendar import OptionExchangeCalendar  # noqa: E402
from options.config import (  # noqa: E402
    load_option_runtime_configuration,
    load_volatility_forecast_policy,
)
from options.domain import ContractType  # noqa: E402

DEFAULT_OUTPUT = BACKEND_DIR.parent / "docs" / "variance_risk_premium_study.json"
POLICY_PATH = BACKEND_DIR / "options" / "policies" / "vol_forecast_policy_v1.json"

SQL_DAILY_BARS = """
SELECT DISTINCT ON (session_date)
       session_date, open_price, high_price, low_price, close_price
FROM equity_bar_revisions
WHERE ticker = %s
  AND interval = '1d'
  AND is_final
  AND session_scope = 'RTH'
  AND adjusted = %s
ORDER BY session_date,
         CASE source_kind
             WHEN 'RECONCILED' THEN 0
             WHEN 'DERIVED' THEN 1
             WHEN 'NATIVE_REST' THEN 2
             ELSE 4
         END,
         COALESCE(replay_available_at, system_observed_at) DESC,
         created_at DESC
"""

SQL_INTRADAY_BARS = """
SELECT DISTINCT ON (bar_start)
       session_date, bar_start, open_price, close_price
FROM equity_bar_revisions
WHERE ticker = %s
  AND interval = %s
  AND is_final
  AND session_scope = 'RTH'
ORDER BY bar_start,
         CASE source_kind
             WHEN 'RECONCILED' THEN 0
             WHEN 'NATIVE_REST' THEN 1
             WHEN 'REALTIME_STREAM' THEN 2
             ELSE 4
         END,
         COALESCE(replay_available_at, system_observed_at) DESC,
         created_at DESC
"""

SQL_OPTION_MARKS = """
SELECT f.settlement_session,
       f.mark_close,
       f.mark_source,
       v.contract_type,
       v.strike,
             v.expiration_date,
             v.valid_from > (f.settlement_session + INTERVAL '1 day')
                     AS reference_reconstructed
FROM option_daily_contract_facts f
JOIN LATERAL (
        SELECT contract_type, strike, expiration_date, valid_from
    FROM option_contract_catalog_versions
    WHERE contract_id = f.contract_id
      AND contract_type IS NOT NULL
        ORDER BY
                CASE WHEN valid_from <= (f.settlement_session + INTERVAL '1 day')
                         THEN 0 ELSE 1 END,
                valid_from DESC
    LIMIT 1
) v ON TRUE
WHERE f.underlying = %s
  AND f.mark_close IS NOT NULL
ORDER BY f.settlement_session, v.expiration_date, v.strike
"""

SQL_DIVIDENDS = """
SELECT effective_date, cash_amount
FROM equity_corporate_actions
WHERE ticker = %s
  AND action_type = 'DIVIDEND'
  AND cash_amount IS NOT NULL
ORDER BY effective_date
"""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--underlyer", default="SPY")
    parser.add_argument("--horizons", default="7,21,45")
    parser.add_argument("--maximum-log-moneyness", type=float, default=0.03)
    parser.add_argument("--contracts-per-side", type=int, default=2)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def _daily_series(ticker: str, adjusted: bool):
    with get_db_cursor() as cursor:
        cursor.execute(SQL_DAILY_BARS, (ticker, adjusted))
        rows = cursor.fetchall()
    return build_bar_series(
        ticker,
        tuple(
            DailyBar(
                session_date=row["session_date"],
                open_price=float(row["open_price"]),
                high_price=float(row["high_price"]),
                low_price=float(row["low_price"]),
                close_price=float(row["close_price"]),
            )
            for row in rows
        ),
    )


def _intraday_variance(ticker: str, interval: str, sessions: tuple[date, ...]) -> np.ndarray:
    with get_db_cursor() as cursor:
        cursor.execute(SQL_INTRADAY_BARS, (ticker, interval))
        rows = cursor.fetchall()
    paths: dict[date, list[tuple[datetime, float, float]]] = defaultdict(list)
    for row in rows:
        paths[row["session_date"]].append(
            (row["bar_start"], float(row["open_price"]), float(row["close_price"]))
        )
    variance = np.full(len(sessions), np.nan)
    for index, session in enumerate(sessions):
        bars = sorted(paths.get(session, ()), key=lambda item: item[0])
        if not bars:
            continue
        variance[index] = intraday_open_to_close_variance(
            bars[0][1], tuple(bar[2] for bar in bars)
        )
    return np.where(variance > 0, variance, np.nan)


def _trailing_dividend_yields(
    ticker: str,
    sessions: tuple[date, ...],
    spots: dict[date, float],
    fallback: float,
) -> tuple[dict[date, float], int]:
    with get_db_cursor() as cursor:
        cursor.execute(SQL_DIVIDENDS, (ticker,))
        dividends = tuple(
            (row["effective_date"], float(row["cash_amount"]))
            for row in cursor.fetchall()
        )
    result: dict[date, float] = {}
    covered = 0
    for session in sessions:
        prior = [
            amount
            for effective, amount in dividends
            if 0 <= (session - effective).days < 365
        ]
        spot = spots.get(session)
        if prior and spot and spot > 0:
            result[session] = float(sum(prior) / spot)
            covered += 1
        else:
            result[session] = float(fallback)
    return result, covered


def _option_iv_points(
    ticker: str,
    spots: dict[date, float],
    dividend_yields: dict[date, float],
    risk_free_rate: float,
) -> tuple[dict[date, list[ContractIvObservation]], dict[str, int]]:
    with get_db_cursor() as cursor:
        cursor.execute(SQL_OPTION_MARKS, (ticker,))
        rows = cursor.fetchall()

    calendar = OptionExchangeCalendar()
    inputs: list[OptionValuationInput] = []
    metadata: list[tuple[date, date, ContractType, Decimal, Decimal, float]] = []
    skipped_no_spot = skipped_expired = 0
    reconstructed_references = 0
    mark_sources: set[str] = set()
    for row in rows:
        session = row["settlement_session"]
        spot = spots.get(session)
        if spot is None:
            skipped_no_spot += 1
            continue
        expiration = row["expiration_date"]
        if expiration <= session:
            skipped_expired += 1
            continue
        maturity = (
            calendar.expiration_cutoff(expiration) - calendar.session_close(session)
        ).total_seconds() / (365.0 * 24 * 3600)
        if maturity <= 0:
            skipped_expired += 1
            continue
        contract_type = ContractType(row["contract_type"])
        strike = Decimal(str(row["strike"]))
        spot_decimal = Decimal(str(spot))
        inputs.append(
            OptionValuationInput(
                contract_type=contract_type,
                spot=spot_decimal,
                strike=strike,
                model_mark=Decimal(str(row["mark_close"])),
                time_to_expiration_years=maturity,
                risk_free_rate=risk_free_rate,
                dividend_yield=dividend_yields[session],
            )
        )
        metadata.append(
            (session, expiration, contract_type, strike, spot_decimal, maturity)
        )
        reconstructed_references += bool(row["reference_reconstructed"])
        mark_sources.add(str(row["mark_source"]))

    by_session: dict[date, list[ContractIvObservation]] = defaultdict(list)
    converged = 0
    for result, meta in zip(solve_local_greeks(tuple(inputs)), metadata):
        if not result.converged or result.local_iv is None:
            continue
        session, expiration, contract_type, strike, spot, maturity = meta
        by_session[session].append(
            ContractIvObservation(
                expiration_date=expiration,
                maturity_years=maturity,
                contract_type=contract_type,
                strike=strike,
                spot=spot,
                implied_volatility=result.local_iv,
            )
        )
        converged += 1
    return by_session, {
        "mark_rows": len(rows),
        "solver_inputs": len(inputs),
        "solver_converged": converged,
        "skipped_no_spot": skipped_no_spot,
        "skipped_expired": skipped_expired,
        "historically_reconstructed_reference_rows": reconstructed_references,
        "mark_source_count": len(mark_sources),
    }


def _matched_iv(
    by_session: dict[date, list[ContractIvObservation]],
    horizons: tuple[int, ...],
    maximum_log_moneyness: float,
    contracts_per_side: int,
) -> tuple[dict[int, dict[date, Any]], dict[str, int]]:
    matched: dict[int, dict[date, Any]] = {horizon: {} for horizon in horizons}
    expiration_points = 0
    rejected_expirations = 0
    sessions_with_lower = {horizon: 0 for horizon in horizons}
    sessions_with_upper = {horizon: 0 for horizon in horizons}
    maximum_maturity = 0.0
    for session, contracts in by_session.items():
        by_expiration: dict[date, list[ContractIvObservation]] = defaultdict(list)
        for contract in contracts:
            by_expiration[contract.expiration_date].append(contract)
        points = []
        for rows in by_expiration.values():
            point = summarize_expiration_atm_iv(
                tuple(rows),
                maximum_absolute_log_moneyness=maximum_log_moneyness,
                contracts_per_side=contracts_per_side,
            )
            if point is None:
                rejected_expirations += 1
            else:
                points.append(point)
                expiration_points += 1
                maximum_maturity = max(maximum_maturity, point.maturity_years)
        for horizon in horizons:
            target = horizon / 252
            sessions_with_lower[horizon] += any(
                point.maturity_years <= target for point in points
            )
            sessions_with_upper[horizon] += any(
                point.maturity_years >= target for point in points
            )
            value = interpolate_total_variance(tuple(points), horizon)
            if value is not None:
                matched[horizon][session] = value
    return matched, {
        "sessions_with_contract_iv": len(by_session),
        "expiration_points": expiration_points,
        "rejected_expirations": rejected_expirations,
        "maximum_expiration_maturity_years": maximum_maturity,
        "sessions_with_lower_expiration": sessions_with_lower,
        "sessions_with_upper_expiration": sessions_with_upper,
    }


def _quintiles(rows: list[dict[str, float]]) -> list[dict[str, float | int]]:
    if len(rows) < 25:
        return []
    ordered = sorted(rows, key=lambda row: row["forecast_richness_ratio"])
    result = []
    for index, chunk in enumerate(np.array_split(np.asarray(ordered, dtype=object), 5), start=1):
        values = list(chunk)
        result.append({
            "quintile": index,
            "observations": len(values),
            "mean_forecast_richness_ratio": statistics.fmean(
                row["forecast_richness_ratio"] for row in values
            ),
            "mean_realized_variance_premium": statistics.fmean(
                row["realized_variance_premium"] for row in values
            ),
            "positive_realized_premium_rate": statistics.fmean(
                row["realized_variance_premium"] > 0 for row in values
            ),
        })
    return result


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"observations": 0}
    forecast = np.asarray([row["forecast_variance_premium"] for row in rows])
    climatology = np.asarray(
        [row["climatology_variance_premium"] for row in rows]
    )
    realized = np.asarray([row["realized_variance_premium"] for row in rows])
    correlation = (
        float(np.corrcoef(forecast, realized)[0, 1])
        if len(rows) > 2 and np.std(forecast) > 0 and np.std(realized) > 0
        else None
    )
    return {
        "observations": len(rows),
        "first_session": min(row["session"] for row in rows).isoformat(),
        "last_session": max(row["session"] for row in rows).isoformat(),
        "mean_implied_volatility": statistics.fmean(row["implied_volatility"] for row in rows),
        "mean_forecast_realized_volatility": statistics.fmean(
            row["forecast_realized_volatility"] for row in rows
        ),
        "mean_actual_realized_volatility": statistics.fmean(
            row["actual_realized_volatility"] for row in rows
        ),
        "mean_forecast_variance_premium": float(forecast.mean()),
        "mean_realized_variance_premium": float(realized.mean()),
        "median_forecast_richness_ratio": statistics.median(
            row["forecast_richness_ratio"] for row in rows
        ),
        "median_realized_richness_ratio": statistics.median(
            row["realized_richness_ratio"] for row in rows
        ),
        "forecast_positive_rate": float((forecast > 0).mean()),
        "climatology_positive_rate": float((climatology > 0).mean()),
        "realized_positive_rate": float((realized > 0).mean()),
        "premium_sign_accuracy": float(((forecast > 0) == (realized > 0)).mean()),
        "climatology_sign_accuracy": float(
            ((climatology > 0) == (realized > 0)).mean()
        ),
        "forecast_realized_premium_correlation": correlation,
        "premium_rmse": float(np.sqrt(np.mean((forecast - realized) ** 2))),
        "climatology_premium_rmse": float(
            np.sqrt(np.mean((climatology - realized) ** 2))
        ),
    }


def _score(rows: list[dict[str, Any]], horizon: int) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: row["session_index"])
    independent: list[dict[str, Any]] = []
    last_index = -horizon
    for row in ordered:
        if row["session_index"] - last_index < horizon:
            continue
        independent.append(row)
        last_index = row["session_index"]
    overlapping = _metrics(ordered)
    overlapping["richness_quintiles"] = _quintiles(ordered)
    return {
        "overlapping": overlapping,
        "independent": _metrics(independent),
        "confidence_status": (
            "MEASURABLE_SAMPLE" if len(independent) >= 40 else "INSUFFICIENT_INDEPENDENT_PERIODS"
        ),
    }


def main() -> int:
    args = _parse_args()
    ticker = args.underlyer.strip().upper()
    horizons = tuple(sorted({int(value) for value in args.horizons.split(",")}))
    configuration = load_option_runtime_configuration()
    forecast_artifact = load_volatility_forecast_policy(POLICY_PATH)
    policy = forecast_artifact.policy

    adjusted = _daily_series(ticker, adjusted=True)
    unadjusted = _daily_series(ticker, adjusted=False)
    spots = dict(zip(unadjusted.sessions, unadjusted.close_prices))
    dividends, dividend_covered = _trailing_dividend_yields(
        ticker,
        unadjusted.sessions,
        spots,
        float(configuration.settings.default_dividend_yield),
    )
    contract_iv, iv_input = _option_iv_points(
        ticker,
        spots,
        dividends,
        float(configuration.settings.risk_free_rate),
    )
    matched, match_counts = _matched_iv(
        contract_iv,
        horizons,
        args.maximum_log_moneyness,
        args.contracts_per_side,
    )

    variance = _intraday_variance(ticker, policy.intraday_interval, adjusted.sessions)
    study: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "underlying": ticker,
        "status": "PRELIMINARY_RESEARCH_ONLY",
        "forecast_policy_version": policy.forecast_policy_version,
        "forecast_policy_sha256": forecast_artifact.sha256,
        "iv_method": "LOCAL_BLACK_SCHOLES_ATM_CALL_PUT_MEDIAN_TOTAL_VARIANCE_INTERPOLATION",
        "maximum_absolute_log_moneyness": args.maximum_log_moneyness,
        "contracts_per_side": args.contracts_per_side,
        "mark_source": "PROVIDER_DAILY_AGGREGATE_ADJUSTED",
        "risk_free_rate": float(configuration.settings.risk_free_rate),
        "risk_free_rate_reason": "STATIC_CONFIGURATION_NOT_DATED_CURVE",
        "dividend_yield_method": "TRAILING_365_DAY_CASH_DIVIDENDS_OVER_UNADJUSTED_SPOT",
        "dividend_covered_sessions": dividend_covered,
        "option_mark_adjustment_reason": "EXISTING_BACKFILL_USED_ADJUSTED_TRUE",
        "graduation_blockers": [
            "STATIC_RISK_FREE_RATE",
            "OPTION_MARK_POLICY_NOT_VERSIONED",
            "SPY_ONLY_HISTORY",
            "NO_STRATEGY_PNL_LINK",
        ],
        "reference_semantics": (
            "IMMUTABLE_CONTRACT_TERMS_RECONSTRUCTED_WHERE_NOT_KNOWN_LIVE"
        ),
        "input_counts": {**iv_input, **match_counts},
        "horizons": {},
    }

    index_by_session = {session: index for index, session in enumerate(adjusted.sessions)}
    for horizon in horizons:
        samples = walk_forward_har(
            adjusted,
            None,
            horizon,
            minimum_training=policy.minimum_training_sessions,
            refit_every=policy.refit_every_sessions,
            variance=variance,
        )
        forecast_by_session = {sample.session_date: sample for sample in samples}
        rows: list[dict[str, Any]] = []
        for session, matched_iv in matched[horizon].items():
            sample = forecast_by_session.get(session)
            if sample is None or session not in index_by_session:
                continue
            premium = calculate_variance_risk_premium(
                matched_iv.implied_volatility,
                sample.har,
                sample.actual,
            )
            rows.append({
                "session": session,
                "session_index": index_by_session[session],
                "implied_volatility": premium.implied_volatility,
                "forecast_realized_volatility": premium.forecast_realized_volatility,
                "actual_realized_volatility": premium.actual_realized_volatility,
                "forecast_variance_premium": premium.forecast_variance_premium,
                "climatology_variance_premium": (
                    premium.implied_volatility**2 - sample.climatology**2
                ),
                "realized_variance_premium": premium.realized_variance_premium,
                "forecast_richness_ratio": premium.forecast_richness_ratio,
                "realized_richness_ratio": premium.realized_richness_ratio,
            })
        horizon_report = _score(rows, horizon)
        horizon_report["matched_iv_sessions"] = len(matched[horizon])
        horizon_report["rv_forecast_sessions"] = len(samples)
        study["horizons"][str(horizon)] = horizon_report

    args.output.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(study, indent=2, default=str, allow_nan=False)
    args.output.write_text(text, encoding="utf-8")
    print(text)
    print(f"\nWrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
