#!/usr/bin/env python3
"""Score realized-volatility forecasts against the baselines they must beat.

Read-only. Reads canonical daily bars, builds four variance estimators plus Yang-Zhang,
fits HAR in a walk-forward loop with a strict observability embargo, and compares the
forecast against a random walk, against the trailing 20-session close-to-close
volatility the portal already reports, and against expanding-window climatology.

Climatology is the baseline that matters. A constant-volatility control arm is run on
every invocation precisely because HAR beats the noisy trailing baselines by ~50% even
when the data-generating process has no predictability at all. Only the margin over
climatology, net of the margin the control arm shows, is evidence of skill.

The decision this informs: whether the platform can produce a physical-measure
volatility forecast at all. Every option gate today is computed under the risk-neutral
measure and therefore cannot identify a rich or cheap option. If HAR does not beat
climatology out of sample, the implied-versus-realized angle dies here, before any
option-history backfill is paid for.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from options.analytics.realized_volatility import (  # noqa: E402
    BarSeries,
    DailyBar,
    ForecastSample,
    VarianceEstimator,
    build_bar_series,
    combined_daily_variance,
    daily_variance,
    diebold_mariano,
    forward_realized_volatility,
    intraday_open_to_close_variance,
    mincer_zarnowitz,
    out_of_sample_r2,
    qlike,
    relative_improvement,
    rolling_volatility,
    session_gap_mask,
    walk_forward_har,
    yang_zhang_volatility,
)

DEFAULT_OUTPUT = BACKEND_DIR.parent / "docs" / "realized_volatility_forecast.json"
MINIMUM_DAILY_SESSIONS = 300

# Mirrors the canonical ranking in the equity_canonical_bars view for daily bars, so the
# study cannot silently score a different bar than production reads.
SQL_DAILY_BARS = """
    SELECT DISTINCT ON (ticker, session_date)
           ticker, session_date, open_price, high_price, low_price, close_price
    FROM equity_bar_revisions
    WHERE interval = '1d'
      AND is_final
      AND session_scope = 'RTH'
      AND adjusted = %s
      AND ticker = ANY(%s)
    ORDER BY ticker, session_date,
             CASE source_kind
                 WHEN 'RECONCILED' THEN 0
                 WHEN 'DERIVED' THEN 1
                 WHEN 'NATIVE_REST' THEN 2
                 ELSE 4
             END,
             COALESCE(replay_available_at, system_observed_at) DESC,
             created_at DESC
"""

# The view ranks native REST above the stream for sub-hourly bars; mirrored here.
SQL_INTRADAY_BARS = """
    SELECT DISTINCT ON (ticker, bar_start)
           ticker, session_date, bar_start, open_price, close_price
    FROM equity_bar_revisions
    WHERE interval = %s
      AND is_final
      AND session_scope = 'RTH'
      AND ticker = ANY(%s)
    ORDER BY ticker, bar_start,
             CASE source_kind
                 WHEN 'RECONCILED' THEN 0
                 WHEN 'NATIVE_REST' THEN 1
                 WHEN 'REALTIME_STREAM' THEN 2
                 ELSE 4
             END,
             COALESCE(replay_available_at, system_observed_at) DESC,
             created_at DESC
"""

SQL_UNIVERSE = """
    SELECT DISTINCT ticker FROM option_universe_members ORDER BY ticker
"""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", default=None, help="Comma-separated; defaults to the option universe.")
    parser.add_argument("--horizons", default="7,21,45", help="Forecast horizons in sessions.")
    parser.add_argument("--minimum-training", type=int, default=252)
    parser.add_argument("--refit-every", type=int, default=21)
    parser.add_argument(
        "--intraday-interval",
        default="30m",
        help="Bar interval used for intraday realized variance.",
    )
    parser.add_argument(
        "--fidelity-interval",
        default="5m",
        help="Finer interval compared against --intraday-interval on overlapping sessions.",
    )
    parser.add_argument(
        "--control-paths",
        type=int,
        default=13,
        help="Constant-volatility control series; 0 disables the control arm.",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


# Roughly the share of daily variance that arrives outside the session for large caps.
_CONTROL_OVERNIGHT_VARIANCE_SHARE = 0.3
_CONTROL_INTRADAY_BARS = 13


def _control_series(sessions: int, daily_sigma: float, seed: int) -> tuple[BarSeries, np.ndarray]:
    """Constant-volatility paths with a consistent intraday leg.

    The intraday path is generated from the same process as the daily bar, so the
    control arm is valid for the intraday estimator too rather than only for the
    open/high/low/close ones.
    """
    generator = np.random.default_rng(seed)
    overnight_sigma = daily_sigma * math.sqrt(_CONTROL_OVERNIGHT_VARIANCE_SHARE)
    step_sigma = daily_sigma * math.sqrt(
        (1.0 - _CONTROL_OVERNIGHT_VARIANCE_SHARE) / _CONTROL_INTRADAY_BARS
    )
    start = date(2020, 1, 1)
    bars: list[DailyBar] = []
    open_to_close = np.full(sessions, np.nan)
    close = 100.0
    for index in range(sessions):
        open_price = close * math.exp(float(generator.normal(0.0, overnight_sigma)))
        steps = generator.normal(0.0, step_sigma, _CONTROL_INTRADAY_BARS)
        path = open_price * np.exp(np.cumsum(steps))
        close = float(path[-1])
        bars.append(
            DailyBar(
                session_date=start + timedelta(days=index),
                open_price=open_price,
                high_price=float(max(open_price, path.max())),
                low_price=float(min(open_price, path.min())),
                close_price=close,
            )
        )
        open_to_close[index] = intraday_open_to_close_variance(
            open_price, tuple(float(value) for value in path)
        )
    return build_bar_series(f"CONTROL_{seed}", tuple(bars)), open_to_close


def _load_series(tickers: tuple[str, ...]) -> tuple[dict[str, BarSeries], dict[str, dict[str, Any]]]:
    """Load one comparable adjusted-bar lineage and report excluded tickers."""
    collected: dict[str, dict[bool, list[DailyBar]]] = {ticker: {True: [], False: []} for ticker in tickers}
    with get_db_cursor() as cursor:
        for adjusted in (True, False):
            cursor.execute(SQL_DAILY_BARS, (adjusted, list(tickers)))
            for row in cursor.fetchall():
                collected[row["ticker"]][adjusted].append(
                    DailyBar(
                        session_date=row["session_date"],
                        open_price=float(row["open_price"]),
                        high_price=float(row["high_price"]),
                        low_price=float(row["low_price"]),
                        close_price=float(row["close_price"]),
                    )
                )

    series: dict[str, BarSeries] = {}
    provenance: dict[str, dict[str, Any]] = {}
    for ticker in tickers:
        adjusted_bars = collected[ticker][True]
        unadjusted_bars = collected[ticker][False]
        if len(adjusted_bars) < MINIMUM_DAILY_SESSIONS:
            provenance[ticker] = {
                "status": "EXCLUDED_INSUFFICIENT_ADJUSTED_HISTORY",
                "adjusted": True,
                "adjusted_sessions": len(adjusted_bars),
                "unadjusted_sessions": len(unadjusted_bars),
                "minimum_sessions": MINIMUM_DAILY_SESSIONS,
            }
            continue
        built = build_bar_series(ticker, tuple(adjusted_bars))
        series[ticker] = built
        provenance[ticker] = {
            "status": "INCLUDED",
            "adjusted": True,
            "sessions": len(built),
            "session_gaps": int(session_gap_mask(built).sum()),
            "first_session": built.sessions[0].isoformat(),
            "last_session": built.sessions[-1].isoformat(),
        }
    return series, provenance


def _intraday_open_to_close(
    interval: str, series_by_ticker: dict[str, BarSeries]
) -> dict[str, np.ndarray]:
    """Per-session intraday path variance, aligned index-for-index with each daily series."""
    paths: dict[str, dict[date, list[tuple[Any, float, float]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    with get_db_cursor() as cursor:
        cursor.execute(SQL_INTRADAY_BARS, (interval, list(series_by_ticker)))
        for row in cursor.fetchall():
            paths[row["ticker"]][row["session_date"]].append(
                (row["bar_start"], float(row["open_price"]), float(row["close_price"]))
            )

    aligned: dict[str, np.ndarray] = {}
    for ticker, series in series_by_ticker.items():
        by_session = paths.get(ticker, {})
        values = np.full(len(series), np.nan)
        for index, session in enumerate(series.sessions):
            bars = by_session.get(session)
            if not bars:
                continue
            bars.sort(key=lambda item: item[0])
            values[index] = intraday_open_to_close_variance(
                bars[0][1], tuple(bar[2] for bar in bars)
            )
        aligned[ticker] = values
    return aligned


def _fidelity(
    coarse: dict[str, np.ndarray],
    fine: dict[str, np.ndarray],
    series_by_ticker: dict[str, BarSeries],
) -> dict[str, Any]:
    """How much of the finer interval's signal the coarser one reproduces.

    Answers whether backfilling the finer bars is worth the ingestion work: if the
    coarse series tracks the fine one closely there is nothing left to buy.
    """
    result: dict[str, Any] = {}
    for ticker, series in series_by_ticker.items():
        coarse_values = coarse.get(ticker)
        fine_values = fine.get(ticker)
        if coarse_values is None or fine_values is None:
            continue
        usable = (
            np.isfinite(coarse_values)
            & np.isfinite(fine_values)
            & (coarse_values > 0)
            & (fine_values > 0)
        )
        if int(usable.sum()) < 20:
            continue
        coarse_log = np.log(coarse_values[usable])
        fine_log = np.log(fine_values[usable])
        _, slope, r_squared = mincer_zarnowitz(fine_log, coarse_log)
        ratio = fine_values[usable] / coarse_values[usable]
        result[ticker] = {
            "overlapping_sessions": int(usable.sum()),
            "log_variance_correlation": float(np.corrcoef(coarse_log, fine_log)[0, 1]),
            "regression_slope": slope,
            "r_squared": r_squared,
            "median_fine_over_coarse_ratio": float(np.median(ratio)),
            "first_session": series.sessions[int(np.argmax(usable))].isoformat(),
        }
    return result


def _score(samples: tuple[ForecastSample, ...], horizon: int, stride: int = 1) -> dict[str, Any] | None:
    selected = samples[::stride]
    if len(selected) < 30:
        return None
    actual = np.array([sample.actual for sample in selected])
    har = np.array([sample.har for sample in selected])
    random_walk = np.array([sample.random_walk for sample in selected])
    trailing = np.array([sample.trailing_twenty for sample in selected])
    climatology = np.array([sample.climatology for sample in selected])

    loss_har = qlike(actual, har)
    loss_random_walk = qlike(actual, random_walk)
    loss_trailing = qlike(actual, trailing)
    loss_climatology = qlike(actual, climatology)
    # Overlapping windows leave the loss differential autocorrelated for horizon-1 lags.
    lag = 0 if stride >= horizon else horizon - 1
    statistic, p_value = diebold_mariano(loss_har, loss_climatology, lag)
    intercept, slope, mz_r2 = mincer_zarnowitz(actual, har)

    return {
        "observations": len(selected),
        "mean_actual_volatility": float(actual.mean()),
        "mse_har": float(((har - actual) ** 2).mean()),
        "mse_random_walk": float(((random_walk - actual) ** 2).mean()),
        "mse_trailing_twenty": float(((trailing - actual) ** 2).mean()),
        "mse_climatology": float(((climatology - actual) ** 2).mean()),
        "rmse_har": float(np.sqrt(((har - actual) ** 2).mean())),
        "rmse_random_walk": float(np.sqrt(((random_walk - actual) ** 2).mean())),
        "rmse_trailing_twenty": float(np.sqrt(((trailing - actual) ** 2).mean())),
        "rmse_climatology": float(np.sqrt(((climatology - actual) ** 2).mean())),
        "r2_vs_climatology": out_of_sample_r2(actual, har, climatology),
        "r2_vs_random_walk": out_of_sample_r2(actual, har, random_walk),
        "r2_vs_trailing_twenty": out_of_sample_r2(actual, har, trailing),
        "mincer_zarnowitz_intercept": intercept,
        "mincer_zarnowitz_slope": slope,
        "mincer_zarnowitz_r2": mz_r2,
        "bias_har": float((har - actual).mean()),
        "qlike_har": float(loss_har.mean()),
        "qlike_random_walk": float(loss_random_walk.mean()),
        "qlike_trailing_twenty": float(loss_trailing.mean()),
        "qlike_climatology": float(loss_climatology.mean()),
        "qlike_gain_vs_random_walk": relative_improvement(loss_har, loss_random_walk),
        "qlike_gain_vs_trailing_twenty": relative_improvement(loss_har, loss_trailing),
        "qlike_gain_vs_climatology": relative_improvement(loss_har, loss_climatology),
        "diebold_mariano_statistic": statistic,
        "diebold_mariano_p_value": p_value,
    }


def _yang_zhang_reference(series: BarSeries, horizon: int) -> dict[str, Any] | None:
    """Score Yang-Zhang as a level forecast: today's YZ vol as the forecast of forward vol."""
    level = yang_zhang_volatility(series, 20)
    actual = forward_realized_volatility(series, horizon)
    usable = np.isfinite(level) & np.isfinite(actual)
    if int(usable.sum()) < 60:
        return None
    loss = qlike(actual[usable], level[usable])
    close_to_close = rolling_volatility(daily_variance(series, VarianceEstimator.CLOSE_TO_CLOSE), 20)
    baseline_usable = usable & np.isfinite(close_to_close)
    baseline_loss = qlike(actual[baseline_usable], close_to_close[baseline_usable])
    return {
        "observations": int(usable.sum()),
        "qlike_yang_zhang": float(loss.mean()),
        "qlike_close_to_close": float(baseline_loss.mean()),
        "qlike_gain_vs_close_to_close": relative_improvement(loss, baseline_loss),
    }


def _pool(per_ticker: dict[str, tuple[ForecastSample, ...]]) -> tuple[ForecastSample, ...]:
    pooled: list[ForecastSample] = []
    for samples in per_ticker.values():
        pooled.extend(samples)
    return tuple(pooled)


def _control_variance(
    label: str, bars: BarSeries, open_to_close: np.ndarray
) -> np.ndarray | None:
    """The control arm must use the same variance construction as the arm it scores."""
    if not label.startswith("INTRADAY_"):
        return None
    if label.endswith("_OTC"):
        return np.where(open_to_close > 0, open_to_close, np.nan)
    return combined_daily_variance(bars, open_to_close)


def main() -> int:
    args = _parse_args()
    horizons = tuple(int(value) for value in args.horizons.split(",") if value.strip())

    if args.tickers:
        tickers = tuple(value.strip().upper() for value in args.tickers.split(",") if value.strip())
    else:
        with get_db_cursor() as cursor:
            cursor.execute(SQL_UNIVERSE)
            tickers = tuple(row["ticker"] for row in cursor.fetchall())

    series, provenance = _load_series(tickers)
    if not series:
        print("No ticker had enough daily history.")
        return 1

    print(f"Loaded {len(series)} tickers")
    for ticker, detail in sorted(provenance.items()):
        if detail["status"] != "INCLUDED":
            print(
                f"  {ticker:<6} excluded: {detail['adjusted_sessions']} adjusted"
                f" sessions; needs {detail['minimum_sessions']}"
            )
            continue
        gaps = f"  gaps={detail['session_gaps']}" if detail["session_gaps"] else ""
        print(
            f"  {ticker:<6} {detail['sessions']:>5} sessions"
            f"  {detail['first_session']} .. {detail['last_session']}  [adjusted]{gaps}"
        )

    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "minimum_training": args.minimum_training,
        "refit_every": args.refit_every,
        "intraday_interval": args.intraday_interval,
        "bar_provenance": provenance,
        "horizons": {},
    }

    intraday_paths = _intraday_open_to_close(args.intraday_interval, series)
    covered = {
        ticker: int(np.isfinite(values).sum()) for ticker, values in intraday_paths.items()
    }
    print(
        f"\nIntraday {args.intraday_interval} sessions per ticker:"
        f" min {min(covered.values())}, median {int(np.median(list(covered.values())))},"
        f" max {max(covered.values())}"
    )

    fine_paths = _intraday_open_to_close(args.fidelity_interval, series)
    fidelity = _fidelity(intraday_paths, fine_paths, series)
    report["interval_fidelity"] = {
        "coarse_interval": args.intraday_interval,
        "fine_interval": args.fidelity_interval,
        "by_ticker": fidelity,
    }
    if fidelity:
        correlations = [item["log_variance_correlation"] for item in fidelity.values()]
        ratios = [item["median_fine_over_coarse_ratio"] for item in fidelity.values()]
        print(
            f"{args.intraday_interval} vs {args.fidelity_interval} on overlapping sessions:"
            f" log-variance correlation median {np.median(correlations):.3f}"
            f" (min {min(correlations):.3f}),"
            f" median {args.fidelity_interval}/{args.intraday_interval} variance ratio"
            f" {np.median(ratios):.3f}"
        )

    for horizon in horizons:
        print(f"\n=== horizon {horizon} sessions ===")
        horizon_report: dict[str, Any] = {"estimators": {}, "control": {}, "yang_zhang_level": {}}
        control = [_control_series(1254, 0.015, seed) for seed in range(args.control_paths)]

        def run(
            label: str,
            variance_by_ticker: dict[str, np.ndarray] | None,
            estimator: VarianceEstimator | None,
        ) -> None:
            per_ticker_samples = {
                ticker: walk_forward_har(
                    bars,
                    estimator,
                    horizon,
                    minimum_training=args.minimum_training,
                    refit_every=args.refit_every,
                    variance=None if variance_by_ticker is None else variance_by_ticker[ticker],
                )
                for ticker, bars in series.items()
                if variance_by_ticker is None or ticker in variance_by_ticker
            }
            pooled = _pool(per_ticker_samples)
            overlapping = _score(pooled, horizon)
            independent = _score(pooled, horizon, stride=horizon)
            horizon_report["estimators"][label] = {
                "pooled_overlapping": overlapping,
                "pooled_independent": independent,
                "by_ticker": {
                    ticker: _score(samples, horizon)
                    for ticker, samples in per_ticker_samples.items()
                },
            }

            control_samples = _pool(
                {
                    bars.ticker: walk_forward_har(
                        bars,
                        estimator,
                        horizon,
                        minimum_training=args.minimum_training,
                        refit_every=args.refit_every,
                        variance=_control_variance(label, bars, control_open_to_close),
                    )
                    for bars, control_open_to_close in control
                }
            )
            control_score = _score(control_samples, horizon) if control_samples else None
            horizon_report["control"][label] = control_score

            if not (overlapping and independent):
                print(f"  {label:<16} insufficient samples")
                return
            control_gain = (
                control_score["qlike_gain_vs_climatology"] if control_score else float("nan")
            )
            print(
                f"  {label:<16} n={overlapping['observations']:>5}"
                f"  QLIKE har={overlapping['qlike_har']:.4f}"
                f" clim={overlapping['qlike_climatology']:.4f}"
                f" rw={overlapping['qlike_random_walk']:.4f}"
                f" hv20={overlapping['qlike_trailing_twenty']:.4f}"
            )
            print(
                f"  {'':<16} gain vs climatology="
                f"{overlapping['qlike_gain_vs_climatology']:+.1%}"
                f"   control arm={control_gain:+.1%}"
                f"   DM p={independent['diebold_mariano_p_value']:.4f} (independent)"
            )
            print(
                f"  {'':<16} R2 vs clim={overlapping['r2_vs_climatology']:+.3f}"
                f"  vs rw={overlapping['r2_vs_random_walk']:+.3f}"
                f"  MSE har={overlapping['mse_har']:.5f}"
                f" clim={overlapping['mse_climatology']:.5f}"
                f"  MZ slope={overlapping['mincer_zarnowitz_slope']:.2f}"
                f" R2={overlapping['mincer_zarnowitz_r2']:.3f}"
            )

        for estimator in VarianceEstimator:
            run(estimator.value, None, estimator)

        intraday_label = args.intraday_interval.upper()
        eligible = {
            ticker: values
            for ticker, values in intraday_paths.items()
            if np.isfinite(values).sum() >= args.minimum_training + horizon
        }
        if eligible:
            # Summed with the overnight gap the estimator inherits that gap's single-return
            # noise, so the open-to-close leg is scored on its own as well.
            run(
                f"INTRADAY_{intraday_label}",
                {
                    ticker: combined_daily_variance(series[ticker], values)
                    for ticker, values in eligible.items()
                },
                None,
            )
            run(
                f"INTRADAY_{intraday_label}_OTC",
                {
                    ticker: np.where(values > 0, values, np.nan)
                    for ticker, values in eligible.items()
                },
                None,
            )

        for ticker, bars in series.items():
            scored = _yang_zhang_reference(bars, horizon)
            if scored:
                horizon_report["yang_zhang_level"][ticker] = scored

        report["horizons"][str(horizon)] = horizon_report

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, default=str, allow_nan=False),
        encoding="utf-8",
    )
    print(f"\nWrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
