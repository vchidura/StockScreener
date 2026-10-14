"""Compute default latest daily scanner portal contracts outside request handlers."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

import pandas as pd

from screeners import (
    analyze_market_regime,
    calculate_fibonacci_swing_pct,
    scan_bearish_bounce,
    scan_fair_value_gaps,
    scan_fibonacci,
    scan_gap_strategies,
    scan_momentum_pullback,
    scan_moving_average_crossover,
)


def compute_default_scanner_snapshots(
    tickers: list[str],
    frames: Mapping[str, pd.DataFrame],
    index_frames: Mapping[str, pd.DataFrame],
    *,
    observed_at: datetime | None = None,
    interval: str = "1d",
) -> dict[str, Any]:
    if interval not in {"1d", "1h"}:
        raise ValueError("Default strategy snapshots support only daily and hourly inputs")
    observed = observed_at or datetime.now(timezone.utc)
    scan_datetime = observed.isoformat()
    gaps = []
    fvgs = []
    moving_average = []
    momentum = []
    bearish = []
    fibonacci = []
    for ticker in tickers:
        frame = frames.get(ticker)
        if frame is None:
            continue
        if len(frame) >= 20:
            gaps.extend(scan_gap_strategies(ticker, frame) if interval == "1d" else scan_gap_strategies(ticker, frame, interval=interval))
            fvgs.extend(scan_fair_value_gaps(ticker, frame, lookback=50))
        if len(frame) >= 26:
            result = scan_moving_average_crossover(
                ticker, frame, 9, 21, interval=interval
            )
            if result:
                result["interval"] = interval
                moving_average.append(result)
        if len(frame) >= (210 if interval == "1d" else 200):
            result = scan_momentum_pullback(ticker, frame, interval=interval)
            if result:
                momentum.append(result)
            result = scan_bearish_bounce(ticker, frame, interval=interval)
            if result:
                bearish.append(result)
        if len(frame) >= 50:
            result = scan_fibonacci(ticker, frame, 5.0 if interval == "1d" else calculate_fibonacci_swing_pct(frame, interval))
            if result:
                fibonacci.append(result)

    gaps_by_type = _group(gaps, "gap_type")
    fvgs_by_type = _group(fvgs, "fvg_type")
    ma_by_signal = _group(moving_average, "signal")
    for signal, values in ma_by_signal.items():
        if "Crossover" in signal or "Recent" in signal:
            values.sort(key=lambda row: row.get("days_since_cross") or 0)
        else:
            values.sort(key=lambda row: abs(row.get("ma_spread_pct") or 0), reverse=True)
    momentum.sort(key=lambda row: row["score"], reverse=True)
    bearish.sort(key=lambda row: row["score"], reverse=True)
    priority = {"Near": 0, "Between": 1, "Below": 2, "Above": 2}
    fibonacci.sort(key=lambda row: (
        priority.get(row["signal"].split()[0], 3), abs(row["distance_pct"])
    ))
    regime = analyze_market_regime(index_frames.get("SPY"), index_frames.get("QQQ"))
    total = len(tickers)
    snapshots = {
        "SCAN_GAPS_1D": {
            "scan_datetime": scan_datetime, "interval": "1d",
            "total_scanned": total, "total_signals": len(gaps),
            "results_by_type": gaps_by_type, "results": gaps,
        },
        "SCAN_FVG_1D_50": {
            "scan_datetime": scan_datetime, "interval": "1d", "lookback": 50,
            "total_scanned": total, "total_signals": len(fvgs),
            "results_by_type": fvgs_by_type, "results": fvgs,
        },
        "SCAN_MA_1D_9_21": {
            "scan_datetime": scan_datetime, "interval": "1d",
            "total_scanned": total, "total_signals": len(moving_average),
            "short_period": 9, "long_period": 21,
            "results_by_signal": ma_by_signal, "results": moving_average,
        },
        "SCAN_MOMENTUM_1D": {
            "scan_datetime": scan_datetime, "interval": "1d",
            "total_scanned": total, "total_signals": len(momentum),
            "results": momentum,
        },
        "SCAN_BEARISH_1D": {
            "scan_datetime": scan_datetime, "interval": "1d",
            "total_scanned": total, "total_signals": len(bearish),
            "results": bearish,
        },
        "SCAN_FIBONACCI_1D_5": {
            "scan_datetime": scan_datetime, "interval": "1d",
            "total_scanned": total, "total_signals": len(fibonacci),
            "min_swing_pct": 5.0, "results": fibonacci,
        },
        "SCAN_ALL_1D_5": {
            "scan_datetime": scan_datetime,
            "total_scanned": total,
            "market_regime": regime,
            "gaps": {"total_signals": len(gaps), "results": gaps},
            "ma_crossover": {
                "total_signals": len(moving_average), "results": moving_average
            },
            "momentum_pullback": {
                "total_signals": len(momentum), "results": momentum
            },
            "bearish_bounce": {
                "total_signals": len(bearish), "results": bearish
            },
            "fibonacci": {
                "total_signals": len(fibonacci), "results": fibonacci,
                "min_swing_pct": 5.0,
            },
        },
    }
    if interval == "1d":
        return snapshots
    hourly = {HOURLY_SCANNER_CONTRACTS[contract]: dict(snapshots[contract], interval="1h")
              for contract in DAILY_SCANNER_MINIMUM_BARS}
    hourly["SCAN_FIBONACCI_1H_ATR"].update(min_swing_pct=None, swing_policy="INTERVAL_AWARE_ATR_STRUCTURAL_CONFIRMED_LEG")
    return hourly


DAILY_SCANNER_MINIMUM_BARS = {
    "SCAN_GAPS_1D": 20, "SCAN_FVG_1D_50": 50, "SCAN_MA_1D_9_21": 26,
    "SCAN_MOMENTUM_1D": 210, "SCAN_BEARISH_1D": 210, "SCAN_FIBONACCI_1D_5": 50,
}
HOURLY_SCANNER_CONTRACTS = {contract: contract.replace("_1D", "_1H") for contract in DAILY_SCANNER_MINIMUM_BARS}
HOURLY_SCANNER_CONTRACTS["SCAN_FIBONACCI_1D_5"] = "SCAN_FIBONACCI_1H_ATR"
HOURLY_SCANNER_MINIMUM_BARS = {HOURLY_SCANNER_CONTRACTS[contract]: 200 if required == 210 else required
                               for contract, required in DAILY_SCANNER_MINIMUM_BARS.items()}


def default_scanner_analysis(tickers, frames, snapshots, *, unavailable_members=None, action_context=None, interval="1d"):
    analysis = {}
    unavailable = unavailable_members or {}
    minimum = DAILY_SCANNER_MINIMUM_BARS if interval == "1d" else HOURLY_SCANNER_MINIMUM_BARS
    for contract, required in minimum.items():
        members = []
        for ticker in tickers:
            frame = frames.get(ticker)
            count = 0 if frame is None else len(frame)
            matches = sum(row["ticker"] == ticker for row in snapshots[contract]["results"])
            status = ("UNAVAILABLE" if count == 0 else "INSUFFICIENT_HISTORY" if count < required
                      else "COMPLETE" if matches else "COMPLETE_EMPTY")
            reason = (f"{'DAILY' if interval == '1d' else 'HOURLY'}_INPUT_MISSING" if count == 0
                      else f"{'DAILY' if interval == '1d' else 'HOURLY'}_WARMUP_NOT_MET" if count < required else None)
            if ticker in unavailable:
                if matches:
                    raise ValueError("Unavailable scanner member cannot retain matches")
                status, reason = "UNAVAILABLE", unavailable[ticker]["reason"]
            members.append(dict(ticker=ticker, status=status, available_bars=count,
                                required_bars=required, total_signals=matches,
                                reason=reason))
            if action_context and ticker in action_context:
                members[-1]["corporate_actions"] = action_context[ticker]["action_refs"]
        evaluated = sum(row["status"] in {"COMPLETE", "COMPLETE_EMPTY"} for row in members)
        status = ("PARTIAL" if 0 < evaluated < len(members) else
                  "COMPLETE" if evaluated and snapshots[contract]["results"] else
                  "COMPLETE_EMPTY" if evaluated else
                  "INSUFFICIENT_HISTORY" if any(row["status"] == "INSUFFICIENT_HISTORY" for row in members) else "UNAVAILABLE")
        analysis[contract] = dict(status=status, total_evaluated=evaluated,
                                  total_unavailable=len(members) - evaluated, members=members)
    return analysis


def _group(rows: list[dict], key: str) -> dict[str, list[dict]]:
    result = {}
    for row in rows:
        result.setdefault(row[key], []).append(row)
    return result