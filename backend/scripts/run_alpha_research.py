"""
Run the cross-sectional alpha research loop and report whether any signal exists.

    python backend/scripts/run_alpha_research.py
    python backend/scripts/run_alpha_research.py --start 2022-03-08 --horizon 1

Reports per-feature IC, walk-forward model IC, decile monotonicity, and the
long/short spread against an always-long baseline.
"""
from __future__ import annotations

import argparse
import logging
import sys
import json
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from research.features import (  # noqa: E402
    FEATURE_COLUMNS,
    HOURLY_FEATURE_COLUMNS,
    RISK_COLUMNS,
    prepare_dataset,
)
from research.evaluate import (  # noqa: E402
    baseline_always_long,
    daily_ic,
    decile_table,
    feature_ic_scan,
    ic_summary,
    long_short_stats,
    neutralize,
    persist_run,
    purged_walk_forward,
    recent_runs,
    top_decile_stats,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("alpha-research")

pd.set_option("display.width", 140)
pd.set_option("display.float_format", lambda v: f"{v:,.4f}")


def _rule(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Cross-sectional alpha research")
    parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="End date YYYY-MM-DD")
    parser.add_argument("--horizon", type=int, default=1, help="Forward return horizon in days")
    parser.add_argument("--train-days", type=int, default=252)
    parser.add_argument("--test-days", type=int, default=21)
    parser.add_argument("--embargo-days", type=int, default=2)
    parser.add_argument("--alpha", type=float, default=10.0, help="Ridge penalty")
    parser.add_argument("--rolling", action="store_true", help="Rolling window instead of expanding")
    parser.add_argument("--cost-bps", type=float, default=2.0,
                        help="One-way transaction cost in bps applied to traded notional")
    parser.add_argument("--rebalance-days", type=int, default=1,
                        help="Hold period; 1 = rebalance daily")
    parser.add_argument("--features", default=None,
                        help="Comma-separated feature subset (default: all daily)")
    parser.add_argument("--hourly", action="store_true",
                        help="Include intraday features derived from hourly bars")
    parser.add_argument(
        "--activity-filter",
        choices=["none", "liquidity", "composite"],
        default="none",
        help="Restrict each test cross-section to its top half by activity",
    )
    parser.add_argument(
        "--model", choices=["ridge", "lgbm"], default="ridge",
        help="ridge (default, linear) or lgbm (gradient-boosted trees, requires pip install lightgbm)",
    )
    parser.add_argument("--no-log", action="store_true",
                        help="Do not record this run in research_runs")
    args = parser.parse_args()

    available = list(FEATURE_COLUMNS) + (HOURLY_FEATURE_COLUMNS if args.hourly else [])
    if args.features:
        selected = [f.strip() for f in args.features.split(",") if f.strip()]
        unknown = [f for f in selected if f not in available]
        if unknown:
            logger.error("Unknown features: %s (use --hourly for intraday ones)", unknown)
            return 1
    else:
        selected = available

    ret_col = f"fwd_ret_{args.horizon}"

    _rule("1. BUILD DATASET (point-in-time)")
    df = prepare_dataset(args.start, args.end, horizon=args.horizon,
                         include_hourly=args.hourly)
    if df.empty:
        logger.error("No data available.")
        return 1
    print(f"rows={len(df):,}  days={df['date'].nunique():,}  tickers={df['ticker'].nunique():,}")
    print(f"range={df['date'].min().date()} .. {df['date'].max().date()}")
    print(f"model features ({len(selected)}): {', '.join(selected)}")

    _rule("2. STANDALONE FEATURE IC (vs forward return)")
    print(feature_ic_scan(df, selected, ret_col).to_string())
    print("\nReference: a genuine daily equity feature sits around |IC| 0.01-0.04.")
    _rule(f"3. WALK-FORWARD {args.model.upper()} (purged, no look-ahead)")
    preds = purged_walk_forward(
        df,
        selected,
        label_col="label",
        train_days=args.train_days,
        test_days=args.test_days,
        embargo_days=args.embargo_days,
        alpha=args.alpha,
        expanding=not args.rolling,
        carry_cols=RISK_COLUMNS + ["sector", "volume_ratio", "vol_ratio", "range_pct"],
        model=args.model,
    )
    if preds.empty:
        logger.error("Walk-forward produced no predictions.")
        return 1

    if args.activity_filter != "none":
        if args.activity_filter == "liquidity":
            activity = preds.groupby("date")["liquidity"].rank(pct=True)
        else:
            activity_cols = ["volume_ratio", "vol_ratio", "range_pct"]
            activity = preds.groupby("date")[activity_cols].rank(pct=True).mean(axis=1)
            activity = activity.groupby(preds["date"]).rank(pct=True)
        preds = preds[activity > 0.5].copy()
        print(f"Activity universe: top half by {args.activity_filter} "
              f"({preds.groupby('date').size().mean():.0f} names/day)")

    # Residualise inside the eligible universe; filtering afterward would leave
    # factor and sector means defined by names the strategy cannot hold.
    preds = neutralize(preds, "pred", factor_cols=RISK_COLUMNS, group_col="sector")
    n_sectors = preds["sector"].nunique() if "sector" in preds.columns else 0
    print(f"Neutralised against {RISK_COLUMNS}"
          + (f" and {n_sectors} sectors" if n_sectors > 1 else " (no sector data)"))

    # A h-day label observed daily overlaps h-1 times, which inflates the IC
    # t-stat by roughly sqrt(h). Sampling every h days restores independence.
    step = max(args.rebalance_days, args.horizon)
    rebalance_dates = sorted(preds["date"].unique())[::step]
    traded = preds[preds["date"].isin(rebalance_dates)]
    periods_per_year = 252 / step

    raw_ic = ic_summary(daily_ic(traded, "pred", ret_col))
    ic_report = pd.DataFrame(
        [
            {"sample": f"raw, all days (overlap x{step})", **ic_summary(daily_ic(preds, "pred", ret_col))},
            {"sample": "raw, non-overlapping", **raw_ic},
            {"sample": "neutralised, non-overlapping", **ic_summary(daily_ic(traded, "pred_neutral", ret_col))},
        ]
    ).set_index("sample")
    print(ic_report.to_string())
    print("\nThe neutralised row is the alpha claim; the rest include risk exposure.")
    indep_ic = ic_summary(daily_ic(traded, "pred_neutral", ret_col))

    _rule("4. DECILE MONOTONICITY (neutralised, non-overlapping)")
    table = decile_table(traded, "pred_neutral", ret_col, periods_per_year=periods_per_year)
    print(table.to_string())
    spread = table["mean_ret_bps"].iloc[-1] - table["mean_ret_bps"].iloc[0]
    print(f"\nD10 - D1 spread: {spread:,.2f} bps per {step}-day period")

    _rule("5. STRATEGY vs BASELINE (net of costs)")
    print(f"Rebalancing every {step} days -> {len(rebalance_dates):,} independent periods")
    ls_raw = long_short_stats(traded, "pred", ret_col, cost_bps=args.cost_bps,
                              periods_per_year=periods_per_year)
    ls = long_short_stats(traded, "pred_neutral", ret_col, cost_bps=args.cost_bps,
                          periods_per_year=periods_per_year)
    top = top_decile_stats(traded, "pred_neutral", ret_col, cost_bps=args.cost_bps,
                           periods_per_year=periods_per_year)
    base = baseline_always_long(traded, ret_col, periods_per_year=periods_per_year)
    comparison = pd.DataFrame(
        [
            {"strategy": "L/S raw", **ls_raw},
            {"strategy": "L/S neutralised", **ls},
            {"strategy": "Always long (equal wt)", **base},
        ]
    ).set_index("strategy")
    print(comparison.to_string())
    print(f"\nCost assumption: {args.cost_bps} bps one-way on traded notional.")
    print(f"L/S turnover: {ls['turnover']:.2f}x gross per rebalance "
          f"(4.0 = full rotation of both sides).")
    print(
        f"Top-decile LONG: {top['net_ann_pct']:.1f}%/yr, "
        f"Sharpe {top['net_sharpe']:.2f}; "
        f"alpha vs eligible universe {top['alpha_ann_pct']:.1f}%/yr, "
        f"t={top['alpha_t_stat']:.2f}"
    )

    _rule("6. VERDICT")
    t = indep_ic["t_stat"]
    base_sharpe = base["net_sharpe"] or 0
    checks = [
        ("IC t-stat > 2 (neutralised)", t == t and t > 2),
        ("IC mean > 0.005 (neutralised)", indep_ic["ic_mean"] > 0.005),
        ("Deciles monotone-ish (D10 > D1)", spread > 0),
        ("Net L/S Sharpe > always-long Sharpe", (ls["net_sharpe"] or 0) > base_sharpe),
        ("Net L/S return positive", (ls["net_ann_pct"] or 0) > 0),
    ]
    for label, ok in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}")

    raw_beat = (ls_raw["net_sharpe"] or 0) > base_sharpe
    neutral_beat = (ls["net_sharpe"] or 0) > base_sharpe
    passed = all(ok for _, ok in checks)
    n_passed = sum(1 for _, ok in checks if ok)
    # Economics can be convincing while the t-stat is still starved of periods.
    economics_ok = neutral_beat and (ls["net_ann_pct"] or 0) > 0 and spread > 0
    underpowered = not passed and economics_ok and len(rebalance_dates) < 60

    print()
    if passed:
        print("ALPHA: survives neutralisation and costs. Capacity is the next gate.")
    elif underpowered:
        print("PROMISING BUT UNDERPOWERED.")
        print(f"  {n_passed}/{len(checks)} checks pass and the economics hold:")
        print(f"    neutralised {ls['net_ann_pct']:.1f}%/yr, Sharpe {ls['net_sharpe']:.2f}")
        print(f"    vs always long {base['net_ann_pct']:.1f}%/yr, Sharpe {base_sharpe:.2f}")
        print(f"  Only {len(rebalance_dates)} independent periods, so the IC t-stat "
              f"({t:.2f}) cannot clear 2.")
        print("  This is a sample-size limit, not evidence against the signal.")
    elif raw_beat and not neutral_beat:
        print("RISK EXPOSURE, NOT ALPHA.")
        print(f"  Raw        : {ls_raw['net_ann_pct']:.1f}%/yr, Sharpe {ls_raw['net_sharpe']:.2f}")
        print(f"  Neutralised: {ls['net_ann_pct']:.1f}%/yr, Sharpe {ls['net_sharpe']:.2f}")
        print(f"  Always long: {base['net_ann_pct']:.1f}%/yr, Sharpe {base_sharpe:.2f}")
        print("  The edge came from beta/size/vol tilts, not stock selection.")
        print("  Holding the index is simpler, cheaper and roughly as good.")
    else:
        print("NO SIGNAL at this horizon. Do not promote to production.")

    print(f"\nIndependent periods: {len(rebalance_dates)}. Below ~40 the Sharpe")
    print("comparison is itself noisy; treat it as directional, not decisive.")

    # Stability / Annual tracking
    years = [d.year for d in rebalance_dates]
    df_metrics = pd.DataFrame({
        "year": years,
        "base_ret": per_period_base,
        "top_ret": per_period_top,
        "alpha_ret": per_period_top - per_period_base
    })
    
    annual_summary = df_metrics.groupby("year")["alpha_ret"].apply(
        lambda s: (s > 0).mean() * 100
    ).to_dict()
    
    annual_details = df_metrics.groupby("year")["alpha_ret"].sum().to_dict()
    
    print("\n  ** Annual Alpha Hit Rate (Stability) **")
    for yr, hr in annual_summary.items():
        print(f"    {yr}: {hr:.1f}% (net sum: {annual_details[yr]:.3f})")

    if not args.no_log:
        verdict = ("ALPHA" if passed
                   else "UNDERPOWERED" if underpowered
                   else "RISK_EXPOSURE" if raw_beat and not neutral_beat
                   else "NO_SIGNAL")
        feature_key = ",".join(sorted(selected))
        if args.activity_filter != "none":
            feature_key += f"|activity={args.activity_filter}"
        if args.model != "ridge":
            feature_key += f"|model={args.model}"
        run_id = persist_run({
            "features": feature_key,
            "horizon_days": args.horizon,
            "rebalance_days": step,
            "cost_bps": args.cost_bps,
            "neutralised": True,
            "activity_filter": args.activity_filter,
            "data_start": df["date"].min().date(),
            "data_end": df["date"].max().date(),
            "rows_used": int(len(df)),
            "tickers": int(df["ticker"].nunique()),
            "test_periods": len(rebalance_dates),
            "ic_mean": indep_ic["ic_mean"],
            "ic_ir": indep_ic["ic_ir"],
            "ic_t_stat": indep_ic["t_stat"],
            "decile_spread": float(spread),
            "ls_net_return": ls["net_ann_pct"],
            "ls_net_sharpe": ls["net_sharpe"],
            "turnover": ls["turnover"],
            "top_net_return": top["net_ann_pct"],
            "top_net_sharpe": top["net_sharpe"],
            "top_turnover": top["turnover"],
            "top_alpha_return": top["alpha_ann_pct"],
            "top_alpha_t_stat": top["alpha_t_stat"],
            "top_alpha_sharpe": top["alpha_sharpe"],
            "base_return": base["net_ann_pct"],
            "base_sharpe": base_sharpe,
            "verdict": verdict,
            "checks_passed": sum(1 for _, ok in checks if ok),
            "checks_total": len(checks),
            "cal_years": json.dumps({k: round(v, 4) for k, v in annual_details.items()}),
            "cal_year_positive_pct": json.dumps({k: round(v, 4) for k, v in annual_summary.items()}),
        })
        if run_id:
            print(f"\nLogged as research_runs.run_id={run_id}")
            history = recent_runs(feature_key, limit=5)
            if len(history) > 1:
                _rule("7. HISTORY FOR THIS FEATURE SET")
                print(history.to_string(index=False))
                print("\nFalling ic_mean or ls_net_sharpe across runs indicates decay.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
