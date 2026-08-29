"""Evaluate fixed scanner confidence filters from persisted next-open outcomes.

Run the historical rank replay in a separate process to release feature memory:
    python backend/scripts/research_scanner_confidence.py --build-rank-cache
    python backend/scripts/research_scanner_confidence.py
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from database import get_db_cursor  # noqa: E402
from research.features import load_daily_panel  # noqa: E402
from research.regime_context import replay_regime_context  # noqa: E402
from research.scanner_confidence import summarize_confidence_slices  # noqa: E402
from research.scanner_events import OUTCOME_ENTRY_MODEL  # noqa: E402
from research.xsmom import (  # noqa: E402
    MODEL_VERSION,
    attach_xsmom_ranks,
    replay_xsmom_ranks,
)


def _load_observations(intervals: list[str]) -> pd.DataFrame:
    with get_db_cursor() as cursor:
        cursor.execute("""
            SELECT e.scanner_name, e.scanner_version, e.interval, e.direction,
                   e.trigger_type, e.discovery_state, e.metadata,
                   o.horizon_bars, e.signal_time, e.trade_date, e.ticker,
                   o.net_signed_return AS net_return,
                   o.net_alpha_return AS net_alpha
            FROM scanner_events e
            JOIN scanner_event_outcomes o ON o.event_id = e.event_id
            WHERE e.interval = ANY(%s) AND o.entry_model = %s
        """, (intervals, OUTCOME_ENTRY_MODEL))
        chunks = []
        while True:
            rows = cursor.fetchmany(10_000)
            if not rows:
                break
            chunks.append(pd.DataFrame.from_records(rows))
        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def _latest_outcome_date(intervals: list[str]):
    with get_db_cursor() as cursor:
        cursor.execute("""
            SELECT MAX(e.trade_date) AS last_date
            FROM scanner_events e
            JOIN scanner_event_outcomes o ON o.event_id = e.event_id
            WHERE e.interval = ANY(%s) AND o.entry_model = %s
        """, (intervals, OUTCOME_ENTRY_MODEL))
        row = cursor.fetchone()
        return row["last_date"] if row else None


def _write_rank_cache(rank_end, output: str) -> None:
    panel = load_daily_panel(end=str(rank_end))
    print(f"loaded daily rank panel={len(panel):,}", flush=True)
    context = replay_regime_context(panel)
    ranks = replay_xsmom_ranks(panel)[
        ["date", "ticker", "percentile", "decile"]
    ].copy()
    ranks = ranks.merge(context, on=["date", "ticker"], how="left")
    ranks["percentile"] = ranks["percentile"].astype("float32")
    ranks["decile"] = ranks["decile"].astype("Int8")
    ranks["ticker"] = ranks["ticker"].astype("category")
    ranks.to_pickle(output)
    print(
        f"replayed ranks={len(ranks):,} dates={ranks['date'].nunique():,}",
        flush=True,
    )


def _load_calendars(intervals: list[str]) -> dict[str, dict]:
    calendars = {}
    with get_db_cursor() as cursor:
        for interval in intervals:
            if interval in ("1d", "1wk"):
                cursor.execute("""
                    SELECT DISTINCT datetime::date AS bar_key
                    FROM stock_prices_daily ORDER BY bar_key
                """)
            else:
                cursor.execute("""
                    SELECT DISTINCT datetime AS bar_key
                    FROM stock_prices_hourly ORDER BY bar_key
                """)
            calendars[interval] = {
                row["bar_key"]: index
                for index, row in enumerate(cursor.fetchall())
            }
    return calendars


def _percent_report(report: pd.DataFrame) -> pd.DataFrame:
    display = report.copy()
    display["direction"] = display["direction"].map({1: "LONG", -1: "SHORT"})
    for column in (
        "mean_net_alpha", "early_alpha", "late_alpha", "hit_rate",
        "mean_incremental_alpha", "early_incremental_alpha",
        "late_incremental_alpha",
    ):
        display[column] = display[column] * 100
    return display


def _refresh_saved_summary(output: Path) -> None:
    if not output.exists():
        raise SystemExit(f"Study report missing: {output}")
    payload = json.loads(output.read_text(encoding="utf-8"))
    report = payload.get("report", [])
    payload["summary_refreshed_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    payload["qualified_primary_scanners"] = [
        row for row in report
        if row.get("slice_name") == "baseline"
        and row.get("status") == "CONFIDENCE_PASS"
    ]
    payload["robust_primary_scanners"] = [
        row for row in report
        if row.get("slice_name") == "baseline"
        and row.get("robustness_status") == "ROBUST_PASS"
    ]
    payload["qualified_filters"] = [
        row for row in report
        if row.get("slice_name") != "baseline"
        and row.get("status") == "CONFIDENCE_PASS"
    ]
    payload["robust_filters"] = [
        row for row in report
        if row.get("slice_name") != "baseline"
        and row.get("robustness_status") == "ROBUST_PASS"
    ]
    payload.setdefault("qualification_contract", {})[
        "maximum_false_discovery_rate"
    ] = 0.05
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(
        "refreshed summary "
        f"primary={len(payload['qualified_primary_scanners'])} "
        f"robust_primary={len(payload['robust_primary_scanners'])} "
        f"filters={len(payload['qualified_filters'])} "
        f"robust_filters={len(payload['robust_filters'])}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scanner confidence-filter evaluation from persisted outcomes"
    )
    parser.add_argument("--intervals", default="1d,1h")
    parser.add_argument(
        "--output",
        default=str(BACKEND_DIR / "research" / "scanner_confidence_study.json"),
    )
    parser.add_argument(
        "--rank-cache",
        default=str(Path(tempfile.gettempdir()) / "stock_screener_xsmom_rank_replay.pkl"),
    )
    parser.add_argument("--build-rank-cache", action="store_true")
    parser.add_argument(
        "--refresh-summary", action="store_true",
        help="Refresh top-level qualification arrays from the saved report rows",
    )
    args = parser.parse_args()
    intervals = [value.strip() for value in args.intervals.split(",") if value.strip()]
    unknown = sorted(set(intervals) - {"1d", "1h", "1wk"})
    if not intervals or unknown:
        parser.error(f"unsupported intervals: {', '.join(unknown)}")

    output = Path(args.output)
    if args.refresh_summary:
        _refresh_saved_summary(output)
        return 0

    rank_end = _latest_outcome_date(intervals)
    if rank_end is None:
        raise SystemExit("No matured next-open scanner outcomes")
    rank_cache = Path(args.rank_cache)
    if args.build_rank_cache:
        _write_rank_cache(rank_end, str(rank_cache))
        print(f"wrote rank cache {rank_cache}")
        return 0
    if not rank_cache.exists():
        raise SystemExit(
            "Rank cache missing; run this command first with --build-rank-cache"
        )
    ranks = pd.read_pickle(rank_cache)
    rank_cache.unlink()
    if ranks.empty or pd.to_datetime(ranks["date"]).max().date() < rank_end:
        raise SystemExit("Rank cache does not cover the latest outcome date; rebuild it")

    observations = _load_observations(intervals)
    if observations.empty:
        raise SystemExit("No matured next-open scanner outcomes")
    print(f"loaded observations={len(observations):,}", flush=True)
    observations = attach_xsmom_ranks(observations, ranks)
    del ranks
    rank_coverage = observations["xs_age_days"].between(0, 7, inclusive="both")
    print(
        "fresh rank coverage={:,}/{:,} ({:.1%})".format(
            int(rank_coverage.sum()), len(observations), float(rank_coverage.mean())
        ),
        flush=True,
    )
    report = summarize_confidence_slices(
        observations, _load_calendars(intervals)
    )
    print(f"summarized rows={len(report):,}", flush=True)
    candidates = report[
        (report["slice_name"] != "baseline")
        & (report["status"] == "CONFIDENCE_PASS")
    ].sort_values(
        ["interval", "mean_incremental_alpha"], ascending=[True, False]
    )
    primary = report[
        (report["slice_name"] == "baseline")
        & (report["status"] == "CONFIDENCE_PASS")
    ].sort_values(["interval", "mean_net_alpha"], ascending=[True, False])

    columns = [
        "scanner_name", "interval", "direction", "horizon_bars", "slice_name",
        "events", "independent_periods", "mean_net_alpha", "alpha_t_stat",
        "early_alpha", "late_alpha", "mean_incremental_alpha",
        "incremental_t_stat", "early_incremental_alpha",
        "late_incremental_alpha", "absolute_fdr_q", "incremental_fdr_q",
        "robustness_status", "status",
    ]
    display = _percent_report(report)
    print(f"observations={len(observations):,} report_rows={len(report):,}")
    print("\nQUALIFIED PRIMARY SCANNERS")
    print("None" if primary.empty else _percent_report(primary)[columns].to_string(
        index=False, float_format=lambda value: f"{value:.3f}"
    ))
    print("\nQUALIFIED CONFIDENCE FILTERS")
    print("None" if candidates.empty else _percent_report(candidates)[columns].to_string(
        index=False, float_format=lambda value: f"{value:.3f}"
    ))
    print("\nTOP NON-BASELINE SLICES BY INCREMENTAL T-STAT")
    ranked = display[display["slice_name"] != "baseline"].sort_values(
        "incremental_t_stat", ascending=False, na_position="last"
    ).head(30)
    print(ranked[columns].to_string(index=False, float_format=lambda value: f"{value:.3f}"))

    output.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "entry_model": OUTCOME_ENTRY_MODEL,
        "intervals": intervals,
        "observations": len(observations),
        "rank_overlay": {
            "model_version": MODEL_VERSION,
            "source": "point_in_time_daily_replay",
            "fresh_observations": int(rank_coverage.sum()),
            "coverage": float(rank_coverage.mean()),
            "daily_timing": "same_session_close",
            "hourly_timing": "latest_strictly_prior_session_close",
        },
        "qualification_contract": {
            "minimum_events": 100,
            "minimum_independent_periods": 40,
            "absolute_alpha_t_stat": 2.0,
            "incremental_alpha_t_stat": 2.0,
            "requires_positive_early_and_late_absolute_alpha": True,
            "requires_positive_early_and_late_incremental_alpha": True,
        },
        "qualified_primary_scanners": json.loads(primary.to_json(orient="records")),
        "robust_primary_scanners": json.loads(report[
            (report["slice_name"] == "baseline")
            & (report["robustness_status"] == "ROBUST_PASS")
        ].to_json(orient="records")),
        "qualified_filters": json.loads(candidates.to_json(orient="records")),
        "robust_filters": json.loads(report[
            (report["slice_name"] != "baseline")
            & (report["robustness_status"] == "ROBUST_PASS")
        ].to_json(orient="records")),
        "report": json.loads(report.to_json(orient="records")),
    }
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"\nwrote {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())