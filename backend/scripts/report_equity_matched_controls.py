"""Bounded matched-control analysis of frozen daily detections; no publication."""
from __future__ import annotations

import argparse
import csv
from collections import Counter
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import sys

import exchange_calendars
import numpy as np
import pandas as pd


BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

CONTROLS = ("NO_SIGNAL_EQUAL_WEIGHT", "MOMENTUM_TOP_DECILE", "MOMENTUM_MATCHED")
VALID_COVERAGE = {"NO_SIGNAL", "MATCH", "MATCH_CONTINUATION"}
HORIZONS = (5, 10, 21)
COST = 4 / 10000


def plan_comparison(pool, signal_tickers, active_tickers, direction):
    signals = sorted(set(signal_tickers))
    available = pool.loc[pool["momentum"].notna()].copy().rename_axis(None)
    available["rank"] = available["momentum"].rank(method="average", pct=True)
    ready_signals = [ticker for ticker in signals if ticker in available.index]
    nonsignals = available.loc[~available.index.isin(active_tickers)]
    momentum = available.sort_values(["momentum", "ticker"], ascending=[direction < 0, True])
    momentum_names = momentum.head(math.ceil(len(momentum) * 0.1)).index.tolist()
    matched = []
    for ticker in ready_signals:
        distances = (nonsignals["rank"] - available.loc[ticker, "rank"]).abs().round(12)
        candidates = nonsignals.assign(distance=distances).loc[distances <= 0.10]
        if not candidates.empty:
            control = candidates.sort_values(["distance", "ticker"]).iloc[0]
            matched.append(dict(signal=ticker, control=control["ticker"], rank_distance=float(control["distance"])))
    return dict(signal_tickers=signals, feature_ready_signals=ready_signals,
                feature_excluded=len(signals) - len(ready_signals),
                no_signal_controls=sorted(nonsignals.index), momentum_controls=momentum_names,
                matched_pairs=matched, unmatched_signals=len(ready_signals) - len(matched),
                eligible_stocks=len(pool), feature_ready_stocks=len(available))


def portfolio_result(names, returns, direction):
    counts = Counter(names)
    if not counts:
        return dict(planned=0, unique=0, missing=0, no_fill=0, complete=False, net_return=None)
    values, missing, no_fill = [], 0, 0
    for ticker, weight in counts.items():
        value, state = returns.get(ticker, (None, "MISSING_RETURN"))
        if value is None or not math.isfinite(float(value)):
            missing += weight
        else:
            no_fill += weight * (state == "NO_FILL")
            values.extend([0.0 if state == "NO_FILL" else direction * value - COST] * weight)
    return dict(planned=len(names), unique=len(counts), missing=missing, no_fill=no_fill,
                complete=missing == 0, net_return=float(np.mean(values)) if missing == 0 else None)


def compare_day(selection, returns, direction, control):
    signals = selection["feature_ready_signals"]
    if control == "MOMENTUM_MATCHED":
        signal_names = [row["signal"] for row in selection["matched_pairs"]]
        control_names = [row["control"] for row in selection["matched_pairs"]]
    else:
        signal_names = signals
        control_names = selection["no_signal_controls"] if control == "NO_SIGNAL_EQUAL_WEIGHT" else selection["momentum_controls"]
    signal_result = portfolio_result(signal_names, returns, direction)
    control_result = portfolio_result(control_names, returns, direction)
    complete = signal_result["complete"] and control_result["complete"]
    return dict(control=control, signal=signal_result, comparator=control_result, complete=complete,
                incremental_return=signal_result["net_return"] - control_result["net_return"] if complete else None,
                original_signals=len(selection["signal_tickers"]), feature_excluded=selection["feature_excluded"],
                unmatched=selection["unmatched_signals"] if control == "MOMENTUM_MATCHED" else 0,
                decision_covered=selection["feature_excluded"] == 0 and (control != "MOMENTUM_MATCHED" or selection["unmatched_signals"] == 0))


def fixed_sessions(start, end, horizon):
    calendar = exchange_calendars.get_calendar("XNYS")
    sessions = [session.date() for session in calendar.sessions_in_range(start, end)]
    return {session for index, session in enumerate(sessions) if index % horizon == 0 and index + horizon < len(sessions)}


def price_panel(ticker, bars, action_dates):
    calendar = exchange_calendars.get_calendar("XNYS")
    if not bars:
        return pd.DataFrame()
    frame = pd.DataFrame([dict(session=bar.session_date, ticker=ticker, security_id=str(bar.security_id),
                               open=float(bar.open_price), high=float(bar.high_price), low=float(bar.low_price),
                               close=float(bar.close_price), volume=float(bar.volume), bar_revision_id=str(bar.bar_revision_id),
                               clock_valid=bar.is_final and bar.interval == "1d"
                                   and bar.bar_start == calendar.session_open(str(bar.session_date)).to_pydatetime()
                                   and bar.bar_end == calendar.session_close(str(bar.session_date)).to_pydatetime()
                                   and bar.replay_available_at == bar.bar_end and bar.system_observed_at >= bar.bar_end,
                               identity_disagreement="REPLAY_MEMBER_IDENTITY_MISMATCH" in bar.quality_codes)
                          for bar in bars]).sort_values("session").reset_index(drop=True)
    if frame.session.duplicated().any():
        raise ValueError("duplicate frozen price session")
    sessions = [value.date() for value in calendar.sessions_in_range(frame.session.min(), frame.session.max())]
    ordinals = {session: index for index, session in enumerate(sessions)}
    frame["ordinal"] = frame.session.map(ordinals)
    prices = frame[["open", "high", "low", "close", "volume"]]
    valid = (np.isfinite(prices).all(axis=1) & (prices[["open", "high", "low", "close"]].min(axis=1) > 0)
             & (frame.volume >= 0) & (frame.high >= prices[["open", "low", "close"]].max(axis=1))
             & (frame.low <= prices[["open", "high", "close"]].min(axis=1)) & frame.clock_valid & ~frame.identity_disagreement)
    broken = (~valid | ~valid.shift(1, fill_value=False) | frame.security_id.ne(frame.security_id.shift())
              | frame.ordinal.diff().ne(1) | frame.session.isin(action_dates))
    frame["segment"] = broken.cumsum()
    grouped = frame.groupby("segment")["close"]
    frame["momentum"] = (grouped.shift(21) / grouped.shift(252) - 1).where(valid)
    entry_valid = (valid & valid.shift(-1, fill_value=False) & frame.ordinal.shift(-1).eq(frame.ordinal + 1)
                   & frame.segment.shift(-1).eq(frame.segment))
    no_fill = entry_valid & frame.volume.shift(-1).eq(0)
    nontrading = frame.volume.eq(0).cumsum()
    for horizon in HORIZONS:
        complete = (valid & frame.segment.shift(-horizon).eq(frame.segment)
                    & frame.ordinal.shift(-horizon).eq(frame.ordinal + horizon)
                    & nontrading.shift(-horizon).eq(nontrading))
        frame[f"gross_{horizon}"] = (frame.close.shift(-horizon) / frame.open.shift(-1) - 1).where(complete)
        frame.loc[no_fill, f"gross_{horizon}"] = 0.0
        frame[f"state_{horizon}"] = np.select([no_fill, complete], ["NO_FILL", "OBSERVED"], default="UNRESOLVED_PATH")
    return frame.set_index("session", drop=False).rename_axis(None)


def hash_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def read_frozen_prices(plan):
    from scripts.run_historical_signal_outcomes import FrozenDailyBars, _utc
    from research.frozen_daily_study import load_samples
    from database import get_db_cursor

    samples, _ = load_samples(BACKEND_DIR.parent / "docs")
    tickers = sorted(set(samples["1"]) | set(samples["2"]))
    after = datetime.fromisoformat(plan["start"]).replace(tzinfo=timezone.utc) - timedelta(days=740)
    reader = FrozenDailyBars(after, _utc(plan["source_cutoff"]))
    expected = set()
    for group in plan["groups"]:
        expected.update(json.loads(Path(group["report"]).read_text())["price_revision_ids"])
    prices, actual, provenance = {}, set(), []
    for index, ticker in enumerate(tickers):
        reader.list_final_after(ticker, "1d", after=after, available_by=_utc(plan["source_cutoff"]), limit=6000,
                                historical_reconstructed_only=True, adjusted=True)
        bars = tuple(bar for bar in reader.paths[ticker][1] if bar.session_date <= date.fromisoformat(plan["end"]))
        identities = [str(bar.bar_revision_id) for bar in bars]
        actual.update(identities)
        prices[ticker] = price_panel(ticker, bars, reader.action_dates[ticker])
        provenance.append(dict(ticker=ticker, price_revision_ids=identities, action_dates=list(map(str, reader.action_dates[ticker]))))
        del reader.paths[ticker]
        if (index + 1) % 50 == 0:
            print(f"Frozen prices: {index + 1}/{len(tickers)} tickers", flush=True)
    if not expected <= actual:
        raise ValueError(f"frozen control reader does not reproduce {len(expected - actual)} replay price revisions")
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SELECT current_setting('transaction_read_only') AS read_only")
        read_only = cursor.fetchone()["read_only"]
    return prices, dict(source_cutoff=plan["source_cutoff"], price_revision_count=len(actual),
                        replay_price_revisions_verified=len(expected), selections=provenance, probe_read_only=read_only)


def load_jsonl(path):
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def daily_comparisons(plan, prices, selection_sink=None):
    from research.signal_scorecard import adapter_signal_cells

    daily, coverage_totals = [], []
    for group in plan["groups"]:
        sample, adapter = group["name"].split("/")
        events = list(load_jsonl(Path(group["events"])))
        active, signals, lifecycles = defaultdict(set), defaultdict(set), set()
        for event in sorted(events, key=lambda row: (row["signal_time"], row["event_id"])):
            if not event["payload"].get("qualification_eligible"):
                continue
            source = event["source_name"]
            session = date.fromisoformat(event["signal_date"])
            active[(source, session)].add(event["ticker"])
            lifecycle = (source, event["source_version"], event["payload"]["security_id"], event["direction"], event["setup_anchor"])
            if lifecycle not in lifecycles:
                signals[(source, event["direction"], session)].add(event["ticker"])
                lifecycles.add(lifecycle)
        coverage_by_date = defaultdict(list)
        for row in load_jsonl(Path(group["coverage"])):
            coverage_by_date[date.fromisoformat(row["session"])].append(row)
        total = Counter()
        cells = {(row["source_name"], row["source_version"], row["direction"]) for row in adapter_signal_cells([adapter])}
        for session, coverage in sorted(coverage_by_date.items()):
            records = []
            for row in coverage:
                total[row["state"]] += 1
                if row["state"] not in VALID_COVERAGE:
                    continue
                panel = prices.get(row["ticker"])
                if panel is None or panel.empty or session not in panel.index:
                    continue
                price = panel.loc[session]
                if price.security_id != row["security_id"]:
                    raise ValueError("control date identity disagrees with frozen membership")
                records.append(price.to_dict())
            pool = pd.DataFrame(records)
            if pool.empty:
                pool = pd.DataFrame(columns=["ticker", "momentum", *[f"gross_{horizon}" for horizon in HORIZONS], *[f"state_{horizon}" for horizon in HORIZONS]])
            pool = pool.set_index("ticker", drop=False).rename_axis(None)
            noncontrol_states = {row["ticker"] for row in coverage if row["state"] != "NO_SIGNAL"}
            returns_by_horizon = {horizon: dict(zip(pool.index, zip(pool[f"gross_{horizon}"], pool[f"state_{horizon}"])))
                                  for horizon in HORIZONS}
            for source, version, direction in sorted(cells):
                selected = signals.get((source, direction, session), set())
                if not selected:
                    continue
                selection = plan_comparison(pool, selected, active.get((source, session), set()) | noncontrol_states, direction)
                if selection_sink is not None:
                    selection_sink(dict(sample=sample, source_name=source, source_version=version,
                                        direction=direction, session=session.isoformat(), **selection))
                for horizon in HORIZONS:
                    returns = returns_by_horizon[horizon]
                    for control in CONTROLS:
                        result = compare_day(selection, returns, direction, control)
                        daily.append(dict(sample=sample, source_name=source, source_version=version, direction=direction,
                                          session=session.isoformat(), horizon=horizon, eligible_stocks=selection["eligible_stocks"],
                                          feature_ready_stocks=selection["feature_ready_stocks"], **result))
        coverage_totals.append(dict(group=group["name"], states=dict(total), first_lifecycle_events=len(lifecycles)))
        print(f"Compared {group['name']}; retained {len(daily)} daily comparison rows", flush=True)
    return daily, coverage_totals


def comparison_statistics(rows):
    from equity.qualification import _t_stat, _student_t_p_value
    from scipy.stats import t as student_t

    count = len(rows)
    if not rows:
        return dict(periods=0, signals=0, mean_signal=None, mean_control=None, mean_incremental=None,
                    t_stat=None, p_value=None, ci_low=None, ci_high=None, early_lift=None, late_lift=None,
                    development_lift=None, later_lift=None)
    lifts = pd.Series([row["incremental_return"] for row in rows], dtype=float)
    statistic = _t_stat(lifts)
    standard_error = float(lifts.std(ddof=1) / math.sqrt(count)) if count > 1 else None
    margin = float(student_t.ppf(.975, count - 1) * standard_error) if count > 1 else None
    midpoint = max(1, count // 2)
    development = [row["incremental_return"] for row in rows if row["session"] < "2025-01-01"]
    later = [row["incremental_return"] for row in rows if row["session"] >= "2025-01-01"]
    mean = float(lifts.mean())
    return dict(periods=count, signals=sum(row["signal"]["planned"] for row in rows),
                mean_signal=float(np.mean([row["signal"]["net_return"] for row in rows])),
                mean_control=float(np.mean([row["comparator"]["net_return"] for row in rows])),
                mean_incremental=mean, t_stat=statistic, p_value=_student_t_p_value(statistic, count),
                ci_low=mean - margin if margin is not None else None, ci_high=mean + margin if margin is not None else None,
                early_lift=float(lifts.iloc[:midpoint].mean()), late_lift=float(lifts.iloc[midpoint:].mean()) if count > 1 else None,
                development_lift=float(np.mean(development)) if development else None,
                later_lift=float(np.mean(later)) if later else None)


def summarize_comparisons(daily, plan):
    from research.signal_scorecard import adapter_signal_cells
    from research.scanner_confidence import _benjamini_hochberg

    adapters = [group["name"].split("/")[-1] for group in plan["groups"]]
    cells = sorted({(row["source_name"], row["source_version"], row["direction"], row["horizon_bars"])
                    for row in adapter_signal_cells(adapters)})
    grouped = defaultdict(dict)
    for row in daily:
        key = (row["source_name"], row["source_version"], row["direction"], row["horizon"], row["control"], row["sample"])
        if row["session"] in grouped[key]:
            raise ValueError("duplicate daily comparison")
        grouped[key][row["session"]] = row
    grids = {horizon: {session.isoformat() for session in fixed_sessions(plan["start"], plan["end"], horizon)} for horizon in HORIZONS}
    summaries = []
    for source, version, direction, horizon in cells:
        for control in CONTROLS:
            key = (source, version, direction, horizon, control)
            first, second = grouped[(*key, "sample-1")], grouped[(*key, "sample-2")]
            planned = sorted(grids[horizon] & first.keys() & second.keys())
            measured_dates = [session for session in planned if first[session]["complete"] and second[session]["complete"]]
            for sample, dates in (("sample-1", first), ("sample-2", second)):
                measured = [dates[session] for session in measured_dates]
                all_measured = [row for _, row in sorted(dates.items()) if row["complete"]]
                summaries.append(dict(source_name=source, source_version=version, direction=direction, horizon=horizon,
                                      control=control, sample=sample, fixed_calendar_dates=len(grids[horizon]),
                                      signal_dates=len(dates), common_signal_dates=len(planned),
                                      missing_pair_dates=len(planned) - len(measured_dates),
                                      decision_incomplete_dates=sum(not dates[session]["decision_covered"] for session in measured_dates),
                                      feature_excluded=sum(row["feature_excluded"] for row in dates.values()),
                                      unmatched_signals=sum(row["unmatched"] for row in dates.values()),
                                      observed_all_date_lift=float(np.mean([row["incremental_return"] for row in all_measured])) if all_measured else None,
                                      observed_all_date_pairs=len(all_measured),
                                      common_dates_sha256=json_hash(planned), measured_dates_sha256=json_hash(measured_dates),
                                      statistics=comparison_statistics(measured)))
    for sample in ("sample-1", "sample-2"):
        selected = [row for row in summaries if row["sample"] == sample]
        adjusted = _benjamini_hochberg(pd.Series([row["statistics"]["p_value"] if row["statistics"]["p_value"] is not None else 1.0 for row in selected]))
        for index, row in enumerate(selected):
            stats = row["statistics"]
            row["fdr_family_cells"] = len(selected)
            row["q_value"] = float(adjusted.iloc[index]) if stats["p_value"] is not None else None
            if stats["periods"] < 40 or stats["signals"] < 100:
                status = "INSUFFICIENT_COMMON_DATE_EVIDENCE"
            elif not all(stats[field] is not None and stats[field] > 0 for field in ("mean_signal", "mean_incremental", "early_lift", "late_lift")):
                status = "NO_STABLE_INCREMENTAL_EDGE"
            elif row["q_value"] is None or row["q_value"] > .05 or stats["ci_low"] <= 0:
                status = "NOT_SUPPORTED_AFTER_UNCERTAINTY"
            elif row["missing_pair_dates"] or row["decision_incomplete_dates"]:
                status = "PROMISING_BUT_INCOMPLETE_MATCHING"
            else:
                status = "RESEARCH_CANDIDATE_NOT_CERTIFIED"
            row["evidence_status"] = status
    return summaries


def dispositions(summaries):
    sources = sorted({row["source_name"] for row in summaries})
    rows = []
    for source in sources:
        primary = [row for row in summaries if row["source_name"] == source and row["horizon"] == 21]
        support = any(all(any(row["direction"] == direction and row["sample"] == sample and row["control"] == control
                             and row["evidence_status"] == "RESEARCH_CANDIDATE_NOT_CERTIFIED" for row in primary)
                         for sample in ("sample-1", "sample-2") for control in CONTROLS) for direction in (1, -1))
        possible = any(all(any(row["direction"] == direction and row["sample"] == sample and row["control"] == "MOMENTUM_MATCHED"
                              and row["statistics"]["periods"] >= 40 and row["statistics"]["mean_signal"] > 0
                              and row["statistics"]["mean_incremental"] > 0 for row in primary)
                          for sample in ("sample-1", "sample-2")) for direction in (1, -1))
        if support:
            decision = "RETAIN_RESEARCH_CANDIDATE_NOT_LIVE"
        elif source == "MA_CROSSOVER_9_21":
            decision = "KEEP_SIMPLE_BASELINE"
        elif source == "compression_breakout":
            decision = "SIMPLIFY_TO_BREAKOUT_ATTRIBUTE"
        elif source in ("level_retest_rejection", "GAP_ENTRY_FILL"):
            decision = "KEEP_LOCATION_CONTEXT_NOT_DIRECTIONAL_GRADE"
        elif source.startswith("PATTERN_"):
            decision = "KEEP_GEOMETRY_ATTRIBUTE_PARK_STANDALONE"
        elif possible:
            decision = "RETAIN_UNCONFIRMED_RESEARCH_LEAD"
        else:
            decision = "PARK_STANDALONE_PENDING_EVIDENCE"
        rows.append(dict(source_name=source, proposed_disposition=decision, applied=False,
                         replicated_primary_support=support, replicated_positive_primary_means=possible))
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    from scripts.report_equity_signal_scorecard import load_batch_scope

    scope = load_batch_scope(args.batch_dir)
    plan = scope["plan"]
    if plan["sample"] != "both":
        raise ValueError("matched replication requires both original samples")
    output = args.output_dir or args.batch_dir / "matched-controls-v1"
    contract = dict(study="FROZEN_DAILY_MATCHED_CONTROLS_V1", batch_plan_sha256=json_hash(plan),
                    source_cutoff=plan["source_cutoff"], start=plan["start"], end=plan["end"],
                    primary_horizon=21, diagnostic_horizons=[5, 10], controls=list(CONTROLS),
                    momentum="close[t-21]/close[t-252]-1 within a contiguous same-security segment",
                    matching="nearest no-signal momentum percentile, maximum 0.10 distance, with replacement, ticker tie-break",
                    no_signal="owning adapter coverage state NO_SIGNAL, not rejected/continuing/other matches",
                    population="dated members with usable adapter coverage; momentum-ready subset and exclusions explicit",
                    schedule="fixed XNYS grid anchored to batch start; identical signal-bearing dates in both samples",
                    missingness="fixed weights; no partial-portfolio renormalization; incomplete pairs omitted and counted in both samples",
                    costs_bps=4, short_control="same signed direction; lowest momentum decile for shorts",
                    exclusions=["causal treatment effect", "sector matching", "total return", "short borrow costs", "live promotion", "new detector optimization"],
                    selection_warning="specified after examining the baseline scorecard; not an untouched holdout",
                    minimum_periods=40, minimum_signals=100, script_sha256=hash_file(Path(__file__)))
    if not args.execute:
        print(json.dumps(contract, indent=2))
        return 0
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise ValueError("use a new empty output directory; matched-control artifacts are not overwritten")
    (output / "contract.json").write_text(json.dumps(contract, indent=2) + "\n", encoding="utf-8")
    prices, price_provenance = read_frozen_prices(plan)
    with (output / "selections.jsonl").open("w", encoding="utf-8") as selections:
        daily, coverage = daily_comparisons(plan, prices, lambda row: selections.write(json.dumps(row, sort_keys=True) + "\n"))
    summaries = summarize_comparisons(daily, plan)
    with (output / "daily.csv").open("w", encoding="utf-8", newline="") as handle:
        flattened = (dict({key: value for key, value in row.items() if not isinstance(value, dict)},
                          **{f"{side}_{key}": value for side in ("signal", "comparator") for key, value in row[side].items()}) for row in daily)
        first = next(flattened, None)
        if first is not None:
            writer = csv.DictWriter(handle, fieldnames=list(first))
            writer.writeheader()
            writer.writerow(first)
            writer.writerows(flattened)
    pd.DataFrame([{key: value for key, value in row.items() if key != "statistics"} | row["statistics"] for row in summaries]).to_csv(output / "summary.csv", index=False)
    report = dict(contract=contract, contract_sha256=json_hash(contract), generated_at=datetime.now(timezone.utc).isoformat(),
                  price_provenance=price_provenance, coverage=coverage, comparisons=summaries, dispositions=dispositions(summaries),
                  status_counts=dict(Counter(row["evidence_status"] for row in summaries)),
                  daily_comparison_rows=len(daily), artifacts={name: hash_file(output / name) for name in ("selections.jsonl", "daily.csv", "summary.csv")},
                  database_mutations=False, production_changes=False)
    (output / "report.json").write_text(json.dumps(report, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"daily_comparison_rows": len(daily), "summary_cells": len(summaries),
                      "status_counts": report["status_counts"], "dispositions": report["dispositions"], "output": str(output)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())