"""Execute isolated v2 research using a verified frozen daily batch."""
from __future__ import annotations

import argparse
import json
import math
import os
from collections import Counter, defaultdict
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import sys
from types import SimpleNamespace
from uuid import UUID

import exchange_calendars
import numpy as np
import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from research.strategy_v2 import CONFIGURATIONS, CENTRAL_VARIANTS, detect, digest, enrich_prices, entry_status
from scripts.report_equity_matched_controls import hash_file, load_jsonl, portfolio_result, price_panel
from scripts.report_equity_signal_scorecard import load_batch_scope
from scripts.run_historical_signal_outcomes import FrozenDailyBars, _utc
from equity.historical_research import historical_event_evidence
from research.historical_signal_replay import HistoricalSignalEvent
from equity.outcomes import default_directional_policy, recommendation_plan_policy, evaluate_directional_outcome, _daily_path_prefix
from database import get_db_cursor


def universe_ranks(plan, tickers):
    calendar = exchange_calendars.get_calendar("XNYS")
    start = date.fromisoformat(plan["start"]) - timedelta(days=740)
    sessions = calendar.sessions_in_range(start, plan["end"])
    cutoff = _utc(plan["source_cutoff"])
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='300s'")
        cursor.execute("""
            WITH calendar AS MATERIALIZED (
                SELECT * FROM UNNEST(%s::date[],%s::timestamptz[],%s::timestamptz[])
                WITH ORDINALITY AS dates(session_date,session_open,session_close,ordinal)
            ), members AS MATERIALIZED (
                SELECT run.effective_from::date AS session_date,member.ticker,member.security_id,
                       run.universe_run_id
                FROM equity_original_universe_runs run JOIN equity_universe_members member USING(universe_run_id)
                WHERE run.policy_version='liquid_us_common_stocks_v2'
                  AND run.availability_mode='HISTORICAL_RECONSTRUCTED'
                  AND run.created_at<=%s AND run.observed_at<=%s
            ), action_dates AS MATERIALIZED (
                SELECT DISTINCT ticker,effective_date AS session_date FROM equity_corporate_actions
                WHERE availability_mode='HISTORICAL_RECONSTRUCTED'
                  AND action_type IN ('MERGER','SYMBOL_CHANGE','SPINOFF')
                  AND first_observed_at<=%s AND created_at<=%s
            ), prices AS MATERIALIZED (
                SELECT DISTINCT ON(bar.ticker,bar.session_date) bar.ticker,bar.session_date,bar.security_id::text,
                       bar.close_price::float8 AS close,bar.bar_revision_id::text,calendar.ordinal,
                       CASE WHEN bar.bar_start=calendar.session_open AND bar.bar_end=calendar.session_close
                         AND bar.replay_available_at=bar.bar_end AND bar.system_observed_at>=bar.bar_end
                         AND bar.open_price>0 AND bar.low_price>0 AND bar.close_price>0 AND bar.volume>=0
                         AND bar.high_price>=GREATEST(bar.open_price,bar.close_price,bar.low_price)
                         AND bar.low_price<=LEAST(bar.open_price,bar.close_price,bar.high_price)
                         AND (members.security_id IS NULL OR members.security_id=bar.security_id)
                         AND action_dates.ticker IS NULL THEN 1 ELSE 0 END AS valid
                FROM equity_bar_revisions bar JOIN calendar USING(session_date)
                LEFT JOIN members USING(ticker,session_date) LEFT JOIN action_dates USING(ticker,session_date)
                WHERE bar.availability_mode='HISTORICAL_RECONSTRUCTED' AND bar.interval='1d'
                  AND bar.session_scope='RTH' AND bar.is_final AND bar.adjusted
                  AND bar.quality_codes @> ARRAY['GROUPED_DAILY_EXACT_TICKER_V2']::text[]
                  AND bar.system_observed_at<=%s AND bar.created_at<=%s
                ORDER BY bar.ticker,bar.session_date,bar.replay_available_at DESC,bar.created_at DESC,bar.bar_revision_id
            ), histories AS (
                SELECT *,close/LAG(close,63) OVER ticker_history-1 AS return63,
                       ordinal-LAG(ordinal,63) OVER ticker_history AS span,
                       MIN(security_id) OVER recent AS min_identity,MAX(security_id) OVER recent AS max_identity,
                       MIN(valid) OVER recent AS valid_history
                FROM prices WINDOW ticker_history AS(PARTITION BY ticker ORDER BY session_date),
                  recent AS(PARTITION BY ticker ORDER BY session_date ROWS BETWEEN 63 PRECEDING AND CURRENT ROW)
            ), eligible AS (
                SELECT histories.*,members.universe_run_id::text FROM histories JOIN members USING(ticker,session_date)
                WHERE span=63 AND min_identity=max_identity AND valid_history=1
                  AND histories.security_id=members.security_id::text AND return63 IS NOT NULL
            ), ranked AS (
                SELECT *, (RANK() OVER(PARTITION BY session_date ORDER BY return63)
                    +(COUNT(*) OVER(PARTITION BY session_date,return63)-1)/2.0)
                    /COUNT(*) OVER(PARTITION BY session_date) AS percentile,
                    COUNT(*) OVER(PARTITION BY session_date) AS eligible_count
                FROM eligible
            ) SELECT ticker,session_date,return63,percentile::float8,eligible_count,bar_revision_id,universe_run_id
              FROM ranked WHERE ticker=ANY(%s::text[]) ORDER BY ticker,session_date
        """, ([value.date() for value in sessions], [calendar.session_open(value).to_pydatetime() for value in sessions],
              [calendar.session_close(value).to_pydatetime() for value in sessions],
              cutoff, cutoff, cutoff, cutoff, cutoff, cutoff, tickers))
        rows = [dict(row) for row in cursor.fetchall()]
    return rows


def load_inputs(plan, pilot=False):
    sample_tickers, memberships = {}, {}
    expected_revisions = set()
    for sample in ("sample-1", "sample-2"):
        group = next(group for group in plan["groups"] if group["name"] == sample + "/composite-scanners-1d-v1")
        coverage = list(load_jsonl(Path(group["coverage"])))
        names = sorted({row["ticker"] for row in coverage})
        if pilot:
            names = names[:4]
        sample_tickers[sample] = names
        memberships.update({(row["ticker"], date.fromisoformat(row["session"])): row for row in coverage if row["ticker"] in names})
        if not pilot:
            expected_revisions.update(__import__("json").loads(Path(group["report"]).read_text())["price_revision_ids"])
    tickers = sorted(set(sample_tickers["sample-1"]) | set(sample_tickers["sample-2"]))
    print(f"Computing full-universe RS63 ranks; selecting {len(tickers)} research tickers", flush=True)
    rank_rows = universe_ranks(plan, tickers)
    rank_frame = pd.DataFrame(rank_rows)
    ranks = {ticker: group.set_index("session_date").percentile for ticker, group in rank_frame.groupby("ticker")}
    after = datetime.fromisoformat(plan["start"]).replace(tzinfo=timezone.utc) - timedelta(days=740)
    reader = FrozenDailyBars(after, _utc(plan["source_cutoff"]))
    reader.list_final_after("SPY", "1d", after=after, available_by=_utc(plan["source_cutoff"]), limit=6000,
                            adjusted=True, historical_reconstructed_only=True)
    spy = price_panel("SPY", reader.paths["SPY"][1], reader.action_dates["SPY"])
    spy_return = spy.close / spy.groupby("segment").close.shift(63) - 1
    frames, sources, bars_by_ticker = {}, [], {}
    actual = set()
    for index, ticker in enumerate(tickers):
        reader.list_final_after(ticker, "1d", after=after, available_by=_utc(plan["source_cutoff"]), limit=6000,
                                adjusted=True, historical_reconstructed_only=True)
        bars = tuple(bar for bar in reader.paths[ticker][1] if bar.session_date <= date.fromisoformat(plan["end"]))
        bars_by_ticker[ticker] = bars
        sources.extend(str(bar.bar_revision_id) for bar in bars)
        actual.update(str(bar.bar_revision_id) for bar in bars)
        frame = price_panel(ticker, bars, reader.action_dates[ticker])
        if frame.empty:
            continue
        frame = enrich_prices(frame, spy_return, ranks.get(ticker, pd.Series(dtype=float)))
        frame["eligible"] = [((ticker, session) in memberships and str(memberships[(ticker, session)]["security_id"]) == identity)
                              for session, identity in zip(frame.index, frame.security_id)]
        frame["ready"] &= frame.eligible
        frame["volatility20"] = frame.groupby("segment").close.transform(lambda prices: prices.pct_change(fill_method=None).rolling(20).std())
        frames[ticker] = frame
        if (index + 1) % 50 == 0:
            print(f"Loaded {index + 1}/{len(tickers)} frozen price histories", flush=True)
    if not expected_revisions <= actual:
        raise ValueError("v2 inputs do not reproduce the original composite replay revisions")
    return frames, bars_by_ticker, reader, sample_tickers, dict(price_revision_ids=sources,
        verified_original_revision_count=len(expected_revisions), rank_rows=rank_rows, rank_sha256=digest(rank_rows),
        memberships_sha256=digest(sorted((ticker, str(session), row["security_id"], row["universe_run_id"]) for (ticker, session), row in memberships.items())))


def evaluate_event(event, reader, end_session):
    calendar = exchange_calendars.get_calendar("XNYS")
    signal = calendar.session_close(event["session"]).to_pydatetime()
    path = reader.list_final_after(event["ticker"], "1d", after=signal, available_by=reader.source_cutoff,
                                  limit=21, historical_reconstructed_only=True, adjusted=True)
    path = tuple(bar for bar in path if bar.session_date <= end_session)
    result = dict(event_id=event["event_id"], family=event["family"], variant=event["variant"], ticker=event["ticker"],
                  direction=event["direction"], session=event["session"], horizon=event["horizon"],
                  maturity_session=str(calendar.session_offset(event["session"], event["horizon"]).date()),
                  path_revision_ids=[str(bar.bar_revision_id) for bar in path], diagnostics={})
    if not path or path[0].session_date != calendar.next_session(event["session"]).date() or str(path[0].security_id) != event["security_id"]:
        return dict(result, entry_state="UNRESOLVED_ENTRY", state="UNAVAILABLE", gross=None, cost_applies=False)
    _, entry_problem = _daily_path_prefix(path[:1], UUID(event["security_id"]), path[0].session_date)
    if entry_problem:
        return dict(result, entry_state="UNRESOLVED_ENTRY", state="UNAVAILABLE", gross=None, cost_applies=False,
                    quality_codes=[entry_problem])
    admitted = entry_status(event, float(path[0].open_price), float(path[0].volume))
    record = HistoricalSignalEvent(event_id=UUID(event["event_id"]), source_name=event["family"], source_version=event["variant"],
        ticker=event["ticker"], signal_date=date.fromisoformat(event["session"]), signal_time=signal, direction=event["direction"],
        setup_anchor=event["setup_id"], universe_run_id=UUID(int=0), universe_policy_version="research_v2",
        source_bar_revision_ids=(), payload={"stop_price": event["stop"], "target_price": event["target"]})
    subject = historical_event_evidence(record, SimpleNamespace(security_id=UUID(event["security_id"]), security_revision_id=None))
    directional = default_directional_policy(source_name=event["family"], source_version=event["variant"], interval="1d",
                    horizons={f"{value}d": value for value in (5, 10, 21)}, effective_from=signal, round_trip_cost_bps=0)
    for horizon in (5, 10, 21):
        outcome = evaluate_directional_outcome(subject, directional, f"{horizon}d", path)
        result["diagnostics"][str(horizon)] = dict(state=outcome.entry_status, gross=outcome.signed_return,
                                                   quality_codes=outcome.quality_codes)
    if admitted != "FILLED":
        return dict(result, entry_state=admitted, state="NOT_FILLED", gross=0.0 if admitted != "UNRESOLVED_ENTRY" else None,
                    cost_applies=False)
    policy = recommendation_plan_policy(source_name=event["family"], source_version=event["variant"], interval="1d",
                                        horizons={f"{event['horizon']}d": event["horizon"]}, effective_from=signal, round_trip_cost_bps=0)
    outcome = evaluate_directional_outcome(subject, policy, f"{event['horizon']}d", path)
    return dict(result, entry_state=admitted, state=outcome.entry_status,
                gross=outcome.signed_return, cost_applies=True, quality_codes=outcome.quality_codes,
                exit_time=str(outcome.exit_time), outcome_policy_id=str(outcome.outcome_policy_id))


def detector_job(arguments):
    ticker, frame = arguments
    events, transitions = [], []
    for config in CONFIGURATIONS:
        detected, changes = detect(frame, config)
        events.extend(detected)
        transitions.extend(changes)
    return events, transitions


def collect_detections(frames, workers=1):
    if workers == 1:
        output = map(detector_job, sorted(frames.items()))
        result = list(output)
    else:
        from concurrent.futures import ProcessPoolExecutor
        with ProcessPoolExecutor(max_workers=workers) as executor:
            result = list(executor.map(detector_job, sorted(frames.items())))
    events = sorted([event for detected, _ in result for event in detected], key=lambda event: event["event_id"])
    transitions = sorted([item for _, changes in result for item in changes], key=lambda item: (item["setup_id"], item["session"], item["status"]))
    return events, transitions


def control_names(pool, signal_names, direction, config):
    ready = pool.loc[pool.momentum.notna() & pool.rs63.notna() & pool.volatility20.notna()].copy().rename_axis(None)
    ready["vol_rank"] = ready.volatility20.rank(pct=True)
    ready["strength_rank"] = ready.rs63.rank(pct=True)
    nonsignals = ready.loc[~ready.index.isin(signal_names)]
    momentum = ready.sort_values(["momentum", "ticker"], ascending=[direction < 0, True])
    selected = momentum.head(math.ceil(len(momentum) * config["momentum_top_fraction"])).index.tolist()
    matches = []
    for ticker in sorted(signal_names):
        if ticker not in ready.index:
            continue
        strength = (nonsignals.strength_rank - ready.loc[ticker, "strength_rank"]).abs().round(12)
        volatility = (nonsignals.vol_rank - ready.loc[ticker, "vol_rank"]).abs().round(12)
        candidates = nonsignals.loc[(strength <= config["matched_control_caliper"]) & (volatility <= config["matched_control_caliper"])]
        if not candidates.empty:
            ranked = candidates.assign(distance=(strength + volatility).reindex(candidates.index))
            matches.append((ticker, ranked.sort_values(["distance", "ticker"]).index[0]))
    return {"ELIGIBLE_EQUAL_WEIGHT": ready.index.tolist(), "MOMENTUM_12_1": selected,
            "MATCHED_STRENGTH_VOLATILITY": matches}


def build_comparisons(events, outcomes, frames, samples, config, selection_sink):
    by_event = {row["event_id"]: row for row in outcomes}
    grouped, active = defaultdict(list), defaultdict(set)
    ticker_sample = {ticker: sample for sample, names in samples.items() for ticker in names}
    for event in events:
        sample = ticker_sample[event["ticker"]]
        key = (sample, event["session"], event["family"], event["variant"], event["direction"])
        grouped[key].append(event)
        active[(sample, event["session"], event["family"])].add(event["ticker"])
    rows = []
    for sample, names in samples.items():
        combined = pd.concat([frames[ticker].loc[frames[ticker].ready] for ticker in names if ticker in frames])
        pools = {str(session): group.set_index("ticker", drop=False).rename_axis(None)
                 for session, group in combined.groupby("session")}
        for key, signals in sorted(grouped.items()):
            group_sample, session, family, variant, direction = key
            if group_sample != sample:
                continue
            pool = pools.get(session, pd.DataFrame(columns=combined.columns).set_index("ticker", drop=False))
            horizon = signals[0]["horizon"]
            controls = control_names(pool, active[(sample, session, family)], direction, config)
            signal_map = {event["ticker"]: by_event[event["event_id"]] for event in signals}
            for control, selected in controls.items():
                signal_names = sorted(signal_map)
                control_names_list = selected
                if control == "MATCHED_STRENGTH_VOLATILITY":
                    selected = [(ticker, match) for ticker, match in selected if ticker in signal_map]
                    signal_names, control_names_list = [pair[0] for pair in selected], [pair[1] for pair in selected]
                selection_sink(dict(sample=sample, session=session, family=family, variant=variant, direction=direction,
                                    control=control, signal_tickers=signal_names, control_tickers=control_names_list,
                                    original_signals=len(signals), unmatched=len(signals) - len(signal_names)))
                returns = dict(zip(pool.index, zip(pool[f"gross_{horizon}"], pool[f"state_{horizon}"])))
                baseline = portfolio_result(control_names_list, returns, direction)
                known_signals = [signal_map[ticker] for ticker in signal_names if signal_map[ticker]["gross"] is not None]
                complete = bool(signal_names) and len(known_signals) == len(signal_names) and baseline["complete"]
                for bps in config["cost_scenarios_bps"]:
                    mean_signal = float(np.mean([row["gross"] - bps / 10000 * row["cost_applies"] for row in known_signals])) if complete else None
                    mean_control = (baseline["net_return"] - (bps - 4) / 10000 * (1 - baseline["no_fill"] / baseline["planned"])) if complete else None
                    rows.append(dict(sample=sample, session=session, family=family, variant=variant, direction=direction,
                                     horizon=horizon, cost_bps=bps, control=control, original_signals=len(signals),
                                     compared_signals=len(signal_names), matched_fraction=len(signal_names) / len(signals),
                                     filled=sum(row["cost_applies"] for row in known_signals), complete=complete,
                                     missing_signal=len(signal_names) - len(known_signals), missing_control=baseline["missing"],
                                     mean_signal=mean_signal, mean_control=mean_control,
                                     lift=mean_signal - mean_control if complete else None,
                                     maturity_session=by_event[signals[0]["event_id"]]["maturity_session"]))
        print(f"Built {sample} signal/control cohorts", flush=True)
    return rows


def walk_forward(daily, sessions, config):
    records, selected_rows = [], []
    family_variants = {family: [trial.variant for trial in CONFIGURATIONS if trial.family == family] for family in CENTRAL_VARIANTS}
    for offset in range(config["training_sessions"], len(sessions), config["test_sessions"]):
        start = sessions[offset]
        end = sessions[min(offset + config["test_sessions"], len(sessions)) - 1]
        train_signal_end = sessions[offset - config["label_purge_sessions"]]
        for family, variants in family_variants.items():
            for direction in (1, -1):
                candidates = []
                for variant in variants:
                    training = [row for row in daily if row["family"] == family and row["direction"] == direction
                                and row["variant"] == variant and row["sample"] == "sample-1"
                                and row["control"] == config["selection_benchmark"] and row["cost_bps"] == config["selection_cost_bps"]
                                and row["session"] < train_signal_end and row["maturity_session"] < start]
                    measured = [row for row in training if row["complete"]]
                    coverage = len(measured) / len(training) if training else 0
                    signal_count = sum(row["compared_signals"] for row in measured)
                    mean_signal = float(np.mean([row["mean_signal"] for row in measured])) if measured else None
                    lift = float(np.mean([row["lift"] for row in measured])) if measured else None
                    eligible = (len(measured) >= config["minimum_training_dates"] and signal_count >= config["minimum_training_signals"]
                                and coverage >= config["minimum_pair_coverage"] and mean_signal > 0 and lift > 0)
                    candidates.append(dict(variant=variant, dates=len(measured), signals=signal_count, coverage=coverage,
                                           mean_signal=mean_signal, lift=lift, eligible=eligible))
                eligible = [row for row in candidates if row["eligible"]]
                chosen = max(eligible, key=lambda row: (row["lift"], row["variant"] == CENTRAL_VARIANTS[family], row["variant"])) if eligible else None
                records.append(dict(family=family, direction=direction, test_start=start, test_end=end,
                                    training_signal_before=train_signal_end, chosen=chosen["variant"] if chosen else None,
                                    candidates=candidates))
                if chosen:
                    selected_rows.extend(dict(row, test_fold_start=start) for row in daily
                                         if row["family"] == family and row["direction"] == direction and row["variant"] == chosen["variant"]
                                         and start <= row["session"] <= end)
    return records, selected_rows


def bootstrap_summary(rows, calendar_dates, config):
    measured = [row for row in rows if row["complete"]]
    base = dict(signal_dates=len(rows), measured_dates=len(measured), signals=sum(row["compared_signals"] for row in measured),
                coverage=len(measured) / len(rows) if rows else 0,
                unmatched_signals=sum(row["original_signals"] - row["compared_signals"] for row in rows),
                mean_signal=None, mean_lift=None, ci_low=None, ci_high=None, p_value=None,
                first_half_lift=None, second_half_lift=None, bootstrap_blocks=math.ceil(len(calendar_dates) / config["bootstrap_block_sessions"]))
    if not measured:
        return base
    positions = {session: index for index, session in enumerate(calendar_dates)}
    values, counts = np.zeros(len(calendar_dates)), np.zeros(len(calendar_dates))
    for row in measured:
        values[positions[row["session"]]], counts[positions[row["session"]]] = row["lift"], 1
    mean = float(values.sum() / counts.sum())
    block = min(config["bootstrap_block_sessions"], len(values))
    rng = np.random.default_rng(config["bootstrap_seed"])
    starts = rng.integers(0, len(values) - block + 1, size=(config["bootstrap_replicates"], math.ceil(len(values) / block)))
    indices = (starts[:, :, None] + np.arange(block)).reshape(config["bootstrap_replicates"], -1)[:, :len(values)]
    denominators = counts[indices].sum(axis=1)
    sampled = values[indices].sum(axis=1)[denominators > 0] / denominators[denominators > 0]
    boundary = calendar_dates[len(calendar_dates) // 2]
    early = [row["lift"] for row in measured if row["session"] < boundary]
    late = [row["lift"] for row in measured if row["session"] >= boundary]
    return dict(base, mean_signal=float(np.mean([row["mean_signal"] for row in measured])), mean_lift=mean,
                ci_low=float(np.quantile(sampled, .025)), ci_high=float(np.quantile(sampled, .975)),
                p_value=float((1 + (np.abs(sampled - mean) >= abs(mean)).sum()) / (len(sampled) + 1)),
                first_half_lift=float(np.mean(early)) if early else None, second_half_lift=float(np.mean(late)) if late else None)


def summarize(daily, selected, sessions, config):
    from research.scanner_confidence import _benjamini_hochberg
    output = []
    for scope, rows, dates in (("ALL_VARIANTS_DESCRIPTIVE", daily, sessions),
                               ("WALK_FORWARD_SELECTED", selected, sessions[config["training_sessions"]:])):
        grouped = defaultdict(list)
        for row in rows:
            variant = row["variant"] if scope == "ALL_VARIANTS_DESCRIPTIVE" else "TRAINING_SELECTED"
            grouped[(row["family"], variant, row["direction"], row["sample"], row["control"], row["cost_bps"])].append(row)
        variants = [(trial.family, trial.variant) for trial in CONFIGURATIONS] if scope == "ALL_VARIANTS_DESCRIPTIVE" else [(family, "TRAINING_SELECTED") for family in CENTRAL_VARIANTS]
        for family, variant in variants:
            for direction in (1, -1):
                for sample in ("sample-1", "sample-2"):
                    for control in ("ELIGIBLE_EQUAL_WEIGHT", "MOMENTUM_12_1", "MATCHED_STRENGTH_VOLATILITY"):
                        for bps in config["cost_scenarios_bps"]:
                            key = (family, variant, direction, sample, control, bps)
                            output.append(dict(scope=scope, family=family, variant=variant, direction=direction, sample=sample,
                                               control=control, cost_bps=bps, **bootstrap_summary(grouped[key], dates, config)))
    for scope in ("ALL_VARIANTS_DESCRIPTIVE", "WALK_FORWARD_SELECTED"):
        for sample in ("sample-1", "sample-2"):
            family = [row for row in output if row["scope"] == scope and row["sample"] == sample]
            adjusted = _benjamini_hochberg(pd.Series([row["p_value"] if row["p_value"] is not None else 1. for row in family]))
            for index, row in enumerate(family):
                row["fdr_cells"] = len(family)
                row["q_value"] = float(adjusted.iloc[index]) if row["p_value"] is not None else None
                if row["measured_dates"] < config["minimum_test_dates"] or row["signals"] < config["minimum_test_signals"]:
                    state = "INSUFFICIENT_EVIDENCE"
                elif row["coverage"] < config["minimum_pair_coverage"] or row["unmatched_signals"]:
                    state = "INCOMPLETE_COMPARISON"
                elif not all(row[name] is not None and row[name] > 0 for name in ("mean_signal", "mean_lift", "first_half_lift", "second_half_lift")):
                    state = "NO_STABLE_INCREMENTAL_EDGE"
                elif row["ci_low"] <= 0 or row["q_value"] is None or row["q_value"] > .05:
                    state = "NOT_SUPPORTED_AFTER_UNCERTAINTY"
                else:
                    state = "RESEARCH_CANDIDATE_NOT_CERTIFIED"
                row["evidence_status"] = state
    return output


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")


def validate_configuration(config):
    expected = {}
    for family in CENTRAL_VARIANTS:
        trials = [trial for trial in CONFIGURATIONS if trial.family == family]
        parameter = "rs_threshold" if "resumption" in family else "chase_atr" if "acceptance" in family else "failure_sessions"
        expected[family] = {parameter: [getattr(trial, parameter) for trial in trials], "horizon": trials[0].horizon}
    if config["parameter_grid"] != expected or config["feature_warmup_bars"] != 200:
        raise ValueError("configuration differs from the implemented fixed nine-trial detector contract")
    if not config["no_data_purge"] or config["production_publication"]:
        raise ValueError("v2 runner is research-only and cannot purge data or publish")
    if config["selection_cost_bps"] not in config["cost_scenarios_bps"] or config["label_purge_sessions"] < 21:
        raise ValueError("invalid cost or forward-label purge contract")


def signal_conflicts(events):
    groups = defaultdict(list)
    for event in events:
        groups[(event["ticker"], event["session"])].append(event)
    return [dict(ticker=ticker, session=session, state="OPPOSED_SIGNALS_ABSTAIN", event_ids=[event["event_id"] for event in rows])
            for (ticker, session), rows in sorted(groups.items()) if len({event["direction"] for event in rows}) > 1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-dir", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=BACKEND_DIR.parent / "docs" / "equity_strategy_v2_config.json")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, choices=range(1, 5), default=4)
    parser.add_argument("--pilot", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    validate_configuration(config)
    scope = load_batch_scope(args.batch_dir)
    plan = scope["plan"]
    contract = dict(configuration=config, configurations=[trial.__dict__ for trial in CONFIGURATIONS],
                    parent_plan_sha256=digest(plan), source_cutoff=plan["source_cutoff"], pilot=args.pilot,
                    code_sha256={str(path.relative_to(BACKEND_DIR.parent)): hash_file(path) for path in
                                 (Path(__file__), BACKEND_DIR / "research" / "strategy_v2.py")})
    if not args.execute:
        print(json.dumps(contract, indent=2))
        return 0
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if any(args.output_dir.iterdir()):
        raise ValueError("v2 requires a new empty output directory; existing studies are never overwritten")
    write_json(args.output_dir / "contract.json", contract)
    for variable in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        os.environ[variable] = "1"
    frames, bars, reader, samples, provenance = load_inputs(plan, args.pilot)
    write_json(args.output_dir / "inputs.json", provenance)
    print(f"Detecting nine configurations with {args.workers} workers", flush=True)
    events, transitions = collect_detections(frames, args.workers)
    if args.pilot:
        serial_events, serial_transitions = collect_detections(frames, 1)
        if digest(serial_events) != digest(events) or digest(serial_transitions) != digest(transitions):
            raise ValueError("serial/parallel v2 parity failed")
    for filename, records in (("events.jsonl", events), ("transitions.jsonl", transitions)):
        with (args.output_dir / filename).open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, sort_keys=True) + "\n")
    write_json(args.output_dir / "conflicts.json", signal_conflicts(events))
    print(f"Detected {len(events)} events; evaluating plans without database writes", flush=True)
    outcomes = [evaluate_event(event, reader, date.fromisoformat(plan["end"])) for event in events]
    for event, outcome in zip(events, outcomes):
        status = "FILLED" if outcome["entry_state"] == "FILLED" else "ENTRY_UNRESOLVED" if outcome["entry_state"] == "UNRESOLVED_ENTRY" else "NOT_FILLED"
        transitions.append(dict(setup_id=event["setup_id"], family=event["family"], variant=event["variant"],
                                ticker=event["ticker"], direction=event["direction"],
                                session=str(exchange_calendars.get_calendar("XNYS").next_session(event["session"]).date()),
                                status=status, reason=outcome["entry_state"]))
    with (args.output_dir / "execution_transitions.jsonl").open("w", encoding="utf-8") as handle:
        for record in transitions:
            if record["status"] in ("FILLED", "NOT_FILLED", "ENTRY_UNRESOLVED"):
                handle.write(json.dumps(record, sort_keys=True) + "\n")
    with (args.output_dir / "outcomes.jsonl").open("w", encoding="utf-8") as handle:
        for record in outcomes:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
    with (args.output_dir / "control_selections.jsonl").open("w", encoding="utf-8") as handle:
        daily = build_comparisons(events, outcomes, frames, samples, config, lambda record: handle.write(json.dumps(record) + "\n"))
    if args.pilot:
        write_json(args.output_dir / "report.json", dict(contract=contract, parity="PASS", events=len(events),
                   transitions=len(transitions), daily_comparisons=len(daily),
                   outcome_states=dict(Counter(row["entry_state"] for row in outcomes))))
        print(json.dumps(dict(pilot="PASS", events=len(events), daily_comparisons=len(daily),
                             outcome_states=dict(Counter(row["entry_state"] for row in outcomes)))))
        return 0
    sessions = [str(value.date()) for value in exchange_calendars.get_calendar("XNYS").sessions_in_range(plan["start"], plan["end"])]
    folds, selected = walk_forward(daily, sessions, config)
    print("Computing calendar-block uncertainty for the fixed trial family", flush=True)
    summary = summarize(daily, selected, sessions, config)
    pd.DataFrame(daily).to_csv(args.output_dir / "daily_comparisons.csv", index=False)
    pd.DataFrame(summary).to_csv(args.output_dir / "summary.csv", index=False)
    write_json(args.output_dir / "folds.json", folds)
    result = dict(contract=contract, events=len(events), transitions=len(transitions),
                  outcome_states=dict(Counter(row["entry_state"] for row in outcomes)),
                  walk_forward_folds=len(folds), selected_folds=sum(row["chosen"] is not None for row in folds),
                  evidence_states=dict(Counter(row["evidence_status"] for row in summary if row["scope"] == "WALK_FORWARD_SELECTED")),
                  summary=summary, database_mutations=False, live_changes=False,
                  artifacts={path.name: hash_file(path) for path in args.output_dir.iterdir() if path.is_file()})
    write_json(args.output_dir / "report.json", result)
    print(json.dumps({key: value for key, value in result.items() if key not in ("contract", "summary", "artifacts")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())