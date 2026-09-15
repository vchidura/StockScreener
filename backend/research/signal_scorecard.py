"""Read-only signal evidence inventory and version-specific research scorecards."""
from collections import Counter, defaultdict
import math

import exchange_calendars
import numpy as np
import pandas as pd

from research.composite_scanners import COMPOSITE_OUTCOME_HORIZONS, COMPOSITE_SCANNER_REGISTRY


def registered_signal_cells():
    return [dict(source_name=registration.source_name, source_version=registration.source_version,
                 interval=interval, direction=direction, horizon_key=horizon, horizon_bars=bars, return_mode=mode)
            for registration in COMPOSITE_SCANNER_REGISTRY.values()
            for interval in registration.supported_intervals
            for direction in (1, -1)
            for horizon, bars in COMPOSITE_OUTCOME_HORIZONS[interval].items()
            for mode in registration.outcome_modes]


def matching_cell_policies(cell, policies):
    return [policy for policy in policies if all(policy[field] == cell[field] for field in ("source_name", "source_version", "interval"))
            and policy["evidence_type"] == "SCANNER_RESULT" and policy["horizons"].get(cell["horizon_key"]) == cell["horizon_bars"]
            and ("RECOMMENDATION_PLAN" if policy["success_definition"].get("exit_model") == "FIRST_STOP_TARGET_OR_HORIZON_CLOSE"
                 else "DIRECTIONAL_HORIZON") == cell["return_mode"]]


def adapter_signal_cells(adapter_names):
    from research.historical_signal_replay import BUILTIN_ADAPTERS, PATTERN_NAMES
    from equity.historical_research import GAP_PRIMARY_SOURCES

    cells = []
    for name in sorted(set(adapter_names)):
        adapter = BUILTIN_ADAPTERS[name]
        if name == "composite-scanners-1d-v1":
            cells.extend(row for row in registered_signal_cells() if row["interval"] == "1d")
            continue
        sources = (GAP_PRIMARY_SOURCES if name == "gap-formation-v2" else
                   tuple(f"PATTERN_{pattern}_BOUNDARY_BREAK" for pattern in PATTERN_NAMES)
                   if name == "pattern-boundary-break-1d-v1" else (adapter.source_name,))
        for source in sources:
            modes = ("DIRECTIONAL_HORIZON", "RECOMMENDATION_PLAN") if source.startswith("GAP_") else ("DIRECTIONAL_HORIZON",)
            cells.extend(dict(source_name=source, source_version=adapter.source_version, interval="1d", direction=direction,
                              horizon_key=f"{horizon}d", horizon_bars=horizon, return_mode=mode)
                         for direction in (1, -1) for horizon in (5, 10, 21) for mode in modes)
    return cells


def evidence_role_contract(evidence_type, evidence_role):
    if evidence_role == "LOCATION" or evidence_type == "PRICE_CHANNEL":
        return "CONDITIONAL_LOCATION_NOT_STANDALONE_DIRECTION"
    if evidence_role == "PARTICIPATION":
        return "INCREMENTAL_PARTICIPATION_NOT_DIRECTIONAL_VOTE"
    if evidence_role in ("RISK", "REGIME"):
        return "CONDITIONAL_RISK_OR_REGIME_INFORMATION"
    if evidence_role == "TRIGGER":
        return "TIMING_VERSUS_IMMEDIATE_AND_DELAYED_ENTRY"
    if evidence_type == "SCANNER_RESULT" and evidence_role == "DIRECTION":
        return "DIRECTIONAL_AND_DECLARED_PLAN_OUTCOMES"
    if evidence_type == "TRADE_SETUP":
        return "COMPOSITE_SETUP_REQUIRES_OWN_QUALIFICATION"
    return "DESCRIPTIVE_OR_REQUIRES_EXPLICIT_OUTCOME_CONTRACT"


def coverage_status(events, evaluated, usable):
    if not 0 <= usable <= evaluated <= events:
        raise ValueError("coverage counts must satisfy usable <= evaluated <= events")
    if events == 0:
        return "NO_EVENTS"
    if evaluated == 0:
        return "NO_RETAINED_OUTCOMES"
    if usable == 0:
        return "NO_USABLE_OUTCOMES"
    return "PARTIAL_OUTCOMES" if usable < events else "COVERED_NOT_QUALIFIED"


def timestamp(value):
    result = pd.Timestamp(value)
    if pd.isna(result) or result.tzinfo is None:
        raise ValueError("signal evidence clocks must be present and timezone-aware")
    return result.tz_convert("UTC")


def deduplicate_lifecycles(events):
    selected = {}
    ordered = sorted(events, key=lambda row: (timestamp(row["market_time"]), timestamp(row["observed_at"]), str(row["evidence_id"])))
    for row in ordered:
        key = tuple(row[field] for field in ("source_name", "source_version", "interval", "direction", "origin", "run_purpose", "security_id"))
        key += (row.get("lifecycle_key") or str(row["evidence_id"]),)
        selected.setdefault(key, row)
    return list(selected.values()), len(events) - len(selected)


def outcome_usability(event, outcome, policy, cutoff):
    if outcome is None:
        return "NO_RETAINED_OUTCOME"
    if outcome["is_stale"]:
        return "STALE_OUTCOME"
    if outcome["entry_status"] != "ENTERED":
        return "ENTRY_" + outcome["entry_status"]
    if event["quality_state"] in ("FAILED", "STALE"):
        return "INVALID_SIGNAL_INPUTS"
    primary = policy["benchmark_policy"].get("primary", "MARKET")
    if primary not in ("MARKET", "SECTOR"):
        return "UNSUPPORTED_BENCHMARK"
    if any(outcome.get(field) is None or not math.isfinite(float(outcome[field])) for field in
           ("net_return", "sector_net_alpha" if primary == "SECTOR" else "net_alpha")):
        return "MISSING_RETURN_OR_PRIMARY_BENCHMARK"
    if str(outcome["outcome_policy_id"]) != str(policy["outcome_policy_id"]) or str(outcome["subject_evidence_id"]) != str(event["evidence_id"]):
        return "IDENTITY_MISMATCH"
    try:
        market, decision, entry, exit_time, available, created = [timestamp(value) for value in (
            event["market_time"], event["observed_at"], outcome["entry_time"], outcome["exit_time"],
            outcome["outcome_available_at"], outcome["created_at"],
        )]
        if not market <= decision < entry < exit_time <= available <= timestamp(cutoff) or created > timestamp(cutoff) \
                or timestamp(outcome["signal_time"]) != decision:
            return "INVALID_CAUSAL_TIMING"
    except (ValueError, TypeError):
        return "INVALID_CAUSAL_TIMING"
    return "USABLE"


def policy_event_scope(events, policy, horizon, outcome_map):
    if policy is None:
        return [], dict(policy_application="NO_EXACT_POLICY", outside_policy_window=len(events), retrospective_policy_events=0)
    within = [row for row in events if timestamp(policy["effective_from"]) <= timestamp(row["observed_at"])
              and (policy["effective_to"] is None or timestamp(row["observed_at"]) < timestamp(policy["effective_to"]))]
    replay = bool(events) and all(row["origin"] == "HISTORICAL_RECONSTRUCTED" and row["run_purpose"] == "RECONSTRUCTED_LEDGER" for row in events)
    linked = any((str(row["evidence_id"]), str(policy["outcome_policy_id"]), horizon) in outcome_map for row in events)
    if replay and linked:
        return events, dict(policy_application="EXPLICIT_RECONSTRUCTED_OUTCOME_POLICY", outside_policy_window=len(events)-len(within),
                            retrospective_policy_events=len(events)-len(within))
    return within, dict(policy_application="RECORDED_EFFECTIVE_WINDOW", outside_policy_window=len(events)-len(within), retrospective_policy_events=0)


def daily_statistics(observations, horizon):
    from equity.qualification import _independent_periods, _mean, _student_t_p_value, _t_stat, _wilson_interval

    empty = dict(measured_events=0, signal_dates=0, distinct_tickers=0, independent_periods=0,
                 mean_net_return=None, mean_primary_alpha=None, mean_mae_pct=None, mean_mfe_pct=None,
                 net_return_t_stat=None, alpha_t_stat=None, alpha_p_value=None, hit_rate=None, hit_rate_ci_low=None,
                 hit_rate_ci_high=None, early_mean_alpha=None, late_mean_alpha=None, early_mean_return=None,
                 late_mean_return=None, top5_event_concentration=None)
    if not observations:
        return empty
    frame = pd.DataFrame(observations)
    for field in ("signal_time", "signal_market_time", "entry_time", "exit_time", "outcome_available_at"):
        frame[field] = pd.to_datetime(frame[field], utc=True)
    frame["signal_session"] = frame["signal_market_time"].dt.date
    portfolio = frame.groupby("signal_session", as_index=False).agg(
        signal_time=("signal_time", "max"), signal_market_time=("signal_market_time", "max"),
        entry_time=("entry_time", "min"), exit_time=("exit_time", "max"),
        outcome_available_at=("outcome_available_at", "max"), net_return=("net_return", "mean"),
        primary_alpha=("primary_alpha", "mean"), mae_pct=("mae_pct", "mean"), mfe_pct=("mfe_pct", "mean"),
    )
    if (portfolio["signal_time"] >= portfolio["entry_time"]).any():
        return empty | dict(measured_events=len(frame), cohort_status="INCOMPATIBLE_WITHIN_DATE_ENTRY_TIMING")
    independent = _independent_periods(portfolio, horizon, interval="1d")
    count = len(independent)
    midpoint = max(1, count // 2)
    alpha_t = _t_stat(independent["primary_alpha"])
    low, high = _wilson_interval(int((independent["net_return"] > 0).sum()), count)
    return dict(measured_events=len(frame), signal_dates=len(portfolio), distinct_tickers=int(frame["ticker"].nunique()),
                independent_periods=count, mean_net_return=_mean(independent["net_return"]), mean_primary_alpha=_mean(independent["primary_alpha"]),
                mean_mae_pct=_mean(independent["mae_pct"]), mean_mfe_pct=_mean(independent["mfe_pct"]),
                net_return_t_stat=_t_stat(independent["net_return"]), alpha_t_stat=alpha_t,
                alpha_p_value=_student_t_p_value(alpha_t, count), hit_rate=float((independent["net_return"] > 0).mean()),
                hit_rate_ci_low=low, hit_rate_ci_high=high,
                early_mean_alpha=_mean(independent.iloc[:midpoint]["primary_alpha"]), late_mean_alpha=_mean(independent.iloc[midpoint:]["primary_alpha"]),
                early_mean_return=_mean(independent.iloc[:midpoint]["net_return"]), late_mean_return=_mean(independent.iloc[midpoint:]["net_return"]),
                top5_event_concentration=float(frame["ticker"].value_counts().head(5).sum() / len(frame)))


def build_daily_scorecard(events, outcomes, policies, samples, config, cutoff, *, cells=None, origin_groups=None,
                          aggregate_scope="ALL_RETAINED_TICKERS"):
    from research.scanner_confidence import _benjamini_hochberg

    calendar = exchange_calendars.get_calendar("XNYS")
    deduped, dropped = deduplicate_lifecycles(events)
    outcome_map = {}
    for row in outcomes:
        key = (str(row["subject_evidence_id"]), str(row["outcome_policy_id"]), row["horizon_key"])
        if key in outcome_map:
            raise ValueError("scorecard requires one latest visible outcome per exact subject/policy/horizon")
        outcome_map[key] = row
    grouped, raw_grouped = defaultdict(list), defaultdict(list)
    dimensions = ("source_name", "source_version", "interval", "direction", "origin", "run_purpose")
    for row in deduped:
        grouped[tuple(row[field] for field in dimensions)].append(row)
    for row in events:
        raw_grouped[tuple(row[field] for field in dimensions)].append(row)
    origins = origin_groups if origin_groups is not None else {(row["origin"], row["run_purpose"]) for row in events} | {("HISTORICAL_RECONSTRUCTED", "RECONSTRUCTED_LEDGER"), ("LIVE_OBSERVED", "ORIGINAL")}
    scopes = {aggregate_scope: None, **{key: set(values) for key, values in samples.items()}}
    rows = []
    maturity_by_session = {}
    for cell in (cells if cells is not None else (row for row in registered_signal_cells() if row["interval"] == "1d")):
        matching = matching_cell_policies(cell, policies)
        for origin, purpose in sorted(origins):
            event_key = tuple(cell[field] for field in dimensions[:4]) + (origin, purpose)
            for scope, names in scopes.items():
                scope_events = [row for row in grouped[event_key] if names is None or row["ticker"] in names]
                raw_events = [row for row in raw_grouped[event_key] if names is None or row["ticker"] in names]
                for policy in matching or [None]:
                    eligible, policy_scope = policy_event_scope(scope_events, policy, cell["horizon_key"], outcome_map)
                    states, quality, observations = Counter(), Counter(), []
                    nominal_mature = 0
                    for event in eligible:
                        maturity_key = (timestamp(event["market_time"]).date(), cell["horizon_bars"])
                        if maturity_key not in maturity_by_session:
                            maturity_by_session[maturity_key] = calendar.session_close(calendar.session_offset(*maturity_key))
                        nominal_close = maturity_by_session[maturity_key]
                        nominal_mature += nominal_close <= timestamp(cutoff)
                        outcome = outcome_map.get((str(event["evidence_id"]), str(policy["outcome_policy_id"]), cell["horizon_key"]))
                        state = outcome_usability(event, outcome, policy, cutoff)
                        states[state] += 1
                        quality.update(event.get("quality_codes") or [])
                        if outcome:
                            quality.update(outcome.get("quality_codes") or [])
                        if state == "USABLE":
                            observations.append(dict(ticker=event["ticker"], signal_time=event["observed_at"], signal_market_time=event["market_time"],
                                                     entry_time=outcome["entry_time"], exit_time=outcome["exit_time"], outcome_available_at=outcome["outcome_available_at"],
                                                     net_return=float(outcome["net_return"]),
                                                     primary_alpha=float(outcome["sector_net_alpha"] if policy["benchmark_policy"].get("primary") == "SECTOR" else outcome["net_alpha"]),
                                                     mae_pct=outcome.get("mae_pct"), mfe_pct=outcome.get("mfe_pct")))
                    evaluated = len(eligible) - states["NO_RETAINED_OUTCOME"]
                    statistics = daily_statistics(observations, cell["horizon_bars"])
                    period_metrics = {}
                    for period in ("DEVELOPMENT", "LATER"):
                        subset = [row for row in observations if (timestamp(row["signal_market_time"]).date().isoformat() >= config["later_start"]) == (period == "LATER")]
                        period_metrics[period] = daily_statistics(subset, cell["horizon_bars"])
                    rows.append(dict(cell, sample_scope=scope, origin=origin, run_purpose=purpose,
                                     outcome_policy_id=str(policy["outcome_policy_id"]) if policy else None,
                                     policy_key=policy["policy_key"] if policy else None, policy_sha256=policy["policy_sha256"] if policy else None,
                                     cost_model=policy["cost_model"] if policy else None, primary_benchmark=policy["benchmark_policy"].get("primary", "MARKET") if policy else None,
                                     raw_evidence_records=len(raw_events), lifecycle_events=len(scope_events), repeated_lifecycle_records=len(raw_events)-len(scope_events),
                                     policy_eligible_events=len(eligible), **policy_scope,
                                     nominal_mature_events=int(nominal_mature), evaluated_events=evaluated, usable_outcomes=len(observations),
                                     outcome_states=dict(states), quality_code_occurrences=dict(quality),
                                     data_status=coverage_status(len(eligible), evaluated, len(observations)) if policy else "NO_EXACT_POLICY",
                                     statistics=statistics, periods=period_metrics, individual_probability=None, quality_score=None,
                                     publication_state="REPORT_ONLY_NOT_PUBLISHED"))
    families = defaultdict(list)
    for row in rows:
        families[(row["sample_scope"], row["origin"], row["run_purpose"])].append(row)
    for family in families.values():
        values = [row["statistics"]["alpha_p_value"] if row["statistics"]["alpha_p_value"] is not None else 1.0 for row in family]
        adjusted = _benjamini_hochberg(pd.Series(values, dtype=float))
        for index, row in enumerate(family):
            stats = row["statistics"]
            row["fdr_family_cells"] = len(family)
            row["alpha_fdr_q"] = float(adjusted.iloc[index]) if stats["alpha_p_value"] is not None else None
            if row["usable_outcomes"] == 0:
                status = "NO_USABLE_EVIDENCE"
            elif stats["measured_events"] < config["minimum_events"] or stats["independent_periods"] < config["minimum_independent_periods"]:
                status = "INSUFFICIENT_EVIDENCE"
            elif not all(stats[key] is not None and stats[key] > 0 for key in ("mean_net_return", "mean_primary_alpha", "early_mean_return", "late_mean_return", "early_mean_alpha", "late_mean_alpha")):
                status = "NO_STABLE_POSITIVE_EDGE"
            elif not all(stats[key] is not None and stats[key] > 2 for key in ("net_return_t_stat", "alpha_t_stat")) or row["alpha_fdr_q"] is None or row["alpha_fdr_q"] > .05:
                status = "NOT_SUPPORTED_AFTER_UNCERTAINTY_CHECKS"
            elif row["data_status"] != "COVERED_NOT_QUALIFIED":
                status = "PROMISING_BUT_INCOMPLETE_COVERAGE"
            elif row["retrospective_policy_events"]:
                status = "PROMISING_POLICY_TIMELINE_REVIEW_REQUIRED"
            else:
                status = "RESEARCH_PASS_NOT_CERTIFIED"
            row["evidence_status"] = status
    return rows, dropped