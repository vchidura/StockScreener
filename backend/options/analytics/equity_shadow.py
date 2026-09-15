"""Research-only equity context and paired option selection; no publication."""
from datetime import datetime, timezone
from decimal import Decimal
import math
from uuid import UUID
from collections import Counter, defaultdict
from statistics import fmean

import exchange_calendars
import pandas as pd


DIRECTIONAL_STRATEGIES = frozenset({"DIRECTIONAL_LONG_PREMIUM", "DIRECTIONAL_DEBIT_SPREAD"})
HOLDING_SESSIONS = (5, 10, 21)


def aware(value):
    result = datetime.fromisoformat(value) if isinstance(value, str) else value
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("shadow comparison requires timezone-aware timestamps")
    return result.astimezone(timezone.utc)


def horizon_exit(market_time, sessions):
    if sessions not in HOLDING_SESSIONS:
        raise ValueError("shadow horizons are fixed at 5, 10 and 21 sessions")
    calendar = exchange_calendars.get_calendar("XNYS")
    session = pd.Timestamp(aware(market_time)).tz_convert("America/New_York").date()
    if not calendar.is_session(session):
        raise ValueError("option decision must belong to a trading session")
    return calendar.session_close(calendar.session_offset(session, sessions)).to_pydatetime()


def equity_alignment(candidate, evidence_rows):
    market, observed = aware(candidate["market_data_time"]), aware(candidate["observed_time"])
    if observed < market:
        raise ValueError("candidate observation precedes its market data")
    visible = [row for row in evidence_rows if row["ticker"] == candidate["underlying"] and row["interval_key"] == "1d"
               and row["evidence_type"] == "TRADE_SETUP" and row["availability_mode"] == "LIVE_OBSERVED"
               and aware(row["market_time"]) <= market and aware(row["observed_at"]) <= observed
               and aware(row["created_at"]) <= observed]
    visible.sort(key=lambda row: (aware(row["market_time"]), aware(row["observed_at"]), aware(row["created_at"])), reverse=True)
    if not visible:
        return dict(alignment="UNAVAILABLE", reason="NO_DECISION_VISIBLE_DAILY_SETUP", equity_evidence_id=None)
    latest = visible[0]
    if len(visible) > 1 and all(visible[0][field] == visible[1][field] for field in ("market_time", "observed_at", "created_at")):
        return dict(alignment="UNAVAILABLE", reason="AMBIGUOUS_EQUITY_SETUP", equity_evidence_id=None)
    result = dict(equity_evidence_id=str(latest["evidence_id"]), equity_market_time=aware(latest["market_time"]).isoformat(),
                  equity_observed_at=aware(latest["observed_at"]).isoformat(), equity_created_at=aware(latest["created_at"]).isoformat(),
                  model_version=latest["model_version"], qualification="UNQUALIFIED_RESEARCH_CONTEXT",
                  horizon_claim="DAILY_SETUP_TRANSFER_TEST_NOT_HORIZON_CALIBRATED")
    if aware(latest["observed_at"]) < aware(latest["market_time"]):
        return result | dict(alignment="UNAVAILABLE", reason="INVALID_EQUITY_AVAILABILITY_CLOCKS")
    calendar = exchange_calendars.get_calendar("XNYS")
    source_date = pd.Timestamp(latest["market_time"]).tz_convert("America/New_York").date()
    decision_date = pd.Timestamp(market).tz_convert("America/New_York").date()
    elapsed = len(calendar.sessions_in_range(source_date, decision_date)) - 1
    if elapsed > 1 or latest["quality_state"] in ("STALE", "FAILED") or latest.get("lifecycle_status") in ("INVALIDATED", "EXPIRED", "UNAVAILABLE") \
            or (latest.get("valid_until") and aware(latest["valid_until"]) <= market):
        return result | dict(alignment="UNAVAILABLE", reason="STALE_OR_INVALID_EQUITY_SETUP")
    if latest.get("lifecycle_status") == "CONFLICTED":
        return result | dict(alignment="CONFLICTED", reason="EQUITY_SETUP_CONFLICT")
    direction = {1: "BULLISH", -1: "BEARISH"}.get(latest.get("direction"))
    if direction is None:
        return result | dict(alignment="NEUTRAL", reason="NO_DIRECTIONAL_SETUP")
    thesis = candidate.get("primary_evidence", {}).get("directional_thesis")
    if candidate["candidate_kind"] == "RESEARCH_ONLY" or thesis not in ("BULLISH", "BEARISH"):
        return result | dict(alignment="NOT_DIRECTIONAL", reason="RAW_ACTIVITY_IS_NOT_A_DIRECTIONAL_TRADE", equity_direction=direction)
    return result | dict(alignment="ALIGNED" if thesis == direction else "OPPOSED", equity_direction=direction,
                         option_direction=thesis, reason="RESEARCH_DIRECTION_COMPARISON")


def select_shadow_pair(candidates, legs_by_candidate, annotations, sessions):
    if not candidates:
        raise ValueError("shadow pair requires a decision cohort")
    group_keys = {(str(row["matrix_id"]), row["underlying"], row["strategy_name"], row["structure_type"]) for row in candidates}
    if len(group_keys) != 1:
        raise ValueError("compare within one underlyer, matrix, strategy and structure")
    eligible, exclusions = [], []
    for row in sorted(candidates, key=lambda row: (row["candidate_rank"], str(row["candidate_id"]))):
        identity = str(row["candidate_id"])
        legs = legs_by_candidate.get(identity, [])
        target = horizon_exit(row["market_data_time"], sessions)
        reason = None
        if row["strategy_name"] not in DIRECTIONAL_STRATEGIES or row["candidate_kind"] == "RESEARCH_ONLY":
            reason = "DETECTOR_OR_UNSUPPORTED_STRATEGY_RETAINED_AS_CONTEXT"
        elif not legs or not row.get("capital_at_risk") or float(row["capital_at_risk"]) <= 0:
            reason = "NO_PRICED_DEFINED_CAPITAL_PACKAGE"
        elif any(pd.Timestamp(leg["expiration_date"]).date() <= target.date() for leg in legs):
            reason = "EXPIRATION_NOT_BEYOND_PLANNED_EXIT"
        if reason:
            exclusions.append(dict(candidate_id=identity, reason=reason))
        else:
            eligible.append(row)
    baseline = eligible[0] if eligible else None
    aligned = next((row for row in eligible if annotations[str(row["candidate_id"])]["alignment"] == "ALIGNED"), None)
    return dict(holding_sessions=sessions, planned_exit=horizon_exit(candidates[0]["market_data_time"], sessions).isoformat(),
                option_only_candidate_id=str(baseline["candidate_id"]) if baseline else None,
                equity_aligned_candidate_id=str(aligned["candidate_id"]) if aligned else None,
                equity_arm_status="SELECTED_SHADOW_ONLY" if aligned else "ABSTAIN_NO_ALIGNED_ELIGIBLE_CANDIDATE",
                eligible_candidates=len(eligible), exclusions=exclusions,
                selection_uses_outcomes=False, execution_gates_overridden=False)


def measure_shadow_outcome(candidate, legs, snapshots, sessions, available_by, policy):
    from options.domain import MarkSource
    from options.outcomes import OptionOutcomeLeg, evaluate_delayed_proxy_outcome
    from options.strategies.domain import OptionSide

    target, cutoff = horizon_exit(candidate["market_data_time"], sessions), aware(available_by)
    base = dict(candidate_id=str(candidate["candidate_id"]), holding_sessions=sessions, planned_exit=target.isoformat(),
                net_return=None, net_pnl=None, classification="INDICATIVE_OPTION_MARKS_NOT_EXECUTED_TRADES")
    if not legs or len({row["contract_id"] for row in legs}) != len(legs):
        return base | dict(status="NO_UNIQUE_ORDERED_PACKAGE")
    if any(pd.Timestamp(row["expiration_date"]).date() <= target.date() for row in legs):
        return base | dict(status="EXPIRATION_NOT_BEYOND_PLANNED_EXIT")
    allowed_entry = {source.value for source in policy.allowed_entry_mark_sources}
    for row in legs:
        if row["valuation_policy_sha256"] != policy.policy_sha256:
            return base | dict(status="ENTRY_VALUATION_POLICY_UNAVAILABLE_OR_MISMATCHED")
        if row["mark_source"] not in allowed_entry or row.get("model_mark") is None \
                or not math.isfinite(float(row["model_mark"])) or float(row["model_mark"]) <= 0:
            return base | dict(status="ENTRY_MARK_UNAVAILABLE_OR_UNAPPROVED")
        if int(row["multiplier"]) != 100:
            return base | dict(status="NONSTANDARD_DELIVERABLE_UNSUPPORTED")
        mark_time = aware(row["source_market_time"])
        age = (aware(candidate["market_data_time"]) - mark_time).total_seconds()
        if not 0 <= age <= policy.maximum_source_age_seconds or row.get("entry_first_observed_at") is None \
                or aware(row["entry_first_observed_at"]) > aware(candidate["observed_time"]):
            return base | dict(status="ENTRY_PRICE_NOT_DECISION_VISIBLE")
    if cutoff < target:
        return base | dict(status="NOT_MATURE")
    if not candidate.get("capital_at_risk") or not math.isfinite(float(candidate["capital_at_risk"])) or float(candidate["capital_at_risk"]) <= 0:
        return base | dict(status="CAPITAL_AT_RISK_UNAVAILABLE")
    contracts = {row["contract_id"]: row for row in legs}
    batches = {}
    allowed_exit = {source.value for source in policy.allowed_exit_mark_sources}
    for snapshot in snapshots:
        contract = contracts.get(snapshot["contract_id"])
        if contract is None or aware(snapshot["scheduled_cycle"]) != target or aware(snapshot["market_data_time"]) > target \
            or aware(snapshot["first_observed_at"]) > cutoff \
                or aware(snapshot["created_at"]) > cutoff or (snapshot.get("revised_observed_at") and aware(snapshot["revised_observed_at"]) > cutoff):
            continue
        if snapshot["valuation_policy_sha256"] != policy.policy_sha256 or snapshot["mark_source"] not in allowed_exit \
                or snapshot.get("model_mark") is None or not math.isfinite(float(snapshot["model_mark"])) or float(snapshot["model_mark"]) <= 0:
            continue
        if snapshot["shares_per_contract"] != contract["multiplier"] or Decimal(str(snapshot["strike"])) != Decimal(str(contract["strike"])) \
                or snapshot["contract_type"] != contract["contract_type"] \
                or pd.Timestamp(snapshot["expiration_date"]).date() != pd.Timestamp(contract["expiration_date"]).date():
            continue
        mark_time = aware(snapshot["mark_market_data_time"])
        if not 0 <= (target - mark_time).total_seconds() <= policy.maximum_source_age_seconds \
                or mark_time <= aware(candidate["market_data_time"]) or aware(snapshot["first_observed_at"]) < target \
                or abs((mark_time - aware(snapshot["spot_market_data_time"])).total_seconds()) > policy.maximum_option_spot_skew_seconds:
            continue
        batch = batches.setdefault(str(snapshot["batch_id"]), {})
        prior = batch.get(snapshot["contract_id"])
        order = lambda row: (aware(row["first_observed_at"]), int(row["revision"]), str(row["snapshot_id"]))
        if prior is None or order(snapshot) > order(prior):
            batch[snapshot["contract_id"]] = snapshot
    complete = [rows for rows in batches.values() if set(rows) == set(contracts)]
    if not complete:
        return base | dict(status="COHERENT_EXIT_MARK_UNAVAILABLE")
    chosen = min(complete, key=lambda rows: (max(aware(row["first_observed_at"]) for row in rows.values()), str(next(iter(rows.values()))["batch_id"])))
    ordered = sorted(legs, key=lambda row: row["leg_index"])
    outcome_legs = tuple(OptionOutcomeLeg(
        contract_id=row["contract_id"], side=OptionSide(row["side"]), ratio=row["ratio"], multiplier=row["multiplier"],
        entry_mark=Decimal(str(row["model_mark"])), exit_mark=Decimal(str(chosen[row["contract_id"]]["model_mark"])),
        source_snapshot_id=UUID(str(chosen[row["contract_id"]]["snapshot_id"])),
        source_batch_id=UUID(str(chosen[row["contract_id"]]["batch_id"])),
        source_market_time=aware(chosen[row["contract_id"]]["mark_market_data_time"]),
        source_observed_time=aware(chosen[row["contract_id"]]["first_observed_at"]),
        entry_mark_source=MarkSource(row["mark_source"]), exit_mark_source=MarkSource(chosen[row["contract_id"]]["mark_source"]),
        entry_valuation_policy_sha256=row["valuation_policy_sha256"], exit_valuation_policy_sha256=chosen[row["contract_id"]]["valuation_policy_sha256"],
    ) for row in ordered)
    outcome = evaluate_delayed_proxy_outcome(
        candidate_id=UUID(str(candidate["candidate_id"])), event_id=None, measurement_type="CURRENT", market_time=target,
        observed_time=max(row.source_observed_time for row in outcome_legs), capital_at_risk=Decimal(str(candidate["capital_at_risk"])),
        legs=outcome_legs, policy=policy,
    )
    return base | dict(status="MEASURED_INDICATIVE", net_return=float(outcome.net_return), net_pnl=float(outcome.net_pnl),
                       gross_pnl=float(outcome.gross_pnl), estimated_commission=float(outcome.estimated_cost),
                       capital_at_risk=float(outcome.capital_at_risk), valuation_policy_sha256=policy.policy_sha256,
                       source_snapshot_ids=[str(row.source_snapshot_id) for row in outcome_legs], source_batch_id=str(outcome.source_batch_id),
                       exit_observed_at=outcome.observed_time.isoformat(), quality_flags=list(outcome.quality_flags),
                       iv_changes=[dict(contract_id=row["contract_id"], entry_iv=row.get("local_iv"), exit_iv=chosen[row["contract_id"]].get("local_iv")) for row in ordered])


def first_decision_cohorts(candidates):
    grouped = defaultdict(list)
    for candidate in candidates:
        session = pd.Timestamp(aware(candidate["market_data_time"])).tz_convert("America/New_York").date().isoformat()
        key = (session, candidate["underlying"], candidate["strategy_name"], candidate["structure_type"])
        grouped[key].append(candidate)
    selected = []
    for key, rows in sorted(grouped.items()):
        first = min(rows, key=lambda row: (aware(row["observed_time"]), aware(row["market_data_time"]), str(row["matrix_id"])))
        selected.append((key, [row for row in rows if row["matrix_id"] == first["matrix_id"]]))
    return selected


def summarize_shadow_pairs(pairs):
    grouped = defaultdict(list)
    for pair in pairs:
        grouped[(pair["strategy_name"], pair["structure_type"], pair["holding_sessions"])].append(pair)
    output = []
    for (strategy, structure, horizon), rows in sorted(grouped.items()):
        measured, matched, opportunity = [], [], []
        for row in rows:
            baseline, aligned = row["option_only_outcome"], row["equity_aligned_outcome"]
            if baseline["status"] != "MEASURED_INDICATIVE":
                continue
            measured.append(row)
            if aligned["status"] == "MEASURED_INDICATIVE":
                value = dict(session=row["entry_session"], baseline=baseline["net_return"], aligned=aligned["net_return"])
                matched.append(value)
                opportunity.append(value)
            elif aligned["status"] == "ABSTAIN_CASH":
                opportunity.append(dict(session=row["entry_session"], baseline=baseline["net_return"], aligned=0.0))
        def metrics(records):
            daily = defaultdict(list)
            for row in records:
                daily[row["session"]].append(row["aligned"] - row["baseline"])
            return dict(pairs=len(records), distinct_entry_sessions=len(daily),
                        equal_entry_session_mean_difference=fmean(fmean(values) for values in daily.values()) if daily else None,
                        option_only_mean=fmean(row["baseline"] for row in records) if records else None,
                        equity_arm_mean=fmean(row["aligned"] for row in records) if records else None)
        packages = [(row["underlying"], tuple(row.get("option_only_contract_ids", []))) for row in rows if row["option_only_candidate_id"]]
        output.append(dict(strategy_name=strategy, structure_type=structure, holding_sessions=horizon,
                           opportunity_cohorts=len(rows), distinct_entry_sessions=len({row["entry_session"] for row in rows}),
                           distinct_option_only_packages=len(set(packages)), repeated_package_occurrences=len(packages) - len(set(packages)),
                           option_only_measured=len(measured), equity_abstentions=sum(row["equity_aligned_outcome"]["status"] == "ABSTAIN_CASH" for row in rows),
                           option_only_outcome_states=dict(Counter(row["option_only_outcome"]["status"] for row in rows)),
                           equity_outcome_states=dict(Counter(row["equity_aligned_outcome"]["status"] for row in rows)),
                           matched_measured=metrics(matched), common_opportunity_with_cash_abstention=metrics(opportunity),
                           inference="DESCRIPTIVE_OVERLAPPING_HORIZONS_NO_SIGNIFICANCE_OR_PORTFOLIO_DRAWDOWN",
                           evidence_status="NO_MEASURED_COMPARISON" if not opportunity else "INDICATIVE_NOT_QUALIFIED"))
    return output