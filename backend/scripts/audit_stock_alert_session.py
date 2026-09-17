"""Read-only review of retained forward alert rules, geometry and paper outcomes."""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
from datetime import date, datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3
from statistics import mean, median
import sys
from zoneinfo import ZoneInfo
import zlib

import exchange_calendars

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from research.stock_idea_engine import entry_gate, read_candidate
from research.stock_idea_forward import build_frame, decision_candidate, forward_config, runtime_sources
from research.stock_idea_models import acceptance_confirmed
from research.stock_idea_replay import available_at, derive_hours, execution_times, mark_position, utc


def clean(value):
    if isinstance(value, dict):
        return {key: clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "item"):
        return clean(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def near(left, right):
    return math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-7)


def eastern(value):
    return utc(value).astimezone(ZoneInfo("America/New_York")).strftime("%H:%M:%S") if value else None


def load_retained(path):
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        policy = json.loads(connection.execute("SELECT payload FROM forward_manifest").fetchone()[0])
        payload = connection.execute("SELECT payload FROM forward_checkpoint").fetchone()[0]
        state = json.loads(zlib.decompress(payload))
        inputs = connection.execute("SELECT payload FROM forward_input_checkpoint").fetchone()[0]
        if state.pop("external_inputs", False):
            state.update(json.loads(zlib.decompress(inputs)))
        publications = [json.loads(zlib.decompress(row[0])) for row in connection.execute(
            "SELECT payload FROM forward_publications ORDER BY window_key")]
        hashes = dict(checkpoint_sha256=hashlib.sha256(payload).hexdigest(), inputs_sha256=hashlib.sha256(inputs).hexdigest(),
            publications_sha256=hashlib.sha256(json.dumps(publications, sort_keys=True).encode()).hexdigest())
    return policy, state, publications, hashes


def candle(row):
    return {key: clean(row[key]) for key in ("bar_start", "bar_end", "open", "high", "low", "close", "volume", "atr_prior", "range_ratio", "ema20", "ema50", "revision_id")}


def geometry_review(candidate, frame, config):
    triggered = frame.index[frame.bar_end.eq(candidate.trigger_at)]
    if len(triggered) != 1:
        return dict(status="TRIGGER_BAR_UNAVAILABLE")
    position = int(triggered[0])
    trigger = frame.iloc[position]
    result = dict(trigger_bar=candle(trigger), trigger_price_matches_bar=near(candidate.price, trigger.close),
        stored_plan_price=candidate.price, trigger_bar_close=float(trigger.close), checks={})
    if position:
        result["trigger_bar_change_pct"] = 100 * (trigger.close / frame.iloc[position - 1].close - 1)
        result["trigger_relative_volume20"] = trigger.volume / frame.iloc[max(0, position - 20):position].volume.mean()
    if candidate.model not in ("acceptance", "failure"):
        anchor_indices = frame.index[frame.bar_start.eq(utc(candidate.anchor))]
        if len(anchor_indices) != 1:
            return result | dict(status="ANCHOR_NOT_RECONSTRUCTED")
        anchor_index = int(anchor_indices[0])
        anchor = frame.iloc[anchor_index]
        reference = float(anchor.high if candidate.direction == 1 else anchor.low)
        episode = frame.iloc[anchor_index + 1:position + 1]
        extreme = float(episode.low.min() if candidate.direction == 1 else episode.high.max())
        activation_rows = []
        for index in range(anchor_index + 1, position):
            row = frame.iloc[index]
            depth = candidate.direction * (reference - row.close)
            window = frame.iloc[max(0, index - 20):index]
            extreme_price = float(window.high.max() if candidate.direction == 1 else window.low.min())
            if near(row.atr_prior, candidate.activation_atr) and near(reference, extreme_price) and row.atr_prior <= depth <= 3 * row.atr_prior:
                activation_rows.append(row)
        previous = frame.iloc[position - 1]
        stop = extreme - candidate.direction * config["stop_buffer_atr"] * trigger.atr_prior
        checks = dict(reference=near(candidate.reference, reference), target=near(candidate.target, reference),
            stop=near(candidate.stop, stop), activation_found=len(activation_rows) == 1,
            trigger_age=2 <= trigger.ordinal - anchor.ordinal <= 10,
            depth_still_valid=candidate.direction * (reference - extreme) <= 3 * candidate.activation_atr,
            close_through_previous=candidate.direction * (trigger.close - (previous.high if candidate.direction == 1 else previous.low)) > 0,
            close_through_ema20=candidate.direction * (trigger.close - trigger.ema20) > 0)
        return result | dict(status="GEOMETRY_RECONSTRUCTED", checks=checks, anchor_bar=candle(anchor),
            activation_bar=candle(activation_rows[0]) if len(activation_rows) == 1 else None,
            episode_extreme=extreme, stop_expected=stop)
    width = 10 if candidate.model == "acceptance" else 20
    matches = [index for index in range(width, position + 1)
        if f"{frame.bar_start.iloc[index - width]}:{frame.bar_start.iloc[index - 1]}" == candidate.anchor]
    if len(matches) != 1:
        return result | dict(status="ANCHOR_NOT_RECONSTRUCTED")
    activation_index = matches[0]
    activation = frame.iloc[activation_index]
    window = frame.iloc[activation_index - width:activation_index]
    episode = frame.iloc[activation_index:position + 1]
    high, low = float(window.high.max()), float(window.low.min())
    extreme = float(episode.low.min() if candidate.direction == 1 else episode.high.max())
    reference = high if (candidate.model == "acceptance") == (candidate.direction == 1) else low
    target = reference + candidate.direction * (high - low) if candidate.model == "acceptance" else (high + low) / 2
    stop = extreme - candidate.direction * config["stop_buffer_atr"] * trigger.atr_prior
    checks = dict(anchor_contiguous=window.segment.nunique() == 1 and window.segment.iloc[-1] == activation.segment,
        reference=near(candidate.reference, reference), target=near(candidate.target, target),
        activation_atr=near(candidate.activation_atr, activation.atr_prior), stop=near(candidate.stop, stop))
    age = int(trigger.ordinal - activation.ordinal)
    if candidate.model == "acceptance":
        checks.update(range_width=0 < high - low <= 4 * activation.atr_prior,
            contraction=activation.range_ratio <= .75,
            previous_close_inside=low <= window.close.iloc[-1] <= high,
            breakout=candidate.direction * (activation.close - reference) > .15 * activation.atr_prior,
            next_bar_confirmation=age == 1 and candidate.direction * (trigger.close - reference) > 0)
    else:
        extension = activation.low if candidate.direction == 1 else activation.high
        checks.update(previous_close_inside=low <= window.close.iloc[-1] <= high,
            outside_close=candidate.direction * (activation.close - reference) < 0,
            extension=candidate.direction * (reference - extension) >= .5 * activation.atr_prior,
            timely_reentry=1 <= age <= 3 and candidate.direction * (trigger.close - reference) >= .1 * activation.atr_prior,
            reversal_body=candidate.direction * (trigger.close - trigger.open) > 0,
            reversal_close=candidate.direction * (trigger.close - frame.iloc[position - 1].close) > 0)
    return result | dict(status="GEOMETRY_RECONSTRUCTED", checks=checks,
        range_high=high, range_low=low, range_width=high - low,
        range_width_atr=(high - low) / activation.atr_prior, activation_bar=candle(activation),
        episode_extreme=extreme, stop_expected=stop, stop_buffer=config["stop_buffer_atr"] * trigger.atr_prior,
        activation_to_trigger_bars=age, range_bars=[candle(row) for _, row in window.iterrows()],
        episode_bars=[candle(row) for _, row in episode.iterrows()],
        first_break_above_boundary_pct=100 * candidate.direction * (activation.close / reference - 1),
        trigger_beyond_boundary_pct=100 * candidate.direction * (trigger.close / reference - 1))


def audit(policy, state, publications, session, focus):
    selected_publications = [row for row in publications if row["session"] == session]
    selected_ids = [key for row in selected_publications for key in row["selected"]]
    frames = {}
    rows = []
    cutoff_now = utc(state["last_source_read"])
    current_hashes = runtime_sources()
    calendar = exchange_calendars.get_calendar("XNYS")
    prior_session = str(calendar.previous_session(session).date())
    for publication in selected_publications:
        for episode_id in publication["selected"]:
            candidate = read_candidate(publication["candidates"][episode_id])
            if candidate.model == "discovery":
                continue
            cutoff = utc(publication["input_deadline"])
            published = utc(publication["actual_publication_at"])
            native = list(state["bars"].get(candidate.security_id + "|30m", {}).values())
            visible = sorted((bar for bar in native if available_at(bar, policy) <= cutoff), key=lambda bar: bar["bar_start"])
            cache_key = (candidate.security_id, candidate.interval, str(cutoff))
            if cache_key not in frames:
                bars = derive_hours(visible, policy) if candidate.interval == "1h" else visible
                if candidate.interval == "1d":
                    bars = [bar for bar in state["bars"].get(candidate.security_id + "|1d", {}).values() if available_at(bar, policy) <= cutoff]
                actions = [action for action in state.get("actions", []) if action["security_id"] == candidate.security_id
                    and utc(action.get("first_observed_at", state["last_source_read"])) <= cutoff
                    and utc(action.get("created_at", state["last_source_read"])) <= cutoff]
                frames[cache_key] = build_frame(bars, policy, daily=candidate.interval == "1d", actions=actions)
            frame = frames[cache_key]
            geometry = geometry_review(candidate, frame, policy)
            risk = candidate.direction * (candidate.price - candidate.stop)
            reward = candidate.direction * (candidate.target - candidate.price)
            opening, ending = execution_times(candidate, published, policy)
            entry_bar = next((bar for bar in native if opening and utc(bar["bar_start"]) == opening), None)
            retained = state.get("positions", {}).get(episode_id, {})
            recomputed = mark_position(dict(state="PENDING", candidate=publication["candidates"][episode_id],
                publication_at=published.isoformat(), marks=[]), native, state.get("actions", []), cutoff_now, policy)
            comparison_fields = ("state", "reason", "entry_at", "entry_price", "exit_at", "exit_price", "gross", "net_by_cost_bps")
            outcome_differences = {key: [retained.get(key), recomputed.get(key)] for key in comparison_fields if retained.get(key) != recomputed.get(key)}
            entry_risk = candidate.direction * (entry_bar["open"] - candidate.stop) if entry_bar else None
            entry_reward = candidate.direction * (candidate.target - entry_bar["open"]) if entry_bar else None
            session_bars = sorted((bar for bar in native if bar["session"] == session and available_at(bar, policy) <= cutoff_now), key=lambda bar: bar["bar_start"])
            after_entry = [bar for bar in session_bars if opening and opening <= utc(bar["bar_start"]) < ending]
            favorable = max((candidate.direction * ((bar["high"] if candidate.direction == 1 else bar["low"]) - candidate.price) for bar in after_entry), default=None)
            adverse = min((candidate.direction * ((bar["low"] if candidate.direction == 1 else bar["high"]) - candidate.price) for bar in after_entry), default=None)
            context = state.get("contexts", {}).get(candidate.security_id + "|" + prior_session, {})
            trend = bool(context and context.get("rs63") is not None and candidate.direction * context["rs63"] > 0
                and (context["rs_percentile"] >= .7 if candidate.direction == 1 else context["rs_percentile"] <= .3)
                and candidate.direction * (context["close"] - context["ema50"]) > 0
                and candidate.direction * (context["ema50"] - context["ema50_prior10"]) > 0)
            if candidate.model == "resumption":
                geometry["checks"]["required_daily_trend"] = trend
            current_bar = visible[-1] if visible else None
            current_price = current_bar["close"] if current_bar else None
            current_risk = candidate.direction * (current_price - candidate.stop) if current_price else None
            current_room = candidate.direction * (candidate.target - current_price) if current_price else None
            gate_at_publication_price = entry_gate(candidate, current_price) if current_price else "UNAVAILABLE"
            close = session_bars[-1]["close"] if session_bars else None
            row = dict(episode_id=episode_id, ticker=candidate.ticker, model=candidate.model,
                interval=candidate.interval, direction=candidate.direction, trigger_at=candidate.trigger_at,
                publication_at=published, trigger_et=eastern(candidate.trigger_at), publication_et=eastern(published),
                evaluation_boundary=publication.get("market_time", publication["window_key"]),
                alert_age_minutes=(published - candidate.trigger_at).total_seconds() / 60,
                expiry=candidate.expires_at, trigger_price=candidate.price, reference=candidate.reference,
                stop=candidate.stop, target=candidate.target, risk_dollars=risk, reward_dollars=reward,
                risk_pct=100 * risk / candidate.price, reward_pct=100 * reward / candidate.price,
                reward_risk=reward / risk if risk > 0 else None, stored_rank_reward_risk=candidate.room_risk,
                rank_ratio_matches_plan=near(candidate.room_risk, reward / risk) if risk > 0 else False,
                stop_atr=risk / candidate.activation_atr, target_atr=reward / candidate.activation_atr,
                activation_atr=candidate.activation_atr, extension=candidate.extension,
                last_known_30m_price=current_price, last_known_30m_time=current_bar["bar_end"] if current_bar else None,
                entry_gate_at_last_known_price=gate_at_publication_price,
                last_known_reward_risk=current_room / current_risk if current_risk and current_risk > 0 else None,
                availability_pass=candidate.available_at <= cutoff, expiry_pass=published <= candidate.expires_at,
                publication_deadline_pass=published <= utc(publication["latest_dispatch_at"]),
                entry_gate_at_alert=entry_gate(candidate, candidate.price),
                model_code_matches=publication.get("runtime_sources", {}).get("research/stock_idea_models.py") == current_hashes["research/stock_idea_models.py"],
                engine_code_matches=publication.get("runtime_sources", {}).get("research/stock_idea_engine.py") == current_hashes["research/stock_idea_engine.py"],
                geometry=geometry, daily_context={key: value for key, value in context.items() if key != "rank_revision_ids"},
                resumption_daily_trend_pass=trend, expected_entry_at=opening, expected_exit_at=ending,
                entry_open=entry_bar["open"] if entry_bar else None,
                entry_reward_risk=entry_reward / entry_risk if entry_risk and entry_risk > 0 else None,
                entry_gate_at_next_open=entry_gate(candidate, entry_bar["open"]) if entry_bar else "BAR_UNAVAILABLE",
                stored_state=retained.get("state"), stored_reason=retained.get("reason"),
                stored_entry=retained.get("entry_price"), stored_exit=retained.get("exit_price"),
                stored_entry_at=retained.get("entry_at"), stored_exit_at=retained.get("exit_at"),
                stored_net_pct=100 * retained.get("net_by_cost_bps", {}).get(str(policy["primary_cost_bps"]), 0) if retained.get("state") == "CLOSED" else None,
                recomputed_state=recomputed.get("state"), recomputed_reason=recomputed.get("reason"),
                recomputed_net_pct=100 * recomputed.get("net_by_cost_bps", {}).get(str(policy["primary_cost_bps"]), 0) if recomputed.get("state") == "CLOSED" else None,
                outcome_differences=outcome_differences, session_close=close,
                trigger_to_close_pct=100 * candidate.direction * (close / candidate.price - 1) if close else None,
                favorable_before_planned_end_pct=100 * favorable / candidate.price if favorable is not None else None,
                adverse_before_planned_end_pct=100 * adverse / candidate.price if adverse is not None else None)
            if candidate.ticker in focus:
                row["focus_session_bars"] = session_bars
                row["focus_lineage_count"] = len(candidate.revision_ids)
            rows.append(clean(row))
    summary = dict(session=session, enrolled_at=state["enrolled_at"], source_read_at=state["last_source_read"],
        publications=len(selected_publications), plans=len(rows), unique_tickers=len({row["ticker"] for row in rows}),
        duplicate_selected_ids=len(selected_ids) - len(set(selected_ids)),
        models=dict(Counter(row["model"] for row in rows)), intervals=dict(Counter(row["interval"] for row in rows)),
        directions=dict(Counter(str(row["direction"]) for row in rows)),
        retained_states=dict(Counter(row["stored_state"] for row in rows)), retained_reasons=dict(Counter(row["stored_reason"] for row in rows)),
        recomputed_states=dict(Counter(row["recomputed_state"] for row in rows)),
        outcome_mismatches=[row["ticker"] for row in rows if row["outcome_differences"]],
        narrow_stops_below_half_atr=sum(row["stop_atr"] < .5 for row in rows),
        reward_risk_at_least_four=sum(row["reward_risk"] >= 4 for row in rows),
        reward_risk_at_least_four_entry=sum(row["entry_reward_risk"] is not None and row["entry_reward_risk"] >= 4 for row in rows),
        failing_alert_gates=[row["ticker"] for row in rows if row["entry_gate_at_alert"] or not all(row[key] for key in ("availability_pass", "expiry_pass", "publication_deadline_pass"))],
        ranking_ratio_mismatches=[row["ticker"] for row in rows if not row["rank_ratio_matches_plan"]],
        trigger_price_mismatches=[row["ticker"] for row in rows if not row["geometry"].get("trigger_price_matches_bar")],
        geometry_failures=[dict(ticker=row["ticker"], episode_id=row["episode_id"], failed=[key for key, value in row["geometry"].get("checks", {}).items() if not value]) for row in rows if any(not value for value in row["geometry"].get("checks", {}).values())],
        favorable_price_to_close=sum(row["trigger_to_close_pct"] is not None and row["trigger_to_close_pct"] > 0 for row in rows),
        model_hashes_match=all(row["model_code_matches"] and row["engine_code_matches"] for row in rows))
    closed = [row for row in rows if row["stored_state"] == "CLOSED"]
    summary.update(closed_wins=sum(row["stored_net_pct"] > 0 for row in closed),
        closed_losses=sum(row["stored_net_pct"] < 0 for row in closed),
        closed_mean_net_pct=mean(row["stored_net_pct"] for row in closed) if closed else None,
        closed_median_net_pct=median(row["stored_net_pct"] for row in closed) if closed else None,
        alerts_with_last_known_price_failing_entry_gate=[dict(ticker=row["ticker"], reason=row["entry_gate_at_last_known_price"],
            trigger_price=row["trigger_price"], current_price=row["last_known_30m_price"]) for row in rows if row["entry_gate_at_last_known_price"]],
        median_alert_age_minutes=median(row["alert_age_minutes"] for row in rows),
        older_than_30_minutes=sum(row["alert_age_minutes"] > 30 for row in rows),
        acceptance_negative_confirmation=sum(row["direction"] * row["geometry"].get("trigger_bar_change_pct", 0) < 0 for row in rows if row["model"] == "acceptance"),
        by_model={model: dict(plans=len(group), no_fills=sum(row["stored_state"] == "NO_FILL" for row in group),
            closed=sum(row["stored_state"] == "CLOSED" for row in group),
            wins=sum(row["stored_net_pct"] is not None and row["stored_net_pct"] > 0 for row in group),
            stops=sum(row["stored_reason"] in ("STOP", "STOP_GAP", "STOP_FIRST_AMBIGUOUS") for row in group),
            targets=sum(row["stored_reason"] in ("TARGET", "TARGET_GAP_CONSERVATIVE") for row in group),
            mean_net_pct=mean(row["stored_net_pct"] for row in group if row["stored_net_pct"] is not None) if any(row["stored_net_pct"] is not None for row in group) else None,
            daily_trend_pass=sum(row["resumption_daily_trend_pass"] for row in group))
            for model in ("resumption", "acceptance", "failure") for group in [[row for row in rows if row["model"] == model]]})
    return dict(summary=summary, publications=[{key: row.get(key) for key in ("window_key", "coverage", "actual_publication_at", "input_deadline", "latest_dispatch_at", "selected")} for row in selected_publications], alerts=rows)


def verify_quality_fixes(policy, state, publications, session):
    from types import SimpleNamespace
    revised = forward_config(quality_version=2)
    decisions, confirmations = [], []
    for publication in publications:
        if publication["session"] != session:
            continue
        boundary = utc(publication.get("market_time", publication["window_key"]))
        cutoff = utc(publication["input_deadline"])
        for episode_id in publication["selected"]:
            plan = read_candidate(publication["candidates"][episode_id])
            if plan.model == "discovery":
                continue
            checked, evidence = decision_candidate(plan, state, boundary=boundary, cutoff=cutoff,
                actual_time=utc(publication["actual_publication_at"]), config=revised)
            decisions.append(dict(ticker=plan.ticker, model=plan.model, reason=checked.selection_block,
                trigger_price=plan.price, decision_price=evidence["price"], decision_reward_risk=evidence["reward_risk"]))
            if plan.model == "acceptance":
                native = [bar for bar in state["bars"][plan.security_id + "|30m"].values() if available_at(bar, policy) <= cutoff]
                frame = build_frame(derive_hours(native, policy) if plan.interval == "1h" else native, policy)
                review = geometry_review(plan, frame, policy)
                if review["status"] != "GEOMETRY_RECONSTRUCTED":
                    raise ValueError("cannot verify confirmation without reconstructed geometry")
                row = SimpleNamespace(**review["trigger_bar"])
                previous = SimpleNamespace(**review["activation_bar"])
                geometry = dict(reference=plan.reference, atr=plan.activation_atr)
                accepted = acceptance_confirmed(row, previous, geometry, plan.direction, review["activation_to_trigger_bars"],
                    revised["models"]["acceptance"]["confirmation_policy"])
                confirmations.append(dict(ticker=plan.ticker, v2_confirmation=accepted))
    expected = dict(HWM="ENTRY_CHASE_OR_BOUNDARY_FAILED", LQD="INSUFFICIENT_TARGET_ROOM",
        VEA="INSUFFICIENT_TARGET_ROOM", RCL="NO_REMAINING_ENTRY_SLOT", MA="NO_REMAINING_ENTRY_SLOT")
    if session == "2026-09-14":
        for ticker, reason in expected.items():
            assert any(row["ticker"] == ticker and row["reason"] == reason for row in decisions), ticker
        assert all(any(row["ticker"] == ticker and not row["v2_confirmation"] for row in confirmations) for ticker in ("SPGI", "RMD"))
    return dict(status="FIXTURE_CHECKS_PASS", policy=revised, examined_plans=len(decisions),
        suppressed_by_execution=[row for row in decisions if row["reason"]], acceptance_checks=confirmations,
        outcomes_recomputed=False, winners_reselected=False, classification="RETAINED_CASE_REGRESSION_NOT_PERFORMANCE_STUDY")


def verify_history_reconciliation(policy, state, publications, state_dir):
    from research.stock_idea_forward import digest
    record = next((row for row in reversed(state.get("input_reconciliation_history", [])) if row.get("backup")), None)
    if record is None:
        raise ValueError("no backed-up history reconciliation is retained")
    backup_path = state_dir / record["backup"]
    assert hashlib.sha256(backup_path.read_bytes()).hexdigest() == record["backup_sha256"]
    before_policy, before, before_publications, _ = load_retained(backup_path)
    assert before_policy == policy and publications == before_publications
    allowed = {"bars", "detectors", "pending_candidates", "prepared_packets", "detector_recovery_boundaries", "input_reconciliation_history"}
    assert {key: value for key, value in state.items() if key not in allowed} == {key: value for key, value in before.items() if key not in allowed}
    for key, rows in before["bars"].items():
        assert all(state["bars"][key].get(start) == bar for start, bar in rows.items())
    with closing(sqlite3.connect(backup_path.resolve().as_uri() + "?mode=ro", uri=True)) as original, closing(sqlite3.connect((state_dir / "forward.sqlite").resolve().as_uri() + "?mode=ro", uri=True)) as current:
        assert original.execute("SELECT * FROM forward_outbox ORDER BY 1").fetchall() == current.execute("SELECT * FROM forward_outbox ORDER BY 1").fetchall()
    added = {bar["revision_id"] for key, rows in state["bars"].items() for start, bar in rows.items() if start not in before["bars"].get(key, {})}
    assert added == {revision for revisions in record["inserted_revision_ids"].values() for revision in revisions}
    ready = Counter()
    for security in record["inserted_revision_ids"]:
        native = sorted(state["bars"][security + "|30m"].values(), key=lambda bar: bar["bar_start"])
        actions = [action for action in state.get("actions", []) if action["security_id"] == security]
        for interval, source in (("30m", native), ("1h", derive_hours(native, policy))):
            frame = build_frame(source, policy, actions=actions)
            ready[interval] += int(not frame.empty and bool(frame.ready.iloc[-1]))
            assert not state["detectors"][security + "|" + interval]["states"]
        assert state["detector_recovery_boundaries"][security] == record["effective_boundary"]
    return dict(status="HISTORY_PRESERVATION_VERIFIED", inserted_bars=len(added), affected_members=len(record["inserted_revision_ids"]),
        effective_boundary=record["effective_boundary"], feature_ready=dict(ready), warmup_requirement=policy["feature_warmup_bars"],
        original_publications=len(publications), positions=len(state.get("positions", {})),
        backup_sha256=record["backup_sha256"], restored_revision_sha256=digest(sorted(added)),
        existing_bars_unchanged=True, other_checkpoint_fields_unchanged=True, outbox_unchanged=True)


def verify_correction_recovery(policy, state, publications, state_dir):
    from research.stock_idea_forward import active_correction_recoveries, alert_selection_cohort, corrected_detector_inputs, correction_inventory_hash
    history = state.get("correction_recovery_history", [])
    if not history:
        raise ValueError("no backed-up correction recovery retained")
    record = history[-1]
    backup_path = state_dir / record["backup"]
    assert hashlib.sha256(backup_path.read_bytes()).hexdigest() == record["backup_sha256"]
    prior_policy, prior, prior_publications, _ = load_retained(backup_path)
    assert policy == prior_policy and publications == prior_publications
    allowed = {"detectors", "pending_candidates", "prepared_packets", "detector_recovery_boundaries", "correction_recovery_history"}
    assert {key: value for key, value in state.items() if key not in allowed} == {key: value for key, value in prior.items() if key not in allowed}
    assert correction_inventory_hash(state) == record["inventory_sha256"]
    assert history[:-1] == prior.get("correction_recovery_history", [])
    with closing(sqlite3.connect(backup_path.resolve().as_uri() + "?mode=ro", uri=True)) as original, closing(sqlite3.connect((state_dir / "forward.sqlite").resolve().as_uri() + "?mode=ro", uri=True)) as current:
        for table in ("forward_manifest", "forward_publications", "forward_outbox"):
            assert original.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall() == current.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
    affected = {pair["security_id"] for pair in record["pairs"]}
    for key, detector in prior.get("detectors", {}).items():
        security, interval = key.split("|", 1)
        if security in affected and interval in ("30m", "1h"):
            assert state["detectors"][key] == dict(detector, states={})
        else:
            assert state["detectors"][key] == detector
    assert state.get("pending_candidates", {}) == {key: value for key, value in prior.get("pending_candidates", {}).items() if value["security_id"] not in affected}
    assert state.get("prepared_packets", []) == [packet for packet in prior.get("prepared_packets", []) if packet["security_id"] not in affected]
    effective, activated = utc(record["effective_boundary"]), utc(record["activated_at"])
    recovered = active_correction_recoveries(state, effective, activated)
    assert affected <= set(recovered)
    assert not affected.intersection(active_correction_recoveries(state, activated, activated))
    for security in affected:
        assert state["detector_recovery_boundaries"][security] == record["effective_boundary"]
    inputs = corrected_detector_inputs(state, recovered)
    ready, model_ready, unavailable = Counter(), Counter(), []
    prior_session = str(exchange_calendars.get_calendar("XNYS").previous_session(str(effective.date())).date())
    members, _ = alert_selection_cohort(state, effective)
    for member in members:
        security = member["security_id"]
        native = sorted(inputs.get(security + "|30m", {}).values(), key=lambda bar: bar["bar_start"])
        actions = [action for action in state.get("actions", []) if action["security_id"] == security]
        for interval, rows in (("30m", native), ("1h", derive_hours(native, policy))):
            frame = build_frame(rows, policy, actions=actions)
            feature_ready = not frame.empty and bool(frame.ready.iloc[-1])
            ready[interval] += int(feature_ready)
            eligible = feature_ready and (security not in state.get("identity_breaks", {}) or security in recovered) and security + "|" + prior_session in state.get("contexts", {})
            model_ready[interval] += int(eligible)
            if interval == "30m" and not eligible:
                unavailable.append(member["ticker"])
    return dict(status="CORRECTION_PRESERVATION_VERIFIED", generation_id=record["generation_id"],
        reviewed_pairs=len(record["pairs"]), recovered_members=len(affected), effective_boundary=record["effective_boundary"],
        prospective_feature_ready=dict(ready), prospective_model_ready_on_retained_inputs=dict(model_ready),
        still_unavailable_30m=unavailable, active_cohort=len(members), warmup_requirement=policy["feature_warmup_bars"],
        original_publications=len(publications), original_positions=len(state.get("positions", {})),
        original_bars_and_corrections_unchanged=True, protected_checkpoint_fields_unchanged=True, outbox_unchanged=True,
        backup_sha256=record["backup_sha256"], future_publication_not_simulated=True)


def publication_summary(state, publications, session):
    from research.stock_idea_forward import active_correction_recoveries, correction_inventory_hash
    now = datetime.now(timezone.utc)
    records = [record for record in publications if record["session"] == session]
    names = {member["security_id"]: member["ticker"] for member in state["members"]}
    recovered = active_correction_recoveries(state, now, now)
    breaks = {security: reason for security, reason in state.get("identity_breaks", {}).items() if security not in recovered}
    latest = records[-1] if records else None
    missing = latest["missing_members"] if latest else []
    missing_reasons = {security: "RECOVERED_FOR_FUTURE_RUNS" if security in recovered
        and latest and utc(latest.get("market_time", latest["window_key"])) < utc(recovered[security]["effective_boundary"])
        else breaks.get(security, "MODEL_INPUT_NOT_READY") for security in missing}
    reasons = Counter(missing_reasons.values())
    setups = Counter()
    for key, detector in state.get("detectors", {}).items():
        security, interval = key.split("|", 1)
        if security in breaks:
            continue
        for model, saved in detector.get("states", {}).items():
            lifecycle = saved.get("lifecycle")
            if lifecycle:
                setups[f"{interval}:{model}:{lifecycle['setup']}"] += 1
    return dict(checked_at=datetime.now(timezone.utc).isoformat(), last_source_read=state.get("last_source_read"),
        runtime_matches_disk=state.get("runtime_sources") == runtime_sources(), next_boundary=state.get("next_boundary"),
        runs=[dict(boundary=record["window_key"], status=record["coverage"], actual_publication_at=record["actual_publication_at"],
            expected=len(record["expected_members"]), missing=len(record["missing_members"]),
            candidates=len(record.get("candidates", {})), selected=len(record["selected"]),
            dispositions=dict(Counter(item["reason"] for item in record["dispositions"]))) for record in records],
        current_missing_reasons=dict(reasons), missing_reason_samples={reason: [names.get(security, security) for security in missing
            if missing_reasons[security] == reason][:8] for reason in reasons},
        current_identity_breaks=dict(Counter(breaks.values())), detector_setups=dict(setups),
        correction_inventory_sha256=correction_inventory_hash(state),
        active_correction_recoveries=len(recovered), correction_recovery=[dict(generation_id=record["generation_id"],
            activated_at=record["activated_at"], effective_boundary=record["effective_boundary"],
            members=len({pair["security_id"] for pair in record["pairs"]})) for record in state.get("correction_recovery_history", [])],
        correction_count=len(state.get("corrections", [])), correction_samples=[dict(record, ticker=names.get(record["security_id"]))
            for record in state.get("corrections", [])[:3]],
        recovery=[dict(observed_at=record["observed_at"], effective_boundary=record["effective_boundary"],
            members=len(record["inserted_revision_ids"])) for record in state.get("input_reconciliation_history", [])],
        latest_source_readiness=latest.get("source_readiness") if latest else None,
        note="Missing reasons and detector setups reflect the current retained checkpoint; runs are immutable publication records")


def summarize_correction_rows(corrections, rows):
    indexed = {row["revision_id"]: row for row in rows}
    counts, samples = Counter(), []
    identity_fields = ("security_id", "ticker", "interval", "bar_start", "bar_end", "session_scope", "adjusted", "is_final")
    value_fields = ("open", "high", "low", "close", "volume")
    for correction in corrections:
        original = indexed.get(correction["original_revision_id"])
        revised = indexed.get(correction["revision_id"])
        changed = []
        if not original or not revised:
            reason = "REVISION_UNAVAILABLE_AT_CUTOFF"
        elif any(original[field] != revised[field] for field in identity_fields):
            reason = "IDENTITY_OR_BAR_CLOCK_CHANGED"
        else:
            changed = [field for field in value_fields if original[field] != revised[field]]
            reason = "+".join(changed) if changed else "IDENTICAL_OHLCV_NEW_REVISION"
        counts[reason] += 1
        if len(samples) < 4 and original and revised:
            samples.append(dict(ticker=original["ticker"], interval=original["interval"], bar_end=original["bar_end"], reason=reason,
                changes={field: dict(original=original[field], revised=revised[field]) for field in changed},
                original_created_at=original["created_at"], revised_created_at=revised["created_at"]))
    return dict(correction_pairs=len(corrections), revision_rows=len(rows), change_types=dict(counts), samples=samples)


def inspect_corrections(state, cutoff):
    from dotenv import load_dotenv
    load_dotenv(BACKEND / ".env")
    from database import get_db_cursor
    corrections = state.get("corrections", [])
    if len(corrections) > 1000:
        raise ValueError("correction audit exceeds 1000-pair bound")
    revisions = sorted({record[key] for record in corrections for key in ("original_revision_id", "revision_id")})
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='15s'")
        cursor.execute("""SELECT bar_revision_id::text AS revision_id,security_id::text,ticker,interval,
            bar_start,bar_end,session_scope,adjusted,is_final,
            open_price::float8 AS open,high_price::float8 AS high,low_price::float8 AS low,
            close_price::float8 AS close,volume::float8,system_observed_at,created_at
            FROM equity_bar_revisions WHERE bar_revision_id=ANY(%s::uuid[])
                AND system_observed_at<=%s AND created_at<=%s""", (revisions, cutoff, cutoff))
        rows = [dict(row) for row in cursor.fetchall()]
    return summarize_correction_rows(corrections, rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session", type=date.fromisoformat, required=True)
    parser.add_argument("--state-dir", type=Path, default=BACKEND / "backups/equity-shadow/stock-ideas-forward-v1")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--verify-quality-fixes", action="store_true")
    parser.add_argument("--verify-history-reconciliation", action="store_true")
    parser.add_argument("--verify-correction-recovery", action="store_true")
    parser.add_argument("--publication-summary", action="store_true")
    parser.add_argument("--inspect-corrections", action="store_true")
    args = parser.parse_args()
    if args.inspect_corrections and not args.publication_summary:
        parser.error("Correction inspection requires --publication-summary")
    if args.output and args.output.exists():
        parser.error("Use a new output path; audit reports are not overwritten")
    policy, state, publications, hashes = load_retained(args.state_dir / "forward.sqlite")
    if args.verify_correction_recovery:
        print(json.dumps(verify_correction_recovery(policy, state, publications, args.state_dir), indent=2), flush=True)
        return
    if args.publication_summary:
        result = publication_summary(state, publications, args.session.isoformat())
        if args.inspect_corrections:
            result["correction_values"] = inspect_corrections(state, utc(state["last_source_read"]))
        print(json.dumps(clean(result), indent=2), flush=True)
        return
    if args.verify_history_reconciliation:
        print(json.dumps(verify_history_reconciliation(policy, state, publications, args.state_dir), indent=2), flush=True)
        return
    result = (verify_quality_fixes(policy, state, publications, args.session.isoformat()) if args.verify_quality_fixes
        else audit(policy, state, publications, args.session.isoformat(), {"SPGI", "RMD"}))
    _, _, _, after = load_retained(args.state_dir / "forward.sqlite")
    result.update(created_at=datetime.now(timezone.utc).isoformat(), input_hashes=hashes,
        checkpoint_inputs_unchanged=after == hashes, source="READ_ONLY_RETAINED_FORWARD_CHECKPOINT",
        limitations=["Single-session diagnostic, not an effectiveness or probability estimate",
            "Shared-source price check, not independent vendor/action validation",
            "Recomputed outcomes are diagnostic only; retained outcomes are never changed",
            "Range-model preconditions reconstructed; full incremental lifecycle parity not claimed"])
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(clean(result), indent=2, allow_nan=False) + "\n", encoding="utf-8")
    if args.verify_quality_fixes:
        print(json.dumps({key: value for key, value in result.items() if key not in ("policy", "limitations")}, indent=2), flush=True)
        return
    print(json.dumps(result["summary"], indent=2), flush=True)
    print("TICKER MODEL SIDE TF TRIGGER_ET PUB_ET STOP_% TARGET_% RR ENTRY_RR RETAINED RECOMPUTED", flush=True)
    for row in result["alerts"]:
        entry_ratio = f"{row['entry_reward_risk']:.2f}" if row["entry_reward_risk"] is not None else "N/A"
        print(f"{row['ticker']} {row['model']} {row['direction']} {row['interval']} {row['trigger_et']} {row['publication_et']} "
            f"{row['risk_pct']:.3f} {row['reward_pct']:.3f} {row['reward_risk']:.2f} {entry_ratio} {row['stored_state']} {row['recomputed_state']}", flush=True)
    print(json.dumps(dict(checkpoint_inputs_unchanged=result["checkpoint_inputs_unchanged"], report=str(args.output)), indent=2))


if __name__ == "__main__":
    main()