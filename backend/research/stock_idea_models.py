"""Versioned bar adapters for stock ideas, independent of capture and storage."""
from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import math

import exchange_calendars
import numpy as np
import pandas as pd

from research.stock_idea_engine import Candidate, advance_episode, read_candidate
from research.stock_idea_replay import available_at, derive_hours, session_windows, utc, valid_bar
from research.strategy_v2 import CENTRAL_VARIANTS, CONFIGURATIONS, detect


def feature_frames(bundle, config, *, derive_hourly=True):
    calendar = exchange_calendars.get_calendar("XNYS")
    source = bundle["bars"] + (derive_hours(bundle["bars"], config) if derive_hourly else [])
    grouped = defaultdict(list)
    for bar in source:
        grouped[(bar["security_id"], bar["interval"])].append(bar)
    frames = {}
    for (security_id, interval), bars in sorted(grouped.items()):
        bars = [bar for bar in bars if valid_bar(bar)
            and (utc(bar["bar_start"]), utc(bar["bar_end"])) in session_windows(bar["session"], interval)]
        if not bars:
            continue
        bars = sorted(bars, key=lambda bar: bar["bar_start"])
        expected = {start: ordinal for ordinal, (start, _) in enumerate(
            window for session in calendar.sessions_in_range(bars[0]["session"], bars[-1]["session"])
            for window in session_windows(str(session.date()), interval))}
        frame = pd.DataFrame(bars)
        frame["bar_start"] = pd.to_datetime(frame.bar_start, utc=True)
        frame["bar_end"] = pd.to_datetime(frame.bar_end, utc=True)
        frame["ordinal"] = frame.bar_start.map(expected)
        frame["visible_at"] = [utc(bar["visible_at"]) if bar.get("visible_at") else available_at(bar, config) for bar in bars]
        frame["clock_valid"] = [valid_bar(bar) and (utc(bar["bar_start"]), utc(bar["bar_end"])) in session_windows(bar["session"], interval) for bar in bars]
        action_dates = {action["effective_date"] for action in bundle["actions"]
                        if action["security_id"] == security_id and action["action_type"] in ("SPLIT", "MERGER", "SYMBOL_CHANGE", "SPINOFF")
                        and not (action["action_type"] == "SPLIT" and interval == "1d"
                                 and bars[0].get("price_basis") == "SPLIT_ADJUSTED")}
        action_break = frame.session.isin(action_dates) & frame.session.ne(frame.session.shift(1))
        frame["segment"] = (frame.ordinal.diff().ne(1) | ~frame.clock_valid | ~frame.clock_valid.shift(1, fill_value=True)
                            | frame.ticker.ne(frame.ticker.shift(1)) | action_break).cumsum()
        frame["identity_disagreement"] = False
        frame["bar_revision_id"] = frame.revision_id
        for _, indices in frame.groupby("segment").groups.items():
            group = frame.loc[indices]
            close = group.close
            previous = close.shift(1)
            true_range = pd.concat([group.high - group.low, (group.high - previous).abs(),
                                    (group.low - previous).abs()], axis=1).max(axis=1)
            frame.loc[indices, "atr_prior"] = true_range.ewm(alpha=config["atr_alpha"], adjust=False).mean().shift(1)
            frame.loc[indices, "ema20"] = close.ewm(span=20, adjust=False).mean()
            ema50 = close.ewm(span=50, adjust=False).mean()
            frame.loc[indices, "ema50"] = ema50
            frame.loc[indices, "ema50_prior10"] = ema50.shift(10)
            frame.loc[indices, "range_ratio"] = true_range.rolling(5).mean().shift(1) / true_range.rolling(20).mean().shift(1)
            frame.loc[indices, "return63"] = close / close.shift(63) - 1
            frame.loc[indices, "momentum"] = close.shift(21) / close.shift(252) - 1
            frame.loc[indices, "liquidity"] = (close * group.volume).rolling(20).median()
            frame.loc[indices, "warmup"] = np.arange(1, len(group) + 1)
            frame.loc[indices, "visible_at"] = pd.to_datetime(group.visible_at, utc=True).cummax()
        frame["ready"] = frame.clock_valid & (frame.warmup >= config["feature_warmup_bars"]) & (frame.atr_prior > 0)
        frames[(security_id, interval)] = frame
    return frames


def daily_contexts(frames, memberships, config):
    calendar = exchange_calendars.get_calendar("XNYS")
    dated = defaultdict(set)
    for member in memberships:
        dated[member["session"]].add(member["security_id"])
    daily = [frame for (_, interval), frame in frames.items() if interval == "1d"]
    if not daily:
        return {}, []
    all_daily = pd.concat(daily, ignore_index=True)
    spy = all_daily.loc[all_daily.ticker.eq("SPY")].set_index("session").return63.to_dict()
    contexts, coverage = {}, []
    for session, group in all_daily.groupby("session", sort=True):
        deadline = calendar.session_close(session).to_pydatetime() + timedelta(minutes=config["publication_delay_minutes"])
        pool = group.loc[group.security_id.isin(dated[session]) & group.ready & group.momentum.notna()
                         & group.return63.notna() & group.close.ge(5) & group.liquidity.ge(20_000_000)
                         & group.visible_at.le(deadline)].copy()
        pool = pool.sort_values(["return63", "security_id"])
        pool["rs_percentile"] = pool.return63.rank(method="average", pct=True)
        momentum_order = pool.sort_values(["momentum", "security_id"], ascending=[False, True])
        momentum_ranks = {security: index + 1 for index, security in enumerate(momentum_order.security_id)}
        rank_ids = sorted(pool.revision_id.tolist())
        for _, row in pool.iterrows():
            data = row.to_dict()
            data.update(rs63=row.return63 - spy.get(session, math.nan),
                        momentum_rank=momentum_ranks[row.security_id], rank_count=len(pool),
                        rank_revision_ids=rank_ids, expected_rank_members=len(dated[session]))
            contexts[(row.security_id, session)] = data
        if session >= config["start"]:
            coverage.append(dict(session=session, expected=len(dated[session]), eligible_ranked=len(pool),
                                 missing_or_ineligible=sorted(dated[session] - set(pool.security_id))))
    return contexts, coverage


def make_candidate(row, model, interval, direction, geometry, context, config):
    trigger = row.bar_end.to_pydatetime()
    calendar = exchange_calendars.get_calendar("XNYS")
    if interval == "1d":
        expiry = calendar.session_open(calendar.next_session(row.session)).to_pydatetime()
        horizon = f"DAILY_{config['models'][model]['daily_horizon_sessions']}"
        version = config["models"][model]["daily_family"] + ":" + config["models"][model]["daily_variant"]
    else:
        closing = session_windows(row.session)[-1][1]
        expiry = next((end for _, end in session_windows(row.session, interval) if end > trigger), closing)
        horizon, version = "INTRADAY", config["models"][model]["intraday_version"]
    stop = geometry["extreme"] - direction * config["stop_buffer_atr"] * row.atr_prior
    risk, room = direction * (row.close - stop), direction * (geometry["target"] - row.close)
    revisions = tuple(sorted(set(geometry["source_ids"] + [row.revision_id] + context.get("rank_revision_ids", []))))
    candidate = Candidate(str(row.security_id), str(row.ticker), model, interval, direction, horizon, geometry["anchor"],
        trigger, expiry, max(row.visible_at.to_pydatetime(), context["visible_at"].to_pydatetime()),
        geometry["reference"], float(stop), geometry["target"], geometry["atr"], float(row.close),
        float(context["rs_percentile"]), abs(float(row.close) - (float(row.ema20) if model == "resumption" else geometry["reference"])) / geometry["atr"],
        room / risk if risk > 0 else -1., float(context["liquidity"]), float(context["momentum"]),
        revisions, policy_version=version)
    if interval == "1d" and row.get("price_basis") == "SPLIT_ADJUSTED":
        scale = row.get("execution_scale")
        if scale is None or not math.isfinite(scale) or scale <= 0:
            return replace(candidate, health="DAILY_EXECUTION_SCALE_UNAVAILABLE")
        scale_available = available_at(dict(bar_end=row.bar_end.isoformat(),
            system_observed_at=row.execution_scale_observed_at, created_at=row.execution_scale_created_at), config)
        candidate = replace(candidate, reference=candidate.reference * scale, stop=candidate.stop * scale,
            target=candidate.target * scale, price=candidate.price * scale, activation_atr=candidate.activation_atr * scale,
            available_at=max(candidate.available_at, scale_available),
            revision_ids=tuple(sorted(set(candidate.revision_ids) | set(row.execution_scale_revision_ids))))
    return candidate


ACCEPTANCE_CONFIRMATION_V2 = dict(version="directional_acceptance_v2", boundary_margin_atr=.15,
    minimum_directional_close_location=.5, favorable_body=True, improve_previous_close=True)


def acceptance_confirmed(row, previous, geometry, direction, age, policy=None):
    if age != 1 or direction * (row.close - geometry["reference"]) <= 0:
        return False
    if policy is None:
        return True
    if policy != ACCEPTANCE_CONFIRMATION_V2:
        raise ValueError("unsupported acceptance confirmation policy")
    if not all(math.isfinite(value) for value in (row.close, row.open, row.high, row.low, previous.close, geometry["atr"])):
        return False
    span = row.high - row.low
    if span <= 0 or geometry["atr"] <= 0:
        return False
    location = (row.close - row.low) / span if direction == 1 else (row.high - row.close) / span
    return bool(direction * (row.close - geometry["reference"]) >= policy["boundary_margin_atr"] * geometry["atr"]
        and direction * (row.close - row.open) > 0 and direction * (row.close - previous.close) > 0
        and location >= policy["minimum_directional_close_location"])


def intraday_observations(frame, interval, contexts, config, *, initial_state=None, after_ordinal=None, state_sink=None):
    calendar = exchange_calendars.get_calendar("XNYS")
    states, output = deepcopy(initial_state) if initial_state else {}, []
    for position, row in frame.iterrows():
        if after_ordinal is not None and row.ordinal <= after_ordinal:
            continue
        prior_session = str(calendar.previous_session(row.session).date())
        context = contexts.get((str(row.security_id), prior_session))
        candidates, updates, ready = [], [], bool(row.ready and context is not None)
        previous = frame.iloc[position - 1] if position else row
        for model in ("resumption", "acceptance", "failure"):
            for direction in (1, -1):
                key = (model, direction)
                saved = states.setdefault(key, dict(lifecycle=None, geometry=None))
                life, geometry = saved["lifecycle"], saved["geometry"]
                if life is not None and (not ready or (life["setup"] != "WATCH" and geometry is None)):
                    old = read_candidate({name: value for name, value in life["candidate"].items() if name != "episode_id"})
                    observed = replace(old, trigger_at=row.bar_end.to_pydatetime(), price=float(row.close),
                                       health="READY" if ready else "UNAVAILABLE")
                    life, changes = advance_episode(life, observed, ordinal=int(row.ordinal), close=float(row.close),
                        range_low=life["range_low"], range_high=life["range_high"], reset_valid=ready,
                        invalidated=ready and direction * (row.close - old.stop) <= 0)
                    updates.extend(changes)
                    saved["lifecycle"] = life
                    if life["setup"] == "TRIGGERED":
                        candidates.append(read_candidate({name: value for name, value in life["candidate"].items() if name != "episode_id"}))
                        continue
                    if life["setup"] in ("EXPIRED", "INVALIDATED") and life["reset_count"] < 2:
                        saved["geometry"] = None
                        continue
                if not ready or position < 20:
                    continue
                trend = (math.isfinite(context["rs63"]) and direction * context["rs63"] > 0
                         and (context["rs_percentile"] >= .7 if direction == 1 else context["rs_percentile"] <= .3)
                         and direction * (context["close"] - context["ema50"]) > 0
                         and direction * (context["ema50"] - context["ema50_prior10"]) > 0)
                if geometry is not None:
                    geometry["source_ids"] = sorted(set(geometry["source_ids"]) | {row.revision_id})
                    geometry["extreme"] = min(geometry["extreme"], row.low) if direction == 1 else max(geometry["extreme"], row.high)
                    age = int(row.ordinal) - geometry["anchor_ordinal"]
                    triggered, invalid = False, False
                    if model == "resumption":
                        invalid = not trend or direction * (geometry["reference"] - geometry["extreme"]) > 3 * geometry["atr"] or age > 10
                        triggered = not invalid and age >= 2 and direction * (row.close - (previous.high if direction == 1 else previous.low)) > 0 and direction * (row.close - row.ema20) > 0
                    elif model == "acceptance":
                        triggered = acceptance_confirmed(row, previous, geometry, direction, age,
                            config["models"]["acceptance"].get("confirmation_policy"))
                        invalid = not triggered
                    else:
                        invalid = age > 3
                        triggered = not invalid and age >= 1 and direction * (row.close - geometry["reference"]) >= .1 * geometry["atr"] and direction * (row.close - row.open) > 0 and direction * (row.close - previous.close) > 0
                    proposed = make_candidate(row, model, interval, direction, geometry, context, config)
                    life, changes = advance_episode(life, proposed, ordinal=int(row.ordinal), close=float(row.close),
                        range_low=geometry["low"], range_high=geometry["high"], triggered=triggered,
                        invalidated=invalid, anchor_transition=True, fresh_retracement=model == "resumption")
                    saved["lifecycle"] = life
                    updates.extend(changes)
                    if triggered and life["setup"] == "TRIGGERED":
                        candidates.append(read_candidate({name: value for name, value in life["candidate"].items() if name != "episode_id"}))
                    if triggered or invalid:
                        saved["geometry"] = None
                    continue
                if life is not None and life["setup"] == "WATCH":
                    continue
                window = frame.iloc[position - (10 if model == "acceptance" else 20):position]
                if window.segment.nunique() != 1 or window.segment.iloc[-1] != row.segment:
                    continue
                high, low, atr = float(window.high.max()), float(window.low.min()), float(row.atr_prior)
                anchor_ordinal = int(row.ordinal)
                if model == "resumption":
                    if not trend:
                        continue
                    reference = high if direction == 1 else low
                    extreme_rows = window.loc[(window.high if direction == 1 else window.low).eq(reference)]
                    anchor_row = extreme_rows.iloc[-1]
                    anchor_ordinal = int(anchor_row.ordinal)
                    depth = direction * (reference - row.close)
                    if not 1 <= int(row.ordinal) - anchor_ordinal <= 10 or not atr <= depth <= 3 * atr:
                        continue
                    target = reference
                    anchor = str(anchor_row.bar_start)
                elif model == "acceptance":
                    reference = high if direction == 1 else low
                    if not (0 < high - low <= 4 * atr and row.range_ratio <= .75 and low <= previous.close <= high
                            and direction * (row.close - reference) > .15 * atr):
                        continue
                    target = reference + direction * (high - low)
                    anchor = f"{window.bar_start.iloc[0]}:{window.bar_start.iloc[-1]}"
                else:
                    reference = low if direction == 1 else high
                    extension = row.low if direction == 1 else row.high
                    if not (low <= previous.close <= high and direction * (row.close - reference) < 0
                            and direction * (reference - extension) >= .5 * atr):
                        continue
                    target = (high + low) / 2
                    anchor = f"{window.bar_start.iloc[0]}:{window.bar_start.iloc[-1]}"
                episode_rows = frame.loc[frame.ordinal.between(anchor_ordinal + (model == "resumption"), row.ordinal)]
                geometry = dict(reference=reference, target=target, atr=atr, low=low, high=high,
                    anchor=anchor, anchor_ordinal=anchor_ordinal,
                    extreme=float(episode_rows.low.min() if direction == 1 else episode_rows.high.max()),
                    source_ids=frame.iloc[max(0, position - 199):position + 1].revision_id.tolist())
                saved["geometry"] = geometry
                if life is None:
                    proposed = make_candidate(row, model, interval, direction, geometry, context, config)
                    saved["lifecycle"], changes = advance_episode(None, proposed, ordinal=int(row.ordinal), close=float(row.close),
                        range_low=low, range_high=high)
                    updates.extend(changes)
        output.append(dict(security_id=str(row.security_id), interval=interval, market_time=row.bar_end.to_pydatetime(),
            available_at=row.visible_at.to_pydatetime(), ready=ready, candidates=candidates, updates=updates,
            revision_ids=[row.revision_id],
            lifecycles={f"{model}:{direction}": saved["lifecycle"] for (model, direction), saved in states.items()}))
    if state_sink is not None:
        state_sink.clear()
        state_sink.update(states)
    return output


def daily_observations(frame, contexts, config):
    frame = frame.copy()
    context_rows = [contexts.get((str(row.security_id), row.session), {}) for _, row in frame.iterrows()]
    for name in ("rs63", "rs_percentile"):
        frame[name] = [context.get(name, math.nan) for context in context_rows]
    frame["ready"] &= np.array([bool(context) for context in context_rows], dtype=bool)
    frame.index = pd.to_datetime(frame.session).dt.date
    events = defaultdict(list)
    setup_ordinals = {}
    for contract in CONFIGURATIONS:
        if contract.variant != CENTRAL_VARIANTS[contract.family]:
            continue
        detected, _ = detect(frame, contract)
        model = next(model for model in ("resumption", "acceptance", "failure") if config["models"][model]["daily_family"] == contract.family)
        for event in detected:
            row = frame.loc[pd.Timestamp(event["session"]).date()]
            context = contexts[(str(row.security_id), row.session)]
            geometry = dict(reference=event["reference"], target=event["target"], atr=event["atr_at_activation"],
                            extreme=event["stop"] + event["direction"] * .1 * event["atr_at_trigger"],
                            anchor=event["setup_session"], source_ids=frame.loc[:row.name].tail(253).revision_id.tolist())
            candidate = make_candidate(row, model, "1d", event["direction"], geometry, context, config)
            events[row.session].append(candidate)
            setup_ordinals[candidate.episode_id] = int(frame.loc[pd.Timestamp(event["setup_session"]).date()].ordinal)
    result, previous_side, states = [], None, {}
    for _, row in frame.iterrows():
        context = contexts.get((str(row.security_id), row.session))
        proposed = {(candidate.model, candidate.direction): candidate for candidate in events[row.session]}
        candidates, updates = [], []
        for key in sorted(set(states) | set(proposed)):
            life = states.get(key)
            current = proposed.get(key)
            old = read_candidate({name: value for name, value in life["candidate"].items() if name != "episode_id"}) if life else None
            observed = current or replace(old, trigger_at=row.bar_end.to_pydatetime(), health="READY" if context else "UNAVAILABLE")
            fresh = current is not None and (life is None or life["reset_ordinal"] is None
                    or setup_ordinals[current.episode_id] > life["reset_ordinal"])
            prior_window = frame.loc[:row.name].tail(21).iloc[:-1]
            scale = row.get("execution_scale", 1.) if row.get("price_basis") == "SPLIT_ADJUSTED" else 1.
            scale_ready = scale is not None and math.isfinite(scale) and scale > 0
            scale = scale if scale_ready else 1.
            life, changes = advance_episode(life, observed, ordinal=int(row.ordinal), close=float(row.close) * scale,
                range_low=float(prior_window.low.min()) * scale, range_high=float(prior_window.high.max()) * scale,
                triggered=fresh, invalidated=old is not None and old.direction * (float(row.close) * scale - old.stop) <= 0,
                reset_valid=bool(row.ready and scale_ready), anchor_transition=True, fresh_retracement=fresh)
            states[key] = life
            updates.extend(changes)
            if current is not None:
                admitted = life is not None and life["setup"] == "TRIGGERED" and life["candidate"]["trigger_at"] == str(current.trigger_at)
                if admitted:
                    candidates.append(read_candidate({name: value for name, value in life["candidate"].items() if name != "episode_id"}))
                else:
                    candidates.append(replace(current, selection_block="LIFECYCLE_NOT_REARMED"))
        if context is not None:
            tail = math.ceil(context["rank_count"] * .1)
            side = 1 if context["momentum_rank"] <= tail and row.momentum > 0 else -1 if context["momentum_rank"] > context["rank_count"] - tail and row.momentum < 0 else 0
            if previous_side is not None and side and side != previous_side:
                candidates.append(Candidate(str(row.security_id), str(row.ticker), "discovery", "1d", side, "WATCH",
                    row.session, row.bar_end.to_pydatetime(), session_windows(str(exchange_calendars.get_calendar("XNYS").next_session(row.session).date()))[0][0],
                    row.visible_at.to_pydatetime(), float(row.close), float(row.close), float(row.close), float(row.atr_prior),
                    float(row.close), float(context["rs_percentile"]), 0., 0., float(row.liquidity), float(row.momentum),
                    tuple(context["rank_revision_ids"])))
            previous_side = side
        else:
            previous_side = None
        result.append(dict(security_id=str(row.security_id), interval="1d", market_time=row.bar_end.to_pydatetime(),
            available_at=row.visible_at.to_pydatetime(), ready=context is not None, candidates=candidates,
            updates=updates, revision_ids=[row.revision_id],
            lifecycles={f"{model}:{direction}": life for (model, direction), life in states.items()}))
    return result


def higher_context_observations(frame, config):
    calendar = exchange_calendars.get_calendar("XNYS")
    output = []
    for interval, frequency in (("1wk", "W-FRI"), ("1mo", "M")):
        periods = pd.to_datetime(frame.session).dt.to_period(frequency)
        previous = None
        for period, group in frame.groupby(periods, sort=True):
            expected = calendar.sessions_in_range(period.start_time, period.end_time.normalize())
            complete = (list(group.session) == [str(session.date()) for session in expected]
                        and group.clock_valid.all() and group.segment.nunique() == 1)
            if not complete:
                previous = None
                continue
            last = group.iloc[-1]
            direction = 0 if previous is None else int(np.sign(last.close - previous))
            output.append(dict(security_id=str(last.security_id), interval=interval,
                market_time=last.bar_end.to_pydatetime(), available_at=group.visible_at.max().to_pydatetime(),
                ready=previous is not None, candidates=[], lifecycles={}, revision_ids=group.revision_id.tolist(),
                updates=[dict(kind="CONTEXT", interval=interval, security_id=str(last.security_id),
                              direction=direction if previous is not None else None,
                              health="READY" if previous is not None else "UNAVAILABLE",
                              market_time=last.bar_end.isoformat(), source_revision_ids=group.revision_id.tolist())]))
            previous = float(last.close)
    return output