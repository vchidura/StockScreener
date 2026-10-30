"""Roadmap detectors. Pure research evaluations; no database, orders, or fills.

State is returned, never mutated in place. A confirmed opportunity is emitted once;
later evaluations must establish a fresh structural setup before another match.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import math

import numpy as np
import pandas as pd

from .catalog import CATALOG, digest, research_policy
from .data import Snapshot, Unavailable, number, ready_value as val, stamp


class NoMatch(Exception):
    pass


class Checks:
    def __init__(self):
        self.values = []

    def gate(self, name, actual, passed, threshold=None):
        if isinstance(actual, (np.floating, np.integer)):
            actual = actual.item()
        self.values.append(dict(name=name, value=actual, threshold=threshold,
                                status="PASS" if bool(passed) else "FAIL"))
        if not passed:
            raise NoMatch(name.upper())


def location(row, direction=1):
    span = row.high - row.low
    return ((row.close - row.low) if direction == 1 else (row.high - row.close)) / span if span > 0 else 0


def suggested(row, stop, target, direction, anchor):
    if min(row.close, stop, target) <= 0 or direction * (row.close - stop) <= 0 or direction * (target - row.close) <= 0:
        raise NoMatch("INVALID_STRUCTURAL_GEOMETRY")
    return dict(direction=direction, reference=float(row.close), stop=float(stop),
                target=float(target), anchor=str(anchor))


def common(snapshot, policy, checks):
    member, actions, price_basis = snapshot.require("eligibility", "member", "actions_covered", "price_basis")
    checks.gate("dated_membership", member, member is True)
    if actions is not True or price_basis not in {"RAW_CONTINUOUS", "SPLIT_ADJUSTED"}:
        raise Unavailable("CORPORATE_ACTION_COVERAGE_REQUIRED")
    if policy.macro_policy == "required":
        covered, blocked = snapshot.require("events", "macro_covered", "macro_blocked")
        if covered is not True:
            raise Unavailable("MACRO_CALENDAR_UNCOVERED")
        checks.gate("macro_event_window", blocked, blocked is False)
    if policy.style == "SWING":
        covered_through, earnings = snapshot.require("events", "earnings_covered_through", "earnings_sessions")
        end = snapshot.data.session_offset(snapshot.session, policy.horizon)
        if str(covered_through) < end:
            raise Unavailable("EARNINGS_HORIZON_UNCOVERED")
        checks.gate("future_earnings", earnings, not any(snapshot.session <= s <= end for s in earnings))


def daily(snapshot, minimum=253):
    frame = snapshot.history("1d", prior_day=snapshot.interval != "1d")
    if len(frame) < minimum:
        raise Unavailable("DAILY_WARMUP_REQUIRED")
    return frame


def trend(snapshot, direction, checks, *, enhanced=False):
    row = daily(snapshot).iloc[-1]
    rs, percentile = snapshot.require("relative_strength", "rs63", "percentile63")
    rs, percentile = number(rs), number(percentile)
    checks.gate("relative_strength", rs, direction * rs > 0)
    checks.gate("strength_percentile", percentile, percentile >= .7 if direction == 1 else percentile <= .3)
    checks.gate("stock_trend", float(row.close), direction * (row.close - row.ema50) > 0 and
                direction * (row.ema50 - row.ema50_prior10) > 0)
    if enhanced:
        continuation(snapshot, direction, checks)
        checks.gate("daily_adx", val(row, "adx"), val(row, "adx") >= 25, 25)
        checks.gate("daily_di", float(row.plus_di - row.minus_di), direction * (row.plus_di - row.minus_di) > 0)
        sector_rs, = snapshot.require("relative_strength", "sector_rs20")
        checks.gate("sector_relative_return20", number(sector_rs), direction * number(sector_rs) > 0)


def continuation(snapshot, direction, checks):
    market_id, sector_id = snapshot.require("benchmarks", "market_security_id", "sector_security_id")
    for name, security in (("market", market_id), ("sector", sector_id)):
        frame = snapshot.history("1d", security, prior_day=True)
        if len(frame) < 220:
            raise Unavailable(f"{name.upper()}_WARMUP_REQUIRED")
        row = frame.iloc[-1]
        checks.gate(f"{name}_alignment", float(row.close), direction * (row.close - row.ema50) > 0 and
                    direction * (row.ema50 - row.ema200) > 0 and direction * (row.ema50 - row.ema50_prior10) > 0)


def _family(snapshot, policy, state, checks):
    frame = snapshot.history()
    if len(frame) < 200:
        raise Unavailable("TRIGGER_WARMUP_REQUIRED")
    row, previous = frame.iloc[-1], frame.iloc[-2]
    atr = val(row, "atr_prior")
    if atr <= 0:
        raise Unavailable("NONPOSITIVE_ATR")
    drow = daily(snapshot).iloc[-1]
    checks.gate("liquidity", val(drow, "liquidity20"), row.close >= 5 and val(drow, "liquidity20") >= 20e6)
    family = policy.family
    use_context = policy.variant in {"context", "combined", "retest"}
    use_participation = policy.variant in {"participation", "combined", "retest"}
    pending = state.get("pending")
    if pending and pending["session"] != snapshot.session:
        state.pop("pending", None)
        pending = None
    if pending:
        direction, ref, frozen = pending["direction"], pending["reference"], pending["atr"]
        age = len(frame.loc[frame.market_at > stamp(pending["setup_at"])])
        # Missing confirmation bars cannot be silently skipped.
        seconds = {"30m": 1800, "1h": 3600}[policy.interval]
        if (row.market_at - stamp(pending["last_at"])).total_seconds() != seconds:
            state.pop("pending", None)
            raise NoMatch("SETUP_CONFIRMATION_GAP_INVALIDATED")
        pending["last_at"] = row.market_at.isoformat()
        old_extreme = pending["extreme"]
        pending["extreme"] = min(old_extreme, row.low) if direction == 1 else max(old_extreme, row.high)
        deadline = 10 if family == "resumption" else 3 if family == "failure" or policy.variant == "retest" else 1
        elapsed = (row.market_at - stamp(pending["setup_at"])).total_seconds() / 60
        reference_age = pending.get("reference_age", 0) + age
        resumption_expired = family == "resumption" and (reference_age > 10 or
            (policy.style == "INTRADAY" and reference_age * seconds / 60 > 300))
        if age > deadline or resumption_expired:
            state.pop("pending", None)
            raise NoMatch("SETUP_EXPIRED")
        if family == "resumption":
            try:
                trend(snapshot, direction, checks, enhanced=use_context)
            except NoMatch:
                state.pop("pending", None)
                raise
            if direction * (ref - pending["extreme"]) > 3 * frozen:
                state.pop("pending", None)
                raise NoMatch("PULLBACK_INVALIDATED")
            confirmed = (age >= 2 and direction * (row.close - (previous.high if direction == 1 else previous.low)) > 0
                         and direction * (row.close - row.ema20) > 0)
        elif family == "acceptance":
            if policy.variant == "retest":
                if direction * (row.close - ref) < -.1 * frozen:
                    state.pop("pending", None)
                    raise NoMatch("RETEST_INVALIDATED")
                touch = row.low <= ref + .2 * frozen if direction == 1 else row.high >= ref - .2 * frozen
                pending["touched"] = pending.get("touched", False) or touch
                confirmed = pending["touched"] and direction * (row.close - ref) > 0 and direction * (
                    row.close - (previous.high if direction == 1 else previous.low)) > 0
            else:
                confirmed = (age == 1 and direction * (row.close - ref) >= .15 * frozen and
                             direction * (row.close - row.open) > 0 and direction * (row.close - previous.close) > 0
                             and location(row, direction) >= .5)
                if not confirmed:
                    state.pop("pending", None)
                    raise NoMatch("NEXT_BAR_ACCEPTANCE_FAILED")
        else:
            reentry = (direction * (row.close - ref) >= .1 * frozen and direction * (row.close - row.open) > 0
                       and direction * (row.close - previous.close) > 0)
            if policy.variant != "combined":
                confirmed = reentry
            elif pending.get("reentry_at"):
                if direction * ((row.low if direction == 1 else row.high) - pending["extension_extreme"]) <= 0:
                    state.pop("pending", None)
                    raise NoMatch("EXTENSION_EXTREME_BREACHED")
                confirmed = direction * (row.close - pending["reentry_break"]) > 0
            else:
                confirmed = False
                if reentry and location(row, direction) >= .65 and val(row, "slot_rvol") >= 1.2:
                    pending.update(reentry_at=row.market_at.isoformat(),
                                   reentry_break=float(row.high if direction == 1 else row.low))
        if not confirmed:
            if age == deadline:
                state.pop("pending", None)
                raise NoMatch("SETUP_EXPIRED")
            raise NoMatch("AWAITING_CONFIRMATION")
        # Consume the opportunity even if a challenger filter rejects it.
        state.pop("pending", None)
        state["last_anchor"] = pending["anchor"]
        if use_context:
            if family == "acceptance":
                continuation(snapshot, direction, checks)
            elif family == "failure":
                checks.gate("range_regime_adx", val(drow, "adx"), val(drow, "adx") < 20, 20)
                aligned, material = snapshot.require("reversal_context", "aligned_breakout", "material_repricing_event")
                checks.gate("no_directional_repricing", [aligned, material], aligned is False and material is False)
        if use_participation and not (family == "failure" and policy.variant == "combined"):
            threshold = 1.0 if family == "acceptance" else 1.2
            checks.gate("confirmation_rvol", val(row, "slot_rvol"), val(row, "slot_rvol") >= threshold, threshold)
            checks.gate("confirmation_location", location(row, direction), location(row, direction) >= .65, .65)
            if family == "acceptance":
                checks.gate("adx_rising3", val(row, "adx_change3"), val(row, "adx_change3") > 0, 0)
        if policy.variant in {"combined", "retest"} and family != "failure":
            boundary = (previous.high if direction == 1 else previous.low) if family == "resumption" else ref
            chase = direction * (row.close - boundary) / frozen
            checks.gate("chase", float(chase), 0 <= chase <= .5, .5)
        return suggested(row, pending["extreme"] - direction * .1 * atr, pending["target"], direction, pending["anchor"])
    size = 10 if family == "acceptance" else 20
    window = frame.iloc[-size-1:-1]
    high, low = float(window.high.max()), float(window.low.min())
    for direction in (1, -1):
        ref = high if direction == 1 else low
        anchor = f"{family}:{direction}:{window.market_at.iloc[0].isoformat()}:{window.market_at.iloc[-1].isoformat()}"
        extreme = float(row.low if direction == 1 else row.high)
        reference_age = 0
        if family == "resumption":
            # A failed side must not hide a valid opposite-side setup.
            side_checks = Checks()
            try:
                trend(snapshot, direction, side_checks, enhanced=use_context)
            except NoMatch:
                continue
            extreme_indices = window.index[(window.high if direction == 1 else window.low) == ref]
            anchor_index = extreme_indices[-1]
            age = frame.index[-1] - anchor_index
            if not 1 <= age <= 10 or not atr <= direction * (ref - row.close) <= 3 * atr:
                continue
            if policy.style == "INTRADAY" and age * {"30m": 30, "1h": 60}[policy.interval] > 300:
                continue
            reference_age = int(age)
            anchor = f"{family}:{direction}:{frame.loc[anchor_index, 'market_at'].isoformat()}"
            target = ref
            episode = frame.loc[anchor_index + 1:]
            extreme = float(episode.low.min() if direction == 1 else episode.high.max())
        elif family == "acceptance":
            if not (0 < high - low <= 4 * atr and val(row, "compression") <= .75 and low <= previous.close <= high
                    and direction * (row.close - ref) > .15 * atr):
                continue
            if use_participation:
                checks.gate("breakout_rvol", val(row, "slot_rvol"), val(row, "slot_rvol") >= 1.5, 1.5)
            if policy.variant in {"combined", "retest"}:
                checks.gate("breakout_chase", direction * (row.close - ref) / atr,
                            direction * (row.close - ref) <= .5 * atr, .5)
            target = ref + direction * (high - low)
        else:
            ref = low if direction == 1 else high
            extension = direction * (ref - extreme) / atr
            if not low <= previous.close <= high or direction * (row.close - ref) >= 0 or extension < .5:
                continue
            if policy.variant == "combined":
                checks.gate("extension_limit", extension, extension <= 1.5, 1.5)
            target = (high + low) / 2
        if state.get("last_anchor") == anchor:
            continue
        state["pending"] = dict(direction=direction, reference=ref, atr=atr, target=target,
                                extreme=extreme, extension_extreme=extreme, anchor=anchor,
                                reference_age=reference_age,
                                setup_at=row.market_at.isoformat(), last_at=row.market_at.isoformat(),
                                session=snapshot.session)
        raise NoMatch("SETUP_DEVELOPING")
    raise NoMatch("NO_STRUCTURAL_SETUP")


def _position(snapshot, policy, state, checks):
    frame = daily(snapshot, 273)
    row = frame.iloc[-1]
    next_session = snapshot.data.session_offset(snapshot.session, 1)
    checks.gate("month_end", snapshot.session, next_session[:7] != snapshot.session[:7])
    common_stock, sector = snapshot.require("fundamentals", "common_stock", "sector")
    checks.gate("comparable_company", [common_stock, sector], common_stock is True and sector not in {"FINANCIALS", "REIT"})
    checks.gate("liquidity", val(row, "liquidity60"), row.close >= 5 and val(row, "liquidity60") >= 50e6, 50e6)
    # Ranks must be built from the same dated eligible population, including delisted names.
    cohort, complete = snapshot.require("fundamental_ranks", "cohort_id", "coverage_complete")
    if not cohort or complete is not True:
        raise Unavailable("POINT_IN_TIME_RANK_COHORT_INCOMPLETE")
    if policy.family == "P1":
        mom, quality = snapshot.require("fundamental_ranks", "total_return_12_minus_1_percentile", "sector_quality_percentile")
        assets, = snapshot.require("fundamentals", "assets")
        checks.gate("positive_assets", number(assets), number(assets) > 0)
        checks.gate("momentum_rank", number(mom), .8 <= number(mom) <= 1, .8)
        checks.gate("quality_rank", number(quality), .7 <= number(quality) <= 1, .7)
        checks.gate("sma200_trend", float(row.close), row.close > row.sma200 > row.sma200_prior20)
    else:
        rank, = snapshot.require("fundamental_ranks", "sector_value_percentile")
        cfo, capex, ev, coverage, margin, prior_margin = map(number, snapshot.require(
            "fundamentals", "operating_cash_flow", "capital_expenditure", "enterprise_value",
            "interest_coverage", "operating_margin", "prior_operating_margin"))
        checks.gate("value_rank", number(rank), .75 <= number(rank) <= 1, .75)
        checks.gate("cash_flow", cfo - capex, cfo - capex > 0 and capex >= 0 and ev > 0)
        checks.gate("interest_coverage", coverage, coverage > 3, 3)
        checks.gate("margin_improvement", margin - prior_margin, margin > prior_margin)
        checks.gate("price_stabilization", float(row.close), row.close > row.sma100 and val(row, "return63") > 0)
    anchor = snapshot.session[:7]
    if state.get("last_anchor") == anchor:
        raise NoMatch("ALREADY_RECORDED_MONTH")
    state["last_anchor"] = anchor
    # The roadmap has no frozen numeric catastrophe stop for Position. Do not invent one.
    return dict(direction=1, reference=float(row.close), stop=None, target=None, anchor=anchor)


def _swing(snapshot, policy, state, checks):
    frame = snapshot.history("1h")
    if len(frame) < 200:
        raise Unavailable("HOURLY_WARMUP_REQUIRED")
    row, previous = frame.iloc[-1], frame.iloc[-2]
    dframe = daily(snapshot)
    drow = dframe.iloc[-1]
    checks.gate("liquidity", val(drow, "liquidity20"), row.close >= 5 and val(drow, "liquidity20") >= 20e6)
    setup = state.get("pending")
    if setup and snapshot.session > setup["deadline"]:
        state.pop("pending", None)
        setup = None
    if setup is None:
        if state.get("last_daily") == drow.session:
            raise NoMatch("DAILY_SETUP_ALREADY_CONSUMED")
        atr = val(drow, "atr_prior")
        if policy.family == "SW1":
            ref = float(dframe.high.iloc[-21:-1].max())
            checks.gate("daily_trend", float(drow.close), drow.close > drow.ema20 > drow.ema50)
            checks.gate("directional_adx", val(drow, "adx"), val(drow, "adx") >= 25 and drow.plus_di > drow.minus_di)
            rs, = snapshot.require("relative_strength", "sector_rs63")
            checks.gate("sector_relative_strength", number(rs), number(rs) > 0)
            sector_id, = snapshot.require("benchmarks", "sector_security_id")
            sector = snapshot.history("1d", sector_id, prior_day=True)
            if len(sector) < 200:
                raise Unavailable("SECTOR_WARMUP_REQUIRED")
            checks.gate("sector_trend", float(sector.close.iloc[-1]), sector.close.iloc[-1] > sector.ema50.iloc[-1])
            checks.gate("daily_breakout", float(drow.close), drow.close > ref + .1 * atr)
            checks.gate("daily_volume", float(drow.volume), drow.volume >= 1.5 * val(drow, "volume_median20"))
            deadline = snapshot.data.session_offset(drow.session, 2)
        else:
            event = snapshot.facts("earnings_reaction")
            required = ("announcement_at", "consensus_available_at", "actual_eps", "consensus_eps",
                        "reaction_session", "pre_announcement_close", "eps_basis_compatible")
            if any(key not in event for key in required):
                raise Unavailable("INCOMPLETE_EARNINGS_REACTION")
            announced = stamp(event["announcement_at"])
            consensus_at = stamp(event["consensus_available_at"])
            if consensus_at >= announced or announced > snapshot.at or event["eps_basis_compatible"] is not True:
                raise Unavailable("INVALID_CONSENSUS_VINTAGE_OR_EPS_BASIS")
            # First full regular session whose opening follows the announcement.
            event_day = str(announced.tz_convert("America/New_York").date())
            candidate = str(snapshot.data.calendar.date_to_session(event_day, direction="next").date())
            if snapshot.data.session_bounds(candidate)[0] <= announced:
                candidate = snapshot.data.session_offset(candidate, 1)
            if event["reaction_session"] != candidate or drow.session != candidate:
                raise NoMatch("NOT_NEXT_SESSION_AFTER_REACTION")
            actual, consensus = number(event["actual_eps"]), number(event["consensus_eps"])
            checks.gate("nonnegative_consensus", consensus, consensus >= 0)
            surprise = (actual - consensus) / max(abs(consensus), .05)
            checks.gate("eps_surprise", surprise, surprise >= .1, .1)
            checks.gate("reaction_price", float(drow.close), drow.close - number(event["pre_announcement_close"]) >= atr)
            checks.gate("reaction_location", location(drow), location(drow) >= .75, .75)
            checks.gate("reaction_volume", float(drow.volume), drow.volume >= 2 * val(drow, "volume_median20"))
            ref = float(drow.high)
            deadline = snapshot.data.session_offset(drow.session, 1)
        setup = dict(reference=ref, atr=atr, extreme=float(drow.low) if policy.family == "SW2" else float(row.low),
                     deadline=deadline, anchor=drow.session)
        state.update(pending=setup, last_daily=drow.session)
    ref, atr = setup["reference"], setup["atr"]
    if policy.family == "SW1":
        if row.close < ref - .2 * atr:
            state.pop("pending", None)
            raise NoMatch("RETEST_INVALIDATED")
        setup["extreme"] = min(setup["extreme"], float(row.low))
        checks.gate("hourly_retest", float(row.low), row.low <= ref + .25 * atr and row.high >= ref - .25 * atr)
        checks.gate("hourly_reclaim", float(row.close), row.close > ref and row.close > previous.high)
    else:
        checks.gate("reaction_high_break", float(row.close), ref < row.close <= ref + .25 * atr)
    stop = setup["extreme"] - .1 * atr
    state.pop("pending", None)
    return suggested(row, stop, row.close + 2 * (row.close - stop), 1, setup["anchor"])


def _intraday(snapshot, policy, state, checks):
    frame = snapshot.history("5m")
    if len(frame) < 200:
        raise Unavailable("FIVE_MINUTE_WARMUP_REQUIRED")
    row, previous = frame.iloc[-1], frame.iloc[-2]
    daily_row = daily(snapshot).iloc[-1]
    checks.gate("liquidity", val(daily_row, "liquidity20"), row.close >= 5 and val(daily_row, "liquidity20") >= 50e6)
    quote = snapshot.data.latest_quote(snapshot.security, snapshot.at, policy.max_quote_age_seconds)
    snapshot.lineage.add(f"{quote['source_id']}:{quote['revision']}")
    opening, closing = snapshot.data.session_bounds(snapshot.session)
    minute = (row.market_at - opening).total_seconds() / 60
    if policy.family == "D1":
        checks.gate("entry_window", minute, 15 <= minute <= 90)
        if state.get("matched_session") == snapshot.session:
            raise NoMatch("SESSION_EPISODE_ALREADY_CONFIRMED")
    else:
        checks.gate("entry_window", minute, 30 <= minute <= 300 and snapshot.at + timedelta(minutes=95) <= closing)
    if snapshot.at >= closing - timedelta(minutes=5):
        raise NoMatch("ENTRY_CUTOFF")
    atr = val(row, "atr_prior")
    today = frame.loc[frame.session == snapshot.session]
    pending = state.get("pending")
    if pending and pending["session"] != snapshot.session:
        state.pop("pending", None)
        pending = None
    if pending:
        last_at = stamp(pending.get("last_at", pending["setup_at"]))
        if row.market_at - last_at != timedelta(minutes=5):
            state.pop("pending", None)
            raise NoMatch("SETUP_CONFIRMATION_GAP_INVALIDATED")
        pending["last_at"] = row.market_at.isoformat()
    if policy.family == "D1":
        first = today.loc[today.market_at <= opening + timedelta(minutes=15)]
        if len(first) != 3 or first.start.iloc[0] != opening:
            raise Unavailable("INCOMPLETE_OPENING_RANGE")
        high, low = float(first.high.max()), float(first.low.min())
        width = (high - low) / val(daily_row, "atr_prior")
        checks.gate("opening_range_width", width, .2 <= width <= .8)
        checks.gate("cumulative_rvol", val(row, "cumulative_rvol"), val(row, "cumulative_rvol") >= 1.5, 1.5)
        market_id, = snapshot.require("benchmarks", "market_security_id")
        market = snapshot.history("5m", market_id)
        if market.market_at.iloc[-1] != row.market_at or len(market) < 2:
            raise Unavailable("MARKET_CUTOFF_MISMATCH")
        m, pm = market.iloc[-1], market.iloc[-2]
        checks.gate("market_vwap_trend", float(m.close), m.close > val(m, "session_vwap") > val(pm, "session_vwap"))
        if pending is None:
            checks.gate("opening_break", float(row.close), row.close > high + .1 * atr)
            state["pending"] = dict(session=snapshot.session, setup_at=row.market_at.isoformat(),
                                    reference=high, atr=atr, extreme=float(row.low))
            raise NoMatch("SETUP_DEVELOPING")
        elapsed = (row.market_at - stamp(pending["setup_at"])).total_seconds() / 300
        if elapsed > 3 or row.close < pending["reference"] - .1 * pending["atr"]:
            state.pop("pending", None)
            raise NoMatch("OPENING_RETEST_EXPIRED_OR_INVALID")
        pending["extreme"] = min(pending["extreme"], float(row.low))
        checks.gate("retest", float(row.low), elapsed >= 1 and row.low <= high + .1 * pending["atr"] and row.high >= high - .1 * pending["atr"])
        checks.gate("reclaim", float(row.close), row.close > high and location(row) >= .65)
        state["matched_session"] = snapshot.session
        reward = 2
    else:
        vwap = val(row, "session_vwap")
        if pending is None:
            if len(today) < 5:
                raise Unavailable("INSUFFICIENT_SESSION_VWAP_HISTORY")
            prior = today.iloc[-4:-1]
            if prior.session_vwap.isna().any():
                raise Unavailable("NATIVE_VWAP_REQUIRED")
            checks.gate("vwap_trend", vwap, bool((prior.close > prior.session_vwap).all()) and vwap > val(today.iloc[-4], "session_vwap"))
            checks.gate("ema_alignment", float(row.ema20 - row.ema50), row.ema20 > row.ema50)
            checks.gate("cumulative_rvol", val(row, "cumulative_rvol"), val(row, "cumulative_rvol") >= 1.2, 1.2)
            checks.gate("fresh_pullback", float(row.low), row.low <= vwap + .2 * atr and row.high >= vwap - .2 * atr
                        and row.close >= vwap - .25 * atr and previous.low > val(previous, "session_vwap") + .2 * atr)
            state["pending"] = dict(session=snapshot.session, setup_at=row.market_at.isoformat(), atr=atr, extreme=float(row.low))
            raise NoMatch("SETUP_DEVELOPING")
        elapsed = (row.market_at - stamp(pending["setup_at"])).total_seconds() / 300
        if elapsed > 4 or row.close < vwap - .25 * pending["atr"]:
            state.pop("pending", None)
            raise NoMatch("VWAP_PULLBACK_EXPIRED_OR_INVALID")
        pending["extreme"] = min(pending["extreme"], float(row.low))
        checks.gate("vwap_reclaim", float(row.close), elapsed >= 1 and row.close > max(vwap, previous.high))
        checks.gate("confirmation_rvol", val(row, "slot_rvol"), val(row, "slot_rvol") >= 1.2, 1.2)
        checks.gate("vwap_chase", (row.close - vwap) / pending["atr"], row.close - vwap <= .5 * pending["atr"], .5)
        reward = 1.5
    stop = pending["extreme"] - .1 * pending["atr"]
    state.pop("pending", None)
    return suggested(row, stop, row.close + reward * (row.close - stop), 1, pending["setup_at"])


def ofi_windows(snapshot):
    """Cont-style best-quote OFI, nonoverlapping seconds, prior 300-second z-score."""
    coverage, venue = snapshot.require("quote_coverage", "continuous_from", "venue")
    end = snapshot.at.floor("s")
    start = end - timedelta(seconds=305)
    if stamp(coverage) > start:
        raise Unavailable("FIVE_MINUTE_QUOTE_COVERAGE_REQUIRED")
    rows = [r for r in snapshot.data.quotes if r["security_id"] == snapshot.security and
            r["venue"] == venue and r["received_at"] <= snapshot.at and start - timedelta(seconds=1) <= r["market_at"] < end]
    rows.sort(key=lambda r: (r["market_at"], r["sequence"]))
    if len(rows) < 2 or rows[0]["market_at"] > start:
        raise Unavailable("QUOTE_BASELINE_REQUIRED")
    if any(b["sequence"] != a["sequence"] + 1 for a, b in zip(rows, rows[1:])):
        raise Unavailable("QUOTE_SEQUENCE_GAP")
    events = []
    for a, b in zip(rows, rows[1:]):
        e = ((b["bid_size"] if b["bid"] >= a["bid"] else 0) -
             (a["bid_size"] if b["bid"] <= a["bid"] else 0) -
             (b["ask_size"] if b["ask"] <= a["ask"] else 0) +
             (a["ask_size"] if b["ask"] >= a["ask"] else 0))
        events.append((b["market_at"].floor("s"), e, (b["ask"] + b["bid"]) / 2))
        snapshot.lineage.add(f"{b['source_id']}:{b['revision']}")
    frame = pd.DataFrame(events, columns=["second", "ofi", "mid"])
    index = pd.date_range(start, end - timedelta(seconds=1), freq="s")
    sums = frame.groupby("second").ofi.sum().reindex(index, fill_value=0)
    mids = frame.groupby("second").mid.last().reindex(index).ffill()
    prior = sums.shift(1)
    mean = prior.rolling(300).mean()
    std = prior.rolling(300).std(ddof=1)
    z = (sums - mean) / std.replace(0, np.nan)
    if z.tail(5).isna().any() or mids.tail(6).isna().any():
        raise Unavailable("UNSTABLE_OFI_NORMALIZATION")
    return z, mids


def _scalping(snapshot, policy, state, checks):
    covered, fees, acknowledgements, halted, news = snapshot.require(
        "microstructure_readiness", "trades_covered", "fees_qualified", "acknowledgements_qualified", "halted", "material_news")
    if not all(x is True for x in (covered, fees, acknowledgements)):
        raise Unavailable("MICROSTRUCTURE_INPUTS_UNQUALIFIED")
    checks.gate("no_halt_or_news", [halted, news], halted is False and news is False)
    quote = snapshot.data.latest_quote(snapshot.security, snapshot.at, .1)
    snapshot.lineage.add(f"{quote['source_id']}:{quote['revision']}")
    venue, = snapshot.require("quote_coverage", "venue")
    if quote["venue"] != venue:
        raise Unavailable("QUOTE_VENUE_MISMATCH")
    z, mids = ofi_windows(snapshot)
    spread, mid = quote["ask"] - quote["bid"], (quote["ask"] + quote["bid"]) / 2
    if policy.family == "C1":
        checks.gate("spread_bps", spread / mid * 10000, spread / mid * 10000 <= 2, 2)
        checks.gate("two_ofi_windows", [float(x) for x in z.tail(2)], bool((z.tail(2) >= 2).all()))
        checks.gate("no_adverse_mid", [float(x) for x in mids.tail(3)], bool((mids.tail(3).diff().dropna() >= 0).all()))
        forecast, cost, trained_until, loss = snapshot.require("microstructure_model", "forecast_move", "roundtrip_cost", "trained_until", "loss_budget")
        if stamp(trained_until) >= snapshot.at - timedelta(seconds=305):
            raise Unavailable("OUT_OF_SAMPLE_MICROSTRUCTURE_MODEL_REQUIRED")
        checks.gate("cost_hurdle", number(forecast) - number(cost), number(forecast) > number(cost) >= 0 and number(loss) > 0)
        stop, target, direction = mid - number(loss), mid + number(forecast), 1
    else:
        depth, execution, cancel, inventory, side, forecast, cost, trained_until = snapshot.require(
            "passive_execution", "venue_depth_qualified", "execution_reports_qualified", "cancel_latency_qualified",
            "inventory_units", "side", "expected_capture", "cost_and_adverse_selection", "trained_until")
        if not all(x is True for x in (depth, execution, cancel)):
            raise Unavailable("PASSIVE_EXECUTION_EVIDENCE_REQUIRED")
        if stamp(trained_until) >= snapshot.at - timedelta(seconds=305):
            raise Unavailable("OUT_OF_SAMPLE_FILL_MODEL_REQUIRED")
        checks.gate("inventory_limit", number(inventory), number(inventory) == 0)
        checks.gate("passive_side", side, side in {"BUY", "SELL"})
        checks.gate("quiet_ofi", [float(x) for x in z.tail(5)], bool((z.tail(5).abs() <= .5).all()))
        checks.gate("stable_mid", float(mids.tail(5).max() - mids.tail(5).min()), mids.tail(5).max() - mids.tail(5).min() <= quote["tick_size"])
        checks.gate("two_tick_spread", spread, spread >= 2 * quote["tick_size"])
        checks.gate("capture_after_costs", number(forecast) - number(cost), number(forecast) > number(cost) >= 0)
        direction = 1 if side == "BUY" else -1
        stop, target = mid - direction * 2 * quote["tick_size"], mid + direction * quote["tick_size"]
    if state.get("condition_active"):
        raise NoMatch("CONDITION_ALREADY_RECORDED")
    state["condition_active"] = True
    # Midpoint is a detection reference, emphatically not an assumed passive fill.
    return dict(direction=direction, reference=mid, stop=stop, target=target, anchor=snapshot.at.floor("s").isoformat())


def evaluate_research(snapshot: Snapshot, model_id: str, previous_state=None, *, macro_policy="required"):
    policy = research_policy(model_id, macro_policy)
    prior = deepcopy(previous_state or {})
    state = deepcopy(prior)
    checks = Checks()
    result = dict(model_id=model_id, policy_hash=policy.policy_hash, style=policy.style, origin=policy.origin,
                  security_id=snapshot.security, decision_at=snapshot.at.isoformat(), session=snapshot.session,
                  interval=policy.interval, publication_permission=False, execution_permission=False,
                  outcome_kind="DETECTION_PRICE_RETURN_ONLY")
    if prior.get("model_id", model_id) != model_id or prior.get("security_id", snapshot.security) != snapshot.security:
        raise ValueError("state belongs to another model or security")
    if prior.get("policy_hash", policy.policy_hash) != policy.policy_hash:
        raise ValueError("state policy mismatch")
    try:
        if snapshot.interval != policy.interval:
            raise Unavailable("POLICY_INTERVAL_MISMATCH")
        if prior.get("evaluated_at") and stamp(prior["evaluated_at"]) > snapshot.at:
            raise ValueError("decision clocks must be monotonic")
        if prior.get("evaluated_at") == snapshot.at.isoformat():
            result.update(status="NO_MATCH", reasons=["ALREADY_EVALUATED"], signal=None, gates=[], state=prior, input_ids=[])
            return result
        opening, closing = snapshot.data.session_bounds(snapshot.session)
        if policy.interval != "1d" and not opening <= snapshot.at < closing:
            raise NoMatch("OUTSIDE_SESSION")
        if policy.interval != "event":
            frame = snapshot.history()
            row = frame.iloc[-1]
            if row.session != snapshot.session:
                raise Unavailable("STALE_TRIGGER_SESSION")
            max_delay = 86400 if policy.interval == "1d" else {"5m": 300, "30m": 1800, "1h": 3600}[policy.interval]
            if (snapshot.at - row.market_at).total_seconds() >= max_delay:
                raise Unavailable("STALE_TRIGGER_BAR")
            if state.get("last_bar") == row.market_at.isoformat():
                raise NoMatch("ALREADY_EVALUATED_BAR")
            state["last_bar"] = row.market_at.isoformat()
            segment = [row.segment_id, row.adjustment_id]
            if state.get("segment", segment) != segment:
                state.pop("pending", None)
            state["segment"] = segment
        common(snapshot, policy, checks)
        if policy.family in {"resumption", "acceptance", "failure"}:
            signal = _family(snapshot, policy, state, checks)
        elif policy.style == "POSITION":
            signal = _position(snapshot, policy, state, checks)
        elif policy.family in {"SW1", "SW2"}:
            signal = _swing(snapshot, policy, state, checks)
        elif policy.family in {"D1", "D2"}:
            signal = _intraday(snapshot, policy, state, checks)
        else:
            signal = _scalping(snapshot, policy, state, checks)
        if policy.interval == "event":
            quote = snapshot.data.latest_quote(snapshot.security, snapshot.at, .1)
            signal.update(reference_at=quote["market_at"].isoformat(), adjustment_id=quote.get("adjustment_id"))
        else:
            row = snapshot.history().iloc[-1]
            signal.update(reference_at=row.market_at.isoformat(), adjustment_id=row.adjustment_id)
        signal["episode_id"] = digest(dict(model_id=model_id, policy_hash=policy.policy_hash,
                                           security_id=snapshot.security, anchor=signal["anchor"], direction=signal["direction"]))
        result.update(status="MATCH", reasons=[], signal=signal)
    except Unavailable as exc:
        # No mutation from a partially evaluated observation with missing evidence.
        state = prior
        result.update(status="UNAVAILABLE", reasons=[str(exc)], signal=None)
    except NoMatch as exc:
        if policy.style == "SCALPING" and str(exc) != "CONDITION_ALREADY_RECORDED":
            state["condition_active"] = False
        result.update(status="NO_MATCH", reasons=[str(exc)], signal=None)
    if result["status"] != "UNAVAILABLE":
        state.update(model_id=model_id, security_id=snapshot.security, policy_hash=policy.policy_hash,
                     evaluated_at=snapshot.at.isoformat())
    result.update(gates=checks.values, state=state, input_ids=sorted(snapshot.lineage))
    return result
