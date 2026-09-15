"""Research-only state machines for the three versioned equity hypotheses."""
from dataclasses import asdict, dataclass
import hashlib
import json
from uuid import NAMESPACE_URL, uuid5

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class StrategyConfig:
    family: str
    variant: str
    horizon: int
    rs_threshold: float = .7
    chase_atr: float = 1.0
    failure_sessions: int = 3


CONFIGURATIONS = tuple(
    [StrategyConfig("relative_trend_resumption_v1", f"rs{int(value * 100)}", 21, rs_threshold=value)
     for value in (.6, .7, .8)]
    + [StrategyConfig("range_breakout_acceptance_v1", f"chase{value:g}", 10, chase_atr=value)
       for value in (.5, 1., 1.5)]
    + [StrategyConfig("failed_extension_reversal_v1", f"deadline{value}", 5, failure_sessions=value)
       for value in (1, 2, 3)]
)
CENTRAL_VARIANTS = {"relative_trend_resumption_v1": "rs70", "range_breakout_acceptance_v1": "chase1",
                    "failed_extension_reversal_v1": "deadline3"}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False).encode()).hexdigest()


def enrich_prices(frame, spy_returns, ranks):
    frame = frame.copy().sort_index()
    if not frame.index.is_unique:
        raise ValueError("v2 requires one price per session")
    grouped = frame.groupby("segment", sort=False)
    prior_close = grouped.close.shift(1)
    frame["true_range"] = pd.concat([frame.high - frame.low, (frame.high - prior_close).abs(),
                                    (frame.low - prior_close).abs()], axis=1).max(axis=1)
    for _, group in grouped:
        close = group.close
        frame.loc[group.index, "ema20"] = close.ewm(span=20, adjust=False).mean()
        ema50 = close.ewm(span=50, adjust=False).mean()
        frame.loc[group.index, "ema50"] = ema50
        frame.loc[group.index, "ema50_prior10"] = ema50.shift(10)
        frame.loc[group.index, "atr_prior"] = frame.loc[group.index, "true_range"].ewm(alpha=1 / 14, adjust=False).mean().shift(1)
        frame.loc[group.index, "rs63"] = close / close.shift(63) - 1 - spy_returns.reindex(group.index)
        frame.loc[group.index, "warmup"] = np.arange(1, len(group) + 1)
        frame.loc[group.index, "range_ratio"] = frame.loc[group.index, "true_range"].rolling(5).mean().shift(1) / frame.loc[group.index, "true_range"].rolling(20).mean().shift(1)
    frame["rs_percentile"] = ranks.reindex(frame.index)
    frame["ready"] = ((frame.warmup >= 200) & frame.clock_valid & ~frame.identity_disagreement
                      & np.isfinite(frame[["open", "high", "low", "close", "volume", "atr_prior"]]).all(axis=1)
                      & (frame.atr_prior > 0))
    return frame


def detect(frame, config):
    events, transitions = [], []
    if frame.empty:
        return events, transitions
    frame = frame.sort_index()
    if not frame.index.is_unique:
        raise ValueError("duplicate decision session")
    states = {1: None, -1: None}
    retired = set()
    prefix = hashlib.sha256()
    segment = None

    def transition(state, status, session, reason=None):
        transitions.append(dict(setup_id=state["setup_id"], family=config.family, variant=config.variant,
                                ticker=state["ticker"], direction=state["direction"], session=str(session),
                                status=status, reason=reason))

    for position, (session, row) in enumerate(frame.iterrows()):
        if segment != row.segment or not row.ready:
            for state in states.values():
                if state is not None:
                    transition(state, "INVALIDATED", session, "INPUT_SEGMENT_OR_READINESS_CHANGE")
            states = {1: None, -1: None}
        if segment != row.segment:
            prefix = hashlib.sha256()
        segment = row.segment
        prefix.update((str(row.bar_revision_id) + "\n").encode())
        if not row.ready or position < 20:
            continue
        prior = frame.iloc[position - 1]
        for direction in (1, -1):
            sign = direction
            is_trend = config.family == "relative_trend_resumption_v1"
            is_acceptance = config.family == "range_breakout_acceptance_v1"
            trend = (np.isfinite(row.rs63) and np.isfinite(row.rs_percentile) and sign * row.rs63 > 0
                     and (row.rs_percentile >= config.rs_threshold if sign == 1 else row.rs_percentile <= 1 - config.rs_threshold)
                     and sign * (row.close - row.ema50) > 0 and sign * (row.ema50 - row.ema50_prior10) > 0)
            state = states[direction]
            if state is not None:
                state["extreme"] = min(state["extreme"], row.low) if sign == 1 else max(state["extreme"], row.high)
                age = position - state["anchor_position"]
                trigger = False
                reason = None
                if is_trend:
                    if not trend or sign * (state["reference"] - state["extreme"]) > 3 * state["atr"]:
                        reason = "TREND_OR_DEPTH_INVALID"
                    elif age > 10:
                        reason = "DEADLINE"
                    else:
                        trigger = age >= 2 and sign * (row.close - (prior.high if sign == 1 else prior.low)) > 0 and sign * (row.close - row.ema20) > 0
                elif is_acceptance:
                    trigger = age == 1 and sign * (row.close - state["reference"]) > 0
                    if not trigger:
                        reason = "ACCEPTANCE_FAILED"
                else:
                    if age > config.failure_sessions:
                        reason = "DEADLINE"
                    else:
                        trigger = (age >= 1 and sign * (row.close - state["reference"]) > .1 * state["atr"]
                                   and sign * (row.close - row.open) > 0 and sign * (row.close - prior.close) > 0)
                if trigger:
                    stop = state["extreme"] - sign * .1 * row.atr_prior
                    data = dict(family=config.family, variant=config.variant, configuration=asdict(config),
                                ticker=str(row.ticker), security_id=str(row.security_id), direction=sign,
                                session=str(session), setup_id=state["setup_id"], setup_session=state["setup_session"],
                                reference=state["reference"], stop=float(stop), target=state["target"],
                                atr_at_activation=state["atr"], atr_at_trigger=float(row.atr_prior),
                                signal_close=float(row.close), horizon=config.horizon, source_prefix_sha256=prefix.hexdigest())
                    data["event_id"] = str(uuid5(NAMESPACE_URL, "strategy-v2:" + digest(data)))
                    events.append(data)
                    transition(state, "TRIGGERED", session)
                elif reason:
                    transition(state, "EXPIRED" if reason in ("DEADLINE", "ACCEPTANCE_FAILED") else "INVALIDATED", session, reason)
                if trigger or reason:
                    retired.add(state["setup_id"])
                    states[direction] = None
                continue
            window = frame.iloc[position - (20 if not is_acceptance else 10):position]
            if window.segment.nunique() != 1 or window.segment.iloc[-1] != segment:
                continue
            high, low = float(window.high.max()), float(window.low.min())
            reference, target, anchor_position = None, None, position
            if is_trend and trend:
                extreme_column = window.high if sign == 1 else window.low
                reference = high if sign == 1 else low
                anchor_session = extreme_column[extreme_column == reference].index[-1]
                anchor_position = frame.index.get_loc(anchor_session)
                depth = sign * (reference - row.close)
                if not 1 <= position - anchor_position <= 10 or not row.atr_prior <= depth <= 3 * row.atr_prior:
                    continue
                target = reference
            elif is_acceptance:
                reference = high if sign == 1 else low
                if not (0 < high - low <= 4 * row.atr_prior and row.range_ratio <= .75
                        and low <= prior.close <= high and sign * (row.close - reference) > .15 * row.atr_prior):
                    continue
                target = reference + sign * (high - low)
            elif not is_trend:
                reference = low if sign == 1 else high
                extension = row.low if sign == 1 else row.high
                if not (low <= prior.close <= high and sign * (row.close - reference) < 0
                        and sign * (reference - extension) >= .5 * row.atr_prior):
                    continue
                target = (high + low) / 2
            else:
                continue
            setup_id = str(uuid5(NAMESPACE_URL, "v2-setup:" + digest([config.family, config.variant, str(row.ticker),
                             str(row.security_id), sign, str(frame.index[anchor_position]), reference])))
            if setup_id in retired:
                continue
            episode = frame.iloc[anchor_position + (1 if is_trend else 0):position + 1]
            state = dict(setup_id=setup_id, direction=sign, ticker=str(row.ticker), setup_session=str(session),
                         reference=reference, target=target, atr=float(row.atr_prior), anchor_position=anchor_position,
                         extreme=float(episode.low.min() if sign == 1 else episode.high.max()))
            states[direction] = state
            transition(state, "SETUP", session)
    return events, transitions


def entry_status(event, price, volume):
    if not np.isfinite(price) or price <= 0 or not np.isfinite(volume) or volume < 0:
        return "UNRESOLVED_ENTRY"
    if volume == 0:
        return "ZERO_VOLUME_NO_FILL"
    direction = event["direction"]
    risk = direction * (price - event["stop"])
    room = direction * (event["target"] - price)
    if risk <= 0 or room <= 0:
        return "ENTRY_OUTSIDE_BRACKET"
    if room < risk:
        return "INSUFFICIENT_TARGET_ROOM"
    if event["family"] == "range_breakout_acceptance_v1":
        distance = direction * (price - event["reference"])
        if distance < 0 or distance > event["configuration"]["chase_atr"] * event["atr_at_activation"]:
            return "ENTRY_CHASE_OR_BOUNDARY_FAILED"
    return "FILLED"