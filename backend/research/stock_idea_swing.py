"""Daily-owned swing plans with next-session intraday confirmation."""
from copy import deepcopy
from dataclasses import replace
import math

import exchange_calendars

from research.stock_idea_engine import candidate_record, entry_gate, read_candidate
from research.stock_idea_replay import available_at, session_windows, utc, valid_bar


SWING_VERSION = "stock_ideas_forward_swing_v1"
SWING_POLICY = "DAILY_SETUP_NEXT_SESSION_CONFIRMATION_V1"
HORIZONS = {"failure": 5, "acceptance": 10, "resumption": 21}


def bind_swing_candidate(daily, confirmation, cutoff):
    cutoff = utc(cutoff)
    calendar = exchange_calendars.get_calendar("XNYS")
    if (daily.interval != "1d" or daily.model not in HORIZONS
            or daily.horizon != f"DAILY_{HORIZONS[daily.model]}"
            or confirmation.interval not in ("30m", "1h") or confirmation.horizon != "INTRADAY"
            or any(getattr(daily, field) != getattr(confirmation, field) for field in ("security_id", "ticker", "model", "direction"))
            or daily.health != "READY" or confirmation.health != "READY"
            or daily.selection_block or confirmation.selection_block
            or not daily.revision_ids or not confirmation.revision_ids):
        return None
    session = calendar.next_session(str(daily.trigger_at.date()))
    opening, closing = calendar.session_open(session).to_pydatetime(), calendar.session_close(session).to_pydatetime()
    if (daily.trigger_at != calendar.session_close(str(daily.trigger_at.date())).to_pydatetime()
            or not opening < confirmation.trigger_at < closing
            or daily.available_at > opening or max(daily.available_at, confirmation.available_at, confirmation.trigger_at) > cutoff
            or confirmation.expires_at <= cutoff):
        return None
    risk = daily.direction * (confirmation.price - daily.stop)
    room = daily.direction * (daily.target - confirmation.price)
    if not all(math.isfinite(value) for value in (risk, room)) or min(risk, room) <= 0:
        return None
    bound = replace(daily, interval=confirmation.interval, trigger_at=confirmation.trigger_at,
        expires_at=min(confirmation.expires_at, closing), available_at=max(daily.available_at, confirmation.available_at),
        price=confirmation.price, room_risk=room / risk, policy_version=SWING_VERSION,
        anchor=daily.episode_id, revision_ids=tuple(sorted(set(daily.revision_ids + confirmation.revision_ids))))
    if entry_gate(bound, bound.price):
        return None
    return bound, dict(policy=SWING_POLICY, daily_episode_id=daily.episode_id,
        daily_setup=candidate_record(daily), confirmation=candidate_record(confirmation),
        holding_sessions=HORIZONS[daily.model], entry_session=str(session.date()),
        observed_at=cutoff.isoformat())


def preconfirmation_path(daily, state, boundary, cutoff, config):
    calendar = exchange_calendars.get_calendar("XNYS")
    session = str(calendar.next_session(str(daily.trigger_at.date())).date())
    if str(boundary.date()) != session:
        return "OUTSIDE_CONFIRMATION_SESSION", []
    if any(action["security_id"] == daily.security_id and action["action_type"] in ("SPLIT", "MERGER", "SPINOFF", "SYMBOL_CHANGE")
           and str(daily.trigger_at.date()) <= str(action["effective_date"]) <= session
           and max(utc(action["first_observed_at"]), utc(action["created_at"])) <= cutoff for action in state.get("actions", [])):
        return "SWING_ACTION_REVIEW_REQUIRED", []
    indexed = {utc(bar["bar_start"]): bar for bar in state.get("bars", {}).get(daily.security_id + "|30m", {}).values()
        if bar["security_id"] == daily.security_id and bar["ticker"] == daily.ticker and bar["interval"] == "30m"
        and available_at(bar, config) <= cutoff}
    revisions = []
    for start, end in session_windows(session):
        if end > boundary:
            break
        bar = indexed.get(start)
        if bar is None or utc(bar["bar_end"]) != end or not valid_bar(bar) or bar["volume"] <= 0:
            return "SWING_PRECONFIRMATION_PATH_UNAVAILABLE", revisions
        revisions.append(bar["revision_id"])
        if bar["low"] <= min(daily.stop, daily.target) or bar["high"] >= max(daily.stop, daily.target):
            return "DAILY_BRACKET_TOUCHED_BEFORE_ENTRY", revisions
    return (None if revisions else "SWING_PRECONFIRMATION_PATH_UNAVAILABLE"), revisions


def swing_candidates(state, candidates, *, boundary, cutoff, actual_time, ready, invalidated, may_seed, config):
    calendar = exchange_calendars.get_calendar("XNYS")
    book = deepcopy(state.get("swing_setup_book", {}))
    diagnostics, evidence, confirmed = [], {}, []
    if may_seed:
        for daily in candidates:
            if (daily.interval == "1d" and daily.model in HORIZONS and daily.trigger_at == boundary
                    and daily.health == "READY" and not daily.selection_block and daily.security_id in ready
                    and daily.available_at <= cutoff and actual_time < daily.expires_at and not entry_gate(daily, daily.price)):
                book.setdefault(daily.episode_id, dict(daily_setup=candidate_record(daily), retained_at=actual_time.isoformat()))
    for episode_id, saved in list(book.items()):
        daily = read_candidate(saved["daily_setup"])
        entry_session = calendar.next_session(str(daily.trigger_at.date()))
        closing = calendar.session_close(entry_session).to_pydatetime()
        if actual_time >= closing or episode_id in invalidated:
            diagnostics.append(dict(daily_episode_id=episode_id, status="EXPIRED_OR_INVALIDATED"))
            book.pop(episode_id)
            continue
        if boundary.date() < entry_session.date():
            diagnostics.append(dict(daily_episode_id=episode_id, status="AWAITING_NEXT_SESSION_CONFIRMATION"))
            continue
        if daily.security_id not in ready:
            diagnostics.append(dict(daily_episode_id=episode_id, status="CURRENT_SECURITY_NOT_READY"))
            continue
        problem, revisions = preconfirmation_path(daily, state, boundary, cutoff, config)
        if problem:
            diagnostics.append(dict(daily_episode_id=episode_id, status=problem))
            if problem in ("DAILY_BRACKET_TOUCHED_BEFORE_ENTRY", "SWING_ACTION_REVIEW_REQUIRED"):
                book.pop(episode_id)
            continue
        if saved.get("candidate"):
            bound = read_candidate(saved["candidate"])
            if bound.expires_at <= actual_time or read_candidate(saved["evidence"]["confirmation"]).episode_id in invalidated:
                diagnostics.append(dict(daily_episode_id=episode_id, status="CONFIRMATION_EXPIRED_OR_INVALIDATED"))
                continue
        else:
            matches = []
            for confirmation in sorted(candidates, key=lambda item: (item.trigger_at, item.interval, item.episode_id)):
                if confirmation.expires_at <= actual_time:
                    continue
                bound_result = bind_swing_candidate(daily, confirmation, cutoff)
                if bound_result:
                    matches.append(bound_result)
            if not matches:
                diagnostics.append(dict(daily_episode_id=episode_id, status="AWAITING_INTRADAY_CONFIRMATION"))
                continue
            bound, retained = matches[0]
            bound = replace(bound, revision_ids=tuple(sorted(set(bound.revision_ids) | set(revisions))))
            retained.update(preconfirmation_revision_ids=revisions, daily_retained_at=saved["retained_at"])
            saved.update(candidate=candidate_record(bound), evidence=retained)
        confirmed.append(bound)
        evidence[bound.episode_id] = saved["evidence"]
        diagnostics.append(dict(daily_episode_id=episode_id, status="CONFIRMED", episode_id=bound.episode_id))
    return book, confirmed, evidence, diagnostics