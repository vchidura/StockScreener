"""Exact finalized RTH inputs for independently pinned native hourly research."""
from datetime import timedelta

import exchange_calendars
import pandas as pd

from .dashboard_projection import SCANNER_BASIS
from .preflight import ProviderTier
from .screening_projection import _validate_bars
from .strategy_projection import strategy_action_context


HOURLY_VERSION = "HOURLY_STRATEGIES_V1"
HOURLY_HISTORY_VERSION = "HOURLY_STRATEGY_HISTORY_V1"
HOURLY_RECONSTRUCTED_VERSION = "HOURLY_RECONSTRUCTED_EVALUATION_V1"
HOURLY_BASIS = dict(SCANNER_BASIS, price="RAW_UNADJUSTED_RTH_ACTION_GATED_OHLC", volume="RAW_UNADJUSTED_RTH_VOLUME",
    weekly="NATIVE_HOURLY_RESAMPLED_D_INCLUDES_FORMING_CURRENT_DAY",
    setup_confirmation="COMPLETED_RAW_DAILY_ONLY", fibonacci="INTERVAL_AWARE_ATR_STRUCTURAL_CONFIRMED_LEG",
    opening_gaps="SESSION_OPENING_EVENTS_HOURLY_LIFECYCLE", finality="FINALIZED_XNYS_RTH_SLOTS_INCLUDING_PARTIAL_CLOSE")


def session_hourly_slots(calendar, session):
    start, close = calendar.session_open(str(session)).to_pydatetime(), calendar.session_close(str(session)).to_pydatetime()
    slots = []
    while start < close:
        end = min(start + timedelta(hours=1), close)
        slots.append((start, end))
        start = end
    return slots

def split_coverage_ref(row):
    return dict(coverage_id=str(row["coverage_id"]), security_id=str(row["security_id"]), action_type=row["action_type"],
        window_start=str(row["window_start"]), window_end=str(row["window_end"]), response_action_count=row["response_action_count"],
        response_sha256=row["response_sha256"], payload_sha256=row["payload_sha256"])

def hourly_split_coverage(inputs, member, first_session):
    actions = [row for row in inputs.actions if str(row["security_id"]) == str(member["security_id"])]
    return [row for row in inputs.coverage if str(row["security_id"]) == str(member["security_id"])
        and row["action_type"] == "SPLIT" and row["window_start"] <= first_session and row["window_end"] >= inputs.session
        and row["received_at"] <= inputs.cutoff and row["response_action_count"] == sum(
            action["action_type"] == "SPLIT" and row["window_start"] <= action["effective_date"] <= row["window_end"] for action in actions)
        and all(action["received_at"] <= row["received_at"] for action in actions if action["action_type"] == "SPLIT"
                and row["window_start"] <= action["effective_date"] <= row["window_end"])]


def hourly_strategy_inputs(inputs, market_time=None):
    calendar = exchange_calendars.get_calendar("XNYS")
    delay = timedelta(minutes=15 if inputs.provider_tier is ProviderTier.MASSIVE_DEVELOPER else 0)
    slots = session_hourly_slots(calendar, inputs.session)
    finalized = [end for _, end in slots if end + delay <= inputs.cutoff]
    if not finalized:
        raise ValueError("Hourly strategy has no provider-qualified finalized slot")
    target = market_time or finalized[-1]
    if target not in finalized:
        raise ValueError("Hourly strategy cutoff must be a provider-qualified finalized RTH slot")
    if (len(inputs.source_manifest_sha256) != 64 or any(value not in "0123456789abcdef" for value in inputs.source_manifest_sha256)
            or type(inputs.source_generation) is not int or inputs.source_generation < 1
            or not 0 < len(inputs.members) <= 1000
            or len({row["ticker"] for row in inputs.members}) != len(inputs.members)
            or len({str(row["security_id"]) for row in inputs.members}) != len(inputs.members)
            or set(row["ticker"] for row in inputs.bars) - {row["ticker"] for row in inputs.members}):
        raise ValueError("Hourly strategy exact cohort/manifest/generation drift")
    frames, raw, unavailable, context = {}, {}, {}, {}
    for member in inputs.members:
        ticker = member["ticker"]
        if (not member.get("security_revision_id") or member.get("security_type") not in {"CS", "ETF", "ETV"}
                or member["observed_at"] > inputs.cutoff):
            raise ValueError("Hourly strategy reference identity/availability drift")
        own = [row for row in inputs.bars if row["ticker"] == ticker and row["interval"] not in {"1wk", "1mo"}]
        _validate_bars(own, member, inputs)
        primary = sorted((row for row in own if row["interval"] == "1h" and row["adjustment_basis"] == "RAW_UNADJUSTED"
                          and row["bar_end"] <= target), key=lambda row: row["bar_start"])
        for row in own:
            clocks = [row["observed_at"], row["created_at"], row["bar_end"] + delay]
            clocks.extend(row[key] for key in ("replay_available_at", "provider_published_at") if row.get(key) is not None)
            if any(clock.tzinfo is None or clock.utcoffset() is None or clock > inputs.cutoff for clock in clocks):
                raise ValueError("Hourly strategy source clock/delay drift")
        reason = None
        if primary:
            expected = [slot for session in calendar.sessions_in_range(str(primary[0]["session_date"]), str(inputs.session))
                        for slot in session_hourly_slots(calendar, session.date()) if slot[1] <= target]
            if [(row["bar_start"], row["bar_end"]) for row in primary] != expected:
                reason = "HOURLY_SLOT_HISTORY_INCOMPLETE"
            if not hourly_split_coverage(inputs, member, primary[0]["session_date"]):
                reason = "HOURLY_SPLIT_COVERAGE_UNAVAILABLE"
        actions = [row for row in inputs.actions if str(row["security_id"]) == str(member["security_id"])]
        if any(row["ticker"] != ticker or row["received_at"] > inputs.cutoff or row["observed_at"] > inputs.cutoff for row in actions):
            raise ValueError("Hourly strategy action identity/availability drift")
        relevant = [row for row in actions if primary and primary[0]["session_date"] < row["effective_date"] <= inputs.session]
        if relevant:
            refs = [strategy_action_context(row) for row in relevant]
            action_reason = "CORPORATE_ACTION_REVIEW" if any(row["handling"] == "CORPORATE_ACTION_REVIEW" for row in refs) else None
            context[ticker] = dict(reason=action_reason, available_bars=len(primary), action_refs=refs)
            reason = action_reason or reason
        if reason:
            unavailable[ticker] = dict(context.get(ticker, {}), reason=reason, available_bars=len(primary))
        if primary:
            frame = pd.DataFrame([{key: row[key] for key in ("open", "high", "low", "close", "volume")} for row in primary],
                                 index=pd.DatetimeIndex([row["bar_start"] for row in primary]).tz_convert("America/New_York"))
        else:
            frame = pd.DataFrame(columns=["open", "high", "low", "close", "volume"], index=pd.DatetimeIndex([]))
        frames[ticker] = frame
        raw[ticker] = primary if primary and primary[-1]["bar_end"] == target else []
    return target, unavailable, context, frames, raw