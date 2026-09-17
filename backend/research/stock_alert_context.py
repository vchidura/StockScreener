"""Pure, point-in-time readiness and annotation values for stock alerts."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import math
from zoneinfo import ZoneInfo

import exchange_calendars
import pandas as pd


VERSION = "stock_alert_context_v1"
READINESS_VERSION = "stock_alert_context_readiness_v1"
DAILY_SAMPLES = 253
SPLIT_PRICE_BASIS = "REVIEWED_SPLIT_ADJUSTED_PRICE_V1"


def utc(value):
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("context timestamps must be timezone aware")
    return parsed.astimezone(timezone.utc)


def observation(status, reason=None, **fields):
    return dict(status=status, reason_codes=[reason] if reason else [], value=None, market_time=None,
        release_at=None, observed_at=None, created_at=None, available_at=None, source_revision_ids=[], **fields)


def financial_context(reports, security_id, reference, cutoff):
    cutoff = utc(cutoff)
    if not reference or str(reference["security_id"]) != str(security_id):
        return observation("UNAVAILABLE", "FINANCIAL_SECURITY_IDENTITY_UNAVAILABLE")
    if reference["security_type"] != "CS":
        return observation("NOT_APPLICABLE", "NOT_A_COMMON_STOCK")
    latest = {}
    for report in reports:
        if str(report["security_id"]) != str(security_id) or report["timeframe"] not in ("quarterly", "annual", "trailing_twelve_months"):
            continue
        if not all(report.get(field) for field in ("availability_time", "observed_at", "created_at", "filing_date", "period_end")):
            continue
        available = max(utc(report[field]) for field in ("availability_time", "observed_at", "created_at"))
        period = date.fromisoformat(str(report["period_end"]))
        if available > cutoff or period > cutoff.date() or date.fromisoformat(str(report["filing_date"])) > cutoff.date():
            continue
        key = (period, utc(report["availability_time"]), utc(report["observed_at"]), utc(report["created_at"]), str(report["fundamental_report_id"]))
        previous = latest.get(report["timeframe"])
        if previous is None or key > previous[0]:
            latest[report["timeframe"]] = (key, report)
    if not latest:
        return observation("UNAVAILABLE", "NO_ELIGIBLE_FILED_FINANCIAL_REPORT")
    fields = ("revenue", "operating_income", "net_income", "diluted_eps", "cash_and_equivalents",
        "current_debt", "long_term_debt", "operating_cash_flow", "capital_expenditures", "free_cash_flow")
    retained, reasons = [], {"STATEMENT_UNITS_NOT_VERIFIED", "CASH_FLOW_PERIOD_BASIS_NOT_VERIFIED"}
    for timeframe, (_, report) in sorted(latest.items()):
        metrics = {}
        for field in fields:
            raw = report.get(field)
            value = float(raw) if raw is not None else None
            metrics[field] = value if value is not None and math.isfinite(value) else None
        quality = list(report.get("quality_codes") or [])
        age = (cutoff.date() - date.fromisoformat(str(report["period_end"]))).days
        stale = age > (550 if timeframe == "annual" else 180)
        reasons.update(quality)
        if stale:
            reasons.add("FINANCIAL_REPORT_STALE")
        retained.append(dict(report_id=str(report["fundamental_report_id"]), timeframe=timeframe,
            period_end=str(report["period_end"]), filing_date=str(report["filing_date"]), age_days=age,
            stale=stale, source=report["source"], accession_number=report.get("accession_number"),
            quality_codes=quality, units="RETAINED_PROVIDER_UNITS_UNVERIFIED", metrics=metrics))
    reports = [item[1] for item in latest.values()]
    result = observation("STALE" if all(row["stale"] for row in retained) else "PARTIAL")
    result.update(reason_codes=sorted(reasons), value=dict(reports=retained, ratios=None, growth=None),
        market_time=max(utc(report["availability_time"]) for report in reports).isoformat(),
        observed_at=max(utc(report["observed_at"]) for report in reports).isoformat(),
        created_at=max(utc(report["created_at"]) for report in reports).isoformat(),
        available_at=max(max(utc(report[field]) for field in ("availability_time", "observed_at", "created_at")) for report in reports).isoformat(),
        source_revision_ids=sorted(str(report["fundamental_report_id"]) for report in reports))
    return result


def reviewed_split_history(ordered, actions, reviews, cutoff):
    from research.stock_idea_engine import digest
    reviewed, slots = [], set()
    for action in actions:
        slot = (str(action["security_id"]), str(action["effective_date"]))
        if action["action_type"] != "SPLIT" or slot in slots:
            raise ValueError("unreviewed or ambiguous corporate action")
        slots.add(slot)
        matches = [review for review in reviews if review.get("action_revision_id") == str(action["revision_id"])]
        if len(matches) != 1:
            raise ValueError("exact split review required")
        review = matches[0]
        if (review.get("status") != "REVIEWED" or not review.get("evidence_url")
            or len(review.get("evidence_sha256", "")) != 64
            or utc(review["evidence_observed_at"]) > utc(review["reviewed_at"])
            or max(utc(action[field]) for field in ("first_observed_at", "created_at")) > utc(review["reviewed_at"])
                or utc(review["reviewed_at"]) > cutoff or not action.get("payload_sha256")
                or review.get("action_payload_sha256") != action["payload_sha256"]
                or str(review.get("security_id")) != str(action["security_id"])
                or review.get("ticker") != ordered[-1]["ticker"] or action.get("ticker") != review["ticker"]
                or str(review.get("effective_date")) != str(action["effective_date"])):
            raise ValueError("split review identity, evidence or availability mismatch")
        before, after = float(action["split_from"]), float(action["split_to"])
        if (not all(math.isfinite(value) and value > 0 for value in (before, after))
                or before != float(review["split_from"]) or after != float(review["split_to"])):
            raise ValueError("invalid split terms")
        reviewed.append((action, review, before / after))
    adjusted = []
    for bar in ordered:
        if bar.get("adjusted", False) or bar.get("context_split_factor", 1) != 1:
            raise ValueError("source bars must remain unadjusted")
        factor = math.prod(ratio for action, _, ratio in reviewed if bar["session"] < str(action["effective_date"]))
        transformed = dict(bar, **{field: float(bar[field]) * factor for field in ("open", "high", "low", "close")},
            volume=float(bar["volume"]) / factor, context_split_factor=factor)
        if not all(math.isfinite(transformed[field]) for field in ("open", "high", "low", "close", "volume")):
            raise ValueError("split adjustment is not finite")
        adjusted.append(transformed)
    return adjusted, [dict(action_revision_id=str(action["revision_id"]), effective_date=str(action["effective_date"]),
        price_factor=ratio, reviewed_at=review["reviewed_at"], review_sha256=digest(review),
        evidence_url=review["evidence_url"]) for action, review, ratio in reviewed]


def daily_price_context(bars, security_id, session, cutoff, actions=(), *, include_history=False, expected_ticker=None, split_reviews=None):
    cutoff = utc(cutoff)
    calendar = exchange_calendars.get_calendar("XNYS")
    ending = calendar.date_to_session(session, direction="previous")
    expected = [str(item.date()) for item in calendar.sessions_in_range(calendar.session_offset(ending, 1 - DAILY_SAMPLES), ending)]
    expected_set = set(expected)
    stored, timely = set(), {}
    late = 0
    for bar in bars:
        if str(bar["security_id"]) != security_id or bar["session"] not in expected_set:
            continue
        stored.add(bar["session"])
        if max(utc(bar["system_observed_at"]), utc(bar["created_at"]), utc(bar["bar_end"])) > cutoff:
            late += 1
            continue
        precedence = {"RECONCILED": 0, "DERIVED": 1}.get(bar.get("source_kind"), 2)
        key = (precedence, -utc(bar["system_observed_at"]).timestamp(), -utc(bar["created_at"]).timestamp(), str(bar["revision_id"]))
        old = timely.get(bar["session"])
        if old is None or key < old[0]:
            timely[bar["session"]] = (key, bar)
    ordered = [timely[item][1] for item in expected if item in timely]
    result = observation("INSUFFICIENT_HISTORY", "MISSING_EXPECTED_DAILY_HISTORY", expected_observations=DAILY_SAMPLES,
        stored_observations=len(stored), timely_observations=len(ordered), late_revisions=late,
        coverage_fraction=len(ordered) / DAILY_SAMPLES, session=str(ending.date()),
        price_basis="RAW_ACTION_GATED" if split_reviews is None else SPLIT_PRICE_BASIS)
    if ordered:
        result.update(market_time=ordered[-1]["bar_end"], observed_at=max(utc(bar["system_observed_at"]) for bar in ordered).isoformat(),
            created_at=max(utc(bar["created_at"]) for bar in ordered).isoformat(),
            available_at=max(max(utc(bar["system_observed_at"]), utc(bar["created_at"])) for bar in ordered).isoformat(),
            source_revision_ids=[str(bar["revision_id"]) for bar in ordered])
    if len(ordered) != DAILY_SAMPLES:
        return result
    for bar in ordered:
        prices = [bar[key] for key in ("open", "high", "low", "close")]
        session_open, session_close = calendar.session_open(bar["session"]), calendar.session_close(bar["session"])
        if (any(not math.isfinite(value) or value <= 0 for value in prices) or not math.isfinite(bar["volume"]) or bar["volume"] < 0
            or (split_reviews is not None and (bar.get("adjusted", False) or bar.get("context_split_factor", 1) != 1))
                or bar["high"] < max(prices) or bar["low"] > min(prices)
                or utc(bar["bar_start"]) != session_open or utc(bar["bar_end"]) != session_close):
            return result | dict(status="UNAVAILABLE", reason_codes=["INVALID_DAILY_BAR"])
    if len({bar["ticker"] for bar in ordered}) != 1 or (expected_ticker and ordered[-1]["ticker"] != expected_ticker):
        return result | dict(status="UNAVAILABLE", reason_codes=["IDENTITY_CONTINUITY_REVIEW"])
    relevant = [action for action in actions if str(action["security_id"]) == security_id
        and action["action_type"] in ("SPLIT", "MERGER", "SPINOFF", "SYMBOL_CHANGE")
        and expected[0] < str(action["effective_date"]) <= expected[-1]
        and max(utc(action["first_observed_at"]), utc(action["created_at"])) <= cutoff]
    if relevant and split_reviews is not None:
        try:
            ordered, adjustments = reviewed_split_history(ordered, relevant, split_reviews, cutoff)
        except (ValueError, KeyError, TypeError, OverflowError, ZeroDivisionError):
            return result | dict(status="UNAVAILABLE", reason_codes=["SPLIT_REVIEW_REQUIRED_OR_INVALID"],
                action_revision_ids=[str(action["revision_id"]) for action in relevant])
        result.update(split_adjustments=adjustments, adjustment_anchor_session=str(ending.date()),
            available_at=max([utc(result["available_at"])] + [utc(item["reviewed_at"]) for item in adjustments]
                + [utc(action[field]) for action in relevant for field in ("first_observed_at", "created_at")]).isoformat(),
            source_revision_ids=sorted(set(result["source_revision_ids"] + [str(action["revision_id"]) for action in relevant]
                + ["split-review:" + item["review_sha256"] for item in adjustments])))
    elif relevant:
        return result | dict(status="UNAVAILABLE", reason_codes=["CORPORATE_ACTION_REVIEW"],
            action_revision_ids=[str(action["revision_id"]) for action in relevant],
            action_review=[dict(type=action["action_type"], effective_date=str(action["effective_date"]),
                split_from=action.get("split_from"), split_to=action.get("split_to"), source=action.get("source"),
                revision_id=str(action["revision_id"])) for action in relevant])
    close = pd.Series([bar["close"] for bar in ordered], dtype=float)
    ema = close.ewm(span=50, adjust=False).mean()
    returns20 = close.pct_change(20).dropna()
    prior_returns20 = returns20.iloc[:-1].tail(252)
    percentile_ready = len(prior_returns20) >= 202 and prior_returns20.nunique() > 1
    return20_percentile = float(((prior_returns20 < returns20.iloc[-1]).sum() + .5 * (prior_returns20 == returns20.iloc[-1]).sum()) / len(prior_returns20)) if percentile_ready else None
    direction = "UP" if close.iloc[-1] > ema.iloc[-1] > ema.iloc[-11] else "DOWN" if close.iloc[-1] < ema.iloc[-1] < ema.iloc[-11] else "NEUTRAL"
    values = dict(direction=direction, close=float(close.iloc[-1]), ema50=float(ema.iloc[-1]),
        ema50_change10=float(ema.iloc[-1] / ema.iloc[-11] - 1),
        return_previous5=float(close.iloc[-6] / close.iloc[-11] - 1),
        sma50=float(close.iloc[-50:].mean()), sma200=float(close.iloc[-200:].mean()),
        realized_volatility20=float(close.pct_change().iloc[-20:].std(ddof=1) * math.sqrt(252)),
        return20_percentile=return20_percentile, return20_percentile_samples=len(prior_returns20),
        volume=float(ordered[-1]["volume"]),
        range252_low=min(float(bar["low"]) for bar in ordered[-252:]),
        range252_high=max(float(bar["high"]) for bar in ordered[-252:]),
        **{f"return{horizon}": float(close.iloc[-1] / close.iloc[-horizon - 1] - 1) for horizon in (1, 5, 20)})
    result.update(status="READY", reason_codes=[], value=values)
    if include_history:
        result["history"] = [dict(bar) for bar in ordered]
    return result


def same_time_volume_context(bars, security_id, boundary, cutoff, actions=()):
    calendar = exchange_calendars.get_calendar("XNYS")
    boundary, cutoff = utc(boundary), utc(cutoff)
    session = calendar.date_to_session(boundary.date())
    opening, closing = calendar.session_open(session), calendar.session_close(session)
    elapsed = boundary - opening.to_pydatetime()
    if elapsed.total_seconds() <= 0 or boundary > closing or elapsed.total_seconds() % 1800:
        raise ValueError("volume boundary must be a completed native30m window")
    candidates = calendar.sessions_in_range(calendar.session_offset(session, -40), calendar.previous_session(session))
    prior = [item for item in candidates if calendar.session_close(item) - calendar.session_open(item) == closing - opening][-20:]
    sessions = prior + [session]
    expected = {str(item.date()): [(calendar.session_open(item) + timedelta(minutes=30 * index)).to_pydatetime()
        for index in range(1, int(elapsed.total_seconds() / 1800) + 1)] for item in sessions}
    result = observation("INSUFFICIENT_HISTORY", "TWENTY_COMPARABLE_VOLUME_SESSIONS_REQUIRED", session=str(session.date()),
        expected_observations=20, timely_observations=0, boundary=boundary.isoformat(),
        elapsed_minutes=int(elapsed.total_seconds() / 60), session_length_minutes=int((closing - opening).total_seconds() / 60),
        price_basis="RAW_ACTION_GATED", comparison="SAME_ELAPSED_TIME_AND_SESSION_LENGTH")
    chosen = {}
    for bar in bars:
        end = utc(bar["bar_end"])
        if str(bar["security_id"]) != security_id or bar["session"] not in expected or end not in expected[bar["session"]]:
            continue
        if max(end, utc(bar["system_observed_at"]), utc(bar["created_at"])) > cutoff:
            continue
        precedence = {"RECONCILED": 0, "DERIVED": 1}.get(bar.get("source_kind"), 2)
        key = (precedence, -utc(bar["system_observed_at"]).timestamp(), -utc(bar["created_at"]).timestamp(), str(bar["revision_id"]))
        if end not in chosen or key < chosen[end][0]:
            chosen[end] = (key, bar)
    complete, retained = {}, []
    for date, ends in expected.items():
        rows = [chosen[end][1] for end in ends if end in chosen]
        if len(rows) != len(ends) or any(utc(row["bar_start"]) != utc(row["bar_end"]) - timedelta(minutes=30)
                or not math.isfinite(row["volume"]) or row["volume"] < 0 for row in rows):
            continue
        complete[date] = sum(row["volume"] for row in rows)
        retained.extend(rows)
    result["timely_observations"] = sum(str(item.date()) in complete for item in prior)
    if retained:
        result.update(source_revision_ids=sorted({str(row["revision_id"]) for row in retained}),
            market_time=max(utc(row["bar_end"]) for row in retained).isoformat(),
            available_at=max(max(utc(row["bar_end"]), utc(row["system_observed_at"]), utc(row["created_at"])) for row in retained).isoformat())
    if str(session.date()) not in complete:
        return result | dict(status="UNAVAILABLE", reason_codes=["EXACT_CUMULATIVE_VOLUME_WINDOW_MISSING"])
    relevant = [row for row in actions if str(row["security_id"]) == security_id
        and row["action_type"] in ("SPLIT", "MERGER", "SPINOFF", "SYMBOL_CHANGE")
        and min(expected) < str(row["effective_date"]) <= str(session.date())
        and max(utc(row["first_observed_at"]), utc(row["created_at"])) <= cutoff]
    if relevant or len({row["ticker"] for row in retained}) != 1:
        return result | dict(status="UNAVAILABLE", reason_codes=["VOLUME_ACTION_OR_IDENTITY_REVIEW"])
    if len(prior) != 20 or result["timely_observations"] != 20:
        return result
    average = sum(complete[str(item.date())] for item in prior) / 20
    if average <= 0:
        return result | dict(status="UNAVAILABLE", reason_codes=["ZERO_COMPARABLE_VOLUME_BASELINE"])
    return result | dict(status="READY", reason_codes=[], value=dict(cumulative_volume=complete[str(session.date())],
        average_volume20=average, relative_volume=complete[str(session.date())] / average))


def market_context(spy, qqq):
    if spy["status"] != "READY" or qqq["status"] != "READY":
        return observation("UNAVAILABLE", "BENCHMARK_CONTEXT_UNAVAILABLE")
    if spy["session"] != qqq["session"]:
        return observation("UNAVAILABLE", "UNPAIRED_BENCHMARK_SESSIONS")
    direction = spy["value"]["direction"] if spy["value"]["direction"] == qqq["value"]["direction"] else "MIXED"
    if direction == "NEUTRAL":
        direction = "MIXED"
    return dict(status="READY", reason_codes=[], value=dict(direction=direction),
        market_time=max(spy["market_time"], qqq["market_time"]), release_at=None,
        observed_at=max(spy["observed_at"], qqq["observed_at"]), created_at=max(spy["created_at"], qqq["created_at"]),
        available_at=max(spy["available_at"], qqq["available_at"]),
        source_revision_ids=sorted(set(spy["source_revision_ids"] + qqq["source_revision_ids"])))


def native_window_context(bars, security_id, boundary, cutoff):
    boundary, cutoff = utc(boundary), utc(cutoff)
    candidates = [row for row in bars if row["security_id"] == security_id and utc(row["bar_end"]) == boundary]
    visible = [row for row in candidates if max(utc(row["system_observed_at"]), utc(row["created_at"]), boundary) <= cutoff]
    result = observation("UNAVAILABLE", "EXACT_NATIVE_WINDOW_NOT_AVAILABLE", expected_observations=1,
        stored_observations=int(bool(candidates)), timely_observations=int(bool(visible)))
    if not visible:
        return result
    row = max(visible, key=lambda item: (utc(item["system_observed_at"]), utc(item["created_at"]), item["revision_id"]))
    if row["close"] <= 0 or row["open"] <= 0 or row["volume"] <= 0 or not all(math.isfinite(row[key]) for key in ("open", "close", "volume")):
        return result | dict(reason_codes=["INVALID_NATIVE_WINDOW"])
    return result | dict(status="READY", reason_codes=[], value=dict(return30m=row["close"] / row["open"] - 1),
        market_time=row["bar_end"], observed_at=row["system_observed_at"], created_at=row["created_at"],
        available_at=max(utc(row["system_observed_at"]), utc(row["created_at"])).isoformat(), source_revision_ids=[row["revision_id"]])


def dated_reference(references, security_id, ticker, cutoff):
    visible = [row for row in references if str(row["security_id"]) == security_id
        and max(utc(row["effective_from"]), utc(row["observed_at"]), utc(row["created_at"])) <= utc(cutoff)]
    row = max(visible, key=lambda item: (utc(item["effective_from"]), utc(item["observed_at"]), str(item["revision_id"])), default=None)
    return row if row and row["ticker"] == ticker and row["active"] else None


def sector_context(reference, stock, proxy, spy):
    from research.gics_sectors import SECTOR_BENCHMARK_ETF, sector_for_sic
    if reference is None:
        return observation("UNAVAILABLE", "DATED_SECURITY_REFERENCE_MISSING")
    if reference["security_type"] != "CS":
        return observation("NOT_APPLICABLE", "NOT_A_COMMON_STOCK")
    sector = sector_for_sic(reference.get("sic_code"))
    proxy_ticker = SECTOR_BENCHMARK_ETF.get(sector)
    result = observation("UNAVAILABLE", "SECTOR_PRICE_CONTEXT_MISSING", sector=sector, proxy=proxy_ticker,
        mapping_basis="SIC_DERIVED_PROXY_NOT_LICENSED_GICS", reference_id=str(reference["revision_id"]))
    if not proxy_ticker:
        return result | dict(reason_codes=["UNMAPPED_DATED_SIC"])
    if any(item is None or item["status"] != "READY" for item in (stock, proxy, spy)):
        return result
    if len({item["session"] for item in (stock, proxy, spy)}) != 1:
        return result | dict(reason_codes=["UNPAIRED_CONTEXT_SESSIONS"])
    return result | dict(status="READY", reason_codes=[],
        value=dict(direction=proxy["value"]["direction"], sector_minus_spy20=proxy["value"]["return20"] - spy["value"]["return20"],
            stock_minus_sector20=stock["value"]["return20"] - proxy["value"]["return20"]),
        market_time=stock["market_time"], observed_at=max(item["observed_at"] for item in (stock, proxy, spy)),
        created_at=max(item["created_at"] for item in (stock, proxy, spy)),
        available_at=max([item["available_at"] for item in (stock, proxy, spy)] + [utc(reference["observed_at"]).isoformat(), utc(reference["created_at"]).isoformat()]),
        source_revision_ids=sorted(set([str(reference["revision_id"])] + [key for item in (stock, proxy, spy) for key in item["source_revision_ids"]])))


def event_context(ticker, event_type, start, end, cutoff, events, coverage, *, max_age_seconds=43200, source="public_calendar_v1"):
    if start is None or end is None:
        return observation("NOT_APPLICABLE", "NO_EXECUTABLE_HORIZON")
    start, end, cutoff = utc(start), utc(end), utc(cutoff)
    scope = ticker if event_type == "EARNINGS" else None
    eligible = [row for row in coverage if row["event_type"] == event_type and row["affected_underlying"] == scope and row["source"] == source
        and max(utc(row["first_observed_at"]), utc(row["created_at"])) <= cutoff
        and utc(row["first_observed_at"]) >= cutoff - timedelta(seconds=max_age_seconds)
        and utc(row["window_start"]) <= start and utc(row["window_end"]) >= end]
    latest = {}
    for row in events:
        if row["event_type"] != event_type or row["affected_underlying"] != scope or row["source"] != source:
            continue
        if max(utc(row["first_observed_at"]), utc(row["created_at"])) > cutoff:
            continue
        key = row["source_key"]
        if key not in latest or utc(row["first_observed_at"]) > utc(latest[key]["first_observed_at"]):
            latest[key] = row
    active, timing = [], {}
    eastern = ZoneInfo("America/New_York")
    for row in latest.values():
        if row["status"] not in ("SCHEDULED", "REVISED"):
            continue
        scheduled = utc(row["scheduled_time"])
        relation = None
        if row["confidence"] != "CONFIRMED":
            if start.astimezone(eastern).date() <= scheduled.astimezone(eastern).date() <= end.astimezone(eastern).date():
                relation = "SAME_DAY_TIME_UNCERTAIN"
        elif start <= scheduled <= end:
            relation = "IN_HOLDING_HORIZON"
        elif cutoff < scheduled < start:
            relation = "UPCOMING_BEFORE_ENTRY"
        elif cutoff - timedelta(minutes=30) <= scheduled <= cutoff:
            relation = "RECENT_EVENT"
        if relation:
            active.append(row)
            timing[str(row["revision_id"])] = relation
    used = active + eligible
    result = observation("READY" if eligible else "UNAVAILABLE", None if eligible else "EVENT_COVERAGE_UNKNOWN",
        coverage_ids=[str(row["revision_id"]) for row in eligible], event_ids=[str(row["revision_id"]) for row in active])
    if used:
        result.update(source_revision_ids=[str(row["revision_id"]) for row in used],
            market_time=max((row["scheduled_time"] for row in active), default=None),
            observed_at=max(utc(row["first_observed_at"]) for row in used).isoformat(),
            created_at=max(utc(row["created_at"]) for row in used).isoformat(),
            available_at=max(max(utc(row["first_observed_at"]), utc(row["created_at"])) for row in used).isoformat())
    if eligible or active:
        relations = set(timing.values())
        risk = ("EVENT_IN_HORIZON" if "IN_HOLDING_HORIZON" in relations else "EVENT_TIME_UNCERTAIN" if "SAME_DAY_TIME_UNCERTAIN" in relations
            else "UPCOMING_BEFORE_ENTRY" if "UPCOMING_BEFORE_ENTRY" in relations else "RECENT_EVENT" if active else "NO_KNOWN_EVENT")
        result["value"] = dict(risk=risk, coverage_complete=bool(eligible),
            timing_uncertain=any(row["confidence"] != "CONFIRMED" for row in active),
            timing_version="stock_alert_event_timing_v2", recent_lookback_minutes=30,
            holding_start=start.isoformat(), holding_end=end.isoformat(),
            events=[dict(type=row["event_type"], scheduled_time=row["scheduled_time"], confidence=row["confidence"],
                relative_timing=timing[str(row["revision_id"])] ) for row in active])
    return result


def shadow_event_decision(factors):
    names = ("earnings", "fomc")
    blocked, unknown, considered = [], [], []
    for name in names:
        factor = factors.get(name, {})
        if factor.get("status") == "NOT_APPLICABLE":
            continue
        considered.append(name)
        value = factor.get("value") or {}
        if factor.get("status") != "READY" or not value.get("coverage_complete") or value.get("timing_uncertain"):
            unknown.append(name)
        if any(event.get("relative_timing") == "IN_HOLDING_HORIZON" and event.get("confidence") == "CONFIRMED"
                for event in value.get("events", [])):
            blocked.append(name)
    return dict(policy_version="shadow_holding_event_v1", live_gate_enabled=False,
        disposition="WOULD_BLOCK" if blocked else "UNKNOWN" if unknown else "WOULD_ALLOW" if considered else "NOT_APPLICABLE",
        blocking_factors=blocked, unknown_factors=unknown,
        interpretation="RECONSTRUCTED_CUTOFF_DIAGNOSTIC_NOT_PRESELECTION_EXECUTION")


def comparable_iv_context(rows, ticker, bucket, cutoff, required_session, policy):
    visible = [row for row in rows if row["underlying"] == ticker and row["expiration_bucket"] == bucket
        and max(utc(row["market_time"]), utc(row["first_observed_time"]), utc(row["created_at"]), utc(row["analysis_completed_at"])) <= utc(cutoff)]
    row = max(visible, key=lambda item: (utc(item["market_time"]), utc(item["first_observed_time"])), default=None)
    if row is None:
        return observation("NOT_COVERED", "COMPARABLE_IV_CONTEXT_NOT_RETAINED")
    result = observation("INSUFFICIENT_HISTORY", "COMPARABLE_IV_GATES_NOT_MET", sample_count=row["sample_count"],
        coverage_fraction=row["coverage_fraction"], expected_observations=policy.iv_lookback_sessions,
        history_availability_mode=row["history_availability_mode"])
    result.update(market_time=row["market_time"], observed_at=row["first_observed_time"], created_at=row["created_at"],
        available_at=max(utc(row["first_observed_time"]), utc(row["created_at"]), utc(row["analysis_completed_at"])).isoformat(),
        source_revision_ids=[str(row["revision_id"])])
    if utc(row["market_time"]).date().isoformat() < required_session:
        return result | dict(status="STALE", reason_codes=["IV_CONTEXT_STALE"])
    if (row["sample_count"] is None or row["sample_count"] < policy.minimum_iv_sample_sessions
            or row["coverage_fraction"] is None or row["coverage_fraction"] < policy.minimum_iv_coverage_fraction
            or row["null_reason_codes"] or row["empirical_percentile"] is None):
        return result
    return result | dict(status="READY", reason_codes=[], value=dict(iv=row["current_comparable_iv"],
        percentile=row["empirical_percentile"], range_position_rank=row["range_position_rank"], maturity=bucket))