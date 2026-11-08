"""Live input for the research equity rules (docs/EQUITY_MODEL_LAB_DESIGN.md section 6).

Builds the `equity_research_dataset_v1` input that `research.equity_models` evaluates, from the realtime database:
completed 30m and full-hour RAW bars and ADJUSTED daily bars from `rt.bar_head` (point in time), split evidence for
continuity segments, and prior-day facts (eligibility, benchmarks, relative strength). The research code is run
unchanged, so live decisions are the decisions Model Lab evaluates offline. Read-only; no provider calls.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Mapping, Sequence

import exchange_calendars
import pandas as pd

from research.equity_models.catalog import CATALOG, research_policy
from research.equity_models.data import ResearchData, Snapshot, Unavailable, stamp
from research.equity_models.detectors import evaluate_research

UTC = timezone.utc
DATASET_SCHEMA = "equity_research_dataset_v1"
RESEARCH_RULES: tuple[str, ...] = tuple(
    [f"{name}_{interval}_{variant}" for name in ("R2", "B3", "F2") for interval in ("30m", "1h")
     for variant in ("structure", "context", "participation", "combined")]
    + ["B3_30m_retest", "B3_1h_retest", "SW3_R", "SW3_B", "SW3_F"]
)
RESEARCH_MACRO_POLICY = "none"          # only Fed dates exist; live events are optional since Nov 5
RESEARCH_EARNINGS_POLICY = "optional"   # the earnings calendar is N/A after Nov 9 (user decision)
TRIGGER_INTERVALS = ("30m", "1h")
# Sessions of history per interval: >= 200 trigger bars, 20 sessions of same-slot volume, 253 daily bars plus the
# 220 the benchmark EMA200 context needs.
HISTORY_SESSIONS = {"30m": 45, "1h": 60, "1d": 330}
FACT_METHODOLOGY = "realtime-research-facts-v1"
_SECONDS = {"30m": 1800, "1h": 3600}


@dataclass(frozen=True, slots=True)
class ResearchMember:
    security_id: str
    instrument_id: int
    ticker: str
    security_type: str | None
    sector_ticker: str | None   # benchmark ETF for a stock; None when it is not in the universe
    is_stock: bool


def rule_policy(model_id: str):
    return research_policy(model_id, RESEARCH_MACRO_POLICY, RESEARCH_EARNINGS_POLICY)


class CachedResearchData(ResearchData):
    """The research data with feature frames memoized by (security, interval, as-of): every rule evaluated for a
    security at one decision time shares one computation."""

    def __init__(self, payload):
        # Before the parent's validation, which already asks for session bounds.
        self._history_cache: dict[tuple[str, str, pd.Timestamp], pd.DataFrame] = {}
        self._session_bounds_cache: dict[str, tuple[pd.Timestamp, pd.Timestamp]] = {}
        self._bar_index: dict[tuple[str, str], list[dict]] | None = None
        super().__init__(payload)

    def bars_for(self, security: str, interval: str) -> list[dict]:
        """This security's bars of one interval, in receipt order (built once per input)."""
        if self._bar_index is None:
            index: dict[tuple[str, str], list[dict]] = {}
            for row in self.bars:
                index.setdefault((row["security_id"], row["interval"]), []).append(row)
            for rows in index.values():
                rows.sort(key=lambda row: (row["received_at"], row["market_at"]))
            self._bar_index = index
        return self._bar_index.get((security, interval), [])

    def session_bounds(self, session):
        key = str(session)
        if key not in self._session_bounds_cache:
            self._session_bounds_cache[key] = super().session_bounds(session)
        return self._session_bounds_cache[key]

    def history(self, security, interval, at):
        key = (security, interval, at)
        if key not in self._history_cache:
            self._history_cache[key] = super().history(security, interval, at)
        return self._history_cache[key]


def history_start(calendar, session: date, interval: str) -> datetime:
    first = calendar.session_offset(pd.Timestamp(session), -HISTORY_SESSIONS[interval])
    return calendar.session_open(first).to_pydatetime().astimezone(UTC)


def split_segments(actions: Iterable[Mapping[str, object]]) -> dict[str, list[date]]:
    """Split dates per security (ex date, else effective date); RAW bars change segment at each one."""
    result: dict[str, list[date]] = {}
    for row in actions:
        if str(row.get("action_type")) != "SPLIT":
            continue
        when = row.get("ex_date") or row.get("effective_date")
        if when is None:
            continue
        result.setdefault(str(row["security_id"]), []).append(pd.Timestamp(when).date())
    return {key: sorted(set(value)) for key, value in result.items()}


def _segment(security_id: str, basis: str, session: date, splits: Mapping[str, list[date]]) -> str:
    if basis == "ADJUSTED":
        return f"{security_id}:adjusted"
    before = [value for value in splits.get(security_id, ()) if value <= session]
    return f"{security_id}:raw-after:{before[-1].isoformat() if before else 'none'}"


def bar_records(member: ResearchMember, interval: str, rows: Sequence[Mapping[str, object]],
                splits: Mapping[str, list[date]], validator: ResearchData) -> tuple[list[dict], int]:
    """Research bar records; a row the research contract would refuse (the 15:30-16:00 hourly fragment, an
    early-close daily bar that does not span the calendar session, invalid OHLC) is left out and counted."""
    kept, skipped = [], 0
    for row in rows:
        start, end = row["bar_start"], row["bar_end"]
        if interval in _SECONDS and (end - start).total_seconds() != _SECONDS[interval]:
            skipped += 1
            continue
        observed = max(end, row["observed_at"])
        vwap = row.get("vwap")
        low, high = float(row["low_price"]), float(row["high_price"])
        record = dict(
            security_id=member.security_id, source_id=str(row["bar_revision_id"]), revision="1",
            start=start.isoformat(), market_at=end.isoformat(), available_at=observed.isoformat(),
            received_at=observed.isoformat(), interval=interval, completed=True,
            adjustment_id=str(row["adjustment_basis"]),
            segment_id=_segment(member.security_id, str(row["adjustment_basis"]), row["session_date"], splits),
            open=float(row["open_price"]), high=high, low=low, close=float(row["close_price"]),
            volume=float(row["volume"]),
            vwap=(None if vwap is None or not low <= float(vwap) <= high else float(vwap)),
        )
        try:
            validator._records([record], "bar")
        except ValueError:
            skipped += 1
            continue
        kept.append(record)
    return kept, skipped


def _returns(rows: Sequence[Mapping[str, object]], before: date) -> dict[int, float]:
    closes = [float(row["close_price"]) for row in rows if row["session_date"] < before]
    result = {}
    for period in (20, 63):
        if len(closes) > period and closes[-1 - period] > 0:
            result[period] = closes[-1] / closes[-1 - period] - 1
    return result


def daily_facts(members: Sequence[ResearchMember], daily: Mapping[int, Sequence[Mapping[str, object]]], *,
                session: date, prior_close: datetime, valid_until: datetime,
                covered: Mapping[str, bool], cohort_id: str) -> list[dict]:
    """Prior-completed-day facts per stock. Every value comes from bars or evidence that exist; a missing input leaves
    the value None (the research code then reads UNAVAILABLE), never a passing default."""
    by_ticker = {member.ticker: member for member in members}
    returns = {member.security_id: _returns(daily.get(member.instrument_id, ()), session) for member in members}
    spy = by_ticker.get("SPY")
    stocks = [member for member in members if member.is_stock and 63 in returns[member.security_id]]
    ranked = sorted(returns[member.security_id][63] for member in stocks)
    facts = []

    def fact(member, kind, values, inputs):
        facts.append(dict(
            security_id=member.security_id, source_id=f"{kind}:{member.security_id}:{session.isoformat()}",
            revision="1", kind=kind, market_at=prior_close.isoformat(), available_at=prior_close.isoformat(),
            received_at=prior_close.isoformat(), valid_until=valid_until.isoformat(),
            methodology=FACT_METHODOLOGY, input_ids=[str(value) for value in inputs] or [cohort_id], values=values,
        ))

    for member in members:
        if not member.is_stock:
            continue
        own = returns[member.security_id]
        sector = by_ticker.get(member.sector_ticker) if member.sector_ticker else None
        market_returns = returns[spy.security_id] if spy else {}
        sector_returns = returns[sector.security_id] if sector else {}
        fact(member, "eligibility", dict(member=True, actions_covered=bool(covered.get(member.security_id)),
                                         price_basis="RAW_CONTINUOUS"), [cohort_id])
        fact(member, "benchmarks", dict(market_security_id=spy.security_id if spy else None,
                                        sector_security_id=sector.security_id if sector else None),
             [cohort_id, *(m.ticker for m in (spy, sector) if m)])
        rs63 = own[63] - market_returns[63] if 63 in own and 63 in market_returns else None
        percentile = None
        if 63 in own and len(ranked) > 1:
            percentile = sum(value <= own[63] for value in ranked) / len(ranked)
        fact(member, "relative_strength", dict(
            rs63=rs63, percentile63=percentile,
            sector_rs20=(own[20] - sector_returns[20] if 20 in own and 20 in sector_returns else None),
            sector_rs63=(own[63] - sector_returns[63] if 63 in own and 63 in sector_returns else None),
            population=len(ranked)), [cohort_id])
    return facts


def build_research_payload(members: Sequence[ResearchMember], bars: Mapping[str, Mapping[int, Sequence[Mapping]]],
                           *, session: date, as_of: datetime, splits: Mapping[str, list[date]],
                           covered: Mapping[str, bool], cohort_id: str) -> tuple[dict, dict]:
    """The research input for one decision session, and a summary of what was left out."""
    calendar = exchange_calendars.get_calendar("XNYS")
    validator = CachedResearchData({"schema": DATASET_SCHEMA, "availability_mode": "OBSERVED"})
    records, skipped = [], {}
    for interval, by_instrument in bars.items():
        for member in members:
            kept, dropped = bar_records(member, interval, by_instrument.get(member.instrument_id, ()), splits,
                                        validator)
            records.extend(kept)
            if dropped:
                skipped[f"{member.ticker}:{interval}"] = dropped
    prior = calendar.session_offset(pd.Timestamp(session), -1)
    prior_close = calendar.session_close(prior).to_pydatetime().astimezone(UTC)
    valid_until = calendar.session_close(pd.Timestamp(session)).to_pydatetime().astimezone(UTC) + timedelta(hours=1)
    facts = daily_facts(members, bars.get("1d", {}), session=session, prior_close=prior_close,
                        valid_until=valid_until, covered=covered, cohort_id=cohort_id)
    payload = {
        "schema": DATASET_SCHEMA, "availability_mode": "OBSERVED",
        "evaluation_start": calendar.session_open(pd.Timestamp(session)).isoformat(),
        "evaluation_end": valid_until.isoformat(), "as_of": as_of.astimezone(UTC).isoformat(),
        "evaluation_securities": [member.security_id for member in members if member.is_stock],
        "bars": records, "facts": facts, "quotes": [],
    }
    return payload, {"bars": len(records), "facts": len(facts), "skipped": skipped}


def due_bar_times(data: ResearchData, member: ResearchMember, interval: str, *, after: pd.Timestamp | None,
                  until: pd.Timestamp) -> list[tuple[pd.Timestamp, str]]:
    """(decision time, session) for each completed trigger bar received after `after` and by `until`: a rule decides
    when its bar arrives, as the research backtest schedules it."""
    rows = data.bars_for(member.security_id, interval) if hasattr(data, "bars_for") else [
        row for row in data.bars if row["security_id"] == member.security_id and row["interval"] == interval]
    times = sorted({(row["received_at"], row["session"]) for row in rows
                    if row["received_at"] <= until and (after is None or row["received_at"] > after)})
    return times


def evaluate_rule(data: ResearchData, member: ResearchMember, model_id: str, at: pd.Timestamp, session: str,
                  previous_state: Mapping | None) -> dict:
    policy = CATALOG[model_id]
    return evaluate_research(Snapshot(data, member.security_id, at, session, policy.interval), model_id,
                             previous_state, macro_policy=RESEARCH_MACRO_POLICY,
                             earnings_policy=RESEARCH_EARNINGS_POLICY)
