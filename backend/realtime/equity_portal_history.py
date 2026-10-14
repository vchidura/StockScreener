from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Mapping, Sequence
from uuid import UUID

import exchange_calendars
import pandas as pd

from equity.domain import BarSourceKind, SecurityReferenceRevision

from .contracts import EQUITY_VALIDATION_UNIVERSE, canonical_sha256
from .equity_bar_adapter import RealtimeEquityBarFact
from .equity_interval_derivation import derive_realtime_interval_facts
from .equity_validation_history import SplitAction, SplitCoverage


@dataclass(frozen=True, slots=True)
class RealtimeEquityPortalHistoryPolicy:
    version: str = "realtime-equity-portal-history-v3"
    adjusted_daily_sessions: int = 283
    native_adjusted_weekly_periods: int = 261
    raw_intraday_sessions: int = 90
    native_one_minute_sessions: int = 5
    native_adjusted_monthly_periods: int = 60
    native_raw_interval: str = "5m"
    derived_raw_intervals: tuple[str, ...] = ("15m", "30m", "1h", "1d")
    derived_adjusted_intervals: tuple[str, ...] = ("1wk",)
    maximum_rows_per_provider_response: int = 50_000
    maximum_persist_batch: int = 1_000
    required_split_tickers: tuple[str, ...] = ("NFLX",)

    def __post_init__(self) -> None:
        minimum_adjusted = {
            "realtime-equity-portal-history-v2": 283,
            "realtime-equity-portal-history-v3": 283,
        }.get(self.version)
        if minimum_adjusted is None:
            raise ValueError("realtime portal history version is unsupported")
        if self.adjusted_daily_sessions < minimum_adjusted or self.raw_intraday_sessions < 90:
            raise ValueError("realtime portal history cannot weaken accepted depth")
        if self.native_one_minute_sessions < 5:
            raise ValueError("realtime portal history requires five native 1m sessions")
        if self.native_adjusted_monthly_periods < 60:
            raise ValueError("realtime portal history requires 60 native adjusted months")
        if self.version == "realtime-equity-portal-history-v3" and self.native_adjusted_weekly_periods < 261:
            raise ValueError("realtime portal history v3 requires 261 native adjusted weeks")
        if self.native_raw_interval != "5m":
            raise ValueError("realtime portal history requires one native 5m source")
        if self.derived_raw_intervals != ("15m", "30m", "1h", "1d"):
            raise ValueError("realtime raw derivation contract changed")
        if self.derived_adjusted_intervals != ("1wk",):
            raise ValueError("realtime adjusted derivation contract changed")
        if self.maximum_rows_per_provider_response < 50_000:
            raise ValueError("realtime provider row bound cannot weaken pagination capacity")
        if not 1 <= self.maximum_persist_batch <= 1_000:
            raise ValueError("realtime persist batch must remain within repository bounds")
        required = tuple(sorted(ticker.strip().upper() for ticker in self.required_split_tickers))
        object.__setattr__(self, "required_split_tickers", required)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


REALTIME_EQUITY_PORTAL_HISTORY_POLICY = RealtimeEquityPortalHistoryPolicy()


@dataclass(frozen=True, slots=True)
class RealtimeEquityPortalHistoryPlan:
    session_date: date
    adjusted_sessions: tuple[date, ...]
    raw_sessions: tuple[date, ...]
    one_minute_sessions: tuple[date, ...]
    monthly_sessions: tuple[date, ...]
    weekly_sessions: tuple[date, ...]
    raw_start: date
    raw_end: date
    one_minute_start: date
    one_minute_end: date
    monthly_start: date
    monthly_end: date
    weekly_start: date
    weekly_end: date
    expected_counts_per_ticker: tuple[tuple[str, str, int], ...]
    expected_provider_requests: int
    expected_facts_per_ticker: int
    expected_total_facts: int
    maximum_rows_per_provider_response: int
    maximum_persist_batch: int
    policy_sha256: str

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)

    def expected_count(self, interval: str, basis: str) -> int:
        for candidate_interval, candidate_basis, count in self.expected_counts_per_ticker:
            if (candidate_interval, candidate_basis) == (interval, basis):
                return count
        raise KeyError((interval, basis))


@dataclass(frozen=True, slots=True)
class RealtimeEquityPortalHistoryPackage:
    plan_sha256: str
    policy_sha256: str
    ticker: str
    facts: tuple[RealtimeEquityBarFact, ...]
    counts: tuple[tuple[str, str, int], ...]
    evidence_sha256: str


def _intraday_count(calendar, sessions, minutes: int) -> int:
    count = 0
    for session in sessions:
        duration = int(
            (calendar.session_close(session) - calendar.session_open(session)).total_seconds()
            // 60
        )
        if duration <= 0 or duration % minutes:
            raise ValueError(f"XNYS session duration is incompatible with {minutes}m")
        count += duration // minutes
    return count


def _complete_period_count(calendar, sessions, interval: str) -> int:
    dates = tuple(session.date() for session in sessions)
    grouped: dict[tuple[int, ...], list[date]] = defaultdict(list)
    for session_date in dates:
        if interval == "1wk":
            iso = session_date.isocalendar()
            key = (iso.year, iso.week)
        else:
            key = (session_date.year, session_date.month)
        grouped[key].append(session_date)
    complete = 0
    for key, present in grouped.items():
        if interval == "1wk":
            start = date.fromisocalendar(key[0], key[1], 1)
            end = date.fromisocalendar(key[0], key[1], 5)
        else:
            start = date(key[0], key[1], 1)
            next_month = date(key[0] + 1, 1, 1) if key[1] == 12 else date(key[0], key[1] + 1, 1)
            end = next_month - timedelta(days=1)
        expected = tuple(
            session.date()
            for session in calendar.sessions_in_range(pd.Timestamp(start), pd.Timestamp(end))
        )
        if tuple(present) == expected:
            complete += 1
    return complete


def _complete_month_ends(calendar, sessions) -> tuple[date, ...]:
    dates = tuple(session.date() for session in sessions)
    grouped: dict[tuple[int, int], list[date]] = defaultdict(list)
    for session_date in dates:
        grouped[(session_date.year, session_date.month)].append(session_date)
    complete = []
    for (year, month), present in grouped.items():
        start = date(year, month, 1)
        next_month = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
        expected = tuple(
            session.date()
            for session in calendar.sessions_in_range(
                pd.Timestamp(start), pd.Timestamp(next_month - timedelta(days=1))
            )
        )
        if tuple(present) == expected:
            complete.append(present[-1])
    return tuple(complete)


def _complete_week_ends(calendar, sessions) -> tuple[date, ...]:
    dates = tuple(session.date() for session in sessions)
    grouped: dict[tuple[int, int], list[date]] = defaultdict(list)
    for session_date in dates:
        iso = session_date.isocalendar()
        grouped[(iso.year, iso.week)].append(session_date)
    complete = []
    for (year, week), present in grouped.items():
        start = date.fromisocalendar(year, week, 1)
        end = date.fromisocalendar(year, week, 5)
        expected = tuple(
            session.date()
            for session in calendar.sessions_in_range(pd.Timestamp(start), pd.Timestamp(end))
        )
        if tuple(present) == expected:
            complete.append(present[-1])
    return tuple(complete)


def build_realtime_equity_portal_history_plan(
    session_date: date,
    *,
    policy: RealtimeEquityPortalHistoryPolicy = REALTIME_EQUITY_PORTAL_HISTORY_POLICY,
    equity_ticker_count: int = len(EQUITY_VALIDATION_UNIVERSE.equity_tickers),
) -> RealtimeEquityPortalHistoryPlan:
    if type(equity_ticker_count) is not int or equity_ticker_count <= 0:
        raise ValueError("realtime portal equity ticker count must be positive")
    calendar = exchange_calendars.get_calendar("XNYS")
    requested = pd.Timestamp(session_date)
    if not calendar.is_session(requested):
        raise ValueError("realtime portal session_date must be an XNYS session")
    completed = calendar.sessions[calendar.sessions <= requested]
    adjusted = completed[-policy.adjusted_daily_sessions:]
    raw = completed[-policy.raw_intraday_sessions:]
    one_minute = completed[-policy.native_one_minute_sessions:]
    monthly = _complete_month_ends(calendar, completed[-2000:])[-policy.native_adjusted_monthly_periods:]
    weekly = _complete_week_ends(calendar, completed[-2000:])[-policy.native_adjusted_weekly_periods:]
    if len(adjusted) != policy.adjusted_daily_sessions or len(raw) != policy.raw_intraday_sessions:
        raise ValueError("realtime portal calendar history is incomplete")
    if len(one_minute) != policy.native_one_minute_sessions or len(monthly) != policy.native_adjusted_monthly_periods:
        raise ValueError("realtime portal native extension history is incomplete")
    if policy.version == "realtime-equity-portal-history-v3" and len(weekly) != policy.native_adjusted_weekly_periods:
        raise ValueError("realtime portal native weekly history is incomplete")
    counts = (
        ("1d", "ADJUSTED", len(adjusted)),
        ("1wk", "ADJUSTED", len(weekly) if policy.version == "realtime-equity-portal-history-v3"
         else _complete_period_count(calendar, adjusted, "1wk")),
        ("1mo", "ADJUSTED", len(monthly)),
        ("1m", "RAW_UNADJUSTED", _intraday_count(calendar, one_minute, 1)),
        ("5m", "RAW_UNADJUSTED", _intraday_count(calendar, raw, 5)),
        ("15m", "RAW_UNADJUSTED", _intraday_count(calendar, raw, 15)),
        ("30m", "RAW_UNADJUSTED", _intraday_count(calendar, raw, 30)),
        ("1h", "RAW_UNADJUSTED", sum(
            (
                int((calendar.session_close(session) - calendar.session_open(session)).total_seconds() // 60)
                + 59
            ) // 60
            for session in raw
        )),
        ("1d", "RAW_UNADJUSTED", len(raw)),
    )
    expected_facts = sum(count for _, _, count in counts)
    tickers = equity_ticker_count
    native_rows = next(count for interval, basis, count in counts if interval == "5m" and basis == "RAW_UNADJUSTED")
    if native_rows > policy.maximum_rows_per_provider_response:
        raise ValueError("native 5m history exceeds one-request row capacity")
    return RealtimeEquityPortalHistoryPlan(
        session_date=session_date,
        adjusted_sessions=tuple(value.date() for value in adjusted),
        raw_sessions=tuple(value.date() for value in raw),
        one_minute_sessions=tuple(value.date() for value in one_minute),
        monthly_sessions=monthly,
        weekly_sessions=weekly,
        raw_start=raw[0].date(),
        raw_end=raw[-1].date(),
        one_minute_start=one_minute[0].date(),
        one_minute_end=one_minute[-1].date(),
        monthly_start=monthly[0],
        monthly_end=monthly[-1],
        weekly_start=weekly[0],
        weekly_end=weekly[-1],
        expected_counts_per_ticker=counts,
        expected_provider_requests=(
            len(adjusted) + tickers * 3
            if policy.version == "realtime-equity-portal-history-v2"
            else tickers * 5
        ),
        expected_facts_per_ticker=expected_facts,
        expected_total_facts=expected_facts * tickers,
        maximum_rows_per_provider_response=policy.maximum_rows_per_provider_response,
        maximum_persist_batch=policy.maximum_persist_batch,
        policy_sha256=policy.sha256,
    )


def build_realtime_equity_portal_history_package(
    security: SecurityReferenceRevision,
    source_facts: Sequence[RealtimeEquityBarFact],
    *,
    plan: RealtimeEquityPortalHistoryPlan,
    observed_at: datetime,
    ingestion_segment_id: UUID,
    split_coverage: SplitCoverage,
    split_actions: Sequence[SplitAction],
    policy: RealtimeEquityPortalHistoryPolicy = REALTIME_EQUITY_PORTAL_HISTORY_POLICY,
) -> RealtimeEquityPortalHistoryPackage:
    adjusted = tuple(sorted((
        fact for fact in source_facts
        if fact.revision.ticker == security.ticker
        and fact.revision.interval == "1d"
        and fact.revision.adjusted
        and fact.revision.session_date in set(plan.adjusted_sessions)
    ), key=lambda fact: fact.revision.bar_start))
    five = tuple(sorted((
        fact for fact in source_facts
        if fact.revision.ticker == security.ticker
        and fact.revision.interval == "5m"
        and not fact.revision.adjusted
        and fact.revision.session_date in set(plan.raw_sessions)
    ), key=lambda fact: fact.revision.bar_start))
    one = tuple(sorted((
        fact for fact in source_facts
        if fact.revision.ticker == security.ticker
        and fact.revision.interval == "1m"
        and not fact.revision.adjusted
        and fact.revision.session_date in set(plan.one_minute_sessions)
    ), key=lambda fact: fact.revision.bar_start))
    monthly = tuple(sorted((
        fact for fact in source_facts
        if fact.revision.ticker == security.ticker
        and fact.revision.interval == "1mo"
        and fact.revision.adjusted
        and fact.revision.session_date in set(plan.monthly_sessions)
    ), key=lambda fact: fact.revision.bar_start))
    weekly = tuple(sorted((
        fact for fact in source_facts
        if fact.revision.ticker == security.ticker
        and fact.revision.interval == "1wk"
        and fact.revision.adjusted
        and fact.revision.session_date in set(plan.weekly_sessions)
    ), key=lambda fact: fact.revision.bar_start))
    if len(adjusted) != plan.expected_count("1d", "ADJUSTED"):
        raise ValueError(f"adjusted daily history is incomplete for {security.ticker}")
    if len(five) != plan.expected_count("5m", "RAW_UNADJUSTED"):
        raise ValueError(f"native 5m history is incomplete for {security.ticker}")
    if len(one) != plan.expected_count("1m", "RAW_UNADJUSTED"):
        raise ValueError(f"native 1m history is incomplete for {security.ticker}")
    if len(monthly) != plan.expected_count("1mo", "ADJUSTED"):
        raise ValueError(f"native adjusted monthly history is incomplete for {security.ticker}")
    if policy.version == "realtime-equity-portal-history-v3" and len(weekly) != plan.expected_count("1wk", "ADJUSTED"):
        raise ValueError(f"native adjusted weekly history is incomplete for {security.ticker}")
    if tuple(fact.revision.session_date for fact in adjusted) != plan.adjusted_sessions:
        raise ValueError(f"adjusted daily sessions changed for {security.ticker}")
    raw_sessions = tuple(sorted({fact.revision.session_date for fact in five}))
    if raw_sessions != plan.raw_sessions:
        raise ValueError(f"native 5m sessions changed for {security.ticker}")
    if tuple(sorted({fact.revision.session_date for fact in one})) != plan.one_minute_sessions:
        raise ValueError(f"native 1m sessions changed for {security.ticker}")
    if tuple(fact.revision.session_date for fact in monthly) != plan.monthly_sessions:
        raise ValueError(f"native adjusted monthly periods changed for {security.ticker}")
    if policy.version == "realtime-equity-portal-history-v3" and tuple(
        fact.revision.session_date for fact in weekly
    ) != plan.weekly_sessions:
        raise ValueError(f"native adjusted weekly periods changed for {security.ticker}")
    native_extensions = (*one, *monthly, *(weekly if policy.version == "realtime-equity-portal-history-v3" else ()))
    if any(fact.revision.source_kind is not BarSourceKind.NATIVE_REST for fact in native_extensions):
        raise ValueError(f"ticker native extension source changed for {security.ticker}")
    if (
        split_coverage.ticker != security.ticker
        or split_coverage.window_start > plan.raw_start
        or split_coverage.window_end < plan.raw_end
    ):
        raise ValueError(f"native 5m split coverage is incomplete for {security.ticker}")
    ticker_actions = tuple(
        action for action in split_actions if action.ticker == security.ticker
    )
    if any(plan.raw_start <= action.effective_date <= plan.raw_end for action in ticker_actions):
        raise ValueError(f"native 5m split window requires review for {security.ticker}")
    if security.ticker in policy.required_split_tickers and not any(
        plan.adjusted_sessions[0] <= action.effective_date <= plan.adjusted_sessions[-1]
        for action in ticker_actions
    ):
        raise ValueError(f"required split scenario is absent for {security.ticker}")
    split_dates = {action.effective_date for action in ticker_actions}
    for previous, current in zip(adjusted, adjusted[1:]):
        return_fraction = float(
            current.revision.close_price / previous.revision.close_price - 1
        )
        if (
            (return_fraction <= -0.38 or return_fraction >= 0.60)
            and current.revision.session_date in split_dates
        ):
            raise ValueError("adjusted history retains split-coincident discontinuities")
    identities = {
        (fact.cohort_id, fact.provider_tier, fact.instrument_id, fact.revision.security_id)
        for fact in (*adjusted, *weekly, *monthly, *one, *five)
    }
    if len(identities) != 1 or next(iter(identities))[3] != security.security_id:
        raise ValueError(f"realtime portal history identity changed for {security.ticker}")
    derived = []
    for interval in ("15m", "30m", "1h", "1d"):
        derived.extend(derive_realtime_interval_facts(
            security,
            five,
            target_interval=interval,
            observed_at=observed_at,
            ingestion_segment_id=ingestion_segment_id,
        ))
    for interval in (() if policy.version == "realtime-equity-portal-history-v3" else ("1wk",)):
        derived.extend(derive_realtime_interval_facts(
            security,
            adjusted,
            target_interval=interval,
            observed_at=observed_at,
            ingestion_segment_id=ingestion_segment_id,
        ))
    facts = tuple(sorted(
        (*adjusted, *weekly, *monthly, *one, *five, *derived),
        key=lambda fact: (
            fact.revision.interval,
            fact.revision.adjusted,
            fact.revision.bar_start,
            fact.revision.bar_revision_id,
        ),
    ))
    counts = tuple(
        (interval, basis, sum(
            fact.revision.interval == interval
            and ("ADJUSTED" if fact.revision.adjusted else "RAW_UNADJUSTED") == basis
            for fact in facts
        ))
        for interval, basis, _ in plan.expected_counts_per_ticker
    )
    if counts != plan.expected_counts_per_ticker or len(facts) != plan.expected_facts_per_ticker:
        raise ValueError(f"realtime portal derived history is incomplete for {security.ticker}")
    evidence_sha256 = canonical_sha256({
        "plan_sha256": plan.sha256,
        "ticker": security.ticker,
        "facts": tuple(
            (fact.revision.interval, fact.revision.adjusted, fact.revision.bar_revision_id, fact.revision.payload_sha256)
            for fact in facts
        ),
    })
    return RealtimeEquityPortalHistoryPackage(
        plan_sha256=plan.sha256,
        policy_sha256=policy.sha256,
        ticker=security.ticker,
        facts=facts,
        counts=counts,
        evidence_sha256=evidence_sha256,
    )
