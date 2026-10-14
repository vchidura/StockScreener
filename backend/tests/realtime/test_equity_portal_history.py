from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest
import exchange_calendars

from equity.domain import BarSourceKind
from realtime.contracts import EQUITY_VALIDATION_UNIVERSE
from realtime.equity_bar_adapter import RealtimeEquityBarFact
from realtime.equity_portal_history import (
    REALTIME_EQUITY_PORTAL_HISTORY_POLICY,
    RealtimeEquityPortalHistoryPolicy,
    build_realtime_equity_portal_history_package,
    build_realtime_equity_portal_history_plan,
)
from realtime.preflight import ProviderTier
from realtime.equity_validation_history import SplitAction, SplitCoverage
from tests.test_equity_bar_derivation import bar, daily, intraday_5m, security


UTC = timezone.utc
COHORT_ID = uuid4()
INSTRUMENT_ID = 23


def _coverage(plan, ticker="AAPL"):
    return SplitCoverage(
        ticker,
        plan.raw_start,
        plan.raw_end,
        datetime(2026, 10, 2, 23, 0, tzinfo=UTC),
    )


def _native_fact(revision, *, instrument_id=INSTRUMENT_ID):
    return RealtimeEquityBarFact(
        cohort_id=COHORT_ID,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        instrument_id=instrument_id,
        revision=replace(
            revision,
            source_kind=BarSourceKind.NATIVE_REST,
            ingestion_segment_id=uuid4(),
            source_bar_revision_ids=(),
        ),
    )


def _source_facts(plan):
    item = security()
    adjusted = tuple(
        _native_fact(replace(revision, adjusted=True))
        for revision in daily(
            item,
            start=plan.adjusted_sessions[0],
            end=plan.adjusted_sessions[-1],
        )
    )
    five = tuple(
        _native_fact(revision)
        for session_date in plan.raw_sessions
        for revision in intraday_5m(item, session_date)
    )
    calendar = exchange_calendars.get_calendar("XNYS")
    one = []
    for session_date in plan.one_minute_sessions:
        start = calendar.session_open(str(session_date)).to_pydatetime().astimezone(UTC)
        end_of_session = calendar.session_close(str(session_date)).to_pydatetime().astimezone(UTC)
        index = 0
        while start < end_of_session:
            end = min(start + timedelta(minutes=1), end_of_session)
            one.append(_native_fact(bar(item, "1m", start, end, session_date, index)))
            start, index = end, index + 1
    monthly = []
    for index, session_date in enumerate(plan.monthly_sessions):
        month_start = date(session_date.year, session_date.month, 1)
        sessions = calendar.sessions_in_range(str(month_start), str(session_date))
        start = calendar.session_open(sessions[0]).to_pydatetime().astimezone(UTC)
        end = calendar.session_close(sessions[-1]).to_pydatetime().astimezone(UTC)
        monthly.append(_native_fact(replace(
            bar(item, "1mo", start, end, session_date, index), adjusted=True,
        )))
    weekly = []
    for index, session_date in enumerate(plan.weekly_sessions):
        iso = session_date.isocalendar()
        week_start = date.fromisocalendar(iso.year, iso.week, 1)
        sessions = calendar.sessions_in_range(str(week_start), str(session_date))
        start = calendar.session_open(sessions[0]).to_pydatetime().astimezone(UTC)
        end = calendar.session_close(sessions[-1]).to_pydatetime().astimezone(UTC)
        weekly.append(_native_fact(replace(
            bar(item, "1wk", start, end, session_date, index), adjusted=True,
        )))
    return item, (*adjusted, *weekly, *monthly, *one, *five)


def test_portal_history_plan_is_exact_bounded_and_deterministic():
    first = build_realtime_equity_portal_history_plan(plan_date := datetime(2026, 10, 2, tzinfo=UTC).date())
    repeated = build_realtime_equity_portal_history_plan(plan_date)

    assert first == repeated
    assert first.sha256 == repeated.sha256
    assert len(first.adjusted_sessions) == 283
    assert len(first.raw_sessions) == 90
    assert len(first.one_minute_sessions) == 5
    assert len(first.monthly_sessions) == 60
    assert first.expected_provider_requests == 45
    assert first.expected_count("1m", "RAW_UNADJUSTED") == 1950
    assert first.expected_count("5m", "RAW_UNADJUSTED") < first.maximum_rows_per_provider_response
    assert first.expected_count("15m", "RAW_UNADJUSTED") * 3 == first.expected_count("5m", "RAW_UNADJUSTED")
    assert first.expected_count("30m", "RAW_UNADJUSTED") * 6 == first.expected_count("5m", "RAW_UNADJUSTED")
    assert first.expected_count("1d", "RAW_UNADJUSTED") == 90
    assert first.expected_count("1d", "ADJUSTED") == 283
    assert first.expected_count("1wk", "ADJUSTED") >= 250
    assert len(first.weekly_sessions) == 261
    assert first.expected_count("1wk", "ADJUSTED") > 50
    assert first.expected_count("1mo", "ADJUSTED") == 60
    assert first.expected_total_facts == first.expected_facts_per_ticker * len(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
    assert first.expected_total_facts > 100_000
    assert first.maximum_persist_batch == 1_000
    assert first.policy_sha256 == REALTIME_EQUITY_PORTAL_HISTORY_POLICY.sha256


def test_portal_history_package_derives_every_interval_with_lineage():
    plan = build_realtime_equity_portal_history_plan(datetime(2026, 10, 2, tzinfo=UTC).date())
    item, source = _source_facts(plan)
    observed_at = datetime(2026, 10, 2, 23, 0, tzinfo=UTC)

    package = build_realtime_equity_portal_history_package(
        item,
        source,
        plan=plan,
        observed_at=observed_at,
        ingestion_segment_id=uuid4(),
        split_coverage=_coverage(plan),
        split_actions=(),
    )

    assert package.counts == plan.expected_counts_per_ticker
    assert len(package.facts) == plan.expected_facts_per_ticker
    assert package.evidence_sha256
    derived = tuple(fact for fact in package.facts if fact.revision.source_kind is BarSourceKind.DERIVED)
    assert {fact.revision.interval for fact in derived} == {"15m", "30m", "1h", "1d"}
    assert all(fact.revision.source_bar_revision_ids for fact in derived)
    assert all(
        fact.revision.adjusted is (fact.revision.interval == "1wk")
        for fact in derived
    )
    native_extensions = tuple(
        fact for fact in package.facts if fact.revision.interval in {"1m", "1wk", "1mo"}
    )
    assert {fact.revision.interval for fact in native_extensions} == {"1m", "1wk", "1mo"}
    assert all(fact.revision.source_kind is BarSourceKind.NATIVE_REST for fact in native_extensions)


def test_portal_history_package_rejects_missing_native_bar_or_identity_drift():
    plan = build_realtime_equity_portal_history_plan(datetime(2026, 10, 2, tzinfo=UTC).date())
    item, source = _source_facts(plan)
    observed_at = datetime(2026, 10, 2, 23, 0, tzinfo=UTC)
    five_index = next(index for index, fact in enumerate(source) if fact.revision.interval == "5m")

    with pytest.raises(ValueError, match="native 5m history is incomplete"):
        build_realtime_equity_portal_history_package(
            item,
            (*source[:five_index], *source[five_index + 1:]),
            plan=plan,
            observed_at=observed_at,
            ingestion_segment_id=uuid4(),
            split_coverage=_coverage(plan),
            split_actions=(),
        )
    drifted = list(source)
    drifted[five_index] = replace(drifted[five_index], instrument_id=INSTRUMENT_ID + 1)
    with pytest.raises(ValueError, match="identity changed"):
        build_realtime_equity_portal_history_package(
            item,
            drifted,
            plan=plan,
            observed_at=observed_at,
            ingestion_segment_id=uuid4(),
            split_coverage=_coverage(plan),
            split_actions=(),
        )

    with pytest.raises(ValueError, match="split window requires review"):
        build_realtime_equity_portal_history_package(
            item,
            source,
            plan=plan,
            observed_at=observed_at,
            ingestion_segment_id=uuid4(),
            split_coverage=_coverage(plan),
            split_actions=(SplitAction("AAPL", plan.raw_sessions[10], "1", "2"),),
        )


def test_portal_history_policy_cannot_reintroduce_duplicate_native_intervals():
    with pytest.raises(ValueError, match="native 5m"):
        RealtimeEquityPortalHistoryPolicy(native_raw_interval="30m")
    with pytest.raises(ValueError, match="raw derivation contract"):
        RealtimeEquityPortalHistoryPolicy(derived_raw_intervals=("15m", "30m"))
