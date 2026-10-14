from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from functools import lru_cache
from uuid import NAMESPACE_URL, uuid5

import exchange_calendars
import pandas as pd
import pytest

from equity.domain import (
    BarAvailabilityMode,
    BarSessionScope,
    BarSourceKind,
    EquityBarRevision,
    SecurityReferenceRevision,
)
from equity.derivation import derive_canonical_bars
from realtime.contracts import EQUITY_VALIDATION_UNIVERSE, canonical_sha256
from realtime.equity_bar_adapter import adapt_developer_equity_bar
from realtime.equity_validation_history import (
    EQUITY_VALIDATION_HISTORY_POLICY,
    SplitAction,
    SplitCoverage,
    build_equity_validation_history_package,
)


UTC = timezone.utc
COHORT_ID = uuid5(NAMESPACE_URL, "equity-validation-history-cohort")
OBSERVED_AT = datetime(2026, 10, 4, 20, 0, tzinfo=UTC)


def _bar(ticker, instrument_id, interval, start, end, *, adjusted, index):
    security_id = uuid5(NAMESPACE_URL, f"equity-validation-security:{ticker}")
    payload_sha256 = canonical_sha256((ticker, interval, start, adjusted, index))
    revision = EquityBarRevision(
        bar_revision_id=uuid5(NAMESPACE_URL, f"equity-validation-bar:{payload_sha256}"),
        security_id=security_id,
        ticker=ticker,
        interval=interval,
        session_date=start.date(),
        bar_start=start,
        bar_end=end,
        open_price=Decimal("100"),
        high_price=Decimal("102"),
        low_price=Decimal("99"),
        close_price=Decimal("101"),
        volume=Decimal("1000"),
        vwap=Decimal("100.5"),
        transaction_count=10,
        source_kind=BarSourceKind.NATIVE_REST,
        availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
        is_final=True,
        system_observed_at=OBSERVED_AT,
        replay_available_at=OBSERVED_AT,
        adjusted=adjusted,
        payload_sha256=payload_sha256,
        ingestion_segment_id=uuid5(NAMESPACE_URL, f"equity-validation-segment:{ticker}"),
        session_scope=BarSessionScope.RTH,
    )
    return adapt_developer_equity_bar(
        revision,
        cohort_id=COHORT_ID,
        instrument_id=instrument_id,
    )


def _security(ticker):
    security_id = uuid5(NAMESPACE_URL, f"equity-validation-security:{ticker}")
    return SecurityReferenceRevision(
        security_revision_id=uuid5(NAMESPACE_URL, f"equity-validation-revision:{ticker}"),
        security_id=security_id,
        ticker=ticker,
        active=True,
        company_name=ticker,
        security_type="CS",
        cik=None,
        composite_figi=None,
        share_class_figi=None,
        primary_exchange="XNAS",
        sic_code=None,
        sic_description=None,
        sector=None,
        industry=None,
        list_date=None,
        delisted_date=None,
        weighted_shares=None,
        free_float=None,
        free_float_percent=None,
        market_cap=None,
        source="EQUITY_VALIDATION_TEST",
        effective_from=OBSERVED_AT - timedelta(days=500),
        observed_at=OBSERVED_AT,
        payload_sha256=canonical_sha256(ticker),
        raw_payload_json="{}",
    )


@lru_cache(maxsize=1)
def _facts():
    policy = EQUITY_VALIDATION_HISTORY_POLICY
    calendar = exchange_calendars.get_calendar("XNYS")
    completed = calendar.sessions[calendar.sessions <= pd.Timestamp("2026-10-02")]
    sessions = completed[-policy.adjusted_daily_sessions:]
    raw_sessions = completed[-policy.raw_intraday_sessions:]
    rows = []
    for instrument_id, ticker in enumerate(EQUITY_VALIDATION_UNIVERSE.equity_tickers, 1):
        security = _security(ticker)
        for index, session in enumerate(sessions):
            start = calendar.session_open(session).to_pydatetime().astimezone(UTC)
            end = calendar.session_close(session).to_pydatetime().astimezone(UTC)
            rows.append(_bar(ticker, instrument_id, "1d", start, end, adjusted=True, index=index))
        thirty_revisions = []
        index = 0
        for session in raw_sessions:
            cursor = calendar.session_open(session).to_pydatetime().astimezone(UTC)
            close = calendar.session_close(session).to_pydatetime().astimezone(UTC)
            while cursor < close:
                end = min(cursor + timedelta(minutes=30), close)
                fact = _bar(
                    ticker,
                    instrument_id,
                    "30m",
                    cursor,
                    end,
                    adjusted=False,
                    index=index,
                )
                rows.append(fact)
                thirty_revisions.append(fact.revision)
                cursor = end
                index += 1
        segment_id = uuid5(NAMESPACE_URL, f"equity-validation-segment:{ticker}")
        for revision in derive_canonical_bars(
            security,
            thirty_revisions,
            target_interval="1h",
            observed_at=OBSERVED_AT,
            ingestion_segment_id=segment_id,
        ) + derive_canonical_bars(
            security,
            thirty_revisions,
            target_interval="1d",
            observed_at=OBSERVED_AT,
            ingestion_segment_id=segment_id,
        ):
            rows.append(adapt_developer_equity_bar(
                revision,
                cohort_id=COHORT_ID,
                instrument_id=instrument_id,
            ))
    return tuple(rows), sessions[0].date(), sessions[-1].date()


def test_equity_validation_history_is_exact_and_split_safe():
    facts, daily_start, daily_end = _facts()
    coverage = {
        ticker: SplitCoverage(
            ticker,
            daily_start - timedelta(days=60),
            daily_end + timedelta(days=60),
            OBSERVED_AT,
        )
        for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers
    }
    nflx_split = SplitAction("NFLX", date(2025, 11, 17), "1", "10")

    package = build_equity_validation_history_package(
        facts,
        split_coverage=coverage,
        split_actions=(nflx_split,),
    )

    calendar = exchange_calendars.get_calendar("XNYS")
    completed = calendar.sessions[calendar.sessions <= pd.Timestamp("2026-10-02")]
    raw_sessions = completed[-EQUITY_VALIDATION_HISTORY_POLICY.raw_intraday_sessions:]
    expected_thirty = sum(
        int((calendar.session_close(row) - calendar.session_open(row)).total_seconds() // 1800)
        for row in raw_sessions
    )
    expected_hourly = sum(
        (
            int((calendar.session_close(row) - calendar.session_open(row)).total_seconds() // 60)
            + 59
        ) // 60
        for row in raw_sessions
    )
    assert len(package.facts) == 9 * (283 + 90 + expected_hourly + expected_thirty)
    assert package.counts_by_ticker == tuple(
        (ticker, 283, 90, expected_hourly, expected_thirty)
        for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers
    )
    assert package.split_actions == (nflx_split,)


def test_equity_validation_history_rejects_missing_required_split():
    facts, daily_start, daily_end = _facts()
    coverage = {
        ticker: SplitCoverage(ticker, daily_start, daily_end, OBSERVED_AT)
        for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers
    }

    with pytest.raises(ValueError, match="required split scenario is absent for NFLX"):
        build_equity_validation_history_package(
            facts,
            split_coverage=coverage,
            split_actions=(),
        )


def test_equity_validation_history_preserves_half_session_windows():
    calendar = exchange_calendars.get_calendar("XNYS")
    session = date(2025, 11, 28)
    session_key = pd.Timestamp(session)
    cursor = calendar.session_open(session_key).to_pydatetime().astimezone(UTC)
    expected_close = calendar.session_close(session_key).to_pydatetime().astimezone(UTC)
    assert int((expected_close - cursor).total_seconds() // 60) == 210
    thirty = []
    while cursor < expected_close:
        end = min(cursor + timedelta(minutes=30), expected_close)
        thirty.append(
            _bar(
                "AAPL",
                1,
                "30m",
                cursor,
                end,
                adjusted=False,
                index=len(thirty),
            ).revision
        )
        cursor = end
    segment_id = uuid5(NAMESPACE_URL, "equity-validation-half-session")
    hourly = derive_canonical_bars(
        _security("AAPL"),
        thirty,
        target_interval="1h",
        observed_at=OBSERVED_AT,
        ingestion_segment_id=segment_id,
    )
    daily = derive_canonical_bars(
        _security("AAPL"),
        thirty,
        target_interval="1d",
        observed_at=OBSERVED_AT,
        ingestion_segment_id=segment_id,
    )

    assert len(thirty) == 7
    assert len(hourly) == 4
    assert len(daily) == 1
    assert thirty[-1].bar_end == expected_close
    assert hourly[-1].bar_end == expected_close
    assert daily[-1].bar_end == expected_close
    assert len(hourly[-1].source_bar_revision_ids) == 1


def test_equity_validation_rejects_split_gap_on_adjusted_lineage():
    facts, daily_start, daily_end = _facts()
    split_date = date(2025, 11, 17)
    changed = []
    for fact in facts:
        revision = fact.revision
        if (
            revision.ticker == "NFLX"
            and revision.interval == "1d"
            and revision.adjusted
            and revision.session_date == split_date
        ):
            revision = replace(
                revision,
                open_price=Decimal("10"),
                high_price=Decimal("11"),
                low_price=Decimal("9"),
                close_price=Decimal("10"),
                payload_sha256=canonical_sha256("nflx-split-gap"),
            )
            fact = replace(fact, revision=revision)
        changed.append(fact)
    coverage = {
        ticker: SplitCoverage(ticker, daily_start, daily_end, OBSERVED_AT)
        for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers
    }

    with pytest.raises(ValueError, match="split-coincident discontinuities"):
        build_equity_validation_history_package(
            changed,
            split_coverage=coverage,
            split_actions=(SplitAction("NFLX", split_date, "1", "10"),),
        )