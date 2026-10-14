from dataclasses import replace
from datetime import date, datetime, timezone
from uuid import uuid4

from realtime.equity_bar_adapter import RealtimeEquityBarFact
from realtime.equity_interval_derivation import derive_realtime_interval_facts
from realtime.preflight import ProviderTier
from equity.domain import BarSourceKind
from tests.test_equity_bar_derivation import daily, intraday_5m, security


UTC = timezone.utc
COHORT_ID = uuid4()
INSTRUMENT_ID = 17


def _facts(revisions):
    return tuple(
        RealtimeEquityBarFact(
            cohort_id=COHORT_ID,
            provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            instrument_id=INSTRUMENT_ID,
            revision=replace(
                revision,
                source_kind=BarSourceKind.NATIVE_REST,
                ingestion_segment_id=uuid4(),
                source_bar_revision_ids=(),
            ),
        )
        for revision in revisions
    )


def test_realtime_derives_fifteen_minutes_only_from_raw_five_minutes():
    item = security()
    source = _facts(intraday_5m(item))

    results = {
        interval: derive_realtime_interval_facts(
            item,
            source,
            target_interval=interval,
            observed_at=source[-1].revision.bar_end,
            ingestion_segment_id=uuid4(),
        )
        for interval in ("15m", "30m", "1h", "1d")
    }
    result = results["15m"]

    assert len(result) == 26
    assert {interval: len(rows) for interval, rows in results.items()} == {
        "15m": 26,
        "30m": 13,
        "1h": 7,
        "1d": 1,
    }
    assert {
        (row.cohort_id, row.provider_tier, row.instrument_id)
        for rows in results.values()
        for row in rows
    } == {(COHORT_ID, ProviderTier.MASSIVE_DEVELOPER, INSTRUMENT_ID)}
    assert all(row.revision.interval == "15m" for row in result)
    assert all(
        row.revision.adjusted is False
        for rows in results.values()
        for row in rows
    )
    assert all(len(row.revision.source_bar_revision_ids) == 3 for row in result)


def test_realtime_derives_weekly_and_monthly_on_adjusted_daily_basis():
    item = security()
    adjusted_daily = tuple(replace(revision, adjusted=True) for revision in daily(item))
    source = _facts(adjusted_daily)
    observed_at = datetime(2026, 8, 31, 21, 0, tzinfo=UTC)

    weekly = derive_realtime_interval_facts(
        item,
        source,
        target_interval="1wk",
        observed_at=observed_at,
        ingestion_segment_id=uuid4(),
    )
    monthly = derive_realtime_interval_facts(
        item,
        source,
        target_interval="1mo",
        observed_at=observed_at,
        ingestion_segment_id=uuid4(),
    )

    assert weekly
    assert len(monthly) == 1
    assert all(row.revision.adjusted is True for row in (*weekly, *monthly))
    assert all(row.revision.source_bar_revision_ids for row in (*weekly, *monthly))
    assert monthly[0].revision.session_date == date(2026, 8, 31)


def test_realtime_derivation_fails_closed_for_missing_basis_or_mixed_identity():
    item = security()
    raw_daily = _facts(daily(item))
    assert derive_realtime_interval_facts(
        item,
        raw_daily,
        target_interval="1wk",
        observed_at=datetime(2026, 8, 31, 21, 0, tzinfo=UTC),
        ingestion_segment_id=uuid4(),
    ) == ()

    five = list(_facts(intraday_5m(item)))
    five[-1] = replace(five[-1], instrument_id=INSTRUMENT_ID + 1)
    try:
        derive_realtime_interval_facts(
            item,
            five,
            target_interval="15m",
            observed_at=five[-1].revision.bar_end,
            ingestion_segment_id=uuid4(),
        )
    except ValueError as error:
        assert "cannot mix" in str(error)
    else:
        raise AssertionError("mixed realtime identity was accepted")
