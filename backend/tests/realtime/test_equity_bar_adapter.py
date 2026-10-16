from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from equity.domain import (
    BarAvailabilityMode,
    BarSessionScope,
    BarSourceKind,
    EquityBarRevision,
)
from realtime.contracts import AssetClass, MarketChannel
from realtime.equity_bar_adapter import (
    AdvancedMinuteFinality,
    adapt_advanced_minute_aggregate,
    adapt_developer_equity_bar,
    adapt_derived_equity_bar,
    adapt_reconciled_equity_bar,
)
from realtime.journal import NormalizedJournalRecord
from realtime.preflight import ProviderTier


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 14, 31, tzinfo=UTC)
COHORT = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
SECURITY = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
GENERATION = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
SESSION = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")


def _developer_revision():
    start = NOW - timedelta(minutes=1)
    return EquityBarRevision(
        bar_revision_id=UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"),
        security_id=SECURITY,
        ticker="SPY",
        interval="1m",
        session_date=date(2026, 10, 5),
        bar_start=start,
        bar_end=NOW,
        open_price=Decimal("100"),
        high_price=Decimal("102"),
        low_price=Decimal("99"),
        close_price=Decimal("101"),
        volume=Decimal("1000"),
        vwap=Decimal("100.5"),
        transaction_count=100,
        source_kind=BarSourceKind.NATIVE_REST,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
        is_final=True,
        system_observed_at=NOW,
        replay_available_at=None,
        adjusted=False,
        payload_sha256="a" * 64,
        ingestion_segment_id=UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
        session_scope=BarSessionScope.RTH,
    )


def _advanced_record():
    start = NOW - timedelta(minutes=1)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(NOW.timestamp() * 1000)
    return NormalizedJournalRecord(
        event_offset=10,
        raw_offset=5,
        event_index=0,
        normalizer_version="massive_market_event_v1",
        journal_generation=GENERATION,
        stream_session_id=SESSION,
        instrument_id=1,
        provider_symbol="SPY",
        underlying="SPY",
        asset_class=AssetClass.EQUITY,
        shard_id=0,
        event_kind=MarketChannel.MINUTE_AGGREGATE,
        source_session=date(2026, 10, 5),
        source_sequence=10,
        source_timestamp=end_ms,
        source_timestamp_unit="ms",
        event_at=NOW,
        price=Decimal("101"),
        size=Decimal("1000"),
        bid=None,
        ask=None,
        bid_size=None,
        ask_size=None,
        exchange=None,
        conditions=(),
        quality="GOOD",
        reason_codes=(),
        details_json=(
            '{"close":"101","end_timestamp":' + str(end_ms)
            + ',"high":"102","low":"99","open":"100","start_timestamp":'
            + str(start_ms) + ',"vwap":"100.5"}'
        ),
        payload_sha256="b" * 64,
        received_at=NOW + timedelta(milliseconds=1),
        committed_at=NOW + timedelta(milliseconds=2),
    )


def _advanced_finality(**changes):
    values = dict(
        journal_generation=GENERATION,
        stream_session_id=SESSION,
        owner_epoch=3,
        normalized_through_offset=10,
        event_time_watermark=NOW,
        proved_at=NOW + timedelta(milliseconds=2),
        gap_free=True,
    )
    values.update(changes)
    return AdvancedMinuteFinality(**values)


def test_developer_rest_bar_retains_delayed_source_identity():
    fact = adapt_developer_equity_bar(
        _developer_revision(),
        cohort_id=COHORT,
        instrument_id=1,
    )

    assert fact.provider_tier is ProviderTier.MASSIVE_DEVELOPER
    assert fact.revision.source_kind is BarSourceKind.NATIVE_REST
    assert fact.revision.ingestion_segment_id is not None
    assert fact.journal_generation is None


def test_advanced_am_bar_retains_exact_journal_and_finality_lineage():
    fact = adapt_advanced_minute_aggregate(
        _advanced_record(),
        cohort_id=COHORT,
        security_id=SECURITY,
        finality=_advanced_finality(),
    )

    assert fact.provider_tier is ProviderTier.MASSIVE_ADVANCED
    assert fact.revision.source_kind is BarSourceKind.REALTIME_STREAM
    assert fact.revision.open_price == Decimal("100")
    assert fact.revision.close_price == Decimal("101")
    assert fact.revision.system_observed_at == NOW + timedelta(milliseconds=2)
    assert fact.journal_generation == GENERATION
    assert fact.stream_session_id == SESSION
    assert fact.source_offset == 10


def test_bar_sources_cannot_be_relabelled_across_tiers():
    with pytest.raises(ValueError, match="Developer bar must use native REST"):
        fact = adapt_advanced_minute_aggregate(
            _advanced_record(),
            cohort_id=COHORT,
            security_id=SECURITY,
            finality=_advanced_finality(),
        )
        type(fact)(
            cohort_id=fact.cohort_id,
            provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            instrument_id=fact.instrument_id,
            revision=fact.revision,
        )


@pytest.mark.parametrize(
    "finality",
    [
        _advanced_finality(event_time_watermark=NOW - timedelta(microseconds=1)),
        _advanced_finality(normalized_through_offset=9),
        _advanced_finality(gap_free=False),
        _advanced_finality(journal_generation=UUID(int=1)),
        _advanced_finality(stream_session_id=UUID(int=2)),
    ],
)
def test_advanced_am_stays_provisional_until_watermark_offset_session_and_gap_prove_finality(finality):
    with pytest.raises(ValueError, match="finality"):
        adapt_advanced_minute_aggregate(
            _advanced_record(),
            cohort_id=COHORT,
            security_id=SECURITY,
            finality=finality,
        )


def test_derived_bar_retains_source_revision_lineage_in_either_tier():
    source = _developer_revision()
    derived = replace(
        source,
        bar_revision_id=UUID("11111111-1111-1111-1111-111111111111"),
        interval="1h",
        source_kind=BarSourceKind.DERIVED,
        source_bar_revision_ids=(source.bar_revision_id,),
    )

    fact = adapt_derived_equity_bar(
        derived,
        cohort_id=COHORT,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        instrument_id=1,
    )

    assert fact.revision.source_bar_revision_ids == (source.bar_revision_id,)
    assert fact.journal_generation is None

    reconciled = replace(
        derived,
        bar_revision_id=UUID("22222222-2222-2222-2222-222222222222"),
        source_kind=BarSourceKind.RECONCILED,
        supersedes_bar_revision_id=source.bar_revision_id,
    )
    reconciled_fact = adapt_reconciled_equity_bar(
        reconciled,
        cohort_id=COHORT,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        instrument_id=1,
    )
    assert reconciled_fact.revision.supersedes_bar_revision_id == source.bar_revision_id