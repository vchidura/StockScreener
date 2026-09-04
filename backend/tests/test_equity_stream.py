import sys
import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.domain import (
    BarAvailabilityMode,
    BarSourceKind,
    EquityBarRevision,
    SecurityReferenceRevision,
)
from equity.polygon_stream import PolygonAdvancedStreamClient
from equity.stream import (
    EquityStreamProcessor,
    SessionBarAccumulator,
    normalize_stream_minute,
    reconcile_interval_bar,
)


UTC = timezone.utc
SECURITY_ID = uuid4()


class FakeBarRepository:
    def __init__(self):
        self.persisted = []

    def persist(self, bars):
        self.persisted.extend(bars)
        return len(bars)


def minute(
    start: datetime,
    *,
    close: str,
    ticker: str = "AAPL",
    observed_at: datetime | None = None,
) -> EquityBarRevision:
    close_value = Decimal(close)
    payload = f"{start.isoformat()}:{close}"
    return EquityBarRevision(
        bar_revision_id=uuid4(),
        security_id=SECURITY_ID,
        ticker=ticker,
        interval="1m",
        session_date=start.date(),
        bar_start=start,
        bar_end=start + timedelta(minutes=1),
        open_price=close_value - Decimal("0.10"),
        high_price=close_value + Decimal("0.20"),
        low_price=close_value - Decimal("0.20"),
        close_price=close_value,
        volume=Decimal("100"),
        vwap=close_value,
        transaction_count=5,
        source_kind=BarSourceKind.REALTIME_STREAM,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
        is_final=True,
        system_observed_at=observed_at or start + timedelta(minutes=1, seconds=1),
        replay_available_at=None,
        adjusted=False,
        payload_sha256=__import__("hashlib").sha256(payload.encode()).hexdigest(),
    )


def test_normalize_polygon_minute_aggregate_uses_regular_session_contract():
    start = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    result = normalize_stream_minute(
        SECURITY_ID,
        {
            "ev": "AM", "sym": "aapl", "s": int(start.timestamp() * 1000),
            "e": int((start + timedelta(minutes=1)).timestamp() * 1000),
            "o": 100, "h": 101, "l": 99, "c": 100.5, "v": 1000, "vw": 100.25,
        },
        observed_at=start + timedelta(minutes=1, seconds=1),
    )

    assert result is not None
    assert result.ticker == "AAPL"
    assert result.interval == "1m"
    assert result.source_kind is BarSourceKind.REALTIME_STREAM
    assert result.bar_end == start + timedelta(minutes=1)


def test_stream_accumulator_finalizes_session_anchored_5m_15m_and_30m_bars():
    start = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    accumulator = SessionBarAccumulator()
    finalized = []

    for index in range(30):
        result = accumulator.ingest(minute(start + timedelta(minutes=index), close=str(100 + index)))
        finalized.extend(result.finalized_bars)

    assert [(row.interval, row.bar_start, row.bar_end) for row in finalized] == [
        ("5m", start, start + timedelta(minutes=5)),
        ("5m", start + timedelta(minutes=5), start + timedelta(minutes=10)),
        ("15m", start, start + timedelta(minutes=15)),
        ("5m", start + timedelta(minutes=10), start + timedelta(minutes=15)),
        ("5m", start + timedelta(minutes=15), start + timedelta(minutes=20)),
        ("5m", start + timedelta(minutes=20), start + timedelta(minutes=25)),
        ("15m", start + timedelta(minutes=15), start + timedelta(minutes=30)),
        ("30m", start, start + timedelta(minutes=30)),
        ("5m", start + timedelta(minutes=25), start + timedelta(minutes=30)),
    ]
    thirty = next(row for row in finalized if row.interval == "30m")
    assert thirty.open_price == Decimal("99.90")
    assert thirty.close_price == Decimal("129")
    assert thirty.high_price == Decimal("129.20")
    assert thirty.low_price == Decimal("99.80")
    assert thirty.volume == Decimal("3000")
    assert len(thirty.source_bar_revision_ids) == 30
    assert thirty.reconciliation_status == "PENDING"
    assert thirty.quality_codes == ()


def test_duplicate_is_ignored_and_late_minute_revises_sparse_window():
    start = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    accumulator = SessionBarAccumulator(intervals=("15m",))
    source = [minute(start + timedelta(minutes=index), close=str(100 + index)) for index in range(15)]

    assert accumulator.ingest(source[0]).minute_bar is source[0]
    duplicate = accumulator.ingest(source[0])
    assert duplicate.minute_bar is None
    assert duplicate.finalized_bars == ()
    for row in source[1:5] + source[6:]:
        result = accumulator.ingest(row)
    assert len(result.finalized_bars) == 1
    sparse = result.finalized_bars[0]
    assert sparse.quality_codes == ("SPARSE_STREAM_WINDOW",)
    assert len(sparse.source_bar_revision_ids) == 14

    late = replace(
        source[5],
        system_observed_at=start + timedelta(minutes=16),
    )
    correction = accumulator.ingest(late)
    assert correction.minute_bar is not None
    assert len(correction.finalized_bars) == 1
    corrected = correction.finalized_bars[0]
    assert corrected.supersedes_bar_revision_id == sparse.bar_revision_id
    assert corrected.quality_codes == ("STREAM_SOURCE_CORRECTION",)
    assert len(corrected.source_bar_revision_ids) == 15


def test_early_close_final_window_ends_at_exchange_close():
    start = datetime(2026, 11, 27, 17, 30, tzinfo=UTC)
    accumulator = SessionBarAccumulator(intervals=("30m",))
    finalized = []

    for index in range(30):
        finalized.extend(
            accumulator.ingest(
                minute(start + timedelta(minutes=index), close=str(100 + index))
            ).finalized_bars
        )

    assert len(finalized) == 1
    assert finalized[0].bar_end == datetime(2026, 11, 27, 18, 0, tzinfo=UTC)


def test_stream_minute_outside_regular_session_is_ignored_or_rejected():
    premarket = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
    assert normalize_stream_minute(
        SECURITY_ID,
        {"sym": "AAPL", "s": int(premarket.timestamp() * 1000), "o": 1, "h": 1, "l": 1, "c": 1, "v": 1},
        observed_at=premarket + timedelta(minutes=1),
    ) is None

    with pytest.raises(ValueError, match="outside regular session"):
        SessionBarAccumulator().ingest(minute(premarket, close="1"))


def interval_bar(
    *,
    source_kind: BarSourceKind,
    close: str = "101",
) -> EquityBarRevision:
    start = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    close_value = Decimal(close)
    return EquityBarRevision(
        bar_revision_id=uuid4(), security_id=SECURITY_ID, ticker="AAPL", interval="30m",
        session_date=start.date(), bar_start=start, bar_end=start + timedelta(minutes=30),
        open_price=Decimal("100"), high_price=max(Decimal("102"), close_value),
        low_price=Decimal("99"), close_price=close_value, volume=Decimal("1000"),
        vwap=Decimal("100.5"), transaction_count=100, source_kind=source_kind,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED, is_final=True,
        system_observed_at=start + timedelta(minutes=30, seconds=1),
        replay_available_at=None, adjusted=False,
        payload_sha256=__import__("hashlib").sha256(f"{source_kind}:{close}".encode()).hexdigest(),
        reconciliation_status="PENDING" if source_kind is BarSourceKind.REALTIME_STREAM else None,
    )


def test_reconciliation_records_match_and_native_correction_immutably():
    stream = interval_bar(source_kind=BarSourceKind.REALTIME_STREAM)
    native = interval_bar(source_kind=BarSourceKind.NATIVE_REST)
    observed_at = stream.bar_end + timedelta(minutes=1)

    matched = reconcile_interval_bar(
        stream_bar=stream, native_bar=native, observed_at=observed_at
    )
    assert matched.source_kind is BarSourceKind.RECONCILED
    assert matched.reconciliation_status == "MATCHED"
    assert matched.supersedes_bar_revision_id == stream.bar_revision_id
    assert matched.source_bar_revision_ids == (stream.bar_revision_id, native.bar_revision_id)

    corrected_native = interval_bar(source_kind=BarSourceKind.NATIVE_REST, close="103")
    corrected = reconcile_interval_bar(
        stream_bar=stream, native_bar=corrected_native, observed_at=observed_at
    )
    assert corrected.reconciliation_status == "CORRECTED"
    assert corrected.close_price == Decimal("103")
    assert "RECONCILED_CLOSE_PRICE" in corrected.quality_codes
    assert corrected.supersedes_bar_revision_id == stream.bar_revision_id


def test_reconciliation_can_preserve_derived_bar_as_canonical_base():
    derived = interval_bar(source_kind=BarSourceKind.DERIVED, close="101")
    native = interval_bar(source_kind=BarSourceKind.NATIVE_REST, close="103")

    reconciled = reconcile_interval_bar(
        stream_bar=derived,
        native_bar=native,
        observed_at=derived.bar_end + timedelta(minutes=1),
        prefer_stream=True,
    )

    assert reconciled.source_kind is BarSourceKind.RECONCILED
    assert reconciled.reconciliation_status == "CORRECTED"
    assert reconciled.close_price == derived.close_price
    assert reconciled.source_bar_revision_ids == (
        derived.bar_revision_id, native.bar_revision_id,
    )


def test_reconciliation_records_missing_side_without_fabrication():
    stream = interval_bar(source_kind=BarSourceKind.REALTIME_STREAM)
    native = interval_bar(source_kind=BarSourceKind.NATIVE_REST)
    observed_at = stream.bar_end + timedelta(minutes=1)

    native_missing = reconcile_interval_bar(
        stream_bar=stream, native_bar=None, observed_at=observed_at
    )
    derived_missing = reconcile_interval_bar(
        stream_bar=None, native_bar=native, observed_at=observed_at
    )

    assert native_missing.reconciliation_status == "NATIVE_MISSING"
    assert native_missing.close_price == stream.close_price
    assert derived_missing.reconciliation_status == "DERIVED_MISSING"
    assert derived_missing.close_price == native.close_price


def test_reconciliation_rejects_mismatched_interval_keys():
    stream = interval_bar(source_kind=BarSourceKind.REALTIME_STREAM)
    native = replace(
        interval_bar(source_kind=BarSourceKind.NATIVE_REST),
        ticker="MSFT",
    )

    with pytest.raises(ValueError, match="same interval"):
        reconcile_interval_bar(
            stream_bar=stream,
            native_bar=native,
            observed_at=stream.bar_end + timedelta(minutes=1),
        )


def test_stream_processor_filters_messages_and_persists_minute_and_interval_bars():
    security = SecurityReferenceRevision(
        security_revision_id=uuid4(), security_id=SECURITY_ID, ticker="AAPL",
        active=True, company_name="Apple Inc.", security_type="CS", cik=None,
        composite_figi=None, share_class_figi=None, primary_exchange="XNAS",
        sic_code=None, sic_description=None, sector=None, industry=None,
        list_date=None, delisted_date=None, weighted_shares=None, free_float=None,
        free_float_percent=None, market_cap=None, source="TEST",
        effective_from=datetime(2026, 8, 1, tzinfo=UTC),
        observed_at=datetime(2026, 8, 1, tzinfo=UTC), payload_sha256="a" * 64,
        raw_payload_json="{}",
    )
    repository = FakeBarRepository()
    processor = EquityStreamProcessor(
        (security,), ingestion_segment_id=uuid4(),
        accumulator=SessionBarAccumulator(intervals=("15m",)),
        bar_repository=repository,
    )
    start = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    messages = [
        {"ev": "status", "message": "connected"},
        {"ev": "AM", "sym": "MSFT", "s": int(start.timestamp() * 1000),
         "o": 1, "h": 1, "l": 1, "c": 1, "v": 1},
        *(
            {"ev": "AM", "sym": "AAPL", "s": int((start + timedelta(minutes=index)).timestamp() * 1000),
             "o": 100 + index, "h": 101 + index, "l": 99 + index,
             "c": 100.5 + index, "v": 100}
            for index in range(15)
        ),
    ]

    result = processor.process(
        messages, observed_at=start + timedelta(minutes=15, seconds=1)
    )

    assert result.received_messages == 17
    assert result.accepted_minutes == 15
    assert result.duplicate_minutes == 0
    assert result.ignored_messages == 2
    assert result.finalized_bars == 1
    assert result.inserted_bars == 16
    assert len(repository.persisted) == 16
    assert repository.persisted[-1].interval == "15m"


def test_stream_processor_flushes_quiet_sparse_window():
    security = SecurityReferenceRevision(
        security_revision_id=uuid4(), security_id=SECURITY_ID, ticker="AAPL",
        active=True, company_name=None, security_type=None, cik=None,
        composite_figi=None, share_class_figi=None, primary_exchange=None,
        sic_code=None, sic_description=None, sector=None, industry=None,
        list_date=None, delisted_date=None, weighted_shares=None, free_float=None,
        free_float_percent=None, market_cap=None, source="TEST",
        effective_from=datetime(2026, 8, 1, tzinfo=UTC),
        observed_at=datetime(2026, 8, 1, tzinfo=UTC), payload_sha256="a" * 64,
        raw_payload_json="{}",
    )
    repository = FakeBarRepository()
    processor = EquityStreamProcessor(
        (security,), ingestion_segment_id=uuid4(),
        accumulator=SessionBarAccumulator(intervals=("15m",)),
        bar_repository=repository,
    )
    start = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    processor.process(
        [{"ev": "AM", "sym": "AAPL", "s": int(start.timestamp() * 1000),
          "o": 100, "h": 101, "l": 99, "c": 100, "v": 100}],
        observed_at=start + timedelta(minutes=1, seconds=1),
    )

    result = processor.flush(observed_at=start + timedelta(minutes=15, seconds=1))

    assert result.finalized_bars == 1
    assert result.inserted_bars == 1
    assert repository.persisted[-1].quality_codes == ("SPARSE_STREAM_WINDOW",)


def test_stream_processor_counts_duplicate_minutes_without_reinserting():
    security = SecurityReferenceRevision(
        security_revision_id=uuid4(), security_id=SECURITY_ID, ticker="AAPL",
        active=True, company_name=None, security_type=None, cik=None,
        composite_figi=None, share_class_figi=None, primary_exchange=None,
        sic_code=None, sic_description=None, sector=None, industry=None,
        list_date=None, delisted_date=None, weighted_shares=None, free_float=None,
        free_float_percent=None, market_cap=None, source="TEST",
        effective_from=datetime(2026, 8, 1, tzinfo=UTC),
        observed_at=datetime(2026, 8, 1, tzinfo=UTC), payload_sha256="a" * 64,
        raw_payload_json="{}",
    )
    repository = FakeBarRepository()
    processor = EquityStreamProcessor(
        (security,), ingestion_segment_id=uuid4(),
        accumulator=SessionBarAccumulator(intervals=("15m",)),
        bar_repository=repository,
    )
    start = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    message = {"ev": "AM", "sym": "AAPL", "s": int(start.timestamp() * 1000),
               "o": 100, "h": 101, "l": 99, "c": 100, "v": 100}

    first = processor.process((message,), observed_at=start + timedelta(minutes=1))
    repeated = processor.process((message,), observed_at=start + timedelta(minutes=1, seconds=1))

    assert first.accepted_minutes == 1
    assert repeated.accepted_minutes == 0
    assert repeated.duplicate_minutes == 1
    assert repeated.inserted_bars == 0


def test_polygon_advanced_adapter_builds_sorted_minute_subscriptions():
    created = {}

    class FakeClient:
        def __init__(self, **kwargs):
            created.update(kwargs)

        def run(self, handler):
            created["handler"] = handler

    handler = lambda messages: None
    client = PolygonAdvancedStreamClient(
        "test-key", max_reconnects=7, client_factory=FakeClient
    )

    client.run_minute_aggregates(("msft", "AAPL", "AAPL"), handler)

    assert created["feed"] == "socket.polygon.io"
    assert created["market"] == "stocks"
    assert created["max_reconnects"] == 7
    assert created["subscriptions"] == ["AM.AAPL", "AM.MSFT"]
    assert created["handler"] is handler


def test_polygon_advanced_adapter_connects_asynchronously():
    created = {}

    class FakeClient:
        def __init__(self, **kwargs):
            created.update(kwargs)

        async def connect(self, handler):
            created["handler"] = handler

    async def handler(messages):
        return None

    client = PolygonAdvancedStreamClient("test-key", client_factory=FakeClient)

    asyncio.run(client.connect_minute_aggregates(("AAPL",), handler))

    assert created["subscriptions"] == ["AM.AAPL"]
    assert created["handler"] is handler
