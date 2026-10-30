import hashlib
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

from realtime.contracts import AssetClass, MarketChannel, MarketFeed
from realtime.journal import RawJournalRecord
from realtime.normalizer import (
    InstrumentAdmission,
    MAXIMUM_MARKET_EVENT_PROVIDER_CLOCK_SKEW,
    MarketEventQuality,
    NORMALIZER_VERSION,
    normalize_raw_message,
)


UTC = timezone.utc
GENERATION = UUID("11111111-1111-1111-1111-111111111111")
SESSION = UUID("22222222-2222-2222-2222-222222222222")
RECEIVED = datetime(2026, 10, 5, 14, 30, 1, tzinfo=UTC)


ADMISSIONS = {
    "SPY": InstrumentAdmission(1, "SPY", "SPY", AssetClass.EQUITY, 0),
    "O:SPY261218C00600000": InstrumentAdmission(
        2,
        "O:SPY261218C00600000",
        "SPY",
        AssetClass.OPTION,
        0,
    ),
}


def _raw(
    payload,
    *,
    feed=MarketFeed.STOCKS,
    offset=1,
    received_at=RECEIVED,
    committed_at=RECEIVED,
):
    digest = hashlib.sha256(payload).hexdigest()
    return RawJournalRecord(
        raw_offset=offset,
        generation=GENERATION,
        stream_session_id=SESSION,
        message_number=offset - 1,
        feed=feed,
        received_at=received_at,
        received_monotonic_ns=100 + offset,
        payload=payload,
        payload_sha256=digest,
        schema_version=1,
        committed_at=committed_at,
    )


def test_normalizer_preserves_exact_trade_and_quote_fields():
    timestamp = int((RECEIVED.timestamp() - 0.1) * 1000)
    raw = _raw(
        (
            '[{"ev":"Q","sym":"SPY","bp":100.01,"ap":100.03,'
            f'"bs":10,"as":12,"t":{timestamp},"q":7}},'
            '{"ev":"T","sym":"SPY","p":100.025,"s":25,"x":4,'
            f'"c":[0,12],"t":{timestamp},"q":8}}]'
        ).encode("ascii")
    )

    batch = normalize_raw_message(raw, ADMISSIONS)

    assert batch.quarantines == ()
    assert [event.event_kind for event in batch.events] == [
        MarketChannel.QUOTE,
        MarketChannel.TRADE,
    ]
    quote, trade = batch.events
    assert quote.bid == Decimal("100.01")
    assert quote.ask == Decimal("100.03")
    assert quote.quality is MarketEventQuality.GOOD
    assert trade.price == Decimal("100.025")
    assert trade.size == Decimal("25")
    assert trade.conditions == (0, 12)
    assert trade.quality is MarketEventQuality.UNCLASSIFIED
    assert trade.source_timestamp_unit == "ms"


def test_normalizer_keeps_one_sided_quote_without_fabricating_zero():
    timestamp = int((RECEIVED.timestamp() - 0.1) * 1000)
    batch = normalize_raw_message(
        _raw(
            f'[{ { } }]'.encode("ascii").replace(
                b"{}",
                (
                    '{"ev":"Q","sym":"SPY","ap":100.03,"as":12,'
                    f'"t":{timestamp},"q":7}}'
                ).encode("ascii"),
            )
        ),
        ADMISSIONS,
    )

    quote = batch.events[0]
    assert quote.bid is None and quote.bid_size is None
    assert quote.ask == Decimal("100.03")
    assert quote.quality is MarketEventQuality.ONE_SIDED
    assert quote.reason_codes == ("MISSING_QUOTE_SIDE",)


def test_normalizer_handles_option_trade_and_stock_minute_aggregate():
    trade_timestamp = int((RECEIVED.timestamp() - 0.2) * 1000)
    option = normalize_raw_message(
        _raw(
            (
                '[{"ev":"T","sym":"O:SPY261218C00600000","p":2.50,'
                f'"s":501,"x":302,"c":[233],"t":{trade_timestamp},"q":9}}]'
            ).encode("ascii"),
            feed=MarketFeed.OPTIONS,
        ),
        ADMISSIONS,
    ).events[0]
    start = trade_timestamp - 60_000
    aggregate = normalize_raw_message(
        _raw(
            (
                '[{"ev":"AM","sym":"SPY","o":100,"h":101,"l":99,'
                f'"c":100.5,"v":1000,"vw":100.25,"s":{start},'
                f'"e":{trade_timestamp}}}]'
            ).encode("ascii")
        ),
        ADMISSIONS,
    ).events[0]

    assert option.instrument_id == 2
    assert option.size == Decimal("501")
    assert option.underlying == "SPY"
    assert aggregate.event_kind is MarketChannel.MINUTE_AGGREGATE
    assert aggregate.price == Decimal("100.5")
    assert aggregate.size == Decimal("1000")
    assert aggregate.quality is MarketEventQuality.GOOD


def test_normalizer_prefers_fractional_am_volume_and_versions_the_contract():
    timestamp = int((RECEIVED.timestamp() - 0.2) * 1000)
    start = timestamp - 60_000

    aggregate = normalize_raw_message(
        _raw(
            (
                '[{"ev":"AM","sym":"SPY","o":100,"h":101,"l":99,'
                f'"c":100.5,"v":1000,"dv":"1000.125","vw":100.25,'
                f'"s":{start},"e":{timestamp}}}]'
            ).encode("ascii")
        ),
        ADMISSIONS,
    ).events[0]

    details = json.loads(aggregate.details_json)
    assert NORMALIZER_VERSION == "massive_market_event_v6"
    assert aggregate.normalizer_version == NORMALIZER_VERSION
    assert aggregate.size == Decimal("1000.125")
    assert details["provider_integer_volume"] == "1000"
    assert details["provider_fractional_volume"] == "1000.125"


def test_normalizer_allows_only_bounded_stocks_am_finality_deferral():
    maximum = int((RECEIVED.timestamp() + 2) * 1000)
    excessive = maximum + 1

    def aggregate(timestamp):
        observed_at = datetime.fromtimestamp(
            timestamp / 1000,
            tz=UTC,
        )
        return normalize_raw_message(
            _raw(
                (
                    '[{"ev":"AM","sym":"SPY","o":100,"h":101,"l":99,'
                    f'"c":100.5,"v":1000,"vw":100.25,'
                    f'"s":{timestamp - 60_000},"e":{timestamp}}}]'
                ).encode("ascii")
            ),
            ADMISSIONS,
            finality_observed_at=observed_at,
        ).events[0]

    accepted = aggregate(maximum)
    rejected = aggregate(excessive)

    assert accepted.quality is MarketEventQuality.GOOD
    assert accepted.reason_codes == ()
    assert rejected.quality is MarketEventQuality.INVALID
    assert rejected.reason_codes == ("SOURCE_TIME_AFTER_RECEIPT",)


def test_normalizer_defers_am_until_bar_end_without_changing_source_clocks():
    event_at = datetime(
        2026, 10, 22, 17, 33, tzinfo=UTC
    )
    received_at = datetime(
        2026, 10, 22, 17, 32, 58, 985583, tzinfo=UTC
    )
    committed_at = datetime(
        2026, 10, 22, 17, 32, 59, 1293, tzinfo=UTC
    )
    timestamp = int(event_at.timestamp() * 1000)

    raw = _raw(
        (
            '[{"ev":"AM","sym":"SPY","o":100,"h":101,"l":99,'
            f'"c":100.5,"v":1000,"vw":100.25,'
            f'"s":{timestamp - 60_000},"e":{timestamp}}}]'
        ).encode("ascii"),
        received_at=received_at,
        committed_at=committed_at,
    )
    pending = normalize_raw_message(
        raw,
        ADMISSIONS,
        finality_observed_at=committed_at,
    ).events[0]
    aggregate = normalize_raw_message(
        raw,
        ADMISSIONS,
        finality_observed_at=event_at,
    ).events[0]

    assert event_at - received_at > timedelta(seconds=1)
    assert event_at - committed_at < timedelta(seconds=2)
    assert pending.quality is MarketEventQuality.INVALID
    assert pending.reason_codes == ("AM_FINALITY_PENDING",)
    assert aggregate.quality is MarketEventQuality.GOOD
    assert aggregate.reason_codes == ()


def test_normalizer_allows_only_bounded_trade_and_quote_provider_clock_skew():
    maximum = int(
        (
            RECEIVED + MAXIMUM_MARKET_EVENT_PROVIDER_CLOCK_SKEW
        ).timestamp()
        * 1000
    )
    accepted = normalize_raw_message(
        _raw(
            (
                '[{"ev":"Q","sym":"SPY","bp":100,"ap":101,"bs":1,'
                f'"as":1,"t":{maximum}}}]'
            ).encode("ascii")
        ),
        ADMISSIONS,
    ).events[0]
    rejected = normalize_raw_message(
        _raw(
            (
                '[{"ev":"Q","sym":"SPY","bp":100,"ap":101,"bs":1,'
                f'"as":1,"t":{maximum + 1}}}]'
            ).encode("ascii")
        ),
        ADMISSIONS,
    ).events[0]

    assert accepted.quality is MarketEventQuality.GOOD
    assert accepted.reason_codes == ()
    assert rejected.quality is MarketEventQuality.INVALID
    assert rejected.reason_codes == (
        "SOURCE_TIME_AFTER_RECEIPT",
    )


def test_normalizer_quarantines_bad_siblings_and_unadmitted_symbols():
    timestamp = int((RECEIVED.timestamp() - 0.1) * 1000)
    batch = normalize_raw_message(
        _raw(
            (
                '[{"ev":"Q","sym":"SPY","bp":100,"ap":101,"bs":1,'
                f'"as":1,"t":{timestamp}}},'
                '{"ev":"Q","sym":"AAPL","bp":1,"ap":2,"bs":1,'
                f'"as":1,"t":{timestamp}}},42]'
            ).encode("ascii")
        ),
        ADMISSIONS,
    )

    assert len(batch.events) == 1
    assert [row.reason_code for row in batch.quarantines] == [
        "INSTRUMENT_NOT_ADMITTED",
        "INVALID_EVENT_SHAPE",
    ]


def test_normalizer_is_deterministic_and_retains_invalid_json():
    timestamp = int((RECEIVED.timestamp() - 0.1) * 1000)
    raw = _raw(
        (
            '[{"sym":"SPY","ev":"Q","as":2,"bs":1,'
            f'"ap":101,"bp":100,"q":3,"t":{timestamp}}}]'
        ).encode("ascii")
    )

    first = normalize_raw_message(raw, ADMISSIONS)
    repeated = normalize_raw_message(raw, ADMISSIONS)
    invalid = normalize_raw_message(_raw(b"not-json", offset=2), ADMISSIONS)

    assert first == repeated
    assert first.events[0].payload_sha256 == repeated.events[0].payload_sha256
    assert invalid.events == ()
    assert invalid.quarantines[0].reason_code == "INVALID_JSON"
    assert invalid.quarantines[0].event_index == -1