import json
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
import requests

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.config import load_option_runtime_configuration
from options.domain import (
    AssetType,
    BatchStatus,
    CatalogEligibility,
    ContractType,
    ExerciseStyle,
    OptionContractCatalogEntry,
    OptionTradeCursor,
)
from options.errors import OptionProviderError, ProviderErrorCategory
from options.data.polygon_developer import PolygonDeveloperEngine
from options.data.polygon_http import PolygonHttpResponse
from options.data.polygon_http import PolygonRateLimitGate


UTC = timezone.utc
NOW = datetime(2026, 8, 29, 20, 15, tzinfo=UTC)


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def get(self, url, params):
        self.requests.append((url, dict(params)))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class FakeIngestionRepository:
    def __init__(self):
        self.pages = []
        self.failed = []
        self.completed = []

    def begin_batch(self, batch, asset_type, policy_version, configuration_sha256):
        self.batch = batch
        self.asset_type = asset_type
        return batch.batch_id

    def persist_page(self, page):
        self.pages.append(page)
        return True

    def load_batch(self, batch_id):
        return None

    def complete_batch(self, *args):
        self.completed.append(args)
        return args[1]

    def fail_batch(self, *args):
        self.failed.append(args)


class FakeTradeRepository:
    def __init__(self):
        self.events = []
        self.cursors = []

    def persist(self, events):
        self.events.extend(events)
        return len(events)

    def advance_cursor(self, *args):
        self.cursors.append(args)
        return True

    def get_cursor(self, provider, contract_id):
        return None


def _response(payload, status=200):
    return PolygonHttpResponse(status, json.dumps(payload).encode("utf-8"))


def _engine(responses, rate_limit_gate=None):
    configuration = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "never-persist-this"}, BACKEND_DIR
    )
    repository = FakeIngestionRepository()
    trade_repository = FakeTradeRepository()
    transport = FakeTransport(responses)
    engine = PolygonDeveloperEngine(
        configuration,
        repository,
        trade_repository,
        transport=transport,
        rate_limit_gate=rate_limit_gate,
        clock=lambda: NOW,
    )
    return engine, repository, transport, trade_repository


def test_chain_consumes_all_pages_and_strips_key_from_next_request_metadata():
    next_url = (
        "https://api.polygon.io/v3/snapshot/options/SPY?cursor=next-1"
        "&apiKey=never-persist-this"
    )
    engine, repository, transport, _ = _engine(
        [
            _response({"request_id": "r1", "results": [{"ticker": "O:1"}], "next_url": next_url}),
            _response({"request_id": "r2", "results": [{"ticker": "O:2"}]}),
        ]
    )

    batch = engine.get_option_chain(
        "SPY", NOW, date(2026, 9, 30), Decimal("500"), Decimal("700")
    )

    assert batch.status is BatchStatus.COMPLETE
    assert len(batch.pages) == 2
    assert repository.completed
    assert repository.failed == []
    assert transport.requests[1][1] == {"cursor": "next-1"}
    persisted_metadata = repr([page.request_metadata for page in repository.pages])
    assert "never-persist-this" not in persisted_metadata


def test_hostile_next_page_is_persisted_invalid_and_quarantines_batch():
    engine, repository, _, _ = _engine(
        [
            _response(
                {
                    "request_id": "r1",
                    "results": [{"ticker": "O:1"}],
                    "next_url": "https://evil.example/v3/snapshot/options/SPY?cursor=x",
                }
            )
        ]
    )

    with pytest.raises(OptionProviderError) as error:
        engine.get_option_chain(
            "SPY", NOW, date(2026, 9, 30), Decimal("500"), Decimal("700")
        )

    assert error.value.category is ProviderErrorCategory.PAGINATION
    assert repository.pages[0].validation_status.value == "INVALID"
    assert repository.failed[0][1] is BatchStatus.QUARANTINED


def test_full_250_row_terminal_page_is_complete_without_next_url():
    engine, repository, _, _ = _engine(
        [
            _response(
                {
                    "request_id": "r1",
                    "results": [{"ticker": f"O:{index}"} for index in range(250)],
                }
            )
        ]
    )

    batch = engine.get_option_chain(
        "SPY", NOW, date(2026, 9, 30), Decimal("500.001"), Decimal("699.999")
    )

    assert batch.complete is True
    assert batch.row_count == 250
    assert batch.pages[0].terminal is True
    assert repository.completed


def test_repeated_pagination_cursor_quarantines_complete_chain_attempt():
    next_url = "https://api.polygon.io/v3/snapshot/options/SPY?cursor=repeated"
    engine, repository, _, _ = _engine(
        [
            _response({"results": [{"ticker": "O:1"}], "next_url": next_url}),
            _response({"results": [{"ticker": "O:2"}], "next_url": next_url}),
        ]
    )

    with pytest.raises(OptionProviderError) as error:
        engine.get_option_chain(
            "SPY", NOW, date(2026, 9, 30), Decimal("500"), Decimal("700")
        )

    assert error.value.category is ProviderErrorCategory.PAGINATION
    assert len(repository.pages) == 2
    assert repository.failed[0][1] is BatchStatus.QUARANTINED


def test_transport_exhaustion_fails_batch_without_fabricating_raw_page():
    engine, repository, _, _ = _engine([requests.Timeout("contains-secret")])

    with pytest.raises(OptionProviderError) as error:
        engine.get_option_chain(
            "SPY", NOW, date(2026, 9, 30), Decimal("500"), Decimal("700")
        )

    assert error.value.category is ProviderErrorCategory.TRANSIENT
    assert "contains-secret" not in str(error.value)
    assert repository.pages == []
    assert repository.failed[0][1] is BatchStatus.FAILED


def test_spot_uses_latest_source_bar_at_or_before_requested_time():
    bar_time = int(NOW.timestamp() * 1000)
    engine, _, _, _ = _engine(
        [_response({"results": [{"t": bar_time, "c": 650.25}]})]
    )

    spot = engine.get_spot_price("SPY", NOW)

    assert spot.price == Decimal("650.25")
    assert spot.market_data_time == NOW


def test_underlying_minute_bars_are_bounded_sorted_and_deduplicated():
    earlier = NOW.replace(minute=0)
    earlier_ms = int(earlier.timestamp() * 1000)
    later_ms = int(NOW.timestamp() * 1000)
    engine, _, _, _ = _engine(
        [
            _response(
                {
                    "results": [
                        {"t": later_ms, "c": 101},
                        {"t": earlier_ms, "c": 100},
                        {"t": later_ms, "c": 102},
                    ]
                }
            )
        ]
    )

    bars = engine.get_underlying_minute_bars("SPY", earlier, NOW)

    assert [bar.market_data_time for bar in bars] == [earlier, NOW]
    assert bars[-1].close == Decimal("102")


def test_reference_preserves_adjustment_metadata_for_catalog_rejection():
    engine, _, _, _ = _engine(
        [
            _response(
                {
                    "results": {
                        "ticker": "O:SPY260904C00650000",
                        "underlying_ticker": "SPY",
                        "contract_type": "call",
                        "expiration_date": "2026-09-04",
                        "strike_price": 650,
                        "exercise_style": "american",
                        "shares_per_contract": 100,
                        "additional_underlyings": [{"ticker": "XYZ"}],
                        "correction": 1,
                    }
                }
            )
        ]
    )

    reference = engine.get_option_reference(
        "O:SPY260904C00650000", date(2026, 8, 29), engine._asset_type("SPY")
    )

    assert reference.changes_deliverables is True
    assert "XYZ" in reference.additional_underlyings_json
    assert reference.valid_from == datetime(2026, 8, 29, tzinfo=UTC)


def test_reference_refresh_consumes_pages_with_inclusive_bounds():
    next_url = (
        "https://api.polygon.io/v3/reference/options/contracts?cursor=reference-next"
    )
    result = {
        "ticker": "O:SPY260904C00650000",
        "underlying_ticker": "SPY",
        "contract_type": "call",
        "expiration_date": "2026-09-04",
        "strike_price": 650,
        "exercise_style": "american",
        "shares_per_contract": 100,
    }
    engine, _, transport, _ = _engine(
        [
            _response({"status": "OK", "results": [result], "next_url": next_url}),
            _response({"status": "OK", "results": [{**result, "ticker": "O:SECOND"}]}),
        ]
    )

    references = engine.list_option_references(
        "SPY",
        date(2026, 8, 29),
        date(2026, 10, 13),
        AssetType.ETF,
        Decimal("500.001"),
        Decimal("699.999"),
    )

    assert len(references) == 2
    initial_params = transport.requests[0][1]
    assert initial_params["expiration_date.gte"] == "2026-08-29"
    assert initial_params["expiration_date.lte"] == "2026-10-13"
    assert initial_params["strike_price.gte"] == "500.001"
    assert initial_params["strike_price.lte"] == "699.999"
    assert transport.requests[1][1] == {"cursor": "reference-next"}


def _contract():
    return OptionContractCatalogEntry(
        contract_id=42,
        contract_ticker="O:SPY260904C00650000",
        underlyer="SPY",
        asset_type=AssetType.ETF,
        provider="polygon",
        provider_version="1",
        contract_type=ContractType.CALL,
        expiration_date=date(2026, 9, 4),
        strike=Decimal("650"),
        exercise_style=ExerciseStyle.AMERICAN,
        shares_per_contract=100,
        primary_exchange="X",
        eligibility_status=CatalogEligibility.VALIDATED_ACTIVE,
        exclusion_reasons=(),
        valid_from=NOW,
        valid_to=None,
        first_observed_at=NOW,
        revised_observed_at=None,
        payload_sha256="a" * 64,
    )


def test_delayed_trades_persist_raw_then_events_and_advance_complete_cursor():
    sip_timestamp = int((NOW.timestamp() - 60) * 1_000_000_000)
    engine, ingestion, transport, trades = _engine(
        [
            _response(
                {
                    "request_id": "trade-r1",
                    "status": "DELAYED",
                    "results": [
                        {
                            "id": "provider-metadata",
                            "sip_timestamp": sip_timestamp,
                            "sequence_number": 10,
                            "participant_timestamp": sip_timestamp,
                            "exchange": 1,
                            "conditions": [1, 2],
                            "correction": 0,
                            "price": 1.25,
                            "size": 2,
                        }
                    ],
                }
            )
        ]
    )
    cursor = OptionTradeCursor(NOW, 9, 30)

    result = engine.get_option_trades(
        _contract(), NOW.replace(minute=0), NOW, cursor
    )

    assert result.complete is True
    assert len(result.events) == 1
    assert result.events[0].notional == Decimal("250.00")
    assert result.events[0].provider_trade_id == "provider-metadata"
    assert ingestion.pages
    assert trades.events == list(result.events)
    assert trades.cursors[0][3] == 10
    assert ingestion.completed[0][-1].value == "CLASSIFY_TRADES"
    expected_overlap = int((NOW.timestamp() - 30) * 1_000_000_000)
    assert transport.requests[0][1]["timestamp.gte"] == str(expected_overlap)


def test_entitlement_probe_requires_expected_quote_denial():
    bar_time = int(NOW.timestamp() * 1000)
    engine, _, _, _ = _engine(
        [
            _response({"results": [{}]}),
            _response({"status": "DELAYED", "results": [{}]}),
            PolygonHttpResponse(403, b'{"error":"NOT_AUTHORIZED"}'),
            _response({"results": [{"t": bar_time, "c": 650.25}]}),
        ]
    )

    capabilities = engine.probe_developer_capabilities(
        "SPY", "O:SPY260904C00650000", NOW
    )

    assert {capability.value for capability in capabilities} == {
        "CHAIN_SNAPSHOT",
        "OPTION_TRADES",
        "UNDERLYING_PRICE",
    }


def test_rate_limit_retry_after_is_honored_without_leaking_response_body():
    monotonic_time = [0.0]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        monotonic_time[0] += seconds

    gate = PolygonRateLimitGate(
        monotonic=lambda: monotonic_time[0],
        sleep=sleep,
    )
    bar_time = int(NOW.timestamp() * 1000)
    engine, _, _, _ = _engine(
        [
            PolygonHttpResponse(
                429,
                b'{"secret":"must-not-appear"}',
                (("Retry-After", "2"),),
            ),
            _response({"results": [{"t": bar_time, "c": 650.25}]}),
        ],
        rate_limit_gate=gate,
    )

    spot = engine.get_spot_price("SPY", NOW)

    assert spot.price == Decimal("650.25")
    assert sleeps == [2.0]


@pytest.mark.parametrize(
    ("response", "category"),
    [
        (PolygonHttpResponse(403, b'{"apiKey":"secret"}'), ProviderErrorCategory.AUTHORIZATION),
        (PolygonHttpResponse(500, b'{"apiKey":"secret"}'), ProviderErrorCategory.TRANSIENT),
        (PolygonHttpResponse(200, b"not-json"), ProviderErrorCategory.SCHEMA),
    ],
)
def test_provider_errors_are_categorized_without_response_content(response, category):
    engine, _, _, _ = _engine([response])

    with pytest.raises(OptionProviderError) as error:
        engine.get_spot_price("SPY", NOW)

    assert error.value.category is category
    assert "secret" not in str(error.value)


def test_malformed_chain_body_is_persisted_before_quarantine():
    engine, repository, _, _ = _engine([PolygonHttpResponse(200, b"not-json")])

    with pytest.raises(OptionProviderError) as error:
        engine.get_option_chain(
            "SPY", NOW, date(2026, 9, 30), Decimal("500"), Decimal("700")
        )

    assert error.value.category is ProviderErrorCategory.SCHEMA
    assert repository.pages[0].response_bytes == b"not-json"
    assert repository.pages[0].validation_status.value == "INVALID"
    assert repository.failed[0][1] is BatchStatus.QUARANTINED