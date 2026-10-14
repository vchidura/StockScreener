import hashlib
import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from options.analytics.marks import UnderlyingMinuteBar
from options.config import load_option_runtime_configuration
from options.data.polygon_chain_discovery import (
    PolygonOptionChainDiscoveryPlan,
    PolygonOptionChainDiscoveryResult,
)
from options.data.normalizer import DeveloperOptionNormalizer
from options.data.polygon_developer import MinuteBarCoverage
from options.data.polygon_http import PolygonHttpResponse
from options.domain import (
    AssetType,
    BatchStatus,
    CatalogEligibility,
    ContractType,
    ExerciseStyle,
    OptionContractCatalogEntry,
    PageValidationStatus,
    RawBatchPage,
    RawOptionBatch,
)
from options.errors import OptionProviderError
from realtime.contracts import CANARY_UNIVERSE, UniverseManifest
from realtime.option_discovery_scope import RealtimeOptionDiscoveryScope
from realtime.option_pre_session_producer import OptionPreSessionRequest
from realtime.option_session_manifest import BoundedOptionManifestPolicy
from realtime.polygon_option_discovery import PolygonOptionDiscoverySource
from realtime.polygon_pre_session_source import (
    AdmittedOptionCatalogEntry,
    PolygonOptionPreSessionSource,
    PreSessionBarsResult,
    PreSessionCatalogResult,
    PreSessionSpotResult,
)
from realtime.preflight import DetectorFamily, ProviderTier
from tests.realtime.test_option_session_manifest import (
    _candidates,
    _developer_policy,
    _scope as _manifest_scope,
)


UTC = timezone.utc
MARKET_TIME = datetime(2026, 10, 5, 14, 30, tzinfo=UTC)
OBSERVED_AT = MARKET_TIME + timedelta(seconds=1)
EXPIRATION = date(2026, 10, 26)
SECURITY_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
BACKEND_DIR = Path(__file__).resolve().parents[2]


class QueueHttp:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params):
        self.calls.append((url, dict(params)))
        return self.responses.pop(0)


def _response(payload):
    return PolygonHttpResponse(200, json.dumps(payload).encode("utf-8"))


def _raw(ticker, strike, contract_type):
    side = "C" if contract_type is ContractType.CALL else "P"
    return {
        "details": {
            "ticker": f"O:{ticker}261026{side}{int(strike * 1000):08d}",
        },
        "underlying_asset": {"price": 100},
        "day": {
            "close": 2.5,
            "vwap": 2.4,
            "volume": 50,
            "last_updated": int(MARKET_TIME.timestamp() * 1_000_000_000),
        },
        "open_interest": 200,
        "implied_volatility": 0.3,
        "greeks": {"gamma": 0.02},
        "last_trade": {
            "sip_timestamp": int(MARKET_TIME.timestamp() * 1_000_000_000),
        },
    }


def _catalog(identity, strike, contract_type):
    raw = _raw("AAPL", strike, contract_type)
    ticker = raw["details"]["ticker"]
    return AdmittedOptionCatalogEntry(
        100_000 + identity,
        OptionContractCatalogEntry(
            contract_id=identity,
            contract_ticker=ticker,
            underlyer="AAPL",
            asset_type=AssetType.STOCK,
            provider="polygon",
            provider_version="1",
            contract_type=contract_type,
            expiration_date=EXPIRATION,
            strike=strike,
            exercise_style=ExerciseStyle.AMERICAN,
            shares_per_contract=100,
            primary_exchange="X",
            eligibility_status=CatalogEligibility.VALIDATED_ACTIVE,
            exclusion_reasons=(),
            valid_from=MARKET_TIME - timedelta(days=1),
            valid_to=None,
            first_observed_at=MARKET_TIME - timedelta(days=1),
            revised_observed_at=None,
            payload_sha256=f"{identity:064x}",
        ),
    )


def _configuration():
    return load_option_runtime_configuration(
        {"POLYGON_API_KEY": "fake-only"},
        BACKEND_DIR,
    )


def _scope():
    universe = UniverseManifest("pre-session-source-v1", ("AAPL",), ("AAPL",))
    return RealtimeOptionDiscoveryScope(
        ProviderTier.MASSIVE_DEVELOPER,
        uuid5(NAMESPACE_URL, "pre-session-source-profile"),
        "d" * 64,
        universe,
    )


def _policy():
    return BoundedOptionManifestPolicy(
        version="pre-session-source-policy-v1",
        target_dtes=(21,),
        maximum_target_dte_deviation=0,
        strikes_per_side_per_expiry=1,
        minimum_dte=1,
        maximum_dte=60,
        strike_corridor_fraction=Decimal("0.15"),
        minimum_day_volume=20,
        minimum_open_interest=100,
        maximum_contracts_per_underlying=2,
        maximum_option_quote_contracts=1_000,
        reserved_option_quote_contracts=998,
        required_detectors=(
            DetectorFamily.S1,
            DetectorFamily.S2,
            DetectorFamily.O1,
            DetectorFamily.O3,
        ),
        unavailable_detectors=(DetectorFamily.O2,),
    )


def _request():
    return OptionPreSessionRequest(
        scope=_scope(),
        policy=_policy(),
        session_date=MARKET_TIME.date(),
        scheduled_cycle=MARKET_TIME,
        effective_at=MARKET_TIME - timedelta(minutes=30),
        underlying_security_ids=(("AAPL", SECURITY_ID),),
        maximum_provider_requests=10,
        maximum_response_bytes=1_000_000,
    )


def _normalizer():
    configuration = _configuration()
    return DeveloperOptionNormalizer(
        configuration.policy,
        configuration.valuation_policy,
    )


def _source(http, catalog_entries, bars_override=None):
    configuration = _configuration()
    catalog_calls = []
    bar_calls = []

    def catalog_source(*args):
        catalog_calls.append(args)
        return PreSessionCatalogResult(tuple(catalog_entries), 1, 20)

    def bars_source(*args):
        bar_calls.append(args)
        if bars_override is not None:
            return bars_override(*args)
        return PreSessionBarsResult(
            (
                UnderlyingMinuteBar(
                    Decimal("100"),
                    MARKET_TIME,
                    bar_start=MARKET_TIME - timedelta(minutes=1),
                    raw_payload_text=(
                        '{"o":100,"h":100,"l":100,"c":100,"v":1000}'
                    ),
                ),
            ),
            1,
            30,
        )

    source = PolygonOptionPreSessionSource(
        PolygonOptionDiscoverySource(
            http,
            configuration,
            _scope(),
            clock=lambda: OBSERVED_AT,
        ),
        _normalizer(),
        spot_source=lambda underlyer, as_of: PreSessionSpotResult(
            Decimal("100"), 1, 10
        ),
        catalog_source=catalog_source,
        bars_source=bars_source,
        risk_free_rate=0.04,
        dividend_yields={"AAPL": 0.0},
        clock=lambda: OBSERVED_AT,
    )
    return source, catalog_calls, bar_calls


def _aged(row, delta):
    stamp = int((MARKET_TIME - delta).timestamp() * 1_000_000_000)
    row["day"]["last_updated"] = stamp
    row["last_trade"]["sip_timestamp"] = stamp
    return row


def _coverage(**overrides):
    values = dict(
        requested_from=MARKET_TIME - timedelta(minutes=1),
        requested_to=MARKET_TIME,
        returned_from=MARKET_TIME,
        returned_to=MARKET_TIME,
        bar_count=1,
        pages_fetched=1,
        pagination_complete=True,
        price_basis="ADJUSTED_BAR_START_V1",
    )
    values.update(overrides)
    return MinuteBarCoverage(**values)


def _single_bar_result(coverage):
    return PreSessionBarsResult(
        (
            UnderlyingMinuteBar(
                Decimal("100"),
                MARKET_TIME,
                bar_start=MARKET_TIME - timedelta(minutes=1),
                raw_payload_text='{"o":100,"h":100,"l":100,"c":100,"v":1000}',
            ),
        ),
        1,
        30,
        coverage,
    )


def test_stale_marks_cannot_widen_the_alignment_window_and_stay_auditable():
    fresh = _raw("AAPL", Decimal("99"), ContractType.CALL)
    stale = _aged(_raw("AAPL", Decimal("101"), ContractType.PUT), timedelta(days=150))
    http = QueueHttp([_response({"status": "OK", "results": [fresh, stale]})])
    source, _, bar_calls = _source(
        http,
        (
            _catalog(1, Decimal("99"), ContractType.CALL),
            _catalog(2, Decimal("101"), ContractType.PUT),
        ),
        bars_override=lambda *args: _single_bar_result(_coverage()),
    )

    package = source.collect(_request()).packages[0]

    # Only the fresh October mark bounds the read; the May-dated row does not.
    assert bar_calls[0][1:] == (MARKET_TIME - timedelta(minutes=1), MARKET_TIME)
    diagnostics = package.diagnostics
    assert diagnostics.usable_mark_count == 1
    assert diagnostics.raw_rows == 2
    assert diagnostics.observation_min == MARKET_TIME - timedelta(days=150)
    assert diagnostics.observation_max == MARKET_TIME
    assert dict(diagnostics.rejection_counts).get("STALE_MARK") == 1
    assert {row.catalog.contract_id for row in package.candidates} == {1}


def test_no_usable_marks_skips_the_speculative_fetch_and_records_why():
    old_call = _aged(_raw("AAPL", Decimal("99"), ContractType.CALL), timedelta(days=150))
    old_put = _aged(_raw("AAPL", Decimal("101"), ContractType.PUT), timedelta(days=30))
    http = QueueHttp([_response({"status": "OK", "results": [old_call, old_put]})])
    source, _, bar_calls = _source(
        http,
        (
            _catalog(1, Decimal("99"), ContractType.CALL),
            _catalog(2, Decimal("101"), ContractType.PUT),
        ),
    )

    package = source.collect(_request()).packages[0]

    assert bar_calls == []
    assert package.candidates == ()
    diagnostics = package.diagnostics
    assert diagnostics.bar_acquisition == "SKIPPED_NO_USABLE_MARKS"
    assert diagnostics.usable_mark_count == 0 and diagnostics.bar_count == 0
    assert diagnostics.candidates == 0 and diagnostics.rejection_counts


def test_zero_candidate_diagnostics_always_carry_a_reason():
    http = QueueHttp([_response({"status": "OK", "results": []})])
    source, _, _ = _source(http, ())

    package = source.collect(_request()).packages[0]

    assert package.candidates == ()
    assert dict(package.diagnostics.rejection_counts) == {"NO_RAW_OBSERVATIONS": 1}
    with pytest.raises(ValueError, match="explicit rejection reasons"):
        replace(package.diagnostics, rejection_counts=())


def test_incomplete_or_wrong_basis_bar_coverage_is_rejected_not_used():
    call = _raw("AAPL", Decimal("99"), ContractType.CALL)

    def collect(coverage):
        http = QueueHttp([_response({"status": "OK", "results": [call]})])
        source, _, _ = _source(
            http,
            (_catalog(1, Decimal("99"), ContractType.CALL),),
            bars_override=lambda *args: _single_bar_result(coverage),
        )
        return source.collect(_request())

    assert len(collect(_coverage()).packages[0].candidates) == 1
    with pytest.raises(ValueError, match="incomplete"):
        collect(_coverage(pagination_complete=False))
    with pytest.raises(ValueError, match="price basis does not match"):
        collect(_coverage(price_basis="RAW_FINALIZED_MINUTE_CLOSE_V1"))


def test_paginated_discovery_reuses_stable_admission_and_normalization():
    call = _raw("AAPL", Decimal("99"), ContractType.CALL)
    put = _raw("AAPL", Decimal("101"), ContractType.PUT)
    next_url = "https://api.massive.com/v3/snapshot/options/AAPL?cursor=second"
    first_response = _response({
        "status": "OK",
        "request_id": "page-1",
        "results": [call],
        "next_url": next_url,
    })
    second_response = _response({
        "status": "OK",
        "request_id": "page-2",
        "results": [put],
    })
    http = QueueHttp([first_response, second_response])
    source, catalog_calls, bar_calls = _source(
        http,
        (
            _catalog(1, Decimal("99"), ContractType.CALL),
            _catalog(2, Decimal("101"), ContractType.PUT),
        ),
    )

    collection = source.collect(_request())

    assert collection.provider_requests == 5
    assert collection.response_bytes == (
        len(first_response.body) + len(second_response.body) + 10 + 20 + 30
    )
    assert len(collection.packages) == 1
    package = collection.packages[0]
    assert package.evidence.result.batch.complete is True
    assert len(package.evidence.result.batch.pages) == 2
    assert {row.catalog.contract_id for row in package.candidates} == {1, 2}
    assert all(
        row.snapshot.batch_id == package.evidence.result.batch.batch_id
        for row in package.candidates
    )
    assert all(
        row.discovery_evidence_sha256 == package.evidence.evidence_sha256
        for row in package.candidates
    )
    assert http.calls[1][1] == {"cursor": "second"}
    assert catalog_calls[0][0] == "AAPL"
    assert catalog_calls[0][1:] == (
        MARKET_TIME.date(),
        MARKET_TIME.date() + timedelta(days=60),
        Decimal("85.00"),
        Decimal("115.00"),
    )
    assert bar_calls[0][0] == "AAPL"
    assert bar_calls[0][1:] == (
        MARKET_TIME - timedelta(minutes=1),
        MARKET_TIME,
    )


def test_hostile_pagination_fails_before_catalog_or_bar_boundaries():
    call = _raw("AAPL", Decimal("99"), ContractType.CALL)
    http = QueueHttp([_response({
        "status": "OK",
        "results": [call],
        "next_url": "https://example.com/v3/snapshot/options/AAPL?cursor=stolen",
    })])
    source, catalog_calls, bar_calls = _source(
        http,
        (_catalog(1, Decimal("99"), ContractType.CALL),),
    )

    with pytest.raises(OptionProviderError, match="next page failed validation"):
        source.collect(_request())

    assert catalog_calls == []
    assert bar_calls == []


def test_unadmitted_catalog_identity_fails_before_normalization_and_bars():
    call = _raw("AAPL", Decimal("99"), ContractType.CALL)
    put = _raw("AAPL", Decimal("101"), ContractType.PUT)
    http = QueueHttp([_response({"status": "OK", "results": [call, put]})])
    source, _, bar_calls = _source(
        http,
        (_catalog(1, Decimal("99"), ContractType.CALL),),
    )

    with pytest.raises(ValueError, match="unadmitted catalog identities"):
        source.collect(_request())

    assert bar_calls == []


def test_liquidity_rejection_retains_complete_discovery_but_omits_candidate():
    call = _raw("AAPL", Decimal("99"), ContractType.CALL)
    put = _raw("AAPL", Decimal("101"), ContractType.PUT)
    put["day"]["volume"] = 0
    put["open_interest"] = 0
    http = QueueHttp([_response({"status": "OK", "results": [call, put]})])
    source, _, _ = _source(
        http,
        (
            _catalog(1, Decimal("99"), ContractType.CALL),
            _catalog(2, Decimal("101"), ContractType.PUT),
        ),
    )

    collection = source.collect(_request())

    package = collection.packages[0]
    assert package.evidence.result.batch.row_count == 2
    assert [row.catalog.contract_id for row in package.candidates] == [1]


def test_aggregate_request_bound_fails_closed_after_complete_underlying_package():
    call = _raw("AAPL", Decimal("99"), ContractType.CALL)
    put = _raw("AAPL", Decimal("101"), ContractType.PUT)
    source, _, _ = _source(
        QueueHttp([_response({"status": "OK", "results": [call, put]})]),
        (
            _catalog(1, Decimal("99"), ContractType.CALL),
            _catalog(2, Decimal("101"), ContractType.PUT),
        ),
    )

    with pytest.raises(ValueError, match="source exceeded the request bound"):
        source.collect(replace(_request(), maximum_provider_requests=3))


class ExactUniverseDiscoverySource:
    def __init__(self, scope, rows_by_ticker):
        self.scope = scope
        self.rows_by_ticker = rows_by_ticker

    def discover(self, underlyer, as_of, spot_price):
        policy = _developer_policy()
        plan = PolygonOptionChainDiscoveryPlan.build(
            underlyer,
            as_of,
            as_of.date() + timedelta(days=policy.maximum_dte),
            spot_price * (Decimal("1") - policy.strike_corridor_fraction),
            spot_price * (Decimal("1") + policy.strike_corridor_fraction),
        )
        batch_id = uuid5(NAMESPACE_URL, f"exact-source-batch:{underlyer}")
        response_bytes = json.dumps({
            "status": "OK",
            "request_id": f"exact-{underlyer}",
            "results": self.rows_by_ticker[underlyer],
        }).encode("utf-8")
        page = RawBatchPage(
            batch_id=batch_id,
            page_number=1,
            row_count=len(self.rows_by_ticker[underlyer]),
            response_bytes=response_bytes,
            payload_sha256=hashlib.sha256(response_bytes).hexdigest(),
            received_at=OBSERVED_AT,
            terminal=True,
            validation_status=PageValidationStatus.VALID,
            request_filter_sha256=plan.request_filter_sha256,
            request_id=f"exact-{underlyer}",
            request_metadata=(("path", plan.path),),
        )
        batch = RawOptionBatch(
            batch_id=batch_id,
            provider="polygon",
            underlyer=underlyer,
            scheduled_cycle=as_of,
            request_filter_sha256=plan.request_filter_sha256,
            policy_sha256=policy.sha256,
            status=BatchStatus.COMPLETE,
            pages=(page,),
            started_at=as_of,
            completed_at=OBSERVED_AT,
        )
        return PolygonOptionChainDiscoveryResult(
            batch,
            tuple(self.rows_by_ticker[underlyer]),
            plan.included_ranges,
            plan.excluded_ranges,
        )


def test_exact_developer_universe_emits_complete_normalized_candidate_packages():
    policy = _developer_policy()
    scope = _manifest_scope(ProviderTier.MASSIVE_DEVELOPER, CANARY_UNIVERSE)
    source_candidates = _candidates(
        CANARY_UNIVERSE,
        policy.target_dtes,
        policy.strikes_per_side_per_expiry,
    )
    rows_by_ticker = {}
    catalog_by_ticker = {}
    for ticker in CANARY_UNIVERSE.option_underlyings:
        ticker_candidates = tuple(
            row for row in source_candidates if row.catalog.underlyer == ticker
        )
        rows_by_ticker[ticker] = [
            {
                "details": {"ticker": row.catalog.contract_ticker},
                "underlying_asset": {"price": 100},
                "day": {
                    "close": 2.5,
                    "vwap": 2.4,
                    "volume": 50,
                    "last_updated": int(MARKET_TIME.timestamp() * 1_000_000_000),
                },
                "open_interest": 200,
                "implied_volatility": 0.3,
                "greeks": {"gamma": 0.02},
                "last_trade": {
                    "sip_timestamp": int(MARKET_TIME.timestamp() * 1_000_000_000),
                },
            }
            for row in ticker_candidates
        ]
        catalog_by_ticker[ticker] = tuple(
            AdmittedOptionCatalogEntry(row.catalog_version_id, row.catalog)
            for row in ticker_candidates
        )

    source = PolygonOptionPreSessionSource(
        ExactUniverseDiscoverySource(scope, rows_by_ticker),
        _normalizer(),
        spot_source=lambda underlyer, as_of: PreSessionSpotResult(
            Decimal("100"), 1, 10
        ),
        catalog_source=lambda underlyer, *args: PreSessionCatalogResult(
            catalog_by_ticker[underlyer], 1, 20
        ),
        bars_source=lambda underlyer, start, end: PreSessionBarsResult(
            (UnderlyingMinuteBar(Decimal("100"), MARKET_TIME),), 1, 30
        ),
        risk_free_rate=0.04,
        dividend_yields={
            ticker: 0.0 for ticker in CANARY_UNIVERSE.option_underlyings
        },
        clock=lambda: OBSERVED_AT,
    )
    request = OptionPreSessionRequest(
        scope=scope,
        policy=policy,
        session_date=MARKET_TIME.date(),
        scheduled_cycle=MARKET_TIME,
        effective_at=MARKET_TIME - timedelta(minutes=30),
        underlying_security_ids=tuple(
            (ticker, uuid5(NAMESPACE_URL, f"exact-source-security:{ticker}"))
            for ticker in CANARY_UNIVERSE.option_underlyings
        ),
        maximum_provider_requests=32,
        maximum_response_bytes=10_000_000,
    )

    collection = source.collect(request)

    assert collection.provider_requests == 32
    assert len(collection.packages) == 8
    assert sum(len(package.candidates) for package in collection.packages) == 128
    assert {
        package.evidence.result.batch.underlyer for package in collection.packages
    } == set(CANARY_UNIVERSE.option_underlyings)
