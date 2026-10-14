import json
import inspect
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest
import exchange_calendars

from options.config import load_option_runtime_configuration
from options.data.normalizer import DeveloperOptionNormalizer
from options.data.polygon_http import PolygonHttpResponse
from options.domain import (
    AssetType,
    CatalogEligibility,
    ContractType,
    ExerciseStyle,
    OptionContractCatalogEntry,
)
from realtime.acquisition_handoff import (
    DeveloperEquityBarAcquisitionFact,
    DeveloperOptionSnapshotAcquisitionFact,
)
from realtime.acquisition_producer import (
    DeveloperAcquisitionProducer,
    DeveloperAcquisitionRequest,
)
from realtime.contracts import CANARY_UNIVERSE
from realtime.contracts import UniverseManifest
from realtime.option_discovery_scope import RealtimeOptionDiscoveryScope
from realtime.polygon_developer_sources import (
    DeveloperEquityIdentity,
    DeveloperOptionIdentity,
    PolygonDeveloperEquitySource,
    PolygonDeveloperStrategyBarSource,
    PolygonDeveloperOptionSource,
    PolygonDeveloperSourceLimits,
)
from realtime.polygon_option_discovery import (
    PolygonAdvancedOptionDiscoverySource,
    PolygonDeveloperOptionDiscoverySource,
)
from realtime.preflight import ProviderTier
import realtime.polygon_developer_sources as polygon_developer_sources
from tests.realtime.test_acquisition_producer import _database


UTC = timezone.utc
MARKET_TIME = datetime(2026, 10, 5, 14, 30, tzinfo=UTC)
OBSERVED_AT = MARKET_TIME + timedelta(minutes=15)
COHORT = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
BACKEND_DIR = Path(__file__).resolve().parents[2]
DISCOVERY_PROFILE = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


def _discovery_scope(
    provider_tier=ProviderTier.MASSIVE_DEVELOPER,
    universe=CANARY_UNIVERSE,
):
    return RealtimeOptionDiscoveryScope(
        provider_tier,
        DISCOVERY_PROFILE,
        "d" * 64,
        universe,
    )


def _response(payload, status=200):
    return PolygonHttpResponse(status, json.dumps(payload).encode("utf-8"))


class RoutingHttp:
    def __init__(self, *, omit_option=None, paginate=False):
        self.omit_option = omit_option
        self.paginate = paginate
        self.calls = []

    def get(self, url, params):
        params = dict(params)
        self.calls.append((url, params))
        path = urlsplit(url).path
        if "/v2/aggs/ticker/" in path:
            payload = {
                "status": "DELAYED",
                "results": [{
                    "t": int((MARKET_TIME - timedelta(minutes=1)).timestamp() * 1000),
                    "o": 99,
                    "h": 101,
                    "l": 98,
                    "c": 100,
                    "v": 1000,
                    "vw": 100,
                    "n": 50,
                }],
            }
        elif path.startswith("/v3/snapshot/options/"):
            underlyer = path.rsplit("/", 1)[-1]
            results = [] if underlyer == self.omit_option else [{
                "details": {"ticker": _contract_ticker(underlyer)},
                "underlying_asset": {"price": 100},
                "day": {
                    "close": 2.5,
                    "vwap": 2.4,
                    "volume": 20,
                    "last_updated": int(MARKET_TIME.timestamp() * 1_000_000_000),
                },
                "open_interest": 100,
                "implied_volatility": 0.3,
                "greeks": {"gamma": 0.02},
                "last_trade": {
                    "sip_timestamp": int(MARKET_TIME.timestamp() * 1_000_000_000),
                },
            }]
            payload = {"status": "DELAYED", "results": results}
        else:
            raise AssertionError(f"unexpected fake Polygon request: {url}")
        if self.paginate:
            payload["next_url"] = "https://api.massive.com/v3/next?cursor=x"
        return _response(payload)


class QueueHttp:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params):
        self.calls.append((url, dict(params)))
        return self.responses.pop(0)


class StrategyBarsHttp:
    def __init__(self, *, complete=True):
        self.calls = []
        self.complete = complete

    def get(self, url, params):
        self.calls.append((url, dict(params)))
        calendar = exchange_calendars.get_calendar("XNYS")
        sessions = calendar.sessions_in_range("2026-10-01", "2026-10-02")
        rows = []
        for session in sessions:
            start = calendar.session_open(session).to_pydatetime().astimezone(UTC)
            close = calendar.session_close(session).to_pydatetime().astimezone(UTC)
            cursor = start
            while cursor < close:
                rows.append({
                    "t": int(cursor.timestamp() * 1000),
                    "o": 99,
                    "h": 101,
                    "l": 98,
                    "c": 100,
                    "v": 1000,
                    "vw": 100,
                    "n": 50,
                })
                cursor += timedelta(minutes=30)
        if not self.complete:
            rows = rows[:1]
        return _response({"status": "DELAYED", "results": rows})


def _contract_ticker(ticker):
    return f"O:{ticker}261120C00100000"


def _equity_identities():
    return tuple(
        DeveloperEquityIdentity(
            ticker,
            uuid5(NAMESPACE_URL, f"security:{ticker}"),
            index,
        )
        for index, ticker in enumerate(CANARY_UNIVERSE.equity_tickers, start=1)
    )


def _option_identities():
    rows = []
    for index, ticker in enumerate(CANARY_UNIVERSE.option_underlyings, start=1):
        rows.append(DeveloperOptionIdentity(
            instrument_id=100 + index,
            catalog=OptionContractCatalogEntry(
                contract_id=1_000 + index,
                contract_ticker=_contract_ticker(ticker),
                underlyer=ticker,
                asset_type=(
                    AssetType.ETF if ticker in {"SPY", "QQQ", "IWM"}
                    else AssetType.STOCK
                ),
                provider="polygon",
                provider_version="1",
                contract_type=ContractType.CALL,
                expiration_date=date(2026, 11, 20),
                strike=Decimal("100"),
                exercise_style=ExerciseStyle.AMERICAN,
                shares_per_contract=100,
                primary_exchange="X",
                eligibility_status=CatalogEligibility.VALIDATED_ACTIVE,
                exclusion_reasons=(),
                valid_from=MARKET_TIME - timedelta(days=1),
                valid_to=None,
                first_observed_at=MARKET_TIME - timedelta(days=1),
                revised_observed_at=None,
                payload_sha256=f"{index:064x}",
            ),
            expiration_cutoff=datetime(2026, 11, 20, 21, tzinfo=UTC),
        ))
    return tuple(rows)


def _normalizer():
    configuration = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "fake-only"},
        BACKEND_DIR,
    )
    return DeveloperOptionNormalizer(
        configuration.policy,
        configuration.valuation_policy,
    )


def _request():
    return DeveloperAcquisitionRequest(COHORT, CANARY_UNIVERSE, MARKET_TIME)


def test_equity_source_is_inert_then_builds_exact_final_bar_batches():
    http = RoutingHttp()
    source = PolygonDeveloperEquitySource(
        http,
        _equity_identities(),
        clock=lambda: OBSERVED_AT,
    )

    assert http.calls == []

    result = source.collect(_request())

    assert result.provider == "polygon"
    assert result.provider_requests == 8
    assert len(result.batches) == 8
    facts = tuple(batch.facts[0] for batch in result.batches)
    assert all(isinstance(fact, DeveloperEquityBarAcquisitionFact) for fact in facts)
    assert {fact.revision.ticker for fact in facts} == set(
        CANARY_UNIVERSE.equity_tickers
    )
    assert all(fact.revision.bar_end == MARKET_TIME for fact in facts)
    assert all(fact.observed_at == OBSERVED_AT for fact in facts)
    assert all("apiKey" not in params for _, params in http.calls)


def test_equity_source_accepts_one_completed_minute_window():
    http = RoutingHttp()
    source = PolygonDeveloperEquitySource(
        http,
        _equity_identities(),
        clock=lambda: OBSERVED_AT,
        bar_window=lambda _: (1_000, 1_000),
    )

    source.collect(_request())

    assert all(urlsplit(url).path.endswith("/1000/1000") for url, _ in http.calls)


def test_strategy_bar_source_reuses_stable_normalization_and_derivation():
    http = StrategyBarsHttp()
    source = PolygonDeveloperStrategyBarSource(
        http,
        _equity_identities(),
        clock=lambda: OBSERVED_AT,
        retained_daily_bars=2,
        retained_hourly_bars=2,
        retained_thirty_minute_bars=2,
    )

    result = source.collect(_request())

    assert result.provider_requests == 8
    assert len(result.batches) == 8
    assert sum(len(batch.facts) for batch in result.batches) == 48
    for batch in result.batches:
        intervals = [fact.revision.interval for fact in batch.facts]
        assert intervals.count("1d") == 2
        assert intervals.count("1h") == 2
        assert intervals.count("30m") == 2
        assert all(
            fact.revision.source_kind.value in {"NATIVE_REST", "DERIVED"}
            for fact in batch.facts
        )
    assert all("/range/30/minute/" in url for url, _ in http.calls)
    assert all(params["limit"] == "50000" for _, params in http.calls)


def test_strategy_bar_source_fails_closed_on_incomplete_history():
    source = PolygonDeveloperStrategyBarSource(
        StrategyBarsHttp(complete=False),
        _equity_identities(),
        clock=lambda: OBSERVED_AT,
        retained_daily_bars=2,
        retained_hourly_bars=2,
        retained_thirty_minute_bars=2,
    )

    with pytest.raises(ValueError, match="lookback coverage is incomplete"):
        source.collect(_request())


def test_option_source_uses_shared_normalizer_for_exact_admitted_contracts():
    http = RoutingHttp()
    source = PolygonDeveloperOptionSource(
        http,
        _option_identities(),
        _normalizer(),
        risk_free_rate=0.04,
        dividend_yields={ticker: 0.0 for ticker in CANARY_UNIVERSE.option_underlyings},
        clock=lambda: OBSERVED_AT,
    )

    assert http.calls == []

    result = source.collect(_request())

    assert result.provider == "polygon"
    assert result.provider_requests == 16
    assert len(result.batches) == 8
    facts = tuple(batch.facts[0] for batch in result.batches)
    assert all(isinstance(fact, DeveloperOptionSnapshotAcquisitionFact) for fact in facts)
    assert {fact.snapshot.underlyer for fact in facts} == set(
        CANARY_UNIVERSE.option_underlyings
    )
    assert all(fact.snapshot.model_mark == Decimal("2.5") for fact in facts)
    assert all(fact.snapshot.bid is None and fact.snapshot.ask is None for fact in facts)
    assert all(fact.snapshot.provider == "polygon" for fact in facts)
    for index in range(0, len(http.calls), 2):
        assert urlsplit(http.calls[index][0]).path.startswith("/v3/snapshot/options/")
        assert "/v2/aggs/ticker/" in urlsplit(http.calls[index + 1][0]).path


def test_option_source_fails_closed_when_admitted_contract_is_missing():
    source = PolygonDeveloperOptionSource(
        RoutingHttp(omit_option="AAPL"),
        _option_identities(),
        _normalizer(),
        risk_free_rate=0.04,
        dividend_yields={ticker: 0.0 for ticker in CANARY_UNIVERSE.option_underlyings},
        clock=lambda: OBSERVED_AT,
    )

    with pytest.raises(ValueError, match="omitted an admitted contract"):
        source.collect(_request())


def test_realtime_discovery_uses_shared_complete_60_dte_adapter_without_persistence():
    configuration = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "fake-only"},
        BACKEND_DIR,
    )
    next_url = (
        "https://api.massive.com/v3/snapshot/options/AAPL?cursor=second"
    )
    http = QueueHttp([
        _response({"status": "DELAYED", "results": [{"details": {"ticker": "O:1"}}], "next_url": next_url}),
        _response({"status": "DELAYED", "results": [{"details": {"ticker": "O:2"}}]}),
    ])
    source = PolygonDeveloperOptionDiscoverySource(
        http,
        configuration,
        _discovery_scope(),
        clock=lambda: OBSERVED_AT,
    )

    assert http.calls == []

    result = source.discover("AAPL", MARKET_TIME, Decimal("100"))

    assert result.batch.complete is True
    assert result.batch.row_count == 2
    assert result.batch.completed_at == OBSERVED_AT
    assert http.calls[0][1]["expiration_date.gte"] == "2026-10-05"
    assert http.calls[0][1]["expiration_date.lte"] == "2026-12-04"
    assert http.calls[0][1]["strike_price.gte"] == "85.00"
    assert http.calls[0][1]["strike_price.lte"] == "115.00"
    assert http.calls[1][1] == {"cursor": "second"}
    assert type(source.adapter).__module__ == "options.data.polygon_chain_discovery"


def test_advanced_discovery_reuses_shared_adapter_for_reviewed_expanded_universe():
    configuration = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "fake-only"},
        BACKEND_DIR,
    )
    universe = UniverseManifest(
        "advanced-expanded-test-v1",
        CANARY_UNIVERSE.equity_tickers + ("AVGO",),
        CANARY_UNIVERSE.option_underlyings + ("AVGO",),
    )
    http = QueueHttp([_response({"status": "OK", "results": []})])
    source = PolygonAdvancedOptionDiscoverySource(
        http,
        configuration,
        _discovery_scope(ProviderTier.MASSIVE_ADVANCED, universe),
        clock=lambda: OBSERVED_AT,
    )

    result = source.discover("AVGO", MARKET_TIME, Decimal("350"))

    assert result.batch.complete is True
    assert result.batch.underlyer == "AVGO"
    assert urlsplit(http.calls[0][0]).path.endswith("/AVGO")
    assert source.scope.provider_tier is ProviderTier.MASSIVE_ADVANCED


def test_tier_specific_discovery_facades_reject_scope_relabelling():
    configuration = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "fake-only"},
        BACKEND_DIR,
    )

    with pytest.raises(ValueError, match="Developer tier"):
        PolygonDeveloperOptionDiscoverySource(
            QueueHttp([]),
            configuration,
            _discovery_scope(ProviderTier.MASSIVE_ADVANCED),
        )


def test_realtime_discovery_composition_has_no_database_or_canonical_fallback_import():
    source = inspect.getsource(polygon_developer_sources)

    assert "options.repositories" not in source
    assert "realtime.database" not in source
    assert "backend.database" not in source
    assert "from database" not in source
    assert "from main" not in source


def test_source_rejects_unapproved_host_pagination_and_response_overflow():
    with pytest.raises(ValueError, match="approved HTTPS host"):
        PolygonDeveloperEquitySource(
            RoutingHttp(),
            _equity_identities(),
            base_url="https://example.com",
        )

    paginated = PolygonDeveloperEquitySource(
        RoutingHttp(paginate=True),
        _equity_identities(),
        clock=lambda: OBSERVED_AT,
    )
    with pytest.raises(ValueError, match="requires pagination"):
        paginated.collect(_request())

    overflow = PolygonDeveloperEquitySource(
        RoutingHttp(),
        _equity_identities(),
        limits=PolygonDeveloperSourceLimits(maximum_response_bytes=10),
        clock=lambda: OBSERVED_AT,
    )
    with pytest.raises(ValueError, match="byte bound"):
        overflow.collect(_request())


def test_fake_http_adapters_flow_through_one_ingress_handoff_transaction():
    http = RoutingHttp()
    equity = PolygonDeveloperEquitySource(
        http,
        _equity_identities(),
        clock=lambda: OBSERVED_AT,
    )
    options = PolygonDeveloperOptionSource(
        http,
        _option_identities(),
        _normalizer(),
        risk_free_rate=0.04,
        dividend_yields={ticker: 0.0 for ticker in CANARY_UNIVERSE.option_underlyings},
        clock=lambda: OBSERVED_AT,
    )
    database, connection, cursor = _database()
    persisted = []

    result = DeveloperAcquisitionProducer(equity, options).run_once(
        database,
        _request(),
        repository=lambda active_cursor, batch: persisted.append(
            (active_cursor, batch)
        ),
    )

    assert len(http.calls) == result.provider_requests == 24
    assert result.equity_fact_count == result.option_fact_count == 8
    assert len(persisted) == len(result.acquisition_batch_ids) == 16
    assert all(active_cursor is cursor for active_cursor, _ in persisted)
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0
