import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics import UnderlyingMinuteBar
from options.config import load_option_runtime_configuration
from options.data.normalizer import (
    DeveloperNormalizationInput,
    DeveloperOptionNormalizer,
    parse_polygon_snapshot,
)
from options.domain import (
    AssetType,
    CatalogEligibility,
    ContractType,
    ExerciseStyle,
    OptionContractCatalogEntry,
)


UTC = timezone.utc
MARKET_TIME = datetime(2026, 8, 31, 14, 0, tzinfo=UTC)
OBSERVED_AT = MARKET_TIME + timedelta(minutes=15)


def _catalog():
    return OptionContractCatalogEntry(
        contract_id=42,
        contract_ticker="O:SPY260904C00650000",
        underlyer="SPY",
        asset_type=AssetType.ETF,
        provider="polygon",
        provider_version="1",
        contract_type=ContractType.CALL,
        expiration_date=date(2026, 9, 4),
        strike=Decimal("100"),
        exercise_style=ExerciseStyle.AMERICAN,
        shares_per_contract=100,
        primary_exchange="X",
        eligibility_status=CatalogEligibility.VALIDATED_ACTIVE,
        exclusion_reasons=(),
        valid_from=MARKET_TIME,
        valid_to=None,
        first_observed_at=MARKET_TIME,
        revised_observed_at=None,
        payload_sha256="a" * 64,
    )


def _payload(**overrides):
    payload = {
        "details": {"ticker": "O:SPY260904C00650000"},
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
        "last_quote": {"bid": 2.45, "ask": 2.55},
    }
    payload.update(overrides)
    return payload


def _normalizer():
    configuration = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "test-secret"}, BACKEND_DIR
    )
    return DeveloperOptionNormalizer(configuration.policy)


def test_developer_fixture_maps_every_snapshot_field_without_fabricating_quotes():
    raw = parse_polygon_snapshot(_payload(), OBSERVED_AT)
    item = DeveloperNormalizationInput(
        raw=raw,
        catalog=_catalog(),
        underlying_bars=(UnderlyingMinuteBar(Decimal("100"), MARKET_TIME),),
        expiration_cutoff=datetime(2026, 9, 4, 20, 0, tzinfo=UTC),
        risk_free_rate=0.04,
        dividend_yield=0.0,
    )

    result = _normalizer().normalize(uuid4(), (item,))

    assert result.received_count == 1
    assert result.retained_count == 1
    assert result.strategy_eligible is True
    snapshot = result.snapshots[0]
    assert snapshot.contract_id == 42
    assert snapshot.model_mark == Decimal("2.5")
    assert snapshot.bid is None and snapshot.ask is None and snapshot.midpoint is None
    assert snapshot.local_iv is not None and snapshot.iv_converged is True
    assert snapshot.provider_iv == 0.3
    assert snapshot.provider_gamma == 0.02
    assert snapshot.risk_free_rate == 0.04
    assert snapshot.raw_payload_sha256 == raw.raw_payload_sha256
    assert len(snapshot.normalized_payload_sha256) == 64


def test_missing_optional_provider_greeks_remain_null_diagnostics():
    raw = parse_polygon_snapshot(_payload(implied_volatility=None, greeks={}), OBSERVED_AT)
    item = DeveloperNormalizationInput(
        raw,
        _catalog(),
        (UnderlyingMinuteBar(Decimal("100"), MARKET_TIME),),
        datetime(2026, 9, 4, 20, 0, tzinfo=UTC),
        0.04,
        0.0,
    )

    snapshot = _normalizer().normalize(uuid4(), (item,)).snapshots[0]

    assert snapshot.provider_iv is None
    assert snapshot.provider_gamma is None


def test_source_skew_over_sixty_seconds_retains_diagnostic_but_blocks_strategy():
    raw = parse_polygon_snapshot(_payload(), OBSERVED_AT)
    item = DeveloperNormalizationInput(
        raw,
        _catalog(),
        (UnderlyingMinuteBar(Decimal("100"), MARKET_TIME - timedelta(seconds=61)),),
        datetime(2026, 9, 4, 20, 0, tzinfo=UTC),
        0.04,
        0.0,
    )

    result = _normalizer().normalize(uuid4(), (item,))

    assert result.snapshots[0].model_mark is None
    assert result.snapshots[0].iv_failure_reason == "MODEL_MARK_UNAVAILABLE"
    assert result.strategy_eligible is False


def test_duplicate_delivery_and_input_order_do_not_change_normalized_facts():
    first = parse_polygon_snapshot(_payload(), OBSERVED_AT)
    revised = parse_polygon_snapshot(
        _payload(day={**_payload()["day"], "close": 2.6}),
        OBSERVED_AT + timedelta(minutes=1),
        OBSERVED_AT + timedelta(minutes=1),
    )

    def item(raw):
        return DeveloperNormalizationInput(
            raw,
            _catalog(),
            (UnderlyingMinuteBar(Decimal("100"), MARKET_TIME),),
            datetime(2026, 9, 4, 20, 0, tzinfo=UTC),
            0.04,
            0.0,
        )

    batch_id = uuid4()
    forward = _normalizer().normalize(batch_id, (item(first), item(first), item(revised)))
    replay = _normalizer().normalize(batch_id, (item(revised), item(first)))

    assert len(forward.snapshots) == 2
    assert [row.normalized_payload_sha256 for row in forward.snapshots] == [
        row.normalized_payload_sha256 for row in replay.snapshots
    ]
    assert forward.matrix_snapshots[0].normalized_payload_sha256 == (
        replay.matrix_snapshots[0].normalized_payload_sha256
    )


def test_normalized_snapshot_is_not_visible_before_all_inputs_are_available():
    raw = parse_polygon_snapshot(_payload(), OBSERVED_AT)
    sealed_at = OBSERVED_AT + timedelta(minutes=1)
    item = DeveloperNormalizationInput(
        raw,
        _catalog(),
        (UnderlyingMinuteBar(Decimal("100"), MARKET_TIME),),
        datetime(2026, 9, 4, 20, 0, tzinfo=UTC),
        0.04,
        0.0,
        normalized_observed_at=sealed_at,
    )

    snapshot = _normalizer().normalize(uuid4(), (item,)).snapshots[0]

    assert snapshot.first_observed_at == sealed_at