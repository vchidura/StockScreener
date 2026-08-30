import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.data import (
    BaseDataEngine,
    OptionsTradeSource,
    PolygonDeveloperEngine,
    build_data_engine,
    validate_developer_capabilities,
)
from options.config import load_option_runtime_configuration
from options.domain import DataCapability, SpotPrice


def test_developer_capability_boundary_requires_delayed_data_without_quotes():
    validate_developer_capabilities(
        {
            DataCapability.CHAIN_SNAPSHOT,
            DataCapability.OPTION_TRADES,
            DataCapability.UNDERLYING_PRICE,
        }
    )

    with pytest.raises(RuntimeError, match="unexpectedly includes: OPTION_QUOTES"):
        validate_developer_capabilities(
            {
                DataCapability.CHAIN_SNAPSHOT,
                DataCapability.OPTION_TRADES,
                DataCapability.OPTION_QUOTES,
                DataCapability.UNDERLYING_PRICE,
            }
        )
    with pytest.raises(RuntimeError, match="missing: OPTION_TRADES"):
        validate_developer_capabilities(
            {DataCapability.CHAIN_SNAPSHOT, DataCapability.UNDERLYING_PRICE}
        )


def test_spot_price_requires_decimal_and_causal_timestamps():
    market_time = datetime(2026, 8, 29, 20, 0, tzinfo=timezone.utc)
    spot = SpotPrice("SPY", "polygon_stocks", Decimal("650.25"), market_time, market_time)
    assert spot.price == Decimal("650.25")

    with pytest.raises(TypeError, match="price must be Decimal"):
        SpotPrice("SPY", "polygon_stocks", 650.25, market_time, market_time)


def test_provider_interfaces_remain_abstract():
    with pytest.raises(TypeError):
        BaseDataEngine()
    with pytest.raises(TypeError):
        OptionsTradeSource()


def test_data_engine_factory_builds_only_implemented_developer_adapter():
    configuration = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "test-secret"}, BACKEND_DIR
    )

    engine = build_data_engine(configuration)

    assert isinstance(engine, PolygonDeveloperEngine)