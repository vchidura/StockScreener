import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.config import load_option_runtime_configuration
from options.domain import (
    AssetType,
    CatalogEligibility,
    ContractType,
    ExerciseStyle,
    MarkSource,
    OptionContractCatalogEntry,
    OptionContractSnapshot,
    OptionTradeCursor,
    OptionTradeFetchResult,
)
from options.orchestration import ManualOptionPipeline

UTC = timezone.utc
MARKET_TIME = datetime(2026, 9, 4, 19, 45, tzinfo=UTC)
OBSERVED_TIME = datetime(2026, 9, 4, 20, 0, tzinfo=UTC)
BATCH = uuid4()


def _configuration(**overrides):
    environ = {"POLYGON_API_KEY": "test-secret"}
    environ.update(overrides)
    return load_option_runtime_configuration(environ, BACKEND_DIR)


def _snapshot(contract_id, contract_type, strike, day_volume, open_interest=100):
    return OptionContractSnapshot(
        snapshot_id=uuid4(), contract_id=contract_id,
        contract_ticker=f"O:SPY{contract_id:08d}", underlyer="SPY", provider="polygon",
        contract_type=contract_type, expiration_date=date(2026, 9, 18),
        expiration_cutoff=datetime(2026, 9, 18, 20, 0, tzinfo=UTC),
        calendar_dte=14, time_to_expiration_years=14 / 365,
        strike=Decimal(str(strike)), shares_per_contract=100,
        exercise_style=ExerciseStyle.AMERICAN, spot=Decimal("100"),
        spot_market_data_time=MARKET_TIME, bid=None, ask=None, midpoint=None,
        display_mark=Decimal("1.50"), model_mark=Decimal("1.50"),
        mark_market_data_time=MARKET_TIME,
        mark_source=MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE,
        day_volume=day_volume, open_interest=open_interest,
        market_data_time=MARKET_TIME, first_observed_at=OBSERVED_TIME,
        revised_observed_at=None, local_iv=0.25, local_gamma=0.02, local_delta=0.5,
        local_theta_per_day=-0.01, local_vega_per_vol_point=0.1,
        local_rho_per_rate_point=0.001, intrinsic_value=Decimal("0"),
        extrinsic_value=Decimal("1.50"), single_contract_breakeven=Decimal("101.50"),
        provider_iv=None, provider_gamma=None, risk_free_rate=0.04, dividend_yield=0.0,
        iv_converged=True, iv_solver="NEWTON", iv_iteration_count=3,
        iv_price_error=1e-9, iv_failure_reason=None, model_version="bs-1",
        quality_flags=(), batch_id=BATCH, raw_payload_sha256="a" * 64,
        normalized_payload_sha256="b" * 64, revision=1,
    )


def _catalog_entry(contract_id, strike, contract_type=ContractType.CALL):
    return OptionContractCatalogEntry(
        contract_id=contract_id, contract_ticker=f"O:SPY{contract_id:08d}",
        underlyer="SPY", asset_type=AssetType.ETF, provider="polygon",
        provider_version=None, contract_type=contract_type,
        expiration_date=date(2026, 9, 18), strike=Decimal(str(strike)),
        exercise_style=ExerciseStyle.AMERICAN, shares_per_contract=100,
        primary_exchange=None, eligibility_status=CatalogEligibility.VALIDATED_ACTIVE,
        exclusion_reasons=(), valid_from=OBSERVED_TIME, valid_to=None,
        first_observed_at=OBSERVED_TIME, revised_observed_at=None,
        payload_sha256="c" * 64,
    )


def _chain():
    """Two OTM calls with volume, one ITM call, one OTM put, one zero-volume call."""
    snapshots = (
        _snapshot(1, ContractType.CALL, 105, 900),
        _snapshot(2, ContractType.CALL, 110, 400),
        _snapshot(3, ContractType.CALL, 95, 5000),
        _snapshot(4, ContractType.PUT, 105, 5000),
        _snapshot(5, ContractType.CALL, 115, 0),
    )
    catalog = {
        snapshot.contract_ticker: _catalog_entry(
            snapshot.contract_id, snapshot.strike, snapshot.contract_type
        )
        for snapshot in snapshots
    }
    return snapshots, catalog


def _pipeline(configuration, engine=None, trades=None):
    pipeline = ManualOptionPipeline.__new__(ManualOptionPipeline)
    pipeline.configuration = configuration
    pipeline.engine = engine or MagicMock()
    pipeline.trade_repository = trades or MagicMock()
    return pipeline


def test_watchlist_keeps_only_otm_calls_with_observed_volume():
    snapshots, catalog = _chain()
    pipeline = _pipeline(_configuration())
    selected = pipeline._trade_watchlist(snapshots, catalog)
    assert [entry.contract_id for entry in selected] == [1, 2]


def test_watchlist_ranks_by_day_volume_then_open_interest():
    snapshots, catalog = _chain()
    pipeline = _pipeline(_configuration())
    selected = pipeline._trade_watchlist(snapshots, catalog)
    assert selected[0].contract_id == 1
    assert selected[1].contract_id == 2


def test_watchlist_respects_the_configured_cap():
    snapshots, catalog = _chain()
    pipeline = _pipeline(
        _configuration(
            POLYGON_API_KEY="test-secret", OPTION_TRADE_WATCHLIST_PER_UNDERLYER="1"
        )
    )
    assert len(pipeline._trade_watchlist(snapshots, catalog)) == 1


def test_ingestion_is_disabled_by_default():
    snapshots, catalog = _chain()
    engine = MagicMock()
    pipeline = _pipeline(_configuration(), engine=engine)
    result = pipeline._ingest_trades(snapshots, catalog, MARKET_TIME)
    assert result.reasons == ("TRADE_INGESTION_DISABLED",)
    assert result.watchlist_count == 0
    engine.get_option_trades.assert_not_called()


def _enabled():
    return _configuration(
        POLYGON_API_KEY="test-secret",
        OPTION_TRADE_INGESTION_ENABLED="true",
        OPTION_TRADE_WATCHLIST_PER_UNDERLYER="2",
    )


def _fetch_result(events, complete=True):
    return OptionTradeFetchResult(
        raw_batch_id=uuid4(), events=tuple(events),
        request_ids=("req-1",), complete=complete,
        terminal_page_received=True,
    )


def _event(contract_id, seconds, sequence):
    event = MagicMock()
    event.sip_timestamp = MARKET_TIME - timedelta(seconds=seconds)
    event.sequence_number = sequence
    event.contract_id = contract_id
    return event


def test_enabled_ingestion_requests_each_watchlist_contract():
    snapshots, catalog = _chain()
    engine = MagicMock()
    engine.get_option_trades.return_value = _fetch_result([])
    trades = MagicMock()
    trades.get_cursor.return_value = None
    pipeline = _pipeline(_enabled(), engine=engine, trades=trades)
    result = pipeline._ingest_trades(snapshots, catalog, MARKET_TIME)
    assert result.watchlist_count == 2
    assert result.requested_count == 2
    assert engine.get_option_trades.call_count == 2


def test_lookback_window_is_applied_to_the_request():
    snapshots, catalog = _chain()
    engine = MagicMock()
    engine.get_option_trades.return_value = _fetch_result([])
    trades = MagicMock()
    trades.get_cursor.return_value = None
    configuration = _configuration(
        POLYGON_API_KEY="test-secret",
        OPTION_TRADE_INGESTION_ENABLED="true",
        OPTION_TRADE_LOOKBACK_SECONDS="600",
    )
    pipeline = _pipeline(configuration, engine=engine, trades=trades)
    pipeline._ingest_trades(snapshots, catalog, MARKET_TIME)
    args = engine.get_option_trades.call_args[0]
    assert args[1] == MARKET_TIME - timedelta(seconds=600)
    assert args[2] == MARKET_TIME


def test_persisted_events_advance_the_cursor_to_the_latest_print():
    snapshots, catalog = _chain()
    engine = MagicMock()
    engine.get_option_trades.return_value = _fetch_result(
        [_event(1, 300, 5), _event(1, 10, 99), _event(1, 120, 7)]
    )
    trades = MagicMock()
    trades.get_cursor.return_value = None
    trades.persist.return_value = 3
    pipeline = _pipeline(_enabled(), engine=engine, trades=trades)
    result = pipeline._ingest_trades(snapshots, catalog, MARKET_TIME)
    assert result.persisted_count == 6
    advanced = trades.advance_cursor.call_args[0]
    assert advanced[2] == MARKET_TIME - timedelta(seconds=10)
    assert advanced[3] == 99


def test_incomplete_fetch_does_not_advance_the_cursor():
    snapshots, catalog = _chain()
    engine = MagicMock()
    engine.get_option_trades.return_value = _fetch_result(
        [_event(1, 30, 4)], complete=False
    )
    trades = MagicMock()
    trades.get_cursor.return_value = None
    trades.persist.return_value = 1
    pipeline = _pipeline(_enabled(), engine=engine, trades=trades)
    result = pipeline._ingest_trades(snapshots, catalog, MARKET_TIME)
    trades.advance_cursor.assert_not_called()
    assert "TRADE_FETCH_INCOMPLETE" in result.reasons


def test_existing_cursor_is_passed_back_to_the_provider():
    snapshots, catalog = _chain()
    cursor = OptionTradeCursor(
        sip_timestamp=MARKET_TIME - timedelta(minutes=5),
        sequence_number=12,
        overlap_seconds=60,
    )
    engine = MagicMock()
    engine.get_option_trades.return_value = _fetch_result([])
    trades = MagicMock()
    trades.get_cursor.return_value = cursor
    pipeline = _pipeline(_enabled(), engine=engine, trades=trades)
    pipeline._ingest_trades(snapshots, catalog, MARKET_TIME)
    assert engine.get_option_trades.call_args[0][3] is cursor


def test_provider_failure_is_reason_coded_and_does_not_abort_the_matrix():
    snapshots, catalog = _chain()
    engine = MagicMock()
    engine.get_option_trades.side_effect = RuntimeError("provider down")
    trades = MagicMock()
    trades.get_cursor.return_value = None
    pipeline = _pipeline(_enabled(), engine=engine, trades=trades)
    result = pipeline._ingest_trades(snapshots, catalog, MARKET_TIME)
    assert result.requested_count == 0
    assert result.persisted_count == 0
    assert any(reason.startswith("TRADE_FETCH_FAILED") for reason in result.reasons)


def test_empty_watchlist_is_reason_coded():
    engine = MagicMock()
    trades = MagicMock()
    pipeline = _pipeline(_enabled(), engine=engine, trades=trades)
    result = pipeline._ingest_trades((), {}, MARKET_TIME)
    assert result.reasons == ("NO_TRADE_WATCHLIST_CONTRACT",)
    engine.get_option_trades.assert_not_called()


def test_settings_stay_out_of_the_configuration_fingerprint():
    baseline = _configuration()
    tuned = _configuration(
        POLYGON_API_KEY="test-secret",
        OPTION_TRADE_INGESTION_ENABLED="true",
        OPTION_TRADE_WATCHLIST_PER_UNDERLYER="40",
        OPTION_TRADE_LOOKBACK_SECONDS="900",
    )
    assert tuned.configuration_sha256 == baseline.configuration_sha256
    assert tuned.policy_sha256 == baseline.policy_sha256
    assert tuned.strategy_policy_sha256 == baseline.strategy_policy_sha256
