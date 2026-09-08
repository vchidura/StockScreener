import sys
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics.gamma_exposure import GammaExposureInput, build_gamma_profile
from options.domain import ContractType, DealerConvention, GammaScope
from options.repositories.gamma import (
    READ_QUERIES,
    SQL_INSERT_PROFILE,
    GammaProfileRecord,
    OptionGammaProfileRepository,
    curve_payload,
)

UTC = timezone.utc
POLICY_HASH = "b" * 64


def _connection_factory(cursor):
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    return factory, connection


def _profile(convention=DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS):
    rows = tuple(
        GammaExposureInput(
            contract_id=index,
            contract_type=contract_type,
            expiration_date=date(2026, 10, 2),
            strike=Decimal(str(strike)),
            open_interest=400 + strike,
            local_iv=0.25,
            time_to_expiration_years=0.08,
            risk_free_rate=0.04,
            dividend_yield=0.0,
        )
        for index, (strike, contract_type) in enumerate(
            (strike, contract_type)
            for strike in range(95, 106)
            for contract_type in (ContractType.CALL, ContractType.PUT)
        )
    )
    return build_gamma_profile(
        rows,
        Decimal("100"),
        convention=convention,
        minimum_contracts_for_profile=1,
    )


def _record(scope=GammaScope.TOTAL, matrix_id=None):
    return GammaProfileRecord(
        matrix_id=matrix_id or uuid4(),
        underlying="SPY",
        scope=scope,
        market_data_time=datetime(2026, 9, 4, 20, 0, tzinfo=UTC),
        first_observed_at=datetime(2026, 9, 4, 20, 15, tzinfo=UTC),
        profile=_profile(),
        gamma_policy_version="gamma_v1",
        gamma_policy_sha256=POLICY_HASH,
    )


def test_profile_id_is_deterministic_across_identical_records():
    matrix_id = uuid4()
    first = _record(matrix_id=matrix_id)
    second = _record(matrix_id=matrix_id)
    assert first.gamma_profile_id == second.gamma_profile_id


def test_profile_id_separates_scope_and_policy():
    matrix_id = uuid4()
    total = _record(scope=GammaScope.TOTAL, matrix_id=matrix_id)
    zero_dte = _record(scope=GammaScope.ZERO_DTE, matrix_id=matrix_id)
    assert total.gamma_profile_id != zero_dte.gamma_profile_id

    other_policy = GammaProfileRecord(
        matrix_id=matrix_id,
        underlying="SPY",
        scope=GammaScope.TOTAL,
        market_data_time=total.market_data_time,
        first_observed_at=total.first_observed_at,
        profile=total.profile,
        gamma_policy_version="gamma_v2",
        gamma_policy_sha256="c" * 64,
    )
    assert other_policy.gamma_profile_id != total.gamma_profile_id


def test_persist_is_idempotent_by_conflict_target():
    cursor = MagicMock()
    cursor.closed = False
    factory, _ = _connection_factory(cursor)
    repository = OptionGammaProfileRepository(factory)
    assert repository.persist([_record()]) == 1
    statement = cursor.execute.call_args[0][0]
    assert "ON CONFLICT (matrix_id, scope, gamma_policy_sha256) DO NOTHING" in statement


def test_persist_of_empty_sequence_does_no_work():
    cursor = MagicMock()
    cursor.closed = False
    factory, connection = _connection_factory(cursor)
    repository = OptionGammaProfileRepository(factory)
    assert repository.persist([]) == 0
    connection.cursor.assert_not_called()


def test_insert_parameter_count_matches_placeholders():
    cursor = MagicMock()
    cursor.closed = False
    factory, _ = _connection_factory(cursor)
    OptionGammaProfileRepository(factory).persist([_record()])
    statement, parameters = cursor.execute.call_args[0]
    assert statement.count("%s") == len(parameters)


def test_persisted_strike_count_matches_curve_length():
    cursor = MagicMock()
    cursor.closed = False
    factory, _ = _connection_factory(cursor)
    record = _record()
    OptionGammaProfileRepository(factory).persist([record])
    _, parameters = cursor.execute.call_args[0]
    strike_count = parameters[25]
    curve = parameters[29].adapted
    assert strike_count == len(curve) == len(record.profile.strikes)


def test_curve_payload_preserves_exact_strikes_as_text():
    profile = _profile()
    payload = curve_payload(profile.strikes)
    assert payload[0]["strike"] == "95"
    assert all(isinstance(row["strike"], str) for row in payload)
    assert {row["strike"] for row in payload} == {
        str(strike) for strike in range(95, 106)
    }


def test_curve_payload_keeps_calls_and_puts_unsigned():
    payload = curve_payload(_profile().strikes)
    assert all(row["call_gamma_notional_per_percent"] >= 0 for row in payload)
    assert all(row["put_gamma_notional_per_percent"] >= 0 for row in payload)


def test_board_read_excludes_the_jsonb_curve():
    # The common portal query must never pull strike_profile into the plan.
    assert "strike_profile" not in READ_QUERIES["latest_by_underlying"]
    assert "strike_profile" in READ_QUERIES["curve_by_matrix"]


def test_read_queries_filter_on_the_policy_hash():
    for name, sql in READ_QUERIES.items():
        assert "gamma_policy_sha256 = %s" in sql, name


def test_explain_rejects_uncatalogued_queries():
    cursor = MagicMock()
    cursor.closed = False
    factory, _ = _connection_factory(cursor)
    repository = OptionGammaProfileRepository(factory)
    with pytest.raises(KeyError):
        repository.explain("drop_everything", ())
    assert "INSERT" not in " ".join(READ_QUERIES.values()).upper()


def test_explain_wraps_the_catalogued_query_and_parses_json():
    cursor = MagicMock()
    cursor.closed = False
    cursor.fetchone.return_value = ([{"Plan": {"Node Type": "Index Scan"}}],)
    factory, _ = _connection_factory(cursor)
    repository = OptionGammaProfileRepository(factory)
    result = repository.explain(
        "latest_by_underlying", (POLICY_HASH, "TOTAL", "SPY", "SPY")
    )
    statement = cursor.execute.call_args[0][0]
    assert statement.startswith("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)")
    assert READ_QUERIES["latest_by_underlying"] in statement
    assert result["query_name"] == "latest_by_underlying"
    assert result["plan"]["Plan"]["Node Type"] == "Index Scan"


def test_explain_without_analyze_does_not_execute_the_statement():
    cursor = MagicMock()
    cursor.closed = False
    cursor.fetchone.return_value = ([{"Plan": {}}],)
    factory, _ = _connection_factory(cursor)
    repository = OptionGammaProfileRepository(factory)
    repository.explain("regime_history", (POLICY_HASH, None, None, 10), analyze=False)
    statement = cursor.execute.call_args[0][0]
    assert statement.startswith("EXPLAIN (FORMAT JSON)")
    assert "ANALYZE" not in statement


def test_insert_statement_is_not_reachable_through_the_read_catalog():
    assert SQL_INSERT_PROFILE not in READ_QUERIES.values()


def test_rehydrated_profile_round_trips_the_persisted_row():
    from options.repositories.gamma import _insert_parameters, rehydrate_profile

    record = _record()
    parameters = _insert_parameters(record)
    row = {
        "scope": parameters[3],
        "spot": parameters[6],
        "dealer_convention": parameters[7],
        "volatility_assumption": parameters[8],
        "shares_per_contract": parameters[9],
        "net_gamma_shares_per_point": parameters[12],
        "net_gamma_notional_per_percent": parameters[13],
        "absolute_gamma_notional_per_percent": parameters[14],
        "call_gamma_notional_per_percent": parameters[15],
        "put_gamma_notional_per_percent": parameters[16],
        "flip_spot": parameters[17],
        "regime_at_spot": parameters[18],
        "sign_change_count": parameters[19],
        "flip_search_low_spot": parameters[20],
        "flip_search_high_spot": parameters[21],
        "flip_grid_points": parameters[22],
        "flip_reasons": parameters[23],
        "peak_gamma_strike": parameters[24],
        "strike_count": parameters[25],
        "contributing_contract_count": parameters[26],
        "eligible_contract_count": parameters[27],
        "coverage_fraction": parameters[28],
        "strike_profile": parameters[29].adapted,
        "quality_reasons": parameters[30],
    }
    scoped = rehydrate_profile(row)
    assert scoped.scope is record.scope
    assert scoped.profile == record.profile


def test_rehydration_accepts_a_json_encoded_curve():
    from options.repositories.gamma import _insert_parameters, rehydrate_profile
    import json as json_module

    record = _record()
    parameters = _insert_parameters(record)
    row = {
        "scope": parameters[3],
        "spot": parameters[6],
        "dealer_convention": parameters[7],
        "volatility_assumption": parameters[8],
        "shares_per_contract": parameters[9],
        "net_gamma_shares_per_point": parameters[12],
        "net_gamma_notional_per_percent": parameters[13],
        "absolute_gamma_notional_per_percent": parameters[14],
        "call_gamma_notional_per_percent": parameters[15],
        "put_gamma_notional_per_percent": parameters[16],
        "flip_spot": parameters[17],
        "regime_at_spot": parameters[18],
        "sign_change_count": parameters[19],
        "flip_search_low_spot": parameters[20],
        "flip_search_high_spot": parameters[21],
        "flip_grid_points": parameters[22],
        "flip_reasons": parameters[23],
        "peak_gamma_strike": parameters[24],
        "strike_count": parameters[25],
        "contributing_contract_count": parameters[26],
        "eligible_contract_count": parameters[27],
        "coverage_fraction": parameters[28],
        "strike_profile": json_module.dumps(parameters[29].adapted),
        "quality_reasons": parameters[30],
    }
    assert rehydrate_profile(row).profile.strikes == record.profile.strikes


def test_profiles_by_matrix_is_catalogued_for_explain():
    assert "profiles_by_matrix" in READ_QUERIES
    assert "strike_profile" in READ_QUERIES["profiles_by_matrix"]
