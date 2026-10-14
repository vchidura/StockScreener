import json
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics import (
    ExpirationContractInput,
    analyze_expirations,
    build_chain_health,
)
from options.domain import ContractType


def _row(contract_id, contract_type, strike, delta, iv, expiration=date(2026, 9, 4), maturity=0.1, rate=0.0, dividend=0.0):
    return ExpirationContractInput(
        contract_id=contract_id,
        contract_type=contract_type,
        expiration_date=expiration,
        strike=Decimal(str(strike)),
        spot=Decimal("100"),
        time_to_expiration_years=maturity,
        risk_free_rate=rate,
        dividend_yield=dividend,
        local_iv=iv,
        local_delta=delta,
        day_volume=100,
        open_interest=200,
    )


def test_expiration_analysis_requires_atm_bracket_and_interpolates_without_extrapolation():
    rows = (
        _row(1, ContractType.CALL, 95, 0.60, 0.20),
        _row(2, ContractType.PUT, 95, -0.15, 0.23),
        _row(3, ContractType.CALL, 105, 0.20, 0.24),
        _row(4, ContractType.PUT, 105, -0.30, 0.27),
        _row(5, ContractType.CALL, 110, 0.30, 0.26),
        _row(6, ContractType.PUT, 110, -0.20, 0.25),
    )

    # The legacy path (no comparability policy) keeps every contract.
    result = analyze_expirations(
        uuid4(), rows, maximum_delta_interpolation_gap=0.15, iv_comparability_policy=None
    )[0]

    assert result.atm_iv == pytest.approx(0.235)
    assert result.call_25_delta_iv == pytest.approx(0.25)
    assert result.put_25_delta_iv == pytest.approx(0.26)
    assert result.risk_reversal_25_delta == pytest.approx(-0.01)
    assert result.call_skew_25_delta == pytest.approx(0.015)
    assert result.put_skew_25_delta == pytest.approx(0.025)
    concentration = json.loads(result.concentration_metrics_json)
    assert concentration["put_call_volume_ratio"] == 1.0


def test_delta_interpolation_does_not_extrapolate_or_cross_large_gap():
    rows = (
        _row(1, ContractType.CALL, 95, 0.50, 0.20),
        _row(2, ContractType.CALL, 105, 0.40, 0.24),
        _row(3, ContractType.PUT, 95, -0.10, 0.23),
        _row(4, ContractType.PUT, 105, -0.40, 0.27),
    )

    result = analyze_expirations(uuid4(), rows, maximum_delta_interpolation_gap=0.15)[0]

    assert result.call_25_delta_iv is None
    assert result.put_25_delta_iv is None
    assert "CALL_25_DELTA_IV_INSUFFICIENT" in result.quality_reasons


def test_chain_health_fails_closed_on_reference_drift_or_low_iv_convergence():
    health = build_chain_health(
        received_count=100,
        retained_count=80,
        catalog_matched_count=99,
        mark_aligned_count=80,
        iv_attempt_count=80,
        iv_converged_count=75,
        unknown_reference_count=1,
        reference_drift_failed=False,
    )

    assert health.iv_convergence_fraction == 0.9375
    assert health.status == "FAILED"
    assert "DATA_QUALITY_GATE_FAILED" in health.reasons


def _realistic(contract_id, contract_type, strike, delta, iv, **kwargs):
    kwargs.setdefault("rate", 0.04)
    kwargs.setdefault("dividend", 0.01)
    return _row(contract_id, contract_type, strike, delta, iv, **kwargs)


def _atm_rows(itm_put_iv):
    return (
        _realistic(1, ContractType.CALL, 95, 0.60, 0.20),
        _realistic(2, ContractType.PUT, 95, -0.30, 0.23),
        _realistic(3, ContractType.CALL, 105, 0.30, 0.24),
        _realistic(4, ContractType.PUT, 105, -0.60, itm_put_iv),
    )


def test_itm_put_no_longer_moves_atm_iv_while_the_legacy_path_does():
    low, high = _atm_rows(0.10), _atm_rows(0.27)

    comparable_low = analyze_expirations(uuid4(), low)[0]
    comparable_high = analyze_expirations(uuid4(), high)[0]
    legacy_low = analyze_expirations(uuid4(), low, iv_comparability_policy=None)[0]
    legacy_high = analyze_expirations(uuid4(), high, iv_comparability_policy=None)[0]

    assert comparable_low.atm_iv == pytest.approx(0.23)
    assert comparable_high.atm_iv == pytest.approx(0.23)
    assert legacy_low.atm_iv == pytest.approx(0.215)
    assert legacy_high.atm_iv == pytest.approx(0.235)
    assert "ATM_IV_NOT_COMPARABLE" not in comparable_low.quality_reasons
    assert "iv_comparability" not in json.loads(legacy_low.interpolation_diagnostics_json)


def test_exclusions_are_reported_in_the_diagnostics_without_changing_the_shape():
    result = analyze_expirations(uuid4(), _atm_rows(0.27))[0]

    diagnostics = json.loads(result.interpolation_diagnostics_json)
    assert diagnostics["call_25_delta_available"] is False
    assert diagnostics["iv_comparability"]["atm_iv"] == {
        "policy_version": "iv_comparability_v1",
        "status": "DEGRADED",
        "considered": 4,
        "comparable": 3,
        "excluded": 1,
        "reasons": {"ITM_PUT_AMERICAN_BIAS": 1},
    }
    assert diagnostics["iv_comparability"]["put_25_delta"]["excluded"] == 1


def test_all_excluded_atm_cohort_is_unavailable_with_reasons_not_zero_or_nan():
    rows = tuple(
        _realistic(index, contract_type, strike, delta, 0.05, maturity=1 / 365)
        for index, (contract_type, strike, delta) in enumerate(
            (
                (ContractType.CALL, 95, 0.60),
                (ContractType.PUT, 95, -0.30),
                (ContractType.CALL, 105, 0.30),
                (ContractType.PUT, 105, -0.60),
            ),
            start=1,
        )
    )

    result = analyze_expirations(uuid4(), rows)[0]

    assert result.atm_iv is None
    assert result.call_skew_25_delta is None and result.put_skew_25_delta is None
    assert "ATM_IV_INSUFFICIENT" in result.quality_reasons
    assert "ATM_IV_NOT_COMPARABLE" in result.quality_reasons
    atm = json.loads(result.interpolation_diagnostics_json)["iv_comparability"]["atm_iv"]
    assert atm["status"] == "UNAVAILABLE" and atm["comparable"] == 0
    assert atm["reasons"] == {"ITM_PUT_AMERICAN_BIAS": 1, "LOW_VEGA": 4}


def test_calls_are_excluded_too_when_the_dividend_removes_the_carry():
    rows = tuple(
        _realistic(row.contract_id, row.contract_type, row.strike, row.local_delta, row.local_iv, dividend=0.04)
        for row in _atm_rows(0.27)
    )

    result = analyze_expirations(uuid4(), rows)[0]

    # The 5%-in-the-money call and put are both outside the band; two rows are not enough.
    assert result.atm_iv is None
    assert "ATM_IV_NOT_COMPARABLE" in result.quality_reasons
    reasons = json.loads(result.interpolation_diagnostics_json)["iv_comparability"]["atm_iv"]["reasons"]
    assert reasons == {"ITM_CALL_AMERICAN_BIAS": 1, "ITM_PUT_AMERICAN_BIAS": 1}


def test_itm_put_is_dropped_from_the_25_delta_interpolation():
    rows = (
        _realistic(1, ContractType.CALL, 95, 0.60, 0.20),
        _realistic(2, ContractType.PUT, 95, -0.10, 0.23),
        _realistic(3, ContractType.CALL, 105, 0.20, 0.24),
        _realistic(4, ContractType.PUT, 105, -0.30, 0.60),
        _realistic(5, ContractType.PUT, 90, -0.20, 0.25),
    )

    comparable = analyze_expirations(uuid4(), rows, maximum_delta_interpolation_gap=0.15)[0]
    legacy = analyze_expirations(
        uuid4(), rows, maximum_delta_interpolation_gap=0.15, iv_comparability_policy=None
    )[0]

    assert legacy.put_25_delta_iv == pytest.approx(0.425)
    assert comparable.put_25_delta_iv is None
    assert "PUT_25_DELTA_IV_INSUFFICIENT" in comparable.quality_reasons
    assert comparable.risk_reversal_25_delta is None and comparable.put_skew_25_delta is None