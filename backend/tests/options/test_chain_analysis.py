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


def _row(contract_id, contract_type, strike, delta, iv, expiration=date(2026, 9, 4), maturity=0.1):
    return ExpirationContractInput(
        contract_id=contract_id,
        contract_type=contract_type,
        expiration_date=expiration,
        strike=Decimal(str(strike)),
        spot=Decimal("100"),
        time_to_expiration_years=maturity,
        risk_free_rate=0.0,
        dividend_yield=0.0,
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

    result = analyze_expirations(uuid4(), rows, maximum_delta_interpolation_gap=0.15)[0]

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