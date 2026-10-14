from datetime import timedelta
from decimal import Decimal
from uuid import UUID

from realtime.local_mock_execution import (
    LocalMockEarningsAssessment,
    LocalMockEarningsStatus,
    LocalMockExecutionPolicy,
    LocalMockStatus,
    simulate_local_mock_execution,
)
from realtime.option_portal_adapter import (
    materialize_developer_option_package_mark,
    materialize_realtime_option_package_mark,
)
from realtime.preflight import ProviderTier
from tests.realtime.test_option_portal_adapter import (
    NOW,
    _developer_policy,
    _developer_snapshot,
    _package,
    _policy,
    _quote,
)


ALERT = UUID("abababab-abab-abab-abab-abababababab")


def _mock_policy(**overrides):
    values = {
        "version": "local_mock_quote_fill_v1",
        "maximum_quote_age_ms": 1_000,
        "maximum_quote_skew_ms": 250,
        "slippage_per_share": Decimal("0.01"),
        "fee_per_contract": Decimal("0.65"),
        "maximum_capital_at_risk": Decimal("1_000"),
        "maximum_total_contracts": 8,
    }
    values.update(overrides)
    return LocalMockExecutionPolicy(**values)


def _earnings(status=LocalMockEarningsStatus.CLEAR, reasons=()):
    return LocalMockEarningsAssessment(status, reasons)


def _advanced_mark():
    valuation_policy = _policy()
    package = _package(valuation_policy.policy_sha256)
    result = materialize_realtime_option_package_mark(
        package,
        {
            1: _quote(1, package.legs[0].contract_ticker, "4.90", "5.10", 10, 100),
            2: _quote(2, package.legs[1].contract_ticker, "1.90", "2.10", 11, 150),
        },
        checked_at=NOW + timedelta(milliseconds=200),
        policy=valuation_policy,
    )
    return result.mark


def test_local_mock_fill_is_deterministic_and_never_grants_execution_permission():
    mark = _advanced_mark()
    requested_at = NOW + timedelta(milliseconds=300)
    policy = _mock_policy()

    first = simulate_local_mock_execution(
        mark,
        alert_id=ALERT,
        requested_at=requested_at,
        earnings=_earnings(),
        policy=policy,
    )
    repeated = simulate_local_mock_execution(
        mark,
        alert_id=ALERT,
        requested_at=requested_at,
        earnings=_earnings(),
        policy=policy,
    )

    assert first == repeated
    assert first.alert_id == ALERT
    assert first.policy == policy
    assert first.earnings == _earnings()
    assert first.evidence_batch_id == mark.evidence_batch_id
    assert first.source_evidence_ids == tuple(
        row.evidence_id for row in mark.mark_evidence
    )
    assert first.status is LocalMockStatus.FILLED
    assert first.execution_permission is False
    assert [fill.fill_price for fill in first.fills] == [
        Decimal("5.11"),
        Decimal("1.89"),
    ]
    assert first.gross_cash_flow == Decimal("-322.00")
    assert first.estimated_fees == Decimal("1.30")
    assert first.net_cash_flow == Decimal("-323.30")


def test_local_mock_fails_closed_for_developer_marks_and_stale_quotes():
    valuation_policy = _developer_policy()
    package = _package(
        valuation_policy.policy_sha256,
        ProviderTier.MASSIVE_DEVELOPER,
    )
    batch_id = UUID("12121212-1212-1212-1212-121212121212")
    developer = materialize_developer_option_package_mark(
        package,
        {
            leg.instrument_id: _developer_snapshot(
                leg,
                valuation_policy,
                batch_id=batch_id,
                mark=str(Decimal("6.20") - leg.leg_index),
                milliseconds=100 + leg.leg_index,
            )
            for leg in package.legs
        },
        checked_at=NOW + timedelta(milliseconds=200),
        policy=valuation_policy,
    )

    unavailable = simulate_local_mock_execution(
        developer.mark,
        alert_id=ALERT,
        requested_at=NOW + timedelta(milliseconds=300),
        earnings=_earnings(),
        policy=_mock_policy(),
    )
    stale = simulate_local_mock_execution(
        _advanced_mark(),
        alert_id=ALERT,
        requested_at=NOW + timedelta(seconds=2),
        earnings=_earnings(),
        policy=_mock_policy(maximum_quote_age_ms=500),
    )

    assert unavailable.status is LocalMockStatus.UNAVAILABLE
    assert unavailable.reason_codes == ("COHERENT_QUOTE_EVIDENCE_REQUIRED",)
    assert unavailable.fills == ()
    assert stale.status is LocalMockStatus.UNAVAILABLE
    assert stale.reason_codes == ("PACKAGE_LEG_QUOTE_STALE:101",)
    assert stale.fills == ()


def test_local_mock_earnings_and_risk_validation_are_terminal_without_fills():
    mark = _advanced_mark()
    requested_at = NOW + timedelta(milliseconds=300)

    missing_earnings = simulate_local_mock_execution(
        mark,
        alert_id=ALERT,
        requested_at=requested_at,
        earnings=_earnings(
            LocalMockEarningsStatus.UNAVAILABLE,
            ("EARNINGS_COVERAGE_UNAVAILABLE",),
        ),
        policy=_mock_policy(),
    )
    risk_rejected = simulate_local_mock_execution(
        mark,
        alert_id=ALERT,
        requested_at=requested_at,
        earnings=_earnings(),
        policy=_mock_policy(maximum_capital_at_risk=Decimal("100")),
    )

    assert missing_earnings.status is LocalMockStatus.UNAVAILABLE
    assert missing_earnings.reason_codes == ("EARNINGS_COVERAGE_UNAVAILABLE",)
    assert risk_rejected.status is LocalMockStatus.REJECTED
    assert risk_rejected.reason_codes == ("MOCK_CAPITAL_AT_RISK_LIMIT_EXCEEDED",)
    assert missing_earnings.fills == risk_rejected.fills == ()