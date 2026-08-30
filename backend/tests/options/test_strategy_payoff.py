from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import uuid4

from options.domain import ContractType
from options.strategies.domain import CandidateLeg, OptionSide
from options.strategies.payoff import evaluate_terminal_payoff, terminal_profit


UTC = timezone.utc
MARKET_TIME = datetime(2026, 8, 28, 20, 0, tzinfo=UTC)
EXPIRATION = date(2026, 9, 18)


def leg(
    index: int,
    strike: str,
    mark: str,
    side: OptionSide,
    contract_type: ContractType,
    *,
    ratio: int = 1,
) -> CandidateLeg:
    return CandidateLeg(
        leg_index=index,
        snapshot_id=uuid4(),
        contract_id=index + 1,
        contract_ticker=f"O:TEST{index}",
        side=side,
        ratio=ratio,
        multiplier=100,
        expiration_date=EXPIRATION,
        strike=Decimal(strike),
        contract_type=contract_type,
        spot=Decimal("100"),
        time_to_expiration_years=30 / 365,
        risk_free_rate=0.04,
        dividend_yield=0.0,
        model_mark=Decimal(mark),
        local_iv=0.25,
        local_delta=0.30,
        local_gamma=0.02,
        local_theta_per_day=-0.05,
        local_vega_per_vol_point=0.10,
        local_rho_per_rate_point=0.02,
        source_market_time=MARKET_TIME,
        mark_source="DEVELOPER_ALIGNED_AGG_CLOSE",
        model_version="black_scholes_european_v1",
        quality_flags=(),
    )


def test_credit_vertical_payoff_is_bounded_and_matches_formula():
    legs = (
        leg(0, "100", "3", OptionSide.SELL, ContractType.PUT),
        leg(1, "95", "1", OptionSide.BUY, ContractType.PUT),
    )

    result = evaluate_terminal_payoff(legs)

    assert result.net_premium == Decimal("200")
    assert result.maximum_profit == Decimal("200")
    assert result.maximum_loss == Decimal("300")
    assert result.breakevens == (Decimal("98"),)


def test_iron_condor_payoff_uses_wider_side_for_maximum_loss():
    legs = (
        leg(0, "95", "2", OptionSide.BUY, ContractType.PUT),
        leg(1, "100", "4", OptionSide.SELL, ContractType.PUT),
        leg(2, "110", "3", OptionSide.SELL, ContractType.CALL),
        leg(3, "116", "1", OptionSide.BUY, ContractType.CALL),
    )

    result = evaluate_terminal_payoff(legs)

    assert result.net_premium == Decimal("400")
    assert result.maximum_profit == Decimal("400")
    assert result.maximum_loss == Decimal("200")
    assert result.breakevens == (Decimal("96"), Decimal("114"))


def test_long_call_has_bounded_loss_and_unbounded_profit():
    legs = (leg(0, "100", "5", OptionSide.BUY, ContractType.CALL),)

    result = evaluate_terminal_payoff(legs)

    assert result.net_premium == Decimal("-500")
    assert result.maximum_loss == Decimal("500")
    assert result.maximum_profit is None
    assert result.breakevens == (Decimal("105"),)


def test_uncovered_short_call_is_rejected_as_unbounded_loss():
    legs = (leg(0, "100", "5", OptionSide.SELL, ContractType.CALL),)

    result = evaluate_terminal_payoff(legs)

    assert result.bounded_maximum_loss is False
    assert result.maximum_loss is None
    assert terminal_profit(legs, Decimal("1000")) < Decimal("-80000")


def test_equal_width_butterfly_matches_terminal_breakpoints():
    legs = (
        leg(0, "95", "7", OptionSide.BUY, ContractType.CALL),
        leg(1, "100", "4", OptionSide.SELL, ContractType.CALL, ratio=2),
        leg(2, "105", "2", OptionSide.BUY, ContractType.CALL),
    )

    result = evaluate_terminal_payoff(legs)

    assert result.net_premium == Decimal("-100")
    assert result.maximum_loss == Decimal("100")
    assert result.maximum_profit == Decimal("400")
    assert result.breakevens == (Decimal("96"), Decimal("104"))