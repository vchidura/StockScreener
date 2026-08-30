from __future__ import annotations

from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from options.analytics.greeks import price_and_greeks
from options.config import ScenarioPolicy

from .domain import OptionCandidate, OptionSide, ScenarioResult


def build_scenario_grid(
    candidate: OptionCandidate,
    policy: ScenarioPolicy,
) -> tuple[ScenarioResult, ...]:
    if not candidate.legs:
        return ()
    results: list[ScenarioResult] = []
    for time_fraction in policy.time_fractions_remaining:
        iv_shocks = (0.0,) if time_fraction == 0 else policy.iv_shock_fractions
        for spot_shock in policy.spot_shock_fractions:
            for iv_shock in iv_shocks:
                key = f"SPOT_{spot_shock:+.4f}:IV_{iv_shock:+.4f}:TIME_{time_fraction:.4f}"
                package_value = Decimal("0")
                package_delta = 0.0
                package_gamma = 0.0
                package_theta = 0.0
                package_vega = 0.0
                scenario_flags: set[str] = set()
                for leg in candidate.legs:
                    shocked_spot = float(leg.spot) * (1 + spot_shock)
                    direction = 1 if leg.side is OptionSide.BUY else -1
                    if time_fraction == 0:
                        intrinsic = (
                            max(shocked_spot - float(leg.strike), 0.0)
                            if leg.contract_type.value == "CALL"
                            else max(float(leg.strike) - shocked_spot, 0.0)
                        )
                        price = intrinsic
                        delta = gamma = theta = vega = 0.0
                    else:
                        volatility = leg.local_iv * (1 + iv_shock)
                        if volatility <= 0:
                            scenario_flags.add("NON_POSITIVE_SHOCKED_IV")
                            continue
                        price, delta, gamma, theta, vega, _ = price_and_greeks(
                            leg.contract_type,
                            spot=shocked_spot,
                            strike=float(leg.strike),
                            maturity=leg.time_to_expiration_years * time_fraction,
                            rate=leg.risk_free_rate,
                            dividend=leg.dividend_yield,
                            volatility=volatility,
                        )
                    scale = direction * leg.ratio * leg.multiplier
                    package_value += Decimal(str(price)) * scale
                    package_delta += delta * scale
                    package_gamma += gamma * scale
                    package_theta += theta * scale
                    package_vega += vega * scale
                profit_loss = (
                    package_value + candidate.net_premium
                    if candidate.net_premium is not None and not scenario_flags
                    else None
                )
                results.append(
                    ScenarioResult(
                        scenario_result_id=uuid5(
                            NAMESPACE_URL,
                            f"option-scenario:{candidate.candidate_id}:{key}",
                        ),
                        candidate_id=candidate.candidate_id,
                        scenario_key=key,
                        spot_shock_fraction=float(spot_shock),
                        iv_shock_fraction=float(iv_shock),
                        time_fraction_remaining=float(time_fraction),
                        repriced_value=package_value if not scenario_flags else None,
                        profit_loss=profit_loss,
                        delta=float(package_delta) if not scenario_flags else None,
                        gamma=float(package_gamma) if not scenario_flags else None,
                        theta_per_day=float(package_theta) if not scenario_flags else None,
                        vega_per_vol_point=float(package_vega) if not scenario_flags else None,
                        terminal=time_fraction == 0,
                        assumptions={
                            "spot_shock_fraction": spot_shock,
                            "iv_shock_fraction": iv_shock,
                            "time_fraction_remaining": time_fraction,
                            "quote_liquidity": "NOT_AVAILABLE",
                        },
                        quality_flags=tuple(sorted(scenario_flags)),
                        model_version=candidate.model_version,
                        policy_sha256=candidate.policy_sha256,
                    )
                )
    return tuple(results)