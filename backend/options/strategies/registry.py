from __future__ import annotations

from dataclasses import dataclass

from .domain import StructureRiskClass, StructureType


@dataclass(frozen=True, slots=True)
class StrategyRegistration:
    strategy_name: str
    display_name: str
    strategy_archetype: str
    persona_tags: tuple[str, ...]
    allowed_structure_types: tuple[StructureType, ...]
    allowed_risk_classes: tuple[StructureRiskClass, ...]
    description: str


STRATEGY_REGISTRY = (
    StrategyRegistration(
        "INCOME_WHEEL",
        "Income Generation / Wheel",
        "INCOME_GENERATION",
        ("INCOME",),
        (StructureType.CASH_SECURED_PUT,),
        (StructureRiskClass.CASH_SECURED,),
        "Cash-secured put research ranked by the reviewed Wheel policy.",
    ),
    StrategyRegistration(
        "SPREAD_RANGE_LOCATOR",
        "Defined-Risk Hedged Income",
        "DEFINED_RISK_INCOME",
        ("DEFINED_RISK_INCOME", "NEUTRAL_VOL"),
        (
            StructureType.PUT_CREDIT_VERTICAL,
            StructureType.CALL_CREDIT_VERTICAL,
            StructureType.IRON_CONDOR,
            StructureType.CALL_BUTTERFLY,
            StructureType.PUT_BUTTERFLY,
        ),
        (StructureRiskClass.DEFINED_RISK_CREDIT, StructureRiskClass.PREMIUM_AT_RISK_DEBIT),
        "Listed-leg bounded structures derived from persisted OI concentration evidence.",
    ),
    StrategyRegistration(
        "ZERO_DTE_GAMMA_SQUEEZE",
        "High-Momentum Directional",
        "MOMENTUM_DIRECTIONAL",
        ("MOMENTUM",),
        (StructureType.LONG_CALL, StructureType.LONG_PUT),
        (StructureRiskClass.PREMIUM_AT_RISK_DEBIT,),
        "Near-the-money 0-DTE Gamma and activity trigger with no inferred trade aggressor.",
    ),
    StrategyRegistration(
        "SWEEP_LIKE_CLUSTER",
        "Sweep-Like Activity",
        "ACTIVITY_RESEARCH",
        ("MOMENTUM", "NEUTRAL_VOL"),
        (StructureType.SWEEP_LIKE_CLUSTER,),
        (StructureRiskClass.RESEARCH_CONTEXT,),
        "Delayed event-time print clustering without institutional-owner or side claims.",
    ),
    StrategyRegistration(
        "VOLUME_OI_ANOMALY",
        "Three-Times Volume/OI",
        "ACTIVITY_RESEARCH",
        ("MOMENTUM", "NEUTRAL_VOL"),
        (StructureType.VOLUME_OI_ANOMALY,),
        (StructureRiskClass.RESEARCH_CONTEXT,),
        "Activity anomaly research; volume greater than OI does not imply opening flow.",
    ),
    StrategyRegistration(
        "VOLATILITY_SMILE_DISTORTION",
        "Volatility Smile Distortion",
        "VOLATILITY_RESEARCH",
        ("NEUTRAL_VOL",),
        (StructureType.VOLATILITY_DISTORTION,),
        (StructureRiskClass.RESEARCH_CONTEXT,),
        "Robust local-IV residual research with neighboring-strike consistency.",
    ),
)

REGISTRY_BY_NAME = {item.strategy_name: item for item in STRATEGY_REGISTRY}