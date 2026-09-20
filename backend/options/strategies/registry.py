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
        "DIRECTIONAL_LONG_PREMIUM",
        "Directional Long Premium",
        "MOMENTUM_DIRECTIONAL",
        ("MOMENTUM",),
        (StructureType.LONG_CALL, StructureType.LONG_PUT),
        (StructureRiskClass.PREMIUM_AT_RISK_DEBIT,),
        "Long calls and puts across DTE lanes whose breakeven sits inside the implied move.",
    ),
    StrategyRegistration(
        "DIRECTIONAL_DEBIT_SPREAD",
        "Directional Debit Spread",
        "MOMENTUM_DIRECTIONAL",
        ("MOMENTUM",),
        (StructureType.CALL_DEBIT_VERTICAL, StructureType.PUT_DEBIT_VERTICAL),
        (StructureRiskClass.PREMIUM_AT_RISK_DEBIT,),
        "Vertical debit spreads whose breakeven and profit target both sit inside the implied move.",
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

DISCOVERY_CATALOG_VERSION = "option_discovery_v1"
DISCOVERY_CATEGORY_LABELS = {
    "INCOME": "Income Generation",
    "DEFINED_RISK_INCOME": "Defined-Risk Income",
    "MOMENTUM": "Momentum / Activity",
    "NEUTRAL_VOL": "Neutral / Volatility",
}

STRUCTURE_DISCOVERY_CATEGORIES = {
    StructureType.CASH_SECURED_PUT: ("INCOME",),
    StructureType.PUT_CREDIT_VERTICAL: ("DEFINED_RISK_INCOME",),
    StructureType.CALL_CREDIT_VERTICAL: ("DEFINED_RISK_INCOME",),
    StructureType.IRON_CONDOR: ("DEFINED_RISK_INCOME", "NEUTRAL_VOL"),
    StructureType.CALL_BUTTERFLY: ("NEUTRAL_VOL",),
    StructureType.PUT_BUTTERFLY: ("NEUTRAL_VOL",),
    StructureType.LONG_CALL: ("MOMENTUM",),
    StructureType.LONG_PUT: ("MOMENTUM",),
    StructureType.CALL_DEBIT_VERTICAL: ("MOMENTUM",),
    StructureType.PUT_DEBIT_VERTICAL: ("MOMENTUM",),
    StructureType.SWEEP_LIKE_CLUSTER: ("MOMENTUM", "NEUTRAL_VOL"),
    StructureType.VOLUME_OI_ANOMALY: ("MOMENTUM", "NEUTRAL_VOL"),
    StructureType.VOLATILITY_DISTORTION: ("NEUTRAL_VOL",),
}


def discovery_categories(structure_type: StructureType) -> tuple[str, ...]:
    return STRUCTURE_DISCOVERY_CATEGORIES[structure_type]


def build_discovery_catalog() -> dict[str, object]:
    return {
        "version": DISCOVERY_CATALOG_VERSION,
        "categories": [
            {"id": category_id, "label": label}
            for category_id, label in DISCOVERY_CATEGORY_LABELS.items()
        ],
        "models": [
            {
                "id": registration.strategy_name,
                "label": registration.display_name,
                "output_kind": (
                    "OBSERVATION"
                    if registration.allowed_risk_classes == (StructureRiskClass.RESEARCH_CONTEXT,)
                    else "STRUCTURE"
                ),
                "structures": [
                    {
                        "id": structure.value,
                        "category_ids": list(discovery_categories(structure)),
                    }
                    for structure in registration.allowed_structure_types
                ],
            }
            for registration in STRATEGY_REGISTRY
        ],
    }