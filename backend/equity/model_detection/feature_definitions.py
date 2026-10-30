"""The reference feature catalog is shared by every adapter and policy caller."""
from dataclasses import dataclass


FEATURE_VERSION = "completed_bar_breakout_features_v1"


@dataclass(frozen=True, slots=True)
class FeatureDefinition:
    name: str
    unit: str
    definition: str


FEATURE_DEFINITIONS = (
    FeatureDefinition("range_high", "price", "Maximum high of preceding lookback completed bars"),
    FeatureDefinition("range_low", "price", "Minimum low of preceding lookback completed bars"),
    FeatureDefinition("close", "price", "Latest completed bar close"),
    FeatureDefinition("volume_multiple", "ratio", "Latest volume divided by mean preceding lookback volume"),
    FeatureDefinition("market_trend", "signed_ratio", "Frozen market trend; positive supports long"),
    FeatureDefinition("sector_trend", "signed_ratio", "Frozen sector trend; positive supports long"),
    FeatureDefinition("event_risk", "bool", "Frozen event veto for this security"),
)
