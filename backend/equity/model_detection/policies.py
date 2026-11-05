"""Versioned reference policy; no legacy policy hashes are redefined here."""
from dataclasses import dataclass
from decimal import Decimal

from .contracts import ModelIdentity, canonical_sha256, finite, nonempty


@dataclass(frozen=True, slots=True)
class BreakoutPolicy:
    model_id: str = "completed_bar_breakout_reference"
    version: str = "1"
    style: str = "INTRADAY"
    origin: str = "COMPLETED_BAR"
    interval: str = "5m"
    lookback: int = 2
    min_volume_multiple: Decimal = Decimal("1.2")
    reward_risk: Decimal = Decimal("2")
    validity_seconds: int = 1800
    max_age_seconds: int = 300

    def __post_init__(self) -> None:
        for value in (self.model_id, self.version, self.style, self.origin):
            nonempty(value)
        if self.interval not in {"1m", "5m", "30m", "1h", "1d"}:
            raise ValueError("unsupported policy interval")
        for value in (self.lookback, self.validity_seconds, self.max_age_seconds):
            if type(value) is not int or value <= 0:
                raise ValueError("policy windows must be positive integers")
        finite(self.min_volume_multiple, positive=True)
        finite(self.reward_risk, positive=True)

    @property
    def policy_sha256(self) -> str:
        return canonical_sha256(self)

    @property
    def optional_context_names(self) -> frozenset[str]:
        return frozenset(getattr(self, "optional_context", ()))

    @property
    def identity(self) -> ModelIdentity:
        return ModelIdentity(self.model_id, self.version, self.style, self.origin, self.policy_sha256)


@dataclass(frozen=True, slots=True)
class ContextOptionalBreakoutPolicy(BreakoutPolicy):
    """Evaluates without the context named here, showing its gate as NOT_APPLICABLE.

    A separate class and version, so BreakoutPolicy's existing hashes do not change.
    """

    version: str = "2"
    optional_context: tuple[str, ...] = ("event_risk", "sector_trend")

    def __post_init__(self) -> None:
        BreakoutPolicy.__post_init__(self)
        if not set(self.optional_context) <= {"event_risk", "sector_trend"}:
            raise ValueError("only event and sector context can be optional")
