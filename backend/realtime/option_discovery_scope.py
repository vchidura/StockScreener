from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from .contracts import UniverseManifest, canonical_sha256
from .preflight import ProviderTier


@dataclass(frozen=True, slots=True)
class RealtimeOptionDiscoveryScope:
    provider_tier: ProviderTier
    profile_id: UUID
    profile_sha256: str
    universe: UniverseManifest

    def __post_init__(self) -> None:
        if not isinstance(self.provider_tier, ProviderTier):
            raise TypeError("option discovery provider tier is invalid")
        if len(self.profile_sha256) != 64 or any(
            character not in "0123456789abcdef"
            for character in self.profile_sha256
        ):
            raise ValueError("option discovery profile SHA-256 is invalid")
        if not self.universe.option_underlyings:
            raise ValueError("option discovery universe has no option underlyings")

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)