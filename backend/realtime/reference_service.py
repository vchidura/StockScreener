from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol

from .contracts import UniverseManifest
from .reference_facts import ReferenceFact, validate_reference_facts
from .reference_seed import (
    ReferenceSeedManifest,
    ReferenceSeedMember,
    build_reference_seed_manifest,
)


EXPECTED_REFERENCE_SOURCES = frozenset(
    {"massive", "finnhub", "federal_reserve", "treasury"}
)


class ProviderEntitlementUnavailable(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ReferenceSeedRequest:
    universe: UniverseManifest
    session_date: date
    created_at: datetime
    effective_at: datetime
    valid_until: datetime


@dataclass(frozen=True, slots=True)
class ReferenceSourceGeneration:
    source: str
    generation_sha256: str
    members: tuple[ReferenceSeedMember, ...]
    provider_requests: int
    response_bytes: int
    facts: tuple[ReferenceFact, ...] = ()

    def __post_init__(self) -> None:
        source = self.source.strip().lower()
        if source not in EXPECTED_REFERENCE_SOURCES:
            raise ValueError("reference generation uses an unreviewed source")
        if len(self.generation_sha256) != 64 or any(
            character not in "0123456789abcdef"
            for character in self.generation_sha256
        ):
            raise ValueError("reference generation hash must be SHA-256")
        if self.provider_requests < 0 or self.response_bytes < 0:
            raise ValueError("reference generation counters cannot be negative")
        if not self.members:
            raise ValueError("reference generation must contain members")
        validate_reference_facts(self.facts)
        object.__setattr__(self, "source", source)


class ReferenceSourceAdapter(Protocol):
    source: str

    def collect(self, request: ReferenceSeedRequest) -> ReferenceSourceGeneration:
        ...


@dataclass(frozen=True, slots=True)
class ReferenceServiceResult:
    manifest: ReferenceSeedManifest
    provider_requests: int
    response_bytes: int
    facts: tuple[ReferenceFact, ...]


class ReferenceSeedService:
    def __init__(self, sources: tuple[ReferenceSourceAdapter, ...]) -> None:
        names = tuple(source.source.strip().lower() for source in sources)
        if set(names) != EXPECTED_REFERENCE_SOURCES or len(names) != len(set(names)):
            raise ValueError("reference service requires each reviewed source exactly once")
        self.sources = tuple(sorted(sources, key=lambda source: source.source))

    def run_once(self, request: ReferenceSeedRequest) -> ReferenceServiceResult:
        generations = tuple(source.collect(request) for source in self.sources)
        names = tuple(generation.source for generation in generations)
        if set(names) != EXPECTED_REFERENCE_SOURCES or len(names) != len(set(names)):
            raise ValueError("reference source result identity drifted")
        members = tuple(
            member for generation in generations for member in generation.members
        )
        facts = tuple(
            fact for generation in generations for fact in generation.facts
        )
        validate_reference_facts(facts)
        manifest = build_reference_seed_manifest(
            request.universe,
            session_date=request.session_date,
            members=members,
            source_generations=tuple(
                (generation.source, generation.generation_sha256)
                for generation in generations
            ),
            created_at=request.created_at,
            effective_at=request.effective_at,
            valid_until=request.valid_until,
        )
        return ReferenceServiceResult(
            manifest=manifest,
            provider_requests=sum(row.provider_requests for row in generations),
            response_bytes=sum(row.response_bytes for row in generations),
            facts=facts,
        )