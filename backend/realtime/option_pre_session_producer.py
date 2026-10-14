from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Callable, Protocol
from uuid import UUID

from options.domain import OptionContractCatalogEntry, OptionContractSnapshot
from .contracts import DEVELOPER_FORWARD_UNIVERSE
from .option_discovery_repository import RealtimeOptionDiscoveryEvidence
from .option_discovery_scope import RealtimeOptionDiscoveryScope
from .option_manifest_repository import option_manifest_selection_id
from .option_session_manifest import (
    BoundedOptionManifestPolicy,
    OptionManifestCandidate,
    SelectedContractManifestPlan,
    build_bounded_option_session_manifest,
)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class OptionPreSessionRequest:
    scope: RealtimeOptionDiscoveryScope
    policy: BoundedOptionManifestPolicy
    session_date: date
    scheduled_cycle: datetime
    effective_at: datetime
    underlying_security_ids: tuple[tuple[str, UUID], ...]
    maximum_provider_requests: int
    maximum_response_bytes: int

    def __post_init__(self) -> None:
        scheduled = _utc(self.scheduled_cycle, "scheduled_cycle")
        effective = _utc(self.effective_at, "effective_at")
        identities = tuple(sorted(
            (ticker.strip().upper(), security_id)
            for ticker, security_id in self.underlying_security_ids
        ))
        if tuple(ticker for ticker, _ in identities) != self.scope.universe.option_underlyings:
            raise ValueError("pre-session security identities must cover the exact option universe")
        if len({security_id for _, security_id in identities}) != len(identities):
            raise ValueError("pre-session security identities must be unique")
        if scheduled.date() != self.session_date or effective.date() != self.session_date:
            raise ValueError("pre-session clocks must belong to the manifest session")
        if effective > scheduled:
            raise ValueError("pre-session manifest cannot become effective after collection")
        if (
            type(self.maximum_provider_requests) is not int
            or self.maximum_provider_requests <= 0
            or type(self.maximum_response_bytes) is not int
            or self.maximum_response_bytes <= 0
        ):
            raise ValueError("pre-session source limits must be positive integers")
        object.__setattr__(self, "scheduled_cycle", scheduled)
        object.__setattr__(self, "effective_at", effective)
        object.__setattr__(self, "underlying_security_ids", identities)

    @property
    def security_id_by_underlyer(self) -> dict[str, UUID]:
        return dict(self.underlying_security_ids)


@dataclass(frozen=True, slots=True)
class PreSessionNormalizationDiagnostics:
    """Bounded evidence of why an underlyer produced its candidates (RT-05).

    Counts are per reason; one raw row can carry several reasons, so they are not a
    partition of the row count. No credential or authenticated URL is retained.
    """

    underlyer: str
    bar_acquisition: str
    requested_from: datetime | None
    requested_to: datetime | None
    returned_from: datetime | None
    returned_to: datetime | None
    bar_count: int
    pages_fetched: int
    pagination_complete: bool
    price_basis: str
    observation_min: datetime | None
    observation_max: datetime | None
    raw_rows: int
    usable_mark_count: int
    candidates: int
    rejection_counts: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        if self.bar_acquisition not in {"FETCHED", "SKIPPED_NO_USABLE_MARKS"}:
            raise ValueError("unknown bar acquisition outcome")
        if min(self.bar_count, self.pages_fetched, self.raw_rows, self.usable_mark_count,
               self.candidates) < 0:
            raise ValueError("diagnostic counts cannot be negative")
        if self.candidates == 0 and not self.rejection_counts:
            raise ValueError("a zero-candidate underlyer requires explicit rejection reasons")


@dataclass(frozen=True, slots=True)
class CompletedOptionDiscoveryPackage:
    evidence: RealtimeOptionDiscoveryEvidence
    candidates: tuple[OptionManifestCandidate, ...]
    diagnostics: PreSessionNormalizationDiagnostics | None = None

    def __post_init__(self) -> None:
        batch = self.evidence.result.batch
        if not batch.complete:
            raise ValueError("pre-session discovery package requires a complete batch")
        if self.diagnostics is not None and self.diagnostics.underlyer != batch.underlyer:
            raise ValueError("pre-session diagnostics changed their underlying")
        if any(candidate.catalog.underlyer != batch.underlyer for candidate in self.candidates):
            raise ValueError("pre-session candidate changed its discovery underlying")
        if any(
            candidate.snapshot.batch_id != batch.batch_id
            for candidate in self.candidates
        ):
            raise ValueError("pre-session candidate changed its discovery batch")
        if any(
            candidate.discovery_evidence_sha256 != self.evidence.evidence_sha256
            for candidate in self.candidates
        ):
            raise ValueError("pre-session candidate changed discovery evidence lineage")
        contract_ids = tuple(candidate.catalog.contract_id for candidate in self.candidates)
        if len(contract_ids) != len(set(contract_ids)):
            raise ValueError("pre-session package contains duplicate candidates")


@dataclass(frozen=True, slots=True)
class OptionPreSessionCollection:
    packages: tuple[CompletedOptionDiscoveryPackage, ...]
    provider_requests: int
    response_bytes: int

    def __post_init__(self) -> None:
        if not self.packages:
            raise ValueError("pre-session collection cannot be empty")
        underlyings = tuple(
            package.evidence.result.batch.underlyer for package in self.packages
        )
        if len(underlyings) != len(set(underlyings)):
            raise ValueError("pre-session collection contains duplicate underlyings")
        if self.provider_requests < 0 or self.response_bytes < 0:
            raise ValueError("pre-session collection counters cannot be negative")


class OptionPreSessionSource(Protocol):
    def collect(self, request: OptionPreSessionRequest) -> OptionPreSessionCollection:
        ...


class StagedOptionPreSessionSource(Protocol):
    def collect_underlyer(self, request: OptionPreSessionRequest, underlyer: str):
        ...

    def account_counters(self) -> tuple[int, int]:
        ...


@dataclass(frozen=True, slots=True)
class OptionPreSessionProducerResult:
    discovery_batch_ids: tuple[UUID, ...]
    selection_id: UUID
    plan: SelectedContractManifestPlan
    provider_requests: int
    response_bytes: int
    catalog: tuple[OptionContractCatalogEntry, ...] = ()
    snapshots: tuple[OptionContractSnapshot, ...] = ()

    def __post_init__(self) -> None:
        if not self.discovery_batch_ids:
            raise ValueError("pre-session producer requires discovery batches")
        if len(self.discovery_batch_ids) != len(set(self.discovery_batch_ids)):
            raise ValueError("pre-session producer discovery batches must be unique")
        if self.selection_id != option_manifest_selection_id(self.plan):
            raise ValueError("pre-session producer selection identity mismatch")
        if self.provider_requests < 0 or self.response_bytes < 0:
            raise ValueError("pre-session producer counters cannot be negative")
        if self.catalog:
            catalog_ids = tuple(row.contract_id for row in self.catalog)
            if len(set(catalog_ids)) != len(catalog_ids) or set(catalog_ids) != {row.contract_id for row in self.plan.dispositions}:
                raise ValueError("pre-session result catalog must cover every discovered disposition")
        if self.snapshots and (len({row.contract_id for row in self.snapshots}) != len(self.snapshots)
                or {row.contract_id for row in self.snapshots} != {row.contract_id for row in self.catalog}):
            raise ValueError("pre-session snapshots must cover the complete discovered catalog")


class OptionPreSessionProducer:
    def __init__(
        self,
        source: OptionPreSessionSource,
        *,
        persist_packages: Callable[
            [tuple[CompletedOptionDiscoveryPackage, ...]], object
        ],
        persist_selection: Callable[[SelectedContractManifestPlan, datetime], UUID],
    ) -> None:
        self.source = source
        self.persist_packages = persist_packages
        self.persist_selection = persist_selection

    @staticmethod
    def _validate_collection(
        request: OptionPreSessionRequest,
        collection: OptionPreSessionCollection,
    ) -> tuple[CompletedOptionDiscoveryPackage, ...]:
        ordered = tuple(sorted(
            collection.packages,
            key=lambda package: package.evidence.result.batch.underlyer,
        ))
        actual_underlyings = tuple(
            package.evidence.result.batch.underlyer for package in ordered
        )
        if actual_underlyings != request.scope.universe.option_underlyings:
            raise ValueError("pre-session collection does not cover the exact option universe")
        security_ids = request.security_id_by_underlyer
        for package in ordered:
            evidence = package.evidence
            batch = evidence.result.batch
            if evidence.scope != request.scope:
                raise ValueError("pre-session discovery scope drifted")
            if evidence.underlying_security_id != security_ids[batch.underlyer]:
                raise ValueError("pre-session discovery security identity drifted")
            if batch.scheduled_cycle != request.scheduled_cycle:
                raise ValueError("pre-session discovery cycle drifted")
            if (request.policy.version.startswith("developer-forward")
                    or request.scope.universe == DEVELOPER_FORWARD_UNIVERSE) and len(package.candidates) > 10_000:
                raise ValueError("forward preparation underlying batch exceeds the native discovery row ceiling")
        if collection.provider_requests > request.maximum_provider_requests:
            raise ValueError("pre-session collection exceeded the request bound")
        if collection.response_bytes > request.maximum_response_bytes:
            raise ValueError("pre-session collection exceeded the response-byte bound")
        candidate_ids = tuple(
            candidate.catalog.contract_id
            for package in ordered
            for candidate in package.candidates
        )
        if len(candidate_ids) != len(set(candidate_ids)):
            raise ValueError("pre-session collection contains duplicate candidate identities")
        return ordered

    def run_once(self, request: OptionPreSessionRequest) -> OptionPreSessionProducerResult:
        if not isinstance(request, OptionPreSessionRequest):
            raise TypeError("pre-session producer request is invalid")
        collection = self.source.collect(request)
        if not isinstance(collection, OptionPreSessionCollection):
            raise TypeError("pre-session source returned an invalid collection")
        packages = self._validate_collection(request, collection)
        candidates = tuple(
            candidate
            for package in packages
            for candidate in package.candidates
        )
        plan = build_bounded_option_session_manifest(
            request.scope,
            request.policy,
            candidates,
            session_date=request.session_date,
        )
        if (request.policy.version.startswith("developer-forward")
            or request.scope.universe == DEVELOPER_FORWARD_UNIVERSE) and len(plan.policy_admitted_contract_ids) > 10_000:
            raise ValueError("forward policy-admitted matrix exceeds the retained global row ceiling")
        self.persist_packages(packages)
        selection_id = self.persist_selection(plan, request.effective_at)
        if not isinstance(selection_id, UUID):
            raise TypeError("pre-session selection persistence returned an invalid identity")
        return OptionPreSessionProducerResult(
            discovery_batch_ids=tuple(
                package.evidence.result.batch.batch_id for package in packages
            ),
            selection_id=selection_id,
            plan=plan,
            provider_requests=collection.provider_requests,
            response_bytes=collection.response_bytes,
            catalog=tuple(sorted((row.catalog for row in candidates), key=lambda row: row.contract_id)),
            snapshots=tuple(sorted((row.snapshot for row in candidates), key=lambda row: row.contract_id)),
        )
