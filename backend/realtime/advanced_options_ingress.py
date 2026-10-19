"""Provider-free P3A contracts for Advanced Options ingress and journals."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from .advanced_equity_ingress import (
    AdvancedJournalProgress,
    AdvancedJournalPump,
)
from .advanced_ingress_plan import (
    ADVANCED_OPTIONS_INGRESS_CHANNEL,
    ADVANCED_OPTIONS_INGRESS_FEED,
    AdvancedStreamAuthorization,
)
from .advanced_ingress_runtime_profile import (
    AdvancedIngressRuntimeProfile,
)
from .advanced_journal_registry import (
    AdvancedJournalRegistration,
    build_advanced_journal_registration,
    resolve_registered_journal_path,
)
from .advanced_options_control_plane import (
    AdvancedOptionsControlPlanePlan,
)
from .buffer import RawMessageBuffer
from .contracts import (
    AssetClass,
    MarketChannel,
    MarketFeed,
    canonical_sha256,
)
from .journal import (
    NormalizedJournalRecord,
    RawJournalReader,
    RawJournalRecord,
)
from .normalizer import InstrumentAdmission
from .option_source_flow import OptionSourceFlowIdentity


ADVANCED_OPTIONS_JOURNAL_CHANNELS = frozenset({
    MarketChannel.TRADE,
    MarketChannel.QUOTE,
})
ADVANCED_OPTIONS_JOURNAL_STATUSES = frozenset({
    "WRITING",
    "DRAINING",
    "DRAINED",
})


@dataclass(frozen=True, slots=True)
class AdvancedOptionsControllerPlan:
    controller_plan_id: UUID
    controller_plan_sha256: str
    control_plane_plan_id: UUID
    authorization_id: UUID
    runtime_profile_sha256: str
    journal_generation: UUID
    stream_session_id: UUID
    source_flow_id: UUID
    cohort_id: UUID
    manifest_id: UUID
    runtime_authorized: bool = False
    service_start_authorized: bool = False
    publication_authorized: bool = False

    @property
    def can_start(self) -> bool:
        return (
            self.runtime_authorized
            and self.service_start_authorized
        )

    @property
    def can_publish(self) -> bool:
        return self.publication_authorized


def build_advanced_options_journal_registration(
    *,
    control_plane: AdvancedOptionsControlPlanePlan,
    root: Path,
    path: Path,
    stream_session_id: UUID,
    owner_epoch: int,
    normalizer_version: str,
    shard_ids: tuple[int, ...],
    shard_map_sha256: str,
) -> AdvancedJournalRegistration:
    if not isinstance(control_plane, AdvancedOptionsControlPlanePlan):
        raise TypeError("Advanced Options control plane is invalid")
    if (
        not control_plane.capability_only
        or stream_session_id
        not in control_plane.source_flow.stream_session_ids
    ):
        raise ValueError(
            "Advanced Options journal registration changed source scope"
        )
    return build_advanced_journal_registration(
        root=root,
        path=path,
        journal_generation=control_plane.source_flow.journal_generation,
        plan_id=control_plane.plan_id,
        stream_session_id=stream_session_id,
        cohort_id=control_plane.cohort_id,
        manifest_id=control_plane.manifest_id,
        session_date=control_plane.session_date,
        owner_epoch=owner_epoch,
        normalizer_version=normalizer_version,
        shard_ids=shard_ids,
        shard_map_sha256=shard_map_sha256,
    )


def build_advanced_options_controller_plan(
    *,
    control_plane: AdvancedOptionsControlPlanePlan,
    authorization: AdvancedStreamAuthorization,
    runtime_profile: AdvancedIngressRuntimeProfile,
    journal_registration: AdvancedJournalRegistration,
) -> AdvancedOptionsControllerPlan:
    if not isinstance(control_plane, AdvancedOptionsControlPlanePlan):
        raise TypeError("Advanced Options control plane is invalid")
    if not control_plane.capability_only:
        raise ValueError(
            "Advanced Options controller requires capability-only P1 scope"
        )
    if (
        not isinstance(authorization, AdvancedStreamAuthorization)
        or authorization.status != "READY"
        or authorization.feed != ADVANCED_OPTIONS_INGRESS_FEED
        or authorization.channel != ADVANCED_OPTIONS_INGRESS_CHANNEL
    ):
        raise ValueError(
            "Advanced Options controller authorization must be exact OPTIONS T/Q"
        )
    target = len(control_plane.universe.equity_tickers)
    if (
        target not in authorization.allowed_universe_targets
        or not authorization.valid_from
        <= control_plane.session_date
        <= authorization.valid_through
        or control_plane.socket_budget.stream_sessions
        > authorization.maximum_budgets.stream_sessions
        or control_plane.socket_budget.connection_attempts
        > authorization.maximum_budgets.connection_attempts
        or control_plane.socket_budget.decoded_bytes
        > authorization.maximum_budgets.decoded_bytes
    ):
        raise ValueError(
            "Advanced Options controller exceeds standing authorization"
        )
    if (
        not isinstance(runtime_profile, AdvancedIngressRuntimeProfile)
        or runtime_profile.provider_tier != "MASSIVE_ADVANCED"
        or runtime_profile.feed != ADVANCED_OPTIONS_INGRESS_FEED
        or runtime_profile.channels != ("T", "Q")
        or runtime_profile.universe_target != target
    ):
        raise ValueError(
            "Advanced Options controller runtime profile must be exact T/Q"
        )
    if not isinstance(journal_registration, AdvancedJournalRegistration):
        raise TypeError("Advanced Options journal registration is invalid")
    source_flow = control_plane.source_flow
    if (
        journal_registration.status != "REGISTERED"
        or journal_registration.plan_id != control_plane.plan_id
        or journal_registration.cohort_id != control_plane.cohort_id
        or journal_registration.manifest_id != control_plane.manifest_id
        or journal_registration.session_date != control_plane.session_date
        or journal_registration.journal_generation
        != source_flow.journal_generation
        or journal_registration.stream_session_id
        not in source_flow.stream_session_ids
    ):
        raise ValueError(
            "Advanced Options journal registration changed control-plane scope"
        )
    identity = {
        "contract": "advanced-options-controller-plan-v1",
        "control_plane_plan_id": control_plane.plan_id,
        "control_plane_sha256": control_plane.plan_sha256,
        "authorization_id": authorization.authorization_id,
        "authorization_scope_sha256": authorization.scope_sha256,
        "runtime_profile_sha256": runtime_profile.sha256,
        "runtime_configuration_sha256": (
            runtime_profile.configuration_sha256(
                authorization.runtime_policy_sha256
            )
        ),
        "journal_metadata_sha256": journal_registration.metadata_sha256,
        "source_flow_sha256": source_flow.source_flow_sha256,
        "runtime_authorized": False,
        "service_start_authorized": False,
        "publication_authorized": False,
    }
    plan_sha256 = canonical_sha256(identity)
    return AdvancedOptionsControllerPlan(
        controller_plan_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-controller:{plan_sha256}",
        ),
        controller_plan_sha256=plan_sha256,
        control_plane_plan_id=control_plane.plan_id,
        authorization_id=authorization.authorization_id,
        runtime_profile_sha256=runtime_profile.sha256,
        journal_generation=journal_registration.journal_generation,
        stream_session_id=journal_registration.stream_session_id,
        source_flow_id=source_flow.source_flow_id,
        cohort_id=control_plane.cohort_id,
        manifest_id=control_plane.manifest_id,
    )


class AdvancedOptionsJournalPump(AdvancedJournalPump):
    def __init__(
        self,
        buffer: RawMessageBuffer,
        journal_path: Path,
        journal_generation: UUID,
        admissions: Mapping[str, InstrumentAdmission],
        *,
        maximum_batch_messages: int,
        maximum_batch_bytes: int,
        on_progress: Callable[[AdvancedJournalProgress], None] | None = None,
    ) -> None:
        super().__init__(
            buffer,
            journal_path,
            journal_generation,
            admissions,
            feed=MarketFeed.OPTIONS,
            channels=ADVANCED_OPTIONS_JOURNAL_CHANNELS,
            maximum_batch_messages=maximum_batch_messages,
            maximum_batch_bytes=maximum_batch_bytes,
            on_progress=on_progress,
        )


class AdvancedOptionsJournalSource:
    def __init__(
        self,
        *,
        root: Path,
        registration: AdvancedJournalRegistration,
        source_flow: OptionSourceFlowIdentity,
        reader_factory: Callable[[Path], RawJournalReader] = RawJournalReader,
    ) -> None:
        if not isinstance(registration, AdvancedJournalRegistration):
            raise TypeError("Advanced Options journal registration is invalid")
        if not isinstance(source_flow, OptionSourceFlowIdentity):
            raise TypeError("Advanced Options source-flow identity is invalid")
        if registration.status not in ADVANCED_OPTIONS_JOURNAL_STATUSES:
            raise ValueError(
                "Advanced Options journal is not admitted for consumption"
            )
        if (
            registration.journal_generation
            != source_flow.journal_generation
            or registration.stream_session_id
            not in source_flow.stream_session_ids
        ):
            raise ValueError(
                "Advanced Options journal source changed source-flow identity"
            )
        if not callable(reader_factory):
            raise TypeError("Advanced Options journal reader factory is invalid")
        self.registration = registration
        self.source_flow = source_flow
        self.path = resolve_registered_journal_path(
            root,
            registration.relative_path,
        )
        self._reader = reader_factory(self.path)
        self.generation = registration.journal_generation

    def last_offset(self) -> int:
        return self.registration.raw_committed_offset

    def last_event_offset(self) -> int:
        return self.registration.normalized_committed_offset

    def read_after(
        self,
        raw_offset: int,
        *,
        limit: int = 1000,
    ) -> Sequence[RawJournalRecord]:
        records = self._reader.read_after(raw_offset, limit=limit)
        previous = raw_offset
        for record in records:
            if (
                record.raw_offset <= previous
                or record.generation != self.generation
                or record.stream_session_id
                not in self.source_flow.stream_session_ids
                or record.feed is not MarketFeed.OPTIONS
            ):
                raise ValueError(
                    "Advanced Options journal source identity mismatch"
                )
            if (
                self.registration.status in {"DRAINING", "DRAINED"}
                and record.raw_offset
                > self.registration.raw_committed_offset
            ):
                raise ValueError(
                    "Advanced Options source exceeds registered durable offset"
                )
            previous = record.raw_offset
        return records

    def read_normalized_after(
        self,
        event_offset: int,
        *,
        limit: int = 1000,
    ) -> Sequence[NormalizedJournalRecord]:
        records = self._reader.read_normalized_after(
            event_offset,
            limit=limit,
        )
        previous = event_offset
        for record in records:
            if (
                record.event_offset <= previous
                or record.journal_generation != self.generation
                or record.stream_session_id
                not in self.source_flow.stream_session_ids
                or record.normalizer_version
                != self.registration.normalizer_version
                or record.asset_class is not AssetClass.OPTION
                or record.event_kind
                not in ADVANCED_OPTIONS_JOURNAL_CHANNELS
            ):
                raise ValueError(
                    "Advanced Options normalized source identity mismatch"
                )
            if (
                self.registration.status in {"DRAINING", "DRAINED"}
                and record.event_offset
                > self.registration.normalized_committed_offset
            ):
                raise ValueError(
                    "Advanced Options normalized source exceeds durable offset"
                )
            previous = record.event_offset
        return records
