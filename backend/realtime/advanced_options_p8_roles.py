"""Bounded production role implementations for the four P8 Docker services."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

from options.config import FlowStrategyPolicy
from psycopg2.extras import execute_batch
from psycopg2.pool import ThreadedConnectionPool

from .advanced_gateway import AdvancedGateway, GatewayConfiguration
from .advanced_options_acquisition_runtime import (
    ACQUISITION_STATUS_SCHEMA,
)
from .advanced_journal_registry import (
    build_advanced_journal_registration,
    load_advanced_journal_registration_cursor,
    mark_advanced_journal_writing_cursor,
    register_advanced_journal_cursor,
    seal_advanced_journal_cursor,
    update_advanced_journal_offsets_cursor,
)
from .advanced_options_consumers import (
    AdvancedOptionsBoundedDetectorRouter,
    AdvancedOptionsMarketStateConsumer,
    load_advanced_options_detector_routing_state_cursor,
    load_advanced_options_market_state_cursor,
    persist_advanced_options_detector_routing_state_cursor,
    persist_advanced_options_market_state_slice_cursor,
)
from .advanced_options_activity_momentum_c6 import (
    ACTIVITY_MOMENTUM_PROFILE_VERSION,
    activity_momentum_allocation_from_payload,
)
from .advanced_options_control_plane import (
    CANONICAL_MARKET_STATE_OWNER,
)
from .advanced_options_flow_policy import (
    AdvancedOptionsContractScope,
    AdvancedOptionsContractScopeEntry,
    load_advanced_options_flow_policy,
)
from .advanced_options_ingress import (
    AdvancedOptionsJournalPump,
    AdvancedOptionsJournalSource,
)
from .advanced_options_p8_production import (
    p8_production_binding_from_payload,
)
from .advanced_options_provider_contract import (
    MASSIVE_OPTIONS_PROVIDER_CONTRACT,
    load_default_options_exchange_reference,
)
from .advanced_options_resident_barrier import (
    ResidentCompletion,
    ResidentCompletionBarrierState,
    ResidentCompletionMember,
    ResidentCompletionStatus,
    evaluate_resident_completion_barrier_cursor,
    load_ready_resident_window_targets_cursor,
    record_resident_completion_cursor,
)
from .advanced_options_v3_authority import (
    consume_session_acquisition_authority_cursor,
    load_window_cycle_authorization_cursor,
    session_acquisition_authorization_from_payload,
)
from .advanced_options_v3_deployment import (
    validate_advanced_options_v3_runtime_identity,
)
from .advanced_options_service import AdvancedOptionsProcessKind
from .advanced_options_service import AdvancedOptionsPostgresLeasePort
from .advanced_options_selection_c6 import (
    C6ConsumerState,
    C6SelectionPlan,
    C6ShardState,
    C6SubscriptionShard,
    persist_c6_consumer_state_cursor,
    persist_c6_physical_subscription_state_cursor,
    persist_c6_shard_state_cursor,
)
from .buffer import BufferLimits, RawMessageBuffer
from .contracts import (
    AssetClass,
    MarketChannel,
    MarketFeed,
    SubscriptionManifest,
    SubscriptionMember,
    UniverseManifest,
    canonical_sha256,
)
from .database_access_v2 import (
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from .materialization import MaterializerShardMap
from .journal import RawJournalReader
from .migrations import EXPECTED_V2_DATABASE_NAME
from .normalizer import InstrumentAdmission, NORMALIZER_VERSION
from .option_flow.conditions import load_default_live_semantics
from .option_flow.checkpoint_state import shard_map_sha256
from .option_flow.consumer import ConsumerLimits
from .option_flow.volume import BaselineKind
from .option_source_flow import (
    build_option_source_flow_identity,
    persist_option_contract_scope_cursor,
    persist_option_flow_policy_cursor,
    persist_option_source_flow_cursor,
)
from .orchestration_repository import (
    PostgresOrchestrationRepository,
    WindowRecord,
)
from .runner_environment import RunnerEnvironment
from .stage_graph import STAGE_CATALOG_VERSION_V5
from .stage_orchestrator import StageContext
from .stage_runners_options import OptionsFlowSettings, options_flow_runner
from .advanced_options_service import OPTIONS_LATE_CANARY_V4_STAGE_ORDER


UTC = timezone.utc
_EFFECTS = {
    "dispatch": False,
    "delivery": False,
    "broker": False,
    "trading": False,
}


def _record_v3_resident_completion(
    provider,
    binding,
    *,
    bundle,
    member: ResidentCompletionMember,
    journal_generation: UUID,
    owner_epoch: int,
    ending_offset: int,
    evidence: dict[str, object],
    target_window_id: UUID | None = None,
) -> None:
    if (
        bundle.get("orchestration", {}).get("catalog_version")
        != STAGE_CATALOG_VERSION_V5
    ):
        return
    completion = ResidentCompletion(
        target_window_id=target_window_id or binding.window_id,
        target_cohort_id=binding.enrollment.control_plane.cohort_id,
        member=member,
        journal_generation=journal_generation,
        owner_epoch=owner_epoch,
        ending_offset=ending_offset,
        evidence_sha256=canonical_sha256(evidence),
        evidence=evidence,
        status=ResidentCompletionStatus.COMPLETE,
        reason_codes=(),
        completed_at=datetime.now(UTC),
    )
    with provider.transaction(APP_ROLE) as unit:
        record_resident_completion_cursor(unit.cursor, completion)


def _snapshot_v3_resident_windows(
    provider,
    binding,
    *,
    bundle,
    member: ResidentCompletionMember,
    owner_epoch: int,
    reader: RawJournalReader,
    raw_offset: int,
    normalized_offset: int,
    flow_checkpoint_offset: int | None = None,
    connection_attempts: int = 1,
    shard_map_sha256_value: str | None = None,
    shard_ids_value: tuple[int, ...] | None = None,
    evidence_ref: str | None = None,
) -> int:
    if (
        bundle.get("orchestration", {}).get("catalog_version")
        != STAGE_CATALOG_VERSION_V5
    ):
        return 0
    authorities = bundle.get("v3_authorities")
    if not isinstance(authorities, dict):
        raise RuntimeError("V3 session authority is unavailable")
    session = session_acquisition_authorization_from_payload(
        authorities.get("session_acquisition")
    )
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        targets = load_ready_resident_window_targets_cursor(
            unit.cursor,
            session_authorization_id=session.authorization_id,
            member=member,
        )
    recorded = 0
    for target in targets:
        target_raw, target_event = reader.offsets_at(
            target.options_cutoff
        )
        if target_raw <= 0 or target_event <= 0:
            continue
        if member is ResidentCompletionMember.INGRESS:
            ready = (
                raw_offset >= target_raw
                and normalized_offset >= target_event
            )
            evidence = {
                "status": "COMPLETE",
                "kind": member.value,
                "raw_end_offset": target_raw,
                "normalized_end_offset": target_event,
                "connection_attempts": connection_attempts,
                "effects": dict(_EFFECTS),
            }
            ending_offset = target_event
        elif member is ResidentCompletionMember.MARKET_STATE:
            ready = normalized_offset >= target_event
            if (
                shard_map_sha256_value is None
                or shard_ids_value is None
            ):
                raise RuntimeError(
                    "V3 market-state snapshot lacks shard identity"
                )
            evidence = {
                "status": "COMPLETE",
                "kind": member.value,
                "normalized_end_offset": target_event,
                "state_sha256": canonical_sha256({
                    "target_window_id": str(target.target_window_id),
                    "journal_generation": str(
                        target.journal_generation
                    ),
                    "normalized_end_offset": target_event,
                }),
                "shard_map_sha256": shard_map_sha256_value,
                "shard_ids": list(shard_ids_value),
                "effects": dict(_EFFECTS),
            }
            ending_offset = target_event
        else:
            ready = (
                raw_offset >= target_raw
                and normalized_offset >= target_event
                and flow_checkpoint_offset is not None
                and flow_checkpoint_offset >= target_raw
            )
            if (
                evidence_ref is None
                or shard_map_sha256_value is None
                or shard_ids_value is None
            ):
                raise RuntimeError(
                    "V3 flow snapshot lacks durable evidence"
                )
            evidence = {
                "status": "COMPLETE",
                "kind": member.value,
                "raw_end_offset": target_raw,
                "normalized_end_offset": target_event,
                "checkpoint_end_offset": target_raw,
                "owner_epoch": owner_epoch,
                "evidence_ref": (
                    f"{evidence_ref}:window:{target.target_window_id}:"
                    f"{target_raw}:{target_event}"
                ),
                "shard_map_sha256": shard_map_sha256_value,
                "shard_ids": list(shard_ids_value),
                "effects": dict(_EFFECTS),
            }
            ending_offset = target_raw
        if not ready:
            continue
        _record_v3_resident_completion(
            provider,
            binding,
            bundle=bundle,
            member=member,
            journal_generation=target.journal_generation,
            owner_epoch=owner_epoch,
            ending_offset=ending_offset,
            evidence=evidence,
            target_window_id=target.target_window_id,
        )
        recorded += 1
    return recorded


def _v3_resident_dependencies(provider, binding, bundle):
    if (
        bundle.get("orchestration", {}).get("catalog_version")
        != STAGE_CATALOG_VERSION_V5
    ):
        return None
    control = binding.enrollment.control_plane
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        verdict = evaluate_resident_completion_barrier_cursor(
            unit.cursor,
            target_window_id=binding.window_id,
            target_cohort_id=control.cohort_id,
            journal_generation=control.source_flow.journal_generation,
        )
    if verdict.state is ResidentCompletionBarrierState.FAILED:
        raise RuntimeError(
            "P8 V3 resident completion failed: "
            + ",".join(verdict.reason_codes)
        )
    if verdict.state is ResidentCompletionBarrierState.WAITING_INPUT:
        return {
            "status": "DEFERRED",
            "reason_codes": verdict.reason_codes,
        }
    return {
        member.member.value: dict(member.evidence)
        for member in verdict.members
    }


def _record_v3_resident_failure(
    provider,
    binding,
    *,
    bundle,
    kind: AdvancedOptionsProcessKind,
    error: BaseException,
) -> None:
    member_by_kind = {
        AdvancedOptionsProcessKind.INGRESS: ResidentCompletionMember.INGRESS,
        AdvancedOptionsProcessKind.MARKET_STATE: (
            ResidentCompletionMember.MARKET_STATE
        ),
        AdvancedOptionsProcessKind.FLOW: ResidentCompletionMember.FLOW,
    }
    member = member_by_kind.get(kind)
    if (
        member is None
        or bundle.get("orchestration", {}).get("catalog_version")
        != STAGE_CATALOG_VERSION_V5
    ):
        return
    try:
        _owner_id, owner_epoch = _lease(provider, bundle, kind)
    except BaseException:
        return
    reason = f"ROLE_EXCEPTION:{type(error).__name__}"
    evidence = {
        "status": "FAILED",
        "kind": kind.value,
        "reason_codes": [reason],
        "effects": dict(_EFFECTS),
    }
    completion = ResidentCompletion(
        target_window_id=binding.window_id,
        target_cohort_id=binding.enrollment.control_plane.cohort_id,
        member=member,
        journal_generation=(
            binding.enrollment.control_plane.source_flow.journal_generation
        ),
        owner_epoch=owner_epoch,
        ending_offset=0,
        evidence_sha256=canonical_sha256(evidence),
        evidence=evidence,
        status=ResidentCompletionStatus.FAILED,
        reason_codes=(reason,),
        completed_at=datetime.now(UTC),
    )
    with provider.transaction(APP_ROLE) as unit:
        record_resident_completion_cursor(unit.cursor, completion)


def _provider(environment):
    dsn = str(environment.get("REALTIME_V2_APP_DSN", "")).strip()
    if not dsn:
        raise RuntimeError("P8 role database credential is unavailable")
    pool = ThreadedConnectionPool(
        1,
        4,
        dsn=dsn,
        options="-c timezone=UTC",
    )
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
            statement_timeout_ms=300_000,
        ),
    )
    provider.verify()
    return provider, pool


def _journal_root(environment) -> Path:
    root = Path(str(
        environment.get(
            "REALTIME_V2_ADVANCED_JOURNAL_DIR",
            "/var/lib/realtime-v2-advanced",
        )
    )).resolve()
    if not root.is_dir():
        raise RuntimeError("P8 journal root is unavailable")
    return root


def _journal_path(root: Path, package) -> Path:
    directory = root / "p8" / str(package["package_nonce"])
    directory.mkdir(parents=True, exist_ok=True)
    return directory / "options.sqlite"


def _service(bundle, kind):
    rows = [
        row for row in bundle["services"] if row["kind"] == kind.value
    ]
    if len(rows) != 1:
        raise ValueError("P8 role service identity is not exact")
    return rows[0]


def _lease(provider, bundle, kind):
    service = _service(bundle, kind)
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT owner_id::text,owner_epoch,lease_until "
            "FROM rt.worker_leases WHERE resource_key=%s",
            (service["lease_resource_key"],),
        )
        row = unit.cursor.fetchone()
    if (
        row is None
        or str(row[0]) != service["owner_id"]
        or int(row[1]) < 1
        or row[2] <= datetime.now(UTC)
    ):
        raise RuntimeError(f"P8 {kind.value} lease is unavailable")
    return UUID(str(row[0])), int(row[1])


def _registration(provider, generation):
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        return load_advanced_journal_registration_cursor(
            unit.cursor,
            generation,
        )


def _wait_registration(provider, generation, stop_event):
    while not stop_event.is_set():
        registration = _registration(provider, generation)
        if registration is not None and registration.status in {
            "WRITING",
            "DRAINING",
            "DRAINED",
        }:
            return registration
        stop_event.wait(0.25)
    raise RuntimeError("P8 role stopped before journal registration")


def _wait_v3_binding(provider, binding, bundle, stop_event):
    from .advanced_options_p8_production import (
        load_v3_pending_window_bindings,
    )

    while not stop_event.is_set():
        values = load_v3_pending_window_bindings(
            provider=provider,
            binding=binding,
            bundle=bundle,
            available_at=datetime.now(UTC),
        )
        if values:
            return values[0]
        stop_event.wait(0.1)
    raise RuntimeError("P8 role stopped before exact E binding")


def _downstream_binding_evidence(provider, binding, bundle):
    authorities = bundle.get("v3_authorities")
    if not isinstance(authorities, dict):
        raise RuntimeError("V3 session authority is unavailable")
    session = session_acquisition_authorization_from_payload(
        authorities.get("session_acquisition")
    )
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        authority = load_window_cycle_authorization_cursor(
            unit.cursor,
            target_window_id=binding.window_id,
            session_authorization_id=session.authorization_id,
        )
    equity = binding.equity_input
    e_o_seconds = (
        authority.options_cutoff - equity.equity_cutoff
    ).total_seconds()
    if (
        authority.target_cohort_id
        != binding.enrollment.control_plane.cohort_id
        or authority.equity_binding_sha256
        != equity.binding_sha256
        or authority.equity_receipt_id != equity.equity_receipt_id
        or authority.equity_cutoff != equity.equity_cutoff
        or not 0 <= e_o_seconds <= 300
    ):
        raise RuntimeError("P8 exact per-E downstream binding changed")
    evidence = {
        "schema": "advanced-options-p8-downstream-binding-v1",
        "session_authorization_id": str(session.authorization_id),
        "target_window_id": str(binding.window_id),
        "equity_receipt_id": str(equity.equity_receipt_id),
        "equity_input_sha256": equity.input_sha256,
        "finalized_cutoff_utc": equity.equity_cutoff.isoformat(),
        "options_cutoff_utc": authority.options_cutoff.isoformat(),
        "e_o_seconds": e_o_seconds,
        "equity_binding_sha256": equity.binding_sha256,
        "window_cycle_authorization_sha256": (
            authority.authorization_sha256
        ),
    }
    evidence["binding_sha256"] = canonical_sha256(evidence)
    return evidence


def _load_acquisition_document(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError(
            "V3 acquisition binding is unavailable"
        ) from error


def _acquisition_document(path: Path):
    from .advanced_options_p8_runtime import (
        LegacyArtifactSessionStateAdapter,
    )

    return LegacyArtifactSessionStateAdapter(
        acquisition_binding_path=path,
        acquisition_loader=_load_acquisition_document,
    ).load_acquisition_binding()


def _run_v3_acquisition_observer(
    *,
    package,
    bundle,
    binding,
    provider,
    environment,
    stop_event,
    session_state=None,
):
    control = binding.enrollment.control_plane
    acquisition = (
        _acquisition_document(Path(str(
            environment.get(
                "REALTIME_V3_OPTIONS_ACQUISITION_BINDING_PATH",
                "/run/advanced-options-acquisition/binding.json",
            )
        )))
        if session_state is None
        else session_state.load_acquisition_binding()
    )
    authorities = bundle.get("v3_authorities")
    if not isinstance(authorities, dict):
        raise RuntimeError("V3 session authority is unavailable")
    session = session_acquisition_authorization_from_payload(
        authorities.get("session_acquisition")
    )
    contracts = tuple(sorted(
        row.contract_ticker
        for row in binding.enrollment.activity_selection.contracts
    ))
    if (
        acquisition.authority != session
        or acquisition.authority.cohort_id != control.cohort_id
        or acquisition.authority.journal_generation
        != control.source_flow.journal_generation
        or acquisition.contract_tickers != contracts
    ):
        raise RuntimeError("V3 acquisition/downstream identity changed")
    selection_plan = _selection_plan(session_state)
    _owner_id, owner_epoch = _lease(
        provider,
        bundle,
        AdvancedOptionsProcessKind.INGRESS,
    )
    root = _journal_root(environment)
    path = (
        root
        / "options-acquisition"
        / str(acquisition.authority.journal_generation)
        / "options.sqlite"
    )
    while not path.is_file() and not stop_event.is_set():
        stop_event.wait(0.1)
    if not path.is_file():
        raise RuntimeError("V3 acquisition journal is unavailable")
    shard_map = _shard_map(binding, selection_plan)
    registration = _registration(
        provider,
        acquisition.authority.journal_generation,
    )
    if registration is None:
        registration = build_advanced_journal_registration(
            root=root,
            path=path,
            journal_generation=acquisition.authority.journal_generation,
            plan_id=control.plan_id,
            stream_session_id=acquisition.stream_session_id,
            cohort_id=control.cohort_id,
            manifest_id=control.manifest_id,
            session_date=control.session_date,
            owner_epoch=owner_epoch,
            normalizer_version=NORMALIZER_VERSION,
            shard_ids=tuple(range(shard_map.shard_count)),
            shard_map_sha256=shard_map.sha256,
        )
        with provider.transaction(APP_ROLE) as unit:
            register_advanced_journal_cursor(unit.cursor, registration)
            mark_advanced_journal_writing_cursor(
                unit.cursor,
                registration.journal_generation,
                checked_at=datetime.now(UTC),
            )
    elif (
        registration.shard_ids
        != tuple(range(shard_map.shard_count))
        or registration.shard_map_sha256 != shard_map.sha256
    ):
        raise RuntimeError(
            "V3 acquisition journal logical shard registration changed"
        )
    status_path = Path(str(environment.get(
        "REALTIME_V3_OPTIONS_ACQUISITION_STATUS_PATH",
        "/var/lib/realtime-v2-reports/options-acquisition-status.json",
    )))
    last_raw_offset = 0
    last_event_offset = 0
    connection_attempts = 0
    last_physical_signature = None
    while not stop_event.is_set():
        try:
            status = json.loads(status_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            stop_event.wait(0.1)
            continue
        if (
            not isinstance(status, dict)
            or status.get("schema") != ACQUISITION_STATUS_SCHEMA
            or status.get("binding_sha256")
            != acquisition.binding_sha256
            or status.get("owner_id") != str(acquisition.owner_id)
            or status.get("journal_generation")
            != str(acquisition.authority.journal_generation)
        ):
            raise RuntimeError("V3 acquisition durable status changed")
        last_raw_offset = int(status.get("last_raw_offset", 0))
        last_event_offset = int(status.get("last_event_offset", 0))
        connection_attempts = int(
            status.get("connection_attempts", 0)
        )
        decoded_messages = int(status.get("decoded_messages", 0))
        decoded_bytes = int(status.get("decoded_bytes", 0))
        transport_gaps = tuple(
            dict(row)
            for row in (status.get("transport_gaps") or ())
        )
        physical_signature = canonical_sha256({
            "state": status.get("state"),
            "decoded_messages": decoded_messages,
            "decoded_bytes": decoded_bytes,
            "connection_attempts": connection_attempts,
            "transport_gaps": transport_gaps,
        })
        observed_at = datetime.now(UTC)
        with provider.transaction(APP_ROLE) as unit:
            update_advanced_journal_offsets_cursor(
                unit.cursor,
                registration.journal_generation,
                raw_committed_offset=last_raw_offset,
                normalized_committed_offset=last_event_offset,
                bridge_committed_offset=None,
                checked_at=observed_at,
            )
            if (
                selection_plan is not None
                and physical_signature != last_physical_signature
            ):
                persist_c6_physical_subscription_state_cursor(
                    unit.cursor,
                    selection_generation_id=(
                        selection_plan.generation_id
                    ),
                    subscription_id=(
                        selection_plan
                        .physical_subscription
                        .subscription_id
                    ),
                    state=(
                        C6ShardState.READY
                        if status.get("state") == "COMPLETE"
                        else C6ShardState.WARMING
                    ),
                    message_count=decoded_messages,
                    byte_count=decoded_bytes,
                    reconnect_count=max(
                        len(transport_gaps),
                        connection_attempts - 1,
                    ),
                    gaps=transport_gaps,
                    reason_codes=(),
                    observed_at=observed_at,
                )
                last_physical_signature = physical_signature
        if last_raw_offset > 0 and last_event_offset > 0:
            _snapshot_v3_resident_windows(
                provider,
                binding,
                bundle=bundle,
                member=ResidentCompletionMember.INGRESS,
                owner_epoch=owner_epoch,
                reader=RawJournalReader(path),
                raw_offset=last_raw_offset,
                normalized_offset=last_event_offset,
                connection_attempts=connection_attempts,
            )
        if status.get("state") == "COMPLETE":
            break
        stop_event.wait(0.1)
    if last_raw_offset <= 0 or last_event_offset <= 0:
        raise RuntimeError("V3 acquisition journal has no durable data")
    with provider.transaction(APP_ROLE) as unit:
        seal_advanced_journal_cursor(
            unit.cursor,
            registration.journal_generation,
            raw_committed_offset=last_raw_offset,
            normalized_committed_offset=last_event_offset,
            sealed_files_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            checked_at=datetime.now(UTC),
        )
    return {
        "status": "COMPLETE",
        "kind": AdvancedOptionsProcessKind.INGRESS.value,
        "raw_end_offset": last_raw_offset,
        "normalized_end_offset": last_event_offset,
        "connection_attempts": connection_attempts,
        "acquisition_owner_id": str(acquisition.owner_id),
        "journal_generation": str(
            acquisition.authority.journal_generation
        ),
        "effects": dict(_EFFECTS),
    }


def _manifest(binding):
    control = binding.enrollment.control_plane
    members = (
        *(
            SubscriptionMember(
                AssetClass.EQUITY,
                ticker,
                ticker,
                MarketChannel.MINUTE_AGGREGATE,
            )
            for ticker in control.universe.equity_tickers
        ),
        *(
            SubscriptionMember(
                AssetClass.OPTION,
                row.contract_ticker,
                row.underlyer,
                channel,
                catalog_version_id=row.catalog_version_id,
            )
            for row in binding.enrollment.activity_selection.contracts
            for channel in (MarketChannel.TRADE, MarketChannel.QUOTE)
        ),
    )
    return SubscriptionManifest(
        version="advanced-options-p8-live-tq-v1",
        session_date=control.session_date,
        universe=UniverseManifest(
            "advanced-options-p8-live-tq-v1",
            control.universe.equity_tickers,
            control.universe.option_underlyings,
        ),
        capability_profile_sha256=control.profile_sha256,
        allocation_policy_sha256=control.slot_set_sha256,
        members=members,
        maximum_option_quote_contracts=len(
            binding.enrollment.activity_selection.contracts
        ),
    )


@dataclass(frozen=True, slots=True)
class _C6ShardLayout:
    shard_map: MaterializerShardMap
    persistent_shards: tuple[C6SubscriptionShard, ...]


def _selection_plan(session_state) -> C6SelectionPlan | None:
    if session_state is None:
        return None
    loader = getattr(session_state, "load_c6_selection_plan", None)
    if not callable(loader):
        return None
    plan = loader()
    if plan is not None and not isinstance(plan, C6SelectionPlan):
        raise TypeError("C6 session selection plan is invalid")
    return plan


def _shard_layout(binding, selection_plan=None) -> _C6ShardLayout:
    contracts = tuple(
        binding.enrollment.activity_selection.contracts
    )
    if selection_plan is None:
        return _C6ShardLayout(
            MaterializerShardMap(
                "advanced-options-p8-market-state-v1",
                1,
                tuple((row.instrument_id, 0) for row in contracts),
            ),
            (),
        )
    selected_tickers = {
        row.contract_ticker for row in contracts
    }
    if selected_tickers != {
        row.contract_ticker for row in selection_plan.members
    }:
        raise ValueError(
            "C6 logical shard membership diverges from enrollment"
        )
    shards = tuple(sorted(
        selection_plan.shards,
        key=lambda row: row.shard_slot,
    ))
    shard_index_by_ticker = {
        ticker: index
        for index, shard in enumerate(shards)
        for ticker in shard.contract_tickers
    }
    if set(shard_index_by_ticker) != selected_tickers:
        raise ValueError("C6 logical shard routing is incomplete")
    return _C6ShardLayout(
        MaterializerShardMap(
            "advanced-options-c6-logical-shards-v1",
            len(shards),
            tuple(
                (
                    row.instrument_id,
                    shard_index_by_ticker[row.contract_ticker],
                )
                for row in contracts
            ),
        ),
        shards,
    )


def _admissions(binding, selection_plan=None):
    shard_map = _shard_layout(binding, selection_plan).shard_map
    return {
        row.contract_ticker: InstrumentAdmission(
            row.instrument_id,
            row.contract_ticker,
            row.underlyer,
            AssetClass.OPTION,
            shard_map.shard_for(row.instrument_id),
        )
        for row in binding.enrollment.activity_selection.contracts
    }


def _shard_map(binding, selection_plan=None):
    return _shard_layout(binding, selection_plan).shard_map


def _source_timestamp_ns(value, unit):
    multipliers = {
        "s": 1_000_000_000,
        "SECOND": 1_000_000_000,
        "ms": 1_000_000,
        "MILLISECOND": 1_000_000,
        "us": 1_000,
        "MICROSECOND": 1_000,
        "ns": 1,
        "NANOSECOND": 1,
    }
    multiplier = multipliers.get(str(unit))
    return (
        None
        if type(value) is not int or multiplier is None
        else value * multiplier
    )


def _persist_c6_market_progress_cursor(
    cursor,
    *,
    selection_plan,
    layout,
    planned,
    state,
    observed_at,
):
    if selection_plan is None:
        return
    for materialized, checkpoint in zip(
        planned.states,
        planned.checkpoints,
    ):
        shard = layout.persistent_shards[materialized.shard_id]
        events = (
            *materialized.quotes,
            *materialized.trades,
        )
        event_times = tuple(row.event_at for row in events)
        watermarks = tuple(
            value
            for value in (
                _source_timestamp_ns(
                    row.source_timestamp,
                    row.source_timestamp_unit,
                )
                for row in events
            )
            if value is not None
        )
        persist_c6_shard_state_cursor(
            cursor,
            selection_generation_id=selection_plan.generation_id,
            shard_id=shard.shard_id,
            state=state,
            message_count=checkpoint.committed_offset,
            byte_count=0,
            reason_codes=(),
            observed_at=observed_at,
        )
        persist_c6_consumer_state_cursor(
            cursor,
            selection_generation_id=selection_plan.generation_id,
            shard_id=shard.shard_id,
            state=C6ConsumerState(
                consumer_name=CANONICAL_MARKET_STATE_OWNER,
                state_version=planned.shard_map.sha256,
                committed_offset=checkpoint.committed_offset,
                event_watermark_ns=(
                    None if not watermarks else max(watermarks)
                ),
                history_start=(
                    None if not event_times else min(event_times)
                ),
                history_end=(
                    None if not event_times else max(event_times)
                ),
                missing_feed_states=(),
                updated_at=observed_at,
            ),
        )


def _run_ingress(
    *,
    package,
    bundle,
    binding,
    provider,
    environment,
    stop_event,
    session_state=None,
):
    control = binding.enrollment.control_plane
    if (
        bundle.get("orchestration", {}).get("catalog_version")
        == STAGE_CATALOG_VERSION_V5
    ):
        validate_advanced_options_v3_runtime_identity(package, bundle)
        return _run_v3_acquisition_observer(
            package=package,
            bundle=bundle,
            binding=binding,
            provider=provider,
            environment=environment,
            stop_event=stop_event,
            session_state=session_state,
        )
    owner_id, owner_epoch = _lease(
        provider,
        bundle,
        AdvancedOptionsProcessKind.INGRESS,
    )
    root = _journal_root(environment)
    path = _journal_path(root, package)
    shard_map = _shard_map(binding)
    registration = build_advanced_journal_registration(
        root=root,
        path=path,
        journal_generation=control.source_flow.journal_generation,
        plan_id=control.plan_id,
        stream_session_id=control.source_flow.stream_session_ids[0],
        cohort_id=control.cohort_id,
        manifest_id=control.manifest_id,
        session_date=control.session_date,
        owner_epoch=owner_epoch,
        normalizer_version=NORMALIZER_VERSION,
        shard_ids=(0,),
        shard_map_sha256=shard_map.sha256,
    )
    with provider.transaction(APP_ROLE) as unit:
        register_advanced_journal_cursor(unit.cursor, registration)
        mark_advanced_journal_writing_cursor(
            unit.cursor,
            registration.journal_generation,
            checked_at=datetime.now(UTC),
        )

    latest_progress = [None]
    attempts_state = [1]

    def progress(value):
        latest_progress[0] = value
        with provider.transaction(APP_ROLE) as unit:
            update_advanced_journal_offsets_cursor(
                unit.cursor,
                registration.journal_generation,
                raw_committed_offset=value.last_raw_offset,
                normalized_committed_offset=value.last_event_offset,
                bridge_committed_offset=None,
                checked_at=datetime.now(UTC),
            )
        _snapshot_v3_resident_windows(
            provider,
            binding,
            bundle=bundle,
            member=ResidentCompletionMember.INGRESS,
            owner_epoch=owner_epoch,
            reader=RawJournalReader(path),
            raw_offset=value.last_raw_offset,
            normalized_offset=value.last_event_offset,
            connection_attempts=attempts_state[0],
        )

    limits = package["websocket_ingress"]
    buffer = RawMessageBuffer(
        BufferLimits(
            int(limits["decoded_messages"]),
            int(limits["decoded_bytes"]),
            60_000_000_000,
        ),
        monotonic_ns=time.monotonic_ns,
    )
    pump = AdvancedOptionsJournalPump(
        buffer,
        path,
        registration.journal_generation,
        _admissions(binding),
        maximum_batch_messages=500,
        maximum_batch_bytes=min(
            int(limits["maximum_frame_bytes"]) * 4,
            4 * 1024 * 1024,
        ),
        on_progress=progress,
    )
    configuration = GatewayConfiguration(
        MarketFeed.OPTIONS,
        GatewayConfiguration.options().endpoint,
        open_timeout_seconds=int(limits["open_timeout_seconds"]),
        ping_interval_seconds=int(limits["ping_interval_seconds"]),
        ping_timeout_seconds=int(limits["ping_timeout_seconds"]),
        close_timeout_seconds=int(limits["close_timeout_seconds"]),
        maximum_message_bytes=int(limits["maximum_frame_bytes"]),
        library_max_queue=int(limits["library_max_queue"]),
    )
    gateway = AdvancedGateway(
        configuration,
        _manifest(binding),
        buffer,
        api_key=str(environment["POLYGON_API_KEY"]),
        monotonic_ns=time.monotonic_ns,
        session_id_factory=lambda: registration.stream_session_id,
    )

    async def capture():
        attempts = 0
        stop = datetime.fromisoformat(package["window"]["stop_utc"])
        continuous = (
            bundle.get("orchestration", {}).get("catalog_version")
            == STAGE_CATALOG_VERSION_V5
        )
        stop_at = (
            stop
            if continuous
            else min(
                stop,
                control.receipts.options_input_ready.spec.cutoff,
            )
        )
        message_target = (
            int(limits["decoded_messages"])
            if continuous
            else min(int(limits["decoded_messages"]), 2_000)
        )
        while (
            not stop_event.is_set()
            and datetime.now(UTC) < stop_at
            and attempts < int(limits["connection_attempts"])
            and gateway.received_messages < message_target
        ):
            attempts += 1
            attempts_state[0] = attempts
            remaining = max(
                0.1,
                (stop_at - datetime.now(UTC)).total_seconds(),
            )
            try:
                await asyncio.wait_for(
                    gateway.run_session(
                        maximum_market_messages=message_target
                    ),
                    timeout=remaining,
                )
            except asyncio.TimeoutError:
                break
            if gateway.received_messages >= message_target:
                break
            if attempts < int(limits["connection_attempts"]):
                await asyncio.sleep(min(5.0 * (2 ** (attempts - 1)), 60.0))
        if continuous:
            while (
                not stop_event.is_set()
                and datetime.now(UTC) < stop_at
            ):
                value = latest_progress[0]
                if value is not None:
                    _snapshot_v3_resident_windows(
                        provider,
                        binding,
                        bundle=bundle,
                        member=ResidentCompletionMember.INGRESS,
                        owner_epoch=owner_epoch,
                        reader=RawJournalReader(path),
                        raw_offset=value.last_raw_offset,
                        normalized_offset=value.last_event_offset,
                        connection_attempts=max(1, attempts),
                    )
                await asyncio.sleep(0.1)
        return attempts

    pump.start()
    try:
        attempts = asyncio.run(capture())
    finally:
        pump.request_stop()
    final = pump.join(30)
    if final.last_raw_offset <= 0 or final.last_event_offset <= 0:
        raise RuntimeError("P8 ingress captured no canonical T/Q data")
    channels = {
        row.event_kind
        for row in RawJournalReader(path).read_normalized_after(
            0,
            limit=final.last_event_offset,
        )
    }
    if channels != {MarketChannel.TRADE, MarketChannel.QUOTE}:
        raise RuntimeError("P8 ingress did not capture exact T/Q membership")
    with provider.transaction(APP_ROLE) as unit:
        seal_advanced_journal_cursor(
            unit.cursor,
            registration.journal_generation,
            raw_committed_offset=final.last_raw_offset,
            normalized_committed_offset=final.last_event_offset,
            sealed_files_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            checked_at=datetime.now(UTC),
        )
    evidence = {
        "status": "COMPLETE",
        "kind": AdvancedOptionsProcessKind.INGRESS.value,
        "raw_end_offset": final.last_raw_offset,
        "normalized_end_offset": final.last_event_offset,
        "connection_attempts": attempts,
        "effects": dict(_EFFECTS),
    }
    if (
        bundle.get("orchestration", {}).get("catalog_version")
        == STAGE_CATALOG_VERSION_V5
    ):
        _snapshot_v3_resident_windows(
            provider,
            binding,
            bundle=bundle,
            member=ResidentCompletionMember.INGRESS,
            owner_epoch=owner_epoch,
            reader=RawJournalReader(path),
            raw_offset=final.last_raw_offset,
            normalized_offset=final.last_event_offset,
            connection_attempts=attempts,
        )
    else:
        _record_v3_resident_completion(
            provider,
            binding,
            bundle=bundle,
            member=ResidentCompletionMember.INGRESS,
            journal_generation=registration.journal_generation,
            owner_epoch=owner_epoch,
            ending_offset=final.last_event_offset,
            evidence=evidence,
        )
    return evidence


def _run_market_state(
    *,
    package,
    bundle,
    binding,
    provider,
    environment,
    stop_event,
    session_state=None,
):
    control = binding.enrollment.control_plane
    owner_id, owner_epoch = _lease(
        provider,
        bundle,
        AdvancedOptionsProcessKind.MARKET_STATE,
    )
    registration = _wait_registration(
        provider,
        control.source_flow.journal_generation,
        stop_event,
    )
    source = AdvancedOptionsJournalSource(
        root=_journal_root(environment),
        registration=registration,
        source_flow=control.source_flow,
    )
    selection_plan = _selection_plan(session_state)
    layout = _shard_layout(binding, selection_plan)
    resource = (
        f"{CANONICAL_MARKET_STATE_OWNER}:{control.cohort_id}:"
        f"{registration.journal_generation}:{layout.shard_map.sha256}"
    )
    leases = AdvancedOptionsPostgresLeasePort(provider)
    shard_epoch = leases.acquire(
        resource,
        owner_id,
        lease_seconds=30,
        now=datetime.now(UTC),
    )
    if shard_epoch is None:
        raise RuntimeError("P8 market-state shard lease is unavailable")
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        restored_states = load_advanced_options_market_state_cursor(
            unit.cursor,
            consumer_name=CANONICAL_MARKET_STATE_OWNER,
            cohort_id=control.cohort_id,
            journal_generation=registration.journal_generation,
            shard_map=layout.shard_map,
        )
        detector_routing_state = (
            None
            if selection_plan is None
            or selection_plan.admission_profile_version
            != ACTIVITY_MOMENTUM_PROFILE_VERSION
            else load_advanced_options_detector_routing_state_cursor(
                unit.cursor,
                selection_generation_id=(
                    selection_plan.generation_id
                ),
            )
        )
    detector_router = None
    if (
        selection_plan is not None
        and selection_plan.admission_profile_version
        == ACTIVITY_MOMENTUM_PROFILE_VERSION
    ):
        detector_router = AdvancedOptionsBoundedDetectorRouter(
            allocation=activity_momentum_allocation_from_payload(
                selection_plan.allocation_document
            ),
            state=detector_routing_state,
        )
    consumer = AdvancedOptionsMarketStateConsumer(
        source=source,
        cohort_id=control.cohort_id,
        shard_map=layout.shard_map,
        owner_epoch=shard_epoch,
        states=restored_states,
        detector_router=detector_router,
    )
    planned = None
    try:
        while True:
            current = _registration(
                provider,
                registration.journal_generation,
            )
            if current is not None:
                source.registration = current
            planned = consumer.plan_slice(maximum_records=10_000)
            if planned.record_count:
                observed_at = datetime.now(UTC)
                with provider.transaction(APP_ROLE) as unit:
                    persist_advanced_options_market_state_slice_cursor(
                        unit.cursor,
                        cohort_id=control.cohort_id,
                        journal_generation=registration.journal_generation,
                        planned=planned,
                        owner_id=owner_id,
                        batch_executor=execute_batch,
                        lease_resource_key=resource,
                    )
                    _persist_c6_market_progress_cursor(
                        unit.cursor,
                        selection_plan=selection_plan,
                        layout=layout,
                        planned=planned,
                        state=C6ShardState.WARMING,
                        observed_at=observed_at,
                    )
                    if planned.detector_routing is not None:
                        persist_advanced_options_detector_routing_state_cursor(
                            unit.cursor,
                            selection_generation_id=(
                                selection_plan.generation_id
                            ),
                            state=(
                                planned.detector_routing.state
                            ),
                            updated_at=observed_at,
                        )
                consumer.commit_slice(planned)
            if not leases.renew(
                resource,
                owner_id,
                shard_epoch,
                lease_seconds=30,
                now=datetime.now(UTC),
            ):
                raise RuntimeError("P8 market-state shard lease was lost")
            _snapshot_v3_resident_windows(
                provider,
                binding,
                bundle=bundle,
                member=ResidentCompletionMember.MARKET_STATE,
                owner_epoch=owner_epoch,
                reader=RawJournalReader(source.path),
                raw_offset=0,
                normalized_offset=consumer.committed_offset,
                shard_map_sha256_value=consumer.shard_map.sha256,
                shard_ids_value=tuple(range(
                    consumer.shard_map.shard_count
                )),
            )
            if (
                current is not None
                and current.status in {"DRAINING", "DRAINED"}
                and consumer.committed_offset
                >= current.normalized_committed_offset
            ):
                break
            if not planned.record_count:
                if stop_event.is_set():
                    time.sleep(0.05)
                else:
                    stop_event.wait(0.1)
    finally:
        leases.release(resource, owner_id, shard_epoch)
    if consumer.committed_offset <= 0 or planned is None:
        raise RuntimeError("P8 market-state consumed no normalized data")
    with provider.transaction(APP_ROLE) as unit:
        _persist_c6_market_progress_cursor(
            unit.cursor,
            selection_plan=selection_plan,
            layout=layout,
            planned=planned,
            state=C6ShardState.READY,
            observed_at=datetime.now(UTC),
        )
    routing_state = (
        None
        if consumer.detector_router is None
        else consumer.detector_router.state
    )
    routing_duration_seconds = (
        None
        if routing_state is None
        or routing_state.first_event_ns is None
        or routing_state.last_event_ns is None
        or routing_state.last_event_ns
        <= routing_state.first_event_ns
        else (
            routing_state.last_event_ns
            - routing_state.first_event_ns
        )
        / 1_000_000_000
    )
    evidence = {
        "status": "COMPLETE",
        "kind": AdvancedOptionsProcessKind.MARKET_STATE.value,
        "normalized_end_offset": consumer.committed_offset,
        "state_sha256": planned.state_sha256,
        "shard_map_sha256": consumer.shard_map.sha256,
        "shard_ids": list(range(consumer.shard_map.shard_count)),
        "detector_routing": (
            None
            if routing_state is None
            else {
                "allocation_sha256": (
                    routing_state.allocation_sha256
                ),
                "route_entry_count": (
                    consumer.detector_router.route_entry_count
                ),
                "scanned_event_count": (
                    routing_state.scanned_event_count
                ),
                "processed_detail_bytes": (
                    routing_state.processed_detail_bytes
                ),
                "detector_invocation_counts": dict(
                    routing_state.detector_invocation_counts
                ),
                "irrelevant_event_count": (
                    routing_state.irrelevant_event_count
                ),
                "cadence_skip_count": (
                    routing_state.cadence_skip_count
                ),
                "readiness_skip_count": (
                    routing_state.readiness_skip_count
                ),
                "quote_cache_size": len(
                    routing_state.quote_contract_tickers
                ),
                "factual_volume_cache_size": len(
                    routing_state.factual_volume
                ),
                "observed_duration_seconds": (
                    routing_duration_seconds
                ),
                "observed_events_per_second": (
                    None
                    if routing_duration_seconds is None
                    else routing_state.scanned_event_count
                    / routing_duration_seconds
                ),
                "observed_detail_bytes_per_second": (
                    None
                    if routing_duration_seconds is None
                    else routing_state.processed_detail_bytes
                    / routing_duration_seconds
                ),
                "checkpoint_lag_events": max(
                    0,
                    source.registration.normalized_committed_offset
                    - routing_state.committed_event_offset,
                ),
                "safe_capacity_claimed": False,
            }
        ),
        "effects": dict(_EFFECTS),
    }
    if (
        bundle.get("orchestration", {}).get("catalog_version")
        == STAGE_CATALOG_VERSION_V5
    ):
        _snapshot_v3_resident_windows(
            provider,
            binding,
            bundle=bundle,
            member=ResidentCompletionMember.MARKET_STATE,
            owner_epoch=owner_epoch,
            reader=RawJournalReader(source.path),
            raw_offset=0,
            normalized_offset=consumer.committed_offset,
            shard_map_sha256_value=consumer.shard_map.sha256,
            shard_ids_value=tuple(range(
                consumer.shard_map.shard_count
            )),
        )
    else:
        _record_v3_resident_completion(
            provider,
            binding,
            bundle=bundle,
            member=ResidentCompletionMember.MARKET_STATE,
            journal_generation=registration.journal_generation,
            owner_epoch=owner_epoch,
            ending_offset=consumer.committed_offset,
            evidence=evidence,
        )
    return evidence


@dataclass(frozen=True, slots=True)
class _FlowUnavailableContract:
    contract_ticker: str
    underlyer: str
    reason_code: str


@dataclass(frozen=True, slots=True)
class _FlowScopeResolution:
    scope: AdvancedOptionsContractScope | None
    unavailable: tuple[_FlowUnavailableContract, ...]


def _contract_scope_resolution(binding, policy):
    control = binding.enrollment.control_plane
    entries = []
    unavailable = []
    for row in binding.enrollment.activity_selection.contracts:
        dte = (row.expiration_date - control.session_date).days
        try:
            lane = policy.lane_for(dte)
        except ValueError:
            lane = None
        try:
            tier = policy.tier_for(row.underlyer)
        except ValueError:
            tier = None
        if lane is None or tier is None:
            unavailable.append(_FlowUnavailableContract(
                row.contract_ticker,
                row.underlyer,
                (
                    "FLOW_V1_DTE_UNAVAILABLE"
                    if 0 <= dte <= 60
                    else "FLOW_V1_DTE_OUTSIDE_POLICY"
                )
                if tier is not None
                else "FLOW_V1_UNDERLYING_UNAVAILABLE",
            ))
            continue
        entries.append(AdvancedOptionsContractScopeEntry(
            row.contract_ticker,
            row.underlyer,
            row.expiration_date,
            tier,
            lane,
        ))
    return _FlowScopeResolution(
        (
            None
            if not entries
            else AdvancedOptionsContractScope(
                version="advanced-options-contract-scope-v1",
                session_date=control.session_date,
                manifest_sha256=control.manifest_sha256,
                slot_sha256=control.slot_set_sha256,
                flow_policy_sha256=policy.sha256,
                contracts=tuple(entries),
            )
        ),
        tuple(unavailable),
    )


def _contract_scope(binding, policy):
    return _contract_scope_resolution(binding, policy).scope


def _flow_environment(provider, binding, source, owner_epoch):
    advanced_policy = load_advanced_options_flow_policy()
    semantics = load_default_live_semantics()
    exchange = load_default_options_exchange_reference()
    resolution = _contract_scope_resolution(
        binding,
        advanced_policy,
    )
    scope = resolution.scope
    if scope is None:
        return None, resolution, advanced_policy
    base_policy = FlowStrategyPolicy(
        minimum_print_notional=Decimal("50000"),
        minimum_sweep_prints=3,
        minimum_distinct_exchanges=2,
        sweep_window_seconds=180,
        minimum_volume_oi_ratio=3.0,
        maximum_candidates=20,
    )
    source_flow = build_option_source_flow_identity(
        journal_generation=source.generation,
        stream_session_ids=(
            binding.enrollment.control_plane.source_flow.stream_session_ids
        ),
        condition_semantics_sha256=semantics.semantics_sha256,
        flow_policy_sha256=canonical_sha256(base_policy),
        advanced_flow_policy_sha256=advanced_policy.sha256,
        contract_scope_sha256=scope.sha256,
        provider_contract_sha256=(
            MASSIVE_OPTIONS_PROVIDER_CONTRACT.sha256
        ),
        condition_reference_sha256=semantics.pin.payload_sha256,
        exchange_reference_sha256=exchange.sha256,
    )
    source.source_flow = source_flow
    with provider.transaction(APP_ROLE) as unit:
        persist_option_flow_policy_cursor(unit.cursor, advanced_policy)
        persist_option_contract_scope_cursor(unit.cursor, scope)
        persist_option_source_flow_cursor(
            unit.cursor,
            source_flow,
            source_cohort_id=(
                binding.enrollment.control_plane.cohort_id
            ),
            contract_scope=scope,
        )
    settings = OptionsFlowSettings(
        owner_epoch=owner_epoch,
        limits=ConsumerLimits(
            batch_size=500,
            max_messages=50_000,
            max_seconds=5.0,
        ),
        initial_baseline=BaselineKind.WARMING,
        advanced_flow_policy_sha256=advanced_policy.sha256,
        contract_scope_sha256=scope.sha256,
        exchange_reference_sha256=exchange.sha256,
    )
    return (
        RunnerEnvironment(
            provider,
            build_id="advanced-options-p8-flow-v1",
            options_flow_source=source,
            options_flow_semantics=semantics,
            options_flow_policy=base_policy,
            options_flow_settings=settings,
            options_flow_advanced_policy=advanced_policy,
            options_flow_contract_scope=scope,
            options_flow_exchange_reference=exchange,
        ),
        resolution,
        advanced_policy,
    )


def _flow_unavailable_payload(resolution):
    return [
        {
            "contract_ticker": row.contract_ticker,
            "underlyer": row.underlyer,
            "state": "UNAVAILABLE",
            "reason_code": row.reason_code,
        }
        for row in resolution.unavailable
    ]


def _persist_c6_flow_consumers_cursor(
    cursor,
    *,
    selection_plan,
    layout,
    resolution,
    committed_offset,
    policy_sha256,
    observed_at,
):
    if selection_plan is None:
        return
    eligible = (
        set()
        if resolution.scope is None
        else {
            row.contract_ticker
            for row in resolution.scope.contracts
        }
    )
    unavailable = {
        row.contract_ticker for row in resolution.unavailable
    }
    for shard in layout.persistent_shards:
        shard_contracts = set(shard.contract_tickers)
        missing = []
        if not shard_contracts & eligible:
            missing.append("FLOW_POLICY_UNAVAILABLE_FOR_SHARD")
        elif shard_contracts & unavailable:
            missing.append("FLOW_POLICY_PARTIAL_MEMBERSHIP")
        persist_c6_consumer_state_cursor(
            cursor,
            selection_generation_id=selection_plan.generation_id,
            shard_id=shard.shard_id,
            state=C6ConsumerState(
                consumer_name="advanced-options-flow-v1",
                state_version=policy_sha256,
                committed_offset=committed_offset,
                event_watermark_ns=None,
                history_start=None,
                history_end=None,
                missing_feed_states=tuple(missing),
                updated_at=observed_at,
            ),
        )


def _run_flow(
    *,
    package,
    bundle,
    binding,
    provider,
    environment,
    stop_event,
    session_state=None,
):
    control = binding.enrollment.control_plane
    _owner_id, owner_epoch = _lease(
        provider,
        bundle,
        AdvancedOptionsProcessKind.FLOW,
    )
    registration = _wait_registration(
        provider,
        control.source_flow.journal_generation,
        stop_event,
    )
    source = AdvancedOptionsJournalSource(
        root=_journal_root(environment),
        registration=registration,
        source_flow=control.source_flow,
    )
    selection_plan = _selection_plan(session_state)
    layout = _shard_layout(binding, selection_plan)
    (
        runner_environment,
        resolution,
        advanced_policy,
    ) = _flow_environment(
        provider,
        binding,
        source,
        owner_epoch,
    )
    if runner_environment is None:
        current = registration
        unavailable = _flow_unavailable_payload(resolution)
        evidence_ref = canonical_sha256({
            "kind": AdvancedOptionsProcessKind.FLOW.value,
            "flow_state": "UNAVAILABLE",
            "flow_unavailable_contracts": unavailable,
            "policy_sha256": advanced_policy.sha256,
        })
        last_progress = None
        next_target_check = 0.0
        target_check_interval_seconds = 0.5
        flow_shard_ids = tuple(range(
            OptionsFlowSettings().shard_count
        ))
        flow_shard_map_sha256 = shard_map_sha256(
            len(flow_shard_ids)
        )
        while not stop_event.is_set():
            progress = (
                current.raw_committed_offset,
                current.normalized_committed_offset,
            )
            progress_changed = progress != last_progress
            if progress_changed:
                with provider.transaction(APP_ROLE) as unit:
                    _persist_c6_flow_consumers_cursor(
                        unit.cursor,
                        selection_plan=selection_plan,
                        layout=layout,
                        resolution=resolution,
                        committed_offset=(
                            current.normalized_committed_offset
                        ),
                        policy_sha256=advanced_policy.sha256,
                        observed_at=datetime.now(UTC),
                    )
                last_progress = progress
            now_monotonic = time.monotonic()
            if (
                bundle.get("orchestration", {}).get("catalog_version")
                == STAGE_CATALOG_VERSION_V5
                and (
                    progress_changed
                    or now_monotonic >= next_target_check
                )
            ):
                _snapshot_v3_resident_windows(
                    provider,
                    binding,
                    bundle=bundle,
                    member=ResidentCompletionMember.FLOW,
                    owner_epoch=owner_epoch,
                    reader=RawJournalReader(source.path),
                    raw_offset=current.raw_committed_offset,
                    normalized_offset=current.normalized_committed_offset,
                    flow_checkpoint_offset=current.raw_committed_offset,
                    shard_map_sha256_value=flow_shard_map_sha256,
                    shard_ids_value=flow_shard_ids,
                    evidence_ref=evidence_ref,
                )
                next_target_check = (
                    now_monotonic + target_check_interval_seconds
                )
            if current.status in {"DRAINING", "DRAINED"}:
                break
            stop_event.wait(0.1)
            observed = _registration(
                provider,
                registration.journal_generation,
            )
            if observed is not None:
                current = observed
                source.registration = observed
        committed_offset = current.raw_committed_offset
        evidence = {
            "status": "COMPLETE",
            "kind": AdvancedOptionsProcessKind.FLOW.value,
            "flow_state": "UNAVAILABLE",
            "raw_end_offset": current.raw_committed_offset,
            "normalized_end_offset": current.normalized_committed_offset,
            "checkpoint_end_offset": committed_offset,
            "shard_map_sha256": flow_shard_map_sha256,
            "shard_ids": list(flow_shard_ids),
            "flow_unavailable_contracts": unavailable,
            "effects": dict(_EFFECTS),
        }
        if (
            bundle.get("orchestration", {}).get("catalog_version")
            != STAGE_CATALOG_VERSION_V5
        ):
            _record_v3_resident_completion(
                provider,
                binding,
                bundle=bundle,
                member=ResidentCompletionMember.FLOW,
                journal_generation=registration.journal_generation,
                owner_epoch=owner_epoch,
                ending_offset=committed_offset,
                evidence=evidence,
            )
        return evidence
    plan = PostgresOrchestrationRepository(provider).load_plan(
        control.plan_id
    )
    context = StageContext(
        plan=plan,
        window=WindowRecord(
            binding.window_id,
            control.plan_id,
            control.cohort_id,
            "MASSIVE_ADVANCED",
            f"p8-{package['package_nonce']}",
            control.receipts.options_input_ready.spec.cutoff,
            control.receipts.options_input_ready.spec.cutoff,
            datetime.fromisoformat(package["window"]["stop_utc"]),
            "RUNNING",
        ),
        stage=STAGES_V4["OPTIONS_FLOW"],
        stage_run_id=uuid5(
            NAMESPACE_URL,
            f"p8-flow-role:{binding.window_id}",
        ),
        attempt=1,
        started_at=datetime.now(UTC),
        database=provider,
        fenced=False,
    )
    runner = options_flow_runner(runner_environment)
    outcome = None
    while True:
        current = _registration(
            provider,
            registration.journal_generation,
        )
        if current is not None:
            source.registration = current
        outcome = runner(context)
        if outcome.state != "COMPLETE":
            raise RuntimeError(
                "P8 canonical flow role failed: "
                + ",".join(outcome.reason_codes)
            )
        session = runner_environment.options_flow_state.session
        if session is not None and outcome.evidence_ref is not None:
            _snapshot_v3_resident_windows(
                provider,
                binding,
                bundle=bundle,
                member=ResidentCompletionMember.FLOW,
                owner_epoch=owner_epoch,
                reader=RawJournalReader(source.path),
                raw_offset=session.consumer.fed_offset,
                normalized_offset=(
                    source.registration.normalized_committed_offset
                ),
                flow_checkpoint_offset=(
                    session.consumer.checkpointed_offset
                ),
                shard_map_sha256_value=shard_map_sha256(
                    runner_environment.options_flow_settings.shard_count
                ),
                shard_ids_value=tuple(range(
                    runner_environment.options_flow_settings.shard_count
                )),
                evidence_ref=outcome.evidence_ref,
            )
        if (
            session is not None
            and current is not None
            and current.status in {"DRAINING", "DRAINED"}
            and session.consumer.backlog() == 0
            and session.consumer.fed_offset
            >= current.raw_committed_offset
        ):
            session.consumer.finish()
            break
        if stop_event.is_set():
            time.sleep(0.05)
        else:
            stop_event.wait(0.1)
    session = runner_environment.options_flow_state.session
    if session is None or session.consumer.checkpointed_offset <= 0:
        raise RuntimeError("P8 flow produced no durable checkpoint")
    evidence = {
        "status": "COMPLETE",
        "kind": AdvancedOptionsProcessKind.FLOW.value,
        "raw_end_offset": session.consumer.fed_offset,
        "normalized_end_offset": (
            source.registration.normalized_committed_offset
        ),
        "checkpoint_end_offset": session.consumer.checkpointed_offset,
        "owner_epoch": owner_epoch,
        "evidence_ref": outcome.evidence_ref,
        "shard_map_sha256": shard_map_sha256(
            runner_environment.options_flow_settings.shard_count
        ),
        "shard_ids": list(range(
            runner_environment.options_flow_settings.shard_count
        )),
        "flow_unavailable_contracts": (
            _flow_unavailable_payload(resolution)
        ),
        "effects": dict(_EFFECTS),
    }
    with provider.transaction(APP_ROLE) as unit:
        _persist_c6_flow_consumers_cursor(
            unit.cursor,
            selection_plan=selection_plan,
            layout=layout,
            resolution=resolution,
            committed_offset=session.consumer.checkpointed_offset,
            policy_sha256=advanced_policy.sha256,
            observed_at=datetime.now(UTC),
        )
    if (
        bundle.get("orchestration", {}).get("catalog_version")
        == STAGE_CATALOG_VERSION_V5
    ):
        _snapshot_v3_resident_windows(
            provider,
            binding,
            bundle=bundle,
            member=ResidentCompletionMember.FLOW,
            owner_epoch=owner_epoch,
            reader=RawJournalReader(source.path),
            raw_offset=session.consumer.fed_offset,
            normalized_offset=(
                source.registration.normalized_committed_offset
            ),
            flow_checkpoint_offset=session.consumer.checkpointed_offset,
            shard_map_sha256_value=shard_map_sha256(
                runner_environment.options_flow_settings.shard_count
            ),
            shard_ids_value=tuple(range(
                runner_environment.options_flow_settings.shard_count
            )),
            evidence_ref=outcome.evidence_ref,
        )
    else:
        _record_v3_resident_completion(
            provider,
            binding,
            bundle=bundle,
            member=ResidentCompletionMember.FLOW,
            journal_generation=registration.journal_generation,
            owner_epoch=owner_epoch,
            ending_offset=session.consumer.checkpointed_offset,
            evidence=evidence,
        )
    return evidence


def _wait_role_evidence(environment, kind, stop_event):
    directory = Path(str(
        environment.get(
            "REALTIME_V2_P8_STATUS_DIR",
            "/var/lib/realtime-v2-reports/p8",
        )
    ))
    path = directory / f"{kind.value.casefold().replace('_', '-')}.json"
    while not stop_event.is_set():
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            stop_event.wait(0.1)
            continue
        evidence = value.get("role_evidence")
        if (
            value.get("state") == "ROLE_COMPLETE"
            and isinstance(evidence, dict)
            and evidence.get("status") == "COMPLETE"
            and evidence.get("kind") == kind.value
            and evidence.get("effects") == _EFFECTS
        ):
            return evidence
        stop_event.wait(0.1)
    raise RuntimeError(
        f"P8 WINDOW stopped before {kind.value} completed"
    )


def _run_window(
    *,
    package,
    bundle,
    binding,
    provider,
    environment,
    stop_event,
    pool=None,
    binding_observer=None,
):
    catalog_version = bundle.get("orchestration", {}).get(
        "catalog_version"
    )
    if catalog_version == STAGE_CATALOG_VERSION_V5:
        validate_advanced_options_v3_runtime_identity(package, bundle)
        if pool is None:
            raise RuntimeError("V3 WINDOW publication pool is unavailable")
        from .advanced_options_p8_production import (
            load_v3_pending_window_bindings,
            run_v3_serial_window,
        )

        results: dict[str, dict[str, object]] = {}
        observed_bindings: set[UUID] = set()
        terminal_windows: set[UUID] = set()
        while not stop_event.is_set():
            windows = load_v3_pending_window_bindings(
                provider=provider,
                binding=binding,
                bundle=bundle,
                available_at=datetime.now(UTC),
            )
            if not windows:
                stop_event.wait(0.1)
                continue
            progressed = False
            for window_binding in windows:
                if window_binding.window_id in terminal_windows:
                    continue
                if (
                    callable(binding_observer)
                    and window_binding.window_id
                    not in observed_bindings
                ):
                    binding_observer(window_binding)
                    observed_bindings.add(window_binding.window_id)
                dependencies = _v3_resident_dependencies(
                    provider,
                    window_binding,
                    bundle,
                )
                window_key = str(window_binding.window_id)
                if (
                    dependencies is not None
                    and dependencies.get("status") == "DEFERRED"
                ):
                    results[window_key] = {
                        "status": "DEFERRED",
                        "reason_codes": list(
                            dependencies["reason_codes"]
                        ),
                    }
                    continue
                result = run_v3_serial_window(
                    provider=provider,
                    pool=pool,
                    binding=window_binding,
                    package=package,
                    bundle=bundle,
                    environment=environment,
                    dependency_evidence=dependencies,
                )
                results[window_key] = dict(result)
                if result.get("window_state") in {
                    "COMPLETE",
                    "FAILED",
                    "MISSED",
                    "RETAINED_GAP",
                    "UNAVAILABLE",
                }:
                    terminal_windows.add(window_binding.window_id)
                progressed = progressed or (
                    result.get("status") != "NOT_CLAIMED"
                    and result.get("persisted", True) is not False
                )
            if not progressed:
                stop_event.wait(0.1)
        return {
            "status": "COMPLETE",
            "kind": AdvancedOptionsProcessKind.WINDOW.value,
            "windows": results,
            "effects": dict(_EFFECTS),
        }
    dependencies = _v3_resident_dependencies(provider, binding, bundle)
    if (
        dependencies is not None
        and dependencies.get("status") == "DEFERRED"
    ):
        return {
            "status": "DEFERRED",
            "kind": AdvancedOptionsProcessKind.WINDOW.value,
            "window_id": str(binding.window_id),
            "reason_codes": list(dependencies["reason_codes"]),
            "effects": dict(_EFFECTS),
        }
    if dependencies is None:
        dependencies = {
            kind.value: _wait_role_evidence(
                environment,
                kind,
                stop_event,
            )
            for kind in (
                AdvancedOptionsProcessKind.INGRESS,
                AdvancedOptionsProcessKind.MARKET_STATE,
                AdvancedOptionsProcessKind.FLOW,
            )
        }
    from .advanced_options_p8_production import run_p8_serial_window

    result = run_p8_serial_window(
        provider=provider,
        binding=binding,
        package=package,
        environment=environment,
        dependency_evidence=dependencies,
    )
    expected_order = list(OPTIONS_LATE_CANARY_V4_STAGE_ORDER)
    if (
        result.get("status") != "FINISHED"
        or result.get("window_state") != "COMPLETE"
        or result.get("stage_order") != expected_order
    ):
        raise RuntimeError(
            "Advanced Options WINDOW did not complete its exact stages"
        )
    return {
        "status": "COMPLETE",
        "kind": AdvancedOptionsProcessKind.WINDOW.value,
        "window_id": str(binding.window_id),
        "stage_result": dict(result),
        "dependency_evidence": dependencies,
        "effects": dict(_EFFECTS),
    }


def run_p8_role(
    *,
    kind,
    package,
    bundle,
    environment,
    stop_event: Event,
    state_observer=None,
    session_state=None,
):
    if state_observer is not None and not callable(state_observer):
        raise TypeError("P8 role state observer is invalid")
    v3 = (
        bundle.get("orchestration", {}).get("catalog_version")
        == STAGE_CATALOG_VERSION_V5
    )
    production_binding = bundle.get("production_binding")
    attached_binding = None
    if not isinstance(production_binding, dict):
        if not v3:
            raise RuntimeError("P8 production binding is unavailable")
        from .advanced_options_p8_runtime import (
            LegacyArtifactSessionStateAdapter,
            p8_downstream_binding_payload,
        )

        state = session_state or LegacyArtifactSessionStateAdapter(
            acquisition_binding_path=Path(str(environment.get(
                "REALTIME_V3_OPTIONS_ACQUISITION_BINDING_PATH",
                "/run/advanced-options-acquisition/binding.json",
            ))),
            downstream_binding_directory=Path(str(environment.get(
                "REALTIME_V3_OPTIONS_DOWNSTREAM_BINDING_DIR",
                "/var/lib/realtime-v2-reports/downstream-bindings",
            ))),
            acquisition_loader=_load_acquisition_document,
        )
        session_state = state
        session_binding = state.load_preopen_session_binding(bundle)
        if session_binding is None:
            raise TypeError(
                "P8 pre-open session binding must be an object"
            )
        while not stop_event.is_set():
            attached_binding = state.load_latest_downstream_binding(
                session_binding
            )
            if attached_binding is not None:
                production_binding = dict(
                    attached_binding.production_binding
                )
                if state_observer is not None:
                    state_observer(
                        "BOUND_TO_E",
                        p8_downstream_binding_payload(
                            attached_binding
                        ),
                    )
                break
            stop_event.wait(0.1)
        if not isinstance(production_binding, dict):
            raise RuntimeError("P8 role stopped before exact E binding")
    template_binding = p8_production_binding_from_payload(
        production_binding
    )
    if (
        template_binding.enrollment.enrollment_sha256
        != (
            template_binding.enrollment.enrollment_sha256
            if attached_binding is not None
            else bundle["enrollment"]["enrollment_sha256"]
        )
        or (
            attached_binding is None
            and str(template_binding.window_id) != bundle["window_id"]
        )
    ):
        raise RuntimeError("P8 role binding changed")
    provider, pool = _provider(environment)
    bound_binding = None
    try:
        if v3:
            validate_advanced_options_v3_runtime_identity(
                package,
                bundle,
            )
            if kind is AdvancedOptionsProcessKind.WINDOW:
                def observe_binding(binding):
                    nonlocal bound_binding
                    evidence = _downstream_binding_evidence(
                        provider,
                        binding,
                        bundle,
                    )
                    bound_binding = binding
                    if state_observer is not None:
                        state_observer("BOUND_TO_E", evidence)

                return _run_window(
                    package=package,
                    bundle=bundle,
                    binding=template_binding,
                    provider=provider,
                    environment=environment,
                    stop_event=stop_event,
                    pool=pool,
                    binding_observer=observe_binding,
                )
            bound_binding = _wait_v3_binding(
                provider,
                template_binding,
                bundle,
                stop_event,
            )
            if state_observer is not None:
                state_observer(
                    "BOUND_TO_E",
                    _downstream_binding_evidence(
                        provider,
                        bound_binding,
                        bundle,
                    ),
                )
        else:
            bound_binding = template_binding
        arguments = {
            "package": package,
            "bundle": bundle,
            "binding": bound_binding,
            "provider": provider,
            "environment": environment,
            "stop_event": stop_event,
        }
        if kind is AdvancedOptionsProcessKind.INGRESS:
            return _run_ingress(
                **arguments,
                session_state=session_state,
            )
        if kind is AdvancedOptionsProcessKind.MARKET_STATE:
            return _run_market_state(
                **arguments,
                session_state=session_state,
            )
        if kind is AdvancedOptionsProcessKind.FLOW:
            return _run_flow(
                **arguments,
                session_state=session_state,
            )
        if kind is AdvancedOptionsProcessKind.WINDOW:
            return _run_window(**arguments, pool=pool)
        raise RuntimeError("P8 Docker role kind is invalid")
    except BaseException as error:
        if bound_binding is not None:
            _record_v3_resident_failure(
                provider,
                bound_binding,
                bundle=bundle,
                kind=kind,
                error=error,
            )
        raise
    finally:
        pool.closeall()
