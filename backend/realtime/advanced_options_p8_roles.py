"""Bounded production role implementations for the four P8 Docker services."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
    resident_consumer_hold_cursor,
)
from .advanced_options_native_loader import (
    AdvancedOptionsNativePreflightBlocked,
)
from .advanced_options_mark_producer import (
    AdvancedOptionsMarkPreflightBlocked,
)
from .advanced_options_rolling_producer import (
    AdvancedOptionsRollingPreflightBlocked,
)
from .advanced_options_rolling_state import (
    AdvancedOptionsWindowRetryDiverged,
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
from .stage_graph import STAGE_CATALOG_VERSION_V5, STAGES_V4
from .stage_orchestrator import StageContext
from .stage_runners_options import (
    OptionsFlowSettings,
    _engine_config,
    options_flow_runner,
)
from .advanced_options_service import OPTIONS_LATE_CANARY_V4_STAGE_ORDER


UTC = timezone.utc
_EFFECTS = {
    "dispatch": False,
    "delivery": False,
    "broker": False,
    "trading": False,
}
_BLOCKED_WINDOW_RETRY_SECONDS = 5.0


def _log_window_attempt(logged, window_key, result, seconds) -> None:
    """One line per change of outcome, so a waiting window leaves a trace without
    filling the log every few seconds."""
    outcome = (
        result.get("status"),
        result.get("window_state"),
        tuple(result.get("reason_codes") or ()),
    )
    if logged.get(window_key) == outcome:
        return
    logged[window_key] = outcome
    print(json.dumps({
        "event": "advanced_options_window_attempt",
        "window_id": window_key,
        "status": outcome[0],
        "window_state": outcome[1],
        "reason_codes": list(outcome[2]),
        "seconds": round(seconds, 1),
        "executed": result.get("executed") or [],
        "lane_errors": result.get("lane_errors") or {},
    }, sort_keys=True), flush=True)


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
            checked_at=datetime.now(UTC),
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


def _record_v3_stream_session_cursor(
    cursor,
    acquisition,
    *,
    manifest_id: UUID,
    owner_epoch: int,
    started_at: datetime,
    subscribed_at: datetime | None,
    observed_at: datetime,
    ended_at: datetime | None,
    connection_attempts: int,
    decoded_messages: int,
    decoded_bytes: int,
    raw_offset: int,
    event_offset: int,
    event_time_watermark: datetime | None,
) -> None:
    """Acquisition owns the socket and no database state, so ingress records
    its one session from durable progress; the journal registry, the resident
    barrier and the rolling producer all read this row."""
    authority = acquisition.authority
    cursor.execute(
        "INSERT INTO rt.stream_sessions ("
        "session_id,provider,feed,manifest_id,owner_id,owner_epoch,"
        "started_at,subscribed_at,ended_at,status,journal_generation,"
        "normalizer_version,authorization_reference,budget_scope_sha256,"
        "connection_attempt,connection_ceiling,decoded_byte_ceiling,"
        "received_frame_count,received_byte_count,"
        "admitted_market_message_count,last_raw_offset,last_event_offset,"
        "event_time_watermark,heartbeat_at"
        ") VALUES (%s,'massive','OPTIONS',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (session_id) DO UPDATE SET "
        "subscribed_at=COALESCE(rt.stream_sessions.subscribed_at,"
        "EXCLUDED.subscribed_at),"
        "ended_at=EXCLUDED.ended_at,status=EXCLUDED.status,"
        "connection_attempt=EXCLUDED.connection_attempt,"
        "received_frame_count=EXCLUDED.received_frame_count,"
        "received_byte_count=EXCLUDED.received_byte_count,"
        "admitted_market_message_count="
        "EXCLUDED.admitted_market_message_count,"
        "last_raw_offset=EXCLUDED.last_raw_offset,"
        "last_event_offset=EXCLUDED.last_event_offset,"
        "event_time_watermark=GREATEST(rt.stream_sessions.event_time_watermark,"
        "EXCLUDED.event_time_watermark),"
        "heartbeat_at=EXCLUDED.heartbeat_at",
        (
            str(acquisition.stream_session_id),
            str(manifest_id),
            str(acquisition.owner_id),
            owner_epoch,
            started_at,
            subscribed_at,
            ended_at,
            "STOPPED" if ended_at is not None else "LIVE",
            str(authority.journal_generation),
            NORMALIZER_VERSION,
            authority.authorization_reference,
            authority.provider_budget_sha256,
            max(1, connection_attempts),
            acquisition.connection_attempt_ceiling,
            acquisition.decoded_byte_ceiling,
            decoded_messages,
            decoded_bytes,
            min(raw_offset, decoded_messages),
            raw_offset,
            event_offset,
            event_time_watermark,
            observed_at,
        ),
    )


STALE_STREAM_SESSION_SECONDS = 120


def _end_stale_v3_stream_sessions_cursor(
    cursor,
    acquisition,
    *,
    checked_at: datetime,
) -> int:
    """B14a (Nov 6): nothing ends an Options stream session at the end of its day.

    At the decision stop the runtime stops the roles, ingress takes the pause path because acquisition's own stop is
    later, and no loop is left to record the terminal status, so the row stays LIVE. The next day's registration then
    violates `uq_rt_active_stream_owner` (one active session per provider and feed) and Options cannot start without
    a manual close (Nov 6, 16:06). A new session ends any other active Options session whose heartbeat stopped more
    than `STALE_STREAM_SESSION_SECONDS` ago, at its last heartbeat, and records why. A session with a recent
    heartbeat is left alone, so a live owner is never taken over."""
    cursor.execute(
        "UPDATE rt.stream_sessions SET status='STOPPED',"
        "ended_at=GREATEST(started_at,COALESCE(heartbeat_at,started_at)),"
        "failure_code='ENDED_BY_NEXT_SESSION' "
        "WHERE provider='massive' AND feed='OPTIONS' AND session_id<>%s "
        "AND ended_at IS NULL AND status NOT IN ('STOPPED','FAILED') "
        "AND COALESCE(heartbeat_at,started_at)<%s",
        (
            str(acquisition.stream_session_id),
            checked_at - timedelta(seconds=STALE_STREAM_SESSION_SECONDS),
        ),
    )
    return max(0, int(getattr(cursor, "rowcount", 0) or 0))


def _acquisition_terminal_ended_at(status) -> datetime | None:
    if (
        status.get("socket_state") not in {"DISCONNECTED", "STOPPED"}
        or status.get("state") not in {"DRAINING", "COMPLETE"}
        or not status.get("last_market_event_at_utc")
    ):
        return None
    return datetime.fromisoformat(status["last_market_event_at_utc"])


def _terminal_journal_is_exact(
    registration,
    *,
    raw_offset: int,
    event_offset: int,
) -> bool:
    if registration.status not in {"DRAINING", "DRAINED"}:
        return False
    if (
        registration.raw_committed_offset != raw_offset
        or registration.normalized_committed_offset != event_offset
        or not registration.sealed_files_sha256
    ):
        raise RuntimeError("V3 terminal journal offsets changed")
    return True


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


def _stop_pauses_session(bundle, stop_event, registration) -> bool:
    """A stop before the session's end leaves its journal open for a restart.
    At or past the end, or once the journal is sealing, roles drain as before."""
    if (
        not stop_event.is_set()
        or bundle.get("orchestration", {}).get("catalog_version")
        != STAGE_CATALOG_VERSION_V5
        or (
            registration is not None
            and registration.status in {"DRAINING", "DRAINED"}
        )
    ):
        return False
    session = session_acquisition_authorization_from_payload(
        bundle["v3_authorities"]["session_acquisition"]
    )
    return datetime.now(UTC) < session.acquisition_stop


def _paused_role_evidence(kind, **offsets) -> dict[str, object]:
    return {
        "status": "PAUSED",
        "kind": kind.value,
        **offsets,
        "effects": dict(_EFFECTS),
    }


def _checkpoint_flow_at_hold(consumer, hold, *, checked_at) -> None:
    """Rows and offsets only flush when something is fed, so a consumer stopped at
    the barrier would never write the checkpoint its window needs. Write it once."""
    if (
        hold is not None
        and checked_at >= hold.cutoff
        and hold.raw_offset > 0
        and consumer.fed_offset == hold.raw_offset
        and consumer.checkpointed_offset < consumer.fed_offset
    ):
        consumer.flush(reason="HOLD_BARRIER", full=True)


class _ResidentConsumerHoldController:
    def __init__(
        self,
        provider,
        bundle,
        source,
        *,
        refresh_seconds: float = 1.0,
    ):
        authorities = bundle.get("v3_authorities")
        if not isinstance(authorities, dict):
            raise RuntimeError("V3 session authority is unavailable")
        session = session_acquisition_authorization_from_payload(
            authorities.get("session_acquisition")
        )
        self._provider = provider
        self._session_authorization_id = session.authorization_id
        self._reader = RawJournalReader(source.path)
        self._refresh_seconds = refresh_seconds
        self._next_refresh = 0.0
        self._hold = None

    def apply(self, source):
        now_monotonic = time.monotonic()
        if now_monotonic >= self._next_refresh:
            with self._provider.transaction(
                APP_ROLE,
                read_only=True,
            ) as unit:
                self._hold = resident_consumer_hold_cursor(
                    unit.cursor,
                    session_authorization_id=(
                        self._session_authorization_id
                    ),
                    reader=self._reader,
                    checked_at=datetime.now(UTC),
                )
            self._next_refresh = (
                now_monotonic + self._refresh_seconds
            )
        source.set_read_limits(
            raw_offset=(
                None
                if self._hold is None
                else self._hold.raw_offset
            ),
            event_offset=(
                None
                if self._hold is None
                else self._hold.normalized_offset
            ),
        )
        return self._hold


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


def _v3_role_binding(
    provider,
    binding,
    bundle,
    stop_event,
    kind: AdvancedOptionsProcessKind,
):
    if kind is AdvancedOptionsProcessKind.INGRESS:
        return binding
    return _wait_v3_binding(
        provider,
        binding,
        bundle,
        stop_event,
    )


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
    reader = RawJournalReader(path)
    first_message = reader.read_after(0, limit=1)
    session_started_at = (
        first_message[0].received_at if first_message else datetime.now(UTC)
    )
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
            _end_stale_v3_stream_sessions_cursor(
                unit.cursor,
                acquisition,
                checked_at=datetime.now(UTC),
            )
            _record_v3_stream_session_cursor(
                unit.cursor,
                acquisition,
                manifest_id=control.manifest_id,
                owner_epoch=owner_epoch,
                started_at=session_started_at,
                subscribed_at=None,
                observed_at=session_started_at,
                ended_at=None,
                connection_attempts=1,
                decoded_messages=0,
                decoded_bytes=0,
                raw_offset=0,
                event_offset=0,
                event_time_watermark=None,
            )
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
    watermark_event_offset = 0
    event_time_watermark = None
    acquisition_complete = False
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
        if last_event_offset > watermark_event_offset:
            newest = reader.latest_event_at(
                after_event_offset=watermark_event_offset,
            )
            if newest is not None:
                event_time_watermark = (
                    newest
                    if event_time_watermark is None
                    else max(event_time_watermark, newest)
                )
            watermark_event_offset = last_event_offset
        with provider.transaction(APP_ROLE) as unit:
            _record_v3_stream_session_cursor(
                unit.cursor,
                acquisition,
                manifest_id=control.manifest_id,
                owner_epoch=registration.owner_epoch,
                started_at=session_started_at,
                subscribed_at=(
                    session_started_at if last_raw_offset > 0 else None
                ),
                observed_at=observed_at,
                ended_at=_acquisition_terminal_ended_at(status),
                connection_attempts=connection_attempts,
                decoded_messages=decoded_messages,
                decoded_bytes=decoded_bytes,
                raw_offset=last_raw_offset,
                event_offset=last_event_offset,
                event_time_watermark=event_time_watermark,
            )
            if not _terminal_journal_is_exact(
                registration,
                raw_offset=last_raw_offset,
                event_offset=last_event_offset,
            ):
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
            acquisition_complete = True
            break
        stop_event.wait(0.1)
    if not acquisition_complete and _stop_pauses_session(
        bundle,
        stop_event,
        registration,
    ):
        return _paused_role_evidence(
            AdvancedOptionsProcessKind.INGRESS,
            raw_end_offset=last_raw_offset,
            normalized_end_offset=last_event_offset,
            connection_attempts=connection_attempts,
            journal_generation=str(
                acquisition.authority.journal_generation
            ),
        )
    if last_raw_offset <= 0 or last_event_offset <= 0:
        raise RuntimeError("V3 acquisition journal has no durable data")
    if not _terminal_journal_is_exact(
        registration,
        raw_offset=last_raw_offset,
        event_offset=last_event_offset,
    ):
        with provider.transaction(APP_ROLE) as unit:
            seal_advanced_journal_cursor(
                unit.cursor,
                registration.journal_generation,
                raw_committed_offset=last_raw_offset,
                normalized_committed_offset=last_event_offset,
                sealed_files_sha256=(
                    hashlib.sha256(path.read_bytes()).hexdigest()
                ),
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
    hold_controller = (
        _ResidentConsumerHoldController(
            provider,
            bundle,
            source,
        )
        if bundle.get("orchestration", {}).get("catalog_version")
        == STAGE_CATALOG_VERSION_V5
        else None
    )
    selection_plan = _selection_plan(session_state)
    layout = _shard_layout(binding, selection_plan)
    leases = AdvancedOptionsPostgresLeasePort(provider)
    with provider.transaction(APP_ROLE, read_only=True) as unit:
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
    shard_ids = tuple(range(layout.shard_map.shard_count))
    resources = {
        shard_id: (
            f"{CANONICAL_MARKET_STATE_OWNER}:{control.cohort_id}:"
            f"{registration.journal_generation}:"
            f"{layout.shard_map.sha256}:{shard_id}"
        )
        for shard_id in shard_ids
    }
    consumers: dict[int, AdvancedOptionsMarketStateConsumer] = {}
    shard_epochs: dict[int, int] = {}
    committed_offsets = {shard_id: 0 for shard_id in shard_ids}
    last_planned = {}
    retry_not_before = {shard_id: 0.0 for shard_id in shard_ids}
    routing_retry_not_before = 0.0
    routing_failure = None

    def claim_shard(shard_id, now):
        shard_epoch = leases.acquire(
            resources[shard_id],
            owner_id,
            lease_seconds=30,
            now=now,
        )
        if shard_epoch is None:
            return False
        try:
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                restored = load_advanced_options_market_state_cursor(
                    unit.cursor,
                    consumer_name=CANONICAL_MARKET_STATE_OWNER,
                    cohort_id=control.cohort_id,
                    journal_generation=registration.journal_generation,
                    shard_map=layout.shard_map,
                    shard_ids=(shard_id,),
                )
            consumer = AdvancedOptionsMarketStateConsumer(
                source=source,
                cohort_id=control.cohort_id,
                shard_map=layout.shard_map,
                owner_epoch=shard_epoch,
                states=restored,
                owned_shard_ids=(shard_id,),
            )
        except Exception:
            leases.release(
                resources[shard_id],
                owner_id,
                shard_epoch,
            )
            raise
        consumers[shard_id] = consumer
        shard_epochs[shard_id] = shard_epoch
        committed_offsets[shard_id] = consumer.committed_offset
        return True

    def record_shard_failure(shard_id, consumer, error, observed_at):
        if selection_plan is None:
            raise error
        reason = (
            "MARKET_STATE_"
            + type(error).__name__.upper()
        )[:160]
        shard = layout.persistent_shards[shard_id]
        with provider.transaction(APP_ROLE) as unit:
            persist_c6_shard_state_cursor(
                unit.cursor,
                selection_generation_id=selection_plan.generation_id,
                shard_id=shard.shard_id,
                state=C6ShardState.FAILED,
                message_count=consumer.committed_offset,
                byte_count=0,
                reason_codes=(reason,),
                observed_at=observed_at,
            )
            persist_c6_consumer_state_cursor(
                unit.cursor,
                selection_generation_id=selection_plan.generation_id,
                shard_id=shard.shard_id,
                state=C6ConsumerState(
                    consumer_name=CANONICAL_MARKET_STATE_OWNER,
                    state_version=layout.shard_map.sha256,
                    committed_offset=consumer.committed_offset,
                    event_watermark_ns=None,
                    history_start=None,
                    history_end=None,
                    missing_feed_states=(reason,),
                    updated_at=observed_at,
                ),
            )

    def record_routing_failure(error, observed_at):
        if selection_plan is None:
            raise error
        reason = (
            "DETECTOR_ROUTING_"
            + type(error).__name__.upper()
        )[:160]
        with provider.transaction(APP_ROLE) as unit:
            for shard_id, shard in enumerate(
                layout.persistent_shards
            ):
                persist_c6_consumer_state_cursor(
                    unit.cursor,
                    selection_generation_id=(
                        selection_plan.generation_id
                    ),
                    shard_id=shard.shard_id,
                    state=C6ConsumerState(
                        consumer_name=(
                            "advanced-options-detector-routing-v1"
                        ),
                        state_version=layout.shard_map.sha256,
                        committed_offset=(
                            committed_offsets[shard_id]
                        ),
                        event_watermark_ns=None,
                        history_start=None,
                        history_end=None,
                        missing_feed_states=(reason,),
                        updated_at=observed_at,
                    ),
                )
        return reason

    completed_offset = 0
    try:
        while True:
            current = _registration(
                provider,
                registration.journal_generation,
            )
            if current is not None:
                source.registration = current
            if hold_controller is not None:
                hold_controller.apply(source)
            now = datetime.now(UTC)
            monotonic_now = time.monotonic()
            made_progress = False
            for shard_id in shard_ids:
                if (
                    shard_id not in consumers
                    and monotonic_now
                    >= retry_not_before[shard_id]
                ):
                    if not claim_shard(shard_id, now):
                        retry_not_before[shard_id] = (
                            monotonic_now + 1.0
                        )
            if selection_plan is None and not consumers:
                raise RuntimeError(
                    "P8 market-state shard lease is unavailable"
                )
            for shard_id, consumer in tuple(consumers.items()):
                observed_at = datetime.now(UTC)
                try:
                    planned = consumer.plan_slice(
                        maximum_records=10_000
                    )
                    last_planned[shard_id] = planned
                    if planned.record_count:
                        with provider.transaction(APP_ROLE) as unit:
                            persist_advanced_options_market_state_slice_cursor(
                                unit.cursor,
                                cohort_id=control.cohort_id,
                                journal_generation=(
                                    registration.journal_generation
                                ),
                                planned=planned,
                                owner_id=owner_id,
                                batch_executor=execute_batch,
                                lease_resource_key=resources[shard_id],
                            )
                            _persist_c6_market_progress_cursor(
                                unit.cursor,
                                selection_plan=selection_plan,
                                layout=layout,
                                planned=planned,
                                state=C6ShardState.WARMING,
                                observed_at=observed_at,
                            )
                        consumer.commit_slice(planned)
                        committed_offsets[shard_id] = (
                            consumer.committed_offset
                        )
                        made_progress = True
                    if not leases.renew(
                        resources[shard_id],
                        owner_id,
                        shard_epochs[shard_id],
                        lease_seconds=30,
                        now=datetime.now(UTC),
                    ):
                        raise RuntimeError(
                            "P8 market-state logical shard lease was lost"
                        )
                except Exception as error:
                    record_shard_failure(
                        shard_id,
                        consumer,
                        error,
                        observed_at,
                    )
                    leases.release(
                        resources[shard_id],
                        owner_id,
                        shard_epochs.pop(shard_id),
                    )
                    consumers.pop(shard_id)
                    retry_not_before[shard_id] = (
                        time.monotonic() + 1.0
                    )
            if (
                detector_router is not None
                and monotonic_now >= routing_retry_not_before
            ):
                try:
                    routing_records = tuple(
                        source.read_normalized_after(
                            detector_router.state.committed_event_offset,
                            limit=10_000,
                        )
                    )
                    if routing_records:
                        routed = detector_router.process(
                            routing_records
                        )
                        with provider.transaction(APP_ROLE) as unit:
                            persist_advanced_options_detector_routing_state_cursor(
                                unit.cursor,
                                selection_generation_id=(
                                    selection_plan.generation_id
                                ),
                                state=routed.state,
                                updated_at=now,
                            )
                        detector_router = (
                            AdvancedOptionsBoundedDetectorRouter(
                                allocation=detector_router.allocation,
                                state=routed.state,
                            )
                        )
                        made_progress = True
                    routing_failure = None
                except Exception as error:
                    routing_failure = record_routing_failure(
                        error,
                        now,
                    )
                    routing_retry_not_before = (
                        time.monotonic() + 1.0
                    )
            completed_offset = min(committed_offsets.values())
            _snapshot_v3_resident_windows(
                provider,
                binding,
                bundle=bundle,
                member=ResidentCompletionMember.MARKET_STATE,
                owner_epoch=owner_epoch,
                reader=RawJournalReader(source.path),
                raw_offset=0,
                normalized_offset=completed_offset,
                shard_map_sha256_value=layout.shard_map.sha256,
                shard_ids_value=shard_ids,
            )
            if _stop_pauses_session(bundle, stop_event, current):
                return _paused_role_evidence(
                    AdvancedOptionsProcessKind.MARKET_STATE,
                    normalized_end_offset=completed_offset,
                )
            if (
                current is not None
                and current.status in {"DRAINING", "DRAINED"}
                and all(
                    offset >= current.normalized_committed_offset
                    for offset in committed_offsets.values()
                )
                and (
                    detector_router is None
                    or (
                        routing_failure is None
                        and detector_router.state.committed_event_offset
                        >= current.normalized_committed_offset
                    )
                )
            ):
                break
            if not made_progress:
                if stop_event.is_set():
                    time.sleep(0.05)
                else:
                    stop_event.wait(0.1)
    finally:
        for shard_id, shard_epoch in tuple(shard_epochs.items()):
            leases.release(
                resources[shard_id],
                owner_id,
                shard_epoch,
            )
    if (
        completed_offset <= 0
        or len(last_planned) != layout.shard_map.shard_count
    ):
        raise RuntimeError("P8 market-state consumed no normalized data")
    with provider.transaction(APP_ROLE) as unit:
        for planned in last_planned.values():
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
        if detector_router is None
        else detector_router.state
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
        "normalized_end_offset": completed_offset,
        "state_sha256": canonical_sha256(tuple(
            consumer.states[0]
            for _, consumer in sorted(consumers.items())
        )),
        "shard_map_sha256": layout.shard_map.sha256,
        "shard_ids": list(shard_ids),
        "detector_routing": (
            None
            if routing_state is None
            else {
                "allocation_sha256": (
                    routing_state.allocation_sha256
                ),
                "route_entry_count": (
                    detector_router.route_entry_count
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
            normalized_offset=completed_offset,
            shard_map_sha256_value=layout.shard_map.sha256,
            shard_ids_value=shard_ids,
        )
    else:
        _record_v3_resident_completion(
            provider,
            binding,
            bundle=bundle,
            member=ResidentCompletionMember.MARKET_STATE,
            journal_generation=registration.journal_generation,
            owner_epoch=owner_epoch,
            ending_offset=completed_offset,
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


class _FlowVolumeBaselineReconciler:
    def __init__(
        self,
        *,
        session_state,
        bundle,
        source,
        refresh_seconds: float = 0.5,
    ):
        self._session_state = session_state
        self._session_binding = (
            None
            if session_state is None
            else session_state.load_preopen_session_binding(bundle)
        )
        self._reader = RawJournalReader(source.path)
        self._refresh_seconds = refresh_seconds
        self._next_refresh = 0.0
        self._seen_sequence = 0
        self._pending = {}

    @staticmethod
    def _baselines(downstream):
        values = downstream.production_binding.get(
            "volume_baselines"
        )
        if values is None:
            return ()
        if not isinstance(values, list):
            raise RuntimeError(
                "P8 volume baseline evidence is invalid"
            )
        baselines = []
        for value in values:
            if (
                not isinstance(value, dict)
                or set(value)
                != {
                    "contract_ticker",
                    "snapshot_volume",
                    "snapshot_at",
                    "source_sha256",
                }
                or not isinstance(value["contract_ticker"], str)
                or not value["contract_ticker"].strip()
                or type(value["snapshot_volume"]) is not int
                or value["snapshot_volume"] < 0
                or not isinstance(value["source_sha256"], str)
                or len(value["source_sha256"]) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in value["source_sha256"]
                )
            ):
                raise RuntimeError(
                    "P8 volume baseline evidence is invalid"
                )
            snapshot_at = datetime.fromisoformat(
                str(value["snapshot_at"])
            )
            if (
                snapshot_at.tzinfo is None
                or snapshot_at.utcoffset() is None
            ):
                raise RuntimeError(
                    "P8 volume baseline evidence is invalid"
                )
            baselines.append({
                **value,
                "contract_ticker": (
                    value["contract_ticker"].strip().upper()
                ),
                "snapshot_at": snapshot_at.astimezone(UTC),
            })
        if len({
            row["contract_ticker"] for row in baselines
        }) != len(baselines):
            raise RuntimeError(
                "P8 volume baseline membership is ambiguous"
            )
        return tuple(sorted(
            baselines,
            key=lambda row: row["contract_ticker"],
        ))

    def _refresh(self):
        if self._session_binding is None:
            return
        now = time.monotonic()
        if now < self._next_refresh:
            return
        bindings = self._session_state.load_downstream_bindings(
            self._session_binding
        )
        for binding in bindings:
            if binding.sequence <= self._seen_sequence:
                continue
            for baseline in self._baselines(binding):
                key = (
                    baseline["contract_ticker"],
                    baseline["snapshot_at"],
                )
                self._pending[key] = baseline
            self._seen_sequence = binding.sequence
        self._next_refresh = now + self._refresh_seconds

    def apply(self, consumer):
        self._refresh()
        # Nov 9: the baselines name every selected contract, the engine routes only its contract scope. A baseline
        # for a contract outside the scope has nothing to reconcile; raising on it put the flow role in a crash loop.
        for key, row in tuple(self._pending.items()):
            if not consumer.routes_contract(row["contract_ticker"]):
                self._pending.pop(key)
        if not self._pending:
            return ()
        ready = tuple(
            (key, row)
            for key, row in sorted(
                self._pending.items(),
                key=lambda item: (
                    item[1]["snapshot_at"],
                    item[1]["contract_ticker"],
                ),
            )
            if self._reader.offsets_at(row["snapshot_at"])[0]
            <= consumer.fed_offset
        )
        if not ready:
            return ()
        results = tuple(
            consumer.reconcile_snapshot(
                row["contract_ticker"],
                row["snapshot_volume"],
                int(row["snapshot_at"].timestamp() * 1000),
            )
            for _key, row in ready
        )
        for key, _row in ready:
            self._pending.pop(key)
        if not self._pending and all(result.accepted for result in results):
            if not consumer.mark_reconciled():
                raise RuntimeError(
                    "P8 volume baseline remained incomplete"
                )
            consumer.flush(
                reason="SNAPSHOT_RECONCILE",
                full=True,
            )
        return results


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
    environment = RunnerEnvironment(
        provider,
        build_id="advanced-options-p8-flow-v1",
        options_flow_source=source,
        options_flow_semantics=semantics,
        options_flow_policy=base_policy,
        options_flow_settings=settings,
        options_flow_advanced_policy=advanced_policy,
        options_flow_contract_scope=scope,
        options_flow_exchange_reference=exchange,
    )
    source_flow = build_option_source_flow_identity(
        journal_generation=source.generation,
        stream_session_ids=(
            binding.enrollment.control_plane.source_flow.stream_session_ids
        ),
        condition_semantics_sha256=semantics.semantics_sha256,
        # The flow stage compares this with its own unbound engine configuration.
        flow_policy_sha256=_engine_config(
            environment,
            settings,
            binding.enrollment.control_plane.cohort_id,
            source.generation,
            source_flow=None,
        ).policy_sha256(),
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
    return (
        environment,
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
    hold_controller = (
        _ResidentConsumerHoldController(
            provider,
            bundle,
            source,
        )
        if bundle.get("orchestration", {}).get("catalog_version")
        == STAGE_CATALOG_VERSION_V5
        else None
    )
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
    volume_reconciler = (
        _FlowVolumeBaselineReconciler(
            session_state=session_state,
            bundle=bundle,
            source=source,
        )
        if bundle.get("orchestration", {}).get("catalog_version")
        == STAGE_CATALOG_VERSION_V5
        else None
    )
    volume_reconciliations = []
    outcome = None
    while True:
        current = _registration(
            provider,
            registration.journal_generation,
        )
        if current is not None:
            source.registration = current
        hold = (
            None
            if hold_controller is None
            else hold_controller.apply(source)
        )
        outcome = runner(context)
        if outcome.state != "COMPLETE":
            raise RuntimeError(
                "P8 canonical flow role failed: "
                + ",".join(outcome.reason_codes)
            )
        session = runner_environment.options_flow_state.session
        if session is not None and volume_reconciler is not None:
            volume_reconciliations.extend(
                volume_reconciler.apply(session.consumer)
            )
        if session is not None:
            _checkpoint_flow_at_hold(
                session.consumer,
                hold,
                checked_at=datetime.now(UTC),
            )
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
        if _stop_pauses_session(bundle, stop_event, current):
            if session is not None:
                session.consumer.flush(reason="PAUSE", full=True)
            return _paused_role_evidence(
                AdvancedOptionsProcessKind.FLOW,
                raw_end_offset=(
                    0 if session is None else session.consumer.fed_offset
                ),
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
        "volume_reconciliations": [
            {
                "contract_ticker": result.contract_ticker,
                "session_date": result.session_date.isoformat(),
                "accepted": result.accepted,
                "reason": result.reason,
                "cumulative_volume": result.cumulative_volume,
                "baseline_kind": result.baseline_kind,
            }
            for result in volume_reconciliations
        ],
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
            fail_v3_window_attempt,
            load_v3_pending_window_bindings,
            run_v3_serial_window,
        )

        results: dict[str, dict[str, object]] = {}
        observed_bindings: set[UUID] = set()
        terminal_windows: set[UUID] = set()
        attempt_outcomes: dict[str, tuple] = {}
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
            blocked = False
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
                    _log_window_attempt(
                        attempt_outcomes,
                        window_key,
                        results[window_key],
                        0.0,
                    )
                    continue
                attempt_started = time.monotonic()
                try:
                    result = run_v3_serial_window(
                        provider=provider,
                        pool=pool,
                        binding=window_binding,
                        package=package,
                        bundle=bundle,
                        environment=environment,
                        dependency_evidence=dependencies,
                    )
                except (
                    AdvancedOptionsRollingPreflightBlocked,
                    AdvancedOptionsMarkPreflightBlocked,
                    AdvancedOptionsNativePreflightBlocked,
                ) as error:
                    results[window_key] = {
                        "status": "DEFERRED",
                        "reason_codes": list(
                            error.result.reason_codes
                        ),
                    }
                    _log_window_attempt(
                        attempt_outcomes,
                        window_key,
                        results[window_key],
                        time.monotonic() - attempt_started,
                    )
                    blocked = True
                    continue
                except AdvancedOptionsWindowRetryDiverged as error:
                    result = fail_v3_window_attempt(
                        provider=provider,
                        window_id=window_binding.window_id,
                        reason=str(error),
                        checked_at=datetime.now(UTC),
                    )
                results[window_key] = dict(result)
                _log_window_attempt(
                    attempt_outcomes,
                    window_key,
                    results[window_key],
                    time.monotonic() - attempt_started,
                )
                # A window still leased by an earlier attempt waits as long as a
                # blocked one: each attempt writes a new reference generation.
                if result.get("status") in {"DEFERRED", "NOT_CLAIMED"}:
                    blocked = True
                    continue
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
            if blocked:
                # Each attempt writes a new reference generation, and a held
                # state will not change within a second.
                stop_event.wait(_BLOCKED_WINDOW_RETRY_SECONDS)
            elif not progressed:
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
            bound_binding = _v3_role_binding(
                provider,
                template_binding,
                bundle,
                stop_event,
                kind,
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
