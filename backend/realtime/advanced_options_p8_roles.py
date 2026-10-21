"""Bounded production role implementations for the four P8 Docker services."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
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
from .advanced_journal_registry import (
    build_advanced_journal_registration,
    load_advanced_journal_registration_cursor,
    mark_advanced_journal_writing_cursor,
    register_advanced_journal_cursor,
    seal_advanced_journal_cursor,
    update_advanced_journal_offsets_cursor,
)
from .advanced_options_consumers import (
    AdvancedOptionsMarketStateConsumer,
    persist_advanced_options_market_state_slice_cursor,
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
from .advanced_options_service import AdvancedOptionsProcessKind
from .advanced_options_service import AdvancedOptionsPostgresLeasePort
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
from .stage_graph import STAGES_V4
from .stage_orchestrator import StageContext
from .stage_runners_options import OptionsFlowSettings, options_flow_runner


UTC = timezone.utc
_EFFECTS = {
    "dispatch": False,
    "delivery": False,
    "broker": False,
    "trading": False,
}


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


def _admissions(binding):
    return {
        row.contract_ticker: InstrumentAdmission(
            row.instrument_id,
            row.contract_ticker,
            row.underlyer,
            AssetClass.OPTION,
            0,
        )
        for row in binding.enrollment.activity_selection.contracts
    }


def _shard_map(binding):
    members = tuple(
        (row.instrument_id, 0)
        for row in binding.enrollment.activity_selection.contracts
    )
    return MaterializerShardMap(
        "advanced-options-p8-market-state-v1",
        1,
        members,
    )


def _run_ingress(*, package, bundle, binding, provider, environment, stop_event):
    control = binding.enrollment.control_plane
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
        stop_at = min(
            stop,
            control.receipts.options_input_ready.spec.cutoff,
        )
        message_target = min(
            int(limits["decoded_messages"]),
            2_000,
        )
        while (
            not stop_event.is_set()
            and datetime.now(UTC) < stop_at
            and attempts < int(limits["connection_attempts"])
            and gateway.received_messages < message_target
        ):
            attempts += 1
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
    return {
        "status": "COMPLETE",
        "kind": AdvancedOptionsProcessKind.INGRESS.value,
        "raw_end_offset": final.last_raw_offset,
        "normalized_end_offset": final.last_event_offset,
        "connection_attempts": attempts,
        "effects": dict(_EFFECTS),
    }


def _run_market_state(
    *, package, bundle, binding, provider, environment, stop_event
):
    control = binding.enrollment.control_plane
    owner_id, _owner_epoch = _lease(
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
    resource = (
        f"{CANONICAL_MARKET_STATE_OWNER}:{control.cohort_id}:"
        f"{registration.journal_generation}:0"
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
    consumer = AdvancedOptionsMarketStateConsumer(
        source=source,
        cohort_id=control.cohort_id,
        shard_map=_shard_map(binding),
        owner_epoch=shard_epoch,
    )
    planned = None
    try:
        while not stop_event.is_set():
            planned = consumer.plan_slice(maximum_records=10_000)
            if planned.record_count:
                with provider.transaction(APP_ROLE) as unit:
                    persist_advanced_options_market_state_slice_cursor(
                        unit.cursor,
                        cohort_id=control.cohort_id,
                        journal_generation=registration.journal_generation,
                        planned=planned,
                        owner_id=owner_id,
                        batch_executor=execute_batch,
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
            current = _registration(
                provider,
                registration.journal_generation,
            )
            if (
                current is not None
                and current.status in {"DRAINING", "DRAINED"}
                and consumer.committed_offset
                >= current.normalized_committed_offset
            ):
                break
            if not planned.record_count:
                stop_event.wait(0.1)
    finally:
        leases.release(resource, owner_id, shard_epoch)
    if consumer.committed_offset <= 0 or planned is None:
        raise RuntimeError("P8 market-state consumed no normalized data")
    return {
        "status": "COMPLETE",
        "kind": AdvancedOptionsProcessKind.MARKET_STATE.value,
        "normalized_end_offset": consumer.committed_offset,
        "state_sha256": planned.state_sha256,
        "shard_map_sha256": consumer.shard_map.sha256,
        "effects": dict(_EFFECTS),
    }


def _contract_scope(binding, policy):
    control = binding.enrollment.control_plane
    entries = []
    for row in binding.enrollment.activity_selection.contracts:
        dte = (row.expiration_date - control.session_date).days
        lane = policy.lane_for(dte)
        tier = policy.tier_for(row.underlyer)
        if lane is None or tier is None:
            raise RuntimeError(
                "P8 selected contract is outside canonical flow policy"
            )
        entries.append(AdvancedOptionsContractScopeEntry(
            row.contract_ticker,
            row.underlyer,
            row.expiration_date,
            tier,
            lane,
        ))
    return AdvancedOptionsContractScope(
        version="advanced-options-contract-scope-v1",
        session_date=control.session_date,
        manifest_sha256=control.manifest_sha256,
        slot_sha256=control.slot_set_sha256,
        flow_policy_sha256=policy.sha256,
        contracts=tuple(entries),
    )


def _flow_environment(provider, binding, source, owner_epoch):
    advanced_policy = load_advanced_options_flow_policy()
    semantics = load_default_live_semantics()
    exchange = load_default_options_exchange_reference()
    scope = _contract_scope(binding, advanced_policy)
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
    return RunnerEnvironment(
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


def _run_flow(*, package, bundle, binding, provider, environment, stop_event):
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
    runner_environment = _flow_environment(
        provider,
        binding,
        source,
        owner_epoch,
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
    outcome = None
    while not stop_event.is_set():
        outcome = runner(context)
        if outcome.state != "COMPLETE":
            raise RuntimeError(
                "P8 canonical flow role failed: "
                + ",".join(outcome.reason_codes)
            )
        session = runner_environment.options_flow_state.session
        current = _registration(provider, registration.journal_generation)
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
        stop_event.wait(0.1)
    session = runner_environment.options_flow_state.session
    if session is None or session.consumer.checkpointed_offset <= 0:
        raise RuntimeError("P8 flow produced no durable checkpoint")
    return {
        "status": "COMPLETE",
        "kind": AdvancedOptionsProcessKind.FLOW.value,
        "raw_end_offset": session.consumer.fed_offset,
        "checkpoint_end_offset": session.consumer.checkpointed_offset,
        "owner_epoch": owner_epoch,
        "evidence_ref": outcome.evidence_ref,
        "effects": dict(_EFFECTS),
    }


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
    *, package, bundle, binding, provider, environment, stop_event
):
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
    if (
        result.get("status") != "FINISHED"
        or result.get("window_state") != "COMPLETE"
        or result.get("stage_order")
        != [
            "OPTIONS_ACQUIRE",
            "OPTIONS_ADMISSION",
            "OPTIONS_FLOW",
            "OPTIONS_INPUT_GATE",
            "OPTIONS_CYCLE",
            "OPTIONS_PROJECTION",
            "OPTIONS_PUBLISH",
        ]
    ):
        raise RuntimeError("P8 serial WINDOW did not complete")
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
):
    production_binding = bundle.get("production_binding")
    if not isinstance(production_binding, dict):
        raise RuntimeError("P8 production binding is unavailable")
    binding = p8_production_binding_from_payload(production_binding)
    if (
        binding.enrollment.enrollment_sha256
        != bundle["enrollment"]["enrollment_sha256"]
        or str(binding.window_id) != bundle["window_id"]
    ):
        raise RuntimeError("P8 role binding changed")
    provider, pool = _provider(environment)
    try:
        arguments = {
            "package": package,
            "bundle": bundle,
            "binding": binding,
            "provider": provider,
            "environment": environment,
            "stop_event": stop_event,
        }
        if kind is AdvancedOptionsProcessKind.INGRESS:
            return _run_ingress(**arguments)
        if kind is AdvancedOptionsProcessKind.MARKET_STATE:
            return _run_market_state(**arguments)
        if kind is AdvancedOptionsProcessKind.FLOW:
            return _run_flow(**arguments)
        if kind is AdvancedOptionsProcessKind.WINDOW:
            return _run_window(**arguments)
        raise RuntimeError("P8 Docker role kind is invalid")
    finally:
        pool.closeall()
