"""Owned-disposable provider-free P8 ingestion-to-alert-publication qualification."""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import time
from contextlib import closing, contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest
from fastapi.testclient import TestClient
from psycopg2.extras import RealDictCursor, execute_batch
from websockets.asyncio.server import serve

from option_flow_support import advanced_policy, policy
from options.config import load_valuation_policy
from realtime.advanced_gateway import (
    AdvancedGateway,
    GatewayConfiguration,
    GatewayFeedState,
)
from realtime.advanced_options_consumers import (
    AdvancedOptionsMarketStateConsumer,
    persist_advanced_options_market_state_slice_cursor,
)
from realtime.advanced_options_control_plane import (
    CANONICAL_MARKET_STATE_OWNER,
    build_advanced_options_equity_input_binding,
    persist_advanced_options_equity_input_binding_cursor,
    persist_advanced_options_enrollment_cursor,
)
from realtime.advanced_options_cycle import (
    AdvancedOptionsDatasetFlowIdentity,
)
from realtime.advanced_options_ingress import (
    AdvancedOptionsJournalPump,
    AdvancedOptionsJournalSource,
    build_advanced_options_journal_registration,
)
from realtime.advanced_options_input import (
    AdvancedOptionsInputDependencyStatus,
    build_advanced_options_input_ready_plan,
    freeze_advanced_options_input_ready_plan,
)
from realtime.advanced_options_equity_input import (
    initial_advanced_options_equity_context_requirements,
)
from realtime.advanced_options_loaders import (
    build_canonical_advanced_options_runtime_binding,
)
from realtime.advanced_options_mark_producer import (
    PostgresAdvancedOptionsMarkProducer,
    build_advanced_options_mark_producer_plan,
    freeze_advanced_options_mark_producer_plan,
)
from realtime.advanced_options_native_loader import (
    NativeInputPlanStatus,
    PostgresAdvancedOptionsNativeInputLoader,
    build_advanced_options_native_input_plan,
    freeze_advanced_options_native_input_plan,
)
from realtime.advanced_options_p8_canary_authorization import (
    PAGE_RELEASE_OBSERVATION_SCHEMA,
    validate_p8_page_release_observation,
)
from realtime.advanced_options_package_evidence import (
    AdvancedOptionsThresholdEvidence,
    build_cluster_observation,
    build_flow_observation,
    build_flow_package_evidence,
    build_flow_package_evidence_batch,
)
from realtime.advanced_options_postgres import (
    AdvancedOptionsPostgresRuntimeScope,
    PostgresAdvancedOptionsCanonicalRuntimePort,
)
from realtime.advanced_options_projection import (
    AdvancedOptionsCanaryPublicationAuthorization,
    OPTIONS_PAGE_SNAPSHOT_TYPES,
    OPTIONS_PROJECTION_TYPES,
    OPTIONS_SNAPSHOT_TYPES,
    authorize_advanced_options_canary_publication,
    build_advanced_options_projection_batch,
    build_advanced_options_publisher_handoff,
)
from realtime.advanced_options_producers import (
    AdvancedOptionsReferenceClocks,
    build_advanced_options_producer_plan,
)
from realtime.advanced_options_provider_contract import (
    MASSIVE_OPTIONS_PROVIDER_CONTRACT,
    load_default_options_exchange_reference,
)
from realtime.advanced_options_reference_producer import (
    PostgresAdvancedOptionsReferenceProducer,
    build_advanced_options_reference_producer_plan,
)
from realtime.advanced_options_rolling_producer import (
    PostgresAdvancedOptionsRollingProducer,
    build_advanced_options_rolling_producer_plan,
)
from realtime.advanced_options_service import (
    OPTIONS_NON_SERVING_STAGE_ORDER,
)
from realtime.buffer import BufferLimits, RawMessageBuffer
from realtime.contracts import (
    AssetClass,
    MarketChannel,
    MarketFeed,
    SubscriptionManifest,
    SubscriptionMember,
    UniverseManifest,
    canonical_sha256,
)
from realtime.database_access import (
    RealtimeDatabasePrincipal,
    RealtimeDatabaseRole,
    RealtimePoolBudget,
    RealtimeProcessDatabasePlan,
    RoleScopedConnectionProvider,
)
from realtime.database_access_v2 import (
    API_ROLE,
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.detector_adapters import StableDetectorAdapterResult
from realtime.detector_evidence import (
    RealtimeDetectorInput,
    RealtimeDetectorState,
    build_detector_checkpoint,
    build_detector_event,
    build_detector_version,
)
from realtime.detector_evidence_repository import (
    persist_detector_evaluation_batch_cursor,
)
from realtime.journal import RawJournalReader
from realtime.materialization import (
    MaterializedQuote,
    MaterializerShardMap,
)
from realtime.normalizer import InstrumentAdmission, NORMALIZER_VERSION
from realtime.option_flow.checkpoint_state import shard_map_sha256
from realtime.option_flow.conditions import load_default_live_semantics
from realtime.option_flow.consumer import ConsumerLimits
from realtime.option_flow.engine import EngineConfig
from realtime.option_flow.records import ClusterRow, FlowEvent
from realtime.option_flow.volume import BaselineKind
from realtime.option_portal_adapter import (
    materialize_realtime_option_package_mark,
)
from realtime.option_source_flow import (
    build_option_source_flow_identity,
    persist_option_contract_scope_cursor,
    persist_option_flow_policy_cursor,
    persist_option_source_flow_cursor,
)
from realtime.options_v2_runtime import OptionsPreSessionStageBinding
from realtime.orchestration_repository import (
    CycleReceiptSpec,
    PostgresOrchestrationRepository,
    persist_cycle_receipt_cursor,
)
from realtime.portal_api import (
    RealtimePortalRepositories,
    create_realtime_portal_app,
)
from realtime.portal_compatibility_repository import (
    RealtimeOptionFlowRepository,
    RealtimeOptionScreenerRepository,
    RealtimeOptionsAlertsPageRepository,
)
from realtime.portal_projection import (
    build_alert_projection,
    build_portal_snapshot,
    build_screening_generation,
)
from realtime.portal_projection_repository import (
    PortalReleaseReceipt,
    persist_screening_generation_cursor,
)
from realtime.portal_publisher import (
    PortalPublicationBatch,
    PortalPublicationConfig,
    run_portal_publication_once,
)
from realtime.preflight import DetectorFamily, ProviderTier
from realtime.stock_alert_projection import STOCK_ALERTS_VERSION
from realtime.security_v2 import (
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from realtime.stage_graph import STAGES_V4, STAGE_CATALOG_VERSION_V4
from realtime.stage_orchestrator import StageOrchestrator
from realtime.stage_runners_options import (
    OptionsAcquireResult,
    OptionsAdmissionResult,
    OptionsFlowSettings,
)
from realtime.stage_runners_v2 import (
    build_advanced_options_non_serving_runners,
)
from realtime.worker_database import RealtimeProcessDatabase
from tests.realtime.test_advanced_options_input_ready_v2_db import (
    _discovery_scope,
    _limits,
    _seed_discovery_coverage,
    _seed_o3_invocation,
)
from tests.realtime.test_advanced_options_native_loader_v2_db import (
    _seed_native_equity_inputs,
)
from tests.realtime.test_advanced_options_reference_producer_v2_db import (
    _contract_scope,
    _disposable_audit,
    _seed_reference_inputs,
)
from tests.realtime.test_option_flow_runner_v2_db import World, _query
from tests.realtime.test_option_slot_sets_v2_db import (
    _owp4_enrollment,
    _role_cursor,
    cursor,
)
from tests.realtime.v2_database import (
    _connect,
    _settings,
    disposable_v2_database,
    requires_v2_database,
)


pytestmark = requires_v2_database
UTC = timezone.utc
BUILD_ID = "advanced-options-p8-provider-free-v1"
REPORT_ENV = "STOCKS_P8_RETAIN_REPORT"
REPORT_ROOT = Path(__file__).parents[2] / ".realtime-canary"


def _digest(value) -> str:
    return canonical_sha256(("p8-provider-free", value))


def _valuation_policy():
    return load_valuation_policy(
        Path(__file__).parents[2]
        / "options"
        / "policies"
        / "valuation_advanced_nbbo_v1.json"
    )


def _frames(contract: str, start: datetime, count: int = 200):
    base_ms = int(start.timestamp() * 1000)
    frames = []
    trade_sequence = 0
    for index in range(count):
        if index % 2 == 0:
            message = {
                "ev": "Q",
                "sym": contract,
                "bx": 301,
                "ax": 302,
                "bp": 2.0,
                "ap": 2.1,
                "bs": 10,
                "as": 12,
                "t": base_ms + index,
                "q": index + 1,
            }
        else:
            trade_sequence += 1
            message = {
                "ev": "T",
                "sym": contract,
                "x": 302 + trade_sequence % 3,
                "p": 2.1,
                "s": 100,
                "c": [209],
                "t": base_ms + index,
                "q": index + 1,
            }
        frames.append(json.dumps([message], separators=(",", ":")).encode("ascii"))
    return tuple(frames)


def _gateway_manifest(control, selected):
    universe = UniverseManifest(
        "p8-provider-free-loopback-v1",
        ("SPY",),
        tuple(control.universe.option_underlyings),
    )
    return SubscriptionManifest(
        version="p8-provider-free-loopback-v1",
        session_date=control.session_date,
        universe=universe,
        capability_profile_sha256=control.profile_sha256,
        allocation_policy_sha256=control.pins.slot_set_policy_sha256,
        members=(
            SubscriptionMember(
                AssetClass.EQUITY,
                "SPY",
                "SPY",
                MarketChannel.MINUTE_AGGREGATE,
            ),
            *(
                SubscriptionMember(
                    AssetClass.OPTION,
                    selected.contract_ticker,
                    selected.underlyer,
                    channel,
                    catalog_version_id=selected.catalog_version_id,
                )
                for channel in (MarketChannel.TRADE, MarketChannel.QUOTE)
            ),
        ),
        maximum_option_quote_contracts=1,
    )


async def _gateway_to_journal(
    *,
    control,
    selected,
    journal_path: Path,
    frames,
    utc_clock,
):
    controls = []
    subscriptions = []

    async def handler(websocket):
        controls.append(json.loads(await websocket.recv()))
        await websocket.send(json.dumps([{
            "ev": "status",
            "status": "auth_success",
        }]))
        subscribe = json.loads(await websocket.recv())
        controls.append(subscribe)
        subscriptions.extend(subscribe["params"].split(","))
        await websocket.send(json.dumps([{
            "ev": "status",
            "status": "success",
        }]))
        for frame in frames:
            await websocket.send(frame)
        await websocket.wait_closed()

    buffer = RawMessageBuffer(
        BufferLimits(512, 4 * 1024 * 1024, 60_000_000_000),
        monotonic_ns=time.monotonic_ns,
    )
    pump = AdvancedOptionsJournalPump(
        buffer,
        journal_path,
        control.source_flow.journal_generation,
        {
            selected.contract_ticker: InstrumentAdmission(
                selected.instrument_id,
                selected.contract_ticker,
                selected.underlyer,
                AssetClass.OPTION,
                0,
            )
        },
        maximum_batch_messages=32,
        maximum_batch_bytes=1024 * 1024,
    )
    pump.start()
    try:
        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            gateway = AdvancedGateway(
                GatewayConfiguration(
                    MarketFeed.OPTIONS,
                    f"ws://127.0.0.1:{port}",
                    maximum_message_bytes=64 * 1024,
                    library_max_queue=4,
                    allow_insecure_loopback=True,
                ),
                _gateway_manifest(control, selected),
                buffer,
                api_key="provider-free-p8",
                utc_clock=lambda: utc_clock,
                monotonic_ns=time.monotonic_ns,
                session_id_factory=lambda: (
                    control.source_flow.stream_session_ids[0]
                ),
            )
            result = await gateway.run_session(
                maximum_market_messages=len(frames),
            )
    finally:
        pump.request_stop()
    progress = pump.join(30)
    return (
        result,
        progress,
        buffer.snapshot(),
        tuple(controls),
        tuple(subscriptions),
    )


def _app_provider(pool, roles):
    provider = V2ConnectionProvider(
        {APP_ROLE: pool, API_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=roles.prefix,
            allow_disposable=True,
        ),
    )
    provider.verify()
    return provider


def _publisher_database(pool, roles):
    provider = RoleScopedConnectionProvider(
        {RealtimeDatabasePrincipal.RUNTIME: pool},
        {
            RealtimeDatabaseRole.PUBLICATION: roles.name("app"),
        },
    )
    return RealtimeProcessDatabase(
        RealtimeProcessDatabasePlan(
            "worker:portal_publisher:p8_disposable",
            RealtimeDatabaseRole.PUBLICATION,
            RealtimePoolBudget(0, 1),
        ),
        provider,
    )


def _seed_service_window(database_cursor, enrollment, cutoff):
    control = enrollment.control_plane
    database_cursor.execute(
        "INSERT INTO rt.service_plans ("
        "plan_id,cohort_id,session_date,stage_catalog_version,"
        "enabled_stages,config_sha256,deadline,scope_reference"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            str(control.plan_id),
            str(control.cohort_id),
            control.session_date,
            STAGE_CATALOG_VERSION_V4,
            list(OPTIONS_NON_SERVING_STAGE_ORDER),
            enrollment.enrollment_sha256,
            cutoff + timedelta(hours=1),
            "owned-disposable P8 provider-free canary",
        ),
    )
    window_id = uuid4()
    database_cursor.execute(
        "INSERT INTO rt.window_runs ("
        "window_id,plan_id,cohort_id,provider_tier,window_key,"
        "release_at,cutoff,deadline,state) VALUES ("
        "%s,%s,%s,'MASSIVE_ADVANCED','p8-provider-free-canary',"
        "%s,%s,%s,'PENDING')",
        (
            str(window_id),
            str(control.plan_id),
            str(control.cohort_id),
            cutoff,
            cutoff,
            cutoff + timedelta(minutes=30),
        ),
    )
    for relation in (
        "rt.bar_revisions",
        "rt.option_flow_events",
        "rt.option_flow_clusters",
        "rt.portal_snapshot_hot",
        "rt.portal_releases",
    ):
        database_cursor.execute(
            "SELECT rt.ensure_partitions(%s,%s)",
            (relation, cutoff.date()),
        )
    return window_id


def _seed_stream_session(
    database_cursor,
    control,
    *,
    raw_offset,
    event_offset,
    started_at,
    ended_at,
):
    session_id = control.source_flow.stream_session_ids[0]
    database_cursor.execute(
        "INSERT INTO rt.stream_sessions ("
        "session_id,provider,feed,manifest_id,owner_id,owner_epoch,"
        "started_at,authenticated_at,subscribed_at,ended_at,status,"
        "journal_generation,normalizer_version,authorization_reference,"
        "budget_scope_sha256,connection_attempt,connection_ceiling,"
        "decoded_byte_ceiling,received_frame_count,received_byte_count,"
        "admitted_market_message_count,admitted_market_byte_count,"
        "last_raw_offset,last_event_offset,event_time_watermark,heartbeat_at"
        ") VALUES (%s,'p8-loopback','OPTIONS',%s,%s,1,%s,%s,%s,%s,"
        "'STOPPED',%s,%s,'P8_PROVIDER_FREE',%s,1,1,1048576,%s,0,%s,0,"
        "%s,%s,%s,%s)",
        (
            str(session_id),
            str(control.manifest_id),
            str(uuid4()),
            started_at,
            started_at,
            started_at,
            ended_at,
            str(control.source_flow.journal_generation),
            NORMALIZER_VERSION,
            _digest("gateway-budget"),
            raw_offset,
            raw_offset,
            raw_offset,
            event_offset,
            ended_at,
            ended_at,
        ),
    )


def _seed_equity_market_state(
    database_cursor,
    enrollment,
    source_flow,
    *,
    known_at,
):
    security_id = dict(enrollment.equity_plan.security_ids)["SPY"]
    database_cursor.execute(
        "SELECT instrument.instrument_id FROM rt.subscription_members AS member "
        "JOIN rt.instruments AS instrument "
        "ON instrument.instrument_id=member.instrument_id "
        "WHERE member.manifest_id=%s AND instrument.security_id=%s",
        (
            str(enrollment.equity_plan.manifest_id),
            str(security_id),
        ),
    )
    instrument_id = int(database_cursor.fetchone()[0])
    session_id = source_flow.stream_session_ids[0]
    quote_at = known_at - timedelta(milliseconds=20)
    trade_at = known_at - timedelta(milliseconds=10)
    database_cursor.execute(
        "INSERT INTO rt.quote_current ("
        "provider,instrument_id,source_session,source_sequence,"
        "event_time_raw,event_time_unit,event_at,received_at,bid,ask,"
        "bid_size,ask_size,bid_exchange,ask_exchange,quality,"
        "reason_codes,session_id,journal_generation,shard_id,"
        "source_offset,payload_sha256) VALUES ("
        "'massive',%s,%s,1,1,'ms',%s,%s,499.90,500.10,"
        "100,120,301,302,'GOOD',ARRAY[]::text[],%s,%s,0,1,%s)",
        (
            instrument_id,
            enrollment.control_plane.session_date,
            quote_at,
            quote_at,
            str(session_id),
            str(source_flow.journal_generation),
            _digest("equity-quote"),
        ),
    )
    database_cursor.execute(
        "INSERT INTO rt.trade_current ("
        "provider,instrument_id,source_session,source_sequence,"
        "event_time_raw,event_time_unit,event_at,received_at,price,size,"
        "exchange,conditions,eligibility,eligibility_policy_version,"
        "reason_codes,session_id,journal_generation,shard_id,"
        "source_offset,payload_sha256) VALUES ("
        "'massive',%s,%s,2,2,'ms',%s,%s,500.00,10,"
        "302,ARRAY[]::integer[],'INCLUDED','p8-equity-v1',"
        "ARRAY[]::text[],%s,%s,0,2,%s)",
        (
            instrument_id,
            enrollment.control_plane.session_date,
            trade_at,
            trade_at,
            str(session_id),
            str(source_flow.journal_generation),
            _digest("equity-trade"),
        ),
    )
    return instrument_id


def _persist_market_state(
    pool,
    provider,
    source,
    *,
    cohort_id,
    generation,
    instrument_id,
):
    shard_map = MaterializerShardMap(
        "p8-options-market-state-v1",
        1,
        ((instrument_id, 0),),
    )
    owner_id = uuid4()
    resource_key = (
        f"{CANONICAL_MARKET_STATE_OWNER}:{cohort_id}:{generation}:0"
    )
    with cursor(pool) as database_cursor:
        database_cursor.execute(
            "INSERT INTO rt.worker_leases "
            "(resource_key,owner_id,owner_epoch,lease_until) "
            "VALUES (%s,%s,1,clock_timestamp()+interval '10 minutes')",
            (resource_key, str(owner_id)),
        )
    consumer = AdvancedOptionsMarketStateConsumer(
        source=source,
        cohort_id=cohort_id,
        shard_map=shard_map,
        owner_epoch=1,
        clock=lambda: datetime.now(UTC),
    )
    planned = consumer.plan_slice(maximum_records=10_000)
    with provider.transaction(APP_ROLE) as unit:
        persist_advanced_options_market_state_slice_cursor(
            unit.cursor,
            cohort_id=cohort_id,
            journal_generation=generation,
            planned=planned,
            owner_id=owner_id,
            batch_executor=execute_batch,
        )
    consumer.commit_slice(planned)
    return planned, shard_map


def _flow_event(row, profile):
    return FlowEvent(
        event_id=UUID(str(row["event_id"])),
        event_time=row["event_time"],
        cohort_id=UUID(str(row["cohort_id"])),
        session_date=row["session_date"],
        provider_tier=str(row["provider_tier"]),
        contract_ticker=str(row["contract_ticker"]),
        underlyer=str(row["underlyer"]),
        source_seq=int(row["source_seq"]),
        exchange_id=row["exchange_id"],
        price=row["price"],
        size=int(row["size"]),
        multiplier=int(row["multiplier"]),
        premium=row["premium"],
        conditions=tuple(row["conditions"]),
        condition_semantics_version=str(
            row["condition_semantics_version"]
        ),
        trade_treatment=str(row["trade_treatment"]),
        is_multi_leg=bool(row["is_multi_leg"]),
        side_class=str(row["side_class"]),
        side_confidence=str(row["side_confidence"]),
        quote_bid=row["quote_bid"],
        quote_ask=row["quote_ask"],
        quote_bid_size=row["quote_bid_size"],
        quote_ask_size=row["quote_ask_size"],
        quote_time=row["quote_time"],
        quote_seq=row["quote_seq"],
        cumulative_volume_before=row["cumulative_volume_before"],
        cumulative_volume_after=row["cumulative_volume_after"],
        volume_baseline_kind=str(row["volume_baseline_kind"]),
        prior_open_interest=row["prior_open_interest"],
        open_interest_as_of=row["open_interest_as_of"],
        volume_exceeds_prior_oi=bool(row["volume_exceeds_prior_oi"]),
        opening_inference=str(row["opening_inference"]),
        spread_position=row["spread_position"],
        opening_score=row["opening_score"],
        opening_reasons=tuple(row["opening_reasons"]),
        journal_generation=UUID(str(row["journal_generation"])),
        shard_id=int(row["shard_id"]),
        source_offset=int(row["source_offset"]),
        source_flow_id=UUID(str(row["source_flow_id"])),
        condition_semantics_sha256=str(
            row["condition_semantics_sha256"]
        ),
        flow_policy_sha256=str(row["flow_policy_sha256"]),
        lineage_version=int(row["lineage_version"]),
        revision=int(row["revision"]),
        supersedes_event_id=(
            None
            if row["supersedes_event_id"] is None
            else UUID(str(row["supersedes_event_id"]))
        ),
        time_ms=int(row["event_time"].timestamp() * 1000),
        reasons=tuple(row["opening_reasons"]),
        is_cross=False,
        is_sweep=True,
        is_floor=False,
        is_late=False,
        matched_profiles=(profile,),
        flow_universe_tier=row["flow_universe_tier"],
        flow_dte_lane=row["flow_dte_lane"],
        contract_expiration=row["contract_expiration"],
        advanced_flow_policy_sha256=str(
            row["advanced_flow_policy_sha256"]
        ),
        contract_scope_sha256=str(row["contract_scope_sha256"]),
        manifest_sha256=str(row["manifest_sha256"]),
        slot_sha256=str(row["slot_sha256"]),
        runtime_policy_sha256=str(row["runtime_policy_sha256"]),
        provider_contract_sha256=str(
            row["provider_contract_sha256"]
        ),
        condition_reference_sha256=str(
            row["condition_reference_sha256"]
        ),
        exchange_reference_sha256=str(
            row["exchange_reference_sha256"]
        ),
    )


def _load_flow_evidence(provider, *, cohort_id, rolling, reference):
    with provider.transaction(
        APP_ROLE,
        read_only=True,
        dict_cursor=True,
    ) as unit:
        unit.cursor.execute(
            "SELECT cluster.* FROM rt.option_flow_clusters AS cluster "
            "WHERE cluster.cohort_id=%s AND cluster.status='FINAL' "
            "ORDER BY cluster.print_count DESC,cluster.cluster_id LIMIT 1",
            (str(cohort_id),),
        )
        cluster_row = unit.cursor.fetchone()
        assert cluster_row is not None
        unit.cursor.execute(
            "SELECT member.event_id,member.event_time,member.profile "
            "FROM rt.option_flow_cluster_members AS member "
            "WHERE member.cluster_id=%s AND member.revision=%s "
            "AND member.session_date=%s AND member.profile=%s "
            "ORDER BY member.event_time,member.event_id",
            (
                str(cluster_row["cluster_id"]),
                cluster_row["revision"],
                cluster_row["session_date"],
                cluster_row["profile"],
            ),
        )
        members = tuple(unit.cursor.fetchall())
        assert members
        unit.cursor.execute(
            "SELECT event.* FROM rt.option_flow_events AS event "
            "WHERE event.event_id=ANY(%s::uuid[]) "
            "ORDER BY event.event_time,event.event_id",
            ([str(row["event_id"]) for row in members],),
        )
        rows = tuple(unit.cursor.fetchall())
    profile = str(cluster_row["profile"])
    events = {
        UUID(str(row["event_id"])): _flow_event(row, profile)
        for row in rows
    }
    cluster = ClusterRow(
        cluster_id=UUID(str(cluster_row["cluster_id"])),
        revision=int(cluster_row["revision"]),
        session_date=cluster_row["session_date"],
        cohort_id=UUID(str(cluster_row["cohort_id"])),
        provider_tier=str(cluster_row["provider_tier"]),
        contract_ticker=str(cluster_row["contract_ticker"]),
        underlyer=str(cluster_row["underlyer"]),
        window_seconds=int(cluster_row["window_seconds"]),
        window_start=cluster_row["window_start"],
        window_end=cluster_row["window_end"],
        side_class=str(cluster_row["side_class"]),
        print_count=int(cluster_row["print_count"]),
        total_size=int(cluster_row["total_size"]),
        total_premium=cluster_row["total_premium"],
        distinct_exchanges=int(cluster_row["distinct_exchanges"]),
        first_event_time=cluster_row["first_event_time"],
        last_event_time=cluster_row["last_event_time"],
        status=str(cluster_row["status"]),
        policy_sha256=str(cluster_row["policy_sha256"]),
        source_flow_id=UUID(str(cluster_row["source_flow_id"])),
        condition_semantics_sha256=str(
            cluster_row["condition_semantics_sha256"]
        ),
        lineage_version=int(cluster_row["lineage_version"]),
        flow_policy_sha256=str(cluster_row["flow_policy_sha256"]),
        profile=profile,
        flow_universe_tier=cluster_row["flow_universe_tier"],
        flow_dte_lane=cluster_row["flow_dte_lane"],
        advanced_flow_policy_sha256=str(
            cluster_row["advanced_flow_policy_sha256"]
        ),
        contract_scope_sha256=str(
            cluster_row["contract_scope_sha256"]
        ),
        manifest_sha256=str(cluster_row["manifest_sha256"]),
        slot_sha256=str(cluster_row["slot_sha256"]),
        runtime_policy_sha256=str(
            cluster_row["runtime_policy_sha256"]
        ),
        cumulative_contracts=cluster_row["cumulative_contracts"],
        qualifies=cluster_row["qualifies"],
        member_event_ids=tuple(
            UUID(str(row["event_id"])) for row in members
        ),
        member_event_keys=tuple(
            (UUID(str(row["event_id"])), row["event_time"])
            for row in members
        ),
        provider_contract_sha256=str(
            cluster_row["provider_contract_sha256"]
        ),
        condition_reference_sha256=str(
            cluster_row["condition_reference_sha256"]
        ),
        exchange_reference_sha256=str(
            cluster_row["exchange_reference_sha256"]
        ),
    )
    flow_observations = tuple(
        build_flow_observation(
            event,
            reference_state=reference,
            rolling_state=rolling,
        )
        for event in events.values()
    )
    cluster_observation = build_cluster_observation(
        cluster,
        events=events,
        watermark=max(
            cluster.window_end,
            max(row.event_time for row in events.values()),
        ),
        rolling_state=rolling,
    )
    return events, cluster, flow_observations, cluster_observation


def _threshold(cluster, events, rolling):
    volume = rolling.contract_volumes[0].checkpoint.cumulative_volume
    observed = max(events.values(), key=lambda row: row.event_time)
    return AdvancedOptionsThresholdEvidence(
        minimum_cumulative_volume=1,
        observed_cumulative_volume=volume,
        minimum_volume_oi_ratio=Decimal("0"),
        observed_volume_oi_ratio=Decimal("1"),
        minimum_premium=Decimal("1"),
        observed_premium=observed.premium,
        accepted_side_classes=(observed.side_class,),
        accepted_confidences=(observed.side_confidence,),
        observed_side_class=observed.side_class,
        observed_confidence=observed.side_confidence,
        minimum_cluster_prints=1,
        observed_cluster_prints=cluster.print_count,
        minimum_distinct_exchanges=1,
        observed_distinct_exchanges=cluster.distinct_exchanges,
        minimum_cluster_premium=Decimal("1"),
        observed_cluster_premium=cluster.total_premium,
        liquidity_policy_sha256=_digest("liquidity-policy"),
        liquidity_evidence_sha256=_digest("liquidity-evidence"),
        liquidity_passed=True,
        maximum_freshness_ms=1000,
        observed_freshness_ms=0,
        completeness=BaselineKind.SESSION_COMPLETE,
        passed=True,
    )


def _package_mark(package, mark, valuation_policy, cutoff):
    quote = MaterializedQuote(
        journal_generation=mark.journal_generation,
        stream_session_id=mark.stream_session_id,
        instrument_id=mark.instrument_id,
        provider_symbol=package.legs[0].contract_ticker,
        source_session=cutoff.date(),
        source_sequence=mark.source_offset,
        source_timestamp=int(mark.market_time.timestamp() * 1000),
        source_timestamp_unit="ms",
        event_at=mark.market_time,
        received_at=mark.observed_time,
        bid=mark.bid,
        ask=mark.ask,
        bid_size=mark.bid_size,
        ask_size=mark.ask_size,
        bid_exchange=301,
        ask_exchange=302,
        quality="GOOD",
        reason_codes=(),
        source_offset=mark.source_offset,
        payload_sha256=mark.payload_sha256,
    )
    result = materialize_realtime_option_package_mark(
        package,
        {mark.instrument_id: quote},
        checked_at=cutoff,
        policy=valuation_policy,
    )
    assert result.mark is not None, result.reason_codes
    return result.mark


def _shape_alert(
    provider,
    *,
    control,
    selected,
    source_flow,
    flow_event,
    evaluation,
    cutoff,
    committed_offset,
):
    version = build_detector_version(
        detector_id=evaluation.detector.value,
        version="advanced_options_p8_canary_v1",
        code_sha256=_digest("canary-alert-code"),
        policy_sha256=_digest("canary-alert-policy"),
        requirements={"stored_cycle": True, "external_delivery": False},
        output_kind="ALERT",
        valuation_sha256=control.pins.valuation_policy_sha256,
    )
    adapter = StableDetectorAdapterResult(
        adapter_id="advanced-options-p8-alert",
        cohort_id=control.cohort_id,
        provider_tier=ProviderTier.MASSIVE_ADVANCED,
        input_sha256=evaluation.payload_sha256,
        status="EXECUTED",
        output={"disposition": "SELECTED"},
    )
    input_row = RealtimeDetectorInput(
        input_role="CANONICAL_OPTIONS_FLOW",
        input_key=str(flow_event.event_id),
        source_kind="JOURNAL_EVENT",
        input_sha256=_digest(("flow-event", flow_event.event_id)),
        available_at=flow_event.event_time,
        received_at=flow_event.event_time,
        retained_input={
            "flow_event_id": str(flow_event.event_id),
            "evaluation_id": str(evaluation.evaluation_id),
        },
        journal_generation=source_flow.journal_generation,
        stream_session_id=source_flow.stream_session_ids[0],
        source_offset=flow_event.source_offset,
        source_flow_id=source_flow.source_flow_id,
        source_cohort_id=control.cohort_id,
    )
    event = build_detector_event(
        adapter,
        version,
        instrument_id=selected.instrument_id,
        trigger_identity=(
            f"P8:{evaluation.detector.value}:"
            f"{evaluation.evaluation_id}"
        ),
        trigger_at=flow_event.event_time,
        decision_at=cutoff,
        valid_until=cutoff + timedelta(minutes=30),
        disposition="SELECTED",
        direction=1,
        reason_codes=(),
        readiness_epoch=1,
        payload_schema="advanced_options_p8_canary_alert_v1",
        payload={
            "evaluation_id": str(evaluation.evaluation_id),
            "candidate_id": (
                None
                if evaluation.package is None
                else str(evaluation.package.candidate_id)
            ),
            "dispatch_authorized": False,
            "delivery_authorized": False,
            "execution_authorized": False,
        },
        inputs=(input_row,),
    )
    state_payload = {
        "open_event_ids": [str(event.event_id)],
        "dispatch_authorized": False,
    }
    state = RealtimeDetectorState(
        detector_sha256=version.detector_sha256,
        cohort_id=control.cohort_id,
        state_key="P8:CANARY:CURRENT",
        revision=1,
        state_schema="advanced_options_p8_canary_state_v1",
        state_sha256=canonical_sha256(state_payload),
        state_payload=state_payload,
        updated_at=cutoff,
    )
    checkpoint = build_detector_checkpoint(
        {"state": state.state_sha256},
        consumer_name="advanced-options-p8-canary-alert-v1",
        cohort_id=control.cohort_id,
        journal_generation=source_flow.journal_generation,
        shard_id=0,
        shard_map_sha256=shard_map_sha256(1),
        owner_epoch=1,
        committed_offset=committed_offset,
        committed_at=cutoff,
    )
    alert = build_alert_projection(
        event,
        alert_kind="OPTION",
        lifecycle="OPEN",
        projection_schema="advanced_options_p8_canary_alert_v1",
        projection={
            "evaluation_id": str(evaluation.evaluation_id),
            "underlyer": evaluation.underlyer,
            "production_authorized": False,
            "delivery_authorized": False,
            "execution_authorized": False,
        },
        destinations=("PORTAL",),
    )
    owner_id = uuid4()
    resource_key = (
        f"advanced-options-p8-canary-alert:"
        f"{control.cohort_id}:0"
    )
    with provider.transaction(APP_ROLE) as unit:
        unit.cursor.execute(
            "INSERT INTO rt.worker_leases "
            "(resource_key,owner_id,owner_epoch,lease_until) "
            "VALUES (%s,%s,1,clock_timestamp()+interval '10 minutes')",
            (resource_key, str(owner_id)),
        )
        persist_detector_evaluation_batch_cursor(
            unit.cursor,
            lease_resource_key=resource_key,
            owner_id=owner_id,
            checkpoint=checkpoint,
            versions=(version,),
            states=(state,),
            events=(event,),
            alerts=(alert,),
        )
    return event, state, alert


class _NoDeliverySink:
    def deliver(self, _message):
        raise AssertionError("P8 canary attempted external delivery")


def _publish(
    database,
    config,
    batch,
    *,
    checked_at,
    release_repository=None,
):
    options = {}
    if release_repository is not None:
        options["release_repository"] = release_repository
    return run_portal_publication_once(
        database,
        config,
        batch,
        _NoDeliverySink(),
        checked_at=checked_at,
        **options,
    )


def _pointer(pool, cohort_id):
    rows = _query(
        pool,
        "SELECT pointer.release_id::text,pointer.cutoff,"
        "release.manifest,release.manifest_sha256 "
        "FROM rt.portal_release_current AS pointer "
        "JOIN rt.portal_releases AS release "
        "ON release.release_id=pointer.release_id "
        "AND release.cutoff=pointer.cutoff "
        "WHERE pointer.cohort_id=%s",
        (str(cohort_id),),
    )
    assert len(rows) == 1
    return rows[0]


def _retain_report(report):
    digest = canonical_sha256(report)
    document = {
        **report,
        "report_sha256": digest,
    }
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    path = REPORT_ROOT / (
        "advanced-options-p8-provider-free-qualification-"
        f"{digest[:16]}.json"
    )
    with path.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(document, stream, sort_keys=True, indent=2)
        stream.write("\n")
    return path, digest


def test_p8_provider_free_ingestion_to_canary_alert_publication(
    tmp_path,
    capsys,
):
    suffix = uuid4().hex[:8]
    database_name = f"test_realtime_v2_p8_{suffix}"
    roles = build_role_names_v2(f"test_rt_p8_{suffix}")
    settings = _settings()
    roles_owned = False
    report = None
    journal_root = tmp_path / "p8-owned-journal"
    now = datetime.now(UTC).replace(microsecond=0)
    checked = now - timedelta(hours=2)
    planned = now
    cutoff = now + timedelta(minutes=3)
    equity_cutoff = cutoff - timedelta(minutes=2)
    history_cohort_id = uuid4()
    expires = now + timedelta(hours=8)
    try:
        with disposable_v2_database(database_name) as database_pool:
            semantics = load_default_live_semantics()
            flow_policy = advanced_policy()
            valuation_policy = _valuation_policy()
            enrollment = _owp4_enrollment(
                database_pool,
                flow_policy_sha256=flow_policy.sha256,
                condition_semantics_sha256=(
                    semantics.semantics_sha256
                ),
                security_free_float=Decimal("1000000000"),
                provider_request_ceiling=2,
                provider_decoded_byte_ceiling=100_000_000,
                valuation_policy_sha256=valuation_policy.policy_sha256,
                activity_valid_until=cutoff,
                session=cutoff.date(),
                checked=checked,
                planned=planned,
                cutoff=cutoff,
                expires=expires,
            )
            control = enrollment.control_plane
            selected = enrollment.activity_selection.contracts[0]
            contract_scope = _contract_scope(enrollment, flow_policy)
            detector_policy = policy(
                sweep_window_seconds=5,
                minimum_sweep_prints=3,
            )
            exchange = load_default_options_exchange_reference()
            engine_config = EngineConfig(
                cohort_id=control.cohort_id,
                provider_tier="MASSIVE_ADVANCED",
                policy=detector_policy,
                semantics=semantics,
                journal_generation=control.source_flow.journal_generation,
                shard_count=1,
                queue_capacity=1024,
                advanced_flow_policy=flow_policy,
                contract_scope=contract_scope,
                exchange_reference=exchange,
            )
            source_flow = build_option_source_flow_identity(
                journal_generation=control.source_flow.journal_generation,
                stream_session_ids=control.source_flow.stream_session_ids,
                condition_semantics_sha256=semantics.semantics_sha256,
                flow_policy_sha256=engine_config.policy_sha256(),
                advanced_flow_policy_sha256=flow_policy.sha256,
                contract_scope_sha256=contract_scope.sha256,
                provider_contract_sha256=(
                    MASSIVE_OPTIONS_PROVIDER_CONTRACT.sha256
                ),
                condition_reference_sha256=(
                    semantics.pin.payload_sha256
                ),
                exchange_reference_sha256=exchange.sha256,
            )
            reference_plan = build_advanced_options_reference_producer_plan(
                enrollment=enrollment,
                contract_scope=contract_scope,
                reference_generation=1,
                known_at=cutoff,
                valid_until=expires,
                discovery_scheduled_cycle=cutoff,
                prior_oi_session=control.session_date - timedelta(days=1),
                treasury_maximum_age_seconds=3_600,
                model_version="black-scholes-merton-v1",
                history_source_cohort_id=history_cohort_id,
            )
            with cursor(database_pool) as database_cursor:
                database_cursor.execute(
                    "INSERT INTO rt.cohorts ("
                    "cohort_id,cohort_sha256,manifest_id,"
                    "configuration_sha256,model_bundle_sha256,"
                    "valuation_sha256,effective_at,mode) "
                    "SELECT %s,%s,manifest_id,configuration_sha256,"
                    "model_bundle_sha256,valuation_sha256,effective_at,"
                    "mode FROM rt.cohorts WHERE cohort_id=%s",
                    (
                        str(history_cohort_id),
                        _digest("p8-history-cohort"),
                        str(enrollment.equity_plan.cohort_id),
                    ),
                )
                persist_advanced_options_enrollment_cursor(
                    database_cursor,
                    enrollment,
                )
                persist_option_flow_policy_cursor(
                    database_cursor,
                    flow_policy,
                )
                persist_option_contract_scope_cursor(
                    database_cursor,
                    contract_scope,
                )
                persist_option_source_flow_cursor(
                    database_cursor,
                    source_flow,
                    source_cohort_id=control.cohort_id,
                    contract_scope=contract_scope,
                )
                _seed_reference_inputs(
                    database_cursor,
                    enrollment,
                    reference_plan,
                    history_source_cohort_id=history_cohort_id,
                )
                _seed_discovery_coverage(
                    database_cursor,
                    control,
                    scheduled_cycle=cutoff,
                )
                window_id = _seed_service_window(
                    database_cursor,
                    enrollment,
                    cutoff,
                )
            connection_value = database_pool.getconn()
            try:
                created_roles, security_audit = harden_database_v2(
                    connection_value,
                    roles,
                )
                roles_owned = True
                assert len(created_roles) == 3
                assert all(
                    status.startswith("PASS")
                    for _, status in security_audit
                )
            finally:
                database_pool.putconn(connection_value)
            provider = _app_provider(database_pool, roles)
            reference = PostgresAdvancedOptionsReferenceProducer(
                provider,
                reference_plan,
                retry_initial_seconds=0,
            ).produce()

            frames = _frames(
                selected.contract_ticker,
                cutoff - timedelta(milliseconds=200),
            )
            journal_path = journal_root / "options.sqlite"
            (
                gateway_result,
                journal_progress,
                buffer_snapshot,
                controls,
                subscriptions,
            ) = asyncio.run(_gateway_to_journal(
                control=control,
                selected=selected,
                journal_path=journal_path,
                frames=frames,
                utc_clock=cutoff,
            ))
            assert gateway_result.final_state is GatewayFeedState.STOPPED
            assert gateway_result.accepted_messages == len(frames)
            assert journal_progress.last_raw_offset == len(frames)
            assert journal_progress.last_event_offset == len(frames)
            assert buffer_snapshot.full_count == 0
            assert controls[0] == {
                "action": "auth",
                "params": "provider-free-p8",
            }
            assert set(subscriptions) == {
                f"{channel.value}.{selected.contract_ticker}"
                for channel in (MarketChannel.TRADE, MarketChannel.QUOTE)
            }
            reader = RawJournalReader(journal_path)
            raw_records = reader.read_after(0, limit=1000)
            normalized_records = reader.read_normalized_after(0, limit=1000)
            assert len(raw_records) == len(frames)
            assert len(normalized_records) == len(frames)
            assert {
                row.event_kind for row in normalized_records
            } == {MarketChannel.TRADE, MarketChannel.QUOTE}
            market_map = MaterializerShardMap(
                "p8-options-market-state-v1",
                1,
                ((selected.instrument_id, 0),),
            )
            registration = build_advanced_options_journal_registration(
                control_plane=control,
                root=journal_root,
                path=journal_path,
                stream_session_id=(
                    control.source_flow.stream_session_ids[0]
                ),
                owner_epoch=1,
                normalizer_version=NORMALIZER_VERSION,
                shard_ids=(0,),
                shard_map_sha256=market_map.sha256,
            )
            registration = replace(
                registration,
                status="DRAINED",
                raw_committed_offset=journal_progress.last_raw_offset,
                normalized_committed_offset=(
                    journal_progress.last_event_offset
                ),
                bridge_committed_offset=(
                    journal_progress.last_event_offset
                ),
            )
            source = AdvancedOptionsJournalSource(
                root=journal_root,
                registration=registration,
                source_flow=source_flow,
            )
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                _seed_stream_session(
                    database_cursor,
                    control,
                    raw_offset=journal_progress.last_raw_offset,
                    event_offset=journal_progress.last_event_offset,
                    started_at=cutoff - timedelta(minutes=1),
                    ended_at=cutoff - timedelta(seconds=1),
                )
                _seed_equity_market_state(
                    database_cursor,
                    enrollment,
                    source_flow,
                    known_at=cutoff,
                )
                (
                    equity_receipt_id,
                    equity_window_id,
                    equity_receipt,
                ) = _seed_native_equity_inputs(
                    database_cursor,
                    enrollment,
                    source_flow,
                    equity_cutoff=equity_cutoff,
                )
                equity = enrollment.equity_plan
                persist_advanced_options_equity_input_binding_cursor(
                    database_cursor,
                    build_advanced_options_equity_input_binding(
                        target_options_window_id=window_id,
                        target_cohort_id=control.cohort_id,
                        source_cohort_id=equity.cohort_id,
                        source_cohort_sha256=equity.cohort_sha256,
                        source_manifest_id=equity.manifest_id,
                        source_manifest_sha256=equity.manifest.sha256,
                        source_profile_id=equity.profile_id,
                        source_profile_sha256=equity.profile_sha256,
                        equity_window_id=equity_window_id,
                        equity_receipt_id=equity_receipt_id,
                        equity_cutoff=equity_receipt.cutoff,
                        reference_generation=(
                            equity_receipt.reference_generation
                        ),
                        universe_sha256=equity_receipt.universe_sha256,
                        input_sha256=equity_receipt.input_sha256,
                        build_id=equity_receipt.build_id,
                        maximum_age_seconds=300,
                        authorization_reference="p8-equity-input",
                    ),
                )
            market_slice, actual_market_map = _persist_market_state(
                database_pool,
                provider,
                source,
                cohort_id=control.cohort_id,
                generation=source_flow.journal_generation,
                instrument_id=selected.instrument_id,
            )
            assert actual_market_map == market_map
            assert market_slice.end_offset == len(frames)
            flow_settings = OptionsFlowSettings(
                shard_count=1,
                queue_capacity=1024,
                history_per_contract=128,
                initial_baseline=BaselineKind.SESSION_COMPLETE,
                limits=ConsumerLimits(
                    batch_size=32,
                    max_messages=1024,
                    max_seconds=30,
                ),
                checkpoint_interval_seconds=0,
                checkpoint_interval_ms=1,
                advanced_flow_policy_sha256=flow_policy.sha256,
                contract_scope_sha256=contract_scope.sha256,
                exchange_reference_sha256=exchange.sha256,
            )
            flow_environment = dict(
                options_flow_semantics=semantics,
                options_flow_policy=detector_policy,
                options_flow_settings=flow_settings,
                options_flow_advanced_policy=flow_policy,
                options_flow_contract_scope=contract_scope,
                options_flow_exchange_reference=exchange,
                options_flow_oi_loader=lambda _context, _session: {},
            )
            world = World(
                database_pool,
                source,
                provider=provider,
                cohort=control.cohort_id,
                plan_id=control.plan_id,
                base_ms=int(cutoff.timestamp() * 1000),
                enabled=set(OPTIONS_NON_SERVING_STAGE_ORDER),
                stages=STAGES_V4,
                catalog_version=STAGE_CATALOG_VERSION_V4,
                **flow_environment,
            )
            flow_consumer = world.run_to_end()
            flow_rows = world.rows()
            assert flow_rows["events"]
            assert flow_rows["clusters"]
            assert len(flow_rows["volume"]) == 1
            assert flow_consumer.backlog() == 0
            before_restart = flow_rows
            restarted = World(
                database_pool,
                source,
                provider=provider,
                cohort=control.cohort_id,
                plan_id=control.plan_id,
                base_ms=int(cutoff.timestamp() * 1000),
                enabled=set(OPTIONS_NON_SERVING_STAGE_ORDER),
                stages=STAGES_V4,
                catalog_version=STAGE_CATALOG_VERSION_V4,
                **flow_environment,
            )
            restarted.run_to_end()
            assert restarted.session().start.mode == "RESTORED"
            assert restarted.rows() == before_restart
            current_heads = _query(
                database_pool,
                "SELECT 'Q',instrument_id,quality,reason_codes,"
                "NULL::text,NULL::bigint FROM rt.quote_current "
                "WHERE provider='massive' "
                "UNION ALL SELECT 'T',instrument_id,NULL,reason_codes,"
                "eligibility,source_sequence FROM rt.trade_current "
                "WHERE provider='massive' ORDER BY 1,2",
            )
            assert all(
                (
                    row[0] == "Q"
                    and row[2] == "GOOD"
                    and not row[3]
                )
                or (
                    row[0] == "T"
                    and row[4] == "INCLUDED"
                    and not row[3]
                )
                for row in current_heads
            ), current_heads

            rolling_plan = build_advanced_options_rolling_producer_plan(
                enrollment=enrollment,
                contract_scope=contract_scope,
                reference_state=reference,
                source_flow=source_flow,
                owner_epoch=1,
                committed_offset=len(frames),
                shard_ids=(0,),
                market_state_shard_map_sha256=market_map.sha256,
                flow_shard_map_sha256=shard_map_sha256(1),
                known_at=cutoff,
                valid_until=expires,
            )
            rolling = PostgresAdvancedOptionsRollingProducer(
                provider,
                rolling_plan,
                retry_initial_seconds=0,
            ).produce()
            mark_plan = build_advanced_options_mark_producer_plan(
                enrollment=enrollment,
                reference_state=reference,
                rolling_state=rolling,
                source_flow=source_flow,
                cutoff=cutoff,
                producer_build_id="advanced-options-nbbo-mark-v1",
                producer_build_sha256=_digest("mark-build"),
                valuation_policy=valuation_policy,
            )
            mark_preflight = PostgresAdvancedOptionsMarkProducer(
                provider,
                mark_plan,
            ).preflight()
            mark_plan = freeze_advanced_options_mark_producer_plan(
                mark_plan,
                mark_preflight,
            )
            marks = PostgresAdvancedOptionsMarkProducer(
                provider,
                mark_plan,
                retry_initial_seconds=0,
            ).produce()
            assert len(marks) == 1
            (
                flow_events,
                flow_cluster,
                flow_observations,
                cluster_observation,
            ) = _load_flow_evidence(
                provider,
                cohort_id=control.cohort_id,
                rolling=rolling,
                reference=reference,
            )
            package_flow_event = next(iter(flow_events.values()))
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                package = _seed_o3_invocation(
                    database_cursor,
                    enrollment,
                    marks[0],
                    cutoff=cutoff,
                    flow_event_id=package_flow_event.event_id,
                )
            producer_plan = build_advanced_options_producer_plan(
                control_plane=control,
                discovery_scope=_discovery_scope(control),
                provider_budget=control.provider_budget,
                clocks=AdvancedOptionsReferenceClocks(
                    session_date=control.session_date,
                    planned_at=planned,
                    market_cutoff=cutoff,
                    known_at=cutoff,
                    authorized_until=expires,
                    prior_oi_session=(
                        control.session_date - timedelta(days=1)
                    ),
                ),
                endpoint_response_byte_limits=_limits(),
            )
            native_candidate = build_advanced_options_native_input_plan(
                enrollment=enrollment,
                producer_plan=producer_plan,
                reference_state=reference,
                rolling_state=rolling,
                source_flow=source_flow,
                cutoff=cutoff,
                equity_input_receipt_id=equity_receipt_id,
                equity_input_receipt=equity_receipt,
                equity_window_id=equity_window_id,
                loader_build_id="advanced-options-native-loader-v1",
                history_source_cohort_id=history_cohort_id,
                maximum_equity_input_age_seconds=300,
                equity_context_requirements=(
                    initial_advanced_options_equity_context_requirements()
                ),
            )
            native_preflight = PostgresAdvancedOptionsNativeInputLoader(
                provider,
                native_candidate,
            ).preflight()
            assert (
                native_preflight.status is NativeInputPlanStatus.READY
            ), native_preflight.reason_codes
            o3_invocation = native_preflight.bundle.o3.invocations[0]
            assert {
                row.snapshot_id for row in o3_invocation.snapshots
            } == {
                row.snapshot_id for row in o3_invocation.candidate.legs
            }, (
                tuple(
                    row.snapshot_id
                    for row in o3_invocation.snapshots
                ),
                tuple(
                    row.snapshot_id
                    for row in o3_invocation.candidate.legs
                ),
            )
            native_plan = freeze_advanced_options_native_input_plan(
                native_candidate,
                native_preflight,
            )
            input_candidate = build_advanced_options_input_ready_plan(
                enrollment=enrollment,
                producer_plan=producer_plan,
                reference_producer_plan=reference_plan,
                rolling_producer_plan=rolling_plan,
                native_input_plan=native_plan,
                mark_producer_plan=mark_plan,
                native_executor_membership_sha256=(
                    native_preflight.executor_membership_sha256
                ),
                service_plan_id=control.plan_id,
                window_id=window_id,
                cutoff=cutoff,
                producer_build_id=BUILD_ID,
                producer_build_sha256=_digest("input-ready-build"),
            )
            package_mark = _package_mark(
                package,
                marks[0],
                valuation_policy,
                cutoff,
            )
            threshold = _threshold(flow_cluster, flow_events, rolling)

            def evidence_builder(cycle):
                evidences = []
                for evaluation in cycle.evaluations:
                    selected_mark = (
                        package_mark
                        if evaluation.package is not None
                        else None
                    )
                    evidences.append(build_flow_package_evidence(
                        evaluation,
                        flow_observations=flow_observations,
                        cluster_observations=(cluster_observation,),
                        threshold_evidence=threshold,
                        reference_state=reference,
                        rolling_state=rolling,
                        package_mark=selected_mark,
                        model_version=(
                            reference.valuation_inputs[0].model_version
                            if selected_mark is not None
                            else None
                        ),
                        model_bundle_sha256=(
                            control.pins.model_bundle_sha256
                            if selected_mark is not None
                            else None
                        ),
                        valuation_policy_sha256=(
                            control.pins.valuation_policy_sha256
                            if selected_mark is not None
                            else None
                        ),
                        degraded_reason_codes=(
                            rolling.iv_surface.reason_codes
                        ),
                    ))
                return build_flow_package_evidence_batch(
                    cycle,
                    tuple(evidences),
                )

            scope = AdvancedOptionsPostgresRuntimeScope(
                control_plane=control,
                producer_plan=producer_plan,
                code_sha256=input_candidate.producer_build_sha256,
                native_inputs_loader=None,
                evidence_builder=evidence_builder,
                close_data_loader=(
                    lambda _context, _projection: pytest.fail(
                        "P8 qualification attempted retention handoff"
                    )
                ),
                flow_identity=AdvancedOptionsDatasetFlowIdentity(
                    source_flow,
                    flow_policy.sha256,
                    contract_scope.sha256,
                    control.manifest_sha256,
                    control.slot_set_sha256,
                    package_flow_event.runtime_policy_sha256,
                ),
                require_discovery_pages=False,
                reference_producer_plan=reference_plan,
                rolling_producer_plan=rolling_plan,
                native_input_plan=native_plan,
                mark_producer_plan=mark_plan,
                input_ready_plan=input_candidate,
            )
            port = PostgresAdvancedOptionsCanonicalRuntimePort(
                provider,
                scope,
            )
            input_preflight = port.preflight_input_ready()
            assert (
                input_preflight.status
                is AdvancedOptionsInputDependencyStatus.AVAILABLE
            )
            input_plan = freeze_advanced_options_input_ready_plan(
                input_candidate,
                input_preflight,
            )
            port = PostgresAdvancedOptionsCanonicalRuntimePort(
                provider,
                replace(scope, input_ready_plan=input_plan),
            )
            operations = SimpleNamespace(
                acquire=lambda _references, _control, *, cancelled: (
                    OptionsAcquireResult(
                    control.manifest_id,
                    tuple(control.universe.option_underlyings),
                    tuple(control.universe.option_underlyings),
                    0,
                    0,
                    )
                ),
                admit=lambda _references, _control, *, cancelled: (
                    OptionsAdmissionResult(
                    uuid5(
                        NAMESPACE_URL,
                        f"p8-admission:{control.cohort_id}",
                    ),
                    "READY",
                    (),
                    control.cohort_id,
                    control.slot_set.version,
                    len(control.slot_set.members),
                    )
                ),
            )
            binding, _loaders = (
                build_canonical_advanced_options_runtime_binding(
                    build_id=BUILD_ID,
                    pre_session=OptionsPreSessionStageBinding(operations),
                    port=port,
                )
            )
            environment = binding.bind(world.environment)
            runners = build_advanced_options_non_serving_runners(
                provider,
                environment=environment,
            )
            repository = PostgresOrchestrationRepository(provider)
            for stage_id in ("REFERENCE", "EQUITY_MATERIALIZE"):
                repository.upsert_readiness(
                    control.cohort_id,
                    stage_id,
                    STAGE_CATALOG_VERSION_V4,
                    "READY",
                    (),
                    f"p8-qualified-{stage_id.lower()}",
                    datetime.now(UTC),
                )
            orchestrator = StageOrchestrator(
                repository,
                runners,
                database=provider,
                stages=STAGES_V4,
                catalog_version=STAGE_CATALOG_VERSION_V4,
                clock=lambda: datetime.now(UTC),
                parallel_lanes=False,
            )
            start = orchestrator.start(control.plan_id)
            window_report = orchestrator.run_window(window_id)
            stage_rows = _query(
                database_pool,
                "SELECT stage_id,state,reason_codes "
                "FROM rt.stage_runs WHERE window_id=%s "
                "ORDER BY started_at,stage_id",
                (str(window_id),),
            )
            assert start.enabled == frozenset(
                OPTIONS_NON_SERVING_STAGE_ORDER
            )
            assert (
                window_report.status,
                window_report.window_state,
            ) == ("FINISHED", "COMPLETE"), stage_rows
            cycle = binding.handoff.cycle(control.cohort_id, cutoff)
            projection = binding.handoff.projection(
                control.cohort_id,
                cutoff,
            )
            assert cycle is not None
            assert projection is not None
            outcomes = {
                row.detector.value: row for row in cycle.outcomes
            }
            assert set(outcomes) == {"O1", "O2", "O3"}
            assert outcomes["O2"].state.value == "UNAVAILABLE"
            assert outcomes["O2"].reason_codes == (
                "O2_MODEL_NOT_ACCEPTED",
            )
            assert all(
                outcomes[name].state.value.startswith("COMPLETE")
                for name in ("O1", "O3")
            )
            assert tuple(
                row.snapshot_type for row in projection.snapshots
            ) == OPTIONS_SNAPSHOT_TYPES
            with provider.transaction(
                APP_ROLE,
                read_only=True,
            ) as unit:
                replayed_page_source = binding.page_source_loader(
                    unit.cursor,
                    control_plane=control,
                    cycle=cycle,
                    generated_at=cutoff,
                )
            replayed_projection = build_advanced_options_projection_batch(
                control_plane=control,
                cycle=cycle,
                generated_at=cutoff,
                build_id=BUILD_ID,
                page_source=replayed_page_source,
            )
            assert replayed_projection == projection
            selected_evaluation = next(
                row for row in cycle.evaluations
                if row.selection_status == "SELECTED"
            )
            screening_underlying = (
                cycle.input_gate.closure.cycle_input.underlyings[0]
            )
            screening = build_screening_generation(
                cohort_id=control.cohort_id,
                provider_tier=ProviderTier.MASSIVE_ADVANCED,
                session_date=control.session_date,
                expected_members=1,
                generated_at=cutoff - timedelta(seconds=40),
                manifest={
                    "version": "advanced-options-p8-stock-screen-v1",
                    "source_manifest_sha256": control.manifest_sha256,
                    "source_generation": (
                        cycle.receipt.reference_generation
                    ),
                },
                rows=((
                    screening_underlying.underlying_security_id,
                    screening_underlying.underlyer,
                    True,
                    {
                        "p8_bound_options_cycle": cycle.cycle_sha256,
                    },
                ),),
            )
            with provider.transaction(APP_ROLE) as unit:
                persist_screening_generation_cursor(
                    unit.cursor,
                    screening,
                )
            alert_event, alert_state, alert = _shape_alert(
                provider,
                control=control,
                selected=selected,
                source_flow=source_flow,
                flow_event=package_flow_event,
                evaluation=selected_evaluation,
                cutoff=cutoff,
                committed_offset=len(frames),
            )
            receipt_rows = _query(
                database_pool,
                "SELECT receipt_kind,receipt_id::text "
                "FROM rt.cycle_receipts WHERE window_id=%s",
                (str(window_id),),
            )
            durable_receipts = {
                kind: UUID(receipt_id)
                for kind, receipt_id in receipt_rows
            }
            probe_receipts = {}
            with provider.transaction(APP_ROLE) as unit:
                for probe, probe_cutoff in (
                    ("stale", cutoff - timedelta(minutes=2)),
                    ("rollback", cutoff + timedelta(minutes=2)),
                ):
                    probe_window_id = uuid4()
                    unit.cursor.execute(
                        "INSERT INTO rt.window_runs ("
                        "window_id,plan_id,cohort_id,provider_tier,"
                        "window_key,release_at,cutoff,deadline,state) "
                        "VALUES (%s,%s,%s,'MASSIVE_ADVANCED',%s,"
                        "%s,%s,%s,'PENDING')",
                        (
                            str(probe_window_id),
                            str(control.plan_id),
                            str(control.cohort_id),
                            f"p8-{probe}-publication-probe",
                            probe_cutoff,
                            probe_cutoff,
                            cutoff + timedelta(minutes=30),
                        ),
                    )
                    probe_receipts[probe] = (
                        persist_cycle_receipt_cursor(
                            unit.cursor,
                            window_id=probe_window_id,
                            cohort_id=control.cohort_id,
                            spec=CycleReceiptSpec(
                                "OPTIONS_PORTAL_READY",
                                probe_cutoff,
                                cycle.receipt.reference_generation,
                                control.universe.sha256,
                                _digest((probe, "publication-probe")),
                                BUILD_ID,
                            ),
                        )
                    )
            publisher_database = _publisher_database(
                database_pool,
                roles,
            )
            publisher_config = PortalPublicationConfig(
                cohort_id=control.cohort_id,
                provider_tier=ProviderTier.MASSIVE_ADVANCED,
                owner_id=uuid4(),
                owner_epoch=1,
                dispatch_authorized=False,
            )
            stock_alert_snapshot = build_portal_snapshot(
                cohort_id=control.cohort_id,
                provider_tier=ProviderTier.MASSIVE_ADVANCED,
                snapshot_type=STOCK_ALERTS_VERSION,
                snapshot_key="PLATFORM",
                source_generation=cycle.receipt.reference_generation,
                source_manifest={
                    "version": STOCK_ALERTS_VERSION,
                    "cohort_id": str(control.cohort_id),
                    "source_manifest_sha256": control.manifest_sha256,
                    "source_generation": (
                        cycle.receipt.reference_generation
                    ),
                    "provider_tier": "MASSIVE_ADVANCED",
                    "screening_generation_id": str(
                        screening.generation_id
                    ),
                    "reference_set": {
                        "count": 0,
                        "sha256": canonical_sha256([]),
                    },
                },
                payload={
                    "version": STOCK_ALERTS_VERSION,
                    "references": [],
                    "screening_generation_id": str(
                        screening.generation_id
                    ),
                    "streams": [],
                    "shadow": {},
                    "review_publications": [],
                    "schedule_streams": [],
                    "missing_producers": {},
                    "provider_fetch_performed": False,
                    "publication_permission": False,
                    "execution_permission": False,
                },
                generated_at=cutoff - timedelta(seconds=30),
                valid_until=expires,
            )
            stock_alert_receipt = PortalReleaseReceipt(
                durable_receipts["OPTIONS_INPUT_READY"],
                control.cohort_id,
                ProviderTier.MASSIVE_ADVANCED,
                cutoff,
                control.universe.sha256,
            )
            stock_alert_status = _publish(
                publisher_database,
                publisher_config,
                PortalPublicationBatch(
                    (stock_alert_snapshot,),
                    receipt=stock_alert_receipt,
                ),
                checked_at=cutoff + timedelta(minutes=1),
            )
            assert stock_alert_status.claimed == 0
            inert_handoff = build_advanced_options_publisher_handoff(
                control_plane=control,
                batch=projection,
            )
            publication_authorization = (
                AdvancedOptionsCanaryPublicationAuthorization(
                    database_name=database_name,
                    cohort_id=control.cohort_id,
                    batch_sha256=projection.batch_sha256,
                    receipt_id=durable_receipts[
                        "OPTIONS_PORTAL_READY"
                    ],
                )
            )
            canary_handoff = (
                authorize_advanced_options_canary_publication(
                    inert_handoff,
                    publication_authorization,
                )
            )
            assert canary_handoff.release_authorized
            assert canary_handoff.pointer_authorized
            assert not canary_handoff.dispatch_authorized
            canary_batch = PortalPublicationBatch(
                canary_handoff.snapshots,
                receipt=canary_handoff.receipt,
            )
            canary_status = _publish(
                publisher_database,
                publisher_config,
                canary_batch,
                checked_at=cutoff + timedelta(minutes=1),
            )
            assert canary_status.promoted_snapshots == len(
                OPTIONS_SNAPSHOT_TYPES
            )
            assert canary_status.claimed == 0
            release_id, release_cutoff, manifest, manifest_sha256 = (
                _pointer(database_pool, control.cohort_id)
            )
            assert release_cutoff == cutoff
            entries = {
                (row["snapshot_type"], row["snapshot_key"]): row
                for row in manifest["snapshots"]
            }
            assert entries[(STOCK_ALERTS_VERSION, "PLATFORM")][
                "snapshot_id"
            ] == str(stock_alert_snapshot.snapshot_id)
            assert {
                identity for identity in entries
                if identity[0].startswith("OPTIONS_")
            } == {
                (snapshot_type, "CURRENT")
                for snapshot_type in OPTIONS_SNAPSHOT_TYPES
            }
            release_count = _query(
                database_pool,
                "SELECT count(*) FROM rt.portal_releases "
                "WHERE cohort_id=%s",
                (str(control.cohort_id),),
            )[0][0]
            replay_status = _publish(
                publisher_database,
                publisher_config,
                canary_batch,
                checked_at=cutoff + timedelta(minutes=1),
            )
            assert replay_status.claimed == 0
            assert _pointer(
                database_pool,
                control.cohort_id,
            ) == (
                release_id,
                release_cutoff,
                manifest,
                manifest_sha256,
            )
            assert _query(
                database_pool,
                "SELECT count(*) FROM rt.portal_releases "
                "WHERE cohort_id=%s",
                (str(control.cohort_id),),
            )[0][0] == release_count

            stale_snapshot = build_portal_snapshot(
                cohort_id=control.cohort_id,
                provider_tier=ProviderTier.MASSIVE_ADVANCED,
                snapshot_type="OPTIONS_MARKS",
                snapshot_key="CURRENT",
                source_generation=1,
                source_manifest={"lane": "OPTIONS", "stale": True},
                payload={"stale": True},
                generated_at=cutoff - timedelta(minutes=2),
            )
            stale_receipt = PortalReleaseReceipt(
                probe_receipts["stale"],
                control.cohort_id,
                ProviderTier.MASSIVE_ADVANCED,
                cutoff - timedelta(minutes=2),
                control.universe.sha256,
            )
            _publish(
                publisher_database,
                publisher_config,
                PortalPublicationBatch(
                    (stale_snapshot,),
                    receipt=stale_receipt,
                ),
                checked_at=cutoff + timedelta(minutes=1),
            )
            assert _pointer(
                database_pool,
                control.cohort_id,
            )[0] == release_id

            rollback_snapshot = build_portal_snapshot(
                cohort_id=control.cohort_id,
                provider_tier=ProviderTier.MASSIVE_ADVANCED,
                snapshot_type="OPTIONS_MARKS",
                snapshot_key="ROLLBACK_PROBE",
                source_generation=100,
                source_manifest={"lane": "OPTIONS", "rollback": True},
                payload={"rollback": True},
                generated_at=cutoff + timedelta(minutes=2),
            )
            rollback_receipt = PortalReleaseReceipt(
                probe_receipts["rollback"],
                control.cohort_id,
                ProviderTier.MASSIVE_ADVANCED,
                cutoff + timedelta(minutes=2),
                control.universe.sha256,
            )

            def fail_release(*_args, **_options):
                raise RuntimeError("P8_FORCED_RELEASE_ROLLBACK")

            with pytest.raises(
                RuntimeError,
                match="P8_FORCED_RELEASE_ROLLBACK",
            ):
                _publish(
                    publisher_database,
                    publisher_config,
                    PortalPublicationBatch(
                        (rollback_snapshot,),
                        receipt=rollback_receipt,
                    ),
                    checked_at=cutoff + timedelta(minutes=3),
                    release_repository=fail_release,
                )
            assert _query(
                database_pool,
                "SELECT count(*) FROM rt.snapshot_catalog "
                "WHERE snapshot_id=%s",
                (str(rollback_snapshot.snapshot_id),),
            )[0][0] == 0
            assert _pointer(
                database_pool,
                control.cohort_id,
            )[0] == release_id

            @contextmanager
            def stored_cursor():
                with provider.transaction(
                    API_ROLE,
                    read_only=True,
                    isolation_level="REPEATABLE READ",
                ) as unit:
                    with unit.connection.cursor(
                        cursor_factory=RealDictCursor,
                    ) as database_cursor:
                        yield database_cursor

            from realtime.advanced_options_api import (
                AdvancedOptionsStoredProjectionRepository,
            )

            advanced_repository = (
                AdvancedOptionsStoredProjectionRepository(stored_cursor)
            )
            option_flow_repository = RealtimeOptionFlowRepository(
                stored_cursor,
                transaction_configured=True,
            )
            option_screener_repository = RealtimeOptionScreenerRepository(
                stored_cursor,
                transaction_configured=True,
            )
            option_alert_repository = RealtimeOptionsAlertsPageRepository(
                stored_cursor,
                transaction_configured=True,
            )
            api_app = create_realtime_portal_app(
                RealtimePortalRepositories(
                    None,
                    None,
                    None,
                    option_flow=option_flow_repository,
                    option_screener=option_screener_repository,
                    option_alert_views=option_alert_repository,
                    advanced_options=advanced_repository,
                ),
                clock=lambda: cutoff + timedelta(minutes=1),
                database_target=database_name,
                advanced_options_v2_enabled=True,
            )
            api_reads = {}
            with TestClient(api_app) as client:
                for snapshot_type in OPTIONS_PROJECTION_TYPES:
                    response = client.get(
                        "/api/realtime/options-v2/projections/"
                        f"{snapshot_type}",
                        params={"cohort_id": str(control.cohort_id)},
                    )
                    assert response.status_code == 200, response.text
                    payload = response.json()
                    assert payload["available"]
                    assert payload["release_id"] == release_id
                    assert not any((
                        payload["provider_fetch_performed"],
                        payload["detector_work_performed"],
                        payload["publication_performed"],
                    ))
                    api_reads[snapshot_type] = payload["snapshot_id"]
                cohort_params = {
                    "cohort_id": str(control.cohort_id),
                }
                flow_response = client.get(
                    "/api/options/flow",
                    params={
                        **cohort_params,
                        "underlyer": screening_underlying.underlyer,
                    },
                )
                assert flow_response.status_code == 200, (
                    flow_response.text
                )
                flow_payload = flow_response.json()
                assert flow_payload["available"]
                assert (
                    flow_payload["data"]["selected"]
                    == screening_underlying.underlyer
                )
                screener_response = client.post(
                    "/api/options/eligible-chain/query",
                    params=cohort_params,
                    json={
                        "filters": [],
                        "sort": "day_volume",
                        "descending": True,
                        "limit": 100,
                        "offset": 0,
                    },
                )
                assert screener_response.status_code == 200, (
                    screener_response.text
                )
                assert screener_response.json()["available"]
                datasets_response = client.get(
                    "/api/options/alerts/datasets",
                    params=cohort_params,
                )
                assert datasets_response.status_code == 200, (
                    datasets_response.text
                )
                assert datasets_response.json()["available"]
                evaluation_response = client.get(
                    "/api/options/alerts/evaluations",
                    params=cohort_params,
                )
                assert evaluation_response.status_code == 200, (
                    evaluation_response.text
                )
                assert evaluation_response.json()["available"]
                detector_response = client.get(
                    "/api/options/alerts/detector-runs",
                    params={
                        **cohort_params,
                        "dataset_id": str(cycle.dataset_id),
                    },
                )
                assert detector_response.status_code == 200, (
                    detector_response.text
                )
                detector_payload = detector_response.json()
                assert detector_payload["available"]
                assert detector_payload["data"]["rows"]
                assert all(
                    row["current_mark"]["status"] == "FRESH"
                    for row in detector_payload["data"]["rows"]
                )
            page_snapshots = {
                row.payload["artifact_kind"]: row
                for row in projection.snapshots
                if row.snapshot_type
                in OPTIONS_PAGE_SNAPSHOT_TYPES.values()
            }
            page_scope_sha256 = next(iter({
                row.payload["scope_sha256"]
                for row in page_snapshots.values()
            }))
            page_gate_package = {
                "package_sha256": _digest("p8-page-gate-package"),
                "window": {
                    "start_utc": (
                        cutoff - timedelta(minutes=1)
                    ).isoformat(),
                    "stop_utc": (
                        cutoff + timedelta(minutes=2)
                    ).isoformat(),
                },
                "cohort": {
                    "cohort_id": str(control.cohort_id),
                },
                "publication": {
                    "projection_types": list(
                        OPTIONS_PROJECTION_TYPES
                    ),
                    "page_artifacts": [
                        {
                            "artifact_kind": kind,
                            "snapshot_type": (
                                OPTIONS_PAGE_SNAPSHOT_TYPES[kind]
                            ),
                            "contract": page_snapshots[
                                kind
                            ].payload["contract"],
                        }
                        for kind in OPTIONS_PAGE_SNAPSHOT_TYPES
                    ],
                },
            }
            page_gate_observation = {
                "schema": PAGE_RELEASE_OBSERVATION_SCHEMA,
                "checked_at_utc": (
                    cutoff + timedelta(minutes=1)
                ).isoformat(),
                "cohort_id": str(control.cohort_id),
                "scope_sha256": page_scope_sha256,
                "release_id": release_id,
                "published_at_utc": cutoff.isoformat(),
                "valid_until_utc": expires.isoformat(),
                "artifacts": [
                    {
                        "artifact_kind": kind,
                        "snapshot_type": (
                            OPTIONS_PAGE_SNAPSHOT_TYPES[kind]
                        ),
                        "contract": page_snapshots[
                            kind
                        ].payload["contract"],
                        "snapshot_id": str(
                            page_snapshots[kind].snapshot_id
                        ),
                        "cohort_id": str(control.cohort_id),
                        "release_id": release_id,
                        "scope_sha256": page_scope_sha256,
                        "pages": (
                            ["OPTIONS_ACTIVITY", "OPTIONS_FLOW"]
                            if kind == "FLOW"
                            else ["OPTIONS_SCREENER"]
                            if kind == "SCREENER"
                            else ["OPTIONS_ALERTS"]
                        ),
                    }
                    for kind in OPTIONS_PAGE_SNAPSHOT_TYPES
                ],
                "stock_publications": {
                    "screening": {
                        "page": "STOCK_SCREENER",
                        "contract": "SCREENING_GENERATION",
                        "generation_id": str(
                            screening.generation_id
                        ),
                        "cohort_id": str(control.cohort_id),
                        "provider_tier": "MASSIVE_ADVANCED",
                        "status": "COMPLETE",
                        "generated_at_utc": (
                            screening.generated_at.isoformat()
                        ),
                    },
                    "alerts": {
                        "page": "STOCK_ALERTS",
                        "contract": "STOCK_ALERTS_V1/PLATFORM",
                        "release_id": release_id,
                        "snapshot_id": str(
                            stock_alert_snapshot.snapshot_id
                        ),
                        "screening_generation_id": str(
                            screening.generation_id
                        ),
                        "cohort_id": str(control.cohort_id),
                        "provider_tier": "MASSIVE_ADVANCED",
                        "generated_at_utc": (
                            stock_alert_snapshot.generated_at.isoformat()
                        ),
                    },
                },
                "portal_projection_diagnostics": list(
                    OPTIONS_PROJECTION_TYPES
                ),
                "request_time_effects": {
                    "provider_fetches": 0,
                    "detector_runs": 0,
                    "publication_retries": 0,
                    "dispatches": 0,
                    "deliveries": 0,
                    "broker_actions": 0,
                    "trades": 0,
                },
            }
            page_gate = validate_p8_page_release_observation(
                page_gate_package,
                page_gate_observation,
            )
            assert page_gate["status"] == (
                "PASS_P8_COMBINED_PAGE_RELEASE_GATE"
            )
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                unit.cursor.execute(
                    "SELECT count(*),count(delivered_at),"
                    "count(abandoned_at),count(lease_owner) "
                    "FROM rt.alert_outbox WHERE cohort_id=%s",
                    (str(control.cohort_id),),
                )
                outbox_counts = tuple(unit.cursor.fetchone())
                unit.cursor.execute(
                    "SELECT count(*) FROM rt.alert_current "
                    "WHERE cohort_id=%s AND event_id=%s",
                    (
                        str(control.cohort_id),
                        str(alert_event.event_id),
                    ),
                )
                assert unit.cursor.fetchone()[0] == 1
                unit.cursor.execute(
                    "SELECT count(*) FROM rt.advanced_options_flow_package_evidence "
                    "WHERE dataset_id=%s",
                    (str(cycle.dataset_id),),
                )
                evidence_count = int(unit.cursor.fetchone()[0])
                unit.cursor.execute(
                    "SELECT count(*) FROM rt.stage_runs AS stage "
                    "JOIN rt.window_runs AS window_run "
                    "ON window_run.window_id=stage.window_id "
                    "WHERE window_run.cohort_id=%s "
                    "AND stage.stage_id LIKE 'EQUITY%%'",
                    (str(control.cohort_id),),
                )
                equity_stage_runs = int(unit.cursor.fetchone()[0])
            assert outbox_counts == (1, 0, 0, 0)
            assert evidence_count == len(cycle.evaluations)
            assert equity_stage_runs == 0
            categories = _disposable_audit(
                database_pool,
                enrollment,
                cutoff,
                equity_cutoff=equity_cutoff,
                history_source_cohort_id=history_cohort_id,
            )
            assert all(
                row["status"] in {"AVAILABLE", "COMPLETE_ZERO"}
                for row in categories.values()
            ), categories

            frontend_result = subprocess.run(
                [
                    "node",
                    "--test",
                    "frontend\\tests\\optionsAlertsRealtime.test.mjs",
                ],
                cwd=Path(__file__).parents[3],
                check=False,
                capture_output=True,
                text=True,
                timeout=60,
            )
            assert frontend_result.returncode == 0, (
                frontend_result.stdout + frontend_result.stderr
            )
            shutil.rmtree(journal_root)
            assert not journal_root.exists()
            report = {
                "contract": (
                    "advanced-options-p8-provider-free-"
                    "qualification-report-v3"
                ),
                "verdict": "PASS",
                "database": database_name,
                "cohort_id": str(control.cohort_id),
                "plan_id": str(control.plan_id),
                "window_id": str(window_id),
                "provider": {
                    "calls": 0,
                    "endpoint": "loopback-only",
                    "frames": len(frames),
                    "normalized_messages": len(normalized_records),
                    "buffer_full_count": buffer_snapshot.full_count,
                },
                "equity_dependencies": {
                    "historical_source_cohort_id": str(
                        history_cohort_id
                    ),
                    "live_source_cohort_id": str(
                        enrollment.equity_plan.cohort_id
                    ),
                    "equity_cutoff_utc": equity_cutoff.isoformat(),
                    "options_cutoff_utc": cutoff.isoformat(),
                    "equity_input_age_seconds": int(
                        (cutoff - equity_cutoff).total_seconds()
                    ),
                    "maximum_equity_input_age_seconds": 300,
                    "sources_are_distinct": (
                        history_cohort_id
                        != enrollment.equity_plan.cohort_id
                    ),
                    "full_path_fixture_underlyings": list(
                        control.universe.option_underlyings
                    ),
                    "required_live_underlyings": ["QQQ", "SPY"],
                    "pre_provider_gate": "EXACT_PER_UNDERLYING",
                },
                "canonical_state": {
                    "market_checkpoint_offset": market_slice.end_offset,
                    "flow_checkpoint_offset": (
                        restarted.session().consumer.checkpointed_offset
                    ),
                    "flow_events": len(flow_rows["events"]),
                    "flow_clusters": len(flow_rows["clusters"]),
                    "volume_rows": len(flow_rows["volume"]),
                    "input_categories": len(categories),
                    "input_ready": "AVAILABLE",
                    "marks": len(marks),
                },
                "cycle": {
                    "dataset_id": str(cycle.dataset_id),
                    "cycle_sha256": cycle.cycle_sha256,
                    "outcomes": {
                        name: {
                            "state": outcome.state.value,
                            "reason_codes": list(outcome.reason_codes),
                        }
                        for name, outcome in sorted(outcomes.items())
                    },
                    "evaluations": len(cycle.evaluations),
                    "flow_package_evidence": evidence_count,
                    "atomic": True,
                },
                "projections": {
                    "count": len(projection.snapshots),
                    "types": list(OPTIONS_SNAPSHOT_TYPES),
                    "diagnostics": list(OPTIONS_PROJECTION_TYPES),
                    "page_artifacts": list(
                        OPTIONS_PAGE_SNAPSHOT_TYPES.values()
                    ),
                    "batch_sha256": projection.batch_sha256,
                    "receipt": "OPTIONS_PORTAL_READY",
                },
                "publication": {
                    "owner": canary_handoff.owner_name,
                    "release_id": release_id,
                    "release_cutoff": release_cutoff.isoformat(),
                    "manifest_sha256": manifest_sha256,
                    "entry_count": len(entries),
                    "stock_alert_snapshot_preserved": str(
                        stock_alert_snapshot.snapshot_id
                    ),
                    "stock_screening_generation_id": str(
                        screening.generation_id
                    ),
                    "pointer_count": 1,
                    "deterministic_replay": True,
                    "stale_release_fenced": True,
                    "forced_rollback_clean": True,
                },
                "stored_reads": {
                    "api_flag": "REALTIME_V2_ADVANCED_OPTIONS_READS",
                    "frontend_flag": "VITE_REALTIME_OPTIONS_V2",
                    "api_snapshot_ids": api_reads,
                    "full_page_routes": {
                        "flow": {
                            "available": True,
                            "received_flow_events": (
                                flow_payload["data"][
                                    "selected_summary"
                                ]["received_row_count"]
                            ),
                            "top_contracts": len(
                                flow_payload["data"]["top_contracts"]
                            ),
                        },
                        "screener": {
                            "available": True,
                            "rows": (
                                screener_response.json()["data"]["total"]
                            ),
                        },
                        "alerts": {
                            "available": True,
                            "evaluations": (
                                evaluation_response.json()["data"]["total"]
                            ),
                            "selected": len(
                                detector_payload["data"]["rows"]
                            ),
                            "fresh_current_marks": sum(
                                row["current_mark"]["status"] == "FRESH"
                                for row in detector_payload["data"]["rows"]
                            ),
                        },
                    },
                    "frontend_test_exit_code": (
                        frontend_result.returncode
                    ),
                },
                "combined_page_gate": page_gate,
                "alerts": {
                    "event_id": str(alert_event.event_id),
                    "state_sha256": alert_state.state_sha256,
                    "alert_id": str(alert.alert_id),
                    "outbox_pending": outbox_counts[0],
                    "outbox_dispatched": 0,
                    "delivered": 0,
                    "broker_execution": False,
                    "trade_execution": False,
                },
                "resilience": {
                    "journal_replay_equal": True,
                    "flow_restart_mode": "RESTORED",
                    "projection_replay_equal": True,
                    "equity_stage_runs": equity_stage_runs,
                },
                "authority": {
                    "owned_disposable_release": True,
                    "owned_disposable_pointer": True,
                    "production_release": False,
                    "production_pointer": False,
                    "provider": False,
                    "dispatch": False,
                    "delivery": False,
                    "broker": False,
                    "trade": False,
                    "repair": False,
                    "backfill": False,
                    "archive": False,
                    "retention": False,
                },
            }
    finally:
        if journal_root.exists():
            shutil.rmtree(journal_root)
        cleanup = {
            "databases": [],
            "roles": [],
            "files": [],
            "processes": [],
            "locks": [],
        }
        with closing(_connect(settings, "postgres")) as connection_value:
            connection_value.autocommit = True
            if roles_owned:
                drop_disposable_roles_v2(connection_value, roles)
            with connection_value.cursor() as database_cursor:
                database_cursor.execute(
                    "SELECT datname FROM pg_database WHERE datname=%s",
                    (database_name,),
                )
                cleanup["databases"] = [
                    row[0] for row in database_cursor.fetchall()
                ]
                database_cursor.execute(
                    "SELECT rolname FROM pg_roles WHERE rolname=ANY(%s)",
                    (list(roles.all_names),),
                )
                cleanup["roles"] = [
                    row[0] for row in database_cursor.fetchall()
                ]
        if journal_root.exists():
            cleanup["files"] = [
                str(path) for path in journal_root.rglob("*")
            ]
        assert cleanup == {
            "databases": [],
            "roles": [],
            "files": [],
            "processes": [],
            "locks": [],
        }
        if report is not None:
            report["cleanup"] = cleanup
            if os.environ.get(REPORT_ENV) == "1":
                path, digest = _retain_report(report)
                with capsys.disabled():
                    print(
                        "\nP8_PROVIDER_FREE_QUALIFICATION "
                        f"report={path} sha256={digest}"
                    )
