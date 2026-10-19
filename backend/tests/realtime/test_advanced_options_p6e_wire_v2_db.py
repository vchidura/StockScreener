"""Provider-free P6E single-owner wire path on disposable PostgreSQL."""
from __future__ import annotations

import asyncio
import json
import time
from contextlib import closing
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from psycopg2.extras import execute_batch
from websockets.asyncio.server import serve

from option_flow_support import advanced_contract_scope, advanced_policy, policy
from realtime.advanced_gateway import (
    AdvancedGateway,
    GatewayConfiguration,
    GatewayFeedState,
)
from realtime.advanced_journal_registry import (
    build_advanced_journal_registration,
)
from realtime.advanced_options_control_plane import (
    CANONICAL_MARKET_STATE_OWNER,
)
from realtime.advanced_options_consumers import (
    AdvancedOptionsMarketStateConsumer,
    persist_advanced_options_market_state_slice_cursor,
)
from realtime.advanced_options_ingress import (
    AdvancedOptionsJournalPump,
    AdvancedOptionsJournalSource,
)
from realtime.advanced_options_provider_contract import (
    MASSIVE_OPTIONS_PROVIDER_CONTRACT,
    load_default_options_exchange_reference,
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
from realtime.database_access_v2 import (
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.materialization import MaterializerShardMap
from realtime.journal import RawJournalReader
from realtime.normalizer import InstrumentAdmission, NORMALIZER_VERSION
from realtime.option_flow.conditions import (
    load_default_live_semantics,
)
from realtime.option_flow.consumer import ConsumerLimits
from realtime.option_flow.engine import (
    DEFAULT_SHARD_COUNT,
    EngineConfig,
)
from realtime.option_flow.volume import BaselineKind, VolumeConfig
from realtime.option_flow.writer import write_condition_reference
from realtime.option_source_flow import (
    build_option_source_flow_identity,
    persist_option_flow_policy_cursor,
)
from realtime.stage_graph import STAGES_V1
from realtime.stage_runners_options import OptionsFlowSettings
from realtime.security_v2 import (
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from test_option_flow_runner_v2_db import (
    World,
    _execute,
    _query,
)
from v2_database import (
    _connect,
    _settings,
    disposable_v2_database,
    insert_cohort,
    requires_v2_database,
)


pytestmark = requires_v2_database
DATABASE = "test_realtime_v2_options_p6e_wire"
ROLE_PREFIX = "test_rt_options_p6e_wire"
UTC = timezone.utc
SESSION = date(2026, 10, 16)
START = datetime(2026, 10, 16, 14, 30, tzinfo=UTC)
CONTRACTS = (
    "O:SPY261120C00500000",
    "O:SPY261120P00450000",
)
CONTRACT_IDS = (101, 102)
VERSION_IDS = (201, 202)
INSTRUMENT_IDS = (1001, 1002)


@pytest.fixture(scope="module")
def database():
    roles = build_role_names_v2(ROLE_PREFIX)
    settings = _settings()
    try:
        with disposable_v2_database(DATABASE) as connection_pool:
            connection = connection_pool.getconn()
            try:
                created, audit = harden_database_v2(connection, roles)
            finally:
                connection_pool.putconn(connection)
            assert all(state.startswith("PASS") for _, state in audit)
            yield connection_pool, roles, tuple(created), tuple(audit)
    finally:
        with closing(_connect(settings, "postgres")) as connection:
            connection.autocommit = True
            drop_disposable_roles_v2(connection, roles)
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT rolname FROM pg_roles WHERE rolname=ANY(%s)",
                    (list(roles.all_names),),
                )
                assert cursor.fetchall() == []


def _app_provider(pool, roles):
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=roles.prefix,
            allow_disposable=True,
        ),
    )
    provider.verify()
    return provider


def _manifest():
    universe = UniverseManifest(
        "p6e-options-wire-v1",
        ("SPY",),
        ("SPY",),
    )
    return SubscriptionManifest(
        version="p6e-options-wire-v1",
        session_date=SESSION,
        universe=universe,
        capability_profile_sha256="1" * 64,
        allocation_policy_sha256="2" * 64,
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
                    contract,
                    "SPY",
                    channel,
                    catalog_version_id=version_id,
                )
                for contract, version_id in zip(CONTRACTS, VERSION_IDS)
                for channel in (MarketChannel.TRADE, MarketChannel.QUOTE)
            ),
        ),
        maximum_option_quote_contracts=2,
    )


def _frames(count=500):
    base_ms = int(START.timestamp() * 1000)
    frames = []
    for index in range(count):
        timestamp = base_ms + index
        rows = []
        for contract_index, contract in enumerate(CONTRACTS):
            sequence = index + 1
            price = 5.0 + contract_index
            rows.extend((
                {
                    "ev": "Q",
                    "sym": contract,
                    "bx": 301,
                    "ax": 302,
                    "bp": price - 0.1,
                    "ap": price,
                    "bs": 10,
                    "as": 12,
                    "t": timestamp - 1,
                    "q": sequence,
                },
                {
                    "ev": "T",
                    "sym": contract,
                    "x": 302 + (index + contract_index) % 3,
                    "p": price,
                    "s": 100,
                    "c": [209],
                    "t": timestamp,
                    "q": sequence,
                },
            ))
        frames.append(json.dumps(
            rows,
            separators=(",", ":"),
        ).encode("ascii"))
    return tuple(frames)


async def _gateway_to_journal(
    *,
    path,
    generation,
    stream_session_id,
    frames,
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
        BufferLimits(2048, 32 * 1024 * 1024, 60_000_000_000),
        monotonic_ns=time.monotonic_ns,
    )
    admissions = {
        contract: InstrumentAdmission(
            instrument_id,
            contract,
            "SPY",
            AssetClass.OPTION,
            0,
        )
        for contract, instrument_id in zip(CONTRACTS, INSTRUMENT_IDS)
    }
    pump = AdvancedOptionsJournalPump(
        buffer,
        path,
        generation,
        admissions,
        maximum_batch_messages=250,
        maximum_batch_bytes=4 * 1024 * 1024,
    )
    pump.start()
    started = time.perf_counter()
    try:
        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            gateway = AdvancedGateway(
                GatewayConfiguration(
                    MarketFeed.OPTIONS,
                    f"ws://127.0.0.1:{port}",
                    maximum_message_bytes=512 * 1024,
                    library_max_queue=8,
                    allow_insecure_loopback=True,
                ),
                _manifest(),
                buffer,
                api_key="provider-free-p6e",
                utc_clock=lambda: START,
                monotonic_ns=time.monotonic_ns,
                session_id_factory=lambda: stream_session_id,
            )
            result = await gateway.run_session(
                maximum_market_messages=len(frames),
            )
            gateway_seconds = time.perf_counter() - started
    finally:
        pump.request_stop()
    drain_started = time.perf_counter()
    progress = pump.join(30)
    drain_seconds = time.perf_counter() - drain_started
    return (
        result,
        progress,
        buffer.snapshot(),
        tuple(controls),
        tuple(subscriptions),
        gateway_seconds,
        drain_seconds,
    )


def _seed_scope(pool, *, generation, stream_session_id):
    now = datetime.now(UTC)
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cohort_id = insert_cohort(cursor, START)
            plan_id = uuid4()
            cursor.execute(
                "INSERT INTO rt.service_plans ("
                "plan_id,cohort_id,session_date,stage_catalog_version,"
                "enabled_stages,config_sha256,deadline,scope_reference"
                ") VALUES (%s,%s,%s,'stages_v1',%s,%s,%s,%s)",
                (
                    str(plan_id),
                    str(cohort_id),
                    SESSION,
                    sorted(STAGES_V1),
                    uuid4().hex + uuid4().hex,
                    now + timedelta(hours=8),
                    "p6e-provider-free-wire",
                ),
            )
            cursor.execute(
                "SELECT manifest_id,manifest_sha256 "
                "FROM rt.cohorts JOIN rt.subscription_manifests "
                "USING(manifest_id) WHERE cohort_id=%s",
                (str(cohort_id),),
            )
            manifest_id, manifest_sha256 = cursor.fetchone()
            manifest_id = UUID(str(manifest_id))
            security_id = uuid4()
            cursor.execute(
                "INSERT INTO ref.securities "
                "(security_id,asset_type,source_origin,created_at) "
                "VALUES (%s,'ETF','p6e-provider-free',%s)",
                (str(security_id), START),
            )
            for contract_id, version_id, instrument_id, contract in zip(
                CONTRACT_IDS,
                VERSION_IDS,
                INSTRUMENT_IDS,
                CONTRACTS,
            ):
                cursor.execute(
                    "INSERT INTO ref.option_contracts ("
                    "contract_id,underlying_security_id,provider,"
                    "contract_ticker,first_observed_at,source_origin) "
                    "VALUES (%s,%s,'massive',%s,%s,'p6e-provider-free')",
                    (
                        contract_id,
                        str(security_id),
                        contract,
                        START,
                    ),
                )
                cursor.execute(
                    "INSERT INTO ref.option_contract_versions ("
                    "version_id,contract_id,call_put,expiration,strike,"
                    "exercise_style,multiplier,eligibility,effective_from,"
                    "observed_at,payload_sha256) VALUES "
                    "(%s,%s,%s,%s,%s,'AMERICAN',100,'VALIDATED_ACTIVE',"
                    "%s,%s,%s)",
                    (
                        version_id,
                        contract_id,
                        "C" if "C005" in contract else "P",
                        date(2026, 11, 20),
                        Decimal("500" if "C005" in contract else "450"),
                        START - timedelta(days=1),
                        START - timedelta(days=1),
                        uuid4().hex + uuid4().hex,
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.instruments "
                    "(instrument_id,asset_class,contract_id) "
                    "VALUES (%s,'OPTION',%s)",
                    (instrument_id, contract_id),
                )
            cursor.execute(
                "INSERT INTO rt.stream_sessions ("
                "session_id,provider,feed,manifest_id,owner_id,owner_epoch,"
                "started_at,ended_at,status,journal_generation,"
                "normalizer_version,authorization_reference,"
                "budget_scope_sha256,connection_attempt,connection_ceiling,"
                "decoded_byte_ceiling,received_frame_count,"
                "received_byte_count,admitted_market_message_count,"
                "admitted_market_byte_count,last_raw_offset,last_event_offset,"
                "event_time_watermark,heartbeat_at) VALUES "
                "(%s,'p6e-loopback','OPTIONS',%s,%s,1,%s,%s,'STOPPED',"
                "%s,%s,'p6e-provider-free',%s,1,1,100000000,0,0,0,0,0,0,"
                "%s,%s)",
                (
                    str(stream_session_id),
                    str(manifest_id),
                    str(uuid4()),
                    START,
                    START + timedelta(minutes=1),
                    str(generation),
                    NORMALIZER_VERSION,
                    "3" * 64,
                    START,
                    START,
                ),
            )
        connection.commit()
    finally:
        pool.putconn(connection)
    return cohort_id, plan_id, manifest_id, str(manifest_sha256)


def _flow_identity(
    cohort_id,
    generation,
    stream_session_id,
    manifest_sha256,
):
    semantics = load_default_live_semantics()
    detector_policy = policy(
        sweep_window_seconds=5,
        minimum_sweep_prints=3,
    )
    advanced = advanced_policy()
    scope = advanced_contract_scope(
        CONTRACTS,
        session_date=SESSION,
        flow_policy=advanced,
    )
    scope = replace(scope, manifest_sha256=manifest_sha256)
    exchange = load_default_options_exchange_reference()
    base = EngineConfig(
        cohort_id=cohort_id,
        provider_tier="MASSIVE_ADVANCED",
        policy=detector_policy,
        semantics=semantics,
        journal_generation=generation,
        shard_count=DEFAULT_SHARD_COUNT,
        queue_capacity=100_000,
        advanced_flow_policy=advanced,
        contract_scope=scope,
        exchange_reference=exchange,
        volume=VolumeConfig(
            initial_baseline=BaselineKind.SESSION_COMPLETE,
        ),
    )
    source_flow = build_option_source_flow_identity(
        journal_generation=generation,
        stream_session_ids=(stream_session_id,),
        condition_semantics_sha256=semantics.semantics_sha256,
        flow_policy_sha256=base.policy_sha256(),
        advanced_flow_policy_sha256=advanced.sha256,
        contract_scope_sha256=scope.sha256,
        provider_contract_sha256=MASSIVE_OPTIONS_PROVIDER_CONTRACT.sha256,
        condition_reference_sha256=semantics.pin.payload_sha256,
        exchange_reference_sha256=exchange.sha256,
    )
    settings = OptionsFlowSettings(
        shard_count=DEFAULT_SHARD_COUNT,
        queue_capacity=100_000,
        history_per_contract=512,
        initial_baseline=BaselineKind.SESSION_COMPLETE,
        limits=ConsumerLimits(
            batch_size=250,
            max_messages=100_000,
            max_seconds=30,
        ),
        checkpoint_interval_seconds=10,
        checkpoint_interval_ms=5_000,
        advanced_flow_policy_sha256=advanced.sha256,
        contract_scope_sha256=scope.sha256,
        exchange_reference_sha256=exchange.sha256,
    )
    return semantics, detector_policy, advanced, scope, exchange, source_flow, settings


def _seed_flow_references(pool, semantics, advanced, scope):
    connection = pool.getconn()
    try:
        write_condition_reference(connection, semantics)
        with connection.cursor() as cursor:
            persist_option_flow_policy_cursor(cursor, advanced)
        connection.commit()
    finally:
        pool.putconn(connection)


def _persist_market_state(
    pool,
    provider,
    source,
    cohort_id,
    generation,
):
    shard_map = MaterializerShardMap(
        "p6e-options-market-state-v1",
        1,
        tuple((instrument_id, 0) for instrument_id in INSTRUMENT_IDS),
    )
    owner_id = uuid4()
    resource_key = (
        f"{CANONICAL_MARKET_STATE_OWNER}:{cohort_id}:{generation}:0"
    )
    _execute(
        pool,
        "INSERT INTO rt.worker_leases "
        "(resource_key,owner_id,owner_epoch,lease_until) "
        "VALUES (%s,%s,1,%s)",
        (
            resource_key,
            str(owner_id),
            datetime.now(UTC) + timedelta(minutes=10),
        ),
    )
    consumer = AdvancedOptionsMarketStateConsumer(
        source=source,
        cohort_id=cohort_id,
        shard_map=shard_map,
        owner_epoch=1,
        clock=lambda: datetime.now(UTC),
    )
    planned = consumer.plan_slice(maximum_records=10_000)
    with provider.transaction() as unit:
        persist_advanced_options_market_state_slice_cursor(
            unit.cursor,
            cohort_id=cohort_id,
            journal_generation=generation,
            planned=planned,
            owner_id=owner_id,
            batch_executor=execute_batch,
        )
    consumer.commit_slice(planned)
    return planned


def test_real_gateway_journal_consumers_writer_and_restart(
    database,
    tmp_path,
    capsys,
):
    pool, roles, created_roles, security_audit = database
    assert created_roles
    assert all(state.startswith("PASS") for _, state in security_audit)
    generation = uuid4()
    stream_session_id = uuid4()
    cohort_id, plan_id, manifest_id, manifest_sha256 = _seed_scope(
        pool,
        generation=generation,
        stream_session_id=stream_session_id,
    )
    (
        semantics,
        detector_policy,
        advanced,
        scope,
        exchange,
        source_flow,
        settings,
    ) = _flow_identity(
        cohort_id,
        generation,
        stream_session_id,
        manifest_sha256,
    )
    _seed_flow_references(pool, semantics, advanced, scope)
    frames = _frames()
    journal_root = tmp_path / "journal"
    journal_path = journal_root / "options.sqlite"
    (
        result,
        progress,
        buffer_snapshot,
        controls,
        subscriptions,
        gateway_seconds,
        drain_seconds,
    ) = asyncio.run(_gateway_to_journal(
        path=journal_path,
        generation=generation,
        stream_session_id=stream_session_id,
        frames=frames,
    ))
    assert result.final_state is GatewayFeedState.STOPPED
    assert result.accepted_messages == len(frames)
    assert result.library_max_queue == 8
    assert (
        result.kernel_receive_buffer_bytes is not None
        and result.kernel_receive_buffer_bytes > 0
    )
    assert progress.admitted_market_messages == len(frames)
    assert progress.last_raw_offset == len(frames)
    assert progress.last_event_offset == len(frames) * 4
    reader = RawJournalReader(journal_path)
    raw_records = reader.read_after(0, limit=10_000)
    normalized_records = reader.read_normalized_after(0, limit=10_000)
    assert tuple(row.raw_offset for row in raw_records) == tuple(
        range(1, len(frames) + 1)
    )
    assert len(normalized_records) == len(frames) * 4
    assert {
        row.raw_offset for row in normalized_records
    } == set(range(1, len(frames) + 1))
    assert buffer_snapshot.admitted_events == 0
    assert 0 < buffer_snapshot.maximum_admitted_events <= 2048
    assert 0 < buffer_snapshot.maximum_payload_bytes <= 32 * 1024 * 1024
    assert buffer_snapshot.full_count == 0
    assert controls[0] == {
        "action": "auth",
        "params": "provider-free-p6e",
    }
    assert set(subscriptions) == {
        f"{channel.value}.{contract}"
        for contract in CONTRACTS
        for channel in (MarketChannel.TRADE, MarketChannel.QUOTE)
    }
    assert not any("*" in value for value in subscriptions)
    registration = build_advanced_journal_registration(
        root=journal_root,
        path=journal_path,
        journal_generation=generation,
        plan_id=plan_id,
        stream_session_id=stream_session_id,
        cohort_id=cohort_id,
        manifest_id=manifest_id,
        session_date=SESSION,
        owner_epoch=1,
        normalizer_version=NORMALIZER_VERSION,
        shard_ids=(0,),
        shard_map_sha256=MaterializerShardMap(
            "p6e-options-market-state-v1",
            1,
            tuple((instrument_id, 0) for instrument_id in INSTRUMENT_IDS),
        ).sha256,
    )
    registration = replace(
        registration,
        status="DRAINED",
        raw_committed_offset=progress.last_raw_offset,
        normalized_committed_offset=progress.last_event_offset,
        bridge_committed_offset=progress.last_event_offset,
    )
    source = AdvancedOptionsJournalSource(
        root=journal_root,
        registration=registration,
        source_flow=source_flow,
    )
    market = _persist_market_state(
        pool,
        _app_provider(pool, roles),
        source,
        cohort_id,
        generation,
    )
    started = time.perf_counter()
    environment = dict(
        options_flow_semantics=semantics,
        options_flow_policy=detector_policy,
        options_flow_settings=settings,
        options_flow_advanced_policy=advanced,
        options_flow_contract_scope=scope,
        options_flow_exchange_reference=exchange,
        options_flow_oi_loader=lambda _context, _session: {},
        options_flow_clock=time.monotonic,
    )
    world = World(
        pool,
        source,
        provider=_app_provider(pool, roles),
        cohort=cohort_id,
        plan_id=plan_id,
        base_ms=int(START.timestamp() * 1000),
        **environment,
    )
    consumer = world.run_to_end()
    database_seconds = time.perf_counter() - started
    rows = world.rows()
    checkpoints = world.checkpoint_rows()
    assert rows["events"]
    assert rows["clusters"], {
        "side_classes": sorted({row[14] for row in rows["events"]}),
        "side_confidences": sorted({row[15] for row in rows["events"]}),
        "treatments": sorted({row[12] for row in rows["events"]}),
        "event_count": len(rows["events"]),
        "stats": repr(consumer.engine.stats),
    }
    assert len(rows["volume"]) == len(CONTRACTS)
    assert len(checkpoints) == DEFAULT_SHARD_COUNT
    assert {row[1] for row in checkpoints} == {len(frames)}
    assert all(0 < row[2] <= 1_048_576 for row in checkpoints)
    assert market.end_offset == progress.last_event_offset
    assert consumer.fed_offset == len(frames)
    assert consumer.backlog() == 0
    assert consumer.writer.pending_rows == 0
    assert consumer.writer.maximum_flush_seconds > 0
    assert _query(
        pool,
        "SELECT count(*) FROM rt.quote_current "
        "WHERE instrument_id=ANY(%s)",
        (list(INSTRUMENT_IDS),),
    )[0][0] == len(CONTRACTS)
    assert _query(
        pool,
        "SELECT count(*) FROM rt.trade_current "
        "WHERE instrument_id=ANY(%s)",
        (list(INSTRUMENT_IDS),),
    )[0][0] == len(CONTRACTS)
    before = rows
    restarted = World(
        pool,
        source,
        provider=_app_provider(pool, roles),
        cohort=cohort_id,
        plan_id=plan_id,
        base_ms=int(START.timestamp() * 1000),
        options_flow_semantics=semantics,
        options_flow_policy=detector_policy,
        options_flow_settings=settings,
        options_flow_advanced_policy=advanced,
        options_flow_contract_scope=scope,
        options_flow_exchange_reference=exchange,
        options_flow_oi_loader=lambda _context, _session: {},
    )
    restarted.run_to_end()
    assert restarted.session().start.mode == "RESTORED"
    assert restarted.rows() == before
    with capsys.disabled():
        print(
            "\nP6E_PROVIDER_FREE single-owner wire: "
            f"{len(frames):,} frames/{len(frames) * 4:,} messages, "
            f"gateway+journal {gateway_seconds:.3f}s, "
            f"drain={drain_seconds:.3f}s, "
            f"consumers+PostgreSQL {database_seconds:.3f}s, "
            f"ws_queue={result.library_max_queue}, "
            f"kernel_rcvbuf={result.kernel_receive_buffer_bytes}, "
            f"buffer_max_events={buffer_snapshot.maximum_admitted_events}, "
            f"buffer_max_bytes={buffer_snapshot.maximum_payload_bytes}, "
            f"buffer_max_age_ms="
            f"{buffer_snapshot.maximum_oldest_age_ns / 1_000_000:.3f}, "
            f"max_pg_flush_ms="
            f"{consumer.writer.maximum_flush_seconds * 1000:.3f}, "
            f"events={len(rows['events']):,}, "
            f"clusters={len(rows['clusters']):,}, "
            f"checkpoints={len(checkpoints)}"
        )
