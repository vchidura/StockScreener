"""Bounded, manifest-gated executor for an Advanced Options P7 live shadow."""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
import shutil
import sys
import time
from collections import Counter
from contextlib import closing, suppress
from dataclasses import asdict, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Callable, Mapping
from uuid import UUID, uuid4

from psycopg2.extras import execute_batch

from .advanced_gateway import AdvancedGateway, GatewayConfiguration
from .advanced_journal_registry import build_advanced_journal_registration
from .advanced_options_consumers import (
    AdvancedOptionsMarketStateConsumer,
    persist_advanced_options_market_state_slice_cursor,
)
from .advanced_options_control_plane import CANONICAL_MARKET_STATE_OWNER
from .advanced_options_ingress import (
    AdvancedOptionsJournalPump,
    AdvancedOptionsJournalSource,
)
from .advanced_options_provider_contract import (
    MASSIVE_OPTIONS_PROVIDER_CONTRACT,
    load_default_options_exchange_reference,
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
from .database_access_v2 import APP_ROLE, V2AccessConfig, V2ConnectionProvider
from .journal import RawJournalReader
from .materialization import MaterializerShardMap
from .normalizer import InstrumentAdmission, NORMALIZER_VERSION
from .option_flow.conditions import load_default_live_semantics
from .option_flow.consumer import ConsumerLimits
from .option_flow.engine import DEFAULT_SHARD_COUNT, EngineConfig
from .option_flow.volume import BaselineKind, VolumeConfig
from .option_flow.writer import write_condition_reference
from .option_source_flow import (
    build_option_source_flow_identity,
    persist_option_flow_policy_cursor,
)
from .security_v2 import (
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from .stage_graph import STAGES_V1
from .stage_runners_options import OptionsFlowSettings


UTC = timezone.utc
_OPTION_PATTERN = re.compile(
    r"^O:(?P<underlyer>[A-Z][A-Z0-9.-]{0,14})"
    r"(?P<expiration>\d{6})(?P<call_put>[CP])(?P<strike>\d{8})$"
)
_INSTRUMENT_IDS = (1001, 1002)
_CONTRACT_IDS = (101, 102)
_VERSION_IDS = (201, 202)


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("P7 timestamps must be timezone-aware")
    return parsed.astimezone(UTC)


def _write_json(path: Path, value: object) -> None:
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, indent=2, sort_keys=True, default=str)
        handle.write("\n")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(
                json.dumps(row, sort_keys=True, default=str) + "\n"
            )


def _option_details(ticker: str) -> tuple[str, date, str, Decimal]:
    match = _OPTION_PATTERN.fullmatch(ticker)
    if match is None:
        raise ValueError("P7 contract ticker is not canonical")
    expiration = datetime.strptime(
        match.group("expiration"),
        "%y%m%d",
    ).date()
    strike = Decimal(match.group("strike")) / Decimal(1000)
    return (
        match.group("underlyer"),
        expiration,
        match.group("call_put"),
        strike,
    )


def _subscription_manifest(plan: Mapping[str, object]) -> SubscriptionManifest:
    scope = plan["scope"]
    contracts = tuple(scope["contracts"])
    underlyings = tuple(scope["underlyings"])
    members = tuple(
        SubscriptionMember(
            AssetClass.OPTION,
            contract,
            underlyer,
            channel,
            catalog_version_id=version_id,
        )
        for contract, underlyer, version_id in zip(
            contracts,
            underlyings,
            _VERSION_IDS,
        )
        for channel in (MarketChannel.TRADE, MarketChannel.QUOTE)
    )
    return SubscriptionManifest(
        version=f"advanced-options-p7-{plan['run_id']}",
        session_date=date.fromisoformat(plan["window"]["session_date"]),
        universe=UniverseManifest(
            f"advanced-options-p7-{plan['run_id']}",
            underlyings,
            underlyings,
        ),
        capability_profile_sha256=plan["plan_sha256"],
        allocation_policy_sha256=canonical_sha256(scope),
        members=members,
        maximum_option_quote_contracts=len(contracts),
    )


def _admissions(plan: Mapping[str, object]) -> dict[str, InstrumentAdmission]:
    return {
        contract: InstrumentAdmission(
            instrument_id,
            contract,
            underlyer,
            AssetClass.OPTION,
            0,
        )
        for contract, underlyer, instrument_id in zip(
            plan["scope"]["contracts"],
            plan["scope"]["underlyings"],
            _INSTRUMENT_IDS,
        )
    }


async def _cancel(task: asyncio.Task) -> None:
    if task.done():
        return
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


async def _run_attempt(
    gateway: AdvancedGateway,
    pump: AdvancedOptionsJournalPump,
    buffer: RawMessageBuffer,
    *,
    deadline: datetime,
    maximum_market_messages: int,
    maximum_received_bytes: int,
    initial_message_number: int,
    plan: Mapping[str, object],
) -> dict[str, object]:
    started = datetime.now(UTC)
    start_offset = pump.progress.last_raw_offset
    task = asyncio.create_task(
        gateway.run_session(
            maximum_market_messages=maximum_market_messages,
            maximum_received_bytes=maximum_received_bytes,
            initial_message_number=initial_message_number,
        )
    )
    termination = "UNKNOWN"
    error_name = None
    result = None
    try:
        while not task.done():
            now = datetime.now(UTC)
            progress = pump.progress
            snapshot = buffer.snapshot()
            if now >= deadline:
                termination = "DEADLINE"
                await _cancel(task)
                break
            if (
                progress.last_event_offset
                > plan["socket_ceiling"]["decoded_market_messages"]
                or snapshot.maximum_admitted_events
                > plan["application_bounds"]["abort_buffer_events"]
                or snapshot.maximum_payload_bytes
                > plan["application_bounds"]["abort_buffer_bytes"]
                or snapshot.maximum_oldest_age_ns
                > plan["application_bounds"]["abort_oldest_age_seconds"]
                * 1_000_000_000
                or snapshot.full_count
            ):
                termination = "APPLICATION_ABORT_GATE"
                await _cancel(task)
                break
            await asyncio.sleep(
                min(0.05, max(0.001, (deadline - now).total_seconds()))
            )
        if task.done() and not task.cancelled():
            try:
                result = task.result()
                termination = "MESSAGE_CEILING"
            except Exception as error:
                termination = "PROVIDER_DISCONNECT"
                error_name = type(error).__name__
    finally:
        await _cancel(task)
    ended = datetime.now(UTC)
    end_offset = pump.progress.last_raw_offset
    return {
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "stream_session_id": str(
            result.stream_session_id
            if result is not None
            else gateway.active_stream_session_id
        ),
        "termination": termination,
        "error_name": error_name,
        "received_frames": gateway.received_messages,
        "received_bytes": gateway.received_bytes,
        "accepted_frames": end_offset - start_offset,
        "raw_offset_start": start_offset,
        "raw_offset_end": end_offset,
        "subscriptions": list(gateway.subscriptions),
        "transitions": [state.value for state in gateway.transitions],
        "kernel_receive_buffer_bytes": (
            gateway.kernel_receive_buffer_bytes
        ),
    }


async def capture_live_shadow(
    plan: Mapping[str, object],
    api_key: str,
) -> dict[str, object]:
    if not api_key.strip():
        raise ValueError("P7 live shadow requires the provider API key")
    evidence_directory = Path(plan["evidence"]["directory"])
    temporary = Path(plan["cleanup"]["temporary_directory"])
    journal_path = Path(plan["cleanup"]["journal_files"][0])
    temporary.mkdir(parents=True, exist_ok=False)
    generation = uuid4()
    buffer = RawMessageBuffer(
        BufferLimits(
            plan["application_bounds"]["buffer_events"],
            plan["application_bounds"]["buffer_bytes"],
            plan["application_bounds"]["buffer_maximum_age_seconds"]
            * 1_000_000_000,
        ),
        monotonic_ns=time.monotonic_ns,
    )
    telemetry: list[dict[str, object]] = []

    def observe(progress) -> None:
        snapshot = buffer.snapshot()
        telemetry.append(
            {
                "observed_at": datetime.now(UTC).isoformat(),
                "last_raw_offset": progress.last_raw_offset,
                "last_event_offset": progress.last_event_offset,
                "admitted_market_bytes": progress.admitted_market_bytes,
                "queued_events": snapshot.queued_events,
                "buffer_payload_bytes": snapshot.payload_bytes,
                "oldest_age_ns": snapshot.oldest_age_ns,
            }
        )

    pump = AdvancedOptionsJournalPump(
        buffer,
        journal_path,
        generation,
        _admissions(plan),
        maximum_batch_messages=plan["application_bounds"][
            "journal_batch_messages"
        ],
        maximum_batch_bytes=plan["application_bounds"][
            "journal_batch_bytes"
        ],
        on_progress=observe,
    )
    stream_session_id = uuid4()
    manifest = _subscription_manifest(plan)
    outbound_control_bytes = 2 * (
        len(
            json.dumps(
                {"action": "auth", "params": api_key},
                separators=(",", ":"),
            ).encode("utf-8")
        )
        + len(
            json.dumps(
                {
                    "action": "subscribe",
                    "params": ",".join(
                        f"{member.channel.value}.{member.provider_symbol}"
                        for member in manifest.members
                    ),
                },
                separators=(",", ":"),
            ).encode("utf-8")
        )
    )
    if outbound_control_bytes > plan["socket_ceiling"]["outbound_bytes"]:
        raise ValueError("P7 outbound control byte ceiling is too small")
    attempts: list[dict[str, object]] = []
    total_received_bytes = 0
    total_received_frames = 0
    next_message_number = 0
    reconnect_at = _timestamp(plan["window"]["controlled_reconnect_utc"])
    stop_at = _timestamp(plan["window"]["stop_utc"])
    frame_ceiling = plan["socket_ceiling"]["websocket_frames"]
    byte_ceiling = plan["socket_ceiling"]["inbound_decoded_bytes"]
    maximum_frame = plan["socket_ceiling"]["maximum_frame_bytes"]
    pump.start()
    progress = None
    try:
        for attempt_number, deadline in enumerate((reconnect_at, stop_at), 1):
            remaining_frames = frame_ceiling - total_received_frames
            remaining_bytes = (
                byte_ceiling - total_received_bytes - maximum_frame
            )
            if remaining_frames <= 0 or remaining_bytes <= 0:
                break
            gateway = AdvancedGateway(
                GatewayConfiguration(
                    MarketFeed.OPTIONS,
                    GatewayConfiguration.options().endpoint,
                    open_timeout_seconds=plan["socket_ceiling"][
                        "open_timeout_seconds"
                    ],
                    ping_interval_seconds=plan["socket_ceiling"][
                        "ping_interval_seconds"
                    ],
                    ping_timeout_seconds=plan["socket_ceiling"][
                        "ping_timeout_seconds"
                    ],
                    close_timeout_seconds=plan["socket_ceiling"][
                        "close_timeout_seconds"
                    ],
                    maximum_message_bytes=maximum_frame,
                    library_max_queue=plan["socket_ceiling"][
                        "library_max_queue"
                    ],
                ),
                manifest,
                buffer,
                api_key=api_key,
                monotonic_ns=time.monotonic_ns,
                session_id_factory=lambda: stream_session_id,
            )
            observation = await _run_attempt(
                gateway,
                pump,
                buffer,
                deadline=deadline,
                maximum_market_messages=remaining_frames,
                maximum_received_bytes=remaining_bytes,
                initial_message_number=next_message_number,
                plan=plan,
            )
            next_message_number = gateway.next_message_number
            observation["attempt"] = attempt_number
            attempts.append(observation)
            total_received_frames += int(observation["received_frames"])
            total_received_bytes += int(observation["received_bytes"])
            if observation["termination"] in {
                "MESSAGE_CEILING",
                "APPLICATION_ABORT_GATE",
            }:
                break
    finally:
        pump.request_stop()
        progress = pump.join(
            plan["application_bounds"]["final_drain_seconds"]
        )
    reader = RawJournalReader(journal_path)
    raw = reader.read_after(0, limit=frame_ceiling + 1)
    normalized = reader.read_normalized_after(
        0,
        limit=plan["socket_ceiling"]["decoded_market_messages"] + 1,
    )
    snapshot = buffer.snapshot()
    event_counts = Counter(row.event_kind.value for row in normalized)
    condition_counts = Counter(
        condition for row in normalized for condition in row.conditions
    )
    exchange_counts = Counter(
        row.exchange for row in normalized if row.exchange is not None
    )
    symbol_event_counts = Counter(
        (row.provider_symbol, row.event_kind.value) for row in normalized
    )
    status = "PASS_CAPTURE"
    blockers = []
    if len(attempts) > plan["socket_ceiling"]["connection_attempts"]:
        blockers.append("CONNECTION_ATTEMPT_CEILING_EXCEEDED")
    if len(attempts) != plan["socket_ceiling"]["connection_attempts"]:
        blockers.append("RECONNECT_WINDOW_NOT_EXERCISED")
    if total_received_bytes > byte_ceiling:
        blockers.append("RECEIVED_BYTE_CEILING_EXCEEDED")
    if len(raw) > frame_ceiling:
        blockers.append("FRAME_CEILING_EXCEEDED")
    if len(normalized) > plan["socket_ceiling"]["decoded_market_messages"]:
        blockers.append("DECODED_MESSAGE_CEILING_EXCEEDED")
    if (
        progress.last_raw_offset != len(raw)
        or progress.last_event_offset != len(normalized)
        or snapshot.admitted_events
        or snapshot.full_count
    ):
        blockers.append("LOSS_ACCOUNTING_MISMATCH")
    if any(
        attempt["termination"] == "APPLICATION_ABORT_GATE"
        for attempt in attempts
    ):
        blockers.append("APPLICATION_ABORT_GATE")
    if any(
        attempt["termination"] == "MESSAGE_CEILING"
        for attempt in attempts
    ):
        blockers.append("MESSAGE_CEILING_REACHED")
    if attempts and attempts[-1]["termination"] == "PROVIDER_DISCONNECT":
        blockers.append("FINAL_PROVIDER_DISCONNECT")
    if not raw:
        blockers.append("NO_MARKET_FRAMES")
    required_membership = {
        (contract, channel)
        for contract in plan["scope"]["contracts"]
        for channel in ("T", "Q")
    }
    if not required_membership.issubset(symbol_event_counts):
        blockers.append("INCOMPLETE_TQ_CONTRACT_COVERAGE")
    if blockers:
        status = "FAIL_CAPTURE"
    return {
        "status": status,
        "blockers": blockers,
        "journal_generation": str(generation),
        "journal_root": str(temporary),
        "journal_path": str(journal_path),
        "session_ids": [str(stream_session_id)],
        "sessions": attempts,
        "received_frames": total_received_frames,
        "received_bytes": total_received_bytes,
        "outbound_control_bytes": outbound_control_bytes,
        "raw_frames": len(raw),
        "normalized_messages": len(normalized),
        "event_counts": dict(sorted(event_counts.items())),
        "condition_counts": {
            str(key): value for key, value in sorted(condition_counts.items())
        },
        "exchange_counts": {
            str(key): value for key, value in sorted(exchange_counts.items())
        },
        "symbol_event_counts": {
            f"{symbol}|{event}": value
            for (symbol, event), value in sorted(symbol_event_counts.items())
        },
        "progress": asdict(progress),
        "buffer": asdict(snapshot),
        "telemetry": telemetry,
        "evidence_directory": str(evidence_directory),
    }


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


def _seed_scope(
    pool,
    plan: Mapping[str, object],
    capture: Mapping[str, object],
):
    test_support = Path(__file__).resolve().parents[1] / "tests" / "realtime"
    if str(test_support) not in sys.path:
        sys.path.insert(0, str(test_support))
    from v2_database import insert_cohort

    start = _timestamp(plan["window"]["start_utc"])
    session_date = date.fromisoformat(plan["window"]["session_date"])
    generation = UUID(capture["journal_generation"])
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cohort_id = insert_cohort(cursor, start)
            plan_id = uuid4()
            cursor.execute(
                "INSERT INTO rt.service_plans ("
                "plan_id,cohort_id,session_date,stage_catalog_version,"
                "enabled_stages,config_sha256,deadline,scope_reference"
                ") VALUES (%s,%s,%s,'stages_v1',%s,%s,%s,%s)",
                (
                    str(plan_id),
                    str(cohort_id),
                    session_date,
                    sorted(STAGES_V1),
                    plan["plan_sha256"],
                    _timestamp(plan["window"]["stop_utc"])
                    + timedelta(minutes=30),
                    plan["authorization_reference"],
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
            securities = {}
            for underlyer in plan["scope"]["underlyings"]:
                security_id = uuid4()
                securities[underlyer] = security_id
                cursor.execute(
                    "INSERT INTO ref.securities "
                    "(security_id,asset_type,source_origin,created_at) "
                    "VALUES (%s,'ETF','p7-live-shadow',%s)",
                    (str(security_id), start),
                )
            for contract_id, version_id, instrument_id, contract in zip(
                _CONTRACT_IDS,
                _VERSION_IDS,
                _INSTRUMENT_IDS,
                plan["scope"]["contracts"],
            ):
                underlyer, expiration, call_put, strike = _option_details(
                    contract
                )
                cursor.execute(
                    "INSERT INTO ref.option_contracts ("
                    "contract_id,underlying_security_id,provider,"
                    "contract_ticker,first_observed_at,source_origin) "
                    "VALUES (%s,%s,'massive',%s,%s,'p7-live-shadow')",
                    (
                        contract_id,
                        str(securities[underlyer]),
                        contract,
                        start,
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
                        call_put,
                        expiration,
                        strike,
                        start - timedelta(days=1),
                        start - timedelta(days=1),
                        canonical_sha256(
                            {
                                "contract": contract,
                                "run_id": plan["run_id"],
                            }
                        ),
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.instruments "
                    "(instrument_id,asset_class,contract_id) "
                    "VALUES (%s,'OPTION',%s)",
                    (instrument_id, contract_id),
                )
            attempts = capture["sessions"]
            cursor.execute(
                "INSERT INTO rt.stream_sessions ("
                "session_id,provider,feed,manifest_id,owner_id,owner_epoch,"
                "started_at,ended_at,status,journal_generation,"
                "normalizer_version,authorization_reference,"
                "budget_scope_sha256,connection_attempt,"
                "connection_ceiling,decoded_byte_ceiling,"
                "received_frame_count,received_byte_count,"
                "admitted_market_message_count,"
                "admitted_market_byte_count,last_raw_offset,"
                "last_event_offset,event_time_watermark,heartbeat_at"
                ") VALUES ("
                "%s,'massive','OPTIONS',%s,%s,1,%s,%s,'STOPPED',"
                "%s,%s,%s,%s,%s,2,33554432,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    capture["session_ids"][0],
                    str(manifest_id),
                    str(uuid4()),
                    datetime.fromisoformat(attempts[0]["started_at"]),
                    datetime.fromisoformat(attempts[-1]["ended_at"]),
                    str(generation),
                    NORMALIZER_VERSION,
                    plan["authorization_reference"],
                    plan["plan_sha256"],
                    len(attempts),
                    sum(row["received_frames"] for row in attempts),
                    sum(row["received_bytes"] for row in attempts),
                    sum(row["accepted_frames"] for row in attempts),
                    0,
                    capture["progress"]["last_raw_offset"],
                    capture["progress"]["last_event_offset"],
                    start,
                    datetime.fromisoformat(attempts[-1]["ended_at"]),
                ),
            )
        connection.commit()
    finally:
        pool.putconn(connection)
    return cohort_id, plan_id, manifest_id, str(manifest_sha256)


def _flow_identity(
    plan,
    cohort_id,
    generation,
    stream_session_ids,
    manifest_sha256,
):
    test_support = Path(__file__).resolve().parents[1] / "tests" / "realtime"
    if str(test_support) not in sys.path:
        sys.path.insert(0, str(test_support))
    from option_flow_support import advanced_contract_scope, advanced_policy, policy

    semantics = load_default_live_semantics()
    detector_policy = policy(
        sweep_window_seconds=5,
        minimum_sweep_prints=3,
    )
    advanced = advanced_policy()
    scope = advanced_contract_scope(
        tuple(plan["scope"]["contracts"]),
        session_date=date.fromisoformat(plan["window"]["session_date"]),
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
        stream_session_ids=tuple(stream_session_ids),
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
    return (
        semantics,
        detector_policy,
        advanced,
        scope,
        exchange,
        source_flow,
        settings,
    )


def _persist_market_state(
    pool,
    provider,
    source,
    cohort_id,
    generation,
):
    test_support = Path(__file__).resolve().parents[1] / "tests" / "realtime"
    if str(test_support) not in sys.path:
        sys.path.insert(0, str(test_support))
    from test_option_flow_runner_v2_db import _execute

    shard_map = MaterializerShardMap(
        "p7-options-market-state-v1",
        1,
        tuple((instrument_id, 0) for instrument_id in _INSTRUMENT_IDS),
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
    planned = None
    while consumer.committed_offset < source.last_event_offset():
        next_slice = consumer.plan_slice(maximum_records=10_000)
        if next_slice.end_offset <= consumer.committed_offset:
            raise RuntimeError("P7 market-state consumer made no progress")
        with provider.transaction() as unit:
            persist_advanced_options_market_state_slice_cursor(
                unit.cursor,
                cohort_id=cohort_id,
                journal_generation=generation,
                planned=next_slice,
                owner_id=owner_id,
                batch_executor=execute_batch,
            )
        consumer.commit_slice(next_slice)
        planned = next_slice
    if planned is None:
        raise RuntimeError("P7 market-state consumer received no records")
    return planned, shard_map


def run_disposable_database_phase(
    plan: Mapping[str, object],
    capture: Mapping[str, object],
) -> dict[str, object]:
    test_support = Path(__file__).resolve().parents[1] / "tests" / "realtime"
    if str(test_support) not in sys.path:
        sys.path.insert(0, str(test_support))
    from test_option_flow_runner_v2_db import World, _query
    from v2_database import _connect, _settings, disposable_v2_database

    database = plan["database"]["name"]
    roles = build_role_names_v2(plan["database"]["role_prefix"])
    settings = _settings()
    report = None
    failure = None
    try:
        try:
            with disposable_v2_database(database) as pool:
                connection = pool.getconn()
                try:
                    created_roles, security_audit = harden_database_v2(
                        connection,
                        roles,
                    )
                finally:
                    pool.putconn(connection)
                if not all(
                    state.startswith("PASS") for _, state in security_audit
                ):
                    raise RuntimeError("P7 disposable security audit failed")
                cohort_id, plan_id, manifest_id, manifest_sha256 = (
                    _seed_scope(pool, plan, capture)
                )
                generation = UUID(capture["journal_generation"])
                stream_session_ids = tuple(
                    UUID(value) for value in capture["session_ids"]
                )
                (
                    semantics,
                    detector_policy,
                    advanced,
                    scope,
                    exchange,
                    source_flow,
                    settings_flow,
                ) = _flow_identity(
                    plan,
                    cohort_id,
                    generation,
                    stream_session_ids,
                    manifest_sha256,
                )
                connection = pool.getconn()
                try:
                    write_condition_reference(connection, semantics)
                    with connection.cursor() as cursor:
                        persist_option_flow_policy_cursor(cursor, advanced)
                    connection.commit()
                finally:
                    pool.putconn(connection)
                journal_root = Path(capture["journal_root"])
                journal_path = Path(capture["journal_path"])
                shard_map = MaterializerShardMap(
                    "p7-options-market-state-v1",
                    1,
                    tuple(
                        (instrument_id, 0)
                        for instrument_id in _INSTRUMENT_IDS
                    ),
                )
                registration = build_advanced_journal_registration(
                    root=journal_root,
                    path=journal_path,
                    journal_generation=generation,
                    plan_id=plan_id,
                    stream_session_id=stream_session_ids[0],
                    cohort_id=cohort_id,
                    manifest_id=manifest_id,
                    session_date=date.fromisoformat(
                        plan["window"]["session_date"]
                    ),
                    owner_epoch=1,
                    normalizer_version=NORMALIZER_VERSION,
                    shard_ids=(0,),
                    shard_map_sha256=shard_map.sha256,
                )
                registration = replace(
                    registration,
                    status="DRAINED",
                    raw_committed_offset=capture["progress"][
                        "last_raw_offset"
                    ],
                    normalized_committed_offset=capture["progress"][
                        "last_event_offset"
                    ],
                    bridge_committed_offset=capture["progress"][
                        "last_event_offset"
                    ],
                )
                source = AdvancedOptionsJournalSource(
                    root=journal_root,
                    registration=registration,
                    source_flow=source_flow,
                )
                provider = _app_provider(pool, roles)
                market, _ = _persist_market_state(
                    pool,
                    provider,
                    source,
                    cohort_id,
                    generation,
                )
                world = World(
                    pool,
                    source,
                    provider=provider,
                    cohort=cohort_id,
                    plan_id=plan_id,
                    base_ms=int(
                        _timestamp(
                            plan["window"]["start_utc"]
                        ).timestamp()
                        * 1000
                    ),
                    options_flow_semantics=semantics,
                    options_flow_policy=detector_policy,
                    options_flow_settings=settings_flow,
                    options_flow_advanced_policy=advanced,
                    options_flow_contract_scope=scope,
                    options_flow_exchange_reference=exchange,
                    options_flow_oi_loader=lambda _context, _session: {},
                    options_flow_clock=time.monotonic,
                )
                consumer = world.run_to_end()
                rows = world.rows(normalized=True)
                checkpoints = world.checkpoint_rows()
                restarted = World(
                    pool,
                    source,
                    provider=_app_provider(pool, roles),
                    cohort=cohort_id,
                    plan_id=plan_id,
                    base_ms=int(
                        _timestamp(
                            plan["window"]["start_utc"]
                        ).timestamp()
                        * 1000
                    ),
                    options_flow_semantics=semantics,
                    options_flow_policy=detector_policy,
                    options_flow_settings=settings_flow,
                    options_flow_advanced_policy=advanced,
                    options_flow_contract_scope=scope,
                    options_flow_exchange_reference=exchange,
                    options_flow_oi_loader=lambda _context, _session: {},
                )
                restarted.run_to_end()
                replay_identical = restarted.rows(normalized=True) == rows
                quote_current = _query(
                    pool,
                    "SELECT count(*) FROM rt.quote_current "
                    "WHERE instrument_id=ANY(%s)",
                    (list(_INSTRUMENT_IDS),),
                )[0][0]
                trade_current = _query(
                    pool,
                    "SELECT count(*) FROM rt.trade_current "
                    "WHERE instrument_id=ANY(%s)",
                    (list(_INSTRUMENT_IDS),),
                )[0][0]
                report = {
                    "status": "PASS_DATABASE",
                    "database": database,
                    "roles": list(roles.all_names),
                    "created_roles": list(created_roles),
                    "security_audit": [
                        list(row) for row in security_audit
                    ],
                    "market_state_end_offset": market.end_offset,
                    "consumer_fed_offset": consumer.fed_offset,
                    "consumer_backlog": consumer.backlog(),
                    "writer_pending_rows": consumer.writer.pending_rows,
                    "maximum_flush_seconds": (
                        consumer.writer.maximum_flush_seconds
                    ),
                    "events": len(rows["events"]),
                    "clusters": len(rows["clusters"]),
                    "volume_rows": len(rows["volume"]),
                    "checkpoint_rows": len(checkpoints),
                    "quote_current": quote_current,
                    "trade_current": trade_current,
                    "rows_sha256": hashlib.sha256(
                        json.dumps(
                            rows,
                            default=str,
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode("utf-8")
                    ).hexdigest(),
                    "replay_identical": replay_identical,
                }
                if (
                    market.end_offset
                    != capture["progress"]["last_event_offset"]
                    or consumer.backlog()
                    or consumer.writer.pending_rows
                    or not replay_identical
                    or consumer.writer.maximum_flush_seconds
                    > plan["application_bounds"][
                        "abort_postgresql_flush_seconds"
                    ]
                ):
                    report["status"] = "FAIL_DATABASE"
        except Exception as error:
            failure = error
    finally:
        with closing(_connect(settings, "postgres")) as connection:
            connection.autocommit = True
            drop_disposable_roles_v2(connection, roles)
        with closing(_connect(settings, "postgres")) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS (SELECT 1 FROM pg_database "
                    "WHERE datname=%s)",
                    (database,),
                )
                database_exists = bool(cursor.fetchone()[0])
                cursor.execute(
                    "SELECT rolname FROM pg_roles "
                    "WHERE rolname=ANY(%s) ORDER BY rolname",
                    (list(roles.all_names),),
                )
                remaining_roles = [
                    row[0] for row in cursor.fetchall()
                ]
    if failure is not None:
        raise failure
    if report is None:
        raise RuntimeError("P7 database phase produced no report")
    report["cleanup_verified"] = (
        not database_exists and not remaining_roles
    )
    report["database_absent"] = not database_exists
    report["roles_absent"] = not remaining_roles
    if not report["cleanup_verified"]:
        report["status"] = "FAIL_DATABASE"
    return report


def _no_effects() -> dict[str, object]:
    return {
        "provider_rest_requests": 0,
        "production_database_connections": 0,
        "publication_performed": False,
        "release_performed": False,
        "pointer_advanced": False,
        "outbox_dispatched": False,
        "alerts_delivered": False,
        "trades_executed": False,
        "repair_performed": False,
        "backfill_performed": False,
        "archive_retention_performed": False,
        "application_data_deleted": False,
    }


def execute_shadow(
    plan: Mapping[str, object],
    *,
    manifest_sha256: str,
    approval_sha256: str,
    api_key: str,
    now: datetime,
    capture: Callable[[Mapping[str, object], str], dict[str, object]]
    | None = None,
    database: Callable[
        [Mapping[str, object], Mapping[str, object]],
        dict[str, object],
    ]
    | None = None,
) -> dict[str, object]:
    start = _timestamp(plan["window"]["start_utc"])
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("P7 execution clock must be timezone-aware")
    current = now.astimezone(UTC)
    if not start <= current <= start + timedelta(seconds=15):
        raise RuntimeError("P7 live executor is outside the exact arm window")
    if not plan["live_execution_implemented"]:
        raise RuntimeError("P7 live execution is not implemented")
    evidence_directory = Path(plan["evidence"]["directory"])
    evidence_directory.parent.mkdir(parents=True, exist_ok=True)
    evidence_directory.mkdir(parents=False, exist_ok=False)
    capture_runner = capture or (
        lambda scope, secret: asyncio.run(capture_live_shadow(scope, secret))
    )
    database_runner = database or run_disposable_database_phase
    capture_report = None
    database_report = None
    error = None
    cleanup_report = {}
    _write_json(
        evidence_directory / "authorization.json",
        {
            "manifest_id": plan["manifest_id"],
            "manifest_sha256": manifest_sha256,
            "approval_sha256": approval_sha256,
            "authorization_reference": plan["authorization_reference"],
            "plan_sha256": plan["plan_sha256"],
        },
    )
    _write_json(
        evidence_directory / "source-hashes.json",
        {
            "baseline_head": plan["baseline_head"],
            "source_bindings": plan["source_bindings"],
            "migrations": plan["migrations"],
        },
    )
    try:
        capture_report = capture_runner(plan, api_key)
        _write_jsonl(
            evidence_directory / "subscription-transcript.jsonl",
            list(capture_report["sessions"]),
        )
        _write_jsonl(
            evidence_directory / "telemetry.jsonl",
            list(capture_report["telemetry"]),
        )
        _write_json(
            evidence_directory / "provider-observations.json",
            {
                key: capture_report[key]
                for key in (
                    "status",
                    "blockers",
                    "received_frames",
                    "received_bytes",
                    "raw_frames",
                    "normalized_messages",
                    "event_counts",
                    "condition_counts",
                    "exchange_counts",
                    "symbol_event_counts",
                )
            },
        )
        _write_json(
            evidence_directory / "loss-accounting.json",
            {
                "received_market_frames": sum(
                    row["accepted_frames"]
                    for row in capture_report["sessions"]
                ),
                "durable_raw_frames": capture_report["raw_frames"],
                "normalized_messages": capture_report[
                    "normalized_messages"
                ],
                "progress": capture_report["progress"],
                "buffer": capture_report["buffer"],
                "explicit_gap_count": 0,
                "quarantine_count": 0,
            },
        )
        if capture_report["status"] != "PASS_CAPTURE":
            raise RuntimeError("P7 live capture failed its bounded gates")
        database_report = database_runner(plan, capture_report)
        _write_json(
            evidence_directory / "database-audit.json",
            database_report,
        )
        if database_report["status"] != "PASS_DATABASE":
            raise RuntimeError("P7 disposable database phase failed")
    except Exception as failure:
        error = f"{type(failure).__name__}:{failure}"
    finally:
        temporary = Path(plan["cleanup"]["temporary_directory"])
        if temporary.exists():
            expected_parent = evidence_directory.resolve()
            if (
                temporary.resolve().parent != expected_parent
                or temporary.name != "temporary"
            ):
                raise RuntimeError("P7 cleanup target lost manifest ownership")
            shutil.rmtree(temporary)
        cleanup_report = {
            "temporary_directory_absent": not temporary.exists(),
            "database_cleanup_verified": bool(
                database_report
                and database_report.get("database_absent")
            ),
            "roles_cleanup_verified": bool(
                database_report
                and database_report.get("roles_absent")
            ),
            "application_data_deleted": False,
        }
    for name, default in (
        ("subscription-transcript.jsonl", []),
        ("telemetry.jsonl", []),
    ):
        path = evidence_directory / name
        if not path.exists():
            _write_jsonl(path, default)
    for name, default in (
        ("provider-observations.json", {"status": "NOT_COMPLETED"}),
        ("loss-accounting.json", {"status": "NOT_COMPLETED"}),
        ("database-audit.json", {"status": "NOT_COMPLETED"}),
    ):
        path = evidence_directory / name
        if not path.exists():
            _write_json(path, default)
    no_effects = _no_effects()
    _write_json(evidence_directory / "no-effects.json", no_effects)
    _write_json(evidence_directory / "cleanup.json", cleanup_report)
    status = (
        "PASS_LIVE_SHADOW"
        if error is None
        and capture_report is not None
        and capture_report["status"] == "PASS_CAPTURE"
        and database_report is not None
        and database_report["status"] == "PASS_DATABASE"
        and database_report["cleanup_verified"]
        and cleanup_report["temporary_directory_absent"]
        and cleanup_report["database_cleanup_verified"]
        and cleanup_report["roles_cleanup_verified"]
        and not any(
            value
            for key, value in no_effects.items()
            if key != "provider_rest_requests"
        )
        else "FAIL"
    )
    report = {
        "status": status,
        "manifest_id": plan["manifest_id"],
        "plan_sha256": plan["plan_sha256"],
        "authorization_reference": plan["authorization_reference"],
        "capture_status": (
            None if capture_report is None else capture_report["status"]
        ),
        "database_status": (
            None if database_report is None else database_report["status"]
        ),
        "cleanup": cleanup_report,
        "no_effects": no_effects,
        "error": error,
    }
    _write_json(evidence_directory / "report.json", report)
    return report
