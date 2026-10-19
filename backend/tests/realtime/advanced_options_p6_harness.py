"""Provider-free Docker workload components for Advanced Options P6 qualification."""
from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import sqlite3
import time
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import psycopg2

from realtime.acquisition_ledger import ProviderRequestLane
from realtime.advanced_equity_control_plane import (
    build_advanced_equity_control_plane_plan,
)
from realtime.advanced_options_control_plane import (
    ADVANCED_OPTIONS_CHANNELS,
    ADVANCED_OPTIONS_REST_LANES,
    AdvancedOptionsFeedScope,
    AdvancedOptionsOwnerNames,
    AdvancedOptionsPolicyPins,
    AdvancedOptionsReceiptInputs,
    AdvancedOptionsSocketBudget,
    build_advanced_options_control_plane_plan,
)
from realtime.advanced_options_cycle import (
    O2_MODEL_NOT_ACCEPTED,
    AdvancedOptionsNativeLaneResult,
    NativeLaneState,
    run_advanced_options_cycle,
)
from realtime.advanced_options_input import (
    build_advanced_options_input_closure,
    build_advanced_options_input_gate_plan,
    reconstruct_options_input_ready_receipt,
)
from realtime.advanced_options_producers import (
    DISCOVERY_ENDPOINT,
    OPEN_INTEREST_ENDPOINT,
    REPAIR_ENDPOINT,
    AdvancedOptionsReferenceClocks,
    build_advanced_options_producer_plan,
)
from realtime.advanced_options_projection import (
    build_advanced_options_projection_batch,
)
from advanced_options_reference_support import reference_state
from advanced_options_rolling_support import rolling_state
from realtime.advanced_options_reference_state import (
    InMemoryAdvancedOptionsReferenceStateRepository,
)
from realtime.contracts import MarketChannel, MarketFeed
from realtime.materialization_jobs import JobBarrierVerdict
from realtime.option_cycle_input import OptionCycleInput, OptionCycleUnderlying
from realtime.option_discovery_scope import RealtimeOptionDiscoveryScope
from realtime.option_session_manifest import BoundedOptionManifestPolicy
from realtime.option_slot_set import SlotSetMember, SlotSetPurpose
from realtime.preflight import (
    DetectorFamily,
    ProductEntitlementProfile,
    ProviderTier,
)
from realtime.provider_budget import ProviderBudgetScope


ROLE_PREFIX = "test_rt_options_p6"
UTC = timezone.utc
SESSION = date(2026, 10, 16)
CHECKED = datetime(2026, 10, 16, 12, 0, tzinfo=UTC)
PLANNED = datetime(2026, 10, 16, 13, 0, tzinfo=UTC)
NOW = datetime(2026, 10, 16, 14, 30, tzinfo=UTC)
EXPIRES = datetime(2026, 10, 16, 23, 0, tzinfo=UTC)
JOURNAL = UUID("11111111-1111-1111-1111-111111111111")
STREAM = UUID("22222222-2222-2222-2222-222222222222")
EQUITY_RECEIPT = UUID("33333333-3333-3333-3333-333333333333")
DISCOVERY_BATCH = UUID("66666666-7777-8888-9999-aaaaaaaaaaaa")
WINDOW = UUID("12121212-3434-5656-7878-909090909090")
EQUITY_WINDOW = UUID("abababab-cdcd-efef-1212-343434343434")
PAYLOAD_BYTES = 512
FAULT_DUPLICATES = 100
FAULT_OUT_OF_ORDER = 100
FAULT_GENERATION_MISMATCH = 1
MAX_DRAIN_SECONDS = 300
PHASES = (
    ("calibration-8-160", 300, 250),
    ("calibration-25-500", 300, 500),
    ("qualification-50-1000", 1800, 1000),
)
BURSTS = ((300, 30, 5000), (900, 30, 5000), (1500, 30, 5000))


def _control_plane():
    equity = build_advanced_equity_control_plane_plan(
        session_date=SESSION,
        security_ids={
            "AAPL": UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            "SPY": UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        },
        entitlement_evidence_sha256="a" * 64,
        authorization_reference="advanced-options-p6-equity",
        checked_at=CHECKED,
        expires_at=EXPIRES,
        schema_sha256="b" * 64,
        security_policy_sha256="c" * 64,
    )
    policy = BoundedOptionManifestPolicy(
        version="advanced-options-p6-v1",
        target_dtes=(21,),
        maximum_target_dte_deviation=0,
        strikes_per_side_per_expiry=1,
        minimum_dte=1,
        maximum_dte=60,
        strike_corridor_fraction=Decimal("0.20"),
        minimum_day_volume=20,
        minimum_open_interest=100,
        maximum_contracts_per_underlying=2,
        maximum_option_quote_contracts=3,
        reserved_option_quote_contracts=1,
        required_detectors=(
            DetectorFamily.S1,
            DetectorFamily.S2,
            DetectorFamily.O1,
            DetectorFamily.O2,
            DetectorFamily.O3,
        ),
    )
    pins = AdvancedOptionsPolicyPins(
        condition_semantics_sha256="1" * 64,
        flow_policy_sha256="2" * 64,
        valuation_policy_sha256="3" * 64,
        model_bundle_sha256="4" * 64,
        catalog_sha256="5" * 64,
        slot_set_policy_sha256="6" * 64,
    )
    slot_members = tuple(
        SlotSetMember(
            instrument_id,
            channel,
            purpose,
            100,
            instrument_id + 1_000,
        )
        for instrument_id, purpose in (
            (101, SlotSetPurpose.PRIMARY),
            (102, SlotSetPurpose.RESERVE),
        )
        for channel in ADVANCED_OPTIONS_CHANNELS
    )
    provider_budget = ProviderBudgetScope(
        account_alias="MASSIVE_ADVANCED",
        session_date=SESSION,
        authorization_reference="advanced-options-p6",
        provider_authorization_reference="provider-free-p6",
        expected_cost="zero provider calls",
        request_ceiling=50,
        decoded_byte_ceiling=50_000_000,
        lanes=ADVANCED_OPTIONS_REST_LANES,
        authorized_until=EXPIRES,
    )
    tier_a_state = reference_state()
    tier_b_state = rolling_state(
        cohort_id=control.cohort_id,
        journal_generation=control.source_flow.journal_generation,
    )
    reference_repository = InMemoryAdvancedOptionsReferenceStateRepository()
    reference_repository.put(tier_a_state)
    receipt_inputs = AdvancedOptionsReceiptInputs(
        cutoff=NOW,
        reference_state=tier_a_state,
        equity_input_receipt_id=EQUITY_RECEIPT,
        equity_input_sha256="7" * 64,
        discovery_batches_sha256="8" * 64,
        open_interest_sha256=tier_a_state.open_interest_sha256,
        consumer_checkpoints_sha256="a" * 64,
        unresolved_gaps_sha256="b" * 64,
        market_state_sha256="c" * 64,
        source_clocks_sha256="d" * 64,
        detector_outcomes_sha256="e" * 64,
        projection_sha256="f" * 64,
        build_id="advanced-options-p6-build",
    )
    return build_advanced_options_control_plane_plan(
        equity_plan=equity,
        entitlement=ProductEntitlementProfile(
            ProviderTier.MASSIVE_ADVANCED,
            ProviderTier.MASSIVE_ADVANCED,
        ),
        option_underlyings=("SPY",),
        options_entitlement_evidence_sha256="0" * 64,
        options_authorization_reference="advanced-options-p6",
        checked_at=CHECKED,
        expires_at=EXPIRES,
        planned_at=PLANNED,
        manifest_policy=policy,
        slot_members=slot_members,
        slot_effective_at=PLANNED,
        feed_scope=AdvancedOptionsFeedScope(
            feed=MarketFeed.OPTIONS,
            channels=(MarketChannel.TRADE, MarketChannel.QUOTE),
        ),
        pins=pins,
        socket_budget=AdvancedOptionsSocketBudget(
            stream_sessions=1,
            connection_attempts=3,
            reconnects=2,
            decoded_messages=1_000_000,
            decoded_bytes=100_000_000,
        ),
        provider_budget=provider_budget,
        owners=AdvancedOptionsOwnerNames(),
        journal_generation=JOURNAL,
        stream_session_ids=(STREAM,),
        receipt_inputs=receipt_inputs,
    )


def _cycle_result():
    control = _control_plane()
    clocks = AdvancedOptionsReferenceClocks(
        session_date=SESSION,
        planned_at=PLANNED,
        market_cutoff=NOW,
        known_at=NOW + timedelta(minutes=1),
        authorized_until=EXPIRES,
        prior_oi_session=SESSION - timedelta(days=1),
    )
    producer = build_advanced_options_producer_plan(
        control_plane=control,
        discovery_scope=RealtimeOptionDiscoveryScope(
            ProviderTier.MASSIVE_ADVANCED,
            control.profile_id,
            control.profile_sha256,
            control.universe,
        ),
        provider_budget=control.provider_budget,
        clocks=clocks,
        endpoint_response_byte_limits={
            DISCOVERY_ENDPOINT: 5_000_000,
            OPEN_INTEREST_ENDPOINT: 5_000_000,
            REPAIR_ENDPOINT: 5_000_000,
        },
    )
    cycle_input = OptionCycleInput(
        provider_tier=ProviderTier.MASSIVE_ADVANCED,
        scheduled_cycle=NOW,
        profile_id=control.profile_id,
        profile_sha256=control.profile_sha256,
        universe_sha256=control.universe.sha256,
        policy_sha256=control.manifest_policy.sha256,
        configuration_sha256="5" * 64,
        code_sha256="6" * 64,
        cohort_id=control.cohort_id,
        underlyings=(
            OptionCycleUnderlying(
                underlyer="SPY",
                underlying_security_id=UUID(
                    "11111111-2222-3333-4444-555555555555"
                ),
                discovery_batch_id=DISCOVERY_BATCH,
                discovery_evidence_sha256="1" * 64,
                payload_chain_sha256="2" * 64,
                coverage_sha256="3" * 64,
                request_filter_sha256="4" * 64,
                page_count=2,
                row_count=100,
                response_bytes=10_000,
                expiration_from=date(2026, 10, 23),
                expiration_through=date(2026, 12, 18),
                strike_min=Decimal("400"),
                strike_max=Decimal("600"),
            ),
        ),
        slot_set_version=control.slot_set.version,
        slot_set_member_sha256=control.slot_set.member_sha256,
    )
    tier_a_state = reference_state()
    closure = build_advanced_options_input_closure(
        control_plane=control,
        producer_plan=producer,
        cycle_input=cycle_input,
        reference_state=tier_a_state,
        rolling_state=tier_b_state,
        reference_policy_sha256=tier_a_state.reference_policy_sha256,
        oi_facts_sha256=tier_a_state.open_interest_sha256,
        market_state_checkpoint_sha256=(
            tier_b_state.market_state_checkpoint_sha256
        ),
        flow_checkpoint_sha256=tier_b_state.flow_checkpoint_sha256,
        unresolved_gaps_sha256="b" * 64,
        market_state_heads_sha256="c" * 64,
        equity_input_receipt_id=EQUITY_RECEIPT,
        equity_input_sha256="d" * 64,
    )
    gate = build_advanced_options_input_gate_plan(
        closure,
        window_id=WINDOW,
        equity_window_id=EQUITY_WINDOW,
        instrument_ids={"SPY": 101},
        build_id="p6-build",
        available_at=NOW,
    )
    complete = lambda _context: AdvancedOptionsNativeLaneResult(  # noqa: E731
        NativeLaneState.COMPLETE,
        (),
        (),
    )
    executors = {
        detector: complete
        for detector in control.manifest_policy.required_detectors
        if detector is not DetectorFamily.O2
    }
    return control, gate, executors, reference_repository


def _root(name: str) -> Path:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    path = Path(value)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _duration_scale() -> float:
    value = float(os.getenv("P6_DURATION_SCALE", "1"))
    if not 0 < value <= 1:
        raise ValueError("P6_DURATION_SCALE must be in (0, 1]")
    return value


def _database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        path,
        timeout=30,
        isolation_level=None,
    )
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS events (
            physical_seq INTEGER PRIMARY KEY AUTOINCREMENT,
            logical_seq INTEGER NOT NULL,
            generation TEXT NOT NULL,
            kind TEXT NOT NULL,
            created_ns INTEGER NOT NULL,
            payload BLOB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS seen (
            logical_seq INTEGER PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS checkpoint (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            physical_seq INTEGER NOT NULL,
            max_logical_seq INTEGER NOT NULL,
            accepted INTEGER NOT NULL,
            duplicates INTEGER NOT NULL,
            out_of_order INTEGER NOT NULL,
            generation_mismatch INTEGER NOT NULL,
            updated_ns INTEGER NOT NULL
        );
        INSERT OR IGNORE INTO checkpoint VALUES (1,0,0,0,0,0,0,0);
        CREATE TABLE IF NOT EXISTS status (
            component TEXT PRIMARY KEY,
            state TEXT NOT NULL,
            heartbeat_ns INTEGER NOT NULL,
            detail TEXT NOT NULL DEFAULT '{}'
        );
        CREATE TABLE IF NOT EXISTS lag_samples (
            sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
            lag_ms REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS queue_samples (
            sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
            depth INTEGER NOT NULL,
            sampled_ns INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS cycles (
            boundary INTEGER PRIMARY KEY,
            cycle_sha256 TEXT NOT NULL,
            o2_reason TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS reader_samples (
            sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
            latency_ms REAL NOT NULL,
            success INTEGER NOT NULL
        );
        """
    )
    return connection


def _status(
    connection: sqlite3.Connection,
    component: str,
    state: str,
    detail: dict[str, object] | None = None,
) -> None:
    connection.execute(
        "INSERT INTO status(component,state,heartbeat_ns,detail) "
        "VALUES (?,?,?,?) ON CONFLICT(component) DO UPDATE SET "
        "state=excluded.state,heartbeat_ns=excluded.heartbeat_ns,"
        "detail=excluded.detail",
        (component, state, time.time_ns(), json.dumps(detail or {})),
    )


def _insert_batch(
    connection: sqlite3.Connection,
    logical_start: int,
    count: int,
) -> int:
    if count <= 0:
        return logical_start
    now = time.time_ns()
    payload = b"x" * PAYLOAD_BYTES
    rows = []
    logical = logical_start
    for index in range(count):
        logical += 2
        rows.append((
            logical,
            "p6",
            "Q" if index % 5 else "T",
            now,
            payload,
        ))
    connection.execute("BEGIN IMMEDIATE")
    connection.executemany(
        "INSERT INTO events(logical_seq,generation,kind,created_ns,payload) "
        "VALUES (?,?,?,?,?)",
        rows,
    )
    connection.execute("COMMIT")
    return logical


def replay(state_path: Path) -> None:
    scale = _duration_scale()
    with closing(_database(state_path)) as connection:
        started_ns = time.time_ns()
        _status(
            connection,
            "replay",
            "RUNNING",
            {"scale": scale, "started_ns": started_ns},
        )
        logical = 0
        for name, base_duration, base_rate in PHASES:
            duration = base_duration * scale
            started = time.monotonic()
            inserted = 0
            carry = 0.0
            previous = started
            while time.monotonic() - started < duration:
                current = time.monotonic()
                elapsed = current - started
                rate = base_rate
                if name == "qualification-50-1000":
                    logical_second = elapsed / scale
                    for burst_start, burst_duration, burst_rate in BURSTS:
                        if burst_start <= logical_second < (
                            burst_start + burst_duration
                        ):
                            rate = burst_rate
                            break
                carry += (current - previous) * rate
                previous = current
                count = min(5000, int(carry))
                if count > 0:
                    logical = _insert_batch(connection, logical, count)
                    inserted += count
                    carry -= count
                _status(
                    connection,
                    "replay",
                    "RUNNING",
                    {"phase": name, "inserted": inserted},
                )
                time.sleep(min(0.05, max(0.001, scale)))
        payload = b"f" * PAYLOAD_BYTES
        duplicates = [
            (logical - index * 2, "p6", "T", time.time_ns(), payload)
            for index in range(FAULT_DUPLICATES)
        ]
        out_of_order = [
            (logical - index * 2 - 1, "p6", "Q", time.time_ns(), payload)
            for index in range(FAULT_OUT_OF_ORDER)
        ]
        mismatch = [
            (logical + 2, "wrong-generation", "T", time.time_ns(), payload)
        ]
        connection.execute("BEGIN IMMEDIATE")
        connection.executemany(
            "INSERT INTO events(logical_seq,generation,kind,created_ns,payload) "
            "VALUES (?,?,?,?,?)",
            [*duplicates, *out_of_order, *mismatch],
        )
        connection.execute("COMMIT")
        _status(
            connection,
            "replay",
            "COMPLETE",
            {
                "logical_high_water": logical,
                "started_ns": started_ns,
                "completed_ns": time.time_ns(),
            },
        )


def consumer(state_path: Path) -> None:
    with closing(_database(state_path)) as connection:
        _status(connection, "consumer", "RUNNING")
        idle_started = None
        while True:
            row = connection.execute(
                "SELECT physical_seq,max_logical_seq,accepted,duplicates,"
                "out_of_order,generation_mismatch FROM checkpoint WHERE singleton=1"
            ).fetchone()
            physical, maximum, accepted, duplicates, out_of_order, mismatch = row
            events = connection.execute(
                "SELECT physical_seq,logical_seq,generation,created_ns "
                "FROM events WHERE physical_seq>? ORDER BY physical_seq LIMIT 2000",
                (physical,),
            ).fetchall()
            if events:
                idle_started = None
                connection.execute("BEGIN IMMEDIATE")
                for event_physical, logical, generation, created_ns in events:
                    physical = event_physical
                    if generation != "p6":
                        mismatch += 1
                        continue
                    seen = connection.execute(
                        "INSERT OR IGNORE INTO seen(logical_seq) VALUES (?)",
                        (logical,),
                    ).rowcount
                    if not seen:
                        duplicates += 1
                    elif logical < maximum:
                        out_of_order += 1
                    else:
                        maximum = logical
                        accepted += 1
                        if accepted % 100 == 0:
                            connection.execute(
                                "INSERT INTO lag_samples(lag_ms) VALUES (?)",
                                ((time.time_ns() - created_ns) / 1_000_000,),
                            )
                connection.execute(
                    "UPDATE checkpoint SET physical_seq=?,max_logical_seq=?,"
                    "accepted=?,duplicates=?,out_of_order=?,generation_mismatch=?,"
                    "updated_ns=? WHERE singleton=1",
                    (
                        physical,
                        maximum,
                        accepted,
                        duplicates,
                        out_of_order,
                        mismatch,
                        time.time_ns(),
                    ),
                )
                connection.execute("COMMIT")
            replay_state = connection.execute(
                "SELECT state FROM status WHERE component='replay'"
            ).fetchone()
            latest = connection.execute(
                "SELECT COALESCE(max(physical_seq),0) FROM events"
            ).fetchone()[0]
            queue_depth = max(0, latest - physical)
            connection.execute(
                "INSERT INTO queue_samples(depth,sampled_ns) VALUES (?,?)",
                (queue_depth, time.time_ns()),
            )
            _status(
                connection,
                "consumer",
                "RUNNING",
                {"accepted": accepted, "queue_depth": queue_depth},
            )
            if replay_state and replay_state[0] == "COMPLETE" and queue_depth == 0:
                idle_started = idle_started or time.monotonic()
                if time.monotonic() - idle_started >= min(
                    1,
                    MAX_DRAIN_SECONDS * _duration_scale(),
                ):
                    break
            time.sleep(min(0.1, max(0.002, _duration_scale())))
        _status(connection, "consumer", "COMPLETE")


def _verify_final_projection(dsn: str) -> dict[str, object]:
    control, gate, executors, reference_repository = _cycle_result()
    verdict = JobBarrierVerdict(
        gate.barrier_id,
        "COMPLETE",
        len(gate.barrier_members),
        tuple(member.member_key for member in gate.barrier_members),
        (),
        (),
    )
    receipt = reconstruct_options_input_ready_receipt(
        gate,
        verdict,
        cutoff=NOW,
        reference_generation=7,
        build_id="p6-build",
    )
    cycle = run_advanced_options_cycle(
        control_plane=control,
        input_gate=gate,
        input_receipt=receipt,
        executors=executors,
        cutoff=NOW,
        reference_generation=7,
        reference_repository=reference_repository,
        build_id="p6-build",
    )
    batch = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p6-build",
    )
    relations = (
        "rt.portal_releases",
        "rt.portal_release_current",
        "rt.alert_outbox",
        "rt.session_archives",
        "rt.archive_segments",
    )
    with psycopg2.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET ROLE test_rt_options_p6_app")
            cursor.execute("SELECT current_database()")
            if cursor.fetchone()[0] != "test_realtime_v2_options_p6":
                raise RuntimeError("P6 app DSN targets the wrong database")
            counts = {}
            for relation in relations:
                cursor.execute(f"SELECT count(*) FROM {relation}")
                counts[relation] = int(cursor.fetchone()[0])
            cursor.execute("RESET ROLE")
    o2 = next(
        outcome for outcome in cycle.outcomes
        if outcome.detector is DetectorFamily.O2
    )
    if o2.reason_codes != (O2_MODEL_NOT_ACCEPTED,):
        raise RuntimeError("P6 O2 disposition changed")
    if batch.publication_authorized:
        raise RuntimeError("P6 projection unexpectedly authorized publication")
    return {
        "forbidden_effect_counts": counts,
        "projection_count": len(batch.snapshots),
        "o2_reason": O2_MODEL_NOT_ACCEPTED,
    }


def orchestrator(state_path: Path) -> None:
    dsn = os.getenv("REALTIME_V2_APP_DSN", "").strip()
    if not dsn:
        raise RuntimeError("P6 orchestrator app DSN is unavailable")
    with closing(_database(state_path)) as connection:
        _status(connection, "orchestrator", "RUNNING")
        last_boundary = 0
        while True:
            checkpoint = connection.execute(
                "SELECT accepted FROM checkpoint WHERE singleton=1"
            ).fetchone()[0]
            boundary = checkpoint // 100_000
            while last_boundary < boundary:
                last_boundary += 1
                (
                    control,
                    gate,
                    executors,
                    reference_repository,
                ) = _cycle_result()
                verdict = JobBarrierVerdict(
                    gate.barrier_id,
                    "COMPLETE",
                    len(gate.barrier_members),
                    tuple(
                        member.member_key for member in gate.barrier_members
                    ),
                    (),
                    (),
                )
                receipt = reconstruct_options_input_ready_receipt(
                    gate,
                    verdict,
                    cutoff=NOW,
                    reference_generation=7,
                    build_id="p6-build",
                )
                cycle = run_advanced_options_cycle(
                    control_plane=control,
                    input_gate=gate,
                    input_receipt=receipt,
                    executors=executors,
                    cutoff=NOW,
                    reference_generation=7,
                    reference_repository=reference_repository,
                    build_id="p6-build",
                )
                o2 = next(
                    outcome for outcome in cycle.outcomes
                    if outcome.detector is DetectorFamily.O2
                )
                if o2.reason_codes != (O2_MODEL_NOT_ACCEPTED,):
                    raise RuntimeError("P6 O2 disposition changed")
                connection.execute(
                    "INSERT OR IGNORE INTO cycles(boundary,cycle_sha256,o2_reason) "
                    "VALUES (?,?,?)",
                    (
                        last_boundary,
                        cycle.cycle_sha256,
                        O2_MODEL_NOT_ACCEPTED,
                    ),
                )
            consumer_state = connection.execute(
                "SELECT state FROM status WHERE component='consumer'"
            ).fetchone()
            _status(
                connection,
                "orchestrator",
                "RUNNING",
                {"cycles": last_boundary},
            )
            if consumer_state and consumer_state[0] == "COMPLETE":
                break
            time.sleep(min(0.5, max(0.01, _duration_scale())))
        verification = _verify_final_projection(dsn)
        _status(
            connection,
            "orchestrator",
            "COMPLETE",
            verification,
        )


def reader(state_path: Path) -> None:
    dsn = os.getenv("REALTIME_V2_API_DSN", "").strip()
    if not dsn:
        raise RuntimeError("P6 reader API DSN is unavailable")
    with closing(_database(state_path)) as state:
        _status(state, "reader", "RUNNING")
        while True:
            started = time.perf_counter_ns()
            success = 0
            try:
                with psycopg2.connect(dsn) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("SET ROLE test_rt_options_p6_api")
                        cursor.execute(
                            "SELECT count(*) FROM rt.snapshot_catalog"
                        )
                        cursor.fetchone()
                        success = 1
            except psycopg2.Error:
                success = 0
            latency = (time.perf_counter_ns() - started) / 1_000_000
            state.execute(
                "INSERT INTO reader_samples(latency_ms,success) VALUES (?,?)",
                (latency, success),
            )
            orchestrator_state = state.execute(
                "SELECT state FROM status WHERE component='orchestrator'"
            ).fetchone()
            _status(
                state,
                "reader",
                "RUNNING",
                {"last_success": bool(success)},
            )
            if orchestrator_state and orchestrator_state[0] == "COMPLETE":
                break
            time.sleep(min(1, max(0.02, _duration_scale())))
        _status(state, "reader", "COMPLETE")


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return math.inf
    ordered = sorted(values)
    index = min(
        len(ordered) - 1,
        max(0, math.ceil(len(ordered) * percentile) - 1),
    )
    return ordered[index]


def metrics(state_path: Path, report_path: Path) -> None:
    scale = _duration_scale()
    deadline = time.monotonic() + (2700 * scale) + 300
    with closing(_database(state_path)) as connection:
        _status(connection, "metrics", "RUNNING")
        while time.monotonic() < deadline:
            states = {
                component: state
                for component, state in connection.execute(
                    "SELECT component,state FROM status"
                )
            }
            if all(
                states.get(component) == "COMPLETE"
                for component in (
                    "replay",
                    "consumer",
                    "orchestrator",
                    "reader",
                )
            ):
                break
            _status(connection, "metrics", "RUNNING", states)
            time.sleep(min(1, max(0.02, scale)))
        else:
            raise RuntimeError("P6 metrics timed out waiting for components")
        checkpoint = connection.execute(
            "SELECT physical_seq,accepted,duplicates,out_of_order,"
            "generation_mismatch FROM checkpoint WHERE singleton=1"
        ).fetchone()
        lag = [
            float(row[0])
            for row in connection.execute("SELECT lag_ms FROM lag_samples")
        ]
        queue = [
            int(row[0])
            for row in connection.execute("SELECT depth FROM queue_samples")
        ]
        reader_rows = connection.execute(
            "SELECT latency_ms,success FROM reader_samples"
        ).fetchall()
        cycle_count = connection.execute(
            "SELECT count(*) FROM cycles"
        ).fetchone()[0]
        status_rows = {
            component: {
                "state": state,
                "heartbeat_ns": int(heartbeat_ns),
                "detail": json.loads(detail),
            }
            for component, state, heartbeat_ns, detail in connection.execute(
                "SELECT component,state,heartbeat_ns,detail FROM status"
            )
        }
        replay_detail = status_rows["replay"]["detail"]
        actual_duration_seconds = (
            int(replay_detail["completed_ns"])
            - int(replay_detail["started_ns"])
        ) / 1_000_000_000
        drain_seconds = max(
            0.0,
            (
                status_rows["consumer"]["heartbeat_ns"]
                - status_rows["replay"]["heartbeat_ns"]
            )
            / 1_000_000_000,
        )
        journal_usage = shutil.disk_usage(state_path.parent)
        report_usage = shutil.disk_usage(report_path.parent)
        report = {
            "status": "MEASURED",
            "duration_scale": scale,
            "actual_input_duration_seconds": actual_duration_seconds,
            "logical_input_duration_seconds": actual_duration_seconds / scale,
            "accepted_per_second": (
                checkpoint[1] / actual_duration_seconds
                if actual_duration_seconds > 0
                else 0
            ),
            "drain_seconds": drain_seconds,
            "events": {
                "physical": checkpoint[0],
                "accepted": checkpoint[1],
                "duplicates": checkpoint[2],
                "out_of_order": checkpoint[3],
                "generation_mismatch": checkpoint[4],
            },
            "lag_ms": {
                "sample_count": len(lag),
                "p95": _percentile(lag, 0.95),
                "p99": _percentile(lag, 0.99),
                "maximum": max(lag, default=math.inf),
            },
            "queue": {
                "sample_count": len(queue),
                "maximum_depth": max(queue, default=0),
            },
            "reader": {
                "samples": len(reader_rows),
                "failures": sum(1 for _, success in reader_rows if not success),
                "p99_ms": _percentile(
                    [float(latency) for latency, _ in reader_rows],
                    0.99,
                ),
            },
            "cycles": cycle_count,
            "o2_reason": O2_MODEL_NOT_ACCEPTED,
            "orchestrator": status_rows["orchestrator"]["detail"],
            "storage": {
                "journal_bytes": sum(
                    path.stat().st_size
                    for path in state_path.parent.rglob("*")
                    if path.is_file()
                ),
                "report_bytes_before_final_report": sum(
                    path.stat().st_size
                    for path in report_path.parent.rglob("*")
                    if path.is_file()
                ),
                "journal_free_bytes": journal_usage.free,
                "journal_free_percent": (
                    journal_usage.free / journal_usage.total * 100
                ),
                "report_free_bytes": report_usage.free,
                "report_free_percent": (
                    report_usage.free / report_usage.total * 100
                ),
            },
            "provider_calls": 0,
            "publication_authorized": False,
            "archive_execution_authorized": False,
            "retention_execution_authorized": False,
            "execution_authorized": False,
            "delivery_authorized": False,
        }
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        _status(connection, "metrics", "COMPLETE")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--component",
        choices=("replay", "consumer", "orchestrator", "reader", "metrics"),
        required=True,
    )
    args = parser.parse_args(argv)
    state_root = _root("P6_STATE_ROOT")
    report_root = _root("P6_REPORT_ROOT")
    state_path = state_root / "state.sqlite"
    if args.component == "replay":
        replay(state_path)
    elif args.component == "consumer":
        consumer(state_path)
    elif args.component == "orchestrator":
        orchestrator(state_path)
    elif args.component == "reader":
        reader(state_path)
    else:
        metrics(state_path, report_root / "options-p6-report.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
