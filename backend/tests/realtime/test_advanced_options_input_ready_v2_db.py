from contextlib import closing
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from psycopg2.extras import Json
from psycopg2 import errors

from option_flow_support import advanced_policy
from options.config import load_valuation_policy
from options.domain import ContractType, MarkSource
from options.strategies.domain import OptionSide
from realtime.advanced_options_control_plane import (
    build_advanced_options_equity_input_binding,
    persist_advanced_options_equity_input_binding_cursor,
    persist_advanced_options_enrollment_cursor,
)
from realtime.advanced_options_input import (
    AdvancedOptionsInputDependencyStatus,
    advanced_options_input_ready_plan_payload,
    build_advanced_options_input_ready_plan,
    freeze_advanced_options_input_ready_plan,
)
from realtime.advanced_options_equity_input import (
    initial_advanced_options_equity_context_requirements,
)
from realtime.advanced_options_mark_producer import (
    PostgresAdvancedOptionsMarkProducer,
    build_advanced_options_mark_producer_plan,
    freeze_advanced_options_mark_producer_plan,
)
from realtime.advanced_options_native_adapters import (
    AdvancedO3Invocation,
    advanced_o3_invocation_document,
)
from realtime.advanced_options_native_loader import (
    NativeInputPlanStatus,
    PostgresAdvancedOptionsNativeInputLoader,
    build_advanced_options_native_input_plan,
    freeze_advanced_options_native_input_plan,
)
from realtime.advanced_options_postgres import (
    AdvancedOptionsPostgresRuntimeScope,
    PostgresAdvancedOptionsCanonicalRuntimePort,
)
from realtime.advanced_options_producers import (
    AdvancedOptionsReferenceClocks,
    build_advanced_options_producer_plan,
)
from realtime.advanced_options_reference_producer import (
    PostgresAdvancedOptionsReferenceProducer,
    build_advanced_options_reference_producer_plan,
)
from realtime.advanced_options_rolling_producer import (
    PostgresAdvancedOptionsRollingProducer,
    build_advanced_options_rolling_producer_plan,
)
from realtime.contracts import canonical_sha256
from realtime.database_access_v2 import (
    API_ROLE,
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import SECURITY_MODE_HARDENED
from realtime.option_source_flow import (
    build_option_source_flow_identity,
    persist_option_contract_scope_cursor,
    persist_option_flow_policy_cursor,
    persist_option_source_flow_cursor,
)
from realtime.option_portal_adapter import (
    RealtimeOptionPackage,
    RealtimeOptionPackageLeg,
)
from realtime.option_portal_repository import (
    persist_realtime_option_package_cursor,
)
from realtime.security_v2 import (
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from scripts import audit_advanced_options_input_readiness as audit
from tests.realtime.test_advanced_options_native_loader_v2_db import (
    _seed_native_equity_inputs,
)
from tests.realtime.test_developer_cycle_compute import _credit_fixture
from tests.realtime.test_advanced_options_producers import (
    _discovery_scope,
    _limits,
)
from tests.realtime.test_advanced_options_reference_producer_v2_db import (
    _contract_scope,
    _disposable_audit,
    _seed_reference_inputs,
)
from tests.realtime.test_advanced_options_rolling_producer_v2_db import (
    _seed_rolling_inputs,
)
from tests.realtime.test_option_slot_sets_v2_db import (
    _owp4_enrollment,
    _prohibited_effect_counts,
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


def _digest(value) -> str:
    return canonical_sha256(("owp4f", value))


def _valuation_policy():
    return load_valuation_policy(
        Path(__file__).parents[2]
        / "options"
        / "policies"
        / "valuation_advanced_nbbo_v1.json"
    )


def _seed_input_window(database_cursor, enrollment, cutoff):
    service_plan_id = uuid4()
    window_id = uuid4()
    database_cursor.execute(
        "INSERT INTO rt.service_plans ("
        "plan_id,cohort_id,session_date,stage_catalog_version,"
        "enabled_stages,config_sha256,deadline,scope_reference"
        ") VALUES (%s,%s,%s,'stages_v1',%s,%s,%s,%s)",
        (
            str(service_plan_id),
            str(enrollment.control_plane.cohort_id),
            enrollment.control_plane.session_date,
            ["OPTIONS_ADMISSION"],
            enrollment.enrollment_sha256,
            cutoff + timedelta(hours=8),
            "owp4f explicit provider-free input-ready action",
        ),
    )
    database_cursor.execute(
        "INSERT INTO rt.window_runs ("
        "window_id,plan_id,cohort_id,provider_tier,window_key,"
        "release_at,cutoff,deadline,state) VALUES ("
        "%s,%s,%s,'MASSIVE_ADVANCED','owp4f-options-input-ready',"
        "%s,%s,%s,'PENDING')",
        (
            str(window_id),
            str(service_plan_id),
            str(enrollment.control_plane.cohort_id),
            cutoff,
            cutoff,
            cutoff + timedelta(minutes=5),
        ),
    )
    return service_plan_id, window_id


def _seed_discovery_coverage(
    database_cursor,
    control,
    scheduled_cycle=None,
):
    database_cursor.execute(
        "SELECT discovery_batch_id,expiration_from,expiration_through,"
        "strike_min,strike_max FROM rt.option_discovery_batches "
        "WHERE profile_id=%s AND scheduled_cycle=%s",
        (
            str(control.profile_id),
            control.planned_at
            if scheduled_cycle is None
            else scheduled_cycle,
        ),
    )
    batch_id, expiration_from, expiration_through, strike_min, strike_max = (
        database_cursor.fetchone()
    )
    rows = (
        (
            "EXPIRATION_DATE",
            "INCLUDED",
            "GTE",
            expiration_from.isoformat(),
            None,
        ),
        (
            "EXPIRATION_DATE",
            "INCLUDED",
            "LTE",
            expiration_through.isoformat(),
            None,
        ),
        (
            "EXPIRATION_DATE",
            "EXCLUDED",
            "LT",
            expiration_from.isoformat(),
            "OUTSIDE_DISCOVERY_WINDOW",
        ),
        (
            "EXPIRATION_DATE",
            "EXCLUDED",
            "GT",
            expiration_through.isoformat(),
            "OUTSIDE_DISCOVERY_WINDOW",
        ),
        (
            "STRIKE_PRICE",
            "INCLUDED",
            "GTE",
            str(strike_min),
            None,
        ),
        (
            "STRIKE_PRICE",
            "INCLUDED",
            "LTE",
            str(strike_max),
            None,
        ),
        (
            "STRIKE_PRICE",
            "EXCLUDED",
            "LT",
            str(strike_min),
            "OUTSIDE_STRIKE_CORRIDOR",
        ),
        (
            "STRIKE_PRICE",
            "EXCLUDED",
            "GT",
            str(strike_max),
            "OUTSIDE_STRIKE_CORRIDOR",
        ),
    )
    for dimension, disposition, comparison, boundary, reason in rows:
        database_cursor.execute(
            "INSERT INTO rt.option_discovery_coverage ("
            "discovery_batch_id,dimension,disposition,comparison,"
            "boundary,reason_code) VALUES (%s,%s,%s,%s,%s,%s)",
            (
                str(batch_id),
                dimension,
                disposition,
                comparison,
                boundary,
                reason,
            ),
        )


def _seed_o3_invocation(
    database_cursor,
    enrollment,
    mark,
    *,
    cutoff,
    flow_event_id=None,
):
    selected = enrollment.activity_selection.contracts[0]
    candidate_id = uuid4()
    matrix_id = uuid4()
    candidate_sha256 = _digest("o3-candidate")
    package = RealtimeOptionPackage(
        candidate_id=candidate_id,
        event_id=flow_event_id,
        matrix_id=matrix_id,
        candidate_identity_sha256=candidate_sha256,
        cohort_id=enrollment.control_plane.cohort_id,
        provider_tier=enrollment.control_plane.binding.options_tier,
        entry_market_time=mark.market_time,
        entry_net_premium=-(mark.midpoint * Decimal("100")),
        capital_at_risk=mark.midpoint * Decimal("100"),
        exit_deadline=cutoff + timedelta(minutes=30),
        legs=(
            RealtimeOptionPackageLeg(
                leg_index=0,
                instrument_id=selected.instrument_id,
                contract_id=selected.contract_id,
                catalog_version_id=selected.catalog_version_id,
                contract_ticker=selected.contract_ticker,
                side=OptionSide.BUY,
                ratio=1,
                multiplier=100,
                strike=selected.strike,
                expiration_date=selected.expiration_date,
                contract_type=ContractType.CALL,
                entry_mark=mark.midpoint,
                entry_market_time=mark.market_time,
                valuation_policy_sha256=(
                    enrollment.control_plane.pins.valuation_policy_sha256
                ),
            ),
        ),
    )
    persist_realtime_option_package_cursor(database_cursor, package)
    candidate, _decision, source_snapshots, invalidation = _credit_fixture()
    candidate_expiration = cutoff.date() + timedelta(days=21)
    candidate = replace(
        candidate,
        candidate_id=candidate_id,
        identity_sha256=candidate_sha256,
        matrix_id=matrix_id,
        underlyer="SPY",
        expiration_date=candidate_expiration,
        market_data_time=cutoff,
        observed_time=cutoff,
        valid_until=cutoff + timedelta(minutes=15),
        legs=tuple(
            replace(
                leg,
                expiration_date=candidate_expiration,
                source_market_time=cutoff,
                mark_source="ADVANCED_NBBO_MIDPOINT",
            )
            for leg in candidate.legs
        ),
    )
    event_id = uuid4()
    decision = SimpleNamespace(
        detector_id="O1",
        origin="OPTIONS_FIRST",
        disposition="CONFIRMED",
        direction=1,
        decision_at=cutoff,
        valid_until=cutoff + timedelta(minutes=10),
        activity=SimpleNamespace(episode_id=uuid4()),
        sha256=_digest("o3-decision"),
    )
    snapshots = tuple(
        SimpleNamespace(
            snapshot_id=row.snapshot_id,
            open_interest=1000,
            day_volume=100,
            bid=mark.bid,
            ask=mark.ask,
            mark_source=MarkSource.ADVANCED_NBBO_MIDPOINT,
        )
        for row in source_snapshots
        if row.snapshot_id in {
            leg.snapshot_id for leg in candidate.legs
        }
    )
    invocation = AdvancedO3Invocation(
        event_id=event_id,
        candidate=candidate,
        decision=decision,
        activity_contract_id=candidate.legs[0].contract_id,
        snapshots=snapshots,
        structural_invalidation=invalidation,
        scheduled_cycle=cutoff,
        package=package,
    )
    document = advanced_o3_invocation_document(invocation)
    detector_sha256 = _digest("o3-input-detector")
    database_cursor.execute(
        "INSERT INTO rt.detector_versions ("
        "detector_sha256,detector_id,version,code_sha256,"
        "policy_sha256,requirements,output_kind) VALUES ("
        "%s,'ADVANCED_OPTIONS_O3_INPUT','owp4f-v1',%s,%s,%s,'SIGNAL')",
        (
            detector_sha256,
            _digest("o3-input-code"),
            _digest("o3-input-policy"),
            Json({
                "canonical_marks": True,
                "execution_permission": False,
            }),
        ),
    )
    database_cursor.execute(
        "INSERT INTO rt.detector_events ("
        "event_id,dedup_sha256,cohort_id,provider_tier,"
        "detector_sha256,instrument_id,trigger_identity,trigger_at,"
        "decision_at,disposition,reason_codes,readiness_epoch,"
        "evidence_sha256,payload_schema,payload) VALUES ("
        "%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,%s,"
        "'CONFIRMED',ARRAY[]::text[],1,%s,"
        "'advanced_options_o3_input_v1',%s)",
        (
            str(event_id),
            _digest("o3-input-event"),
            str(enrollment.control_plane.cohort_id),
            detector_sha256,
            selected.instrument_id,
            f"O3:{candidate_id}:{cutoff.isoformat()}",
            cutoff,
            cutoff,
            _digest("o3-input-evidence"),
            Json({
                "detector_id": "O3",
                "candidate_id": str(candidate_id),
                "disposition": "CONFIRMED",
            }),
        ),
    )
    database_cursor.execute(
        "INSERT INTO rt.detector_event_inputs ("
        "event_id,input_role,input_key,source_kind,"
        "option_mark_evidence_id,input_sha256,available_at,received_at,"
        "retained_input) VALUES ("
        "%s,'O3_NATIVE_INVOCATION',%s,'OPTION_MARK',"
        "%s,%s,%s,%s,%s)",
        (
            str(event_id),
            str(candidate_id),
            str(mark.evidence_id),
            canonical_sha256(document),
            mark.observed_time,
            mark.observed_time,
            Json(document),
        ),
    )
    return package


def _runtime_scope(
    *,
    control,
    producer_plan,
    reference_plan,
    rolling_plan,
    native_plan,
    mark_plan,
    input_plan,
):
    return AdvancedOptionsPostgresRuntimeScope(
        control_plane=control,
        producer_plan=producer_plan,
        code_sha256=input_plan.producer_build_sha256,
        native_inputs_loader=None,
        evidence_builder=lambda _cycle: pytest.fail(
            "input-ready qualification attempted cycle evidence"
        ),
        close_data_loader=lambda _context, _projection: pytest.fail(
            "input-ready qualification attempted close"
        ),
        require_discovery_pages=False,
        reference_producer_plan=reference_plan,
        rolling_producer_plan=rolling_plan,
        native_input_plan=native_plan,
        mark_producer_plan=mark_plan,
        input_ready_plan=input_plan,
    )


def test_owp4f_input_ready_action_and_disposable_readiness(monkeypatch):
    suffix = uuid4().hex[:8]
    database = f"test_realtime_v2_owp4f_{suffix}"
    roles = build_role_names_v2(f"test_rt_owp4f_{suffix}")
    roles_owned = False
    try:
        with disposable_v2_database(database) as database_pool:
            valuation_policy = _valuation_policy()
            flow_policy = advanced_policy()
            enrollment = _owp4_enrollment(
                database_pool,
                flow_policy_sha256=flow_policy.sha256,
                security_free_float=Decimal("1000000000"),
                valuation_policy_sha256=(
                    valuation_policy.policy_sha256
                ),
                provider_request_ceiling=2,
                provider_decoded_byte_ceiling=100_000_000,
            )
            control = enrollment.control_plane
            cutoff = control.planned_at
            contract_scope = _contract_scope(enrollment, flow_policy)
            reference_plan = build_advanced_options_reference_producer_plan(
                enrollment=enrollment,
                contract_scope=contract_scope,
                reference_generation=1,
                known_at=control.planned_at,
                valid_until=control.expires_at,
                discovery_scheduled_cycle=control.planned_at,
                prior_oi_session=control.session_date - timedelta(days=1),
                model_version="black-scholes-merton-v1",
            )
            source_flow = build_option_source_flow_identity(
                journal_generation=control.source_flow.journal_generation,
                stream_session_ids=control.source_flow.stream_session_ids,
                condition_semantics_sha256=(
                    control.pins.condition_semantics_sha256
                ),
                flow_policy_sha256=_digest("flow-engine"),
                advanced_flow_policy_sha256=flow_policy.sha256,
                contract_scope_sha256=contract_scope.sha256,
            )
            with cursor(database_pool) as database_cursor:
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
                )
                _seed_discovery_coverage(database_cursor, control)
                service_plan_id, window_id = _seed_input_window(
                    database_cursor,
                    enrollment,
                    cutoff,
                )
            connection_value = database_pool.getconn()
            try:
                created, security_audit = harden_database_v2(
                    connection_value,
                    roles,
                )
                roles_owned = True
                assert len(created) == 3
                assert all(
                    status.startswith("PASS")
                    for _, status in security_audit
                )
            finally:
                database_pool.putconn(connection_value)
            provider = V2ConnectionProvider(
                {APP_ROLE: database_pool, API_ROLE: database_pool},
                V2AccessConfig(
                    security_mode=SECURITY_MODE_HARDENED,
                    role_prefix=roles.prefix,
                    allow_disposable=True,
                ),
            )
            assert provider.verify() == database
            reference = PostgresAdvancedOptionsReferenceProducer(
                provider,
                reference_plan,
                retry_initial_seconds=0,
            ).produce()
            owner_epoch = 3
            committed_offset = 11
            shard_map_sha256 = _digest("shard-map")
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                _seed_rolling_inputs(
                    database_cursor,
                    enrollment,
                    source_flow,
                    owner_epoch=owner_epoch,
                    committed_offset=committed_offset,
                    shard_map_sha256=shard_map_sha256,
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
                    equity_cutoff=cutoff,
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
                        authorization_reference="owp4f-equity-input",
                    ),
                )
            rolling_plan = build_advanced_options_rolling_producer_plan(
                enrollment=enrollment,
                contract_scope=contract_scope,
                reference_state=reference,
                source_flow=source_flow,
                owner_epoch=owner_epoch,
                market_state_committed_offset=committed_offset,
                flow_committed_offset=committed_offset,
                market_state_shard_ids=(0,),
                flow_shard_ids=(0,),
                market_state_shard_map_sha256=shard_map_sha256,
                flow_shard_map_sha256=shard_map_sha256,
                known_at=cutoff,
                valid_until=control.expires_at,
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
            producer_plan = build_advanced_options_producer_plan(
                control_plane=control,
                discovery_scope=_discovery_scope(control),
                provider_budget=control.provider_budget,
                clocks=AdvancedOptionsReferenceClocks(
                    session_date=control.session_date,
                    planned_at=cutoff,
                    market_cutoff=cutoff,
                    known_at=cutoff,
                    authorized_until=(
                        control.provider_budget.authorized_until
                    ),
                    prior_oi_session=control.session_date - timedelta(days=1),
                ),
                endpoint_response_byte_limits=_limits(),
            )

            def native_plan():
                candidate = build_advanced_options_native_input_plan(
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
                    history_source_cohort_id=uuid4(),
                    maximum_equity_input_age_seconds=300,
                    equity_context_requirements=(
                        initial_advanced_options_equity_context_requirements()
                    ),
                )
                preflight = PostgresAdvancedOptionsNativeInputLoader(
                    provider,
                    candidate,
                ).preflight()
                assert (
                    preflight.status is NativeInputPlanStatus.READY
                ), preflight.reason_codes
                return (
                    freeze_advanced_options_native_input_plan(
                        candidate,
                        preflight,
                    ),
                    preflight,
                )

            native_before_marks, native_before_preflight = native_plan()
            candidate = build_advanced_options_input_ready_plan(
                enrollment=enrollment,
                producer_plan=producer_plan,
                reference_producer_plan=reference_plan,
                rolling_producer_plan=rolling_plan,
                native_input_plan=native_before_marks,
                mark_producer_plan=mark_plan,
                native_executor_membership_sha256=(
                    native_before_preflight.executor_membership_sha256
                ),
                service_plan_id=service_plan_id,
                window_id=window_id,
                cutoff=cutoff,
                producer_build_id="advanced-options-input-ready-v1",
                producer_build_sha256=_digest("input-ready-build"),
            )
            blocked_port = PostgresAdvancedOptionsCanonicalRuntimePort(
                provider,
                _runtime_scope(
                    control=control,
                    producer_plan=producer_plan,
                    reference_plan=reference_plan,
                    rolling_plan=rolling_plan,
                    native_plan=native_before_marks,
                    mark_plan=mark_plan,
                    input_plan=candidate,
                ),
            )
            blocked = blocked_port.preflight_input_ready()
            assert (
                blocked.status
                is AdvancedOptionsInputDependencyStatus.BLOCKED
            )
            assert "IMMUTABLE_ADVANCED_NBBO_MARKS_PARTIAL" in (
                blocked.reason_codes
            )
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                unit.cursor.execute(
                    "SELECT count(*) FROM rt.cycle_receipts "
                    "WHERE receipt_kind='OPTIONS_INPUT_READY'"
                )
                assert unit.cursor.fetchone()[0] == 0

            mark_producer = PostgresAdvancedOptionsMarkProducer(
                provider,
                mark_plan,
                retry_initial_seconds=0,
            )
            assert mark_producer.produce() == mark_preflight.marks
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                _seed_o3_invocation(
                    database_cursor,
                    enrollment,
                    mark_preflight.marks[0],
                    cutoff=cutoff,
                )
            native_frozen, native_preflight = native_plan()
            candidate = build_advanced_options_input_ready_plan(
                enrollment=enrollment,
                producer_plan=producer_plan,
                reference_producer_plan=reference_plan,
                rolling_producer_plan=rolling_plan,
                native_input_plan=native_frozen,
                mark_producer_plan=mark_plan,
                native_executor_membership_sha256=(
                    native_preflight.executor_membership_sha256
                ),
                service_plan_id=service_plan_id,
                window_id=window_id,
                cutoff=cutoff,
                producer_build_id="advanced-options-input-ready-v1",
                producer_build_sha256=_digest("input-ready-build"),
            )
            assert advanced_options_input_ready_plan_payload(candidate)[
                "mode"
            ] == "PLAN"
            scope = _runtime_scope(
                control=control,
                producer_plan=producer_plan,
                reference_plan=reference_plan,
                rolling_plan=rolling_plan,
                native_plan=native_frozen,
                mark_plan=mark_plan,
                input_plan=candidate,
            )
            port = PostgresAdvancedOptionsCanonicalRuntimePort(
                provider,
                scope,
            )
            preflight = port.preflight_input_ready()
            assert (
                preflight.status
                is AdvancedOptionsInputDependencyStatus.AVAILABLE
            )
            assert any(
                row.status
                is AdvancedOptionsInputDependencyStatus.COMPLETE_ZERO
                for row in preflight.evidence
            )
            assert {
                row.member_key
                for row in preflight.evidence
                if row.accepted_unavailable
            } == {
                "O2", "IV_SURFACE", "DARK_POOL",
                *(
                    detector
                    for detector, lane in (
                        ("O1", native_preflight.bundle.o1),
                        ("O3", native_preflight.bundle.o3),
                        ("OS1", native_preflight.bundle.os1),
                        ("OS2", native_preflight.bundle.os2),
                    )
                    if lane.blocked_reason_codes
                ),
            }
            assert all(
                row.status is AdvancedOptionsInputDependencyStatus.UNAVAILABLE
                for row in preflight.evidence if row.accepted_unavailable
            )
            frozen = freeze_advanced_options_input_ready_plan(
                candidate,
                preflight,
            )
            port = PostgresAdvancedOptionsCanonicalRuntimePort(
                provider,
                replace(scope, input_ready_plan=frozen),
            )
            frozen_preflight = port.preflight_input_ready()
            assert frozen_preflight.closure == preflight.closure
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                effects_before = _prohibited_effect_counts(unit.cursor)
            original_assemble = port._assemble_input_ready
            attempts = 0

            def fail_once(unit, *, lock_window):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    unit.cursor.execute(
                        "DO $block$ BEGIN RAISE EXCEPTION 'retry' "
                        "USING ERRCODE='40001'; END $block$"
                    )
                return original_assemble(
                    unit,
                    lock_window=lock_window,
                )

            monkeypatch.setattr(port, "_assemble_input_ready", fail_once)
            first = port.produce_input_ready(
                maximum_attempts=2,
                retry_initial_seconds=0,
                wait=lambda _seconds: None,
            )
            second = port.produce_input_ready(
                maximum_attempts=2,
                retry_initial_seconds=0,
                wait=lambda _seconds: None,
            )
            assert attempts == 3
            assert first == second
            assert first.receipt.input_sha256 == first.closure_sha256
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                unit.cursor.execute(
                    "SELECT count(*) FROM rt.cycle_receipts "
                    "WHERE receipt_kind='OPTIONS_INPUT_READY'"
                )
                assert unit.cursor.fetchone()[0] == 1
                unit.cursor.execute(
                    "SELECT count(*) FROM rt.job_barriers "
                    "WHERE barrier_kind='OPTIONS_INPUT_READY' "
                    "AND state='COMPLETE'"
                )
                assert unit.cursor.fetchone()[0] == 1
                effects_after = _prohibited_effect_counts(unit.cursor)
                assert effects_after["rt.cycle_receipts"] == (
                    effects_before["rt.cycle_receipts"] + 1
                )
                assert {
                    key: value
                    for key, value in effects_after.items()
                    if key != "rt.cycle_receipts"
                } == {
                    key: value
                    for key, value in effects_before.items()
                    if key != "rt.cycle_receipts"
                }

            divergent = replace(
                frozen,
                expected_closure_sha256="0" * 64,
            )
            with pytest.raises(
                ValueError,
                match="PLAN identity changed",
            ):
                PostgresAdvancedOptionsCanonicalRuntimePort(
                    provider,
                    replace(scope, input_ready_plan=divergent),
                )
            wrong_build = build_advanced_options_input_ready_plan(
                enrollment=enrollment,
                producer_plan=producer_plan,
                reference_producer_plan=reference_plan,
                rolling_producer_plan=rolling_plan,
                native_input_plan=native_frozen,
                mark_producer_plan=mark_plan,
                native_executor_membership_sha256=(
                    native_preflight.executor_membership_sha256
                ),
                service_plan_id=service_plan_id,
                window_id=window_id,
                cutoff=cutoff,
                producer_build_id="advanced-options-input-ready-wrong",
                producer_build_sha256=_digest("wrong-build"),
            )
            wrong_port = PostgresAdvancedOptionsCanonicalRuntimePort(
                provider,
                replace(scope, input_ready_plan=wrong_build),
            )
            with pytest.raises(
                ValueError,
                match="durable OPTIONS_INPUT_READY receipt identity changed",
            ):
                wrong_port.preflight_input_ready()
            with pytest.raises(errors.InsufficientPrivilege):
                with provider.transaction(
                    API_ROLE,
                    read_only=True,
                ) as unit:
                    unit.cursor.execute(
                        "SELECT count(*) FROM rt.job_barrier_members"
                    )
            categories = _disposable_audit(
                database_pool,
                enrollment,
                cutoff,
            )
            assert set(categories) == set(audit.REQUIRED_CATEGORIES)
            assert all(
                row["status"] in {"AVAILABLE", "COMPLETE_ZERO"}
                for name, row in categories.items()
                if name not in {
                    "o1_equity_context",
                    "o3_equity_context",
                }
            ), categories
            assert categories["o1_equity_context"]["status"] == "AVAILABLE"
            assert categories["o3_equity_context"]["status"] == "AVAILABLE"
            assert audit._next_actionable_blocker(categories) is None
    finally:
        if roles_owned:
            with closing(_connect(_settings(), "postgres")) as connection_value:
                connection_value.autocommit = True
                drop_disposable_roles_v2(connection_value, roles)
                with connection_value.cursor() as database_cursor:
                    database_cursor.execute(
                        "SELECT count(*) FROM pg_roles WHERE rolname=ANY(%s)",
                        (list(roles.all_names),),
                    )
                    assert database_cursor.fetchone()[0] == 0
