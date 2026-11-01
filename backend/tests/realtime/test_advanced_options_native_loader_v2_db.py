from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from psycopg2 import errors
from psycopg2.extras import Json

from option_flow_support import advanced_policy
from realtime.advanced_options_control_plane import (
    persist_advanced_options_enrollment_cursor,
)
from realtime.advanced_options_equity_input import (
    initial_advanced_options_equity_context_requirements,
)
from realtime.advanced_options_native_loader import (
    NativeInputPlanStatus,
    PostgresAdvancedOptionsNativeInputLoader,
    build_advanced_options_native_input_plan,
    freeze_advanced_options_native_input_plan,
)
from realtime.advanced_options_producers import (
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
from realtime.preflight import DetectorFamily
from realtime.orchestration_repository import CycleReceiptSpec
from realtime.security_v2 import (
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from scripts import audit_advanced_options_input_readiness as audit
from tests.realtime.test_advanced_options_producers import (
    _clocks,
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
UTC = timezone.utc


def _digest(value) -> str:
    return canonical_sha256(("owp4d", value))


def _seed_bar(
    database_cursor,
    *,
    cohort_id,
    security_id,
    instrument_id,
    generation,
    stream_session_id,
    interval,
    session_date,
    bar_start,
    bar_end,
    source_offset,
    availability_mode="LIVE_OBSERVED",
):
    revision_id = uuid4()
    observed_at = bar_end
    payload_sha256 = _digest(
        ("bar", interval, session_date, source_offset)
    )
    database_cursor.execute(
        "INSERT INTO rt.bar_catalog ("
        "bar_revision_id,cohort_id,provider_tier,instrument_id,"
        "interval,bar_start,bar_end,observed_at,session_date) VALUES ("
        "%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,%s,%s)",
        (
            str(revision_id),
            str(cohort_id),
            instrument_id,
            interval,
            bar_start,
            bar_end,
            observed_at,
            session_date,
        ),
    )
    close = Decimal("500") + Decimal(source_offset) / Decimal("10")
    database_cursor.execute(
        "INSERT INTO rt.bar_revisions ("
        "bar_revision_id,cohort_id,provider_tier,security_id,"
        "instrument_id,ticker,interval,session_date,session_scope,"
        "adjustment_basis,bar_start,bar_end,open_price,high_price,"
        "low_price,close_price,volume,vwap,transaction_count,"
        "source_kind,availability_mode,is_final,observed_at,"
        "replay_available_at,journal_generation,stream_session_id,"
        "source_offset,payload_sha256,created_at) VALUES ("
        "%s,%s,'MASSIVE_ADVANCED',%s,%s,'SPY',%s,%s,'RTH',"
        "'RAW_UNADJUSTED',%s,%s,%s,%s,%s,%s,1000000,%s,100,"
        "'REALTIME_STREAM',%s,true,%s,%s,%s,%s,%s,%s,%s)",
        (
            str(revision_id),
            str(cohort_id),
            str(security_id),
            instrument_id,
            interval,
            session_date,
            bar_start,
            bar_end,
            close,
            close + Decimal("1"),
            close - Decimal("1"),
            close,
            close,
            availability_mode,
            observed_at,
            observed_at,
            str(generation),
            str(stream_session_id),
            source_offset,
            payload_sha256,
            observed_at,
        ),
    )
    database_cursor.execute(
        "INSERT INTO rt.bar_current ("
        "cohort_id,provider_tier,instrument_id,interval,bar_start,"
        "session_scope,adjustment_basis,source_kind,bar_revision_id,"
        "bar_end,observed_at,updated_at) VALUES ("
        "%s,'MASSIVE_ADVANCED',%s,%s,%s,'RTH','RAW_UNADJUSTED',"
        "'REALTIME_STREAM',%s,%s,%s,%s)",
        (
            str(cohort_id),
            instrument_id,
            interval,
            bar_start,
            str(revision_id),
            bar_end,
            observed_at,
            observed_at,
        ),
    )
    return revision_id, payload_sha256, bar_end, observed_at


def _seed_native_equity_inputs(
    database_cursor,
    enrollment,
    source_flow,
    *,
    equity_cutoff,
):
    equity = enrollment.equity_plan
    security_id = dict(equity.security_ids)["SPY"]
    database_cursor.execute(
        "SELECT instrument.instrument_id FROM rt.subscription_members AS member "
        "JOIN rt.instruments AS instrument "
        "ON instrument.instrument_id=member.instrument_id "
        "WHERE member.manifest_id=%s AND instrument.security_id=%s",
        (str(equity.manifest_id), str(security_id)),
    )
    instrument_id = int(database_cursor.fetchone()[0])
    database_cursor.execute(
        "SELECT rt.ensure_partitions('rt.bar_revisions',%s)",
        (equity_cutoff.date(),),
    )
    offset = 1
    for minutes in (15, 10, 5):
        bar_end = equity_cutoff - timedelta(minutes=minutes - 5)
        _seed_bar(
            database_cursor,
            cohort_id=equity.cohort_id,
            security_id=security_id,
            instrument_id=instrument_id,
            generation=source_flow.journal_generation,
            stream_session_id=source_flow.stream_session_ids[0],
            interval="5m",
            session_date=equity_cutoff.date(),
            bar_start=bar_end - timedelta(minutes=5),
            bar_end=bar_end,
            source_offset=offset,
        )
        offset += 1
    service_plan_id = uuid4()
    database_cursor.execute(
        "INSERT INTO rt.service_plans ("
        "plan_id,cohort_id,session_date,stage_catalog_version,"
        "enabled_stages,config_sha256,deadline,scope_reference"
        ") VALUES (%s,%s,%s,'stages_v3',%s,%s,%s,%s)",
        (
            str(service_plan_id),
            str(equity.cohort_id),
            equity_cutoff.date(),
            [
                "EQUITY_INPUT_GATE",
                "EQUITY_MATERIALIZE",
                "EQUITY_PROJECTION",
            ],
            _digest("equity-service-plan"),
            equity_cutoff + timedelta(hours=8),
            "owp4d disposable Equity input",
        ),
    )
    window_id = uuid4()
    database_cursor.execute(
        "INSERT INTO rt.window_runs ("
        "window_id,plan_id,cohort_id,provider_tier,window_key,"
        "release_at,cutoff,deadline,state) VALUES ("
        "%s,%s,%s,'MASSIVE_ADVANCED','owp4d-equity-window',"
        "%s,%s,%s,'COMPLETE')",
        (
            str(window_id),
            str(service_plan_id),
            str(equity.cohort_id),
            equity_cutoff,
            equity_cutoff,
            equity_cutoff + timedelta(minutes=5),
        ),
    )
    receipt_id = uuid4()
    receipt = CycleReceiptSpec(
        "INPUT_READY",
        equity_cutoff,
        1,
        equity.universe.sha256,
        _digest("equity-input"),
        "advanced-equity-owp4d-v1",
    )
    database_cursor.execute(
        "INSERT INTO rt.cycle_receipts ("
        "receipt_id,window_id,cohort_id,receipt_kind,cutoff,"
        "reference_generation,universe_sha256,input_sha256,build_id"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            str(receipt_id),
            str(window_id),
            str(equity.cohort_id),
            receipt.receipt_kind,
            receipt.cutoff,
            receipt.reference_generation,
            receipt.universe_sha256,
            receipt.input_sha256,
            receipt.build_id,
        ),
    )
    return (
        receipt_id,
        window_id,
        receipt,
    )


def test_owp4d_native_loader_and_disposable_readiness_audit(monkeypatch):
    suffix = uuid4().hex[:8]
    database = f"test_realtime_v2_owp4d_{suffix}"
    roles = build_role_names_v2(f"test_rt_owp4d_{suffix}")
    roles_owned = False
    try:
        with disposable_v2_database(database) as database_pool:
            flow_policy = advanced_policy()
            enrollment = _owp4_enrollment(
                database_pool,
                flow_policy_sha256=flow_policy.sha256,
                security_free_float=Decimal("1000000000"),
                provider_request_ceiling=2,
                provider_decoded_byte_ceiling=100_000_000,
            )
            scope = _contract_scope(enrollment, flow_policy)
            control = enrollment.control_plane
            reference_plan = build_advanced_options_reference_producer_plan(
                enrollment=enrollment,
                contract_scope=scope,
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
                contract_scope_sha256=scope.sha256,
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
                    scope,
                )
                persist_option_source_flow_cursor(
                    database_cursor,
                    source_flow,
                    source_cohort_id=control.cohort_id,
                    contract_scope=scope,
                )
                _seed_reference_inputs(
                    database_cursor,
                    enrollment,
                    reference_plan,
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
                    known_at=control.planned_at,
                )
            rolling_plan = build_advanced_options_rolling_producer_plan(
                enrollment=enrollment,
                contract_scope=scope,
                reference_state=reference,
                source_flow=source_flow,
                owner_epoch=owner_epoch,
                market_state_committed_offset=committed_offset,
                flow_committed_offset=committed_offset,
                market_state_shard_ids=(0,),
                flow_shard_ids=(0,),
                market_state_shard_map_sha256=shard_map_sha256,
                flow_shard_map_sha256=shard_map_sha256,
                known_at=control.planned_at,
                valid_until=control.expires_at,
            )
            rolling = PostgresAdvancedOptionsRollingProducer(
                provider,
                rolling_plan,
                retry_initial_seconds=0,
            ).produce()
            cutoff = control.receipts.options_input_ready.spec.cutoff
            equity_cutoff = cutoff - timedelta(minutes=2)
            history_cohort_id = uuid4()
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
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
                        _digest("history-cohort"),
                        str(enrollment.equity_plan.cohort_id),
                    ),
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
            producer_plan = build_advanced_options_producer_plan(
                control_plane=control,
                discovery_scope=_discovery_scope(control),
                provider_budget=control.provider_budget,
                clocks=_clocks(control),
                endpoint_response_byte_limits=_limits(),
            )
            plan = build_advanced_options_native_input_plan(
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
            loader = PostgresAdvancedOptionsNativeInputLoader(
                provider,
                plan,
                maximum_attempts=2,
                retry_initial_seconds=0,
                wait=lambda _seconds: None,
            )
            preflight = loader.preflight()
            assert preflight.status is NativeInputPlanStatus.READY
            assert not preflight.bundle.o1.invocations
            assert preflight.bundle.o3.blocked_reason_codes == (
                "CANONICAL_MARKS_UNAVAILABLE",
            )
            assert set(preflight.bundle.executors()) == {
                DetectorFamily.O1,
                DetectorFamily.O3,
            }
            missing_context_plan = build_advanced_options_native_input_plan(
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
                    replace(
                        initial_advanced_options_equity_context_requirements()[
                            0
                        ],
                        session_count=4,
                    ),
                ),
            )
            missing_context = PostgresAdvancedOptionsNativeInputLoader(
                provider,
                missing_context_plan,
            ).preflight()
            assert missing_context.status is NativeInputPlanStatus.READY
            assert not missing_context.bundle.o1.blocked_reason_codes
            assert (
                "O3_SPY_E_CONTEXT_INCOMPLETE"
                in missing_context.bundle.o3.blocked_reason_codes
            )
            frozen = freeze_advanced_options_native_input_plan(
                plan,
                preflight,
            )
            loader = PostgresAdvancedOptionsNativeInputLoader(
                provider,
                frozen,
                maximum_attempts=2,
                retry_initial_seconds=0,
                wait=lambda _seconds: None,
            )
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                effects_before = _prohibited_effect_counts(unit.cursor)
            original_assemble = loader._assemble
            attempts = 0

            def fail_once(unit):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    unit.cursor.execute(
                        "DO $block$ BEGIN RAISE EXCEPTION 'retry' "
                        "USING ERRCODE='40001'; END $block$"
                    )
                return original_assemble(unit)

            monkeypatch.setattr(loader, "_assemble", fail_once)
            first = loader.load()
            second = loader.load()
            assert attempts == 3
            assert first == second
            assert first.input_sha256 == preflight.input_sha256
            assert first.executor_membership_sha256 == (
                preflight.executor_membership_sha256
            )
            divergent = build_advanced_options_native_input_plan(
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
                expected_input_sha256="0" * 64,
            )
            with pytest.raises(
                ValueError,
                match="native input replay diverged",
            ):
                PostgresAdvancedOptionsNativeInputLoader(
                    provider,
                    divergent,
                ).preflight()
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                assert _prohibited_effect_counts(unit.cursor) == effects_before
            with pytest.raises(errors.InsufficientPrivilege):
                with provider.transaction(
                    API_ROLE,
                    read_only=True,
                ) as unit:
                    unit.cursor.execute(
                        "SELECT count(*) FROM "
                        "rt.advanced_options_rolling_checkpoints"
                    )
            categories = _disposable_audit(
                database_pool,
                enrollment,
                cutoff,
                equity_cutoff=equity_cutoff,
                history_source_cohort_id=history_cohort_id,
            )
            assert categories["o1_equity_context"]["status"] == "AVAILABLE", (
                categories["o1_equity_context"]
            )
            assert categories["o3_equity_context"]["status"] == "AVAILABLE", (
                categories["o3_equity_context"]
            )
            assert audit._next_actionable_blocker(categories)[
                "reason_code"
            ] == "OPTIONS_INPUT_READY_RECEIPT_ABSENT"
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
