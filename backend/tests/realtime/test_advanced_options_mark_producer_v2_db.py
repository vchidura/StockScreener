from contextlib import closing
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg2 import errors

from option_flow_support import advanced_policy
from options.config import load_valuation_policy
from realtime.advanced_options_control_plane import (
    persist_advanced_options_enrollment_cursor,
)
from realtime.advanced_options_mark_producer import (
    AdvancedOptionsMarkStatus,
    PostgresAdvancedOptionsMarkProducer,
    build_advanced_options_mark_producer_plan,
    freeze_advanced_options_mark_producer_plan,
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
from realtime.security_v2 import (
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from scripts import audit_advanced_options_input_readiness as audit
from tests.realtime.test_advanced_options_native_loader_v2_db import (
    _seed_native_equity_inputs,
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
    return canonical_sha256(("owp4e", value))


def _valuation_policy():
    return load_valuation_policy(
        Path(__file__).parents[2]
        / "options"
        / "policies"
        / "valuation_advanced_nbbo_v1.json"
    )


def test_owp4e_mark_producer_and_disposable_readiness_audit(monkeypatch):
    suffix = uuid4().hex[:8]
    database = f"test_realtime_v2_owp4e_{suffix}"
    roles = build_role_names_v2(f"test_rt_owp4e_{suffix}")
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
            )
            control = enrollment.control_plane
            cutoff = control.planned_at
            scope = _contract_scope(enrollment, flow_policy)
            reference_plan = build_advanced_options_reference_producer_plan(
                enrollment=enrollment,
                contract_scope=scope,
                reference_generation=1,
                known_at=control.planned_at,
                valid_until=control.expires_at,
                discovery_scheduled_cycle=control.planned_at,
                prior_oi_session=control.session_date - timedelta(days=1),
                treasury_maximum_age_seconds=3_600,
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
                    known_at=cutoff,
                )
                _seed_native_equity_inputs(
                    database_cursor,
                    enrollment,
                    source_flow,
                    cutoff=cutoff,
                )
            rolling_plan = build_advanced_options_rolling_producer_plan(
                enrollment=enrollment,
                contract_scope=scope,
                reference_state=reference,
                source_flow=source_flow,
                owner_epoch=owner_epoch,
                committed_offset=committed_offset,
                shard_ids=(0,),
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
            plan = build_advanced_options_mark_producer_plan(
                enrollment=enrollment,
                reference_state=reference,
                rolling_state=rolling,
                source_flow=source_flow,
                cutoff=cutoff,
                producer_build_id="advanced-options-nbbo-mark-v1",
                producer_build_sha256=_digest("producer-build"),
                valuation_policy=valuation_policy,
            )
            producer = PostgresAdvancedOptionsMarkProducer(
                provider,
                plan,
                maximum_attempts=2,
                retry_initial_seconds=0,
                wait=lambda _seconds: None,
            )
            preflight = producer.preflight()
            assert preflight.status is AdvancedOptionsMarkStatus.READY
            assert len(preflight.marks) == 1
            mark = preflight.marks[0]
            assert mark.midpoint == (mark.bid + mark.ask) / Decimal(2)
            assert mark.bid_size == 100
            assert mark.ask_size == 120
            assert mark.bid_exchange == 1
            assert mark.ask_exchange == 2
            assert mark.quote_payload_sha256
            assert mark.source_flow_sha256 == source_flow.source_flow_sha256
            frozen = freeze_advanced_options_mark_producer_plan(
                plan,
                preflight,
            )
            producer = PostgresAdvancedOptionsMarkProducer(
                provider,
                frozen,
                maximum_attempts=2,
                retry_initial_seconds=0,
                wait=lambda _seconds: None,
            )
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                effects_before = _prohibited_effect_counts(unit.cursor)
            original_assemble = producer._assemble
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

            monkeypatch.setattr(producer, "_assemble", fail_once)
            first = producer.produce()
            second = producer.produce()
            assert attempts == 3
            assert first == second == preflight.marks
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                unit.cursor.execute(
                    "SELECT count(*) FROM rt.option_mark_evidence "
                    "WHERE source_kind='ADVANCED_NBBO'"
                )
                assert unit.cursor.fetchone()[0] == 1
                assert _prohibited_effect_counts(unit.cursor) == effects_before
            option_instrument_id = (
                enrollment.activity_selection.contracts[0].instrument_id
            )
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                database_cursor.execute(
                    "UPDATE rt.quote_current SET bid=bid + 0.01 "
                    "WHERE instrument_id=%s",
                    (option_instrument_id,),
                )
            with pytest.raises(
                ValueError,
                match="mark replay diverged",
            ):
                producer.produce()
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                database_cursor.execute(
                    "UPDATE rt.quote_current SET bid=bid - 0.01 "
                    "WHERE instrument_id=%s",
                    (option_instrument_id,),
                )
                database_cursor.execute(
                    "UPDATE rt.quote_current SET quality='CROSSED',"
                    "bid=ask + 1 WHERE instrument_id=%s",
                    (option_instrument_id,),
                )
            blocked = PostgresAdvancedOptionsMarkProducer(
                provider,
                plan,
            ).preflight()
            assert blocked.status is AdvancedOptionsMarkStatus.BLOCKED
            assert "OPTION_NBBO_CROSSED_OR_NONPOSITIVE" in (
                blocked.reason_codes
            )
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                database_cursor.execute(
                    "UPDATE rt.quote_current SET quality='GOOD',"
                    "bid=2.00,ask=2.10 WHERE instrument_id=%s",
                    (option_instrument_id,),
                )
            assert PostgresAdvancedOptionsMarkProducer(
                provider,
                plan,
            ).preflight().status is AdvancedOptionsMarkStatus.READY
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
            )
            assert categories["coherent_nbbo_marks"]["status"] == "AVAILABLE", (
                categories["coherent_nbbo_marks"]
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
