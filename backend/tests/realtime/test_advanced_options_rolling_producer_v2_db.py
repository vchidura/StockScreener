from contextlib import closing
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from psycopg2 import errors

from option_flow_support import advanced_policy
from realtime.advanced_options_control_plane import (
    CANONICAL_MARKET_STATE_OWNER,
    persist_advanced_options_enrollment_cursor,
)
from realtime.advanced_options_reference_producer import (
    PostgresAdvancedOptionsReferenceProducer,
    build_advanced_options_reference_producer_plan,
)
from realtime.advanced_options_rolling_producer import (
    PostgresAdvancedOptionsRollingProducer,
    RollingProducerStatus,
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
    CANONICAL_OPTION_FLOW_OWNER,
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
from tests.realtime.test_advanced_options_reference_producer_v2_db import (
    _contract_scope,
    _disposable_audit,
    _seed_reference_inputs,
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
    return canonical_sha256(("owp4c", value))


def _seed_rolling_inputs(
    database_cursor,
    enrollment,
    source_flow,
    *,
    owner_epoch,
    committed_offset,
    shard_map_sha256,
    known_at,
):
    control = enrollment.control_plane
    session_id = source_flow.stream_session_ids[0]
    database_cursor.execute(
        "INSERT INTO rt.stream_sessions ("
        "session_id,provider,feed,manifest_id,owner_id,owner_epoch,"
        "started_at,authenticated_at,subscribed_at,ended_at,status,"
        "journal_generation,normalizer_version,authorization_reference,"
        "budget_scope_sha256,connection_attempt,connection_ceiling,"
        "decoded_byte_ceiling,received_frame_count,received_byte_count,"
        "admitted_market_message_count,admitted_market_byte_count,"
        "last_raw_offset,last_event_offset,event_time_watermark,heartbeat_at"
        ") VALUES (%s,'massive','OPTIONS',%s,%s,%s,%s,%s,%s,%s,'STOPPED',"
        "%s,'advanced-options-normalizer-v1','owp4c-disposable',%s,"
        "1,1,1024,11,512,11,512,%s,%s,%s,%s)",
        (
            str(session_id),
            str(control.manifest_id),
            str(uuid4()),
            owner_epoch,
            known_at - timedelta(minutes=5),
            known_at - timedelta(minutes=4),
            known_at - timedelta(minutes=3),
            known_at,
            str(source_flow.journal_generation),
            _digest("budget"),
            committed_offset,
            committed_offset,
            known_at - timedelta(milliseconds=80),
            known_at - timedelta(milliseconds=20),
        ),
    )
    selected = enrollment.activity_selection.contracts[0]
    database_cursor.execute(
        "SELECT instrument.instrument_id FROM rt.subscription_members AS member "
        "JOIN rt.instruments AS instrument "
        "ON instrument.instrument_id=member.instrument_id "
        "WHERE member.manifest_id=%s AND instrument.security_id=%s",
        (
            str(enrollment.equity_plan.manifest_id),
            str(dict(enrollment.equity_plan.security_ids)["SPY"]),
        ),
    )
    equity_instrument_id = int(database_cursor.fetchone()[0])
    instruments = (
        (
            selected.instrument_id,
            known_at - timedelta(milliseconds=90),
            known_at - timedelta(milliseconds=85),
            Decimal("2.00"),
            Decimal("2.10"),
            Decimal("2.05"),
            "option-trade-v1",
        ),
        (
            equity_instrument_id,
            known_at - timedelta(milliseconds=100),
            known_at - timedelta(milliseconds=80),
            Decimal("499.90"),
            Decimal("500.10"),
            Decimal("500.00"),
            "equity-trade-v1",
        ),
    )
    for (
        instrument_id,
        quote_at,
        trade_at,
        bid,
        ask,
        price,
        semantics,
    ) in instruments:
        database_cursor.execute(
            "INSERT INTO rt.quote_current ("
            "provider,instrument_id,source_session,source_sequence,"
            "event_time_raw,event_time_unit,event_at,received_at,bid,ask,"
            "bid_size,ask_size,bid_exchange,ask_exchange,quality,"
            "reason_codes,session_id,journal_generation,shard_id,"
            "source_offset,payload_sha256) VALUES ("
            "'massive',%s,%s,10,10,'ms',%s,%s,%s,%s,100,120,1,2,"
            "'GOOD',ARRAY[]::text[],%s,%s,0,10,%s)",
            (
                instrument_id,
                control.session_date,
                quote_at,
                quote_at + timedelta(milliseconds=5),
                bid,
                ask,
                str(session_id),
                str(source_flow.journal_generation),
                _digest(("quote", instrument_id)),
            ),
        )
        database_cursor.execute(
            "INSERT INTO rt.trade_current ("
            "provider,instrument_id,source_session,source_sequence,"
            "event_time_raw,event_time_unit,event_at,received_at,price,size,"
            "exchange,conditions,eligibility,eligibility_policy_version,"
            "reason_codes,session_id,journal_generation,shard_id,"
            "source_offset,payload_sha256) VALUES ("
            "'massive',%s,%s,11,11,'ms',%s,%s,%s,10,1,"
            "ARRAY[]::integer[],'INCLUDED',%s,ARRAY[]::text[],%s,%s,0,11,%s)",
            (
                instrument_id,
                control.session_date,
                trade_at,
                trade_at + timedelta(milliseconds=5),
                price,
                semantics,
                str(session_id),
                str(source_flow.journal_generation),
                _digest(("trade", instrument_id)),
            ),
        )
    for owner in (
        CANONICAL_MARKET_STATE_OWNER,
        CANONICAL_OPTION_FLOW_OWNER,
    ):
        database_cursor.execute(
            "INSERT INTO rt.consumer_checkpoints ("
            "consumer_name,cohort_id,journal_generation,shard_id,"
            "shard_map_sha256,owner_epoch,committed_offset,checkpoint_id,"
            "state_codec,state_hash,state_payload,committed_at) VALUES ("
            "%s,%s,%s,0,%s,%s,%s,%s,'owp4c-disposable-v1',%s,%s,%s)",
            (
                owner,
                str(control.cohort_id),
                str(source_flow.journal_generation),
                shard_map_sha256,
                owner_epoch,
                committed_offset,
                str(uuid4()),
                _digest(("checkpoint", owner)),
                b"owp4c",
                known_at,
            ),
        )
    database_cursor.execute(
        "INSERT INTO rt.option_volume_checkpoints ("
        "cohort_id,session_date,contract_ticker,cumulative_volume,"
        "last_source_seq,last_event_time,baseline_kind,snapshot_volume,"
        "snapshot_watermark_seq) VALUES (%s,%s,%s,0,0,NULL,"
        "'SESSION_COMPLETE',NULL,NULL)",
        (
            str(control.cohort_id),
            control.session_date,
            selected.contract_ticker,
        ),
    )
    return equity_instrument_id


def test_owp4c_rolling_producer_and_disposable_readiness_audit(monkeypatch):
    suffix = uuid4().hex[:8]
    database = f"test_realtime_v2_owp4c_{suffix}"
    roles = build_role_names_v2(f"test_rt_owp4c_{suffix}")
    roles_owned = False
    try:
        with disposable_v2_database(database) as database_pool:
            flow_policy = advanced_policy()
            enrollment = _owp4_enrollment(
                database_pool,
                flow_policy_sha256=flow_policy.sha256,
                security_free_float=Decimal("1000000000"),
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
            owner_epoch = 3
            committed_offset = 11
            shard_map_sha256 = _digest("shard-map")
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
            plan = build_advanced_options_rolling_producer_plan(
                enrollment=enrollment,
                contract_scope=scope,
                reference_state=reference,
                source_flow=source_flow,
                owner_epoch=owner_epoch,
                committed_offset=committed_offset,
                shard_ids=(0,),
                market_state_shard_map_sha256=shard_map_sha256,
                flow_shard_map_sha256=shard_map_sha256,
                known_at=control.planned_at,
                valid_until=control.expires_at,
            )
            producer = PostgresAdvancedOptionsRollingProducer(
                provider,
                plan,
                maximum_attempts=2,
                retry_initial_seconds=0,
                wait=lambda _seconds: None,
            )
            blocked = producer.preflight()
            assert blocked.status is RollingProducerStatus.BLOCKED
            assert {
                "SOURCE_SESSION_MEMBERSHIP_INCOMPLETE",
                "MARKET_STATE_CHECKPOINT_INCOMPLETE",
                "FLOW_CHECKPOINT_INCOMPLETE",
                "CANONICAL_CURRENT_HEADS_INCOMPLETE",
                "OPTION_VOLUME_CHECKPOINT_MEMBERSHIP_INCOMPLETE",
            }.issubset(blocked.reason_codes)
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                equity_instrument_id = _seed_rolling_inputs(
                    database_cursor,
                    enrollment,
                    source_flow,
                    owner_epoch=owner_epoch,
                    committed_offset=committed_offset,
                    shard_map_sha256=shard_map_sha256,
                    known_at=control.planned_at,
                )
            preflight = producer.preflight()
            assert preflight.status is RollingProducerStatus.READY
            assert preflight.state is not None
            assert not preflight.reason_codes
            assert not preflight.state.order_windows.members
            assert preflight.state.producer_binding is not None
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
            state = producer.produce()
            assert attempts == 2
            assert producer.produce() == state
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                assert _prohibited_effect_counts(unit.cursor) == effects_before
                unit.cursor.execute(
                    "SELECT count(*) FROM "
                    "rt.advanced_options_rolling_checkpoints"
                )
                assert unit.cursor.fetchone()[0] == 1
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                database_cursor.execute(
                    "UPDATE rt.quote_current SET payload_sha256=%s "
                    "WHERE instrument_id=%s",
                    (_digest("divergent-head"), equity_instrument_id),
                )
            with pytest.raises(
                ValueError,
                match="persisted rolling checkpoint conflicts",
            ):
                producer.produce()
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                database_cursor.execute(
                    "UPDATE rt.quote_current SET payload_sha256=%s "
                    "WHERE instrument_id=%s",
                    (
                        _digest(("quote", equity_instrument_id)),
                        equity_instrument_id,
                    ),
                )
                database_cursor.execute(
                    "UPDATE rt.consumer_checkpoints "
                    "SET committed_offset=%s WHERE consumer_name=%s",
                    (committed_offset + 1, CANONICAL_OPTION_FLOW_OWNER),
                )
            assert (
                "CONSUMER_CHECKPOINT_OFFSET_MISMATCH"
                in producer.preflight().reason_codes
            )
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                database_cursor.execute(
                    "UPDATE rt.consumer_checkpoints "
                    "SET committed_offset=%s WHERE consumer_name=%s",
                    (committed_offset - 1, CANONICAL_OPTION_FLOW_OWNER),
                )
            assert (
                "CONSUMER_CHECKPOINT_OFFSET_MISMATCH"
                in producer.preflight().reason_codes
            )
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                database_cursor.execute(
                    "UPDATE rt.consumer_checkpoints "
                    "SET committed_offset=%s WHERE consumer_name=%s",
                    (committed_offset, CANONICAL_OPTION_FLOW_OWNER),
                )
                database_cursor.execute(
                    "UPDATE rt.option_volume_checkpoints "
                    "SET baseline_kind='INCOMPLETE' WHERE cohort_id=%s",
                    (str(control.cohort_id),),
                )
            assert (
                "OPTION_VOLUME_CHECKPOINT_INCOMPLETE"
                in producer.preflight().reason_codes
            )
            gap_id = uuid4()
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                database_cursor.execute(
                    "UPDATE rt.option_volume_checkpoints "
                    "SET baseline_kind='SESSION_COMPLETE' WHERE cohort_id=%s",
                    (str(control.cohort_id),),
                )
                database_cursor.execute(
                    "INSERT INTO rt.stream_gaps ("
                    "gap_id,session_id,gap_start,journal_generation,shard_id,"
                    "last_durable_offset,reason,recovery_status) VALUES ("
                    "%s,%s,%s,%s,0,%s,'DISPOSABLE_GAP','UNRESOLVED')",
                    (
                        str(gap_id),
                        str(source_flow.stream_session_ids[0]),
                        control.planned_at,
                        str(source_flow.journal_generation),
                        committed_offset,
                    ),
                )
            assert (
                "UNRESOLVED_OR_WARMING_STREAM_GAPS"
                in producer.preflight().reason_codes
            )
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                database_cursor.execute(
                    "DELETE FROM rt.stream_gaps WHERE gap_id=%s",
                    (str(gap_id),),
                )
            assert producer.preflight().status is RollingProducerStatus.READY
            with pytest.raises(errors.InsufficientPrivilege):
                with provider.transaction(
                    API_ROLE,
                    read_only=True,
                ) as unit:
                    unit.cursor.execute(
                        "SELECT count(*) FROM "
                        "rt.advanced_options_rolling_checkpoints"
                    )
            cutoff = control.receipts.options_input_ready.spec.cutoff
            categories = _disposable_audit(
                database_pool,
                enrollment,
                cutoff,
            )
            assert categories["rolling_state_generation"]["status"] == (
                "AVAILABLE"
            )
            assert categories["market_state_checkpoint"]["status"] == (
                "AVAILABLE"
            )
            assert categories["flow_checkpoint"]["status"] == "AVAILABLE"
            assert categories["unresolved_gaps"]["status"] == "COMPLETE_ZERO"
            assert categories["source_clocks"]["status"] == "AVAILABLE"
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
