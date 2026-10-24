from contextlib import closing
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from psycopg2 import errors
from psycopg2.extras import Json, RealDictCursor

from option_flow_support import advanced_policy
from options.calendar import OptionExchangeCalendar
from realtime.advanced_options_control_plane import (
    persist_advanced_options_enrollment_cursor,
)
from realtime.advanced_options_flow_policy import (
    AdvancedOptionsContractScope,
    AdvancedOptionsContractScopeEntry,
)
from realtime.advanced_options_reference_producer import (
    PostgresAdvancedOptionsReferenceProducer,
    ReferenceProducerStatus,
    build_advanced_options_reference_producer_plan,
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
    persist_option_contract_scope_cursor,
    persist_option_flow_policy_cursor,
)
from realtime.security_v2 import (
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from scripts import audit_advanced_options_input_readiness as audit
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
    return canonical_sha256(("owp4b", value))


def _contract_scope(enrollment, policy):
    control = enrollment.control_plane
    selected = enrollment.activity_selection.contracts
    entries = tuple(
        AdvancedOptionsContractScopeEntry(
            contract_ticker=row.contract_ticker,
            underlyer=row.underlyer,
            expiration=row.expiration_date,
            universe_tier=policy.tier_for(row.underlyer),
            dte_lane=policy.lane_for(
                (row.expiration_date - control.session_date).days
            ),
        )
        for row in selected
    )
    return AdvancedOptionsContractScope(
        version="advanced-options-contract-scope-v1",
        session_date=control.session_date,
        manifest_sha256=control.manifest_sha256,
        slot_sha256=control.slot_set_sha256,
        flow_policy_sha256=policy.sha256,
        contracts=entries,
    )


def _seed_reference_inputs(
    database_cursor,
    enrollment,
    plan,
    *,
    history_source_cohort_id=None,
    include_optional_metrics=True,
):
    control = enrollment.control_plane
    session = control.session_date
    known = plan.known_at
    history_cohort_id = (
        history_source_cohort_id
        or enrollment.equity_plan.cohort_id
    )
    ticker = "SPY"
    security_id = dict(enrollment.equity_plan.security_ids)[ticker]
    database_cursor.execute(
        "SELECT instrument.instrument_id FROM rt.subscription_members AS member "
        "JOIN rt.instruments AS instrument "
        "ON instrument.instrument_id=member.instrument_id "
        "WHERE member.manifest_id=%s AND instrument.security_id=%s",
        (str(enrollment.equity_plan.manifest_id), str(security_id)),
    )
    equity_instrument_id = int(database_cursor.fetchone()[0])
    if include_optional_metrics:
        metrics = {"short_interest_percent": "0.08"}
        raw_report = {"ticker": ticker, "metrics": metrics}
        database_cursor.execute(
            "INSERT INTO ref.fundamental_reports ("
            "report_id,security_id,timeframe,period_end,availability_time,"
            "observed_at,source,source_key,metrics,raw_payload,payload_sha256,"
            "quality_codes) VALUES (%s,%s,'quarterly',%s,%s,%s,"
            "'canonical-fundamentals','SPY:SHORT_INTEREST',%s,%s,%s,"
            "ARRAY[]::text[])",
            (
                str(uuid4()),
                str(security_id),
                session - timedelta(days=1),
                known - timedelta(days=2),
                known - timedelta(days=1),
                Json(metrics),
                Json(raw_report),
                canonical_sha256(raw_report),
            ),
        )
    for tenor, rate in ((30, Decimal("0.0400")), (60, Decimal("0.0410"))):
        database_cursor.execute(
            "INSERT INTO ref.risk_free_rate_observations ("
            "observation_id,rate_date,tenor_days,annual_rate,source,"
            "source_key,source_observed_at,received_at,payload_sha256"
            ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                str(uuid4()),
                session - timedelta(days=1),
                tenor,
                rate,
                "us_treasury_par_yield_v1",
                f"{(session - timedelta(days=1)).isoformat()}:{tenor}d",
                known - timedelta(minutes=25),
                known - timedelta(minutes=20),
                _digest(("treasury", tenor)),
            ),
        )
    selected = enrollment.activity_selection.contracts[0]
    database_cursor.execute(
        "INSERT INTO ref.oi_baselines ("
        "baseline_id,contract_id,settlement_session,open_interest,source,"
        "date_basis,observed_session,observed_at,received_at,"
        "payload_sha256,supersedes_baseline_id) VALUES ("
        "%s,%s,%s,2500,'massive',"
        "'PROVIDER_CHAIN_SNAPSHOT_PRIOR_SESSION',%s,%s,%s,%s,NULL)",
        (
            str(uuid4()),
            selected.contract_id,
            plan.prior_oi_session,
            session,
            known - timedelta(minutes=15),
            known - timedelta(minutes=10),
            _digest("oi"),
        ),
    )
    expiration = selected.expiration_date
    spot = Decimal("500")
    corridor = Decimal("0.20")
    database_cursor.execute(
        "INSERT INTO rt.option_discovery_batches ("
        "discovery_batch_id,underlying_security_id,profile_id,"
        "profile_sha256,universe_sha256,provider_tier,provider,underlyer,"
        "scheduled_cycle,expiration_from,expiration_through,maximum_dte,"
        "spot_price,strike_corridor_fraction,strike_min,strike_max,"
        "request_filter_sha256,policy_sha256,started_at,completed_at,"
        "page_count,row_count,response_bytes,evidence_sha256) VALUES ("
        "%s,%s,%s,%s,%s,'MASSIVE_ADVANCED','massive','SPY',%s,%s,%s,"
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,1,1,128,%s)",
        (
            str(uuid4()),
            str(security_id),
            str(control.profile_id),
            control.profile_sha256,
            control.universe.sha256,
            plan.discovery_scheduled_cycle,
            expiration,
            expiration,
            (expiration - session).days,
            spot,
            corridor,
            spot * (Decimal("1") - corridor),
            spot * (Decimal("1") + corridor),
            _digest("discovery-filter"),
            control.manifest_policy.sha256,
            known - timedelta(minutes=4),
            known - timedelta(minutes=3),
            _digest("discovery"),
        ),
    )
    response_sha256 = canonical_sha256(())
    source_key = "SPY:DIVIDEND:COVERAGE"
    coverage_payload_sha256 = canonical_sha256({
        "source_key": source_key,
        "response_action_count": 0,
        "response_sha256": response_sha256,
    })
    database_cursor.execute(
        "INSERT INTO ref.corporate_action_coverage ("
        "coverage_id,security_id,action_type,window_start,window_end,"
        "source,source_key,response_action_count,response_sha256,"
        "observed_at,received_at,payload_sha256) VALUES ("
        "%s,%s,'DIVIDEND',%s,%s,'massive',%s,0,%s,%s,%s,%s)",
        (
            str(uuid4()),
            str(security_id),
            session,
            expiration,
            source_key,
            response_sha256,
            known - timedelta(minutes=20),
            known - timedelta(minutes=19),
            coverage_payload_sha256,
        ),
    )
    sessions = OptionExchangeCalendar().trailing_sessions_before(
        session,
        31,
    )
    for partition_date in {sessions[0], sessions[-1]}:
        database_cursor.execute(
            "SELECT rt.ensure_partitions('rt.bar_revisions',%s)",
            (partition_date,),
        )
    for index, bar_session in enumerate(sessions):
        revision_id = uuid4()
        source_revision_id = uuid4()
        bar_start = datetime.combine(
            bar_session,
            time(13, 30),
            tzinfo=UTC,
        )
        bar_end = datetime.combine(
            bar_session,
            time(20),
            tzinfo=UTC,
        )
        observed_at = bar_end + timedelta(minutes=5)
        close = Decimal("470") + Decimal(index)
        payload_sha256 = _digest(("bar", index))
        database_cursor.execute(
            "INSERT INTO rt.bar_catalog ("
            "bar_revision_id,cohort_id,provider_tier,instrument_id,"
            "interval,bar_start,bar_end,observed_at,session_date) VALUES ("
            "%s,%s,'MASSIVE_ADVANCED',%s,'1d',%s,%s,%s,%s)",
            (
                str(revision_id),
                str(history_cohort_id),
                equity_instrument_id,
                bar_start,
                bar_end,
                observed_at,
                bar_session,
            ),
        )
        database_cursor.execute(
            "INSERT INTO rt.bar_revisions ("
            "bar_revision_id,cohort_id,provider_tier,security_id,"
            "instrument_id,ticker,interval,session_date,session_scope,"
            "adjustment_basis,bar_start,bar_end,open_price,high_price,"
            "low_price,close_price,volume,vwap,transaction_count,"
            "source_kind,availability_mode,is_final,observed_at,"
            "ingestion_segment_id,source_revision_ids,payload_sha256"
            ") VALUES (%s,%s,'MASSIVE_ADVANCED',%s,%s,'SPY','1d',%s,"
            "'FULL_DAY','ADJUSTED',%s,%s,%s,%s,%s,%s,%s,%s,100,"
            "'DERIVED','HISTORICAL_RECONSTRUCTED',true,%s,%s,%s,%s)",
            (
                str(revision_id),
                str(history_cohort_id),
                str(security_id),
                equity_instrument_id,
                bar_session,
                bar_start,
                bar_end,
                close,
                close + Decimal("1"),
                close - Decimal("1"),
                close,
                Decimal("1000000") + Decimal(index * 1000),
                close,
                observed_at,
                str(uuid4()),
                [source_revision_id],
                payload_sha256,
            ),
        )
        database_cursor.execute(
            "INSERT INTO rt.bar_current ("
            "cohort_id,provider_tier,instrument_id,interval,bar_start,"
            "session_scope,adjustment_basis,source_kind,bar_revision_id,"
            "bar_end,observed_at,updated_at) VALUES ("
            "%s,'MASSIVE_ADVANCED',%s,'1d',%s,'FULL_DAY','ADJUSTED',"
            "'DERIVED',%s,%s,%s,%s)",
            (
                str(history_cohort_id),
                equity_instrument_id,
                bar_start,
                str(revision_id),
                bar_end,
                observed_at,
                observed_at,
            ),
        )


def _disposable_audit(
    database_pool,
    enrollment,
    cutoff,
    *,
    equity_cutoff=None,
    history_source_cohort_id=None,
):
    connection_value = database_pool.getconn()
    observed_at = cutoff
    try:
        connection_value.set_session(
            readonly=True,
            autocommit=False,
            isolation_level="REPEATABLE READ",
        )
        with connection_value.cursor(
            cursor_factory=RealDictCursor
        ) as raw_cursor:
            read_only = audit.ReadOnlyCursor(raw_cursor)
            read_only.execute("SET LOCAL statement_timeout = '15s'")
            snapshot = audit._snapshot(
                read_only,
                session_date=enrollment.control_plane.session_date,
                cutoff=cutoff,
                observed_at=observed_at,
                requested_cohort_id=enrollment.control_plane.cohort_id,
                equity_cutoff=equity_cutoff,
                history_source_cohort_id=history_source_cohort_id,
            )
        connection_value.rollback()
    finally:
        database_pool.putconn(connection_value)
    return audit._classify(
        snapshot,
        audit._source_evidence(),
        session_date=enrollment.control_plane.session_date,
        cutoff=cutoff,
        observed_at=observed_at,
    )


def test_owp4b_reference_producer_and_disposable_readiness_audit(
    monkeypatch,
):
    suffix = uuid4().hex[:8]
    database = f"test_realtime_v2_owp4b_{suffix}"
    roles = build_role_names_v2(f"test_rt_owp4b_{suffix}")
    roles_owned = False
    try:
        with disposable_v2_database(database) as database_pool:
            flow_policy = advanced_policy()
            enrollment = _owp4_enrollment(
                database_pool,
                flow_policy_sha256=flow_policy.sha256,
            )
            scope = _contract_scope(enrollment, flow_policy)
            control = enrollment.control_plane
            plan = build_advanced_options_reference_producer_plan(
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
            producer = PostgresAdvancedOptionsReferenceProducer(
                provider,
                plan,
                maximum_attempts=2,
                retry_initial_seconds=0,
                wait=lambda _seconds: None,
            )
            blocked = producer.preflight()
            assert blocked.status is ReferenceProducerStatus.BLOCKED
            assert blocked.state is None
            assert {
                "DATED_OI_MISSING",
                "TREASURY_CURVE_MISSING",
                "PRE_SESSION_DISCOVERY_SCOPE_MISSING",
            }.issubset(blocked.reason_codes)
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                unit.cursor.execute(
                    "SELECT count(*) FROM "
                    "rt.advanced_options_reference_states"
                )
                assert unit.cursor.fetchone()[0] == 0
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                _seed_reference_inputs(
                    database_cursor,
                    enrollment,
                    plan,
                    include_optional_metrics=False,
                )
            preflight = producer.preflight()
            assert preflight.status is ReferenceProducerStatus.READY
            assert preflight.state is not None
            assert not preflight.reason_codes
            assert {
                (row.metric_kind.value, row.availability.value)
                for row in preflight.state.equity_metrics
            } == {
                ("AVERAGE_DAILY_VOLUME_30", "AVAILABLE"),
                ("SHORT_INTEREST_PERCENT", "UNAVAILABLE"),
                ("TOTAL_FLOAT", "UNAVAILABLE"),
            }
            assert {
                row.member_key
                for row in preflight.evidence
                if row.availability.value == "UNAVAILABLE"
            } == {
                "SPY:SHORT_INTEREST_PERCENT",
                "SPY:TOTAL_FLOAT",
            }
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
                    "rt.advanced_options_reference_states"
                )
                assert unit.cursor.fetchone()[0] == 1
                unit.cursor.execute(
                    "SELECT count(*) FROM "
                    "rt.advanced_options_reference_state_members"
                )
                assert unit.cursor.fetchone()[0] == len(
                    state.member_identities
                )
            with pytest.raises(errors.InsufficientPrivilege):
                with provider.transaction(
                    API_ROLE,
                    read_only=True,
                ) as unit:
                    unit.cursor.execute(
                        "SELECT count(*) FROM "
                        "rt.advanced_options_reference_states"
                    )
            cutoff = control.receipts.options_input_ready.spec.cutoff
            categories = _disposable_audit(
                database_pool,
                enrollment,
                cutoff,
            )
            assert categories["reference_generation"]["status"] == "AVAILABLE"
            assert categories["dated_open_interest"]["status"] == "AVAILABLE"
            assert categories["treasury_inputs"]["status"] == "AVAILABLE"
            assert categories["dividend_inputs"]["status"] == "COMPLETE_ZERO"
            assert categories["model_inputs"]["status"] == "AVAILABLE"
            assert categories["rolling_state_generation"][
                "reason_codes"
            ] == [
                "ROLLING_STATE_ABSENT",
                "SOURCE_FLOW_IDENTITY_ABSENT",
            ]
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
