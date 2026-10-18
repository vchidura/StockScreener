from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql

from realtime.advanced_ingress_plan import (
    AdvancedIngressBudgets,
    AdvancedIngressPlanRequest,
    build_advanced_stream_authorization,
)
from realtime.advanced_ingress_repository import (
    AdvancedIngressAdmissionError,
    AdvancedCohortResolutionError,
    advanced_ingress_status_cursor,
    admit_advanced_ingress_session_cursor,
    create_advanced_stream_authorization_cursor,
    prepare_advanced_ingress_plan_cursor,
    activate_advanced_runtime_profile_cursor,
    advanced_ingress_controller_status_cursor,
    load_advanced_ingress_runtime_plan_cursor,
    finish_advanced_ingress_plan_cursor,
    register_advanced_runtime_profile_cursor,
    record_advanced_ingress_usage_cursor,
    resolve_advanced_ingress_plan_cursor,
    revoke_advanced_stream_authorization_cursor,
)
from realtime.advanced_ingress_runtime_profile import (
    load_advanced_ingress_runtime_catalog,
)
from realtime.advanced_scheduler import (
    ADVANCED_WINDOW_BUILD_ID,
    audit_completed_session_coverage_cursor,
    bind_advanced_input_gate_build_id,
    discover_advanced_window_recovery_cursor,
    ensure_advanced_scheduler_session_cursor,
    ensure_advanced_window_retry_cursor,
    persist_advanced_scheduler_lifecycle_cursor,
    prioritize_live_over_blocked_backfill_cursor,
    set_advanced_window_retry_status_cursor,
)
from realtime.advanced_gap_repair import (
    GapContinuity,
    plan_advanced_gap_repair_cursor,
    plan_advanced_cold_start_cursor,
    publish_cohort_local_backfill_release_cursor,
    record_first_post_reconnect_event_cursor,
    reserve_advanced_gap_repair_cursor,
    seal_advanced_repair_payload_cursor,
    verify_and_close_gap_repair_cursor,
)
from realtime.advanced_equity_journal_bridge import (
    AdvancedEquityJournalBridgeConfig,
    process_advanced_equity_journal_once,
)
from realtime.advanced_journal_registry import (
    build_advanced_journal_registration,
    discover_incomplete_advanced_journals_cursor,
    mark_advanced_journal_writing_cursor,
    measure_journal_capacity,
    register_advanced_journal_cursor,
)
from realtime.advanced_journal_replay import replay_registered_journal_once
from realtime.advanced_stream_control import (
    AdvancedStreamScope,
    AdvancedStreamUsage,
    record_advanced_stream_gap_cursor,
    register_advanced_stream_session_cursor,
    update_advanced_stream_session_cursor,
)
from realtime.contracts import MarketFeed, RawMessageEnvelope
from realtime.database_access import RealtimeDatabaseRole
from realtime.journal import RawJournalReader, RawJournalWriter
from realtime.database_access_v2 import (
    API_ROLE,
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.security_v2 import (
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from realtime.orchestration_repository import (
    PostgresOrchestrationRepository,
    WindowSpec,
)
from realtime.stage_graph import STAGE_CATALOG_VERSION_V3
from realtime.normalizer import NORMALIZER_VERSION
from realtime.stage_database_v2 import V2WorkerDatabase
from realtime.stage_runners_advanced_equity import (
    advanced_input_gate_identity,
)
from scripts.run_realtime_v2_advanced_journal_replay import (
    replay_registered_once,
)
from v2_database import (
    _connect,
    _settings,
    disposable_v2_database,
    requires_v2_database,
)


pytestmark = requires_v2_database
UTC = timezone.utc
SESSION_DATE = date(2030, 1, 2)
SESSION_OPEN = datetime(2030, 1, 2, 14, 30, tzinfo=UTC)
SESSION_CLOSE = datetime(2030, 1, 2, 21, 0, tzinfo=UTC)
PROFILE_SHA = "b" * 64
RUNTIME_CATALOG = load_advanced_ingress_runtime_catalog(
    Path(__file__).resolve().parents[2]
    / "realtime"
    / "policies"
    / "advanced_equity_ingress_v1.json"
)
RUNTIME_PROFILE = RUNTIME_CATALOG.profile_for_target(1)
POLICY_SHA = RUNTIME_CATALOG.scale_policy.sha256
CONFIGURATION_SHA = RUNTIME_PROFILE.configuration_sha256(POLICY_SHA)


class _Calendar:
    def session_open(self, session_date):
        assert session_date == SESSION_DATE
        return SESSION_OPEN

    def session_close(self, session_date):
        assert session_date == SESSION_DATE
        return SESSION_CLOSE


class _MissingSessionCalendar:
    def session_open(self, session_date):
        return datetime.combine(
            session_date,
            datetime.min.time(),
            tzinfo=UTC,
        ) + timedelta(hours=14, minutes=30)

    def session_close(self, session_date):
        return datetime.combine(
            session_date,
            datetime.min.time(),
            tzinfo=UTC,
        ) + timedelta(hours=21)


class _PromotionCalendar:
    def next_session_date(self, session_date):
        assert session_date == SESSION_DATE
        return SESSION_DATE + timedelta(days=1)


def _digest():
    return uuid4().hex + uuid4().hex


@contextmanager
def _admin_cursor(pool, roles):
    connection = pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("SET LOCAL ROLE {}").format(
                        sql.Identifier(roles.name("admin"))
                    )
                )
                yield cursor
    finally:
        pool.putconn(connection)


def _seed_advanced_cohort(pool):
    profile_id = uuid4()
    manifest_id = uuid4()
    cohort_id = uuid4()
    security_id = uuid4()
    connection = pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO ref.securities ("
                    "security_id,asset_type,source_origin,created_at) "
                    "VALUES (%s,'STOCK','fixture',%s)",
                    (str(security_id), SESSION_OPEN - timedelta(days=1)),
                )
                cursor.execute(
                    "INSERT INTO ref.security_revisions ("
                    "revision_id,security_id,ticker,exchange,calendar_id,"
                    "effective_from,observed_at,payload_sha256,"
                    "company_name,security_type,active) VALUES ("
                    "%s,%s,'SPY','ARCX','XNYS',%s,%s,%s,"
                    "'SPY','ETF',true)",
                    (
                        str(uuid4()),
                        str(security_id),
                        datetime(2020, 1, 1, tzinfo=UTC),
                        datetime(2020, 1, 1, tzinfo=UTC),
                        _digest(),
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.capability_profiles ("
                    "profile_id,config_sha256,schema_sha256,account_alias,"
                    "capabilities_sha256,universe_sha256,checked_at,"
                    "expires_at,status,profile_sha256,provider_tier,"
                    "security_policy_sha256,preflight_mode,"
                    "provider_requests,action_authorized) VALUES ("
                    "%s,%s,%s,'fixture',%s,%s,%s,%s,'PASS',%s,"
                    "'MASSIVE_ADVANCED',%s,'ACTIVE',true,true)",
                    (
                        str(profile_id),
                        CONFIGURATION_SHA,
                        _digest(),
                        _digest(),
                        _digest(),
                        SESSION_OPEN - timedelta(days=1),
                        SESSION_CLOSE + timedelta(days=1),
                        PROFILE_SHA,
                        _digest(),
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.subscription_manifests ("
                    "manifest_id,manifest_sha256,universe_sha256,"
                    "session_date,effective_at,profile_id,manifest_kind,"
                    "stock_count,option_underlying_count,member_count,"
                    "allocation_policy_sha256) VALUES ("
                    "%s,%s,%s,%s,%s,%s,'EQUITY_VALIDATION',1,0,1,%s)",
                    (
                        str(manifest_id),
                        _digest(),
                        _digest(),
                        SESSION_DATE,
                        SESSION_OPEN - timedelta(days=1),
                        str(profile_id),
                        _digest(),
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.cohorts ("
                    "cohort_id,cohort_sha256,manifest_id,"
                    "configuration_sha256,model_bundle_sha256,"
                    "valuation_sha256,effective_at,mode) VALUES ("
                    "%s,%s,%s,%s,%s,%s,%s,'SHADOW')",
                    (
                        str(cohort_id),
                        _digest(),
                        str(manifest_id),
                        CONFIGURATION_SHA,
                        _digest(),
                        _digest(),
                        SESSION_OPEN - timedelta(days=1),
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.instruments (asset_class,security_id) "
                    "VALUES ('EQUITY',%s) RETURNING instrument_id",
                    (str(security_id),),
                )
                instrument_id = cursor.fetchone()[0]
                cursor.execute(
                    "INSERT INTO rt.subscription_members ("
                    "manifest_id,instrument_id,channel,priority,purpose,"
                    "option_underlying) VALUES (%s,%s,'AM',1,'PRIMARY',false)",
                    (str(manifest_id), instrument_id),
                )
    finally:
        pool.putconn(connection)
    return cohort_id, manifest_id


@pytest.fixture(scope="module")
def hardened_database():
    suffix = uuid4().hex[:8]
    database = f"test_realtime_v2_advanced_plan_{suffix}"
    roles = build_role_names_v2(f"test_rt_plan_{suffix}")
    roles_owned = False
    try:
        with disposable_v2_database(database) as pool:
            cohort_id, manifest_id = _seed_advanced_cohort(pool)
            connection = pool.getconn()
            try:
                created, audit = harden_database_v2(connection, roles)
                roles_owned = True
                assert len(created) == 3
                assert all(
                    status.startswith("PASS") for _, status in audit
                )
            finally:
                pool.putconn(connection)
            provider = V2ConnectionProvider(
                {APP_ROLE: pool, API_ROLE: pool},
                V2AccessConfig(
                    security_mode="hardened",
                    role_prefix=roles.prefix,
                    allow_disposable=True,
                ),
            )
            provider.verify()
            with _admin_cursor(pool, roles) as cursor:
                register_advanced_runtime_profile_cursor(
                    cursor,
                    RUNTIME_PROFILE,
                    runtime_policy_sha256=POLICY_SHA,
                    ladder_position=0,
                    previous_universe_target=None,
                )
                activate_advanced_runtime_profile_cursor(
                    cursor,
                    RUNTIME_PROFILE.sha256,
                    stability_evidence_sha256="c" * 64,
                    capacity_evidence_sha256="d" * 64,
                    approval_reference="db-test-profile-activation",
                    activated_at=SESSION_OPEN - timedelta(days=2),
                )
            yield pool, provider, roles, cohort_id, manifest_id
    finally:
        if roles_owned:
            with closing(_connect(_settings(), "postgres")) as connection:
                connection.autocommit = True
                drop_disposable_roles_v2(connection, roles)


def _authorization(ticket, *, budgets=None):
    return build_advanced_stream_authorization(
        parent_authorization_id=None,
        allowed_universe_targets=(1,),
        valid_from=SESSION_DATE,
        valid_through=SESSION_DATE,
        maximum_budgets=budgets
        or AdvancedIngressBudgets(2, 2, 100, 1, 10),
        runtime_policy_sha256=POLICY_SHA,
        approval_source="db-test",
        approval_reference=ticket,
    )


def _create_authorization(pool, roles, ticket, *, budgets=None):
    authorization = _authorization(ticket, budgets=budgets)
    with _admin_cursor(pool, roles) as cursor:
        return create_advanced_stream_authorization_cursor(
            cursor,
            authorization,
        )


def _prepare_plan(provider, authorization, *, budgets=None):
    request = AdvancedIngressPlanRequest(
        authorization.authorization_id,
        SESSION_DATE,
        1,
        budgets or AdvancedIngressBudgets(2, 2, 100, 1, 10),
    )
    with provider.transaction(APP_ROLE) as unit:
        plan = resolve_advanced_ingress_plan_cursor(
            unit.cursor,
            request,
            runtime_catalog=RUNTIME_CATALOG,
            calendar=_Calendar(),
        )
        prepare_advanced_ingress_plan_cursor(
            unit.cursor,
            plan,
            checked_at=SESSION_OPEN - timedelta(minutes=30),
        )
    return plan


def _admit(provider, plan, *, attempt, owner_epoch, session_id):
    with provider.transaction(APP_ROLE) as unit:
        return admit_advanced_ingress_session_cursor(
            unit.cursor,
            plan_id=plan.plan_id,
            cohort_id=plan.cohort_id,
            manifest_id=plan.manifest_id,
            authorization_reference=plan.authorization_reference,
            connection_attempt=attempt,
            connection_ceiling=plan.budgets.connection_attempts,
            decoded_byte_ceiling=plan.budgets.decoded_bytes,
            stream_session_id=session_id,
            owner_epoch=owner_epoch,
            checked_at=SESSION_OPEN + timedelta(minutes=1),
        )


def _insert_repair_bar(
    cursor,
    *,
    cohort_id,
    security_id,
    instrument_id,
    interval,
    bar_start,
    bar_end,
    observed_at,
    source_kind,
    source_revision_ids=(),
):
    revision_id = uuid4()
    ingestion_segment_id = uuid4()
    cursor.execute(
        "INSERT INTO rt.bar_catalog ("
        "bar_revision_id,cohort_id,provider_tier,instrument_id,interval,"
        "bar_start,bar_end,observed_at,session_date) VALUES ("
        "%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,%s,%s)",
        (
            str(revision_id),
            str(cohort_id),
            instrument_id,
            interval,
            bar_start,
            bar_end,
            observed_at,
            bar_start.date(),
        ),
    )
    cursor.execute(
        "INSERT INTO rt.bar_revisions ("
        "bar_revision_id,cohort_id,provider_tier,security_id,"
        "instrument_id,ticker,interval,session_date,session_scope,"
        "adjustment_basis,bar_start,bar_end,open_price,high_price,"
        "low_price,close_price,volume,source_kind,availability_mode,"
        "is_final,observed_at,ingestion_segment_id,source_revision_ids,"
        "payload_sha256,quality_codes) VALUES ("
        "%s,%s,'MASSIVE_ADVANCED',%s,%s,'SPY',%s,%s,'RTH',"
        "'RAW_UNADJUSTED',%s,%s,100,101,99,100.5,1000,%s,"
        "'HISTORICAL_RECONSTRUCTED',true,%s,%s,%s,%s,%s)",
        (
            str(revision_id),
            str(cohort_id),
            str(security_id),
            instrument_id,
            interval,
            bar_start.date(),
            bar_start,
            bar_end,
            source_kind,
            observed_at,
            str(ingestion_segment_id),
            list(source_revision_ids),
            _digest(),
            (
                ["EXPLICIT_ADVANCED_REST_REPAIR_V1"]
                if source_kind == "NATIVE_REST"
                else []
            ),
        ),
    )
    cursor.execute(
        "INSERT INTO rt.bar_current ("
        "cohort_id,provider_tier,instrument_id,interval,bar_start,"
        "session_scope,adjustment_basis,source_kind,bar_revision_id,"
        "bar_end,observed_at,updated_at) VALUES ("
        "%s,'MASSIVE_ADVANCED',%s,%s,%s,'RTH','RAW_UNADJUSTED',"
        "%s,%s,%s,%s,%s)",
        (
            str(cohort_id),
            instrument_id,
            interval,
            bar_start,
            source_kind,
            str(revision_id),
            bar_end,
            observed_at,
            observed_at,
        ),
    )
    return revision_id


def test_admin_grants_idempotency_and_redacted_api_status(
    hardened_database,
):
    pool, provider, roles, cohort_id, _manifest_id = hardened_database
    authorization = _create_authorization(pool, roles, "grant-idempotency")
    with _admin_cursor(pool, roles) as cursor:
        repeated = create_advanced_stream_authorization_cursor(
            cursor,
            authorization,
        )
    assert repeated.authorization_id == authorization.authorization_id

    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        with provider.transaction(APP_ROLE) as unit:
            unit.cursor.execute(
                "INSERT INTO rt.advanced_stream_authorizations DEFAULT VALUES"
            )

    plan = _prepare_plan(provider, authorization)
    repeated = _prepare_plan(provider, authorization)
    assert repeated.plan_id == plan.plan_id
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT count(*) FROM rt.advanced_ingress_plans "
            "WHERE plan_id=%s",
            (str(plan.plan_id),),
        )
        assert unit.cursor.fetchone()[0] == 1
        unit.cursor.execute(
            "SELECT count(*) FROM rt.advanced_ingress_plan_usage "
            "WHERE plan_id=%s",
            (str(plan.plan_id),),
        )
        assert unit.cursor.fetchone()[0] == 1
    with provider.transaction(API_ROLE, read_only=True) as unit:
        status = advanced_ingress_status_cursor(
            unit.cursor,
            plan.plan_id,
            checked_at=SESSION_OPEN,
        )
        assert status.authorization_status == "READY"
        assert status.stream_sessions == 0
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        with provider.transaction(API_ROLE, read_only=True) as unit:
            unit.cursor.execute(
                "SELECT approval_reference "
                "FROM rt.advanced_stream_authorizations"
            )


def test_revocation_and_expiry_block_child_session_admission(
    hardened_database,
):
    pool, provider, roles, cohort_id, _manifest_id = hardened_database
    revoked = _create_authorization(pool, roles, "revoked-admission")
    revoked_plan = _prepare_plan(provider, revoked)
    with _admin_cursor(pool, roles) as cursor:
        revoke_advanced_stream_authorization_cursor(
            cursor,
            revoked.authorization_id,
            revoked_at=SESSION_OPEN,
            revocation_reference="owner-revoked",
        )
    with pytest.raises(AdvancedIngressAdmissionError, match="revoked"):
        _admit(
            provider,
            revoked_plan,
            attempt=1,
            owner_epoch=1,
            session_id=uuid4(),
        )
    with provider.transaction(API_ROLE, read_only=True) as unit:
        status = advanced_ingress_status_cursor(
            unit.cursor,
            revoked_plan.plan_id,
            checked_at=SESSION_OPEN,
        )
        assert status.authorization_status == "REVOKED"

    expiring = _create_authorization(pool, roles, "expired-admission")
    expired_plan = _prepare_plan(provider, expiring)
    with provider.transaction(APP_ROLE) as unit:
        with pytest.raises(AdvancedIngressAdmissionError, match="expired"):
            admit_advanced_ingress_session_cursor(
                unit.cursor,
                plan_id=expired_plan.plan_id,
                cohort_id=expired_plan.cohort_id,
                manifest_id=expired_plan.manifest_id,
                authorization_reference=(
                    expired_plan.authorization_reference
                ),
                connection_attempt=1,
                connection_ceiling=2,
                decoded_byte_ceiling=100,
                stream_session_id=uuid4(),
                owner_epoch=1,
                checked_at=SESSION_CLOSE + timedelta(seconds=1),
            )


def test_concurrent_final_slot_and_cumulative_restart_safe_usage(
    hardened_database,
):
    pool, provider, roles, cohort_id, _manifest_id = hardened_database
    authorization = _create_authorization(pool, roles, "final-slot")
    plan = _prepare_plan(provider, authorization)
    first_session_id = uuid4()
    _admit(
        provider,
        plan,
        attempt=1,
        owner_epoch=1,
        session_id=first_session_id,
    )

    barrier = Barrier(2)

    def contend(owner_epoch):
        session_id = uuid4()
        barrier.wait()
        try:
            usage = _admit(
                provider,
                plan,
                attempt=2,
                owner_epoch=owner_epoch,
                session_id=session_id,
            )
            return True, owner_epoch, session_id, usage
        except AdvancedIngressAdmissionError:
            return False, owner_epoch, session_id, None

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(contend, (2, 3)))
    successful = tuple(outcome for outcome in outcomes if outcome[0])
    assert len(successful) == 1
    _, latest_epoch, latest_session_id, usage = successful[0]
    assert usage.stream_session_count == 2
    assert usage.connection_attempt_count == 2

    with provider.transaction(APP_ROLE) as unit:
        first = record_advanced_ingress_usage_cursor(
            unit.cursor,
            plan_id=plan.plan_id,
            stream_session_id=latest_session_id,
            owner_epoch=latest_epoch,
            decoded_bytes=60,
            checked_at=SESSION_OPEN + timedelta(minutes=2),
        )
    with provider.transaction(APP_ROLE) as unit:
        final = record_advanced_ingress_usage_cursor(
            unit.cursor,
            plan_id=plan.plan_id,
            stream_session_id=latest_session_id,
            owner_epoch=latest_epoch,
            decoded_bytes=40,
            rest_repair_requests=1,
            rest_repair_decoded_bytes=10,
            checked_at=SESSION_OPEN + timedelta(minutes=3),
        )
    assert first.decoded_byte_count == 60
    assert final.decoded_byte_count == 100
    assert final.rest_repair_request_count == 1
    assert final.rest_repair_decoded_byte_count == 10

    with provider.transaction(APP_ROLE) as unit:
        with pytest.raises(
            AdvancedIngressAdmissionError,
            match="old stream owner",
        ):
            record_advanced_ingress_usage_cursor(
                unit.cursor,
                plan_id=plan.plan_id,
                stream_session_id=first_session_id,
                owner_epoch=1,
                decoded_bytes=1,
                checked_at=SESSION_OPEN + timedelta(minutes=4),
            )
    with provider.transaction(APP_ROLE) as unit:
        with pytest.raises(
            AdvancedIngressAdmissionError,
            match="budget is exhausted",
        ):
            record_advanced_ingress_usage_cursor(
                unit.cursor,
                plan_id=plan.plan_id,
                stream_session_id=latest_session_id,
                owner_epoch=latest_epoch,
                decoded_bytes=1,
                checked_at=SESSION_OPEN + timedelta(minutes=4),
            )


def test_terminal_controller_outcome_is_durable_and_cannot_restart(
    hardened_database,
):
    pool, provider, roles, _cohort_id, _manifest_id = hardened_database
    authorization = _create_authorization(
        pool,
        roles,
        "terminal-controller-outcome",
    )
    plan = _prepare_plan(provider, authorization)
    with provider.transaction(APP_ROLE) as unit:
        first = finish_advanced_ingress_plan_cursor(
            unit.cursor,
            plan.plan_id,
            terminal_outcome="BLOCKED_ENTITLEMENT",
            completed_at=SESSION_OPEN,
        )
    with provider.transaction(APP_ROLE) as unit:
        repeated = finish_advanced_ingress_plan_cursor(
            unit.cursor,
            plan.plan_id,
            terminal_outcome="BLOCKED_ENTITLEMENT",
            completed_at=SESSION_OPEN + timedelta(minutes=1),
        )
    assert first.plan_status == repeated.plan_status == "BLOCKED"
    assert first.terminal_outcome == "BLOCKED_ENTITLEMENT"
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        status = advanced_ingress_controller_status_cursor(
            unit.cursor,
            plan.plan_id,
        )
    assert status.terminal_outcome == "BLOCKED_ENTITLEMENT"
    with pytest.raises(AdvancedIngressAdmissionError):
        _admit(
            provider,
            plan,
            attempt=1,
            owner_epoch=1,
            session_id=uuid4(),
        )


def test_registered_raw_commit_replays_to_one_canonical_bar_and_checkpoint(
    hardened_database,
    tmp_path,
):
    pool, provider, roles, _cohort_id, _manifest_id = hardened_database
    budgets = AdvancedIngressBudgets(2, 2, 10_000, 0, 0)
    authorization = _create_authorization(
        pool,
        roles,
        "journal-crash-replay",
        budgets=budgets,
    )
    plan = _prepare_plan(provider, authorization, budgets=budgets)
    generation = uuid4()
    scope = AdvancedStreamScope(
        ingress_plan_id=plan.plan_id,
        cohort_id=plan.cohort_id,
        manifest_id=plan.manifest_id,
        journal_generation=generation,
        owner_id=uuid4(),
        feed=MarketFeed.STOCKS,
        normalizer_version=NORMALIZER_VERSION,
        authorization_reference=plan.authorization_reference,
        connection_attempt=1,
        connection_ceiling=2,
        decoded_byte_ceiling=10_000,
        stale_after_seconds=60,
        started_at=SESSION_OPEN + timedelta(minutes=1),
    )
    with provider.transaction(APP_ROLE) as unit:
        session = register_advanced_stream_session_cursor(
            unit.cursor,
            scope,
        )
        unit.cursor.execute(
            "SELECT instrument_id FROM rt.subscription_members "
            "WHERE manifest_id=%s AND channel='AM'",
            (str(plan.manifest_id),),
        )
        instrument_id = int(unit.cursor.fetchone()[0])
    with _admin_cursor(pool, roles) as cursor:
        cursor.execute(
            "SELECT rt.ensure_partitions('rt.bar_revisions',%s)",
            (SESSION_DATE,),
        )

    root = tmp_path / "mounted-journals"
    root.mkdir()
    path = root / "crash.sqlite"
    bridge_config = AdvancedEquityJournalBridgeConfig(
        cohort_id=plan.cohort_id,
        journal_generation=generation,
        stream_session_id=session.session_id,
        owner_epoch=session.owner_epoch,
        normalizer_version=NORMALIZER_VERSION,
        shard_ids=(0,),
        batch_limit=100,
    )
    registration = build_advanced_journal_registration(
        root=root,
        path=path,
        journal_generation=generation,
        plan_id=plan.plan_id,
        stream_session_id=session.session_id,
        cohort_id=plan.cohort_id,
        manifest_id=plan.manifest_id,
        session_date=plan.session_date,
        owner_epoch=session.owner_epoch,
        normalizer_version=NORMALIZER_VERSION,
        shard_ids=(0,),
        shard_map_sha256=bridge_config.shard_map_sha256,
    )
    capacity = measure_journal_capacity(
        root,
        minimum_free_bytes=1,
        minimum_free_percent=1,
    )
    assert capacity.admitted
    with provider.transaction(APP_ROLE) as unit:
        register_advanced_journal_cursor(unit.cursor, registration)
        mark_advanced_journal_writing_cursor(
            unit.cursor,
            generation,
            checked_at=scope.started_at,
        )

    start = int(SESSION_OPEN.timestamp() * 1000)
    end = start + 60_000
    payload = json.dumps([{
        "ev": "AM",
        "sym": "SPY",
        "o": 100,
        "h": 101,
        "l": 99,
        "c": 100.5,
        "v": 1000,
        "vw": 100.25,
        "s": start,
        "e": end,
    }]).encode()
    with RawJournalWriter(path, generation=generation) as writer:
        writer.append_batch((
            RawMessageEnvelope(
                session.session_id,
                0,
                MarketFeed.STOCKS,
                SESSION_OPEN + timedelta(minutes=1),
                1,
                payload,
            ),
        ))

    database = V2WorkerDatabase(
        provider,
        RealtimeDatabaseRole.COMPUTE,
    )
    checked_at = SESSION_OPEN + timedelta(minutes=5)
    replay = replay_registered_journal_once(
        provider,
        database,
        root,
        replace(registration, status="WRITING"),
        checked_at=checked_at,
        batch_limit=100,
    )
    repeated = process_advanced_equity_journal_once(
        database,
        RawJournalReader(path),
        bridge_config,
        checked_at=checked_at + timedelta(seconds=1),
    )

    assert replay.persisted_bars == 1
    assert repeated.persisted_bars == 0
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT status,raw_committed_offset,"
            "normalized_committed_offset,bridge_committed_offset "
            "FROM rt.advanced_journal_registry "
            "WHERE journal_generation=%s",
            (str(generation),),
        )
        assert unit.cursor.fetchone() == ("DRAINED", 1, 1, 1)
        unit.cursor.execute(
            "SELECT count(*) FROM rt.bar_revisions "
            "WHERE journal_generation=%s AND instrument_id=%s",
            (str(generation), instrument_id),
        )
        assert unit.cursor.fetchone()[0] == 1
        assert discover_incomplete_advanced_journals_cursor(
            unit.cursor
        ) == ()


def test_input_ready_accepts_exact_bars_from_two_fenced_generations(
    hardened_database,
    tmp_path,
):
    pool, provider, roles, _cohort_id, _manifest_id = hardened_database
    budgets = AdvancedIngressBudgets(2, 2, 100_000, 0, 0)
    authorization = _create_authorization(
        pool,
        roles,
        "multi-generation-input-ready",
        budgets=budgets,
    )
    plan = _prepare_plan(provider, authorization, budgets=budgets)
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT instrument_id FROM rt.subscription_members "
            "WHERE manifest_id=%s AND channel='AM'",
            (str(plan.manifest_id),),
        )
        instrument_id = int(unit.cursor.fetchone()[0])
    with _admin_cursor(pool, roles) as cursor:
        cursor.execute(
            "SELECT rt.ensure_partitions('rt.bar_revisions',%s)",
            (SESSION_DATE,),
        )
    root = tmp_path / "multi-generation-journals"
    root.mkdir()
    window_start = SESSION_OPEN + timedelta(hours=2)
    database = V2WorkerDatabase(
        provider,
        RealtimeDatabaseRole.COMPUTE,
    )

    def run_generation(
        *,
        attempt,
        starts,
        terminal,
    ):
        generation = uuid4()
        scope = AdvancedStreamScope(
            ingress_plan_id=plan.plan_id,
            cohort_id=plan.cohort_id,
            manifest_id=plan.manifest_id,
            journal_generation=generation,
            owner_id=uuid4(),
            feed=MarketFeed.STOCKS,
            normalizer_version=NORMALIZER_VERSION,
            authorization_reference=plan.authorization_reference,
            connection_attempt=attempt,
            connection_ceiling=2,
            decoded_byte_ceiling=100_000,
            stale_after_seconds=60,
            started_at=starts[0],
        )
        with provider.transaction(APP_ROLE) as unit:
            session = register_advanced_stream_session_cursor(
                unit.cursor,
                scope,
            )
            update_advanced_stream_session_cursor(
                unit.cursor,
                session,
                AdvancedStreamUsage(0, 0, 0, 0, 0, 0, None),
                status="LIVE",
                checked_at=scope.started_at,
                terminal=False,
            )
        config = AdvancedEquityJournalBridgeConfig(
            cohort_id=plan.cohort_id,
            journal_generation=generation,
            stream_session_id=session.session_id,
            owner_epoch=session.owner_epoch,
            normalizer_version=NORMALIZER_VERSION,
            shard_ids=(0,),
            batch_limit=100,
        )
        path = root / f"{generation}.sqlite"
        registration = build_advanced_journal_registration(
            root=root,
            path=path,
            journal_generation=generation,
            plan_id=plan.plan_id,
            stream_session_id=session.session_id,
            cohort_id=plan.cohort_id,
            manifest_id=plan.manifest_id,
            session_date=plan.session_date,
            owner_epoch=session.owner_epoch,
            normalizer_version=NORMALIZER_VERSION,
            shard_ids=(0,),
            shard_map_sha256=config.shard_map_sha256,
        )
        with provider.transaction(APP_ROLE) as unit:
            register_advanced_journal_cursor(unit.cursor, registration)
            mark_advanced_journal_writing_cursor(
                unit.cursor,
                generation,
                checked_at=scope.started_at,
            )
        payload = json.dumps([
            {
                "ev": "AM",
                "sym": "SPY",
                "o": 100,
                "h": 101,
                "l": 99,
                "c": 100.5,
                "v": 1000,
                "vw": 100.25,
                "s": int(start.timestamp() * 1000),
                "e": int(
                    (start + timedelta(minutes=1)).timestamp() * 1000
                ),
            }
            for start in starts
        ]).encode()
        with RawJournalWriter(path, generation=generation) as writer:
            writer.append_batch((
                RawMessageEnvelope(
                    session.session_id,
                    0,
                    MarketFeed.STOCKS,
                    starts[-1] + timedelta(minutes=1),
                    attempt,
                    payload,
                ),
            ))
        replay_registered_journal_once(
            provider,
            database,
            root,
            replace(registration, status="WRITING"),
            checked_at=starts[-1] + timedelta(minutes=1, seconds=1),
            batch_limit=100,
        )
        usage = AdvancedStreamUsage(
            1,
            len(payload),
            1,
            len(payload),
            1,
            len(starts),
            starts[-1] + timedelta(minutes=1),
        )
        with provider.transaction(APP_ROLE) as unit:
            update_advanced_stream_session_cursor(
                unit.cursor,
                session,
                usage,
                status="STOPPED" if terminal else "LIVE",
                checked_at=starts[-1] + timedelta(minutes=1),
                terminal=terminal,
            )
        return session, generation

    first = run_generation(
        attempt=1,
        starts=(
            window_start,
            window_start + timedelta(minutes=1),
        ),
        terminal=True,
    )
    second = run_generation(
        attempt=2,
        starts=(
            window_start + timedelta(minutes=2),
            window_start + timedelta(minutes=3),
            window_start + timedelta(minutes=4),
        ),
        terminal=True,
    )
    cutoff = window_start + timedelta(minutes=5)
    identity = advanced_input_gate_identity(
        database,
        SimpleNamespace(
            plan=SimpleNamespace(cohort_id=plan.cohort_id),
            window=SimpleNamespace(window_id=uuid4(), cutoff=cutoff),
        ),
    )

    assert len(identity) == 64
    assert first[0].owner_epoch < second[0].owner_epoch
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT count(DISTINCT journal_generation) "
            "FROM rt.bar_revisions WHERE cohort_id=%s "
            "AND bar_start>=%s AND bar_end<=%s",
            (str(plan.cohort_id), window_start, cutoff),
        )
        assert unit.cursor.fetchone()[0] == 2


def test_missing_registered_journal_is_durably_quarantined(
    hardened_database,
    tmp_path,
):
    pool, provider, roles, _cohort_id, _manifest_id = hardened_database
    budgets = AdvancedIngressBudgets(2, 2, 10_000, 0, 0)
    authorization = _create_authorization(
        pool,
        roles,
        "missing-journal-quarantine",
        budgets=budgets,
    )
    plan = _prepare_plan(provider, authorization, budgets=budgets)
    generation = uuid4()
    scope = AdvancedStreamScope(
        ingress_plan_id=plan.plan_id,
        cohort_id=plan.cohort_id,
        manifest_id=plan.manifest_id,
        journal_generation=generation,
        owner_id=uuid4(),
        feed=MarketFeed.STOCKS,
        normalizer_version=NORMALIZER_VERSION,
        authorization_reference=plan.authorization_reference,
        connection_attempt=1,
        connection_ceiling=2,
        decoded_byte_ceiling=10_000,
        stale_after_seconds=60,
        started_at=SESSION_OPEN + timedelta(minutes=10),
    )
    with provider.transaction(APP_ROLE) as unit:
        session = register_advanced_stream_session_cursor(
            unit.cursor,
            scope,
        )
    root = tmp_path / "missing-mounted"
    root.mkdir()
    bridge_config = AdvancedEquityJournalBridgeConfig(
        cohort_id=plan.cohort_id,
        journal_generation=generation,
        stream_session_id=session.session_id,
        owner_epoch=session.owner_epoch,
        normalizer_version=NORMALIZER_VERSION,
        shard_ids=(0,),
    )
    registration = build_advanced_journal_registration(
        root=root,
        path=root / "never-created.sqlite",
        journal_generation=generation,
        plan_id=plan.plan_id,
        stream_session_id=session.session_id,
        cohort_id=plan.cohort_id,
        manifest_id=plan.manifest_id,
        session_date=plan.session_date,
        owner_epoch=session.owner_epoch,
        normalizer_version=NORMALIZER_VERSION,
        shard_ids=(0,),
        shard_map_sha256=bridge_config.shard_map_sha256,
    )
    with provider.transaction(APP_ROLE) as unit:
        register_advanced_journal_cursor(unit.cursor, registration)
    database = V2WorkerDatabase(
        provider,
        RealtimeDatabaseRole.COMPUTE,
    )

    drained, quarantined = replay_registered_once(
        provider,
        database,
        root,
        checked_at=SESSION_OPEN + timedelta(minutes=12),
        emit=lambda _message: None,
    )

    assert (drained, quarantined) == (0, 1)
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT status,quarantine_reason "
            "FROM rt.advanced_journal_registry "
            "WHERE journal_generation=%s",
            (str(generation),),
        )
        assert unit.cursor.fetchone() == (
            "QUARANTINED",
            "MISSING_JOURNAL",
        )


def test_gap_reconnect_stays_open_until_authorized_exact_repair(
    hardened_database,
):
    pool, provider, roles, _cohort_id, _manifest_id = hardened_database
    budgets = AdvancedIngressBudgets(2, 2, 10_000, 1, 10)
    authorization = _create_authorization(
        pool,
        roles,
        "authorized-gap-repair",
        budgets=budgets,
    )
    plan = _prepare_plan(provider, authorization, budgets=budgets)
    generation = uuid4()
    scope = AdvancedStreamScope(
        ingress_plan_id=plan.plan_id,
        cohort_id=plan.cohort_id,
        manifest_id=plan.manifest_id,
        journal_generation=generation,
        owner_id=uuid4(),
        feed=MarketFeed.STOCKS,
        normalizer_version=NORMALIZER_VERSION,
        authorization_reference=plan.authorization_reference,
        connection_attempt=1,
        connection_ceiling=2,
        decoded_byte_ceiling=10_000,
        stale_after_seconds=60,
        started_at=SESSION_OPEN + timedelta(minutes=15),
    )
    with provider.transaction(APP_ROLE) as unit:
        session = register_advanced_stream_session_cursor(
            unit.cursor,
            scope,
        )
        unit.cursor.execute(
            "SELECT member.instrument_id,instrument.security_id "
            "FROM rt.subscription_members AS member "
            "JOIN rt.instruments AS instrument USING(instrument_id) "
            "WHERE member.manifest_id=%s AND member.channel='AM'",
            (str(plan.manifest_id),),
        )
        instrument_id, security_id = unit.cursor.fetchone()
        gap_id = record_advanced_stream_gap_cursor(
            unit.cursor,
            session,
            gap_start=SESSION_OPEN + timedelta(minutes=16),
            gap_end=None,
            last_durable_offset=10,
            reason="disconnect",
            last_durable_watermark=(
                SESSION_OPEN + timedelta(minutes=15)
            ),
            instrument_id=int(instrument_id),
        )
        unit.cursor.execute(
            "UPDATE rt.stream_gaps SET reconnect_at=%s WHERE gap_id=%s",
            (
                SESSION_OPEN + timedelta(minutes=21),
                str(gap_id),
            ),
        )
        classification = record_first_post_reconnect_event_cursor(
            unit.cursor,
            plan_id=plan.plan_id,
            reconnect_journal_generation=uuid4(),
            first_event_at=SESSION_OPEN + timedelta(minutes=21),
            first_offset=1,
        )
    assert classification is GapContinuity.PROVIDER_TIME_OUTAGE
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT recovery_status,continuity_class "
            "FROM rt.stream_gaps WHERE gap_id=%s",
            (str(gap_id),),
        )
        assert unit.cursor.fetchone() == (
            "UNRESOLVED",
            "PROVIDER_TIME_OUTAGE",
        )

    with provider.transaction(APP_ROLE) as unit:
        repair = plan_advanced_gap_repair_cursor(
            unit.cursor,
            gap_id,
            reserved_response_bytes=10,
        )
        request_id = reserve_advanced_gap_repair_cursor(
            unit.cursor,
            repair,
            reserved_at=SESSION_OPEN + timedelta(minutes=22),
        )
    with _admin_cursor(pool, roles) as cursor:
        for relation in (
            "rt.raw_payloads",
            "rt.raw_payload_chunks",
            "rt.bar_revisions",
        ):
            cursor.execute(
                "SELECT rt.ensure_partitions(%s,%s)",
                (relation, SESSION_DATE),
            )
    with provider.transaction(APP_ROLE) as unit:
        payload_id = seal_advanced_repair_payload_cursor(
            unit.cursor,
            repair.repair_id,
            b"[]",
            sealed_at=SESSION_OPEN + timedelta(minutes=23),
        )
        unit.cursor.execute(
            "UPDATE rt.stream_gaps SET repair_status='REPLAYED' "
            "WHERE repair_id=%s",
            (str(repair.repair_id),),
        )
    assert request_id and payload_id

    requested_from = SESSION_OPEN + timedelta(minutes=16)
    requested_to = SESSION_OPEN + timedelta(minutes=21)
    observed_at = SESSION_OPEN + timedelta(minutes=25)
    with provider.transaction(APP_ROLE) as unit:
        source_ids = []
        for minute in range(5):
            source_ids.append(
                _insert_repair_bar(
                    unit.cursor,
                    cohort_id=plan.cohort_id,
                    security_id=security_id,
                    instrument_id=int(instrument_id),
                    interval="1m",
                    bar_start=requested_from + timedelta(minutes=minute),
                    bar_end=requested_from + timedelta(minutes=minute + 1),
                    observed_at=observed_at,
                    source_kind="NATIVE_REST",
                )
            )
        _insert_repair_bar(
            unit.cursor,
            cohort_id=plan.cohort_id,
            security_id=security_id,
            instrument_id=int(instrument_id),
            interval="5m",
            bar_start=requested_from,
            bar_end=requested_to,
            observed_at=observed_at,
            source_kind="DERIVED",
            source_revision_ids=tuple(source_ids),
        )
        coverage_sha256 = verify_and_close_gap_repair_cursor(
            unit.cursor,
            repair.repair_id,
            checked_at=SESSION_OPEN + timedelta(minutes=26),
        )
    assert len(coverage_sha256) == 64
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT recovery_status,operator_alert_required "
            "FROM rt.stream_gaps WHERE gap_id=%s",
            (str(gap_id),),
        )
        assert unit.cursor.fetchone() == ("REPAIRED", False)
        unit.cursor.execute(
            "SELECT rest_repair_request_count,"
            "rest_repair_decoded_byte_count "
            "FROM rt.advanced_ingress_plan_usage WHERE plan_id=%s",
            (str(plan.plan_id),),
        )
        assert unit.cursor.fetchone() == (1, 10)


def test_unauthorized_gap_repair_preserves_gap_and_performs_zero_requests(
    hardened_database,
):
    pool, provider, roles, _cohort_id, _manifest_id = hardened_database
    budgets = AdvancedIngressBudgets(2, 2, 10_000, 0, 0)
    authorization = _create_authorization(
        pool,
        roles,
        "unauthorized-gap-repair",
        budgets=budgets,
    )
    plan = _prepare_plan(provider, authorization, budgets=budgets)
    scope = AdvancedStreamScope(
        ingress_plan_id=plan.plan_id,
        cohort_id=plan.cohort_id,
        manifest_id=plan.manifest_id,
        journal_generation=uuid4(),
        owner_id=uuid4(),
        feed=MarketFeed.STOCKS,
        normalizer_version=NORMALIZER_VERSION,
        authorization_reference=plan.authorization_reference,
        connection_attempt=1,
        connection_ceiling=2,
        decoded_byte_ceiling=10_000,
        stale_after_seconds=60,
        started_at=SESSION_OPEN + timedelta(minutes=30),
    )
    with provider.transaction(APP_ROLE) as unit:
        session = register_advanced_stream_session_cursor(
            unit.cursor,
            scope,
        )
        gap_id = record_advanced_stream_gap_cursor(
            unit.cursor,
            session,
            gap_start=SESSION_OPEN + timedelta(minutes=31),
            gap_end=SESSION_OPEN + timedelta(minutes=36),
            last_durable_offset=0,
            reason="disconnect",
        )
        unit.cursor.execute(
            "UPDATE rt.stream_gaps SET continuity_class="
            "'UNKNOWN_CONTINUITY' WHERE gap_id=%s",
            (str(gap_id),),
        )
        repair = plan_advanced_gap_repair_cursor(
            unit.cursor,
            gap_id,
            reserved_response_bytes=1,
        )
    assert repair.status == "BLOCKED"
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT recovery_status,operator_alert_required "
            "FROM rt.stream_gaps WHERE gap_id=%s",
            (str(gap_id),),
        )
        assert unit.cursor.fetchone() == ("UNRESOLVED", True)
        unit.cursor.execute(
            "SELECT COUNT(*) FROM rt.provider_request_ledger "
            "WHERE request_id=("
            "SELECT repair_reference FROM rt.stream_gaps "
            "WHERE repair_id=%s)",
            (str(repair.repair_id),),
        )
        assert unit.cursor.fetchone()[0] == 0


def test_backfill_blocked_and_cohort_local_wait_state_are_durable(
    hardened_database,
):
    pool, provider, roles, _cohort_id, _manifest_id = hardened_database
    budgets = AdvancedIngressBudgets(2, 2, 10_000, 0, 0)
    authorization = _create_authorization(
        pool,
        roles,
        "blocked-startup-backfill",
        budgets=budgets,
    )
    plan = _prepare_plan(provider, authorization, budgets=budgets)
    historical_session = date(2026, 10, 15)
    with provider.transaction(APP_ROLE) as unit:
        blocked = plan_advanced_cold_start_cursor(
            unit.cursor,
            plan_id=plan.plan_id,
            sessions=(historical_session,),
            known_by=SESSION_OPEN,
            request_ceiling=0,
            decoded_byte_ceiling=0,
            expected_cost="no provider allocation",
        )
    assert blocked.state == "BACKFILL_BLOCKED"
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT state FROM rt.window_runs WHERE plan_id=%s",
            (str(blocked.backfill_id),),
        )
        assert unit.cursor.fetchone() == ("RETAINED_GAP",)
        unit.cursor.execute(
            "SELECT COUNT(*) FROM rt.provider_request_ledger "
            "WHERE window_id IN ("
            "SELECT window_id FROM rt.window_runs WHERE plan_id=%s)",
            (str(blocked.backfill_id),),
        )
        request_count = int(unit.cursor.fetchone()[0])
    assert request_count == 0

    with _admin_cursor(pool, roles) as cursor:
        cursor.execute(
            "SELECT rt.ensure_partitions('rt.portal_releases',%s)",
            (SESSION_DATE,),
        )
    with provider.transaction(APP_ROLE) as unit:
        unit.cursor.execute(
            "UPDATE rt.window_runs SET state='COMPLETE',updated_at=%s "
            "WHERE plan_id=%s",
            (SESSION_OPEN, str(blocked.backfill_id)),
        )
        release_id = publish_cohort_local_backfill_release_cursor(
            unit.cursor,
            blocked.backfill_id,
            coverage_sha256="a" * 64,
            job_barrier_sha256="b" * 64,
            published_at=SESSION_OPEN + timedelta(minutes=1),
        )
    assert release_id
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT COUNT(*) FROM rt.portal_releases "
            "WHERE release_id=%s",
            (str(release_id),),
        )
        assert unit.cursor.fetchone()[0] == 1
        unit.cursor.execute(
            "SELECT COUNT(*) FROM rt.portal_release_current "
            "WHERE cohort_id=%s",
            (str(plan.cohort_id),),
        )
        assert unit.cursor.fetchone()[0] == 0


def test_scheduler_restart_uses_durable_receipts_retries_and_lifecycle(
    hardened_database,
):
    pool, provider, roles, _cohort_id, _manifest_id = hardened_database
    authorization = _create_authorization(
        pool,
        roles,
        "restart-safe-scheduler",
    )
    ingress_plan = _prepare_plan(provider, authorization)
    repository = PostgresOrchestrationRepository(provider)
    cutoff = SESSION_OPEN + timedelta(minutes=5)
    release_at = cutoff + timedelta(seconds=5)
    deadline = release_at + timedelta(minutes=2)
    service_plan_id = repository.create_plan(
        ingress_plan.cohort_id,
        ingress_plan.session_date,
        ("EQUITY_INPUT_GATE", "EQUITY_MATERIALIZE", "EQUITY_PROJECTION"),
        config_sha256=_digest(),
        deadline=deadline + timedelta(minutes=5),
        scope_reference="restart-safe-scheduler",
        catalog_version=STAGE_CATALOG_VERSION_V3,
    )
    failed_window = repository.ensure_window(
        service_plan_id,
        WindowSpec(
            ingress_plan.cohort_id,
            "MASSIVE_ADVANCED",
            f"advanced-{cutoff:%Y%m%dT%H%M%SZ}",
            release_at,
            cutoff,
            deadline,
        ),
    )
    with provider.transaction(APP_ROLE) as unit:
        unit.cursor.execute(
            "SELECT manifest.universe_sha256 "
            "FROM rt.cohorts AS cohort "
            "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
            "WHERE cohort.cohort_id=%s",
            (str(ingress_plan.cohort_id),),
        )
        universe_sha256 = unit.cursor.fetchone()[0]
        unit.cursor.execute(
            "UPDATE rt.window_runs SET state='FAILED',updated_at=%s "
            "WHERE window_id=%s",
            (
                SESSION_OPEN + timedelta(minutes=6),
                str(failed_window.window_id),
            ),
        )
        unit.cursor.execute(
            "INSERT INTO rt.cycle_receipts ("
            "receipt_id,window_id,cohort_id,receipt_kind,cutoff,"
            "reference_generation,universe_sha256,input_sha256,build_id"
            ") VALUES (%s,%s,%s,'INPUT_READY',%s,1,%s,%s,%s)",
            (
                str(uuid4()),
                str(failed_window.window_id),
                str(ingress_plan.cohort_id),
                cutoff,
                universe_sha256,
                "b" * 64,
                bind_advanced_input_gate_build_id(
                    ADVANCED_WINDOW_BUILD_ID,
                    "c" * 64,
                ),
            ),
        )
        scheduler_session = ensure_advanced_scheduler_session_cursor(
            unit.cursor,
            authorization_id=authorization.authorization_id,
            ingress_plan_id=ingress_plan.plan_id,
            cohort_id=ingress_plan.cohort_id,
            session_date=ingress_plan.session_date,
            first_authorized_cutoff=cutoff,
            session_close_at=SESSION_CLOSE,
            drain_deadline=SESSION_CLOSE + timedelta(seconds=30),
            checked_at=SESSION_OPEN + timedelta(minutes=12),
        )
        recovery = discover_advanced_window_recovery_cursor(
            unit.cursor,
            cohort_id=ingress_plan.cohort_id,
            cutoff=cutoff,
            checked_at=SESSION_OPEN + timedelta(minutes=12),
        )
        retry = ensure_advanced_window_retry_cursor(
            unit.cursor,
            scheduler_session.scheduler_session_id,
            cohort_id=ingress_plan.cohort_id,
            cutoff=cutoff,
            attempt=recovery.next_attempt,
            retry_not_before=recovery.retry_not_before,
            input_receipt=recovery.input_receipt,
            portal_receipt=recovery.portal_receipt,
        )
        repeated = ensure_advanced_window_retry_cursor(
            unit.cursor,
            scheduler_session.scheduler_session_id,
            cohort_id=ingress_plan.cohort_id,
            cutoff=cutoff,
            attempt=recovery.next_attempt,
            retry_not_before=recovery.retry_not_before,
            input_receipt=recovery.input_receipt,
            portal_receipt=recovery.portal_receipt,
        )
        set_advanced_window_retry_status_cursor(
            unit.cursor,
            retry.retry_id,
            status="FAILED",
            reason_code="SYNTHETIC_RESTART",
            checked_at=SESSION_OPEN + timedelta(minutes=13),
        )
    assert retry == repeated
    assert retry.reused_input_receipt
    assert recovery.input_gate_sha256 == "c" * 64
    assert recovery.input_known_by == deadline
    assert retry.report_relative_path.endswith("-retry1.json")
    with provider.transaction(APP_ROLE) as unit:
        blocked_backfill = plan_advanced_cold_start_cursor(
            unit.cursor,
            plan_id=ingress_plan.plan_id,
            sessions=(date(2026, 10, 14),),
            known_by=SESSION_OPEN,
            request_ceiling=0,
            decoded_byte_ceiling=0,
            expected_cost="none",
        )
        assert prioritize_live_over_blocked_backfill_cursor(
            unit.cursor,
            ingress_plan.plan_id,
            checked_at=SESSION_OPEN,
        ) == 1
        unit.cursor.execute(
            "SELECT state FROM rt.window_runs WHERE cohort_id=%s "
            "AND window_key=%s",
            (
                str(ingress_plan.cohort_id),
                "advanced-backfill-2026-10-14",
            ),
        )
        assert unit.cursor.fetchone() == ("RETAINED_GAP",)

    later_cutoff = cutoff + timedelta(minutes=5)
    later_window = repository.ensure_window(
        service_plan_id,
        WindowSpec(
            ingress_plan.cohort_id,
            "MASSIVE_ADVANCED",
            f"advanced-{later_cutoff:%Y%m%dT%H%M%SZ}",
            later_cutoff + timedelta(seconds=5),
            later_cutoff,
            later_cutoff + timedelta(minutes=2),
        ),
    )
    with provider.transaction(APP_ROLE) as unit:
        unit.cursor.execute(
            "UPDATE rt.window_runs SET state='COMPLETE',updated_at=%s "
            "WHERE window_id=%s",
            (
                SESSION_OPEN + timedelta(minutes=14),
                str(later_window.window_id),
            ),
        )
        later = discover_advanced_window_recovery_cursor(
            unit.cursor,
            cohort_id=ingress_plan.cohort_id,
            cutoff=later_cutoff,
            checked_at=SESSION_OPEN + timedelta(minutes=14),
        )
        persist_advanced_scheduler_lifecycle_cursor(
            unit.cursor,
            scheduler_session.scheduler_session_id,
            status="IDLE",
            reason_code="SESSION_DRAINED",
            next_action="WAIT_NEXT_SESSION",
            last_discovered_cutoff=later_cutoff,
            checked_at=SESSION_CLOSE + timedelta(seconds=30),
        )
    assert later.complete

    with provider.transaction(APP_ROLE, read_only=True) as unit:
        audit = audit_completed_session_coverage_cursor(
            unit.cursor,
            cohort_id=ingress_plan.cohort_id,
            sessions=(date(2026, 10, 15),),
            checked_at=SESSION_OPEN,
        )
    assert audit["provider_requests"] == 0


def test_two_controllers_reclaim_one_stale_owner_and_fence_old_writes(
    hardened_database,
):
    pool, provider, roles, _cohort_id, _manifest_id = hardened_database
    budgets = AdvancedIngressBudgets(3, 3, 100, 0, 0)
    authorization = _create_authorization(
        pool,
        roles,
        "stale-owner-reclamation",
        budgets=budgets,
    )
    plan = _prepare_plan(
        provider,
        authorization,
        budgets=budgets,
    )
    first_scope = AdvancedStreamScope(
        ingress_plan_id=plan.plan_id,
        cohort_id=plan.cohort_id,
        manifest_id=plan.manifest_id,
        journal_generation=uuid4(),
        owner_id=uuid4(),
        feed=MarketFeed.STOCKS,
        normalizer_version=NORMALIZER_VERSION,
        authorization_reference=plan.authorization_reference,
        connection_attempt=1,
        connection_ceiling=3,
        decoded_byte_ceiling=100,
        stale_after_seconds=60,
        started_at=SESSION_OPEN + timedelta(minutes=40),
    )
    with provider.transaction(APP_ROLE) as unit:
        first_session = register_advanced_stream_session_cursor(
            unit.cursor,
            first_scope,
        )
        update_advanced_stream_session_cursor(
            unit.cursor,
            first_session,
            AdvancedStreamUsage(0, 0, 0, 0, 0, 0, None),
            status="LIVE",
            checked_at=first_scope.started_at,
            terminal=False,
        )

    barrier = Barrier(2)

    def reclaim():
        scope = AdvancedStreamScope(
            ingress_plan_id=plan.plan_id,
            cohort_id=plan.cohort_id,
            manifest_id=plan.manifest_id,
            journal_generation=uuid4(),
            owner_id=uuid4(),
            feed=MarketFeed.STOCKS,
            normalizer_version=NORMALIZER_VERSION,
            authorization_reference=plan.authorization_reference,
            connection_attempt=2,
            connection_ceiling=3,
            decoded_byte_ceiling=100,
            stale_after_seconds=60,
            started_at=SESSION_OPEN + timedelta(minutes=43),
        )
        barrier.wait()
        try:
            with provider.transaction(APP_ROLE) as unit:
                return register_advanced_stream_session_cursor(
                    unit.cursor,
                    scope,
                )
        except ValueError:
            return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        reclaimed = tuple(executor.map(lambda _value: reclaim(), (1, 2)))
    winners = tuple(session for session in reclaimed if session is not None)
    assert len(winners) == 1
    assert winners[0].owner_epoch > first_session.owner_epoch

    with provider.transaction(APP_ROLE) as unit:
        with pytest.raises(ValueError, match="fence rejected"):
            update_advanced_stream_session_cursor(
                unit.cursor,
                first_session,
                AdvancedStreamUsage(0, 0, 0, 0, 0, 0, None),
                status="LIVE",
                checked_at=SESSION_OPEN + timedelta(minutes=44),
                terminal=False,
            )
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT status,failure_code,ended_at "
            "FROM rt.stream_sessions WHERE session_id=%s",
            (str(first_session.session_id),),
        )
        status, failure_code, ended_at = unit.cursor.fetchone()
        unit.cursor.execute(
            "SELECT journal_generation,last_durable_offset,"
            "last_durable_watermark,reconnect_at,recovery_status,reason "
            "FROM rt.stream_gaps WHERE session_id=%s",
            (str(first_session.session_id),),
        )
        gaps = tuple(unit.cursor.fetchall())
    assert status == "FAILED"
    assert failure_code == "STALE_OWNER_RECLAIMED"
    assert ended_at is not None
    assert gaps == ((
        first_session.journal_generation,
        0,
        None,
        SESSION_OPEN + timedelta(minutes=43),
        "UNRESOLVED",
        "STALE_OWNER_RECLAIMED",
    ),)


def test_promotion_requires_active_profile_next_session_and_one_ladder_rung(
    hardened_database,
):
    pool, _provider, roles, _cohort_id, _manifest_id = hardened_database
    parent = _create_authorization(pool, roles, "promotion-parent")
    for target in (9, 25):
        profile = RUNTIME_CATALOG.profile_for_target(target)
        position = RUNTIME_CATALOG.scale_policy.universe_ladder.index(target)
        with _admin_cursor(pool, roles) as cursor:
            register_advanced_runtime_profile_cursor(
                cursor,
                profile,
                runtime_policy_sha256=POLICY_SHA,
                ladder_position=position,
                previous_universe_target=(
                    RUNTIME_CATALOG.scale_policy.universe_ladder[
                        position - 1
                    ]
                ),
            )
            activate_advanced_runtime_profile_cursor(
                cursor,
                profile.sha256,
                stability_evidence_sha256=(
                    f"{target:064x}"[-64:]
                ),
                capacity_evidence_sha256=(
                    f"{target + 1:064x}"[-64:]
                ),
                approval_reference=f"activate-target-{target}",
                activated_at=SESSION_OPEN - timedelta(days=1),
            )
    promoted = build_advanced_stream_authorization(
        parent_authorization_id=parent.authorization_id,
        allowed_universe_targets=(9,),
        valid_from=SESSION_DATE + timedelta(days=1),
        valid_through=SESSION_DATE + timedelta(days=1),
        maximum_budgets=parent.maximum_budgets,
        runtime_policy_sha256=POLICY_SHA,
        approval_source="db-test",
        approval_reference="promotion-to-nine",
    )
    with _admin_cursor(pool, roles) as cursor:
        stored = create_advanced_stream_authorization_cursor(
            cursor,
            promoted,
            promotion_ladder=RUNTIME_CATALOG.scale_policy.universe_ladder,
            calendar=_PromotionCalendar(),
        )
    assert stored.allowed_universe_targets == (9,)

    skipped = build_advanced_stream_authorization(
        parent_authorization_id=parent.authorization_id,
        allowed_universe_targets=(25,),
        valid_from=SESSION_DATE + timedelta(days=1),
        valid_through=SESSION_DATE + timedelta(days=1),
        maximum_budgets=parent.maximum_budgets,
        runtime_policy_sha256=POLICY_SHA,
        approval_source="db-test",
        approval_reference="invalid-skip-to-25",
    )
    with _admin_cursor(pool, roles) as cursor:
        with pytest.raises(ValueError, match="one configured ladder rung"):
            create_advanced_stream_authorization_cursor(
                cursor,
                skipped,
                promotion_ladder=(
                    RUNTIME_CATALOG.scale_policy.universe_ladder
                ),
                calendar=_PromotionCalendar(),
            )


def test_exact_resolution_requires_one_candidate_or_admin_binding_and_pins_profile(
    hardened_database,
):
    pool, provider, roles, cohort_id, manifest_id = hardened_database
    pinned_authorization = _create_authorization(
        pool,
        roles,
        "pinned-before-profile-supersession",
    )
    pinned_plan = _prepare_plan(provider, pinned_authorization)
    missing_request = AdvancedIngressPlanRequest(
        pinned_authorization.authorization_id,
        SESSION_DATE + timedelta(days=1),
        1,
        AdvancedIngressBudgets(2, 2, 100, 1, 10),
    )
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        with pytest.raises(
            AdvancedCohortResolutionError,
            match="COHORT_NOT_PROVISIONED",
        ):
            resolve_advanced_ingress_plan_cursor(
                unit.cursor,
                missing_request,
                runtime_catalog=RUNTIME_CATALOG,
                calendar=_MissingSessionCalendar(),
            )

    duplicate_cohort_id = uuid4()
    with _admin_cursor(pool, roles) as cursor:
        cursor.execute(
            "INSERT INTO rt.cohorts ("
            "cohort_id,cohort_sha256,manifest_id,configuration_sha256,"
            "model_bundle_sha256,valuation_sha256,effective_at,mode"
            ") VALUES (%s,%s,%s,%s,%s,%s,%s,'SHADOW')",
            (
                str(duplicate_cohort_id),
                _digest(),
                str(manifest_id),
                CONFIGURATION_SHA,
                _digest(),
                _digest(),
                SESSION_OPEN - timedelta(days=1),
            ),
        )
    ambiguous_authorization = _create_authorization(
        pool,
        roles,
        "ambiguous-before-admin-binding",
    )
    ambiguous_request = AdvancedIngressPlanRequest(
        ambiguous_authorization.authorization_id,
        SESSION_DATE,
        1,
        AdvancedIngressBudgets(2, 2, 100, 1, 10),
    )
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        with pytest.raises(
            AdvancedCohortResolutionError,
            match="COHORT_AMBIGUOUS",
        ):
            resolve_advanced_ingress_plan_cursor(
                unit.cursor,
                ambiguous_request,
                runtime_catalog=RUNTIME_CATALOG,
                calendar=_Calendar(),
            )
    selected_request = AdvancedIngressPlanRequest(
        ambiguous_authorization.authorization_id,
        SESSION_DATE,
        1,
        AdvancedIngressBudgets(2, 2, 100, 1, 10),
        preferred_cohort_id=cohort_id,
        cohort_selection_reference="explicit-ambiguity-resolution",
    )
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        bound_plan = resolve_advanced_ingress_plan_cursor(
            unit.cursor,
            selected_request,
            runtime_catalog=RUNTIME_CATALOG,
            calendar=_Calendar(),
        )
    assert bound_plan.cohort_id == cohort_id
    assert bound_plan.manifest_id == manifest_id

    changed_profile = replace(
        RUNTIME_PROFILE,
        version="advanced-equity-ingress-1-v2",
        settings=replace(
            RUNTIME_PROFILE.settings,
            heartbeat_interval_seconds=21,
        ),
    )
    with _admin_cursor(pool, roles) as cursor:
        register_advanced_runtime_profile_cursor(
            cursor,
            changed_profile,
            runtime_policy_sha256=POLICY_SHA,
            ladder_position=0,
            previous_universe_target=None,
        )
        activate_advanced_runtime_profile_cursor(
            cursor,
            changed_profile.sha256,
            stability_evidence_sha256="e" * 64,
            capacity_evidence_sha256="f" * 64,
            approval_reference="activate-behavior-v2",
            activated_at=SESSION_OPEN - timedelta(days=1),
        )
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        replay_scope = load_advanced_ingress_runtime_plan_cursor(
            unit.cursor,
            pinned_plan.plan_id,
        )
    assert replay_scope.runtime_profile_sha256 == RUNTIME_PROFILE.sha256
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        with pytest.raises(
            AdvancedCohortResolutionError,
            match="RUNTIME_PROFILE_NOT_ACTIVE",
        ):
            resolve_advanced_ingress_plan_cursor(
                unit.cursor,
                ambiguous_request,
                runtime_catalog=RUNTIME_CATALOG,
                calendar=_Calendar(),
            )
