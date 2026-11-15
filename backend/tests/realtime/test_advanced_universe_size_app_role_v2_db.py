"""G102 R62 (Nov 15): the session activation path runs as the hardened APP role at any universe size.

Nov 13: the 45-name activation crash-looped the controller because the app role could not INSERT the exact-45 runtime
profile, and the only test of that path ran as the database owner. Here a disposable database is provisioned with every
migration (053 registers the size-independent profile ACTIVE) and hardened with the production grants; the controller's
session steps (one standing authorization, plan resolution, plan preparation) then run through `V2ConnectionProvider`
as the app role, exactly as the controller does, for sizes 1, 45, 52 and 400 and for a shrink (45 -> 25) and a growth
(25 -> 100) on consecutive sessions. No admin step runs between provisioning and the sessions. A cohort provisioned
before migration 053 (the live Nov 13 exact-45 configuration) still resolves to its stored profile.

Opt-in: STOCKS_V2_DB_TESTS=1 (see tests/realtime/v2_database.py).
"""
from __future__ import annotations

from contextlib import closing, contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql

from realtime.advanced_equity_lifecycle import ensure_automatic_stream_authorization_cursor
from realtime.advanced_ingress_plan import AdvancedIngressPlanRequest, automatic_advanced_ingress_budgets
from realtime.advanced_ingress_repository import (
    activate_advanced_runtime_profile_cursor,
    prepare_advanced_ingress_plan_cursor,
    register_advanced_runtime_profile_cursor,
    resolve_advanced_ingress_plan_cursor,
)
from realtime.advanced_ingress_runtime_profile import load_advanced_ingress_runtime_catalog
from realtime.database_access_v2 import APP_ROLE, V2AccessConfig, V2ConnectionProvider
from realtime.security_v2 import build_role_names_v2, drop_disposable_roles_v2, harden_database_v2
from scripts.validate_realtime_v2_schema import _connect, _settings
from realtime.universe_limits import SCHEMA_MAX_UNIVERSE_NAMES
from v2_database import disposable_v2_database, requires_v2_database

pytestmark = requires_v2_database
UTC = timezone.utc
CATALOG = load_advanced_ingress_runtime_catalog(
    Path(__file__).resolve().parents[2] / "realtime" / "policies" / "advanced_equity_ingress_v1.json"
)
POLICY = CATALOG.scale_policy.sha256
CURRENT_CONFIGURATION = CATALOG.profile.configuration_sha256(POLICY)
LEGACY_EXACT_45 = next(
    profile for profile in CATALOG.legacy_by_sha.values()
    if profile.version == "advanced-equity-ingress-v1-exact-45-capacity-50"
)
FIRST_SESSION = date(2031, 3, 3)


class _Calendar:
    def session_open(self, session_date):
        return datetime.combine(session_date, datetime.min.time(), tzinfo=UTC) + timedelta(hours=14, minutes=30)

    def session_close(self, session_date):
        return datetime.combine(session_date, datetime.min.time(), tzinfo=UTC) + timedelta(hours=21)


CALENDAR = _Calendar()


def _digest() -> str:
    return uuid4().hex + uuid4().hex


@contextmanager
def _owner_cursor(pool):
    connection = pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                yield cursor
    finally:
        pool.putconn(connection)


@contextmanager
def _admin_cursor(pool, roles):
    with _owner_cursor(pool) as cursor:
        cursor.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(roles.name("admin"))))
        yield cursor


def _instruments(cursor, count: int) -> list[int]:
    """`count` Equity instruments (created once per database, reused by every session)."""
    cursor.execute("SELECT instrument_id FROM rt.instruments WHERE asset_class='EQUITY' ORDER BY instrument_id")
    existing = [int(row[0]) for row in cursor.fetchall()]
    while len(existing) < count:
        security_id = uuid4()
        cursor.execute(
            "INSERT INTO ref.securities (security_id,asset_type,source_origin,created_at) "
            "VALUES (%s,'STOCK','r62-fixture',%s)",
            (str(security_id), datetime(2030, 1, 1, tzinfo=UTC)),
        )
        cursor.execute(
            "INSERT INTO rt.instruments (asset_class,security_id) VALUES ('EQUITY',%s) RETURNING instrument_id",
            (str(security_id),),
        )
        existing.append(int(cursor.fetchone()[0]))
    return existing[:count]


def _provision_session_cohort(pool, session_date: date, size: int, configuration_sha256: str):
    """The control plane the capability refresh and desired-state snapshot provision before a session (owner)."""
    profile_id, manifest_id, cohort_id = uuid4(), uuid4(), uuid4()
    opened = CALENDAR.session_open(session_date)
    with _owner_cursor(pool) as cursor:
        instruments = _instruments(cursor, size)
        cursor.execute(
            "INSERT INTO rt.capability_profiles (profile_id,config_sha256,schema_sha256,account_alias,"
            "capabilities_sha256,universe_sha256,checked_at,expires_at,status,profile_sha256,provider_tier,"
            "security_policy_sha256,preflight_mode,provider_requests,action_authorized) VALUES "
            "(%s,%s,%s,'r62-fixture',%s,%s,%s,%s,'PASS',%s,'MASSIVE_ADVANCED',%s,'PASSIVE',false,false)",
            (str(profile_id), configuration_sha256, _digest(), _digest(), _digest(),
             opened - timedelta(hours=8), CALENDAR.session_close(session_date) + timedelta(days=2),
             _digest(), _digest()),
        )
        cursor.execute(
            "INSERT INTO rt.subscription_manifests (manifest_id,manifest_sha256,universe_sha256,session_date,"
            "effective_at,profile_id,manifest_kind,stock_count,option_underlying_count,member_count,"
            "allocation_policy_sha256) VALUES (%s,%s,%s,%s,%s,%s,'EQUITY_VALIDATION',%s,0,%s,%s)",
            (str(manifest_id), _digest(), _digest(), session_date, opened - timedelta(hours=8),
             str(profile_id), size, size, _digest()),
        )
        for priority, instrument_id in enumerate(instruments, start=1):
            cursor.execute(
                "INSERT INTO rt.subscription_members (manifest_id,instrument_id,channel,priority,purpose,"
                "option_underlying) VALUES (%s,%s,'AM',%s,'PRIMARY',false)",
                (str(manifest_id), instrument_id, min(priority, 32767)),
            )
        cursor.execute(
            "INSERT INTO rt.cohorts (cohort_id,cohort_sha256,manifest_id,configuration_sha256,"
            "model_bundle_sha256,valuation_sha256,effective_at,mode) VALUES (%s,%s,%s,%s,%s,%s,%s,'SHADOW')",
            (str(cohort_id), _digest(), str(manifest_id), configuration_sha256, _digest(), _digest(),
             opened - timedelta(hours=8)),
        )
    return cohort_id


def _activate_session_as_app(provider, session_date: date, size: int, cohort_id):
    """The controller's `_prepare_continuous_session_cursor` steps after the desired snapshot, as the app role."""
    desired = SimpleNamespace(
        equity_member_count=size,
        snapshot=SimpleNamespace(session_date=session_date, snapshot_sha256=_digest(), cohort_id=cohort_id),
    )
    budgets = automatic_advanced_ingress_budgets(
        universe_target=size, stream_sessions=4, connection_attempts=12, decoded_bytes=1024 * 1024 * 1024,
    )
    with provider.transaction(APP_ROLE) as unit:
        authorization = ensure_automatic_stream_authorization_cursor(
            unit.cursor, desired=desired, budgets=budgets, runtime_policy_sha256=POLICY,
        )
        plan = resolve_advanced_ingress_plan_cursor(
            unit.cursor,
            AdvancedIngressPlanRequest(
                authorization.authorization_id, session_date, size, budgets,
                preferred_cohort_id=cohort_id,
                cohort_selection_reference=f"desired-snapshot:{desired.snapshot.snapshot_sha256}",
            ),
            runtime_catalog=CATALOG,
            calendar=CALENDAR,
        )
        prepared = prepare_advanced_ingress_plan_cursor(
            unit.cursor, plan, checked_at=CALENDAR.session_open(session_date) - timedelta(minutes=15),
        )
    return authorization, prepared


@pytest.fixture(scope="module")
def hardened():
    suffix = uuid4().hex[:8]
    roles = build_role_names_v2(f"test_rt_r62_{suffix}")
    roles_owned = False
    try:
        with disposable_v2_database(f"test_realtime_v2_r62_size_{suffix}") as pool:
            connection = pool.getconn()
            try:
                created, audit = harden_database_v2(connection, roles)
                roles_owned = True
                assert len(created) == 3
                assert all(status.startswith("PASS") for _, status in audit)
            finally:
                pool.putconn(connection)
            provider = V2ConnectionProvider(
                {APP_ROLE: pool},
                V2AccessConfig(security_mode="hardened", role_prefix=roles.prefix, allow_disposable=True),
            )
            provider.verify()
            yield pool, provider, roles
    finally:
        if roles_owned:
            with closing(_connect(_settings(), "postgres")) as connection:
                connection.autocommit = True
                drop_disposable_roles_v2(connection, roles)


def test_a_fresh_database_has_only_the_size_independent_profile(hardened):
    pool, _provider, _roles = hardened
    with _owner_cursor(pool) as cursor:
        cursor.execute(
            "SELECT runtime_profile_sha256,universe_target,ladder_position,status "
            "FROM rt.advanced_ingress_runtime_profiles"
        )
        rows = cursor.fetchall()
    assert rows == [(CATALOG.profile.sha256, SCHEMA_MAX_UNIVERSE_NAMES, 0, "ACTIVE")]


@pytest.mark.parametrize(
    ("offset", "size"),
    (
        (0, 1),    # a fresh database with one name, no admin step after provisioning
        (1, 45),
        (2, 25),   # shrink 45 -> 25 on the next session
        (3, 100),  # growth 25 -> 100 on the next session
        (4, 52),
        (5, 400),
    ),
)
def test_app_role_activates_any_size_on_consecutive_sessions(hardened, offset, size):
    pool, provider, _roles = hardened
    session_date = FIRST_SESSION + timedelta(days=offset)
    cohort_id = _provision_session_cohort(pool, session_date, size, CURRENT_CONFIGURATION)

    authorization, plan = _activate_session_as_app(provider, session_date, size, cohort_id)

    assert authorization.allowed_universe_targets == (size,)
    assert authorization.parent_authorization_id is None
    assert (plan.cohort_id, plan.session_date, plan.universe_target) == (cohort_id, session_date, size)
    assert plan.runtime_profile_sha256 == CATALOG.profile.sha256
    assert plan.runtime_profile_version == "advanced-equity-ingress-v6"
    assert plan.configuration_sha256 == CURRENT_CONFIGURATION
    with _owner_cursor(pool) as cursor:
        cursor.execute(
            "SELECT count(*) FROM rt.advanced_stream_authorizations WHERE valid_from=%s", (session_date,)
        )
        assert cursor.fetchone()[0] == 1  # one row, no back-dated ladder chain
        cursor.execute("SELECT count(*) FROM rt.advanced_ingress_runtime_profiles")
        assert cursor.fetchone()[0] == 1  # nothing registered per size


def test_a_cohort_provisioned_before_053_keeps_its_exact_size_profile(hardened):
    """Monday Nov 16: the 45-name day keeps the Nov 13 cohort configuration (exact-45, registered by admin)."""
    pool, provider, roles = hardened
    with _admin_cursor(pool, roles) as cursor:
        register_advanced_runtime_profile_cursor(
            cursor, LEGACY_EXACT_45, runtime_policy_sha256=POLICY, ladder_position=3, previous_universe_target=25,
        )
        activate_advanced_runtime_profile_cursor(
            cursor, LEGACY_EXACT_45.sha256,
            stability_evidence_sha256="c" * 64, capacity_evidence_sha256="d" * 64,
            approval_reference="r62-fixture-legacy-exact-45", activated_at=datetime(2030, 1, 1, tzinfo=UTC),
        )
    session_date = FIRST_SESSION + timedelta(days=20)
    legacy_configuration = LEGACY_EXACT_45.configuration_sha256(POLICY)
    cohort_id = _provision_session_cohort(pool, session_date, 45, legacy_configuration)

    _authorization, plan = _activate_session_as_app(provider, session_date, 45, cohort_id)

    assert plan.runtime_profile_sha256 == LEGACY_EXACT_45.sha256
    assert plan.configuration_sha256 == legacy_configuration
    # The next session changes size: its cohort carries the size-independent configuration.
    grown = FIRST_SESSION + timedelta(days=21)
    grown_cohort = _provision_session_cohort(pool, grown, 52, CURRENT_CONFIGURATION)
    _authorization, grown_plan = _activate_session_as_app(provider, grown, 52, grown_cohort)
    assert grown_plan.runtime_profile_sha256 == CATALOG.profile.sha256


def test_the_app_role_still_cannot_write_runtime_profiles(hardened):
    """The grant model is unchanged: the controller no longer needs this write, and it is still refused."""
    _pool, provider, _roles = hardened
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        with provider.transaction(APP_ROLE) as unit:
            register_advanced_runtime_profile_cursor(
                unit.cursor,
                next(iter(CATALOG.legacy_by_sha.values())),
                runtime_policy_sha256=POLICY,
                ladder_position=0,
                previous_universe_target=None,
            )
