"""DB-backed option slot sets and option manifests on the V2 baseline (opt-in: STOCKS_V2_DB_TESTS=1, no offline guard).

Uses the disposable database test_realtime_v2_slot_a only; it is always dropped. Part one drives the slot-set
repository on the real tables with synthetic reference rows. Part two runs the real staged pre-session package
persistence and `persist_option_manifest_selection` end to end and reads the active slot set back.
"""
from __future__ import annotations

import itertools
from contextlib import closing
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from psycopg2 import errors
from psycopg2 import sql
from psycopg2.extras import execute_values

from advanced_options_reference_support import reference_state
from realtime.advanced_equity_control_plane import (
    build_advanced_equity_control_plane_plan,
    persist_advanced_equity_control_plane_cursor,
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
    build_advanced_options_enrollment_plan,
    persist_advanced_options_enrollment_cursor,
)
from realtime.contracts import MarketChannel, UniverseManifest
from realtime.option_manifest_repository import (
    option_manifest_selection_id,
    option_subscription_manifest_id,
    persist_option_manifest_selection,
)
from realtime.option_pre_session_target import persist_option_pre_session_package_cursor
from realtime.option_session_manifest import (
    BoundedOptionManifestPolicy,
    ContractSelectionStatus,
    ManifestSelectionStatus,
    OptionActivityContract,
    OptionActivitySelectionRecord,
    build_bounded_option_session_manifest,
)
from realtime.option_slot_set import (
    MAXIMUM_SLOT_SET_CONTRACTS,
    SlotSetKind,
    SlotSetMember,
    SlotSetPurpose,
    SlotSetVersion,
    option_universe_manifest_sha256,
)
from realtime.option_slot_set_repository import (
    activate_slot_set_cursor,
    load_active_slot_set_cursor,
    load_slot_set_version_cursor,
    persist_and_activate_slot_set,
    persist_slot_set_version_cursor,
)
from realtime.preflight import (
    DetectorFamily,
    ProductEntitlementProfile,
    ProviderTier,
)
from realtime.provider_budget import ProviderBudgetScope
from realtime.security_v2 import (
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from tests.realtime.test_option_pre_session_producer import _packages, _security_id
from tests.realtime.test_option_session_manifest import (
    MARKET_TIME,
    SESSION_DATE,
    _candidates,
    _developer_policy,
    _scope,
)
from tests.realtime.v2_database import (
    _connect,
    _settings,
    disposable_v2_database,
    insert_cohort,
    requires_v2_database,
)

pytestmark = requires_v2_database

DATABASE_NAME = "test_realtime_v2_slot_a"
UTC = timezone.utc
T, Q = MarketChannel.TRADE, MarketChannel.QUOTE
POLICY = "a" * 64
_blocks = itertools.count()


@pytest.fixture(scope="module")
def pool():
    with disposable_v2_database(DATABASE_NAME) as database_pool:
        yield database_pool


@contextmanager
def cursor(pool):
    """A committed transaction on one pooled connection."""
    connection = pool.getconn()
    try:
        with connection.cursor() as database_cursor:
            yield database_cursor
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        pool.putconn(connection)


@contextmanager
def connection(pool):
    """A pooled connection for the repository wrappers that commit or roll back themselves."""
    pooled = pool.getconn()
    try:
        yield pooled
    finally:
        pooled.rollback()
        pool.putconn(pooled)


def _digest() -> str:
    return uuid4().hex + uuid4().hex


@dataclass(frozen=True)
class Contract:
    instrument_id: int
    contract_id: int
    version_id: int


@dataclass(frozen=True)
class OptionWorld:
    cohort_id: UUID
    manifest_id: UUID
    underlying_security: UUID
    contracts: tuple[Contract, ...]
    now: datetime


def _insert_security(database_cursor, now) -> UUID:
    security = uuid4()
    database_cursor.execute(
        "INSERT INTO ref.securities (security_id, asset_type, source_origin, created_at) "
        "VALUES (%s, 'STOCK', 'fixture', %s)", (str(security), now - timedelta(days=40)))
    return security


def _insert_contracts(database_cursor, security: UUID, count: int, now) -> tuple[Contract, ...]:
    base = 1_000_000 + next(_blocks) * 5_000
    ids = [base + index for index in range(count)]
    execute_values(
        database_cursor,
        "INSERT INTO ref.option_contracts (contract_id, underlying_security_id, provider, contract_ticker, "
        "first_observed_at, source_origin) VALUES %s",
        [(contract_id, str(security), "polygon", f"O:SLOT{contract_id}", now, "fixture") for contract_id in ids])
    execute_values(
        database_cursor,
        "INSERT INTO ref.option_contract_versions (version_id, contract_id, call_put, expiration, strike, "
        "exercise_style, multiplier, eligibility, effective_from, observed_at, payload_sha256) VALUES %s",
        [(contract_id + 5_000_000, contract_id, "C", (now + timedelta(days=30)).date(), 100 + index, "AMERICAN",
          100, "VALIDATED_ACTIVE", now - timedelta(days=1), now - timedelta(days=1), _digest())
         for index, contract_id in enumerate(ids)])
    execute_values(
        database_cursor,
        "INSERT INTO rt.instruments (asset_class, contract_id) VALUES %s RETURNING instrument_id, contract_id",
        [("OPTION", contract_id) for contract_id in ids], fetch=True)
    database_cursor.execute(
        "SELECT instrument_id, contract_id FROM rt.instruments WHERE contract_id=ANY(%s) ORDER BY contract_id",
        (ids,))
    return tuple(Contract(int(instrument), int(contract), int(contract) + 5_000_000)
                 for instrument, contract in database_cursor.fetchall())


def new_world(pool, contracts: int = 8) -> OptionWorld:
    """A MARKET_OPTIONS cohort whose manifest declares one option underlying, plus its option contracts."""
    now = datetime.now(UTC).replace(microsecond=0)
    with cursor(pool) as database_cursor:
        cohort = insert_cohort(database_cursor, now)
        database_cursor.execute("SELECT manifest_id FROM rt.cohorts WHERE cohort_id=%s", (str(cohort),))
        manifest = database_cursor.fetchone()[0]
        security = _insert_security(database_cursor, now)
        database_cursor.execute(
            "INSERT INTO rt.instruments (asset_class, security_id) VALUES ('EQUITY', %s) RETURNING instrument_id",
            (str(security),))
        underlying_instrument = database_cursor.fetchone()[0]
        database_cursor.execute(
            "INSERT INTO rt.subscription_members (manifest_id, instrument_id, channel, priority, purpose, "
            "option_underlying) VALUES (%s, %s, 'AM', 100, 'PRIMARY', true)", (manifest, underlying_instrument))
        options = _insert_contracts(database_cursor, security, contracts, now)
    return OptionWorld(cohort, UUID(str(manifest)), security, options, now)


def members(contracts, purpose=SlotSetPurpose.PRIMARY, channels=(T, Q), priority=100):
    return tuple(SlotSetMember(row.instrument_id, channel, purpose, priority, row.version_id)
                 for row in contracts for channel in channels)


def slot_set(world, contracts, *, version=1, kind=None, effective_at=None, member_rows=None, policy=POLICY):
    kind = kind or (SlotSetKind.PRE_SESSION if version == 1 else SlotSetKind.REFRESH)
    return SlotSetVersion(world.cohort_id, version, kind, effective_at or world.now, policy,
                          member_rows if member_rows is not None else members(contracts))


def _count(pool, relation: str, world: OptionWorld) -> int:
    with cursor(pool) as database_cursor:
        database_cursor.execute(f"SELECT count(*) FROM {relation} WHERE cohort_id=%s", (str(world.cohort_id),))
        return database_cursor.fetchone()[0]


def _current(pool, world: OptionWorld):
    with cursor(pool) as database_cursor:
        database_cursor.execute(
            "SELECT version, activated_at FROM rt.slot_set_current WHERE cohort_id=%s", (str(world.cohort_id),))
        return database_cursor.fetchone()


def test_slot_set_persists_activates_and_reads_back_with_verified_hash(pool):
    world = new_world(pool)
    expected = slot_set(world, world.contracts[:6])

    with connection(pool) as pooled:
        assert persist_and_activate_slot_set(pooled, expected) is True

    with cursor(pool) as database_cursor:
        database_cursor.execute(
            "SELECT kind, effective_at, policy_sha256, member_sha256, primary_contract_count, "
            "reserve_contract_count FROM rt.slot_set_versions WHERE cohort_id=%s AND version=1",
            (str(world.cohort_id),))
        assert database_cursor.fetchone() == (
            "PRE_SESSION", world.now, POLICY, expected.member_sha256, 6, 0)
        active = load_active_slot_set_cursor(database_cursor, world.cohort_id)
    assert active.slot_set == expected and active.activated_at == world.now
    assert _count(pool, "rt.slot_set_members", world) == 12
    assert _current(pool, world) == (1, world.now)


def test_identical_replay_is_a_no_op_and_a_divergent_replay_raises(pool):
    world = new_world(pool)
    original = slot_set(world, world.contracts[:5])
    with connection(pool) as pooled:
        persist_and_activate_slot_set(pooled, original)
    before = (_count(pool, "rt.slot_set_versions", world), _count(pool, "rt.slot_set_members", world))

    with connection(pool) as pooled:
        assert persist_and_activate_slot_set(pooled, original) is False

    divergent = (
        slot_set(world, world.contracts[:6]),
        slot_set(world, (), member_rows=tuple(replace(row, priority=7) for row in original.members)),
        slot_set(world, (), member_rows=members(world.contracts[:5], SlotSetPurpose.RESERVE)),
        slot_set(world, world.contracts[:5], policy="b" * 64),
    )
    for candidate in divergent:
        with connection(pool) as pooled:
            with pytest.raises(ValueError, match="divergent replay"):
                persist_and_activate_slot_set(pooled, candidate)
    assert (_count(pool, "rt.slot_set_versions", world), _count(pool, "rt.slot_set_members", world)) == before
    with cursor(pool) as database_cursor:
        assert load_slot_set_version_cursor(database_cursor, world.cohort_id, 1) == original


def test_refresh_is_make_before_break_never_backwards_and_contiguous(pool):
    world = new_world(pool)
    first = slot_set(world, world.contracts[:5])
    refresh = slot_set(world, world.contracts[2:8], version=2, effective_at=world.now + timedelta(minutes=5))
    with connection(pool) as pooled:
        persist_and_activate_slot_set(pooled, first)

    with connection(pool) as pooled:
        with pytest.raises(ValueError, match="contiguous"):
            persist_and_activate_slot_set(pooled, replace(refresh, version=3))
    with cursor(pool) as database_cursor:
        assert persist_slot_set_version_cursor(database_cursor, refresh) == refresh
    assert _current(pool, world) == (1, world.now)

    later = world.now + timedelta(minutes=6)
    with cursor(pool) as database_cursor:
        assert activate_slot_set_cursor(database_cursor, world.cohort_id, 2, activated_at=later) is True
    assert _current(pool, world) == (2, later)
    with cursor(pool) as database_cursor:
        assert load_active_slot_set_cursor(database_cursor, world.cohort_id).slot_set == refresh

    with pytest.raises(ValueError, match="backwards"):
        with cursor(pool) as database_cursor:
            activate_slot_set_cursor(database_cursor, world.cohort_id, 1, activated_at=later + timedelta(minutes=1))
    assert _current(pool, world) == (2, later)
    assert _count(pool, "rt.slot_set_versions", world) == 2


def test_a_failed_refresh_rolls_back_the_version_and_leaves_the_pointer(pool):
    world = new_world(pool)
    with connection(pool) as pooled:
        persist_and_activate_slot_set(pooled, slot_set(world, world.contracts[:4]))
    wrong_catalog = tuple(
        replace(row, catalog_version_id=world.contracts[7].version_id) if row.instrument_id == world.contracts[0].instrument_id
        else row for row in members(world.contracts[:5]))

    with connection(pool) as pooled:
        with pytest.raises(ValueError, match="does not belong to its instrument"):
            persist_and_activate_slot_set(
                pooled, slot_set(world, (), version=2, member_rows=wrong_catalog,
                                 effective_at=world.now + timedelta(minutes=1)))

    assert _count(pool, "rt.slot_set_versions", world) == 1
    assert _current(pool, world) == (1, world.now)


def test_member_identity_is_checked_against_the_instrument_and_the_cohort_universe(pool):
    world = new_world(pool, contracts=3)
    with cursor(pool) as database_cursor:
        outside_security = _insert_security(database_cursor, world.now)
        outside = _insert_contracts(database_cursor, outside_security, 1, world.now)[0]
        database_cursor.execute(
            "INSERT INTO rt.instruments (asset_class, security_id) VALUES ('EQUITY', %s) RETURNING instrument_id",
            (str(outside_security),))
        equity_instrument = database_cursor.fetchone()[0]
    cases = {
        "outside the cohort option universe": members((outside,)),
        "does not belong to its instrument": members((replace(world.contracts[0], version_id=world.contracts[1].version_id),)),
        "catalog version is absent": members((replace(world.contracts[0], version_id=9_999_999_999),)),
    }
    for message, member_rows in cases.items():
        with pytest.raises(ValueError, match=message):
            with cursor(pool) as database_cursor:
                persist_slot_set_version_cursor(database_cursor, slot_set(world, (), member_rows=member_rows))
    equity_member = (SlotSetMember(equity_instrument, Q, SlotSetPurpose.PRIMARY, 100, world.contracts[0].version_id),)
    with pytest.raises(ValueError, match="does not belong to its instrument"):
        with cursor(pool) as database_cursor:
            persist_slot_set_version_cursor(database_cursor, slot_set(world, (), member_rows=equity_member))
    assert _count(pool, "rt.slot_set_versions", world) == 0


def test_slot_set_requires_a_market_options_cohort_with_option_underlyings(pool):
    now = datetime.now(UTC).replace(microsecond=0)
    with cursor(pool) as database_cursor:
        profile, manifest, cohort = uuid4(), uuid4(), uuid4()
        database_cursor.execute(
            "INSERT INTO rt.capability_profiles (profile_id, config_sha256, schema_sha256, account_alias, "
            "capabilities_sha256, universe_sha256, checked_at, expires_at, status, profile_sha256, provider_tier, "
            "security_policy_sha256, preflight_mode, provider_requests, action_authorized) VALUES "
            "(%s,%s,%s,'fixture',%s,%s,%s,%s,'PASS',%s,'MASSIVE_DEVELOPER',%s,'PASSIVE',false,false)",
            (str(profile), _digest(), _digest(), _digest(), _digest(), now, now + timedelta(hours=1), _digest(),
             _digest()))
        database_cursor.execute(
            "INSERT INTO rt.subscription_manifests (manifest_id, manifest_sha256, universe_sha256, session_date, "
            "effective_at, profile_id, manifest_kind, stock_count, option_underlying_count, member_count, "
            "allocation_policy_sha256) VALUES (%s,%s,%s,%s,%s,%s,'EQUITY_VALIDATION',9,0,27,%s)",
            (str(manifest), _digest(), _digest(), now.date(), now, str(profile), _digest()))
        database_cursor.execute(
            "INSERT INTO rt.cohorts (cohort_id, cohort_sha256, manifest_id, configuration_sha256, "
            "model_bundle_sha256, valuation_sha256, effective_at, mode) VALUES (%s,%s,%s,%s,%s,%s,%s,'SHADOW')",
            (str(cohort), _digest(), str(manifest), _digest(), _digest(), _digest(), now))
        security = _insert_security(database_cursor, now)
        contract = _insert_contracts(database_cursor, security, 1, now)[0]
    candidate = SlotSetVersion(cohort, 1, SlotSetKind.PRE_SESSION, now, POLICY, members((contract,)))
    with pytest.raises(ValueError, match="MARKET_OPTIONS"):
        with cursor(pool) as database_cursor:
            persist_slot_set_version_cursor(database_cursor, candidate)

    bare = new_world(pool, contracts=1)
    with pytest.raises(ValueError, match="cohort is absent"):
        with cursor(pool) as database_cursor:
            persist_slot_set_version_cursor(database_cursor, replace(
                slot_set(bare, bare.contracts), cohort_id=uuid4()))


def test_the_thousand_contract_cap_is_a_python_gate_in_front_of_the_database_check(pool):
    world = new_world(pool, contracts=MAXIMUM_SLOT_SET_CONTRACTS + 1)
    primary, reserve = world.contracts[:600], world.contracts[600:1_000]
    full = slot_set(world, (), member_rows=members(primary) + members(reserve, SlotSetPurpose.RESERVE))

    with connection(pool) as pooled:
        assert persist_and_activate_slot_set(pooled, full) is True

    with cursor(pool) as database_cursor:
        stored = load_slot_set_version_cursor(database_cursor, world.cohort_id, 1)
    assert stored == full
    assert (stored.primary_contract_count, stored.reserve_contract_count) == (600, 400)
    assert _count(pool, "rt.slot_set_members", world) == 2_000

    with pytest.raises(ValueError, match="1000 contract provider cap"):
        slot_set(world, world.contracts)
    with pytest.raises(ValueError, match="provider cap"):
        slot_set(world, (), member_rows=members(primary) + members(reserve, SlotSetPurpose.RESERVE)
                 + members(world.contracts[1_000:], SlotSetPurpose.RETAINED_ALERT), version=2,
                 kind=SlotSetKind.REFRESH)
    assert _count(pool, "rt.slot_set_versions", world) == 1

    with pytest.raises(errors.CheckViolation):
        with cursor(pool) as database_cursor:
            database_cursor.execute(
                "INSERT INTO rt.slot_set_versions (cohort_id, version, kind, effective_at, policy_sha256, "
                "member_sha256, primary_contract_count, reserve_contract_count) "
                "VALUES (%s, 2, 'REFRESH', %s, %s, %s, 601, 400)",
                (str(world.cohort_id), world.now, POLICY, POLICY))


def test_slot_set_rows_are_immutable_in_the_database(pool):
    world = new_world(pool, contracts=2)
    with connection(pool) as pooled:
        persist_and_activate_slot_set(pooled, slot_set(world, world.contracts))
    cohort = str(world.cohort_id)
    for statement, parameters in (
        ("UPDATE rt.slot_set_versions SET policy_sha256 = %s WHERE cohort_id = %s", ("c" * 64, cohort)),
        ("DELETE FROM rt.slot_set_versions WHERE cohort_id = %s", (cohort,)),
        ("UPDATE rt.slot_set_members SET priority = 5 WHERE cohort_id = %s", (cohort,)),
        ("DELETE FROM rt.slot_set_members WHERE cohort_id = %s", (cohort,)),
    ):
        with pytest.raises(errors.RaiseException):
            with cursor(pool) as database_cursor:
                database_cursor.execute(statement, parameters)
    assert _count(pool, "rt.slot_set_members", world) == 4


# ---- Part two: the real staged pre-session persistence feeding the selection ------------------------------

@dataclass(frozen=True)
class PreSession:
    plan: object
    effective_at: datetime
    cohort_id: UUID
    manifest_id: UUID
    tickers: tuple[str, ...]


@pytest.fixture(scope="module")
def pre_session(pool):
    universe = UniverseManifest("slot-db-pre-session-v1", ("SLA", "SLB"), ("SLA", "SLB"))
    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, universe)
    policy = _developer_policy()
    candidates = _candidates(universe, policy.target_dtes, policy.strikes_per_side_per_expiry)
    packages = _packages(scope, policy, candidates)
    effective_at = MARKET_TIME - timedelta(minutes=30)
    now = datetime.now(UTC)
    with cursor(pool) as database_cursor:
        database_cursor.execute(
            "INSERT INTO rt.capability_profiles (profile_id, config_sha256, schema_sha256, account_alias, "
            "capabilities_sha256, universe_sha256, checked_at, expires_at, status, profile_sha256, provider_tier, "
            "security_policy_sha256, preflight_mode, provider_requests, action_authorized) VALUES "
            "(%s,%s,%s,'fixture',%s,%s,%s,%s,'UNAVAILABLE',%s,'MASSIVE_DEVELOPER',%s,'PASSIVE',false,false)",
            (str(scope.profile_id), _digest(), _digest(), _digest(), universe.sha256, now,
             now + timedelta(hours=1), scope.profile_sha256, _digest()))
        for ticker in universe.equity_tickers:
            security = _security_id(ticker)
            database_cursor.execute(
                "INSERT INTO ref.securities (security_id, asset_type, source_origin, created_at) "
                "VALUES (%s, 'STOCK', 'fixture', %s)", (str(security), effective_at - timedelta(days=400)))
            database_cursor.execute(
                "INSERT INTO ref.security_revisions (revision_id, security_id, ticker, calendar_id, "
                "effective_from, observed_at, payload_sha256) VALUES (%s, %s, %s, 'XNYS', %s, %s, %s)",
                (str(uuid4()), str(security), ticker, effective_at - timedelta(days=399),
                 effective_at - timedelta(days=399), _digest()))
            database_cursor.execute(
                "INSERT INTO rt.instruments (asset_class, security_id) VALUES ('EQUITY', %s)", (str(security),))
        database_cursor.execute("SELECT rt.ensure_partitions('rt.option_discovery_pages', %s)", (SESSION_DATE,))
    with cursor(pool) as database_cursor:
        for package in packages:
            persist_option_pre_session_package_cursor(database_cursor, package)
    plan = build_bounded_option_session_manifest(scope, policy, candidates, session_date=SESSION_DATE)
    assert plan.status is ManifestSelectionStatus.READY and plan.selected_contract_count == 32
    with connection(pool) as pooled:
        assert persist_option_manifest_selection(pooled, plan, effective_at=effective_at) \
            == option_manifest_selection_id(plan)
    manifest_id = option_subscription_manifest_id(plan)
    with cursor(pool) as database_cursor:
        database_cursor.execute("SELECT cohort_id FROM rt.cohorts WHERE manifest_id=%s", (str(manifest_id),))
        (cohort_id,), = database_cursor.fetchall()
    return PreSession(plan, effective_at, UUID(str(cohort_id)), manifest_id, universe.equity_tickers)


def test_selection_persists_an_equity_only_manifest(pool, pre_session):
    plan = pre_session.plan
    with cursor(pool) as database_cursor:
        database_cursor.execute(
            "SELECT manifest_sha256, manifest_kind, stock_count, option_underlying_count, member_count, "
            "allocation_policy_sha256, session_date FROM rt.subscription_manifests WHERE manifest_id=%s",
            (str(pre_session.manifest_id),))
        assert database_cursor.fetchone() == (
            option_universe_manifest_sha256(plan.manifest), "MARKET_OPTIONS", 2, 2, 6,
            plan.policy.sha256, SESSION_DATE)
        database_cursor.execute(
            "SELECT member.channel, member.purpose, member.option_underlying, instrument.asset_class "
            "FROM rt.subscription_members AS member JOIN rt.instruments AS instrument "
            "ON instrument.instrument_id=member.instrument_id WHERE member.manifest_id=%s", (str(pre_session.manifest_id),))
        rows = database_cursor.fetchall()
    assert sorted(rows) == sorted(
        [(channel, "PRIMARY", True, "EQUITY") for _ in range(2) for channel in ("AM", "Q", "T")])
    assert plan.manifest.sha256 != option_universe_manifest_sha256(plan.manifest)


def test_selection_persists_the_pre_session_slot_set_and_activates_it(pool, pre_session):
    plan = pre_session.plan
    with cursor(pool) as database_cursor:
        active = load_active_slot_set_cursor(database_cursor, pre_session.cohort_id)
        included = {row.contract_ticker for row in plan.dispositions
                    if row.status is ContractSelectionStatus.INCLUDED}
        database_cursor.execute(
            "SELECT contract.contract_ticker, member.channel FROM rt.slot_set_members AS member "
            "JOIN rt.instruments AS instrument ON instrument.instrument_id=member.instrument_id "
            "JOIN ref.option_contracts AS contract ON contract.contract_id=instrument.contract_id "
            "JOIN ref.option_contract_versions AS version ON version.version_id=member.catalog_version_id "
            "AND version.contract_id=contract.contract_id WHERE member.cohort_id=%s AND member.version=1",
            (str(pre_session.cohort_id),))
        joined = database_cursor.fetchall()
    slot_set = active.slot_set
    assert (slot_set.version, slot_set.kind, active.activated_at) == (1, SlotSetKind.PRE_SESSION, pre_session.effective_at)
    assert slot_set.policy_sha256 == plan.policy.sha256
    assert (slot_set.primary_contract_count, slot_set.reserve_contract_count, len(slot_set.members)) == (32, 0, 64)
    assert {row.purpose for row in slot_set.members} == {SlotSetPurpose.PRIMARY}
    assert {ticker for ticker, _ in joined} == included and len(joined) == 64
    assert {channel for _, channel in joined} == {"T", "Q"}


def test_selection_audit_and_cohort_are_bound_to_the_equity_manifest(pool, pre_session):
    plan = pre_session.plan
    with cursor(pool) as database_cursor:
        database_cursor.execute(
            "SELECT manifest_id::text, status, selected_contract_count, primary_option_quote_contracts, "
            "reserved_option_quote_contracts, maximum_option_quote_contracts "
            "FROM rt.option_manifest_selection_audits WHERE selection_id=%s", (str(option_manifest_selection_id(plan)),))
        assert database_cursor.fetchone() == (
            str(pre_session.manifest_id), "READY", 32, 800, 200, 1_000)
        database_cursor.execute("SELECT count(*), min(mode) FROM rt.cohorts WHERE manifest_id=%s",
                                (str(pre_session.manifest_id),))
        assert database_cursor.fetchone() == (1, "SHADOW")


def test_replaying_the_selection_changes_nothing(pool, pre_session):
    def snapshot():
        with cursor(pool) as database_cursor:
            totals = []
            for relation in ("rt.subscription_manifests", "rt.subscription_members", "rt.cohorts",
                             "rt.option_manifest_selection_audits", "rt.option_manifest_contract_dispositions",
                             "rt.option_manifest_coverage", "rt.slot_set_versions", "rt.slot_set_members",
                             "rt.slot_set_current"):
                database_cursor.execute(f"SELECT count(*) FROM {relation}")
                totals.append(database_cursor.fetchone()[0])
            return totals, load_active_slot_set_cursor(database_cursor, pre_session.cohort_id)

    before = snapshot()
    with connection(pool) as pooled:
        persist_option_manifest_selection(pooled, pre_session.plan, effective_at=pre_session.effective_at)
    assert snapshot() == before


def test_a_refresh_keeps_the_manifest_and_cohort_and_moves_only_the_pointer(pool, pre_session):
    with cursor(pool) as database_cursor:
        first = load_active_slot_set_cursor(database_cursor, pre_session.cohort_id).slot_set
    kept = sorted({row.instrument_id for row in first.members})[:20]
    retained = set(kept[:3])
    refresh_members = tuple(
        replace(row, purpose=SlotSetPurpose.RETAINED_ALERT) if row.instrument_id in retained else row
        for row in first.members if row.instrument_id in set(kept))
    refresh = SlotSetVersion(pre_session.cohort_id, 2, SlotSetKind.REFRESH,
                             pre_session.effective_at + timedelta(minutes=10), "d" * 64, refresh_members)

    with connection(pool) as pooled:
        assert persist_and_activate_slot_set(pooled, refresh) is True

    with cursor(pool) as database_cursor:
        active = load_active_slot_set_cursor(database_cursor, pre_session.cohort_id)
        assert load_slot_set_version_cursor(database_cursor, pre_session.cohort_id, 1) == first
        database_cursor.execute("SELECT count(*) FROM rt.subscription_manifests WHERE manifest_id=%s",
                                (str(pre_session.manifest_id),))
        assert database_cursor.fetchone()[0] == 1
        database_cursor.execute("SELECT count(*) FROM rt.cohorts WHERE manifest_id=%s", (str(pre_session.manifest_id),))
        assert database_cursor.fetchone()[0] == 1
    assert active.slot_set == refresh and active.slot_set.reserve_contract_count == 0
    assert (active.slot_set.primary_contract_count, active.slot_set.contract_count) == (17, 20)

    # An idempotent retry of the pre-session selection must not move the pointer back to version 1.
    with connection(pool) as pooled:
        persist_option_manifest_selection(pooled, pre_session.plan, effective_at=pre_session.effective_at)
    with cursor(pool) as database_cursor:
        assert load_active_slot_set_cursor(database_cursor, pre_session.cohort_id).slot_set == refresh
    with pytest.raises(ValueError, match="backwards"):
        with cursor(pool) as database_cursor:
            activate_slot_set_cursor(database_cursor, pre_session.cohort_id, 1,
                                     activated_at=pre_session.effective_at + timedelta(hours=1))


@contextmanager
def _role_cursor(pool, role_name):
    connection_value = pool.getconn()
    try:
        with connection_value:
            with connection_value.cursor() as database_cursor:
                database_cursor.execute(
                    sql.SQL("SET LOCAL ROLE {}").format(
                        sql.Identifier(role_name)
                    )
                )
                yield database_cursor
    finally:
        pool.putconn(connection_value)


def _owp4_enrollment(
    pool,
    *,
    flow_policy_sha256="2" * 64,
    condition_semantics_sha256="1" * 64,
    security_free_float=None,
    provider_request_ceiling=1,
    provider_decoded_byte_ceiling=1,
    valuation_policy_sha256="3" * 64,
    activity_valid_until=None,
    session=None,
    checked=None,
    planned=None,
    cutoff=None,
    expires=None,
):
    custom_clocks = any(
        value is not None
        for value in (session, checked, planned, cutoff, expires)
    )
    session = session or date(2026, 10, 16)
    checked = checked or datetime(2026, 10, 16, 12, 0, tzinfo=UTC)
    planned = planned or datetime(2026, 10, 16, 13, 0, tzinfo=UTC)
    cutoff = cutoff or datetime(2026, 10, 16, 14, 30, tzinfo=UTC)
    expires = expires or datetime(2026, 10, 16, 23, 0, tzinfo=UTC)
    security_ids = {"AAPL": uuid4(), "SPY": uuid4()}
    catalog_payload_sha256 = _digest()
    contract_id = 9_100_001
    version_id = 14_100_001
    with cursor(pool) as database_cursor:
        for ticker, security_id in security_ids.items():
            database_cursor.execute(
                "INSERT INTO ref.securities ("
                "security_id,asset_type,source_origin,created_at) "
                "VALUES (%s,'STOCK','owp4-fixture',%s)",
                (str(security_id), checked - timedelta(days=10)),
            )
            database_cursor.execute(
                "INSERT INTO ref.security_revisions ("
                "revision_id,security_id,ticker,calendar_id,"
                "effective_from,observed_at,payload_sha256,free_float) VALUES ("
                "%s,%s,%s,'XNYS',%s,%s,%s,%s)",
                (
                    str(uuid4()),
                    str(security_id),
                    ticker,
                    checked - timedelta(days=9),
                    checked - timedelta(days=9),
                    _digest(),
                    (
                        security_free_float
                        if ticker == "SPY"
                        else None
                    ),
                ),
            )
        equity = build_advanced_equity_control_plane_plan(
            session_date=session,
            security_ids=security_ids,
            entitlement_evidence_sha256="a" * 64,
            authorization_reference="owp4-equity-source",
            checked_at=checked,
            expires_at=expires,
            schema_sha256="b" * 64,
            security_policy_sha256="c" * 64,
        )
        persist_advanced_equity_control_plane_cursor(
            database_cursor,
            equity,
        )
        database_cursor.execute(
            "INSERT INTO ref.option_contracts ("
            "contract_id,underlying_security_id,provider,contract_ticker,"
            "first_observed_at,source_origin) VALUES ("
            "%s,%s,'massive','O:SPY261120C00500000',%s,'owp4-fixture')",
            (contract_id, str(security_ids["SPY"]), checked),
        )
        database_cursor.execute(
            "INSERT INTO ref.option_contract_versions ("
            "version_id,contract_id,call_put,expiration,strike,"
            "exercise_style,multiplier,deliverables,eligibility,"
            "effective_from,observed_at,payload_sha256) VALUES ("
            "%s,%s,'C','2026-11-20',500,'AMERICAN',100,'[]'::jsonb,"
            "'VALIDATED_ACTIVE',%s,%s,%s)",
            (
                version_id,
                contract_id,
                checked,
                planned - timedelta(minutes=3),
                catalog_payload_sha256,
            ),
        )
        database_cursor.execute(
            "INSERT INTO rt.instruments (asset_class,contract_id) "
            "VALUES ('OPTION',%s) RETURNING instrument_id",
            (contract_id,),
        )
        instrument_id = int(database_cursor.fetchone()[0])
    policy = BoundedOptionManifestPolicy(
        version="owp4-primary-activity-v1",
        target_dtes=(21,),
        maximum_target_dte_deviation=0,
        strikes_per_side_per_expiry=1,
        minimum_dte=0,
        maximum_dte=60,
        strike_corridor_fraction=Decimal("0.20"),
        minimum_day_volume=100,
        minimum_open_interest=100,
        maximum_contracts_per_underlying=2,
        maximum_option_quote_contracts=2,
        reserved_option_quote_contracts=0,
        required_detectors=(
            DetectorFamily.S1,
            DetectorFamily.S2,
            DetectorFamily.O1,
            DetectorFamily.O2,
            DetectorFamily.O3,
        ),
    )
    pins = AdvancedOptionsPolicyPins(
        condition_semantics_sha256=condition_semantics_sha256,
        flow_policy_sha256=flow_policy_sha256,
        valuation_policy_sha256=valuation_policy_sha256,
        model_bundle_sha256="4" * 64,
        catalog_sha256="5" * 64,
        slot_set_policy_sha256="6" * 64,
    )
    slot_members = tuple(
        SlotSetMember(
            instrument_id,
            channel,
            SlotSetPurpose.PRIMARY,
            100,
            version_id,
        )
        for channel in ADVANCED_OPTIONS_CHANNELS
    )
    state = (
        reference_state(
            session_date=session,
            known_at=checked,
            valid_until=expires,
        )
        if custom_clocks
        else reference_state()
    )
    control = build_advanced_options_control_plane_plan(
        equity_plan=equity,
        entitlement=ProductEntitlementProfile(
            ProviderTier.MASSIVE_ADVANCED,
            ProviderTier.MASSIVE_ADVANCED,
        ),
        option_underlyings=("SPY",),
        options_entitlement_evidence_sha256="0" * 64,
        options_authorization_reference="owp4-options-scope",
        checked_at=checked,
        expires_at=expires,
        planned_at=planned,
        manifest_policy=policy,
        slot_members=slot_members,
        slot_effective_at=planned,
        feed_scope=AdvancedOptionsFeedScope(),
        pins=pins,
        socket_budget=AdvancedOptionsSocketBudget(
            stream_sessions=1,
            connection_attempts=1,
            reconnects=0,
            decoded_messages=1,
            decoded_bytes=1,
        ),
        provider_budget=ProviderBudgetScope(
            account_alias="MASSIVE_ADVANCED",
            session_date=session,
            authorization_reference="owp4-options-scope",
            provider_authorization_reference="external-selection-only",
            expected_cost="zero provider calls by enrollment",
            request_ceiling=provider_request_ceiling,
            decoded_byte_ceiling=provider_decoded_byte_ceiling,
            lanes=ADVANCED_OPTIONS_REST_LANES,
            authorized_until=expires,
        ),
        owners=AdvancedOptionsOwnerNames(),
        journal_generation=uuid4(),
        stream_session_ids=(uuid4(),),
        receipt_inputs=AdvancedOptionsReceiptInputs(
            cutoff=cutoff,
            reference_state=state,
            equity_input_receipt_id=uuid4(),
            equity_input_sha256="7" * 64,
            discovery_batches_sha256="8" * 64,
            open_interest_sha256=state.open_interest_sha256,
            consumer_checkpoints_sha256="9" * 64,
            unresolved_gaps_sha256="a" * 64,
            market_state_sha256="b" * 64,
            source_clocks_sha256="c" * 64,
            detector_outcomes_sha256="d" * 64,
            projection_sha256="e" * 64,
            build_id="owp4-disposable",
        ),
    )
    selection = OptionActivitySelectionRecord(
        authorization_reference="owp4-external-activity-selection",
        reviewed_at=planned - timedelta(minutes=1),
        valid_until=(
            planned + timedelta(minutes=2)
            if activity_valid_until is None
            else activity_valid_until
        ),
        policy_sha256="f" * 64,
        source_sha256="e" * 64,
        quote_ceiling=2,
        contracts=(
            OptionActivityContract(
                instrument_id=instrument_id,
                contract_id=contract_id,
                catalog_version_id=version_id,
                contract_ticker="O:SPY261120C00500000",
                underlyer="SPY",
                call_put="C",
                expiration_date=date(2026, 11, 20),
                strike=Decimal("500"),
                exercise_style="AMERICAN",
                multiplier=Decimal("100"),
                deliverables=(),
                eligibility="VALIDATED_ACTIVE",
                catalog_effective_from=checked,
                catalog_effective_to=None,
                catalog_observed_at=planned - timedelta(minutes=3),
                catalog_payload_sha256=catalog_payload_sha256,
                market_data_time=planned - timedelta(minutes=2),
                source_sha256="d" * 64,
            ),
        ),
    )
    return build_advanced_options_enrollment_plan(
        control_plane=control,
        equity_plan=equity,
        activity_selection=selection,
    )


def _prohibited_effect_counts(database_cursor):
    relations = (
        "rt.stream_sessions",
        "rt.provider_request_ledger",
        "rt.service_plans",
        "rt.window_runs",
        "rt.stage_runs",
        "rt.cycle_receipts",
        "rt.release_current",
        "rt.alert_outbox",
        "rt.portal_releases",
        "rt.portal_release_current",
        "rt.archive_segments",
        "rt.session_archives",
    )
    counts = {}
    for relation in relations:
        database_cursor.execute(f"SELECT count(*) FROM {relation}")
        counts[relation] = int(database_cursor.fetchone()[0])
    return counts


def test_owp4_enrollment_is_idempotent_hardened_and_zero_effect():
    suffix = uuid4().hex[:8]
    database = f"test_realtime_v2_owp4_{suffix}"
    roles = build_role_names_v2(f"test_rt_owp4_{suffix}")
    roles_owned = False
    try:
        with disposable_v2_database(database) as database_pool:
            enrollment = _owp4_enrollment(database_pool)
            connection_value = database_pool.getconn()
            try:
                created, audit = harden_database_v2(
                    connection_value,
                    roles,
                )
                roles_owned = True
                assert len(created) == 3
                assert all(
                    status.startswith("PASS") for _, status in audit
                )
            finally:
                database_pool.putconn(connection_value)
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                effects_before = _prohibited_effect_counts(database_cursor)
                assert persist_advanced_options_enrollment_cursor(
                    database_cursor,
                    enrollment,
                ) is True
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                assert persist_advanced_options_enrollment_cursor(
                    database_cursor,
                    enrollment,
                ) is False
                active = load_active_slot_set_cursor(
                    database_cursor,
                    enrollment.control_plane.cohort_id,
                )
                assert active.slot_set == enrollment.control_plane.slot_set
                assert active.slot_set.version == 1
                assert active.slot_set.primary_contract_count == 1
                assert active.slot_set.reserve_contract_count == 0
                database_cursor.execute(
                    "SELECT manifest_kind,option_underlying_count "
                    "FROM rt.subscription_manifests WHERE manifest_id=%s",
                    (str(enrollment.control_plane.manifest_id),),
                )
                assert database_cursor.fetchone() == ("MARKET_OPTIONS", 1)
                database_cursor.execute(
                    "SELECT channel,option_underlying "
                    "FROM rt.subscription_members WHERE manifest_id=%s",
                    (str(enrollment.equity_plan.manifest_id),),
                )
                assert database_cursor.fetchall() == [
                    ("AM", False),
                    ("AM", False),
                ]
                assert _prohibited_effect_counts(
                    database_cursor
                ) == effects_before
            with pytest.raises(ValueError, match="identity changed"):
                with _role_cursor(
                    database_pool,
                    roles.name("admin"),
                ) as database_cursor:
                    persist_advanced_options_enrollment_cursor(
                        database_cursor,
                        replace(
                            enrollment,
                            enrollment_sha256="0" * 64,
                        ),
                    )
            with pytest.raises(errors.InsufficientPrivilege):
                with _role_cursor(
                    database_pool,
                    roles.name("api"),
                ) as database_cursor:
                    persist_advanced_options_enrollment_cursor(
                        database_cursor,
                        enrollment,
                    )
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
