"""DB-backed SHADOW control plane and tier cohort activation on the V2 baseline (opt-in: STOCKS_V2_DB_TESTS=1).

Uses the disposable database test_realtime_v2_slot_b only; it is always dropped. The SHADOW plan must persist an
equity-only manifest plus a PRE_SESSION slot set of its eight contracts; the tier cohort tables are unchanged
from v1, so this checks them against the real constraints (including the uuid[] column) rather than a fake.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from realtime.contracts import CANARY_UNIVERSE, AssetClass
from realtime.option_slot_set import (
    SlotSetKind,
    SlotSetPurpose,
    option_universe_manifest_sha256,
)
from realtime.option_slot_set_repository import load_active_slot_set_cursor
from realtime.preflight import ProviderTier, TierState, transition_tier
from realtime.shadow_control_plane import persist_shadow_control_plane, persist_shadow_instruments
from realtime.tier_cohort import (
    ADVANCED_CAPABILITIES,
    DEVELOPER_CAPABILITIES,
    DEVELOPER_DISABLED_DETECTORS,
    TierCohortBinding,
    build_initial_developer_activation,
    build_tier_cohort_switch,
    persist_tier_cohort_activation,
    persist_tier_cohort_binding_cursor,
)
from tests.realtime.test_shadow_control_plane import _plan
from tests.realtime.v2_database import (
    disposable_v2_database,
    insert_cohort,
    requires_v2_database,
)

pytestmark = requires_v2_database

DATABASE_NAME = "test_realtime_v2_slot_b"
UTC = timezone.utc


@pytest.fixture(scope="module")
def pool():
    with disposable_v2_database(DATABASE_NAME) as database_pool:
        yield database_pool


@contextmanager
def cursor(pool):
    pooled = pool.getconn()
    try:
        with pooled.cursor() as database_cursor:
            yield database_cursor
        pooled.commit()
    except BaseException:
        pooled.rollback()
        raise
    finally:
        pool.putconn(pooled)


@contextmanager
def connection(pool):
    pooled = pool.getconn()
    try:
        yield pooled
    finally:
        pooled.rollback()
        pool.putconn(pooled)


def _digest() -> str:
    return uuid4().hex + uuid4().hex


@pytest.fixture(scope="module")
def shadow(pool):
    plan = _plan()
    now = plan.effective_at
    securities = {row.underlying: row.security_id for row in plan.instruments
                  if row.asset_class is AssetClass.EQUITY}
    with cursor(pool) as database_cursor:
        database_cursor.execute(
            "INSERT INTO rt.capability_profiles (profile_id, config_sha256, schema_sha256, account_alias, "
            "capabilities_sha256, universe_sha256, checked_at, expires_at, status, profile_sha256, provider_tier, "
            "security_policy_sha256, preflight_mode, provider_requests, action_authorized) VALUES "
            "(%s,%s,%s,'fixture',%s,%s,%s,%s,'UNAVAILABLE',%s,'MASSIVE_DEVELOPER',%s,'PASSIVE',false,false)",
            (str(plan.profile_id), _digest(), _digest(), _digest(), CANARY_UNIVERSE.sha256,
             now - timedelta(hours=1), now + timedelta(hours=1), plan.profile_sha256, _digest()))
        for security in securities.values():
            database_cursor.execute(
                "INSERT INTO ref.securities (security_id, asset_type, source_origin, created_at) "
                "VALUES (%s, 'STOCK', 'fixture', %s)", (str(security), now - timedelta(days=40)))
        for row in plan.instruments:
            if row.asset_class is not AssetClass.OPTION:
                continue
            database_cursor.execute(
                "INSERT INTO ref.option_contracts (contract_id, underlying_security_id, provider, "
                "contract_ticker, first_observed_at, source_origin) VALUES (%s, %s, 'polygon', %s, %s, 'fixture')",
                (row.contract_id, str(securities[row.underlying]), row.provider_symbol, now - timedelta(days=1)))
            database_cursor.execute(
                "INSERT INTO ref.option_contract_versions (version_id, contract_id, call_put, expiration, strike, "
                "exercise_style, multiplier, eligibility, effective_from, observed_at, payload_sha256) VALUES "
                "(%s, %s, 'C', %s, 100, 'AMERICAN', 100, 'VALIDATED_ACTIVE', %s, %s, %s)",
                (row.catalog_version_id, row.contract_id, (now + timedelta(days=30)).date(),
                 now - timedelta(days=1), now - timedelta(days=1), _digest()))
    with connection(pool) as pooled:
        instrument_ids = persist_shadow_instruments(pooled, plan)
        persist_shadow_control_plane(pooled, plan, instrument_ids)
    return plan, instrument_ids


def _totals(pool):
    with cursor(pool) as database_cursor:
        result = []
        for relation in ("rt.subscription_manifests", "rt.subscription_members", "rt.cohorts",
                         "rt.instruments", "rt.slot_set_versions", "rt.slot_set_members", "rt.slot_set_current"):
            database_cursor.execute(f"SELECT count(*) FROM {relation}")
            result.append(database_cursor.fetchone()[0])
        return result


def test_shadow_control_plane_persists_an_equity_only_manifest(pool, shadow):
    plan, instrument_ids = shadow
    with cursor(pool) as database_cursor:
        database_cursor.execute(
            "SELECT manifest_sha256, manifest_kind, stock_count, option_underlying_count, member_count, "
            "allocation_policy_sha256 FROM rt.subscription_manifests WHERE manifest_id=%s", (str(plan.manifest_id),))
        assert database_cursor.fetchone() == (
            option_universe_manifest_sha256(plan.manifest), "MARKET_OPTIONS", 8, 8, 24,
            plan.manifest.allocation_policy_sha256)
        database_cursor.execute(
            "SELECT member.channel, member.option_underlying, instrument.asset_class "
            "FROM rt.subscription_members AS member JOIN rt.instruments AS instrument "
            "ON instrument.instrument_id=member.instrument_id WHERE member.manifest_id=%s", (str(plan.manifest_id),))
        rows = database_cursor.fetchall()
        database_cursor.execute("SELECT mode, manifest_id::text FROM rt.cohorts WHERE cohort_id=%s",
                                (str(plan.cohort_id),))
        cohort = database_cursor.fetchone()
    assert len(rows) == 24 and {(row[1], row[2]) for row in rows} == {(True, "EQUITY")}
    assert {row[0] for row in rows} == {"T", "Q", "AM"}
    assert cohort == ("SHADOW", str(plan.manifest_id))


def test_shadow_control_plane_activates_the_eight_contract_pre_session_slot_set(pool, shadow):
    plan, instrument_ids = shadow
    with cursor(pool) as database_cursor:
        active = load_active_slot_set_cursor(database_cursor, plan.cohort_id)
    slot_set = active.slot_set
    assert (slot_set.version, slot_set.kind, active.activated_at) == (1, SlotSetKind.PRE_SESSION, plan.effective_at)
    assert (slot_set.primary_contract_count, slot_set.reserve_contract_count, len(slot_set.members)) == (8, 0, 16)
    assert slot_set.policy_sha256 == plan.manifest.allocation_policy_sha256
    assert {row.purpose for row in slot_set.members} == {SlotSetPurpose.PRIMARY}
    assert {row.instrument_id for row in slot_set.members} == {
        instrument_ids[(AssetClass.OPTION, row.provider_symbol)]
        for row in plan.instruments if row.asset_class is AssetClass.OPTION}


def test_replaying_the_shadow_control_plane_changes_nothing(pool, shadow):
    plan, _ = shadow
    before = _totals(pool)
    with connection(pool) as pooled:
        instrument_ids = persist_shadow_instruments(pooled, plan)
        persist_shadow_control_plane(pooled, plan, instrument_ids)
    assert _totals(pool) == before


# ---- tier cohorts: unchanged tables, verified against the real constraints -------------------------------

def _tier_bindings(pool):
    now = datetime.now(UTC).replace(microsecond=0)
    with cursor(pool) as database_cursor:
        developer_cohort, advanced_cohort = insert_cohort(database_cursor, now), insert_cohort(database_cursor, now)
        profiles = {}
        for cohort in (developer_cohort, advanced_cohort):
            database_cursor.execute(
                "SELECT manifest.profile_id FROM rt.cohorts AS cohort JOIN rt.subscription_manifests AS manifest "
                "ON manifest.manifest_id=cohort.manifest_id WHERE cohort.cohort_id=%s", (str(cohort),))
            profiles[cohort] = UUID(str(database_cursor.fetchone()[0]))
    developer = TierCohortBinding(
        developer_cohort, profiles[developer_cohort], ProviderTier.MASSIVE_DEVELOPER, TierState.DEVELOPER_READY,
        _digest(), _digest(), DEVELOPER_CAPABILITIES, DEVELOPER_DISABLED_DETECTORS, now)
    advanced = TierCohortBinding(
        advanced_cohort, profiles[advanced_cohort], ProviderTier.MASSIVE_ADVANCED, TierState.ADVANCED_READY,
        _digest(), _digest(), ADVANCED_CAPABILITIES, frozenset(), now + timedelta(minutes=10))
    return now, developer, advanced


def _pointer(pool):
    with cursor(pool) as database_cursor:
        database_cursor.execute(
            "SELECT cohort_id::text, provider_tier, activation_id::text FROM rt.portal_cohort_current "
            "WHERE scope='PLATFORM'")
        return database_cursor.fetchone()


def test_tier_cohort_initial_activation_and_switch_persist_on_the_real_tables(pool):
    now, developer, advanced = _tier_bindings(pool)
    initial = build_initial_developer_activation(
        developer, activated_at=now + timedelta(minutes=1), approval_reference="approved-developer-bootstrap",
        dependencies_warm=True)

    with connection(pool) as pooled:
        persist_tier_cohort_activation(pooled, initial)
        persist_tier_cohort_activation(pooled, initial)
    assert _pointer(pool) == (str(developer.cohort_id), "MASSIVE_DEVELOPER", str(initial.activation_id))

    warming = transition_tier(
        TierState.DEVELOPER_READY, TierState.ADVANCED_WARMING, transitioned_at=now + timedelta(minutes=2),
        reason_code="ADVANCED_ENTITLEMENT_APPROVED", operator_approved=True,
        operator_approval_reference="approved-advanced-switch")
    ready = transition_tier(
        TierState.ADVANCED_WARMING, TierState.ADVANCED_READY, transitioned_at=now + timedelta(minutes=11),
        reason_code="ADVANCED_DEPENDENCIES_WARM", dependencies_warm=True)
    switch = build_tier_cohort_switch(developer, advanced, (warming, ready), activated_at=now + timedelta(minutes=12))

    with connection(pool) as pooled:
        persist_tier_cohort_activation(pooled, switch)
    assert _pointer(pool) == (str(advanced.cohort_id), "MASSIVE_ADVANCED", str(switch.activation_id))
    # A switch is applied once: its previous cohort is no longer current, so a replay is rejected (existing contract).
    with connection(pool) as pooled:
        with pytest.raises(ValueError, match="previous cohort does not match"):
            persist_tier_cohort_activation(pooled, switch)
    assert _pointer(pool) == (str(advanced.cohort_id), "MASSIVE_ADVANCED", str(switch.activation_id))
    with cursor(pool) as database_cursor:
        database_cursor.execute(
            "SELECT previous_cohort_id::text, target_cohort_id::text, transition_ids::text[], approval_reference "
            "FROM rt.provider_tier_activations WHERE activation_id=%s", (str(switch.activation_id),))
        previous, target, transition_ids, approval = database_cursor.fetchone()
    assert (previous, target, approval) == (str(developer.cohort_id), str(advanced.cohort_id), "approved-advanced-switch")
    assert transition_ids == [str(row.transition_id) for row in (warming, ready)]

    with connection(pool) as pooled:
        with pytest.raises(ValueError, match="existing cohort"):
            persist_tier_cohort_activation(pooled, initial)
    assert _pointer(pool)[0] == str(advanced.cohort_id)


def test_a_conflicting_tier_binding_for_the_same_cohort_fails_closed(pool):
    _, developer, _ = _tier_bindings(pool)
    with cursor(pool) as database_cursor:
        persist_tier_cohort_binding_cursor(database_cursor, developer)
        persist_tier_cohort_binding_cursor(database_cursor, developer)
    conflicting = TierCohortBinding(
        developer.cohort_id, developer.profile_id, developer.provider_tier, developer.ready_state,
        _digest(), developer.valuation_policy_sha256, developer.capabilities, developer.disabled_detectors,
        developer.effective_at)
    with pytest.raises(ValueError, match="tier cohort binding conflict"):
        with cursor(pool) as database_cursor:
            persist_tier_cohort_binding_cursor(database_cursor, conflicting)
