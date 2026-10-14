from dataclasses import replace
from unittest.mock import MagicMock
from uuid import NAMESPACE_URL, uuid5

import pytest

from realtime.contracts import CANARY_UNIVERSE, AssetClass, CapabilityVerdict
from realtime.option_manifest_cohort_repository import option_universe_manifest_id
from realtime.option_slot_set import (
    SlotSetKind,
    SlotSetPurpose,
    equity_manifest_members,
    option_universe_manifest_sha256,
)
from realtime.option_slot_set_repository import load_active_slot_set_cursor
from realtime.preflight import ProviderTier, run_passive_preflight
from realtime.shadow_control_plane import (
    build_shadow_control_plane_plan,
    persist_shadow_control_plane,
    persist_shadow_instruments,
)
from test_preflight import NOW, _configuration, _evidence
from test_reference_seed import _manifest
from tests.realtime.slot_set_fakes import FakeSlotSetStore, SlotSetCursor


def _profile():
    return run_passive_preflight(
        _configuration(ProviderTier.MASSIVE_DEVELOPER),
        _evidence(),
        checked_at=NOW,
    )


def _plan(profile=None):
    security_ids = {
        ticker: uuid5(NAMESPACE_URL, f"shadow-security:{ticker}")
        for ticker in CANARY_UNIVERSE.equity_tickers
    }
    option_contracts = {
        ticker: (index, f"O:{ticker}261218C00000001", 100 + index)
        for index, ticker in enumerate(CANARY_UNIVERSE.option_underlyings, start=1)
    }
    seed = _manifest()
    return build_shadow_control_plane_plan(
        seed,
        profile or _profile(),
        security_ids=security_ids,
        option_contracts=option_contracts,
        configuration_sha256="c" * 64,
        model_bundle_sha256="d" * 64,
        valuation_sha256="e" * 64,
        effective_at=seed.effective_at,
    )


def test_shadow_plan_is_deterministic_exact_and_never_ready():
    first = _plan()
    repeated = _plan()

    assert first == repeated
    assert len(first.instruments) == 16
    assert len(first.manifest.members) == 40
    assert first.manifest.maximum_option_quote_contracts == 8
    assert first.cohort_id == repeated.cohort_id
    assert {row.underlying for row in first.instruments} == set(
        CANARY_UNIVERSE.equity_tickers
    )


def test_shadow_plan_rejects_partial_identity_coverage():
    security_ids = {
        ticker: uuid5(NAMESPACE_URL, f"shadow-security:{ticker}")
        for ticker in CANARY_UNIVERSE.equity_tickers
        if ticker != "TSLA"
    }
    with pytest.raises(ValueError, match="exact canary"):
        build_shadow_control_plane_plan(
            _manifest(),
            _profile(),
            security_ids=security_ids,
            option_contracts={
                ticker: (index, f"O:{ticker}261218C00000001", 100 + index)
                for index, ticker in enumerate(CANARY_UNIVERSE.option_underlyings, start=1)
            },
            configuration_sha256="c" * 64,
            model_bundle_sha256="d" * 64,
            valuation_sha256="e" * 64,
            effective_at=_manifest().effective_at,
        )


def test_shadow_plan_accepts_fail_blocked_profile_without_claiming_readiness():
    profile = replace(_profile(), verdict=CapabilityVerdict.FAIL)

    plan = _plan(profile)

    assert plan.profile_id == profile.profile_id


class ShadowControlCursor:
    """Fake of the CONTROL-role statements of persist_shadow_control_plane on the V2 tables."""

    def __init__(self, plan, instrument_ids, *, profile_status="UNAVAILABLE", store=None):
        self.plan = plan
        self.instrument_ids = instrument_ids
        self.profile_status = profile_status
        self.statements = []
        self._one = None
        self._many = []
        self.store = store or FakeSlotSetStore()
        securities = {row.underlying: row.security_id for row in plan.instruments
                      if row.asset_class is AssetClass.EQUITY}
        self.store.register_cohort(plan.cohort_id, "MARKET_OPTIONS", securities.values())
        for row in plan.instruments:
            if row.asset_class is AssetClass.OPTION:
                self.store.register_option(
                    instrument_ids[(AssetClass.OPTION, row.provider_symbol)], row.contract_id,
                    row.catalog_version_id, securities[row.underlying])

    def execute(self, statement, parameters=None):
        self.statements.append((statement, parameters))
        self._one, self._many = None, []
        plan = self.plan
        if self.store.handles(statement):
            self._one, self._many = self.store.execute(statement, parameters)
        elif statement.startswith("SELECT status, profile_sha256"):
            self._one = (self.profile_status, plan.profile_sha256)
        elif statement.startswith("SELECT manifest_sha256"):
            self._one = (
                option_universe_manifest_sha256(plan.manifest), CANARY_UNIVERSE.sha256, str(plan.profile_id),
                plan.manifest.session_date, "MARKET_OPTIONS", 8, 8, 24, plan.manifest.allocation_policy_sha256)
        elif statement.startswith("SELECT channel, instrument_id, priority"):
            self._many = [
                (row.channel.value, self.instrument_ids[(AssetClass.EQUITY, row.provider_symbol)],
                 row.priority, row.purpose.value, True)
                for row in equity_manifest_members(plan.manifest)]
        elif statement.startswith("SELECT cohort_sha256"):
            self._one = (plan.cohort_sha256, "SHADOW")

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


def _control_connection(cursor):
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    return connection


def _persisted_instrument_ids(plan):
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(index,) for index in range(1, 17)]
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    return persist_shadow_instruments(connection, plan), cursor, connection


def test_staged_repositories_persist_instruments_then_shadow_only_control_plane():
    plan = _plan()
    instrument_ids, instrument_cursor, instrument_connection = _persisted_instrument_ids(plan)

    assert len(instrument_ids) == 16
    assert instrument_connection.commit.call_count == 1
    instrument_statements = [call.args[0] for call in instrument_cursor.execute.call_args_list]
    assert sum("INSERT INTO rt.instruments" in row for row in instrument_statements) == 16

    control_cursor = ShadowControlCursor(plan, instrument_ids)
    control_connection = _control_connection(control_cursor)

    persist_shadow_control_plane(control_connection, plan, instrument_ids)

    statements = [statement for statement, _ in control_cursor.statements]
    # Equity members only (8 stocks x T, Q, AM); the 8 option contracts are a slot set, not manifest members.
    member_inserts = [parameters for statement, parameters in control_cursor.statements
                      if statement.startswith("INSERT INTO rt.subscription_members")]
    assert len(member_inserts) == 24
    assert all(row[5] is True for row in member_inserts)
    assert {row[1] for row in member_inserts} == {instrument_ids[(AssetClass.EQUITY, ticker)]
                                                  for ticker in CANARY_UNIVERSE.equity_tickers}
    manifest_insert, = [parameters for statement, parameters in control_cursor.statements
                        if statement.startswith("INSERT INTO rt.subscription_manifests")]
    assert manifest_insert[0] == str(plan.manifest_id) == str(option_universe_manifest_id(plan.manifest))
    assert manifest_insert[1] == option_universe_manifest_sha256(plan.manifest) != plan.manifest.sha256
    assert manifest_insert[6:10] == (8, 8, 24, plan.manifest.allocation_policy_sha256)
    assert not any("option_quote_contract_count" in row or " feed" in row for row in statements)
    assert any("INSERT INTO rt.cohorts" in row for row in statements)
    assert not any("readiness" in row for row in statements)
    assert not any("provider_tier" in row for row in statements)
    assert not any("portal_cohort" in row for row in statements)
    assert control_connection.commit.call_count == 1

    active = load_active_slot_set_cursor(SlotSetCursor(control_cursor.store), plan.cohort_id)
    slot_set = active.slot_set
    assert (slot_set.version, slot_set.kind, active.activated_at) == (1, SlotSetKind.PRE_SESSION, plan.effective_at)
    assert (slot_set.primary_contract_count, slot_set.reserve_contract_count) == (8, 0)
    assert slot_set.policy_sha256 == plan.manifest.allocation_policy_sha256
    assert {row.purpose for row in slot_set.members} == {SlotSetPurpose.PRIMARY}
    assert {row.instrument_id for row in slot_set.members} == {
        instrument_ids[(AssetClass.OPTION, row.provider_symbol)]
        for row in plan.instruments if row.asset_class is AssetClass.OPTION}
    assert {row.catalog_version_id for row in slot_set.members} == {
        row.catalog_version_id for row in plan.instruments if row.asset_class is AssetClass.OPTION}


def test_shadow_control_plane_replay_is_idempotent_and_a_tampered_slot_set_fails_closed():
    plan = _plan()
    instrument_ids, _, _ = _persisted_instrument_ids(plan)
    cursor = ShadowControlCursor(plan, instrument_ids)
    persist_shadow_control_plane(_control_connection(cursor), plan, instrument_ids)
    writes = sum(statement.startswith(("INSERT INTO rt.slot_set_versions", "INSERT INTO rt.slot_set_members"))
                 for statement, _ in cursor.statements)

    persist_shadow_control_plane(_control_connection(cursor), plan, instrument_ids)

    assert sum(statement.startswith(("INSERT INTO rt.slot_set_versions", "INSERT INTO rt.slot_set_members"))
               for statement, _ in cursor.statements) == writes == 2

    key = next(iter(cursor.store.versions))
    kind, stamp, policy, _, primary, reserve = cursor.store.versions[key]
    cursor.store.versions[key] = (kind, stamp, policy, "0" * 64, primary, reserve)
    connection = _control_connection(cursor)
    with pytest.raises(ValueError, match="hash or counts"):
        persist_shadow_control_plane(connection, plan, instrument_ids)
    assert connection.rollback.call_count == 1


def test_shadow_control_plane_rejects_a_contract_outside_the_cohort_option_universe():
    plan = _plan()
    instrument_ids, _, _ = _persisted_instrument_ids(plan)
    cursor = ShadowControlCursor(plan, instrument_ids)
    some_option = next(row for row in plan.instruments if row.asset_class is AssetClass.OPTION)
    cursor.store.catalog[some_option.catalog_version_id] = (some_option.contract_id, "unrelated-security")
    connection = _control_connection(cursor)

    with pytest.raises(ValueError, match="outside the cohort option universe"):
        persist_shadow_control_plane(connection, plan, instrument_ids)

    assert not any(statement.startswith("INSERT INTO rt.slot_set")
                   for statement, _ in cursor.statements)
    assert connection.commit.call_count == 0 and connection.rollback.call_count == 1


def test_shadow_control_plane_rejects_non_unavailable_profile():
    plan = _plan()
    instrument_ids = {
        (binding.asset_class, binding.provider_symbol): index
        for index, binding in enumerate(plan.instruments, start=1)
    }
    cursor = MagicMock()
    cursor.fetchone.return_value = ("PASS", plan.profile_sha256)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    with pytest.raises(ValueError, match="not blocked"):
        persist_shadow_control_plane(connection, plan, instrument_ids)

    assert connection.rollback.call_count == 1