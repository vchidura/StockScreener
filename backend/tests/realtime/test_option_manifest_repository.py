from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from options.domain import ContractType
from realtime.contracts import DEVELOPER_FORWARD_UNIVERSE, UniverseManifest
from realtime.option_manifest_cohort_repository import DeveloperForwardCohortBinding, developer_forward_cohort_id
from realtime.option_manifest_repository import (
    option_manifest_selection_id,
    option_subscription_manifest_id,
    persist_option_manifest_selection,
)
from realtime.contracts import canonical_sha256
from realtime.option_session_manifest import (
    ContractSelectionStatus,
    ManifestSelectionStatus,
    build_bounded_option_session_manifest,
)
from realtime.option_slot_set import (
    SlotSetKind,
    SlotSetPurpose,
    equity_manifest_members,
    option_universe_manifest_sha256,
)
from realtime.option_slot_set_repository import load_active_slot_set_cursor
from realtime.preflight import ProviderTier
from tests.realtime.slot_set_fakes import FakeSlotSetStore, SlotSetCursor
from tests.realtime.test_option_session_manifest import (
    MARKET_TIME,
    SESSION_DATE,
    _candidates,
    _developer_policy,
    _scope,
)


def _ready_plan(universe=None, *, spot=Decimal("100")):
    universe = universe or UniverseManifest("manifest-repository-v1", ("AAPL",), ("AAPL",))
    policy = _developer_policy()
    return build_bounded_option_session_manifest(
        _scope(ProviderTier.MASSIVE_DEVELOPER, universe),
        policy,
        _candidates(
            universe,
            policy.target_dtes,
            policy.strikes_per_side_per_expiry,
            spot=spot,
        ),
        session_date=SESSION_DATE,
    )


def _blocked_plan():
    ready = _ready_plan()
    candidates = _candidates(
        ready.scope.universe,
        ready.policy.target_dtes,
        ready.policy.strikes_per_side_per_expiry,
    )
    partial = tuple(
        row
        for row in candidates
        if not (
            row.snapshot.calendar_dte == 60
            and row.catalog.contract_type is ContractType.PUT
        )
    )
    return build_bounded_option_session_manifest(
        ready.scope,
        ready.policy,
        partial,
        session_date=SESSION_DATE,
    )


class SelectionCursor:
    def __init__(
        self,
        plan,
        *,
        wrong_profile_tier=False,
        missing_selected_instrument=False,
        conflicting_audit=False,
        conflicting_manifest=False,
        incomplete_members=False,
        slot_store=None,
    ):
        self.plan = plan
        self.wrong_profile_tier = wrong_profile_tier
        self.missing_selected_instrument = missing_selected_instrument
        self.conflicting_audit = conflicting_audit
        self.conflicting_manifest = conflicting_manifest
        self.incomplete_members = incomplete_members
        self.statements = []
        self._one = None
        self._many = []
        self.security_ids = {
            ticker: uuid5(NAMESPACE_URL, f"manifest-security:{ticker}")
            for ticker in plan.scope.universe.equity_tickers
        }
        self.equity_instruments = {
            ticker: index
            for index, ticker in enumerate(plan.scope.universe.equity_tickers, start=1)
        }
        self.option_instruments = {
            row.contract_id: 10_000 + row.contract_id
            for row in plan.dispositions
        }
        if missing_selected_instrument:
            selected = next(
                row for row in plan.dispositions
                if row.status is ContractSelectionStatus.INCLUDED
            )
            self.option_instruments[selected.contract_id] = None
        self.slot_store = slot_store or FakeSlotSetStore()
        for row in plan.dispositions:
            if self.option_instruments[row.contract_id] is not None:
                self.slot_store.register_option(
                    self.option_instruments[row.contract_id], row.contract_id,
                    row.catalog_version_id, self.security_ids[row.underlyer],
                )
        self.configuration_sha256 = canonical_sha256("manifest-repository-config-v1")
        self.cohort_row = None

    @staticmethod
    def _call_put(contract_type):
        return "C" if contract_type is ContractType.CALL else "P"

    def execute(self, statement, parameters=None):
        self.statements.append((statement, parameters))
        self._one = None
        self._many = []
        if self.slot_store.handles(statement):
            self._one, self._many = self.slot_store.execute(statement, parameters)
        elif statement.startswith("SELECT profile_sha256, provider_tier"):
            tier = (
                ProviderTier.MASSIVE_ADVANCED.value
                if self.wrong_profile_tier
                else self.plan.scope.provider_tier.value
            )
            self._one = (
                self.plan.scope.profile_sha256,
                tier,
                self.plan.scope.universe.sha256,
            )
        elif statement.startswith("SELECT revision.ticker"):
            self._many = [
                (
                    ticker,
                    str(self.security_ids[ticker]),
                    self.equity_instruments[ticker],
                )
                for ticker in sorted(self.plan.scope.universe.equity_tickers)
            ]
        elif statement.startswith("SELECT contract.contract_id"):
            self._many = [
                (
                    row.contract_id,
                    row.contract_ticker,
                    row.catalog_version_id,
                    self.option_instruments[row.contract_id],
                )
                for row in sorted(self.plan.dispositions, key=lambda item: item.contract_id)
            ]
        elif statement.startswith("SELECT manifest_sha256"):
            manifest = self.plan.manifest
            universe = self.plan.scope.universe
            self._one = (
                "0" * 64 if self.conflicting_manifest else option_universe_manifest_sha256(manifest),
                universe.sha256,
                str(self.plan.scope.profile_id),
                manifest.session_date,
                "MARKET_OPTIONS",
                len(universe.equity_tickers),
                len(universe.option_underlyings),
                len(equity_manifest_members(manifest)),
                manifest.allocation_policy_sha256,
            )
        elif statement.startswith("SELECT channel, instrument_id, priority"):
            option_underlyings = set(self.plan.scope.universe.option_underlyings)
            rows = [
                (
                    member.channel.value,
                    self.equity_instruments[member.provider_symbol],
                    member.priority,
                    member.purpose.value,
                    member.provider_symbol in option_underlyings,
                )
                for member in equity_manifest_members(self.plan.manifest)
            ]
            self._many = sorted(rows)[1:] if self.incomplete_members else sorted(rows)
        elif statement.startswith("SELECT selection_sha256"):
            manifest_id = option_subscription_manifest_id(self.plan)
            selection_sha256 = (
                "0" * 64 if self.conflicting_audit else self.plan.selection_sha256
            )
            self._one = (
                selection_sha256,
                str(manifest_id) if manifest_id is not None else None,
                str(self.plan.scope.profile_id),
                self.plan.scope.provider_tier.value,
                self.plan.scope.universe.sha256,
                self.plan.policy.sha256,
                self.plan.source_evidence_sha256,
                self.plan.status.value,
                list(self.plan.reason_codes),
                self.plan.selected_contract_count,
                len(self.plan.dispositions) - self.plan.selected_contract_count,
                len(self.plan.coverage),
                self.plan.policy.primary_option_quote_contracts,
                self.plan.policy.reserved_option_quote_contracts,
                self.plan.policy.maximum_option_quote_contracts,
            )
        elif statement.startswith("SELECT contract_id, catalog_version_id"):
            self._many = [
                (
                    row.contract_id,
                    row.catalog_version_id,
                    row.contract_ticker,
                    row.underlyer,
                    row.expiration_date,
                    row.calendar_dte,
                    self._call_put(row.contract_type),
                    row.strike,
                    row.day_volume,
                    row.open_interest,
                    row.status.value,
                    list(row.reason_codes),
                )
                for row in sorted(self.plan.dispositions, key=lambda item: item.contract_id)
            ]
        elif statement.startswith("SELECT underlying_security_id::text"):
            self._many = [
                (
                    str(self.security_ids[row.underlyer]),
                    row.underlyer,
                    row.target_dte,
                    self._call_put(row.contract_type),
                    row.expiration_date,
                    row.selected_count,
                    row.required_count,
                    row.status,
                    list(row.reason_codes),
                )
                for row in sorted(
                    self.plan.coverage,
                    key=lambda item: (
                        item.underlyer,
                        item.target_dte,
                        self._call_put(item.contract_type),
                    ),
                )
            ]
        elif statement.startswith("SELECT profile_sha256, config_sha256"):
            self._one = (
                self.plan.scope.profile_sha256,
                self.configuration_sha256,
                self.plan.scope.provider_tier.value,
                self.plan.scope.universe.sha256,
                "UNAVAILABLE",
                False,
                False,
            )
        elif statement.startswith("INSERT INTO rt.cohorts"):
            self.cohort_row = (
                parameters[1],
                parameters[2],
                parameters[3],
                parameters[4],
                parameters[5],
                parameters[6],
                "SHADOW",
            )
            self.slot_store.register_cohort(
                parameters[0],
                "MARKET_OPTIONS",
                {self.security_ids[ticker] for ticker in self.plan.scope.universe.option_underlyings},
            )
        elif statement.startswith("SELECT cohort_sha256"):
            self._one = self.cohort_row

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


def _connection(cursor):
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    return connection


def _statements(cursor):
    return [statement for statement, _ in cursor.statements]


def _inserted(cursor, prefix):
    return [parameters for statement, parameters in cursor.statements if statement.startswith(prefix)]


def test_ready_plan_persists_equity_manifest_slot_set_and_complete_audit_atomically():
    plan = _ready_plan()
    cursor = SelectionCursor(plan)
    connection = _connection(cursor)
    effective_at = MARKET_TIME - timedelta(minutes=30)

    selection_id = persist_option_manifest_selection(connection, plan, effective_at=effective_at)

    assert selection_id == option_manifest_selection_id(plan)
    statements = _statements(cursor)
    assert sum("INSERT INTO rt.subscription_manifests" in row for row in statements) == 1
    assert sum("INSERT INTO rt.subscription_members" in row for row in statements) == 3
    assert sum("INSERT INTO rt.option_manifest_selection_audits" in row for row in statements) == 1
    assert sum("INSERT INTO rt.option_manifest_contract_dispositions" in row for row in statements) == 16
    assert sum("INSERT INTO rt.option_manifest_coverage" in row for row in statements) == 8
    assert sum("INSERT INTO rt.cohorts" in row for row in statements) == 1
    assert sum("INSERT INTO rt.slot_set_versions" in row for row in statements) == 1
    assert sum("INSERT INTO rt.slot_set_current" in row for row in statements) == 1
    assert cursor.cohort_row[1] == str(option_subscription_manifest_id(plan))
    assert not any("option_quote_contract_count" in row or " feed" in row for row in statements)
    for forbidden in ("readiness", "provider_tier_activations", "portal_cohort"):
        assert not any(forbidden in row for row in statements)
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0

    manifest_insert, = _inserted(cursor, "INSERT INTO rt.subscription_manifests")
    assert manifest_insert[0] == str(option_subscription_manifest_id(plan))
    assert manifest_insert[1] == option_universe_manifest_sha256(plan.manifest) != plan.manifest.sha256
    assert manifest_insert[6:10] == (1, 1, 3, plan.policy.sha256)
    member_inserts = _inserted(cursor, "INSERT INTO rt.subscription_members")
    assert {(row[2], row[4], row[5]) for row in member_inserts} == {
        ("T", "PRIMARY", True), ("Q", "PRIMARY", True), ("AM", "PRIMARY", True)}

    cohort_id = UUID(_inserted(cursor, "INSERT INTO rt.cohorts")[0][0])
    active = load_active_slot_set_cursor(SlotSetCursor(cursor.slot_store), cohort_id)
    assert active.activated_at == effective_at
    slot_set = active.slot_set
    assert (slot_set.version, slot_set.kind) == (1, SlotSetKind.PRE_SESSION)
    assert (slot_set.primary_contract_count, slot_set.reserve_contract_count) == (16, 0)
    assert slot_set.policy_sha256 == plan.policy.sha256
    assert len(slot_set.members) == 32
    assert {row.purpose for row in slot_set.members} == {SlotSetPurpose.PRIMARY}
    included = {row.catalog_version_id for row in plan.dispositions
                if row.status is ContractSelectionStatus.INCLUDED}
    assert {row.catalog_version_id for row in slot_set.members} == included
    assert {row.instrument_id for row in slot_set.members} == {
        10_000 + row.contract_id for row in plan.dispositions if row.status is ContractSelectionStatus.INCLUDED}


def test_manifest_identity_excludes_option_contracts_so_a_new_selection_keeps_the_manifest():
    first, shifted = _ready_plan(), _ready_plan(spot=Decimal("105"))
    assert first.manifest.sha256 != shifted.manifest.sha256
    assert first.selection_sha256 != shifted.selection_sha256
    assert option_subscription_manifest_id(first) == option_subscription_manifest_id(shifted)
    assert option_subscription_manifest_id(_blocked_plan()) is None


def test_replaying_the_same_selection_is_idempotent_and_a_divergent_slot_set_raises():
    plan = _ready_plan()
    cursor = SelectionCursor(plan)
    effective_at = MARKET_TIME - timedelta(minutes=30)
    persist_option_manifest_selection(_connection(cursor), plan, effective_at=effective_at)
    writes = sum(row.startswith(("INSERT INTO rt.slot_set_versions", "INSERT INTO rt.slot_set_members"))
                 for row in _statements(cursor))
    pointer = dict(cursor.slot_store.current)

    persist_option_manifest_selection(_connection(cursor), plan, effective_at=effective_at)

    assert sum(row.startswith(("INSERT INTO rt.slot_set_versions", "INSERT INTO rt.slot_set_members"))
               for row in _statements(cursor)) == writes == 2
    assert cursor.slot_store.current == pointer
    assert len(cursor.slot_store.versions) == 1 and len(cursor.slot_store.current) == 1

    key = next(iter(cursor.slot_store.versions))
    kind, stamp, policy, member_sha, primary, reserve = cursor.slot_store.versions[key]
    cursor.slot_store.versions[key] = (kind, stamp, policy, "0" * 64, primary, reserve)
    connection = _connection(cursor)
    with pytest.raises(ValueError, match="hash or counts"):
        persist_option_manifest_selection(connection, plan, effective_at=effective_at)
    assert connection.rollback.call_count == 1


def test_slot_policy_hash_is_an_input_that_defaults_to_the_plan_policy():
    plan = _ready_plan()
    cursor = SelectionCursor(plan)

    persist_option_manifest_selection(
        _connection(cursor), plan, effective_at=MARKET_TIME - timedelta(minutes=30),
        slot_policy_sha256="e" * 64)

    (stored,) = cursor.slot_store.versions.values()
    assert stored[2] == "e" * 64


def test_advanced_selection_binds_no_cohort_and_persists_no_slot_set():
    from tests.realtime.test_option_session_manifest import _advanced_policy

    universe = UniverseManifest("manifest-repository-advanced-v1", ("AAPL",), ("AAPL",))
    policy = _advanced_policy()
    plan = build_bounded_option_session_manifest(
        _scope(ProviderTier.MASSIVE_ADVANCED, universe), policy,
        _candidates(universe, policy.target_dtes, policy.strikes_per_side_per_expiry),
        session_date=SESSION_DATE)
    assert plan.status is ManifestSelectionStatus.READY
    cursor = SelectionCursor(plan)

    persist_option_manifest_selection(_connection(cursor), plan, effective_at=MARKET_TIME - timedelta(minutes=30))

    statements = _statements(cursor)
    assert sum("INSERT INTO rt.subscription_manifests" in row for row in statements) == 1
    assert not any("INSERT INTO rt.cohorts" in row or "rt.slot_set_" in row for row in statements)


def test_manifest_conflicts_fail_closed_before_the_audit():
    plan = _ready_plan()
    for flag, message in (("conflicting_manifest", "manifest identity conflict"),
                          ("incomplete_members", "members are incomplete")):
        cursor = SelectionCursor(plan, **{flag: True})
        connection = _connection(cursor)
        with pytest.raises(ValueError, match=message):
            persist_option_manifest_selection(connection, plan, effective_at=MARKET_TIME - timedelta(minutes=30))
        assert not any("selection_audits" in row for row in _statements(cursor))
        assert (connection.commit.call_count, connection.rollback.call_count) == (0, 1)


def test_forward_enrollment_pins_reviewed_hashes_without_readiness_or_activation():
    plan = _ready_plan(DEVELOPER_FORWARD_UNIVERSE)
    binding = DeveloperForwardCohortBinding("a" * 64, "b" * 64, "c" * 64)
    cursor = SelectionCursor(plan)
    connection = _connection(cursor)
    effective_at = MARKET_TIME - timedelta(minutes=30)

    persist_option_manifest_selection(connection, plan, effective_at=effective_at, cohort_binding=binding)

    assert cursor.cohort_row[2:5] == (binding.configuration_sha256, binding.model_bundle_sha256, binding.valuation_sha256)
    inserted = next(parameters for statement, parameters in cursor.statements if statement.startswith("INSERT INTO rt.cohorts"))
    assert inserted[0] == str(developer_forward_cohort_id(plan, binding, effective_at=effective_at))
    assert inserted[0] not in {"d1baee6f-4ab2-543c-8a86-ce10f8952e69", "04c9bc0a-366d-52d9-ace8-e15a48aafdbd"}
    assert not any("provider_tier_cohorts" in statement or "readiness" in statement or "portal_cohort" in statement for statement, _ in cursor.statements)
    connection.commit.assert_called_once()


def test_forward_binding_rejects_wrong_scope_before_any_persistence():
    plan = _ready_plan()
    cursor = SelectionCursor(plan)
    connection = _connection(cursor)
    with pytest.raises(ValueError, match="universe identities differ"):
        persist_option_manifest_selection(connection, plan, effective_at=MARKET_TIME,
            cohort_binding=DeveloperForwardCohortBinding("a" * 64, "b" * 64, "c" * 64))
    assert cursor.statements == []
    connection.rollback.assert_called_once()


def test_blocked_plan_persists_audit_without_manifest_or_members():
    plan = _blocked_plan()
    assert plan.status is ManifestSelectionStatus.BLOCKED
    cursor = SelectionCursor(plan)
    connection = _connection(cursor)

    persist_option_manifest_selection(
        connection,
        plan,
        effective_at=MARKET_TIME - timedelta(minutes=30),
    )

    statements = [statement for statement, _ in cursor.statements]
    assert not any("INSERT INTO rt.subscription_manifests" in row for row in statements)
    assert not any("INSERT INTO rt.subscription_members" in row for row in statements)
    assert not any("INSERT INTO rt.cohorts" in row for row in statements)
    assert sum("INSERT INTO rt.option_manifest_selection_audits" in row for row in statements) == 1
    assert sum("INSERT INTO rt.option_manifest_contract_dispositions" in row for row in statements) == len(plan.dispositions)
    assert sum("INSERT INTO rt.option_manifest_coverage" in row for row in statements) == len(plan.coverage)
    assert connection.commit.call_count == 1


def test_selected_contract_missing_instrument_rolls_back_before_manifest_insert():
    plan = _ready_plan()
    cursor = SelectionCursor(plan, missing_selected_instrument=True)
    connection = _connection(cursor)

    with pytest.raises(ValueError, match="missing instrument identities"):
        persist_option_manifest_selection(
            connection,
            plan,
            effective_at=MARKET_TIME - timedelta(minutes=30),
        )

    assert not any(
        "INSERT INTO rt.subscription_manifests" in statement
        for statement, _ in cursor.statements
    )
    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1


def test_profile_relabelling_and_audit_conflict_fail_closed():
    plan = _ready_plan()
    relabelled_cursor = SelectionCursor(plan, wrong_profile_tier=True)
    relabelled_connection = _connection(relabelled_cursor)

    with pytest.raises(ValueError, match="profile identity mismatch"):
        persist_option_manifest_selection(
            relabelled_connection,
            plan,
            effective_at=MARKET_TIME - timedelta(minutes=30),
        )
    assert relabelled_connection.rollback.call_count == 1

    conflict_cursor = SelectionCursor(plan, conflicting_audit=True)
    conflict_connection = _connection(conflict_cursor)
    with pytest.raises(ValueError, match="selection audit conflict"):
        persist_option_manifest_selection(
            conflict_connection,
            plan,
            effective_at=MARKET_TIME - timedelta(minutes=30),
        )
    assert conflict_connection.commit.call_count == 0
    assert conflict_connection.rollback.call_count == 1


def test_developer_ready_plan_without_explicit_o2_unavailability_rolls_back():
    ready = _ready_plan()
    plan = replace(
        ready,
        policy=replace(ready.policy, unavailable_detectors=()),
    )
    cursor = SelectionCursor(plan)
    connection = _connection(cursor)

    with pytest.raises(ValueError, match="O2 to remain unavailable"):
        persist_option_manifest_selection(
            connection,
            plan,
            effective_at=MARKET_TIME - timedelta(minutes=30),
        )

    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1
