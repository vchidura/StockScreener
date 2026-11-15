"""G102 universe sizing, Options (WP-C, Nov 15): any underlying, any Equity list.

O-1 the acquisition binding carries the session's underlyings (not the October validation nine); O-2 the Options
scope is the underlyings only; O-3 any underlying has a flow tier; O-4 a session runs the configured underlyings
whose Equity is READY (the rest are UNAVAILABLE for the day); O-5 underlyings are ranked by priority when the
contract budget cannot serve them all; the scheduler's snapshot deadline Timeout is a labelled skip."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from requests import Timeout

from realtime import advanced_options_continuous_scheduler as scheduler
from realtime import advanced_options_lifecycle as lifecycle
from realtime.advanced_equity_control_plane import build_advanced_equity_control_plane_plan
from realtime.advanced_options_acquisition_runtime import (
    acquisition_binding_from_payload,
    acquisition_binding_payload,
    build_acquisition_binding,
)
from realtime.advanced_options_control_plane import (
    AdvancedOptionsStoredControlIdentity,
    advanced_options_market_universe,
)
from realtime.advanced_options_flow_policy import (
    DEFAULT_FLOW_UNDERLYING_TIER,
    FlowUniverseTier,
    load_advanced_options_flow_policy,
)
from realtime.contracts import EQUITY_VALIDATION_UNIVERSE, UniverseManifest

import test_advanced_options_acquisition_runtime as acquisition_fixtures
import test_advanced_options_continuous_scheduler as scheduler_fixtures
import test_advanced_options_control_plane as control_fixtures


GOOGL_CONTRACTS = ("O:GOOGL261218C00200000", "O:GOOGL261218P00200000")


def _binding(contracts, equity_tickers=None):
    return build_acquisition_binding(
        source_sha256="1" * 64,
        backend_image_id=f"sha256:{'2' * 64}",
        authority=acquisition_fixtures._authority(),
        contract_tickers=contracts,
        connection_attempt_ceiling=78,
        decoded_message_ceiling=18_874_386,
        decoded_byte_ceiling=18_874_386,
        equity_tickers=equity_tickers,
    )


# ---- O-1: acquisition binding ------------------------------------------------------------------------------------


def test_a_binding_for_one_underlying_outside_the_october_nine_round_trips():
    binding = _binding(GOOGL_CONTRACTS, ("GOOGL",))
    assert "GOOGL" not in EQUITY_VALIDATION_UNIVERSE.equity_tickers
    assert binding.equity_tickers == ("GOOGL",)
    payload = acquisition_binding_payload(binding)
    assert payload["membership"]["equity_target"] == 1
    assert acquisition_binding_from_payload(payload) == binding


def test_a_binding_without_a_list_carries_its_contracts_underlyings():
    binding = _binding(acquisition_fixtures.CONTRACTS)
    assert binding.equity_tickers == ("QQQ", "SPY")
    assert acquisition_binding_from_payload(acquisition_binding_payload(binding)) == binding


def test_a_binding_written_with_the_october_nine_still_loads():
    """Bindings stored before Nov 15 (an Options session prepared by the old code) must keep loading."""
    binding = _binding(acquisition_fixtures.CONTRACTS, EQUITY_VALIDATION_UNIVERSE.equity_tickers)
    payload = acquisition_binding_payload(binding)
    assert payload["membership"]["equity_target"] == 9
    assert acquisition_binding_from_payload(payload) == binding


def test_a_binding_refuses_contracts_outside_its_underlyings():
    with pytest.raises(ValueError, match="scope is invalid"):
        _binding(acquisition_fixtures.CONTRACTS, ("SPY",))


def test_a_binding_payload_with_a_wrong_count_is_refused():
    payload = acquisition_binding_payload(_binding(GOOGL_CONTRACTS, ("GOOGL",)))
    payload["membership"]["equity_target"] = 9
    with pytest.raises(ValueError, match="contract changed"):
        acquisition_binding_from_payload(payload)


# ---- O-2: the Options scope is the underlyings only -------------------------------------------------------------


def test_the_scope_depends_on_the_underlyings_only():
    universe = advanced_options_market_universe(("SPY", "GOOGL", "spy"))
    assert universe == UniverseManifest("market-options-2-2-v1", ("GOOGL", "SPY"), ("GOOGL", "SPY"))
    assert universe.sha256 == advanced_options_market_universe(["GOOGL", "SPY"]).sha256
    with pytest.raises(ValueError):
        advanced_options_market_universe(())


def _equity_plan(*tickers):
    return build_advanced_equity_control_plane_plan(
        session_date=control_fixtures.SESSION,
        security_ids={ticker: uuid4() for ticker in tickers},
        entitlement_evidence_sha256="a" * 64,
        authorization_reference="advanced-equity-v2-20261016",
        checked_at=control_fixtures.CHECKED,
        expires_at=control_fixtures.EXPIRES,
        schema_sha256="b" * 64,
        security_policy_sha256="c" * 64,
    )


COHORT = UUID("44444444-4444-4444-4444-444444444444")


def _stored(universe_sha256):
    return AdvancedOptionsStoredControlIdentity(
        profile_id=UUID("55555555-5555-5555-5555-555555555555"), profile_sha256="1" * 64,
        manifest_id=UUID("66666666-6666-6666-6666-666666666666"), manifest_sha256="2" * 64,
        cohort_id=COHORT, cohort_sha256="3" * 64, universe_sha256=universe_sha256,
        configuration_sha256="4" * 64, mode="LIVE", service_plan_sha256="5" * 64,
        allocation_policy_sha256="6" * 64,
    )


@pytest.mark.parametrize("equity", [
    ("AAPL", "SPY"),                                    # the list preparation saw
    ("AAPL", "SPY", "MSFT", "GOOGL", "XOM", "WMT"),     # grown overnight (R63, Nov 13: 25 -> 45)
    ("SPY",),                                           # shrunk to the underlying alone
])
def test_a_stored_scope_binds_whatever_the_equity_list(equity):
    stored = _stored(advanced_options_market_universe(("SPY",)).sha256)
    plan = control_fixtures._plan(
        equity_plan=_equity_plan(*equity), stored_identity=stored, authorized_cohort_id=COHORT,
    )
    assert plan.universe == advanced_options_market_universe(("SPY",))


def test_an_underlying_missing_from_todays_equity_list_is_refused():
    stored = _stored(advanced_options_market_universe(("SPY",)).sha256)
    with pytest.raises(ValueError, match="not members of today's Equity plan"):
        control_fixtures._plan(
            equity_plan=_equity_plan("AAPL", "MSFT"), stored_identity=stored, authorized_cohort_id=COHORT,
        )


def test_a_cohort_prepared_by_the_old_code_still_binds_through_its_stored_universe():
    """The r104 fallback stays for one release: a cohort prepared before the deploy stored the full-list form."""
    old = UniverseManifest("market-options-2-1-v1", ("AAPL", "SPY"), ("SPY",))
    stored = _stored(old.sha256)
    grown = _equity_plan("AAPL", "SPY", "MSFT")
    plan = control_fixtures._plan(
        equity_plan=grown, stored_identity=stored, authorized_cohort_id=COHORT, stored_universe=old,
    )
    assert plan.universe == old
    with pytest.raises(ValueError, match="stored Options control scope changed"):
        control_fixtures._plan(equity_plan=grown, stored_identity=stored, authorized_cohort_id=COHORT)


def test_without_a_stored_identity_the_legacy_scope_is_unchanged():
    plan = control_fixtures._plan()
    assert plan.universe == UniverseManifest("market-options-2-1-v1", ("AAPL", "SPY"), ("SPY",))


# ---- O-3: flow tiers ---------------------------------------------------------------------------------------------


def test_any_underlying_has_a_flow_tier_and_the_listed_ones_keep_theirs():
    policy = load_advanced_options_flow_policy()
    assert policy.tier_for("SPY") is FlowUniverseTier.ETF
    assert policy.tier_for("NVDA") is FlowUniverseTier.HIGH_BETA_HIGH_PREMIUM
    assert policy.tier_for("AAPL") is FlowUniverseTier.CORE_MEGA_CAP
    assert policy.tier_for("googl") is DEFAULT_FLOW_UNDERLYING_TIER is FlowUniverseTier.CORE_MEGA_CAP
    with pytest.raises(ValueError):
        policy.tier_for("O:SPY")


# ---- scheduler: snapshot deadline Timeout ------------------------------------------------------------------------


def _timeout_cycle(monkeypatch, *, clock_at):
    now = scheduler_fixtures.NOW
    receipt = ("receipt-1", now - timedelta(seconds=20), now - timedelta(seconds=15), "a" * 64)
    acquisition = SimpleNamespace(
        authority=SimpleNamespace(acquisition_stop=now + timedelta(hours=6), authorization_id=uuid4()),
        contract_tickers=("O:SPY261106C00700000",),
    )
    state = SimpleNamespace(
        load_package=lambda: {"authorization_reference": "ref"},
        load_bundle_document=lambda: {}, load_preopen_session_binding=lambda bundle: "session",
        load_downstream_bindings=lambda session: (), load_acquisition_binding=lambda: acquisition,
    )
    monkeypatch.setattr(scheduler, "PostgresContinuousSessionStateRepository", lambda *a, **k: state)
    monkeypatch.setattr(scheduler, "_scheduler_budget", lambda *a, **k: "budget")

    def fake_capture(*_args, **_kwargs):
        raise Timeout("Options snapshot interval deadline expired")

    monkeypatch.setattr(scheduler, "_capture", fake_capture)
    return scheduler._run_schedule_cycle(
        scheduler_fixtures._CycleProvider(receipt), session_date=now.date(), api_key="k",
        clock=lambda: clock_at, transport_factory=None, skipped_receipts=frozenset(),
    )


def test_a_snapshot_deadline_timeout_is_a_labelled_skip(monkeypatch):
    # The window's snapshot deadline is its Equity cutoff + 300 s (NOW + 280 s here).
    result = _timeout_cycle(monkeypatch, clock_at=scheduler_fixtures.NOW + timedelta(seconds=281))
    assert result["status"] == scheduler.STALE_SNAPSHOT_SKIPPED
    assert result["reason"] == "OPTIONS_SNAPSHOT_DEADLINE_EXPIRED"


def test_a_provider_timeout_before_the_deadline_still_raises_for_a_retry(monkeypatch):
    with pytest.raises(Timeout):
        _timeout_cycle(monkeypatch, clock_at=scheduler_fixtures.NOW)


# ---- O-4: the session's underlyings at contract selection ---------------------------------------------------------


class _Row(SimpleNamespace):
    pass


class _ReconcileCursor:
    """Answers the reconcile's queries in order; records the session rows it writes."""

    def __init__(self, work_rows, snapshot_members=()):
        self.work_rows = list(work_rows)
        self.snapshot_members = list(snapshot_members)
        self.sessions = []
        self.work_inserts = []
        self._next = None

    def execute(self, sql, params=()):
        text = " ".join(sql.split())
        if text.startswith("SELECT expiration_state"):
            self._next = ("one", None)
        elif text.startswith("SELECT ticker,bool_or(status='COMPLETE')"):
            self._next = ("all", self.work_rows)
        elif "FROM rt.subscription_members" in text:
            self._next = ("all", [(security,) for security in self.snapshot_members])
        elif text.startswith("SELECT session_binding_id"):
            self._next = ("one", None)
        elif text.startswith("INSERT INTO rt.advanced_options_preparation_sessions"):
            self.sessions.append(params)
            self._next = None
        elif text.startswith("SELECT ticker FROM rt.advanced_options_preparation_work"):
            self._next = ("one", None)
        elif text.startswith("SELECT intent_id::text"):
            self._next = ("one", (str(uuid4()),))
        elif text.startswith("INSERT INTO rt.advanced_options_preparation_work"):
            self.work_inserts.append(params[2:4])
            self._next = None
        elif text.startswith("SELECT stage,status"):
            self._next = ("all", [
                (stage, "PENDING", None, control_fixtures.CHECKED) for _ticker, stage in self.work_inserts
            ])
        elif text.startswith("SELECT ticker,bool_and"):
            self._next = ("all", [])
        else:
            raise AssertionError(f"unexpected query: {text[:80]}")

    def fetchone(self):
        kind, value = self._next
        assert kind == "one"
        return value

    def fetchall(self):
        kind, value = self._next
        assert kind == "all"
        return value


def _reconcile(monkeypatch, *, ready, work_rows=(), snapshot_members=None):
    from realtime.advanced_realtime_desired_state import DesiredMemberKind, MemberPreparationStatus

    configured = ("GOOGL", "QQQ", "SPY")
    revision = SimpleNamespace(
        revision_id=uuid4(), effective_session_date=control_fixtures.SESSION,
        members=tuple(
            _Row(ticker=ticker, enabled=True, kind=DesiredMemberKind.OPTION_UNDERLYING) for ticker in configured
        ),
    )
    securities = {ticker: uuid4() for ticker in ("GOOGL", "QQQ", "SPY", "AAPL")}
    preparations = tuple(
        _Row(
            ticker=ticker, enabled=True, kind=DesiredMemberKind.EQUITY, security_id=security,
            status=(MemberPreparationStatus.READY if ticker in ready else MemberPreparationStatus.WARMING),
        )
        for ticker, security in securities.items()
    )
    snapshot = None
    if snapshot_members is not None:
        snapshot = SimpleNamespace(snapshot_sha256="9" * 64, cohort_id=uuid4(), manifest_id=uuid4())
    reconciliation = SimpleNamespace(state="READY" if snapshot else "WARMING", snapshot=snapshot)
    monkeypatch.setattr(lifecycle, "load_current_desired_state_revision_cursor", lambda cursor: revision)
    monkeypatch.setattr(lifecycle, "_supersede_date_preparation_cursor", lambda *a, **k: None)
    monkeypatch.setattr(lifecycle, "load_desired_member_preparations_cursor", lambda *a: preparations)
    monkeypatch.setattr(
        lifecycle, "load_effective_desired_state_reconciliation_cursor", lambda *a, **k: reconciliation,
    )
    cursor = _ReconcileCursor(
        work_rows,
        snapshot_members=[str(securities[ticker]) for ticker in (snapshot_members or ())],
    )
    status = lifecycle.reconcile_advanced_options_lifecycle_cursor(
        cursor, target_session_date=control_fixtures.SESSION, checked_at=control_fixtures.CHECKED,
    )
    return status, cursor


def test_an_underlying_whose_equity_is_not_ready_is_unavailable_and_the_rest_prepare(monkeypatch):
    status, cursor = _reconcile(monkeypatch, ready={"QQQ", "SPY", "AAPL"})
    assert status.status == "PREPARING"
    assert status.option_underlyings == ("QQQ", "SPY")
    assert "UNDERLYING_UNAVAILABLE:GOOGL" in status.reason_codes
    assert {ticker for ticker, _stage in cursor.work_inserts} == {"QQQ", "SPY"}
    assert cursor.sessions[-1][6] == ["QQQ", "SPY"]


def test_a_new_name_ready_before_selection_joins_the_session(monkeypatch):
    # Earlier passes created QQQ/SPY work, nothing completed yet (the catalog capture opens after the open).
    status, _cursor = _reconcile(
        monkeypatch, ready={"GOOGL", "QQQ", "SPY"}, work_rows=[("QQQ", False), ("SPY", False)],
    )
    assert status.option_underlyings == ("GOOGL", "QQQ", "SPY")
    assert not any(code.startswith("UNDERLYING_UNAVAILABLE") for code in status.reason_codes)


def test_the_set_is_frozen_once_a_stage_completed(monkeypatch):
    status, cursor = _reconcile(
        monkeypatch, ready={"GOOGL", "QQQ", "SPY"}, work_rows=[("QQQ", True), ("SPY", False)],
    )
    assert status.option_underlyings == ("QQQ", "SPY")
    assert "UNDERLYING_UNAVAILABLE:GOOGL" in status.reason_codes
    assert {ticker for ticker, _stage in cursor.work_inserts} == {"QQQ", "SPY"}


def test_once_activated_an_underlying_outside_the_activated_equity_list_is_unavailable(monkeypatch):
    status, _cursor = _reconcile(
        monkeypatch, ready={"GOOGL", "QQQ", "SPY"}, snapshot_members=("QQQ", "SPY", "AAPL"),
    )
    assert status.option_underlyings == ("QQQ", "SPY")
    assert "UNDERLYING_UNAVAILABLE:GOOGL" in status.reason_codes


def test_with_no_ready_underlying_the_session_waits_for_equity(monkeypatch):
    status, _cursor = _reconcile(monkeypatch, ready={"AAPL"})
    assert status.status == "WAITING_EQUITY"
    assert status.reason_codes == tuple(f"EQUITY_NOT_READY:{t}" for t in ("GOOGL", "QQQ", "SPY"))


# ---- O-5: ranking when the budget cannot serve every underlying ----------------------------------------------------


def test_a_budget_smaller_than_the_underlyings_serves_them_in_priority_order():
    from realtime.advanced_options_activity_momentum_c6 import (
        allocate_activity_momentum,
        load_activity_momentum_profile,
    )
    import test_advanced_options_activity_momentum_c6 as allocation_fixtures

    profile = load_activity_momentum_profile()
    names = ("A", "B", "C", "D")
    groups = tuple(allocation_fixtures._group(name, 0) for name in names)
    for priority in (("D", "C", "B", "A"), ("B", "D", "A", "C")):
        allocation = allocate_activity_momentum(
            profile, session_date=allocation_fixtures.SESSION_DATE, rotation_key="o5-ranking",
            ready_underlyings=names, groups=groups, active_contract_budget=2, priority_order=priority,
        )
        assert {group.underlying for group in allocation.selected_groups} == set(priority[:2])


def test_priority_does_not_change_an_allocation_that_serves_every_underlying():
    from realtime.advanced_options_activity_momentum_c6 import (
        allocate_activity_momentum,
        load_activity_momentum_profile,
    )
    import test_advanced_options_activity_momentum_c6 as allocation_fixtures

    profile = load_activity_momentum_profile()
    names = ("A", "B", "C")
    groups = tuple(allocation_fixtures._group(name, index) for name in names for index in range(12))
    arguments = dict(
        session_date=allocation_fixtures.SESSION_DATE, rotation_key="o5-unchanged",
        ready_underlyings=names, groups=groups, active_contract_budget=20,
    )
    plain = allocate_activity_momentum(profile, **arguments)
    ranked = allocate_activity_momentum(profile, **arguments, priority_order=("C", "B", "A"))
    assert ranked.allocation_sha256 == plain.allocation_sha256


# ---- mixed versions during the rollout (the live nine underlyings) ---------------------------------------------------


def test_for_the_october_nine_the_new_forms_equal_what_the_old_code_accepts():
    """Monday's underlyings are exactly the October nine: the new scope equals the old code's G96 narrowed form, and
    the new binding equals the old default binding, so old and new Options services accept each other's artifacts."""
    nine = EQUITY_VALIDATION_UNIVERSE.equity_tickers
    equity_45 = (*nine, "GS", "WMT", "XOM")
    covered = tuple(ticker for ticker in equity_45 if ticker in set(nine))
    old_narrowed = UniverseManifest(f"market-options-{len(covered)}-{len(nine)}-v1", covered, nine)
    assert advanced_options_market_universe(nine).sha256 == old_narrowed.sha256
    contracts = ("O:SPY261218C00600000", "O:QQQ261218P00500000")
    assert acquisition_binding_payload(_binding(contracts, nine))["membership"]["equity_tickers"] == sorted(nine)
