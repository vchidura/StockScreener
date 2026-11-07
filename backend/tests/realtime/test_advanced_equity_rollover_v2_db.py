from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from options.calendar import OptionExchangeCalendar
from realtime.advanced_equity_preparation import (
    EquityPreparationChunk,
    EquityPreparationLimits,
    EquityPreparationSourceError,
    MassiveEquityPreparationSource,
)
from realtime.advanced_equity_rollover import (
    AdvancedEquityRolloverResident,
    _reserve_rollover_requests_cursor,
    plan_equity_rollover_cursor,
)
from realtime.advanced_realtime_desired_state import default_advanced_realtime_desired_state
from realtime.advanced_realtime_desired_state_repository import apply_desired_state
from realtime.equity_history import load_equity_history_cursor
from realtime.advanced_realtime_fencing import claim_durable_runtime_lease_cursor
from realtime.database_access_v2 import APP_ROLE, V2AccessConfig, V2ConnectionProvider
from realtime.security_v2 import (
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from tests.realtime import test_advanced_realtime_desired_state_v2_db as desired_fixture
from tests.realtime.test_advanced_equity_preparation_v2_db import (
    CONFIGURATION_SHA256,
    _activate_runtime_profile,
    _fact,
    _provider,
)
from tests.realtime.v2_database import (
    _connect,
    _settings,
    disposable_v2_database,
    requires_v2_database,
)


pytestmark = requires_v2_database
UTC = timezone.utc


def _drop_roles(roles):
    with closing(_connect(_settings(), "postgres")) as connection:
        connection.autocommit = True
        drop_disposable_roles_v2(connection, roles)


@pytest.fixture
def rollover_scope(monkeypatch, request):
    calendar = OptionExchangeCalendar("XNYS")
    now = datetime.now(UTC)
    session = calendar.previous_session_date(calendar.session_on_or_after(now.date()))
    monkeypatch.setattr(desired_fixture, "SOURCE_SESSION", session)
    monkeypatch.setattr(desired_fixture, "SOURCE_EFFECTIVE", calendar.session_open(session))
    with ExitStack() as stack:
        roles = (
            build_role_names_v2(f"test_rt_rollover_{uuid4().hex[:8]}")
            if getattr(request, "param", False) else None
        )
        if roles is not None:
            stack.callback(_drop_roles, roles)
        pool = stack.enter_context(disposable_v2_database(
            f"test_realtime_v2_rollover_{uuid4().hex[:8]}",
        ))
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                source, _manifest = desired_fixture._seed_source(
                    cursor, runtime_configuration_sha256=CONFIGURATION_SHA256,
                )
                _activate_runtime_profile(cursor)
            connection.commit()
            apply_desired_state(
                connection, default_advanced_realtime_desired_state(source), observed_at=now,
            )
        finally:
            pool.putconn(connection)
        provider = _provider(pool)
        with provider.transaction(APP_ROLE, read_only=True, dict_cursor=True) as unit:
            plan = plan_equity_rollover_cursor(
                unit.cursor, checked_at=now, calendar=calendar,
            )
        work = next(
            item for item in plan.work
            if item.member.ticker == "SPY" and item.action == "ADJUSTED_1D"
        )
        with provider.transaction(APP_ROLE) as unit:
            for session in work.sessions:
                unit.cursor.execute(
                    "SELECT rt.ensure_partitions('rt.bar_revisions',%s)", (session,),
                )
        if roles is not None:
            connection = pool.getconn()
            try:
                _created, audit = harden_database_v2(connection, roles)
                assert all(status.startswith("PASS") for _, status in audit)
            finally:
                pool.putconn(connection)
            provider = V2ConnectionProvider(
                {APP_ROLE: pool},
                V2AccessConfig(
                    security_mode="hardened", role_prefix=roles.prefix, allow_disposable=True,
                ),
            )
            provider.verify()
        yield provider, plan, work


class RolloverSource:
    def __init__(self, *, during_fetch=None, fail=False):
        self.calls = 0
        self.during_fetch = during_fetch
        self.fail = fail

    def fetch(self, request):
        self.calls += 1
        request.before_provider_request(0)
        if self.during_fetch is not None:
            self.during_fetch()
        if self.fail:
            raise EquityPreparationSourceError(
                "FIXTURE_FAILURE", provider_requests=1, response_bytes=64,
            )
        calendar = OptionExchangeCalendar("XNYS")
        facts = []
        for index, value in enumerate(request.claim.target["interval_sessions"]["1d"]):
            session = datetime.fromisoformat(value).date()
            fact = _fact(
                request, interval="1d", start=calendar.session_open(session),
                end=calendar.session_close(session), adjusted=True, index=index,
            )
            facts.append(replace(fact, revision=replace(fact.revision, session_date=session)))
        return EquityPreparationChunk(
            request.claim.stage, {"complete": True},
            {"available_intervals": {"1d:ADJUSTED": len(facts)}},
            {"provider": "FIXTURE"}, True,
            provider_requests=1, response_bytes=64, bars=tuple(facts),
        )


def _resident(provider, source, *, clock=None):
    return AdvancedEquityRolloverResident(
        provider, source, owner_id=uuid4(), clock=clock or (lambda: datetime.now(UTC)),
    )


def _ledger(provider):
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT state,attempt,failure_code FROM rt.provider_request_ledger "
            "WHERE authorization_reference LIKE 'advanced-equity-rollover:%' "
            "ORDER BY attempt,request_id",
        )
        return tuple(unit.cursor.fetchall())


def test_overlapping_rollover_and_normal_restart_do_not_repeat_completed_work(rollover_scope):
    provider, plan, work = rollover_scope
    source = RolloverSource()
    contender = _resident(provider, source)

    def overlap():
        result = contender._execute(plan, work, datetime.now(UTC), 1)
        assert result.reason == "ROLLOVER_ACTIVE_OWNER"
        assert _ledger(provider) == (("RESERVED", 1, None),)

    source.during_fetch = overlap
    result = _resident(provider, source)._execute(plan, work, datetime.now(UTC), 1)
    assert result.status == "PROGRESSED"
    assert result.persisted_bars == len(work.sessions)
    assert _ledger(provider) == (("COMPLETE", 1, None),)
    restarted = _resident(provider, source)._execute(plan, work, datetime.now(UTC), 1)
    assert restarted.reason == "ROLLOVER_WORK_ALREADY_SATISFIED"
    assert source.calls == 1


@pytest.mark.parametrize("rollover_scope", (True,), indirect=True)
def test_rollover_uses_existing_hardened_app_permissions(rollover_scope):
    provider, plan, work = rollover_scope
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute("SELECT current_user")
        assert unit.cursor.fetchone()[0] == build_role_names_v2(
            provider.config.role_prefix,
        ).name(APP_ROLE)
        for relation in (
            "rt.realtime_desired_revisions",
            "rt.realtime_desired_member_intents",
            "rt.realtime_desired_member_bindings",
        ):
            unit.cursor.execute(
                "SELECT has_table_privilege(current_user,%s,'SELECT')", (relation,),
            )
            assert unit.cursor.fetchone()[0]
    source = RolloverSource()
    result = _resident(provider, source)._execute(plan, work, datetime.now(UTC), 1)
    assert result.status == "PROGRESSED"
    assert source.calls == 1
    assert _ledger(provider) == (("COMPLETE", 1, None),)


def test_split_stale_history_repairs_through_the_real_source_without_refetching_valid_rows(
    rollover_scope,
):
    provider, plan, work = rollover_scope
    initial = _resident(provider, RolloverSource())._execute(plan, work, datetime.now(UTC), 1)
    assert initial.status == "PROGRESSED"
    calendar = OptionExchangeCalendar("XNYS")

    def history(known_by):
        with provider.transaction(APP_ROLE, read_only=True, dict_cursor=True) as unit:
            return load_equity_history_cursor(
                unit.cursor, cohort_id=plan.source_cohort_id,
                security_id=work.member.security_id, ticker=work.member.ticker,
                interval="1d", adjusted=True, sessions=work.sessions, checked_at=known_by,
                adjustments_through=plan.completed_session_date,
            ).rows

    before = history(datetime.now(UTC))
    with provider.transaction(APP_ROLE) as unit:
        unit.cursor.execute(
            "INSERT INTO ref.corporate_action_revisions (action_id,security_id,ticker,action_type,"
            "effective_date,split_from,split_to,source,source_key,observed_at,received_at,"
            "payload_sha256,raw_payload) VALUES "
            "(%s,%s,'SPY','SPLIT',%s,1,2,'ROLLOVER-TEST','new-split',"
            "clock_timestamp(),clock_timestamp(),%s,'{}')",
            (
                str(uuid4()), str(work.member.security_id), work.sessions[-10], "e" * 64,
            ),
        )
        unit.cursor.execute(
            "SELECT max(completed_at)+interval '61 seconds' FROM rt.provider_request_ledger "
            "WHERE authorization_reference LIKE 'advanced-equity-rollover:%'",
        )
        due = unit.cursor.fetchone()[0]
    with provider.transaction(APP_ROLE, read_only=True, dict_cursor=True) as unit:
        refreshed_plan = plan_equity_rollover_cursor(
            unit.cursor, checked_at=due, calendar=calendar,
        )
    repair = next(
        item for item in refreshed_plan.work
        if item.member.ticker == "SPY" and item.action == "ADJUSTED_1D"
    )
    assert len(repair.sessions) > 20
    assert repair.sessions == work.sessions[:-10]

    class AdjustedClient:
        def __init__(self):
            self.calls = []
            self.before_request = None

        def begin_chunk(self, *, before_request=None, **_kwargs):
            self.before_request = before_request

        def fetch_native_bars(self, ticker, interval, start, end, *, adjusted):
            self.before_request(0)
            self.calls.append((ticker, interval, start, end, adjusted))
            return tuple({
                "t": int(calendar.session_open(session).timestamp() * 1000),
                "o": 50, "h": 51, "l": 49.5, "c": 50.5,
                "v": 2000, "vw": 50.25, "n": 100,
            } for session in repair.sessions)

        def chunk_metrics(self):
            return len(self.calls), len(repair.sessions) * 128

    client = AdjustedClient()
    result = _resident(
        provider, MassiveEquityPreparationSource(client), clock=lambda: due,
    )._execute(refreshed_plan, repair, due, 1)
    assert result.status == "PROGRESSED"
    assert client.calls == [
        ("SPY", "1d", repair.sessions[0], repair.sessions[-1], True),
    ]
    after = history(due)
    assert not any(row["stale_adjustment_lineage"] for row in after)
    assert [
        row["bar_revision_id"] for row in after[-10:]
    ] == [row["bar_revision_id"] for row in before[-10:]]
    assert all(
        old["bar_revision_id"] != new["bar_revision_id"]
        for old, new in zip(before[:-10], after[:-10])
    )
    assert all(row["close_price"] == Decimal("50.5") for row in after[:-10])
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT count(*),count(DISTINCT budget_scope_sha256) FROM rt.provider_request_ledger "
            "WHERE authorization_reference LIKE 'advanced-equity-rollover:%'",
        )
        assert unit.cursor.fetchone() == (2, 1)


def test_retry_timing_and_consumed_budget_survive_rollover_restart(rollover_scope):
    provider, plan, work = rollover_scope
    source = RolloverSource(fail=True)
    first = _resident(provider, source)._execute(plan, work, datetime.now(UTC), 1)
    assert first.status == "RETRY"
    restarted = _resident(provider, source)._execute(plan, work, datetime.now(UTC), 1)
    assert restarted.reason == "ROLLOVER_BACKOFF"
    assert restarted.next_retry_at == first.next_retry_at
    assert source.calls == 1
    assert _ledger(provider) == (("FAILED", 1, "FIXTURE_FAILURE"),)
    source.fail = False
    due = first.next_retry_at + timedelta(seconds=1)
    completed = _resident(provider, source, clock=lambda: due)._execute(plan, work, due, 1)
    assert completed.status == "PROGRESSED"
    assert source.calls == 2
    assert _ledger(provider) == (("FAILED", 1, "FIXTURE_FAILURE"), ("COMPLETE", 2, None))


def _reserve_abandoned(provider, plan, work):
    now = datetime.now(UTC)
    resource = f"advanced-equity-rollover:{plan.revision_id}"
    with provider.transaction(APP_ROLE) as unit:
        lease = claim_durable_runtime_lease_cursor(
            unit.cursor, resource_key=resource, owner_id=uuid4(),
            checked_at=now, lease_seconds=300,
        )
        _reserve_rollover_requests_cursor(
            unit.cursor, plan, work, checked_at=now,
            limits=EquityPreparationLimits(), calendar=OptionExchangeCalendar("XNYS"),
            lease=lease,
        )
    return lease


def test_only_expired_proven_reservations_are_recovered_and_remain_consumed(rollover_scope):
    provider, plan, work = rollover_scope
    lease = _reserve_abandoned(provider, plan, work)
    source = RolloverSource()
    active = _resident(provider, source)._execute(plan, work, datetime.now(UTC), 1)
    assert active.reason == "ROLLOVER_ACTIVE_OWNER"
    assert _ledger(provider) == (("RESERVED", 1, None),)
    with provider.transaction(APP_ROLE) as unit:
        unit.cursor.execute(
            "UPDATE rt.worker_leases SET lease_until=clock_timestamp()-interval '1 second' "
            "WHERE resource_key=%s", (lease.resource_key,),
        )
    recovery = _resident(provider, source)._execute(plan, work, datetime.now(UTC), 1)
    assert recovery.reason == "ROLLOVER_BACKOFF"
    assert _ledger(provider) == (("AMBIGUOUS", 1, "ROLLOVER_PROCESS_LOST"),)
    due = recovery.next_retry_at + timedelta(seconds=1)
    completed = _resident(provider, source, clock=lambda: due)._execute(plan, work, due, 1)
    assert completed.status == "PROGRESSED"
    assert source.calls == 1
    assert _ledger(provider) == (
        ("AMBIGUOUS", 1, "ROLLOVER_PROCESS_LOST"), ("COMPLETE", 2, None),
    )


def test_unproven_old_reservation_is_not_rewritten_on_repeated_restarts(rollover_scope):
    provider, plan, work = rollover_scope
    lease = _reserve_abandoned(provider, plan, work)
    with provider.transaction(APP_ROLE) as unit:
        unit.cursor.execute("DELETE FROM rt.worker_leases WHERE resource_key=%s", (lease.resource_key,))
    source = RolloverSource()
    for _restart in range(2):
        blocked = _resident(provider, source)._execute(plan, work, datetime.now(UTC), 1)
        assert blocked.reason == "ROLLOVER_UNPROVEN_RESERVATION_OWNER"
        assert _ledger(provider) == (("RESERVED", 1, None),)
    assert source.calls == 0


def test_stale_rollover_completion_cannot_write_or_release_the_replacement_lease(rollover_scope):
    provider, plan, work = rollover_scope
    successor = uuid4()
    resource = f"advanced-equity-rollover:{plan.revision_id}"

    def replace_owner():
        with provider.transaction(APP_ROLE) as unit:
            unit.cursor.execute(
                "UPDATE rt.worker_leases SET owner_id=%s,owner_epoch=owner_epoch+1 "
                "WHERE resource_key=%s", (str(successor), resource),
            )

    source = RolloverSource(during_fetch=replace_owner)
    blocked = _resident(provider, source)._execute(plan, work, datetime.now(UTC), 1)
    assert blocked.reason == "ROLLOVER_LEASE_LOST"
    assert _ledger(provider) == (("RESERVED", 1, None),)
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT count(*) FROM rt.bar_head WHERE written_by_cohort_id=%s "
            "AND instrument_id=%s AND interval='1d' AND adjustment_basis='ADJUSTED'",
            (str(plan.source_cohort_id), work.member.instrument_id),
        )
        assert unit.cursor.fetchone()[0] == 0
        unit.cursor.execute(
            "SELECT owner_id,lease_until>clock_timestamp() FROM rt.worker_leases "
            "WHERE resource_key=%s", (resource,),
        )
        owner_id, active = unit.cursor.fetchone()
        assert str(owner_id) == str(successor)
        assert active
from contextlib import ExitStack, closing
