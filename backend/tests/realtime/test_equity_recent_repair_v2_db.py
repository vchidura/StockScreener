import json
import re
from contextlib import closing
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

import pytest
import requests
from uuid import uuid4

from options.data.polygon_http import PolygonHttpResponse
from realtime.database_access_v2 import OWNER_POOL, V2AccessConfig, V2ConnectionProvider
from realtime.equity_recent_repair import NativeRepairPlan, execute_recent_native_repair
from realtime.equity_adjusted_daily_repair import (
    AdjustedDailyRepairPlan,
    execute_adjusted_daily_repair,
)
from realtime.equity_bar_adapter import adapt_developer_equity_bar
from realtime.equity_bar_repository import persist_realtime_equity_bars_cursor
from realtime.equity_validation_source import EquityValidationIdentity
from realtime.ticker_projection import _security
from realtime.security_v2 import build_role_names_v2, drop_disposable_roles_v2, harden_database_v2
from equity.domain import BarAvailabilityMode
from equity.polygon import normalize_grouped_daily_bars
from realtime.equity_validation_source import bind_equity_validation_adjustment_identity
from tests.realtime.test_session_close_coverage_v2_db import _seed_session
from v2_database import _connect, _settings, disposable_v2_database, requires_v2_database

pytestmark = requires_v2_database


@pytest.fixture
def pool():
    with disposable_v2_database(f"test_realtime_v2_native_repair_{uuid4().hex[:8]}") as value:
        yield value


def _scope(pool, session_date=date(2026, 10, 15)):
    cohort, members, _, _ = _seed_session(pool, session_date, ("AAPL",))
    connection = pool.getconn()
    try:
        from psycopg2.extras import RealDictCursor
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT reference.*, reference.revision_id AS security_revision_id "
                "FROM ref.security_revisions reference JOIN rt.instruments instrument USING (security_id) "
                "WHERE instrument.instrument_id=%s",
                (members[0][0],),
            )
            security = _security(cursor.fetchone())
    finally:
        connection.rollback()
        pool.putconn(connection)
    return NativeRepairPlan(
        cohort, (EquityValidationIdentity("AAPL", security, members[0][0]),),
        (session_date,), datetime.now(timezone.utc) + timedelta(minutes=30),
        3, 6 * 1024 * 1024 + 3, 2 * 1024 * 1024,
        f"fixture-{uuid4()}", "fixture entitlement", "fixture cost",
    )


class Transport:
    def __init__(self, provider, *, ambiguous=False, close=100, volume=1000.123456789):
        self.provider = provider
        self.ambiguous = ambiguous
        self.calls = []
        self.close = close
        self.volume = volume

    def get(self, url, params):
        with self.provider.transaction(read_only=True) as unit:
            unit.cursor.execute(
                "SELECT count(*) FROM rt.provider_request_ledger "
                "WHERE state='RESERVED' AND endpoint_class='NATIVE_SESSION_REPAIR'"
            )
            assert unit.cursor.fetchone()[0] >= 1
        self.calls.append(url)
        width, start, end = map(int, re.search(r"/range/(\d+)/minute/(\d+)/(\d+)", url).groups())
        if self.ambiguous and width == 5:
            raise requests.Timeout("fixture ambiguous timeout")
        rows = [
            {
                "t": value,
                "o": 100,
                "h": 101,
                "l": 99,
                "c": self.close,
                "v": self.volume,
            }
            for value in range(start, end + 1, width * 60_000)
        ]
        return PolygonHttpResponse(200, json.dumps({"results": rows}).encode(), ())


class AdjustedDailyTransport:
    def __init__(self, provider):
        self.provider = provider
        self.calls = []

    def get(self, url, params):
        with self.provider.transaction(read_only=True) as unit:
            unit.cursor.execute(
                "SELECT count(*) FROM rt.provider_request_ledger "
                "WHERE state='RESERVED' "
                "AND endpoint_class='ADJUSTED_DAILY_SESSION_REPAIR'"
            )
            assert unit.cursor.fetchone()[0] == 1
        self.calls.append((url, params))
        return PolygonHttpResponse(
            200,
            json.dumps({
                "results": [{
                    "T": "AAPL",
                    "o": 100,
                    "h": 101,
                    "l": 99,
                    "c": 100.5,
                    "v": 1000.123456789,
                }]
            }).encode(),
            (),
        )


def _provider(pool):
    return V2ConnectionProvider({OWNER_POOL: pool}, V2AccessConfig(allow_disposable=True))


def test_native_repair_replays_without_provider_calls_and_preserves_basis(pool):
    plan = _scope(pool)
    provider = _provider(pool)
    transport = Transport(provider)
    unrelated = uuid4()
    with provider.transaction() as unit:
        unit.cursor.execute(
            "INSERT INTO rt.materialization_jobs (job_id,closure_sha256,job_kind,"
            "cohort_id,instrument_id,source_revision_id,status,available_at) "
            "VALUES (%s,%s,'DEVELOPER_EQUITY_ACQUISITION',%s,%s,%s,'PENDING',%s)",
            (
                str(unrelated), uuid4().hex * 2, str(plan.cohort_id),
                plan.identities[0].instrument_id, str(uuid4()), datetime.now(timezone.utc),
            ),
        )
    clock = lambda: datetime.now(timezone.utc)
    result = execute_recent_native_repair(provider, transport, plan, clock=clock)
    assert len(transport.calls) == result["provider_requests"] == 3
    assert all(row["status"] == "COMPLETE" for row in result["outcomes"][:3])
    assert {row["interval"] for row in result["outcomes"] if row["status"] == "BLOCKED"} == {"1wk", "1mo"}
    assert not result["release_advanced"]
    replay = execute_recent_native_repair(provider, transport, plan, clock=clock)
    assert len(transport.calls) == 3
    assert replay["provider_requests"] == 0
    assert replay["provider_response_replays"] == 3
    assert replay["source_observed_through"] == result["source_observed_through"]
    assert replay["status"] == "BLOCKED"
    with provider.transaction(read_only=True) as unit:
        unit.cursor.execute(
            "SELECT count(*) FROM rt.provider_request_ledger WHERE cohort_id=%s "
            "AND endpoint_class='NATIVE_SESSION_REPAIR'", (str(plan.cohort_id),)
        )
        assert unit.cursor.fetchone()[0] == 3
        unit.cursor.execute(
            "SELECT interval,source_kind,count(*) FROM rt.bar_current WHERE cohort_id=%s "
            "AND adjustment_basis='RAW_UNADJUSTED' "
            "GROUP BY interval,source_kind ORDER BY interval,source_kind",
            (str(plan.cohort_id),)
        )
        assert unit.cursor.fetchall() == [
            ("15m", "DERIVED", 26),
            ("1d", "DERIVED", 1),
            ("1h", "DERIVED", 7),
            ("30m", "DERIVED", 13),
            ("5m", "NATIVE_REST", 78),
        ]
        unit.cursor.execute(
            "SELECT status,attempts FROM rt.materialization_jobs WHERE job_id=%s",
            (str(unrelated),),
        )
        assert unit.cursor.fetchone() == ("PENDING", 0)


def test_adjusted_daily_repair_replays_without_provider_and_preserves_basis(pool):
    native = _scope(pool)
    provider = _provider(pool)
    plan = AdjustedDailyRepairPlan(
        native.cohort_id,
        native.identities,
        native.sessions[0],
        datetime.now(timezone.utc) + timedelta(minutes=30),
        1,
        8 * 1024 * 1024 + 1,
        8 * 1024 * 1024,
        f"adjusted-{uuid4()}",
        "fixture entitlement",
        "fixture cost",
    )
    transport = AdjustedDailyTransport(provider)
    clock = lambda: datetime.now(timezone.utc)

    result = execute_adjusted_daily_repair(
        provider,
        transport,
        plan,
        clock=clock,
    )
    replay = execute_adjusted_daily_repair(
        provider,
        transport,
        plan,
        clock=clock,
    )

    assert result["status"] == "COMPLETE"
    assert result["provider_requests"] == 1
    assert replay["provider_requests"] == 0
    assert replay["provider_response_replays"] == 1
    assert len(transport.calls) == 1
    with provider.transaction(read_only=True) as unit:
        unit.cursor.execute(
            "SELECT count(*),max(bar_end) FROM rt.bar_current "
            "WHERE cohort_id=%s AND interval='1d' "
            "AND adjustment_basis='ADJUSTED'",
            (str(plan.cohort_id),),
        )
        assert unit.cursor.fetchone() == (
            1,
            datetime(2026, 10, 15, 20, tzinfo=timezone.utc),
        )


def test_ambiguous_request_is_never_automatically_refetched(pool):
    plan = _scope(pool)
    provider = _provider(pool)
    transport = Transport(provider, ambiguous=True)
    clock = lambda: datetime.now(timezone.utc)
    result = execute_recent_native_repair(provider, transport, plan, clock=clock)
    assert result["status"] == "BLOCKED"
    assert result["provider_requests"] == 3
    assert len(transport.calls) == 3
    replay = execute_recent_native_repair(provider, transport, plan, clock=clock)
    assert replay["status"] == "BLOCKED"
    assert len(transport.calls) == 3


def test_adjusted_weekly_monthly_rebuild_requires_the_latest_complete_period(pool):
    import exchange_calendars
    import pandas as pd

    plan = _scope(pool)
    provider = _provider(pool)
    identity = plan.identities[0]
    clock = lambda: datetime.now(timezone.utc)
    calendar = exchange_calendars.get_calendar("XNYS")
    facts = []
    for session in calendar.sessions_window(pd.Timestamp(plan.sessions[-1]), -64):
        revisions = normalize_grouped_daily_bars(
            ({"T": "AAPL", "o": 100, "h": 101, "l": 99, "c": 100, "v": 1000},),
            session_date=session.date(), security_ids={"AAPL": identity.security.security_id},
            observed_at=clock(), ingestion_segment_id=uuid4(),
            availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED, adjusted=True,
        )
        revision = bind_equity_validation_adjustment_identity(revisions[0])
        facts.append(adapt_developer_equity_bar(
            replace(revision, replay_available_at=clock()),
            cohort_id=plan.cohort_id, instrument_id=identity.instrument_id,
        ))
    with provider.transaction() as unit:
        persist_realtime_equity_bars_cursor(unit.cursor, facts)
    transport = Transport(provider)
    result = execute_recent_native_repair(provider, transport, plan, clock=clock)
    assert result["status"] == "COMPLETE"
    with provider.transaction() as unit:
        unit.cursor.execute(
            "SELECT DISTINCT adjustment_basis FROM rt.bar_current WHERE cohort_id=%s "
            "AND interval IN ('1wk','1mo')", (str(plan.cohort_id),),
        )
        assert unit.cursor.fetchall() == [("ADJUSTED",)]
        unit.cursor.execute(
            "DELETE FROM rt.bar_current WHERE cohort_id=%s AND interval='1d' "
            "AND adjustment_basis='ADJUSTED' AND bar_start::date='2026-09-30'",
            (str(plan.cohort_id),),
        )
    missing = execute_recent_native_repair(provider, transport, plan, clock=clock)
    assert missing["status"] == "BLOCKED"
    assert any(row["interval"] == "1mo" and row["status"] == "BLOCKED" for row in missing["outcomes"])
    assert len(transport.calls) == 3


def test_changed_payload_creates_new_revision_without_mutating_prior_bars(pool):
    plan = _scope(pool)
    provider = _provider(pool)
    clock = lambda: datetime.now(timezone.utc)
    execute_recent_native_repair(provider, Transport(provider), plan, clock=clock)
    with provider.transaction(read_only=True) as unit:
        unit.cursor.execute(
            "SELECT bar_revision_id FROM rt.bar_current WHERE cohort_id=%s AND interval='5m' "
            "ORDER BY bar_start LIMIT 1", (str(plan.cohort_id),),
        )
        original = str(unit.cursor.fetchone()[0])
    changed_plan = replace(plan, authorization_reference="explicit revised-payload approval")
    execute_recent_native_repair(
        provider, Transport(provider, close=100.25), changed_plan, clock=clock
    )
    with provider.transaction(read_only=True) as unit:
        unit.cursor.execute(
            "SELECT close_price FROM rt.bar_revisions WHERE bar_revision_id=%s", (original,)
        )
        assert unit.cursor.fetchone()[0] == 100
        unit.cursor.execute(
            "SELECT bar_revision_id,close_price FROM rt.bar_current "
            "JOIN rt.bar_revisions USING (bar_revision_id,observed_at,cohort_id,provider_tier,"
            "instrument_id,interval,bar_start,bar_end,session_scope,adjustment_basis,source_kind) "
            "WHERE cohort_id=%s AND interval='5m' ORDER BY bar_start LIMIT 1",
            (str(plan.cohort_id),),
        )
        latest_id, close = unit.cursor.fetchone()
        assert str(latest_id) != original and close == 100.25


def test_frozen_authorization_cannot_expand_and_expired_replay_makes_no_requests(pool):
    plan = _scope(pool)
    provider = _provider(pool)
    transport = Transport(provider)
    clock = lambda: datetime.now(timezone.utc)
    execute_recent_native_repair(provider, transport, plan, clock=clock)
    with pytest.raises(ValueError, match="conflicts with its plan"):
        execute_recent_native_repair(
            provider, transport, replace(plan, byte_ceiling=plan.byte_ceiling + 1024),
            clock=clock,
        )
    assert len(transport.calls) == 3
    cached = execute_recent_native_repair(
        provider, transport, plan,
        clock=lambda: plan.authorized_until + timedelta(seconds=1),
    )
    assert cached["provider_requests"] == 0 and len(transport.calls) == 3


def test_native_repair_runs_with_audited_app_grants_and_cleans_up_roles():
    from realtime.database_access_v2 import APP_ROLE

    suffix = uuid4().hex[:8]
    roles = build_role_names_v2("test_rt_native_" + suffix)
    owned = False
    try:
        with disposable_v2_database("test_realtime_v2_native_role_" + suffix) as pool:
            plan = _scope(pool)
            connection = pool.getconn()
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT count(*) FROM pg_roles WHERE rolname=ANY(%s)",
                        (list(roles.all_names),),
                    )
                    assert cursor.fetchone()[0] == 0
                connection.rollback()
                owned = True
                created, audit = harden_database_v2(connection, roles)
                assert len(created) == 3 and all(status.startswith("PASS") for _, status in audit)
            finally:
                pool.putconn(connection)
            provider = V2ConnectionProvider(
                {APP_ROLE: pool},
                V2AccessConfig(
                    security_mode="hardened", role_prefix=roles.prefix, allow_disposable=True
                ),
            )
            provider.verify()
            transport = Transport(provider)
            result = execute_recent_native_repair(
                provider, transport, plan, clock=lambda: datetime.now(timezone.utc)
            )
            assert result["provider_requests"] == len(transport.calls) == 3
            assert all(row["status"] == "COMPLETE" for row in result["outcomes"][:3])
    finally:
        if owned:
            with closing(_connect(_settings(), "postgres")) as connection:
                connection.autocommit = True
                drop_disposable_roles_v2(connection, roles)


def test_native_half_day_has_exact_membership_and_no_bars_after_close(pool):
    plan = _scope(pool, date(2025, 11, 28))
    provider = _provider(pool)
    transport = Transport(provider)
    result = execute_recent_native_repair(
        provider, transport, plan, clock=lambda: datetime.now(timezone.utc)
    )
    assert len(transport.calls) == result["provider_requests"] == 3
    with provider.transaction(read_only=True) as unit:
        unit.cursor.execute(
            "SELECT interval,count(*),max(bar_end) FROM rt.bar_revisions WHERE cohort_id=%s "
            "AND source_kind='NATIVE_REST' "
            "AND availability_mode='HISTORICAL_RECONSTRUCTED' GROUP BY interval",
            (str(plan.cohort_id),)
        )
        rows = unit.cursor.fetchall()
        assert {row[0]: row[1] for row in rows} == {"5m": 42, "15m": 14, "30m": 7}
        assert all(row[2] == datetime(2025, 11, 28, 18, tzinfo=timezone.utc) for row in rows)
