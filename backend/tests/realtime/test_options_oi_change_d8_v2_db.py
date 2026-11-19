"""G106 D8 / G102 R51 matched OI change on the real schema (STOCKS_V2_DB_TESTS=1): the RECORD read runs as the
hardened app role in one read-only transaction, joins today's rt.option_breadth_current with the previous session's
rt.option_breadth_daily (H1) per watched contract and per name, flags the large changes the unit rule flags, and writes
nothing."""
from __future__ import annotations

from contextlib import closing, contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

from realtime import option_breadth_daily as daily
from realtime.advanced_options_oi_change import (
    OI_CHANGE_RECORD_EVENT,
    OI_CHANGE_SESSION_EVENT,
    OiChangeRecorder,
)
from realtime.database_access_v2 import APP_ROLE, V2AccessConfig, V2ConnectionProvider
from realtime.security_v2 import build_role_names_v2, drop_disposable_roles_v2, harden_database_v2
from scripts.validate_realtime_v2_schema import _connect, _settings
from test_option_breadth_daily_v2_db import COLUMNS, _contract, _row, transaction
from v2_database import disposable_v2_database, requires_v2_database

pytestmark = requires_v2_database

UTC = timezone.utc
SESSION = date(2026, 11, 17)
PREVIOUS = date(2026, 11, 16)
OLDER = date(2026, 11, 13)
EXPIRY = date(2026, 11, 20)
OBSERVED = datetime(2026, 11, 17, 14, 37, tzinfo=UTC)
CUTOFF = datetime(2026, 11, 17, 14, 35, tzinfo=UTC)


def _today(underlying, kind, strike, oi, volume=10):
    return _row(SESSION, underlying, kind, strike, delta=0.5, iv=0.3, expiry=EXPIRY, observed=OBSERVED, oi=oi,
                volume=volume)


def _before(session, underlying, kind, strike, oi, volume):
    observed = datetime(session.year, session.month, session.day, 20, 57, tzinfo=UTC)
    return _row(session, underlying, kind, strike, delta=0.5, iv=0.3, expiry=EXPIRY, observed=observed, oi=oi,
                volume=volume)


def _insert_daily(cursor, rows):
    columns = COLUMNS + ("final_capture", "snapshot_at", "snapshot_source", "source_reference")
    for row in rows:
        cursor.execute(
            f"INSERT INTO rt.option_breadth_daily ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))})",
            tuple(row[name] for name in COLUMNS) + (True, row["observed_at"] + timedelta(minutes=30),
                                                     daily.CURRENT_TABLE_SOURCE, daily.CURRENT_TABLE_REFERENCE),
        )


class ProbedProvider:
    """The real provider; inside each transaction, before it ends, who and how it ran."""

    def __init__(self, inner):
        self.inner, self.probes = inner, []

    @contextmanager
    def transaction(self, role, **kwargs):
        with self.inner.transaction(role, **kwargs) as unit:
            yield unit
            unit.cursor.execute("SELECT current_user,current_setting('transaction_read_only')")
            self.probes.append(tuple(unit.cursor.fetchone()))


def test_record_matches_today_with_the_previous_close_as_the_app_role_and_writes_nothing():
    suffix = uuid4().hex[:8]
    roles = build_role_names_v2(f"test_rt_oichg_{suffix}")
    settings = _settings()
    try:
        with disposable_v2_database(f"test_realtime_v2_oichg_{suffix}") as pool:
            connection = pool.getconn()
            try:
                harden_database_v2(connection, roles)
            finally:
                pool.putconn(connection)
            db = SimpleNamespace(pool=pool, roles=roles)
            today = [_today("SPY", "call", 600, 12000), _today("SPY", "call", 610, 700),
                     _today("SPY", "put", 590, 30000), _today("QQQ", "put", 480, 2500),
                     _today("QQQ", "call", 505, 9000), _today("QQQ", "call", 500, 1200)]
            before = [_before(PREVIOUS, "SPY", "call", 600, 8000, 9000), _before(PREVIOUS, "SPY", "call", 610, 4000, 1000),
                      _before(PREVIOUS, "SPY", "put", 590, 29500, 5000), _before(PREVIOUS, "QQQ", "call", 505, 4000, 6000),
                      _before(PREVIOUS, "QQQ", "call", 500, 1300, 400),
                      # An older session must not be the comparison.
                      _before(OLDER, "SPY", "call", 600, 1, 1), _before(OLDER, "QQQ", "put", 480, 1, 1)]
            with transaction(db, "app") as cursor:
                for row in today:
                    cursor.execute(
                        f"INSERT INTO rt.option_breadth_current ({','.join(COLUMNS)}) "
                        f"VALUES ({','.join(['%s'] * len(COLUMNS))})", tuple(row[name] for name in COLUMNS))
                _insert_daily(cursor, before)
            provider = ProbedProvider(V2ConnectionProvider(
                {APP_ROLE: pool},
                V2AccessConfig(security_mode="hardened", role_prefix=f"test_rt_oichg_{suffix}",
                               statement_timeout_ms=30_000),
            ))
            lines = []
            recorder = OiChangeRecorder("RECORD", emit=lambda event, document: lines.append((event, document)),
                                        fault=lambda **_: None)
            watched = (_contract("SPY", EXPIRY, "call", 600), _contract("SPY", EXPIRY, "call", 610),
                       _contract("SPY", EXPIRY, "put", 590), _contract("QQQ", EXPIRY, "put", 480),
                       _contract("QQQ", EXPIRY, "call", 500), _contract("NVDA", EXPIRY, "call", 200))
            with transaction(db) as cursor:
                cursor.execute("SELECT (SELECT count(*) FROM rt.option_breadth_current),"
                               "(SELECT count(*) FROM rt.option_breadth_daily)")
                counts_before = cursor.fetchone()
            result = recorder.record_window(provider, session_date=SESSION, equity_cutoff=CUTOFF, window_id="w-db",
                                            watched=watched, breadth={"SPY": {"state": "COMPLETE"}})
            with transaction(db) as cursor:
                cursor.execute("SELECT (SELECT count(*) FROM rt.option_breadth_current),"
                               "(SELECT count(*) FROM rt.option_breadth_daily)")
                counts_after = cursor.fetchone()

            assert result["status"] == "RECORDED", lines
            assert result["previous_session"] == "2026-11-16" and result["matched"] == 4 and result["flagged"] == 3
            assert counts_before == counts_after == (6, 7)
            assert provider.probes == [(roles.name("app"), "on")]
            named = {document["underlying"]: document for event, document in lines if event == OI_CHANGE_RECORD_EVENT}
            spy, qqq = named["SPY"], named["QQQ"]
            assert {item["strike"]: item["flag"] for item in spy["contracts"]} == {
                "600": "OPENED_LARGE", "610": "CLOSED_LARGE", "590": None}
            assert {item["strike"]: (item["oi"], item["oi_prev"], item["change"]) for item in spy["contracts"]} == {
                "600": (12000, 8000, 4000), "610": (700, 4000, -3300), "590": (30000, 29500, 500)}
            assert next(item for item in spy["contracts"] if item["strike"] == "600")["opened_share"] == 0.444
            assert {item["strike"]: item["flag"] for item in qqq["contracts"]} == {"480": "NEW", "500": None}
            assert spy["chain"]["contracts"] == 3 and spy["chain"]["matched"] == 3
            assert spy["chain"]["opened_large"] == 1 and spy["chain"]["closed_large"] == 1
            assert spy["chain"]["call_oi"] == 12700 and spy["chain"]["call_oi_prev"] == 12000
            assert qqq["chain"]["opened_large"] == 1 and qqq["chain"]["new_large"] == 1  # 505: +5000 (+125%)
            assert [item["strike"] for item in qqq["chain"]["top_opened"]] == ["505"]
            assert [item["strike"] for item in spy["chain"]["top_opened"]] == ["600", "590"]
            (session,) = [document for event, document in lines if event == OI_CHANGE_SESSION_EVENT]
            assert session["not_in_capture"] == [_contract("NVDA", EXPIRY, "call", 200)]
            assert session["days_back"] == 1 and session["read_seconds"] < 2.0
            print(f"OI_CHANGE_DB {session}")
    finally:
        with closing(_connect(settings, "postgres")) as connection:
            connection.autocommit = True
            drop_disposable_roles_v2(connection, roles)
