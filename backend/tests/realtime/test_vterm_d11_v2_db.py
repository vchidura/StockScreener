"""G106 D11 V-TERM (Nov 18 night) on the real schema: the RECORD read runs as the hardened app role in one REPEATABLE
READ READ ONLY transaction (statement timeout and savepoints inside it), reads the A2 IV summary of each name from
rt.option_breadth_current at the window's cutoff, holds no SIRead lock and writes nothing."""
from __future__ import annotations

from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

from realtime.advanced_options_vterm import VTERM_RECORD_EVENT, VTERM_SETTING, record_vterm
from realtime.database_access_v2 import APP_ROLE, V2AccessConfig, V2ConnectionProvider
from realtime.security_v2 import build_role_names_v2, drop_disposable_roles_v2, harden_database_v2
from scripts.validate_realtime_v2_schema import _connect, _settings
from test_option_breadth_daily_v2_db import COLUMNS, _row, transaction
from v2_database import disposable_v2_database, requires_v2_database

pytestmark = requires_v2_database

UTC = timezone.utc
SESSION = date(2026, 11, 17)
OBSERVED = datetime(2026, 11, 17, 17, 55, tzinfo=UTC)
CUTOFF = datetime(2026, 11, 17, 18, 0, tzinfo=UTC)


def _chain(underlying, ivs):
    rows = []
    for days, iv in zip((4, 32, 60), ivs):
        expiry = SESSION + timedelta(days=days)
        for strike, call_delta in ((95, 0.75), (100, 0.5), (105, 0.25)):
            rows.append(_row(SESSION, underlying, "call", strike, delta=call_delta, iv=iv, expiry=expiry,
                             observed=OBSERVED))
            rows.append(_row(SESSION, underlying, "put", strike, delta=call_delta - 1, iv=iv, expiry=expiry,
                             observed=OBSERVED))
    return rows


def test_record_reads_as_the_app_role_in_a_snapshot_and_writes_nothing(monkeypatch):
    suffix = uuid4().hex[:8]
    roles = build_role_names_v2(f"test_rt_vterm_{suffix}")
    settings = _settings()
    try:
        with disposable_v2_database(f"test_realtime_v2_vterm_{suffix}") as pool:
            connection = pool.getconn()
            try:
                harden_database_v2(connection, roles)
            finally:
                pool.putconn(connection)
            db = SimpleNamespace(pool=pool, roles=roles)
            with transaction(db, "app") as cursor:
                for row in _chain("NVDA", (1.2, 0.6, 0.55)) + _chain("SPY", (0.8, 0.8, 0.8)):
                    cursor.execute(
                        f"INSERT INTO rt.option_breadth_current ({','.join(COLUMNS)}) "
                        f"VALUES ({','.join(['%s'] * len(COLUMNS))})",
                        tuple(row[name] for name in COLUMNS),
                    )
            provider = V2ConnectionProvider(
                {APP_ROLE: pool},
                V2AccessConfig(security_mode="hardened", role_prefix=f"test_rt_vterm_{suffix}",
                               statement_timeout_ms=30_000),
            )
            probes = []
            monkeypatch.setenv(VTERM_SETTING, "RECORD")
            lines = []

            def emit(event, document):
                lines.append((event, document))

            def read(cursor, **kwargs):
                from realtime.options_iv_summary_read import iv_summary_from_breadth_cursor

                summary = iv_summary_from_breadth_cursor(cursor, **kwargs)
                cursor.execute(
                    "SELECT current_user,current_setting('transaction_isolation'),"
                    "current_setting('transaction_read_only'),"
                    "(SELECT count(*) FROM pg_locks WHERE mode='SIReadLock' AND pid=pg_backend_pid())")
                probes.append(tuple(cursor.fetchone()))
                return summary

            with transaction(db) as cursor:
                cursor.execute("SELECT count(*) FROM rt.option_breadth_current")
                before = cursor.fetchone()[0]
            result = record_vterm(provider, session_date=SESSION, cutoff=CUTOFF,
                                  underlyings=("SPY", "NVDA", "TSLA"), window_id="w-db", emit=emit, read=read)
            with transaction(db) as cursor:
                cursor.execute("SELECT count(*) FROM rt.option_breadth_current")
                after = cursor.fetchone()[0]
            ((event, document),) = lines
            assert event == VTERM_RECORD_EVENT and result["status"] == "RECORDED"
            names = {row["underlyer"]: row for row in document["names"]}
            assert names["NVDA"]["status"] == "OK" and names["NVDA"]["inverted"] is True
            assert names["NVDA"]["ratio"] > 1.3 and names["NVDA"]["capture_age_s"] == 300.0
            assert names["SPY"]["status"] == "OK" and names["SPY"]["inverted"] is False
            assert 0.9 < names["SPY"]["ratio"] < 1.3
            assert names["TSLA"]["status"] == "NO_CHAIN_CAPTURE"
            assert document["inverted"] == ["NVDA"] and document["read_seconds"] < 2.0
            assert before == after == 36
            assert all(row[0] == roles.name("app") for row in probes), probes
            assert all(row[1:] == ("repeatable read", "on", 0) for row in probes), probes
            print(f"VTERM_DB {document['names']}")
    finally:
        with closing(_connect(settings, "postgres")) as connection:
            connection.autocommit = True
            drop_disposable_roles_v2(connection, roles)
