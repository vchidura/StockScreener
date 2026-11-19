"""G106 D16 R-REGIME on the real schema (STOCKS_V2_DB_TESTS=1): the regime's IV read runs as the hardened app role in one
read-only transaction (statement timeout and a savepoint per name), reads the A2 IV summary of SPY and QQQ from the
capture the levels used, labels the term structure, and writes nothing."""
from __future__ import annotations

from contextlib import closing, contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

from realtime.advanced_options_regime import RegimeRecorder, start_regime
from realtime.database_access_v2 import APP_ROLE, V2AccessConfig, V2ConnectionProvider
from realtime.security_v2 import build_role_names_v2, drop_disposable_roles_v2, harden_database_v2
from scripts.validate_realtime_v2_schema import _connect, _settings
from test_option_breadth_daily_v2_db import COLUMNS, _row, transaction
from v2_database import disposable_v2_database, requires_v2_database

pytestmark = requires_v2_database

UTC = timezone.utc
SESSION = date(2026, 11, 17)
OBSERVED = datetime(2026, 11, 17, 17, 55, tzinfo=UTC)


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


class ProbedProvider:
    def __init__(self, inner):
        self.inner, self.probes = inner, []

    @contextmanager
    def transaction(self, role, **kwargs):
        with self.inner.transaction(role, **kwargs) as unit:
            yield unit
            unit.cursor.execute("SELECT current_user,current_setting('transaction_read_only'),"
                                "current_setting('statement_timeout')")
            self.probes.append(tuple(unit.cursor.fetchone()))


def _level(sign):
    return {"available": True, "gex": -1 if sign == "NEGATIVE" else 1, "gex_sign": sign, "spot": "100",
            "flip": "102", "flip_pct": 2.0, "observed_at": OBSERVED.isoformat()}


def test_the_regime_reads_the_iv_summaries_as_the_app_role_and_writes_nothing():
    suffix = uuid4().hex[:8]
    roles = build_role_names_v2(f"test_rt_regime_{suffix}")
    settings = _settings()
    try:
        with disposable_v2_database(f"test_realtime_v2_regime_{suffix}") as pool:
            connection = pool.getconn()
            try:
                harden_database_v2(connection, roles)
            finally:
                pool.putconn(connection)
            db = SimpleNamespace(pool=pool, roles=roles)
            with transaction(db, "app") as cursor:
                # SPY: front IV above the back (inverted); QQQ: rising term (contango).
                for row in _chain("SPY", (0.30, 0.22, 0.21)) + _chain("QQQ", (0.26, 0.34, 0.35)):
                    cursor.execute(
                        f"INSERT INTO rt.option_breadth_current ({','.join(COLUMNS)}) "
                        f"VALUES ({','.join(['%s'] * len(COLUMNS))})", tuple(row[name] for name in COLUMNS))
            provider = ProbedProvider(V2ConnectionProvider(
                {APP_ROLE: pool},
                V2AccessConfig(security_mode="hardened", role_prefix=f"test_rt_regime_{suffix}",
                               statement_timeout_ms=30_000),
            ))
            with transaction(db) as cursor:
                cursor.execute("SELECT count(*) FROM rt.option_breadth_current")
                before = cursor.fetchone()[0]
            # The scheduler's start loads the IV summary's modules (start_regime), not the first window.
            assert start_regime("RECORD", levels_mode="RECORD", emit=lambda *_: None) is not None
            document = RegimeRecorder().record(
                provider, head={"window_id": "w-db"}, session_date=SESSION,
                documents={"SPY": _level("NEGATIVE"), "QQQ": _level("NEGATIVE")}, chains={})
            with transaction(db) as cursor:
                cursor.execute("SELECT count(*) FROM rt.option_breadth_current")
                after = cursor.fetchone()[0]
            assert document["read_error"] is None, document
            spy, qqq = document["iv"]["SPY"], document["iv"]["QQQ"]
            assert spy["available"] is True and spy["term"] == "INVERTED" and spy["term_ratio"] > 1.0, spy
            assert qqq["available"] is True and qqq["term"] == "CONTANGO" and qqq["term_ratio"] < 0.95, qqq
            assert document["labels"] == {"gamma": "NEGATIVE", "flip": "BELOW_FLIP", "term": "INVERTED",
                                          "regime": "STRESS"}
            assert before == after == 36
            assert provider.probes == [(roles.name("app"), "on", "10s")]
            assert document["read_seconds"] < 2.0
            print(f"REGIME_DB {document['labels']} SPY {spy['term_ratio']} QQQ {qqq['term_ratio']}")
    finally:
        with closing(_connect(settings, "postgres")) as connection:
            connection.autocommit = True
            drop_disposable_roles_v2(connection, roles)
