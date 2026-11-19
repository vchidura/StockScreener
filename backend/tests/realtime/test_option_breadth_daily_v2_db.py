"""G106 H1 on a disposable V2 database (STOCKS_V2_DB_TESTS=1): migration 055 on a schema built from the migrations
like every V2 DB test, hardened with disposable roles (the registry's grants, audit PASS), then the writer as the app
role: an open session is refused, a closed one is copied exactly and only once, the retained backfill copies every
closed session, a CSV copy loads the same rows, the rows are immutable, and the IV summary reads a past session."""
from __future__ import annotations

import gzip
import json
from contextlib import closing, contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest
from psycopg2 import errors, sql

from realtime import option_breadth_daily as daily
from realtime.options_iv_summary_read import iv_summary_from_daily_cursor
from realtime.security_v2 import build_role_names_v2, drop_disposable_roles_v2, harden_database_v2
from scripts.validate_realtime_v2_schema import _connect, _settings
from v2_database import disposable_v2_database, requires_v2_database

pytestmark = requires_v2_database

UTC = timezone.utc
DATABASE = "test_realtime_v2_g106_h1_daily"
ROLE_PREFIX = "test_rt_g106h1"
NOW = datetime(2026, 11, 17, 5, 0, tzinfo=UTC)          # Nov 9-16 are closed, Nov 17 has not opened
IN_SESSION = datetime(2026, 11, 17, 18, 0, tzinfo=UTC)   # Nov 17 open
FINAL = {
    date(2026, 11, 10): datetime(2026, 11, 10, 20, 57, 10, tzinfo=UTC),
    date(2026, 11, 12): datetime(2026, 11, 12, 20, 57, tzinfo=UTC),
    date(2026, 11, 13): datetime(2026, 11, 13, 20, 56, 40, tzinfo=UTC),
    date(2026, 11, 17): datetime(2026, 11, 17, 17, 55, tzinfo=UTC),
}
COLUMNS = daily.BREADTH_COLUMNS


def _contract(underlying, expiry, kind, strike):
    return f"O:{underlying}{expiry:%y%m%d}{'C' if kind == 'call' else 'P'}{int(Decimal(str(strike)) * 1000):08d}"


def _row(session, underlying, kind, strike, *, delta, iv, expiry=None, observed=None, bid="0.95", ask="1.05",
         volume=10, oi=100, spot="100"):
    expiry = expiry or session + timedelta(days=30)
    observed = observed or FINAL[session]
    return {
        "session_date": session, "contract_ticker": _contract(underlying, expiry, kind, strike),
        "underlying": underlying, "contract_type": kind, "expiration_date": expiry,
        "strike_price": Decimal(str(strike)), "day_volume": volume, "open_interest": oi,
        "last_trade_price": Decimal("1.00"), "last_trade_at": observed - timedelta(minutes=3),
        "bid": Decimal(bid), "ask": Decimal(ask), "quote_at": observed - timedelta(seconds=30),
        "implied_volatility": None if iv is None else iv + 0.01, "delta": delta, "gamma": 0.05, "theta": -0.02,
        "vega": 0.11, "underlying_price": Decimal("100.05"),
        "local_iv": iv, "local_delta": delta, "local_gamma": None if iv is None else 0.051,
        "local_theta_per_day": None if iv is None else -0.021, "local_vega_per_vol_point": None if iv is None else 0.112,
        "local_rho_per_rate_point": None if iv is None else 0.03, "spot": Decimal(spot), "risk_free_rate": 0.04,
        "dividend_yield": 0.0, "valuation_basis": "EXACT_EXPIRY", "greeks_reason": None if iv is not None else "QUOTE_UNAVAILABLE",
        "equity_cutoff": observed.replace(second=0, microsecond=0) - timedelta(minutes=2), "observed_at": observed,
    }


def _hand_checked_spy(session):
    """The A2 unit fixture (ATM .205, 25-delta call .23 / put .28) as stored rows."""
    rows = [("call", 90, 0.85, 0.26), ("call", 95, 0.70, 0.24), ("call", 99.5, 0.53, 0.21), ("call", 100.5, 0.47, 0.19),
            ("call", 105, 0.30, 0.22), ("call", 108, 0.20, 0.24), ("call", 115, 0.08, 0.30), ("put", 85, -0.08, 0.35),
            ("put", 92, -0.20, 0.29), ("put", 95, -0.30, 0.27), ("put", 99.5, -0.47, 0.22), ("put", 100.5, -0.53, 0.20),
            ("put", 105, -0.70, 0.21)]
    quotes = {("call", 100.5): ("2.0", "2.2"), ("put", 100.5): ("2.3", "2.5")}
    built = []
    for kind, strike, delta, iv in rows:
        bid, ask = quotes.get((kind, strike), ("0.95", "1.05"))
        built.append(_row(session, "SPY", kind, strike, delta=delta, iv=iv, bid=bid, ask=ask))
    return built


def _seed_rows():
    rows = []
    stale_at = datetime(2026, 11, 12, 15, 0, tzinfo=UTC)
    # Nov 12: SPY 13 final + 2 stale (left the band at 15:00, R55), NVDA 3 final.
    rows += _hand_checked_spy(date(2026, 11, 12))
    rows += [_row(date(2026, 11, 12), "SPY", "call", strike, delta=0.01, iv=0.5, observed=stale_at, spot="95")
             for strike in (130, 131)]
    rows += [_row(date(2026, 11, 12), "NVDA", kind, strike, delta=d, iv=0.45)
             for kind, strike, d in (("call", 190, 0.5), ("put", 190, -0.5), ("call", 200, 0.3))]
    # Nov 13: SPY 4 (one unpriced); Nov 10: SPY 3 (loaded from a CSV copy); Nov 17: SPY 2 (the open session).
    rows += [_row(date(2026, 11, 13), "SPY", kind, strike, delta=d, iv=iv)
             for kind, strike, d, iv in (("call", 100, 0.5, 0.2), ("put", 100, -0.5, 0.21), ("call", 104, 0.3, 0.19),
                                         ("put", 96, -0.3, None))]
    rows += [_row(date(2026, 11, 10), "SPY", kind, strike, delta=d, iv=0.2)
             for kind, strike, d in (("call", 100, 0.5), ("put", 100, -0.5), ("call", 105, 0.25))]
    rows += [_row(date(2026, 11, 17), "SPY", kind, 100, delta=d, iv=0.2) for kind, d in (("call", 0.5), ("put", -0.5))]
    return rows


@pytest.fixture(scope="module")
def database():
    roles = build_role_names_v2(ROLE_PREFIX)
    settings = _settings()
    try:
        with disposable_v2_database(DATABASE) as pool:
            connection = pool.getconn()
            try:
                created, audit = harden_database_v2(connection, roles)
            finally:
                pool.putconn(connection)
            db = SimpleNamespace(pool=pool, roles=roles, created=created, audit=audit, settings=settings)
            with transaction(db, "app") as cursor:
                for row in _seed_rows():
                    cursor.execute(
                        f"INSERT INTO rt.option_breadth_current ({','.join(COLUMNS)}) VALUES ({','.join(['%s'] * len(COLUMNS))})",
                        tuple(row[name] for name in COLUMNS),
                    )
            yield db
    finally:
        with closing(_connect(settings, "postgres")) as connection:
            connection.autocommit = True
            drop_disposable_roles_v2(connection, roles)


@contextmanager
def transaction(db, role=None):
    connection = db.pool.getconn()
    try:
        with connection.cursor() as cursor:
            if role:
                cursor.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(db.roles.name(role))))
            yield cursor
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        db.pool.putconn(connection)


def _count(cursor, session=None):
    cursor.execute("SELECT count(*) FROM rt.option_breadth_daily" + (" WHERE session_date=%s" if session else ""),
                   (session,) if session else ())
    return cursor.fetchone()[0]


def _differences(cursor, session):
    """Rows of the session in one table and not the other, over every current-table column (both directions)."""
    columns = ",".join(COLUMNS)
    cursor.execute(
        f"SELECT count(*) FROM ((SELECT {columns} FROM rt.option_breadth_current WHERE session_date=%s "
        f"EXCEPT SELECT {columns} FROM rt.option_breadth_daily WHERE session_date=%s) UNION ALL "
        f"(SELECT {columns} FROM rt.option_breadth_daily WHERE session_date=%s "
        f"EXCEPT SELECT {columns} FROM rt.option_breadth_current WHERE session_date=%s)) AS diff",
        (session, session, session, session),
    )
    return cursor.fetchone()[0]


def test_h1_01_migration_055_applies_and_the_hardening_audit_passes(database):
    assert [name for name, status in database.audit if not status.startswith("PASS")] == []
    with transaction(database) as cursor:
        cursor.execute("SELECT version FROM rt.schema_migrations ORDER BY version DESC LIMIT 1")
        assert cursor.fetchone()[0] == "055_g106_option_breadth_daily"
        cursor.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='rt' "
            "AND table_name='option_breadth_daily' ORDER BY ordinal_position"
        )
        assert tuple(row[0] for row in cursor.fetchall()) == COLUMNS + (
            "final_capture", "snapshot_at", "snapshot_source", "source_reference")
        cursor.execute("SELECT indexname FROM pg_indexes WHERE schemaname='rt' AND tablename='option_breadth_daily' "
                       "ORDER BY indexname")
        assert [row[0] for row in cursor.fetchall()] == [
            "idx_rt_option_breadth_daily_contract", "idx_rt_option_breadth_daily_underlying", "option_breadth_daily_pkey"]
        privileges = {}
        for role in ("app", "api"):
            for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE"):
                cursor.execute("SELECT has_table_privilege(%s,'rt.option_breadth_daily',%s)",
                               (database.roles.name(role), privilege))
                privileges[(role, privilege)] = cursor.fetchone()[0]
        assert {key for key, granted in privileges.items() if granted} == {
            ("app", "SELECT"), ("app", "INSERT"), ("app", "UPDATE"), ("api", "SELECT")}
        cursor.execute("SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='rt.option_breadth_daily'::regclass")
        assert cursor.fetchone()[0] == database.roles.name("admin")


def test_h1_02_an_open_session_is_refused_and_nothing_is_written(database):
    with pytest.raises(daily.SessionStillOpen):
        with transaction(database, "app") as cursor:
            daily.snapshot_session_cursor(cursor, session_date=date(2026, 11, 17), now=IN_SESSION)
    with transaction(database, "app") as cursor:
        assert _count(cursor, date(2026, 11, 17)) == 0
        plan = daily.plan_session_cursor(cursor, session_date=date(2026, 11, 17), now=IN_SESSION)
    assert plan["status"] == "OPEN" and plan["would_insert"] == 2 and plan["allowed_from"] == "2026-11-17T21:20:00+00:00"


def test_h1_03_csv_copy_loads_the_same_rows_once(database, tmp_path):
    session = date(2026, 11, 10)
    path = tmp_path / "close-retained.csv.gz"
    with transaction(database) as cursor, gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
        cursor.copy_expert(
            "COPY (SELECT * FROM rt.option_breadth_current WHERE session_date = DATE '2026-11-10' "
            "ORDER BY underlying, contract_ticker) TO STDOUT WITH (FORMAT csv, HEADER true)", handle)
    with transaction(database, "app") as cursor:
        loaded = daily.load_csv_session_cursor(cursor, path=path, now=NOW, session_date=session)
    assert (loaded["inserted"], loaded["already_present"], loaded["complete"]) == (3, 0, True)
    assert loaded["snapshot_source"] == "CSV_COPY" and loaded["source_reference"].startswith("csv:close-retained.csv.gz:sha256:")
    with transaction(database, "app") as cursor:
        assert _differences(cursor, session) == 0
        again = daily.load_csv_session_cursor(cursor, path=path, now=NOW)
        assert (again["inserted"], again["already_present"]) == (0, 3)
        copy = daily.snapshot_session_cursor(cursor, session_date=session, now=NOW)
        assert (copy["inserted"], copy["already_present"], copy["complete"]) == (0, 3, True)
        cursor.execute("SELECT DISTINCT snapshot_source FROM rt.option_breadth_daily WHERE session_date=%s", (session,))
        assert cursor.fetchall() == [("CSV_COPY",)]
    wrong = tmp_path / "wrong.csv"
    wrong.write_text("session_date,contract_ticker\n2026-11-10,O:X\n", encoding="utf-8")
    with pytest.raises(ValueError, match="header"):
        with transaction(database, "app") as cursor:
            daily.load_csv_session_cursor(cursor, path=wrong, now=NOW)
    two = tmp_path / "two.csv"
    with transaction(database) as cursor, two.open("w", encoding="utf-8", newline="") as handle:
        cursor.copy_expert(
            "COPY (SELECT * FROM rt.option_breadth_current WHERE session_date IN ('2026-11-10','2026-11-13')) "
            "TO STDOUT WITH (FORMAT csv, HEADER true)", handle)
    with pytest.raises(ValueError, match="expected one session"):
        with transaction(database, "app") as cursor:
            daily.load_csv_session_cursor(cursor, path=two, now=NOW)


def test_h1_04_a_closed_session_is_copied_exactly_and_only_once(database):
    session = date(2026, 11, 12)
    with transaction(database, "app") as cursor:
        first = daily.snapshot_session_cursor(cursor, session_date=session, now=NOW)
    assert (first["source_rows"], first["inserted"], first["already_present"], first["complete"]) == (18, 18, 0, True)
    assert first["by_underlying"]["SPY"] == {"source_rows": 15, "inserted": 15, "inserted_final_capture": 13,
                                             "already_present": 0, "daily_rows": 15}
    assert first["by_underlying"]["NVDA"]["inserted_final_capture"] == 3
    with transaction(database, "app") as cursor:
        assert _differences(cursor, session) == 0
        cursor.execute("SELECT strike_price FROM rt.option_breadth_daily WHERE session_date=%s AND NOT final_capture "
                       "ORDER BY 1", (session,))
        assert [row[0] for row in cursor.fetchall()] == [Decimal("130"), Decimal("131")]
        cursor.execute("SELECT DISTINCT snapshot_at,snapshot_source,source_reference FROM rt.option_breadth_daily "
                       "WHERE session_date=%s", (session,))
        assert cursor.fetchall() == [(NOW, "CURRENT_TABLE", "rt.option_breadth_current")]
        second = daily.snapshot_session_cursor(cursor, session_date=session, now=NOW + timedelta(hours=1))
    assert (second["inserted"], second["already_present"], second["daily_rows"]) == (0, 18, 18)


def test_h1_05_backfill_of_the_retained_sessions(database):
    with transaction(database, "app") as cursor:
        results = daily.backfill_retained_cursor(cursor, now=IN_SESSION)
    by_session = {item["session_date"]: item for item in results}
    assert list(by_session) == ["2026-11-10", "2026-11-12", "2026-11-13", "2026-11-17"]
    assert by_session["2026-11-10"]["inserted"] == 0 and by_session["2026-11-12"]["inserted"] == 0
    assert (by_session["2026-11-13"]["inserted"], by_session["2026-11-13"]["complete"]) == (4, True)
    assert by_session["2026-11-17"]["status"] == "SKIPPED_OPEN"
    with transaction(database, "app") as cursor:
        for session in (date(2026, 11, 10), date(2026, 11, 12), date(2026, 11, 13)):
            assert _differences(cursor, session) == 0
        assert _count(cursor, date(2026, 11, 17)) == 0
        assert _count(cursor) == 3 + 18 + 4
    print(json.dumps({item["session_date"]: item.get("inserted", item.get("status")) for item in results}))


def test_h1_06_rows_are_immutable_and_the_api_role_only_reads(database):
    with pytest.raises(errors.RaiseException):
        with transaction(database, "app") as cursor:
            cursor.execute("UPDATE rt.option_breadth_daily SET day_volume=0 WHERE session_date='2026-11-12'")
    with pytest.raises(errors.InsufficientPrivilege):  # no DELETE grant; the trigger is the second guard
        with transaction(database, "app") as cursor:
            cursor.execute("DELETE FROM rt.option_breadth_daily WHERE session_date='2026-11-12'")
    with pytest.raises(errors.RaiseException):
        with transaction(database) as cursor:  # even the owner cannot delete a row
            cursor.execute("DELETE FROM rt.option_breadth_daily WHERE session_date='2026-11-12'")
    with pytest.raises(errors.InsufficientPrivilege):
        with transaction(database, "app") as cursor:
            cursor.execute("TRUNCATE rt.option_breadth_daily")
    with transaction(database, "api") as cursor:
        assert _count(cursor, date(2026, 11, 12)) == 18
    with pytest.raises(errors.InsufficientPrivilege):
        with transaction(database, "api") as cursor:
            cursor.execute(
                "INSERT INTO rt.option_breadth_daily SELECT *,true,now(),'CURRENT_TABLE','x' "
                "FROM rt.option_breadth_current WHERE session_date='2026-11-17'"
            )


def test_h1_07_iv_summary_of_a_past_session_from_the_daily_table(database):
    with transaction(database, "api") as cursor:
        summary = iv_summary_from_daily_cursor(cursor, underlyer="SPY", session_date=date(2026, 11, 12))
        missing = iv_summary_from_daily_cursor(cursor, underlyer="SPY", session_date=date(2026, 11, 17))
    assert summary["available"] is True and summary["source"] == "DAILY_SNAPSHOT"
    assert summary["contracts"] == 13 and summary["spot"] == "100"  # the two stale rows are not in the final capture
    (expiry,) = summary["expiries"]
    assert expiry["atm_iv"] == pytest.approx(0.205) and expiry["risk_reversal_25_delta"] == pytest.approx(-0.05)
    assert expiry["straddle_mid"] == "4.5"
    assert missing["reason"] == "NO_DAILY_SNAPSHOT"


def test_h1_08_the_cli_dry_run_apply_and_refusal(database, tmp_path, capsys):
    from scripts import snapshot_option_breadth_daily as cli

    settings = database.settings
    dsn = tmp_path / "dsn"
    dsn.write_text(
        f"host={settings['DB_HOST']} port={settings['DB_PORT']} dbname={DATABASE} "
        f"user={settings['POSTGRES_ADMIN_USER']} password={settings['POSTGRES_ADMIN_PASSWORD']}", encoding="utf-8")
    app_role = database.roles.name("app")
    assert cli.main(["--session", "2026-11-13", "--dsn-file", str(dsn), "--role", app_role]) == 0
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert lines[0]["status"] == "CLOSED" and lines[0]["would_insert"] == 0 and lines[-1]["applied"] is False
    assert cli.main(["--backfill-retained", "--apply", "--dsn-file", str(dsn), "--role", app_role]) == 0
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    # r138 (Nov 18): the CLI uses the real clock, so the seeded "open" Nov 17 session (2 rows) is open only before
    # Nov 17 21:20 UTC; on any later run the retained backfill copies it. Every other session was copied above.
    nov17_closed = datetime.now(UTC) >= daily.snapshot_allowed_at(date(2026, 11, 17))
    assert lines[-1]["inserted"] == (2 if nov17_closed else 0) and lines[-1]["applied"] is True
    # The CLI uses the real clock: a session a week ahead has not closed whenever this runs.
    from options.calendar import OptionExchangeCalendar

    ahead = OptionExchangeCalendar().session_on_or_after(datetime.now(UTC).date() + timedelta(days=7))
    assert cli.main(["--session", ahead.isoformat(), "--apply", "--dsn-file", str(dsn), "--role", app_role]) == 2
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert lines[0]["status"] == "REFUSED_OPEN" and lines[-1]["applied"] is False
    assert cli.main(["--session", "2026-11-15", "--apply", "--dsn-file", str(dsn), "--role", app_role]) == 2
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert lines[0]["status"] == "REFUSED_NOT_A_SESSION" and lines[-1]["applied"] is False
