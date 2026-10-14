"""Offline and disposable-database checks for the V2 realtime audit scripts.

Offline tests use a recording cursor and a fake connection. The DB-backed tests opt in with
STOCKS_V2_DB_TESTS=1, run WITHOUT the offline I/O guard plugin, and use the disposable database
test_realtime_v2_scr_audit (created from the sql_v2 baseline and always dropped).
"""
from __future__ import annotations

import re
import sys
from contextlib import contextmanager
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import exchange_calendars
import pytest
from psycopg2 import errors
from psycopg2.extensions import STATUS_READY
BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.equity_bar_adapter import adapt_developer_equity_bar  # noqa: E402
from realtime.equity_bar_repository import persist_realtime_equity_bars_cursor  # noqa: E402
from realtime.equity_evidence_repository import (  # noqa: E402
    RealtimeFeatureDefinition, persist_realtime_equity_materialization_cursor,
)
from realtime.equity_materialization_adapter import materialize_realtime_equity_evidence  # noqa: E402
from scripts import audit_realtime_historical_coverage as coverage  # noqa: E402
from scripts import audit_realtime_ticker_history as history  # noqa: E402
from scripts import realtime_v2_audit_support as support  # noqa: E402
from tests.realtime.test_equity_bar_adapter import _developer_revision  # noqa: E402
from tests.realtime.test_equity_materialization_adapter import _security  # noqa: E402
from v2_database import disposable_v2_database, insert_cohort, requires_v2_database  # noqa: E402

UTC = timezone.utc
DATABASE = "test_realtime_v2_scr_audit"
WRITE_WORDS = re.compile(r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|COPY)\b", re.IGNORECASE)


class RecordingCursor:
    """Returns empty result sets and records every statement."""

    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple]] = []

    def execute(self, statement, parameters=()):
        self.statements.append((statement, tuple(parameters)))

    def fetchall(self):
        return []

    def fetchone(self):
        return {"checked_sources": 0, "missing_sources": 0, "pointer_count": 0, "mismatches": 0}


def _control():
    return {"database": DATABASE, "lineage": "advanced_realtime_v2", "read_only": "on",
            "isolation": "repeatable read", "checked_at": datetime(2026, 10, 13, 12, 0, tzinfo=UTC)}


def _assert_select_only_with_matching_parameters(cursor: RecordingCursor):
    assert cursor.statements
    for statement, parameters in cursor.statements:
        assert statement.lstrip().upper().startswith(("SELECT", "WITH"))
        assert not WRITE_WORDS.search(statement)
        assert statement.count("%s") == len(parameters), statement


def test_historical_coverage_queries_are_select_only_and_parameterized():
    cursor = RecordingCursor()
    report = coverage.build_report(cursor, _control(), start_date=date(2026, 10, 5), end_date=date(2026, 10, 9),
                                   tickers=("SPY", "AAPL"))
    _assert_select_only_with_matching_parameters(cursor)
    assert report["status"] == "GAPS_FOUND"
    assert [row["ticker"] for row in report["per_ticker"]] == ["SPY", "AAPL"]
    assert "SECURITY_IDENTITY_MISSING" in report["per_ticker"][0]["downstream_blockers"]
    assert report["controls"]["database_writes"] == report["controls"]["provider_requests"] == 0
    assert report["controls"]["lineage"] == "advanced_realtime_v2"
    assert "immutable_selection" not in report
    assert set(report["per_ticker"][0]["bar_gaps"]) == {name for name, *_ in coverage.BAR_FAMILIES}


def test_ticker_history_queries_are_select_only_and_parameterized():
    cursor = RecordingCursor()
    report = history.build_report(cursor, _control(), cohort_id=UUID(int=7), session_date=date(2026, 10, 9))
    _assert_select_only_with_matching_parameters(cursor)
    assert report["cohort_id"] == str(UUID(int=7))
    assert report["database_writes"] == report["provider_requests"] == 0
    assert report["evidence_pointers"] == {"checked_sources": 0, "missing_sources": 0, "pointer_count": 0,
                                           "mismatches": 0}
    assert report["daily_scale"] == []


def test_bar_gap_report_counts_missing_and_incomplete_sessions():
    sessions = (date(2026, 10, 5), date(2026, 10, 6), date(2026, 10, 7))
    rows = [
        {"interval": "30m", "adjustment_basis": "RAW_UNADJUSTED", "session_scope": "RTH",
         "session_date": sessions[0], "bar_count": 13},
        {"interval": "30m", "adjustment_basis": "RAW_UNADJUSTED", "session_scope": "RTH",
         "session_date": sessions[1], "bar_count": 4},
        {"interval": "30m", "adjustment_basis": "ADJUSTED", "session_scope": "RTH",
         "session_date": sessions[2], "bar_count": 13},
    ]
    report = coverage._bar_gap_report(rows, sessions, interval="30m", adjustment_basis="RAW_UNADJUSTED",
                                      session_scope="RTH")
    assert report["covered_session_count"] == 2 and report["missing_session_count"] == 1
    assert report["missing_session_ranges"] == ({"start": "2026-10-07", "end": "2026-10-07"},)
    assert report["incomplete_sessions"] == ({"session_date": "2026-10-06", "actual_count": 4, "expected_count": 13},)


def test_daily_scale_compares_adjusted_and_raw_closes():
    rows = [
        {"ticker": "SPY", "interval": "1d", "adjustment_basis": "ADJUSTED", "latest_close": 50.0},
        {"ticker": "SPY", "interval": "1d", "adjustment_basis": "RAW_UNADJUSTED", "latest_close": 100.0},
        {"ticker": "SPY", "interval": "30m", "adjustment_basis": "RAW_UNADJUSTED", "latest_close": 1.0},
        {"ticker": "QQQ", "interval": "1d", "adjustment_basis": "RAW_UNADJUSTED", "latest_close": 9.0},
    ]
    assert history._scale(rows) == [
        {"ticker": "QQQ", "adjusted_close": None, "raw_close": 9.0, "absolute_difference": None,
         "relative_difference": None},
        {"ticker": "SPY", "adjusted_close": 50.0, "raw_close": 100.0, "absolute_difference": -50.0,
         "relative_difference": 1.0},
    ]


_ARGUMENTS = {
    coverage: ["--end-date", "2026-10-09"],
    history: ["--cohort-id", str(UUID(int=1)), "--session-date", "2026-10-09"],
}


@pytest.mark.parametrize("module", [coverage, history])
def test_targets_must_be_v2_and_confirmed(module, tmp_path):
    extra = [*_ARGUMENTS[module], "--report-path", str(tmp_path / "r.json")]
    with pytest.raises(ValueError, match="realtime v2"):
        module.parse_args(["--database", "stocks_realtime_v1", "--confirm-database-name", "stocks_realtime_v1",
                           "--allow-disposable", *extra])
    with pytest.raises(ValueError, match="realtime v2"):
        module.parse_args(["--database", DATABASE, "--confirm-database-name", DATABASE, *extra])
    with pytest.raises(ValueError, match="confirmation"):
        module.parse_args(["--database", DATABASE, "--confirm-database-name", DATABASE + "x",
                           "--allow-disposable", *extra])
    parsed = module.parse_args(["--database", DATABASE, "--confirm-database-name", DATABASE,
                                "--allow-disposable", *extra])
    assert parsed.database == DATABASE


def test_coverage_default_start_is_the_required_adjusted_history(tmp_path):
    args = coverage.parse_args(["--database", DATABASE, "--confirm-database-name", DATABASE, "--allow-disposable",
                                "--end-date", "2026-10-09", "--report-path", str(tmp_path / "r.json")])
    assert args.start_date < args.end_date
    assert len(coverage._expected_sessions(args.start_date, args.end_date)) == 283
    with pytest.raises(SystemExit):
        coverage.parse_args(["--database", DATABASE, "--confirm-database-name", DATABASE, "--allow-disposable",
                             "--start-date", "2026-10-09", "--end-date", "2026-10-01",
                             "--report-path", str(tmp_path / "r.json")])


def test_report_path_must_be_new_and_inside_the_workspace(tmp_path):
    inside = tmp_path / "report.json"
    assert support.require_new_workspace_report(inside, tmp_path) == inside.resolve()
    inside.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="new workspace-local"):
        support.require_new_workspace_report(inside, tmp_path)
    with pytest.raises(ValueError, match="new workspace-local"):
        support.require_new_workspace_report(tmp_path.parent / "elsewhere.json", tmp_path)


class _FakeCursor:
    def __init__(self, connection) -> None:
        self.connection = connection
        self.rows = [dict(database=connection.database, read_only=connection.read_only,
                          isolation="repeatable read", checked_at=datetime(2026, 10, 13, tzinfo=UTC)),
                     dict(database=connection.database, lineage_name=connection.lineage)]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, parameters=()):
        self.connection.statements.append((statement, parameters))

    def fetchone(self):
        return self.rows.pop(0)


class _FakeConnection:
    def __init__(self, database=DATABASE, lineage="advanced_realtime_v2", read_only="on") -> None:
        self.database, self.lineage, self.read_only = database, lineage, read_only
        self.statements, self.session, self.rollbacks = [], None, 0

    def set_session(self, **options):
        self.session = options

    def cursor(self, cursor_factory=None):
        return _FakeCursor(self)

    def rollback(self):
        self.rollbacks += 1


def test_read_only_transaction_sets_the_session_timeout_and_always_rolls_back():
    connection = _FakeConnection()
    with support.read_only_transaction(connection, DATABASE) as (cursor, control):
        assert control["lineage"] == "advanced_realtime_v2"
    assert connection.session == {"readonly": True, "isolation_level": "REPEATABLE READ"}
    assert connection.statements[0] == ("SELECT set_config('statement_timeout', %s, true)", ("30s",))
    assert connection.rollbacks == 1


@pytest.mark.parametrize("connection", [_FakeConnection(database="stocks_realtime_v1"),
                                        _FakeConnection(lineage="advanced_realtime_v1"),
                                        _FakeConnection(read_only="off")])
def test_read_only_transaction_rejects_a_wrong_target_lineage_or_writable_session(connection):
    with pytest.raises(RuntimeError, match="audit"):
        with support.read_only_transaction(connection, DATABASE):
            pytest.fail("the body must not run")
    assert connection.rollbacks == 1


# ---- disposable V2 database -------------------------------------------------------------------------


@pytest.fixture(scope="module")
def pool():
    with disposable_v2_database(DATABASE) as database_pool:
        yield database_pool


@contextmanager
def _connection(pool):
    connection = pool.getconn()
    try:
        yield connection
    finally:
        connection.rollback()
        connection.set_session(readonly=False, isolation_level="READ COMMITTED")
        pool.putconn(connection)


def _seed_security(cursor, ticker: str):
    """A cohort whose manifest has one AM equity member; returns (cohort, security, instrument)."""
    now = datetime.now(UTC).replace(microsecond=0)
    cohort = insert_cohort(cursor, now)
    cursor.execute("SELECT manifest_id::text FROM rt.cohorts WHERE cohort_id=%s", (str(cohort),))
    manifest = cursor.fetchone()[0]
    security = uuid4()
    cursor.execute("INSERT INTO ref.securities (security_id, asset_type, source_origin, created_at) "
                   "VALUES (%s, 'ETF', 'fixture', %s)", (str(security), now - timedelta(days=400)))
    cursor.execute("INSERT INTO ref.security_revisions (revision_id, security_id, ticker, exchange, calendar_id, "
                   "effective_from, observed_at, payload_sha256) VALUES (%s, %s, %s, 'ARCX', 'XNYS', %s, %s, %s)",
                   (str(uuid4()), str(security), ticker, now - timedelta(days=399), now - timedelta(days=399),
                    "a" * 64))
    cursor.execute("INSERT INTO rt.instruments (asset_class, security_id) VALUES ('EQUITY', %s) "
                   "RETURNING instrument_id", (str(security),))
    instrument = cursor.fetchone()[0]
    cursor.execute("INSERT INTO rt.subscription_members (manifest_id, instrument_id, channel, priority, purpose, "
                   "option_underlying) VALUES (%s, %s, 'AM', 1, 'PRIMARY', false)", (manifest, instrument))
    return cohort, security, instrument


def _daily_bar(cohort, security, instrument, ticker, start, end, session, *, adjusted):
    prices = dict(open_price=Decimal("50"), high_price=Decimal("51"), low_price=Decimal("49"),
                  close_price=Decimal("50.5")) if adjusted else {}
    revision = replace(
        _developer_revision(), bar_revision_id=uuid4(), security_id=security, ticker=ticker, interval="1d",
        session_date=session, bar_start=start, bar_end=end, system_observed_at=end + timedelta(minutes=5),
        adjusted=adjusted, payload_sha256=uuid4().hex + uuid4().hex, ingestion_segment_id=uuid4(), **prices)
    return adapt_developer_equity_bar(revision, cohort_id=cohort, instrument_id=instrument)


def _recent_sessions(count: int) -> list[date]:
    calendar = exchange_calendars.get_calendar("XNYS")
    window = calendar.sessions_in_range(date.today() - timedelta(days=20), date.today() - timedelta(days=1))
    return [value.date() for value in window[-count:]]


@requires_v2_database
def test_historical_coverage_reads_v2_bars_and_references_read_only(pool):
    sessions = _recent_sessions(3)
    calendar = exchange_calendars.get_calendar("XNYS")
    with _connection(pool) as connection:
        with connection.cursor() as cursor:
            cohort, security, instrument = _seed_security(cursor, "QQQ")
            facts = tuple(
                _daily_bar(cohort, security, instrument, "QQQ", calendar.session_open(session).to_pydatetime(),
                           calendar.session_close(session).to_pydatetime(), session, adjusted=adjusted)
                for session in sessions for adjusted in (False, True))
            persist_realtime_equity_bars_cursor(cursor, facts)
        connection.commit()
        report = coverage.run_audit(connection, database=DATABASE, start_date=sessions[0], end_date=sessions[-1],
                                    tickers=("QQQ", "AAPL"), cohort_id=cohort)
        assert report["controls"]["transaction_read_only"] == "on"
        qqq, aapl = report["per_ticker"]
        assert [row["revision_count"] for row in qqq["bar_coverage"]] == [3, 3]
        assert {(row["interval"], row["adjustment_basis"]) for row in qqq["bar_coverage"]} == {
            ("1d", "ADJUSTED"), ("1d", "RAW_UNADJUSTED")}
        for family in ("raw_final_daily", "adjusted_final_daily"):
            assert qqq["bar_gaps"][family]["covered_session_count"] == 3
            assert qqq["bar_gaps"][family]["missing_session_count"] == 0
        assert qqq["bar_gaps"]["raw_final_1m"]["missing_session_count"] == 3
        assert "RAW_FINAL_1M_COVERAGE_GAP" in qqq["downstream_blockers"]
        assert "RAW_FINAL_DAILY_COVERAGE_GAP" not in qqq["downstream_blockers"]
        assert "SECURITY_IDENTITY_MISSING" not in qqq["downstream_blockers"]
        assert "SECURITY_IDENTITY_MISSING" in aapl["downstream_blockers"]
        assert aapl["bar_coverage"] == ()
        assert report["status"] == "GAPS_FOUND"
        other_cohort = coverage.run_audit(connection, database=DATABASE, start_date=sessions[0],
                                          end_date=sessions[-1], tickers=("QQQ",), cohort_id=uuid4())
        assert other_cohort["per_ticker"][0]["bar_coverage"] == ()
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM rt.bar_current WHERE cohort_id=%s", (str(cohort),))
            assert cursor.fetchone()[0] == 6


def _evidence_for_session(cursor, cohort, security, instrument):
    """Persist 60 thirty-minute bars and the evidence materialized from them; return the audited session."""
    last_end = (datetime.now(UTC) - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    first_start = last_end - timedelta(minutes=30 * 60)
    facts = tuple(
        adapt_developer_equity_bar(
            replace(_developer_revision(), bar_revision_id=uuid4(), security_id=security, interval="30m",
                    session_date=(first_start + timedelta(minutes=30 * index)).date(),
                    bar_start=first_start + timedelta(minutes=30 * index),
                    bar_end=first_start + timedelta(minutes=30 * (index + 1)),
                    system_observed_at=first_start + timedelta(minutes=30 * (index + 1)),
                    payload_sha256=f"{index + 1:064x}", ingestion_segment_id=uuid4()),
            cohort_id=cohort, instrument_id=instrument)
        for index in range(60))
    persist_realtime_equity_bars_cursor(cursor, facts)
    result = materialize_realtime_equity_evidence(
        analysis_run_id=uuid4(), security=replace(_security(), security_id=security), bars=facts,
        observed_at=facts[-1].revision.bar_end)
    definition = RealtimeFeatureDefinition(
        bundle_sha256="b" * 64, bundle_version="equity_features_v1", feature_schema_version="1.0",
        dependency_spec={"bars": {"interval": "1m", "final": True}}, policy_sha256="c" * 64, code_sha256="d" * 64)
    persist_realtime_equity_materialization_cursor(cursor, result, definition, generation=1)
    cursor.execute("SELECT (market_time AT TIME ZONE 'America/New_York')::date FROM "
                   "rt.equity_evidence_source_windows WHERE cohort_id=%s", (str(cohort),))
    return cursor.fetchone()[0]


@requires_v2_database
def test_ticker_history_audit_reads_v2_evidence_pointers_and_bars_read_only(pool):
    with _connection(pool) as connection:
        with connection.cursor() as cursor:
            cohort, security, instrument = _seed_security(cursor, "SPY")
            session = _evidence_for_session(cursor, cohort, security, instrument)
            start = datetime.combine(session, time(14, 30), tzinfo=UTC)
            persist_realtime_equity_bars_cursor(cursor, tuple(
                _daily_bar(cohort, security, instrument, "SPY", start, start + timedelta(hours=6, minutes=30),
                           session, adjusted=adjusted) for adjusted in (False, True)))
            cursor.execute("SELECT count(*) FROM rt.equity_evidence WHERE cohort_id=%s AND source_window_id IS NOT NULL",
                           (str(cohort),))
            windowed_evidence = cursor.fetchone()[0]
            cursor.execute("SELECT cardinality(primary_bar_revision_ids) + cardinality(confirmation_bar_revision_ids) "
                           "FROM rt.equity_evidence_source_windows WHERE cohort_id=%s", (str(cohort),))
            window_sources = cursor.fetchone()[0]
        connection.commit()
        report = history.run_audit(connection, database=DATABASE, cohort_id=cohort, session_date=session)
        assert report["transaction_read_only"] == "on" and report["lineage"] == "advanced_realtime_v2"
        (member,) = report["reference_membership"]
        assert member["visible_revision_count"] == 1 and member["tickers"] == ["SPY"]
        (validity,) = report["reference_validity"]
        assert validity["ticker"] == "SPY" and validity["has_open_ended_revision"] is True
        (closure,) = report["historical_evidence_counts"]
        assert closure["interval"] == "30m" and closure["closures"] == 1
        assert closure["evidence_rows"] == windowed_evidence > 0
        assert report["historical_lineage"] == {"checked_sources": window_sources, "missing_sources": 0,
                                                "future_market_sources": 0, "future_observation_sources": 0}
        assert report["evidence_pointers"]["pointer_count"] > 0 and report["evidence_pointers"]["mismatches"] == 0
        assert report["current_releases"] == () and report["current_screening"] == ()
        counts = {(row["interval"], row["adjustment_basis"]): row for row in report["interval_counts"]}
        assert counts[("1d", "RAW_UNADJUSTED")]["latest_close"] == 101.0
        assert counts[("1d", "ADJUSTED")]["latest_close"] == 50.5
        assert counts[("30m", "RAW_UNADJUSTED")]["count"] > 0
        (scale,) = report["daily_scale"]
        assert scale["ticker"] == "SPY" and scale["relative_difference"] == pytest.approx(1.0)
        assert {(row["interval"], row["adjustment_basis"]) for row in report["latest_retained_sessions"]} >= {
            ("1d", "ADJUSTED"), ("1d", "RAW_UNADJUSTED"), ("30m", "RAW_UNADJUSTED")}
        empty = history.run_audit(connection, database=DATABASE, cohort_id=uuid4(), session_date=session)
        assert empty["reference_membership"] == () and empty["interval_counts"] == ()


@requires_v2_database
def test_the_audit_transaction_is_read_only_bounded_and_identity_checked(pool):
    with _connection(pool) as connection:
        with support.read_only_transaction(connection, DATABASE) as (cursor, control):
            assert control["read_only"] == "on" and control["isolation"] == "repeatable read"
            cursor.execute("SHOW statement_timeout")
            assert cursor.fetchone()["statement_timeout"] == "30s"
            with pytest.raises(errors.ReadOnlySqlTransaction):
                cursor.execute("INSERT INTO rt.schema_migrations (version, checksum_sha256) VALUES ('x', %s)",
                               ("f" * 64,))
        with pytest.raises(RuntimeError, match="audit target"):
            with support.read_only_transaction(connection, "test_realtime_v2_scr_other"):
                pytest.fail("the body must not run")
        assert connection.status == STATUS_READY  # rolled back, nothing left open
