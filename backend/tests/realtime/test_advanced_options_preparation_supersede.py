"""G102 design 5.1 (G90): a newer desired revision for a session date supersedes an Options preparation that was never
activated, instead of failing on UniqueViolation at every reconcile (162 restarts on Nov 6)."""
from datetime import date, datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest

import realtime.advanced_options_lifecycle as lifecycle
from realtime.advanced_realtime_desired_state import DesiredMemberKind
from realtime.migrations import V2_MIGRATIONS_DIR, load_realtime_migrations

NEW = UUID(int=23)
OLD = UUID(int=9)
DAY = date(2026, 11, 9)
CHECKED = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)


class Cursor:
    """Answers the statements the supersede step and the start of the reconcile run."""

    def __init__(self, current=None, rowcount=1):
        self.current, self.update_rowcount = current, rowcount
        self.statements, self.rowcount, self.sql = [], 0, ""

    def execute(self, sql, parameters=None):
        self.sql = sql
        self.statements.append((sql, parameters))
        self.rowcount = self.update_rowcount if sql.startswith("UPDATE") else 0

    def fetchone(self):
        if "AND revision_id<>%s FOR UPDATE" in self.sql:
            return self.current
        return None


def supersede(cursor):
    return lifecycle._supersede_date_preparation_cursor(
        cursor, revision_id=NEW, session_date=DAY, checked_at=CHECKED,
    )


def test_a_free_date_is_prepared_by_the_revision():
    cursor = Cursor()
    assert supersede(cursor) is None
    assert len(cursor.statements) == 1


@pytest.mark.parametrize("status", ["WAITING_EQUITY", "PREPARING", "DEGRADED", "FAILED"])
def test_a_never_activated_preparation_is_superseded_not_deleted(status):
    cursor = Cursor(current=(str(OLD), status, None))
    assert supersede(cursor) is None
    sql = [statement for statement, _ in cursor.statements]
    assert not any("DELETE" in statement for statement in sql)
    work, session = cursor.statements[1], cursor.statements[2]
    assert work[0].startswith("UPDATE rt.advanced_options_preparation_work SET status='BLOCKED'")
    assert "AND status IN ('PENDING','RUNNING','RETRY')" in work[0]
    assert work[1] == ("SUPERSEDED", CHECKED, "SUPERSEDED", CHECKED, f"SUPERSEDED_BY_REVISION:{NEW}", str(OLD))
    assert session[0].startswith("UPDATE rt.advanced_options_preparation_sessions SET status='DEGRADED'")
    assert "AND activated_session_binding_id IS NULL AND expiration_state IS NULL" in session[0]
    assert session[1][-1] == str(OLD) and session[1][3] == "SUPERSEDED"


@pytest.mark.parametrize("current", [(str(OLD), "READY", None), (str(OLD), "ACTIVE", UUID(int=5)),
                                     (str(OLD), "DEGRADED", UUID(int=5))])
def test_a_ready_or_activated_preparation_holds_the_date(current):
    cursor = Cursor(current=current)
    assert supersede(cursor) == f"OPTIONS_DATE_HELD_BY_REVISION:{OLD}:{current[1]}"
    assert len(cursor.statements) == 1


def test_a_supersede_that_loses_its_row_fails_the_transaction():
    with pytest.raises(ValueError, match="supersede lost its scope"):
        supersede(Cursor(current=(str(OLD), "PREPARING", None), rowcount=0))


def test_reconcile_waits_on_a_held_date_without_writing(monkeypatch):
    member = SimpleNamespace(ticker="AAPL", enabled=True, kind=DesiredMemberKind.OPTION_UNDERLYING)
    revision = SimpleNamespace(revision_id=NEW, effective_session_date=DAY, members=(member,))
    monkeypatch.setattr(lifecycle, "load_current_desired_state_revision_cursor", lambda _cursor: revision)
    calendar = SimpleNamespace(session_on_or_after=lambda day: day)
    cursor = Cursor(current=(str(OLD), "READY", None))
    status = lifecycle.reconcile_advanced_options_lifecycle_cursor(
        cursor, target_session_date=DAY, checked_at=CHECKED, calendar=calendar,
    )
    assert (status.revision_id, status.status, status.reason_codes) == (
        NEW, "WAITING_DESIRED", (f"OPTIONS_DATE_HELD_BY_REVISION:{OLD}:READY",))
    assert not any(statement.startswith(("INSERT", "UPDATE")) for statement, _ in cursor.statements)


def test_migration_049_makes_the_date_unique_among_current_preparations_only():
    migrations = load_realtime_migrations(V2_MIGRATIONS_DIR)
    latest = next(item for item in migrations if item.sequence == 49)
    assert latest.version == "049_g102_options_preparation_supersede"
    assert "DROP CONSTRAINT advanced_options_preparation_sessions_session_date_key" in latest.sql
    assert "ON rt.advanced_options_preparation_sessions (session_date)\n    WHERE expiration_state IS NULL" in latest.sql
