"""G102 design 5.3 (G99, B23): planner statistics for the tables the session writes fastest.

On Nov 6 `rt.bar_current` statistics predated the day's cohort (43,249 changes, under the 10% autoanalyze threshold
of a 467k-row table); the planner estimated one row, chose a nested loop, and the controller's repair query took 42 s
until a manual ANALYZE (0.06 s). The retention service runs this step every few minutes under its leadership lock,
with its admin connection (the admin role owns these tables; ANALYZE needs ownership):

- a watched table is analyzed when its statistics predate the session's activation (30 minutes before the open) or
  more than 5,000 rows changed since its last analyze;
- each watched table gets `autovacuum_analyze_scale_factor = 0, autovacuum_analyze_threshold = 5000`, so autovacuum
  follows the same rule between steps (a new monthly partition gets it on its first step);
- the partitioned parent `rt.bar_revisions`, which autovacuum never analyzes, is analyzed once after the close;
- the result lists the last-analyzed times, and an alarm names a watched table whose statistics still predate the
  activation at the start of a step once earlier steps have had time to analyze it (an ANALYZE without ownership
  only warns).

The watched names are fixed here (plus the current month's `rt.bar_revisions` partition), never taken from input.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re

UTC = timezone.utc
STATISTICS_INTERVAL_SECONDS = 300
MODIFIED_ROWS_THRESHOLD = 5_000
ACTIVATION_LEAD = timedelta(minutes=30)
WATCHED_TABLES = ("bar_catalog", "bar_head")  # G102 A4: the head is the hot current-bar table
PARTITIONED_PARENT = "bar_revisions"
AUTOANALYZE_OPTIONS = ("autovacuum_analyze_scale_factor=0", "autovacuum_analyze_threshold=5000")
_NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def watched_tables(checked_at: datetime) -> tuple[str, ...]:
    """The fixed watch list plus the `rt.bar_revisions` partition of the current UTC month."""
    return (*WATCHED_TABLES, f"bar_revisions_p{checked_at.astimezone(UTC):%Y%m}01")


def _qualified(table: str) -> str:
    if not _NAME.fullmatch(table):
        raise ValueError(f"statistics table name is invalid: {table!r}")
    return f'rt."{table}"'


def _statistics_cursor(cursor, tables: tuple[str, ...]) -> dict[str, tuple[int, datetime | None, tuple[str, ...]]]:
    cursor.execute(
        "SELECT stat.relname,stat.n_mod_since_analyze,"
        "GREATEST(stat.last_analyze,stat.last_autoanalyze),class.reloptions "
        "FROM pg_stat_user_tables AS stat JOIN pg_class AS class ON class.oid=stat.relid "
        "WHERE stat.schemaname='rt' AND stat.relname=ANY(%s)",
        (list(tables),),
    )
    return {
        str(row[0]): (int(row[1] or 0), row[2], tuple(row[3] or ()))
        for row in cursor.fetchall()
    }


def refresh_planner_statistics_cursor(
    cursor,
    *,
    checked_at: datetime,
    session_open: datetime | None,
    session_close: datetime | None,
    threshold: int = MODIFIED_ROWS_THRESHOLD,
) -> dict[str, object]:
    """Analyze what is stale (see the module docstring) and report the statistics' state. `session_open` and
    `session_close` are today's, or None on a day without a session."""
    tables = watched_tables(checked_at)
    activation = None if session_open is None else session_open - ACTIVATION_LEAD
    active = activation is not None and checked_at >= activation
    # One read: the statistics views are cached for the transaction, so the state after this step's ANALYZEs is
    # taken from what it analyzed, not read again.
    before = _statistics_cursor(cursor, (*tables, PARTITIONED_PARENT))
    analyzed, tuned = [], []
    for table in tables:
        if table not in before:
            continue  # the month's partition does not exist yet
        modified, last, options = before[table]
        if not set(AUTOANALYZE_OPTIONS) <= set(options):
            cursor.execute(
                f"ALTER TABLE {_qualified(table)} SET "
                "(autovacuum_analyze_scale_factor = 0, autovacuum_analyze_threshold = 5000)"
            )
            tuned.append(table)
        if last is None or modified > threshold or (active and last < activation):
            cursor.execute(f"ANALYZE {_qualified(table)}")
            analyzed.append(table)
    parent = before.get(PARTITIONED_PARENT)
    if (
        session_close is not None and checked_at >= session_close
        and parent is not None and (parent[1] is None or parent[1] < session_close)
    ):
        cursor.execute(f"ANALYZE {_qualified(PARTITIONED_PARENT)}")
        analyzed.append(PARTITIONED_PARENT)
    after = {table: checked_at if table in analyzed else values[1] for table, values in before.items()}
    last_analyzed = {table: None if value is None else value.isoformat() for table, value in sorted(after.items())}
    # An ANALYZE that cannot take effect (a role that does not own the table only gets a warning) leaves the
    # statistics predating the activation at the start of the next steps: alarm once two steps have had the chance.
    late = active and checked_at >= activation + timedelta(seconds=2 * STATISTICS_INTERVAL_SECONDS)
    alarms = sorted(
        table for table in tables
        if late and table in before and (before[table][1] is None or before[table][1] < activation)
    )
    return {
        "checked_at": checked_at.isoformat(),
        "activation": None if activation is None else activation.isoformat(),
        "analyzed": analyzed,
        "autoanalyze_tuned": tuned,
        "last_analyzed": last_analyzed,
        "alarms": alarms,
        "state": "ALARM" if alarms else "READY",
    }
