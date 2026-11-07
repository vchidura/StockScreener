"""G102 design 5.3 and 5.4 (G99, B23, B24): planner statistics refreshed by the retention service, and the index for the
journal bridge's offset check."""
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace

import pytest

from realtime.migrations import V2_MIGRATIONS_DIR, load_realtime_migrations
from realtime.statistics_maintenance import (
    AUTOANALYZE_OPTIONS,
    refresh_planner_statistics_cursor,
    watched_tables,
)
from scripts import run_realtime_v2_retention_service as script

UTC = timezone.utc
OPEN = datetime(2026, 11, 9, 14, 30, tzinfo=UTC)
CLOSE = datetime(2026, 11, 9, 21, 0, tzinfo=UTC)
YESTERDAY = datetime(2026, 11, 6, 21, 30, tzinfo=UTC)
TUNED = list(AUTOANALYZE_OPTIONS)


class StatsCursor:
    def __init__(self, rows):
        self.rows, self.statements = rows, []

    def execute(self, sql, params=()):
        self.statements.append(sql)
        self.result = (
            [(name, *values) for name, values in self.rows.items() if name in params[0]]
            if sql.startswith("SELECT stat.relname") else []
        )

    def fetchall(self):
        return self.result


def refresh(rows, checked_at):
    cursor = StatsCursor(rows)
    result = refresh_planner_statistics_cursor(cursor, checked_at=checked_at, session_open=OPEN, session_close=CLOSE)
    return result, [sql for sql in cursor.statements if not sql.startswith("SELECT")]


def test_the_watch_list_follows_the_month():
    assert watched_tables(OPEN) == ("bar_catalog", "bar_head", "bar_revisions_p20261101")
    assert watched_tables(datetime(2026, 12, 1, 0, 5, tzinfo=UTC))[-1] == "bar_revisions_p20261201"


def test_before_activation_fresh_statistics_are_left_alone():
    rows = {name: (100, YESTERDAY, TUNED) for name in watched_tables(OPEN)} | {"bar_revisions": (0, YESTERDAY, [])}
    result, writes = refresh(rows, OPEN - timedelta(hours=1))
    assert writes == [] and result["analyzed"] == [] and result["state"] == "READY"


def test_at_activation_yesterdays_statistics_are_refreshed_and_autoanalyze_is_tuned():
    rows = {"bar_catalog": (100, YESTERDAY, []), "bar_head": (100, YESTERDAY, TUNED),
            "bar_revisions_p20261101": (100, None, ["fillfactor=80"])}
    result, writes = refresh(rows, OPEN - timedelta(minutes=29))
    assert writes == [
        'ALTER TABLE rt."bar_catalog" SET (autovacuum_analyze_scale_factor = 0, autovacuum_analyze_threshold = 5000)',
        'ANALYZE rt."bar_catalog"',
        'ANALYZE rt."bar_head"',
        'ALTER TABLE rt."bar_revisions_p20261101" SET '
        '(autovacuum_analyze_scale_factor = 0, autovacuum_analyze_threshold = 5000)',
        'ANALYZE rt."bar_revisions_p20261101"',
    ]
    assert result["autoanalyze_tuned"] == ["bar_catalog", "bar_revisions_p20261101"]
    assert result["last_analyzed"]["bar_head"] == (OPEN - timedelta(minutes=29)).isoformat()


def test_during_the_session_only_tables_with_many_changes_are_analyzed():
    fresh = OPEN + timedelta(minutes=5)
    rows = {"bar_catalog": (4_000, fresh, TUNED), "bar_head": (5_001, fresh, TUNED),
            "bar_revisions_p20261101": (12_000, fresh, TUNED)}
    result, writes = refresh(rows, OPEN + timedelta(hours=1))
    assert writes == ['ANALYZE rt."bar_head"', 'ANALYZE rt."bar_revisions_p20261101"']
    assert result["state"] == "READY"


def test_after_the_close_the_partitioned_parent_is_analyzed_once():
    fresh = CLOSE - timedelta(minutes=5)
    rows = {name: (0, fresh, TUNED) for name in watched_tables(CLOSE)}
    result, writes = refresh(dict(rows, bar_revisions=(0, YESTERDAY, [])), CLOSE + timedelta(minutes=10))
    assert writes == ['ANALYZE rt."bar_revisions"']
    _again, writes = refresh(dict(rows, bar_revisions=(0, CLOSE + timedelta(minutes=10), [])),
                             CLOSE + timedelta(minutes=15))
    assert writes == []


def test_statistics_that_stay_stale_raise_an_alarm():
    """An ANALYZE that cannot take effect (no ownership) leaves the statistics predating activation."""
    rows = {name: (0, YESTERDAY, TUNED) for name in watched_tables(OPEN)}
    early, _ = refresh(rows, OPEN - timedelta(minutes=25))
    late, _ = refresh(rows, OPEN - timedelta(minutes=15))
    assert early["state"] == "READY" and late["state"] == "ALARM"
    assert late["alarms"] == sorted(watched_tables(OPEN))


def loop_args(*, once):
    return SimpleNamespace(
        once=once, interval_seconds=3600, statistics_interval_seconds=300,
        partition_relation=("rt.portal_snapshot_hot",), retention_relation=("rt.raw_payloads",),
        archive_relation=("rt.option_flow_clusters",), archive_kind=("OPTION_FLOW_CLUSTERS",),
    )


def test_the_retention_loop_runs_the_statistics_step_between_cycles(tmp_path):
    args = loop_args(once=False)
    now = {"value": OPEN}
    cycles, steps, waits, output = [], [], [], []

    class Stop:
        def is_set(self):
            return len(cycles) >= 2

        def wait(self, seconds):
            waits.append(seconds)
            now["value"] += timedelta(seconds=seconds)
            return False

    class Cycle:
        def run(self, request, *, execute):
            cycles.append(now["value"])
            return SimpleNamespace(state="READY", as_dict=lambda: {"state": "READY"})

    def statistics(checked_at):
        steps.append(checked_at)
        if len(steps) == 2:
            raise RuntimeError("lock timeout")
        return {"statistics": {"state": "READY"}}

    script.run_cycles(Cycle(), args, stop=Stop(), clock=lambda: now["value"], emit=output.append,
                      calendar=SimpleNamespace(latest_completed_session=lambda _at: OPEN.date()),
                      statistics=statistics)
    assert cycles == [OPEN, OPEN + timedelta(hours=1)]
    assert steps[:3] == [OPEN, OPEN + timedelta(minutes=5), OPEN + timedelta(minutes=10)]
    assert len(steps) == 13 and set(waits) == {60}
    assert json.loads(output[2]) == {"statistics": {"error": "lock timeout", "state": "FAILED"}}


def test_without_the_statistics_step_the_loop_is_unchanged():
    args = loop_args(once=True)
    calls = []

    class Cycle:
        def run(self, request, *, execute):
            calls.append(request)
            return SimpleNamespace(state="READY", as_dict=lambda: {"state": "READY"})

    output = []
    assert script.run_cycles(Cycle(), args, stop=SimpleNamespace(is_set=lambda: False), clock=lambda: OPEN,
                             calendar=SimpleNamespace(latest_completed_session=lambda _at: OPEN.date()),
                             emit=output.append) == 0
    assert len(calls) == 1 and output == ['{"state": "READY"}']


@pytest.mark.parametrize("version,fragments", [
    ("050_g102_bar_revision_journal_offset", (
        "CREATE INDEX idx_rt_bar_revision_journal_offset",
        "ON rt.bar_revisions (journal_generation, stream_session_id, source_offset)",
        "WHERE stream_session_id IS NOT NULL",
    )),
])
def test_migration_050_indexes_the_bridge_offset_check(version, fragments):
    migration = next(item for item in load_realtime_migrations(V2_MIGRATIONS_DIR) if item.version == version)
    assert migration.sequence == 50 and all(fragment in migration.sql for fragment in fragments)
