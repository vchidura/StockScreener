from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from realtime.partition_maintenance import (
    PartitionAlarm,
    _partition_ranges,
    ensure_selected_partitions_and_check,
    partition_creation_plan,
)
from realtime.retention_maintenance import (
    ARCHIVE_FIRST_RELATIONS,
    HEALTH_BLOCKED,
    HEALTH_DEGRADED,
    HEALTH_READY,
    STATUS_ALARM,
    STATUS_READY,
    MaintenanceAlarm,
    RetentionMaintenanceCycle,
    RetentionMaintenanceRequest,
    RetentionMaintenanceStatus,
    assess_retention_health,
    read_retention_status,
    write_retention_status,
)
from realtime.session_archive_worker import ArchiveAlarm

UTC = timezone.utc
NOW = datetime(2026, 10, 17, 20, 0, tzinfo=UTC)
REQUEST = RetentionMaintenanceRequest(
    as_of_date=date(2026, 10, 17),
    as_of_session=date(2026, 10, 16),
    archive_session_dates=(date(2026, 10, 16),),
)


class Report:
    def __init__(self, *, alarms=(), dropped=(), failed=(), sessions=()):
        self.all_alarms = tuple(alarms)
        self.alarms = tuple(alarms)
        self.dropped = tuple(dropped)
        self.failed = tuple(failed)
        self.sessions = tuple(sessions)


class Session:
    def __init__(self, outcomes=()):
        self.outcomes = tuple(outcomes)


class Outcome:
    def __init__(self, status):
        self.status = status


class Retention:
    def __init__(self):
        self.calls = []
        self.report = Report(dropped=("old",))
        self.error = None

    def run(self, **options):
        self.calls.append(options)
        if self.error:
            raise self.error
        return self.report


class Archive:
    def __init__(self):
        self.calls = []
        self.archive_report = Report(sessions=(Session((Outcome("ARCHIVED"),)),))
        self.drop_report = Report(dropped=("old-flow",))
        self.archive_error = None
        self.drop_error = None

    def archive_sessions(self, sessions):
        self.calls.append(("archive", tuple(sessions)))
        if self.archive_error:
            raise self.archive_error
        return self.archive_report

    def drop_archived_partitions(self, **options):
        self.calls.append(("drop", options))
        if self.drop_error:
            raise self.drop_error
        return self.drop_report


class PartitionMaintenance:
    def __init__(self):
        self.calls = []
        self.result = (2, [])
        self.error = None

    def __call__(self, today):
        self.calls.append(today)
        if self.error:
            raise self.error
        return self.result


@pytest.fixture
def components(tmp_path):
    partitions = PartitionMaintenance()
    retention = Retention()
    archive = Archive()
    status_path = (tmp_path / "retention" / "status.json").resolve()
    cycle = RetentionMaintenanceCycle(
        partitions,
        retention,
        archive,
        status_path,
        clock=lambda: NOW,
    )
    return cycle, partitions, retention, archive, status_path


def test_plan_is_side_effect_free_and_names_the_exact_scope(components):
    cycle, partitions, retention, archive, status_path = components
    plan = cycle.run(REQUEST)
    assert plan.as_dict() == {
        "mode": "PLAN",
        "as_of_date": "2026-10-17",
        "as_of_session": "2026-10-16",
        "archive_session_dates": ["2026-10-16"],
        "partition_relations": list(REQUEST.partition_relations),
        "retention_relations": list(REQUEST.retention_relations),
        "archive_relations": list(ARCHIVE_FIRST_RELATIONS),
        "archive_kinds": list(REQUEST.archive_kinds),
        "operations": [
            "ensure_partitions_and_check",
            "archive_sessions",
            "drop_archived_partitions",
            "partition_retention",
        ],
        "database_changes": False,
    }
    assert partitions.calls == [] and retention.calls == [] and archive.calls == []
    assert not status_path.exists()


def test_successful_execute_writes_a_restart_safe_ready_status(components):
    cycle, partitions, retention, archive, status_path = components
    status = cycle.run(REQUEST, execute=True)
    assert status.state == STATUS_READY
    assert status.last_success_at == NOW
    assert status.consecutive_failures == 0
    assert status.created_partitions == 2
    assert status.retention_dropped == 1
    assert status.archive_outcomes == 1
    assert status.archive_partitions_dropped == 1
    assert partitions.calls == [NOW.date()]
    assert retention.calls == [{
        "execute": True,
        "as_of_date": REQUEST.as_of_date,
        "relations": REQUEST.retention_relations,
    }]
    assert archive.calls == [
        ("archive", REQUEST.archive_session_dates),
        ("drop", {
            "as_of_session": REQUEST.as_of_session,
            "relations": REQUEST.archive_relations,
            "health_date": NOW.date(),
        }),
    ]
    assert read_retention_status(status_path) == status


def test_alarms_preserve_last_success_and_increment_failures_across_restarts(components):
    cycle, partitions, retention, archive, status_path = components
    assert cycle.run(REQUEST, execute=True).state == STATUS_READY
    partitions.result = (0, [PartitionAlarm("rt.raw_payloads", date(2026, 10, 20), 3)])
    retention.report = Report(
        alarms=(ArchiveAlarm("PARTITION_DROP_FAILED", "lock timeout", relation="rt.raw_payloads/old"),),
        failed=("old",),
    )
    first = cycle.run(REQUEST, execute=True)
    assert first.state == STATUS_ALARM
    assert first.last_success_at == NOW
    assert first.consecutive_failures == 1
    assert first.alarm_count == 2
    assert first.retention_failed == 1

    restarted = RetentionMaintenanceCycle(
        partitions,
        retention,
        archive,
        status_path,
        clock=lambda: NOW + timedelta(minutes=5),
    )
    second = restarted.run(REQUEST, execute=True)
    assert second.last_success_at == NOW
    assert second.consecutive_failures == 2
    assert read_retention_status(status_path) == second


def test_stage_exceptions_are_durable_alarms_and_do_not_hide_other_stage_results(components):
    cycle, partitions, retention, archive, status_path = components
    partitions.error = RuntimeError("partition function unavailable")
    archive.archive_error = OSError("archive volume unavailable")
    archive.drop_error = RuntimeError("drop refused")
    retention.error = RuntimeError("retention catalog unavailable")
    status = cycle.run(REQUEST, execute=True)
    assert status.state == STATUS_ALARM and status.consecutive_failures == 1
    assert {alarm.code for alarm in status.alarms} == {
        "PARTITION_MAINTENANCE_FAILED",
        "SESSION_ARCHIVE_FAILED",
        "ARCHIVE_RETENTION_FAILED",
        "PARTITION_RETENTION_FAILED",
    }
    assert read_retention_status(status_path) == status


def test_a_corrupt_previous_status_refuses_before_any_operation(components):
    cycle, partitions, retention, archive, status_path = components
    status_path.parent.mkdir(parents=True)
    status_path.write_text('{"schema":"other"}', encoding="utf-8")
    with pytest.raises(ValueError, match="identity mismatch"):
        cycle.run(REQUEST, execute=True)
    assert partitions.calls == [] and retention.calls == [] and archive.calls == []


def test_status_validation_rejects_inconsistent_failure_state(tmp_path):
    path = (tmp_path / "status.json").resolve()
    invalid = RetentionMaintenanceStatus(
        checked_at=NOW,
        state=STATUS_ALARM,
        last_success_at=None,
        consecutive_failures=1,
        request=REQUEST,
        created_partitions=0,
        retention_dropped=0,
        retention_failed=0,
        archive_outcomes=0,
        archive_outcomes_failed=0,
        archive_partitions_dropped=0,
        alarms=(MaintenanceAlarm("TEST", "FAILED", "failure"),),
        alarm_count=1,
    )
    write_retention_status(path, invalid)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["consecutive_failures"] = 0
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="ALARM retention status"):
        read_retention_status(path)


def test_requests_refuse_unknown_archive_relations_and_future_archive_sessions():
    with pytest.raises(ValueError, match="ordinary-retention"):
        RetentionMaintenanceRequest(
            date(2026, 10, 17),
            date(2026, 10, 16),
            (),
            retention_relations=("rt.bar_revisions",),
        )
    with pytest.raises(ValueError, match="partitioned"):
        RetentionMaintenanceRequest(
            date(2026, 10, 17),
            date(2026, 10, 16),
            (),
            partition_relations=("rt.detector_events",),
        )
    with pytest.raises(ValueError, match="ARCHIVE_FIRST"):
        RetentionMaintenanceRequest(
            date(2026, 10, 17),
            date(2026, 10, 16),
            (),
            archive_relations=("rt.raw_payloads",),
        )
    with pytest.raises(ValueError, match="later than"):
        RetentionMaintenanceRequest(
            date(2026, 10, 17),
            date(2026, 10, 16),
            (date(2026, 10, 17),),
        )


def test_execute_refuses_a_future_as_of_date_before_any_operation(components):
    cycle, partitions, retention, archive, _ = components
    future = RetentionMaintenanceRequest(
        date(2026, 10, 18),
        REQUEST.as_of_session,
        REQUEST.archive_session_dates,
    )
    with pytest.raises(ValueError, match="current UTC date"):
        cycle.run(future, execute=True)
    assert partitions.calls == [] and retention.calls == [] and archive.calls == []


def test_file_status_health_fails_closed_on_missing_stale_future_and_repeated_failures(components):
    cycle, partitions, retention, archive, status_path = components
    assert assess_retention_health(
        None,
        checked_at=NOW,
        maximum_age_seconds=60,
        failure_threshold=2,
    ).state == HEALTH_BLOCKED

    ready = cycle.run(REQUEST, execute=True)
    health = assess_retention_health(
        ready,
        checked_at=NOW + timedelta(seconds=30),
        maximum_age_seconds=60,
        failure_threshold=2,
    )
    assert health.state == HEALTH_READY and health.reason_codes == ()

    stale = assess_retention_health(
        ready,
        checked_at=NOW + timedelta(seconds=61),
        maximum_age_seconds=60,
        failure_threshold=2,
    )
    assert stale.state == HEALTH_BLOCKED
    assert stale.reason_codes == ("RETENTION_STATUS_STALE",)

    future = assess_retention_health(
        ready,
        checked_at=NOW - timedelta(seconds=1),
        maximum_age_seconds=60,
        failure_threshold=2,
    )
    assert future.state == HEALTH_BLOCKED
    assert future.reason_codes == ("RETENTION_STATUS_FROM_FUTURE",)

    retention.report = Report(alarms=(ArchiveAlarm("FAILED", "failure"),))
    first = cycle.run(REQUEST, execute=True)
    degraded = assess_retention_health(
        first,
        checked_at=NOW,
        maximum_age_seconds=60,
        failure_threshold=2,
    )
    assert degraded.state == HEALTH_DEGRADED
    second = cycle.run(REQUEST, execute=True)
    blocked = assess_retention_health(
        second,
        checked_at=NOW,
        maximum_age_seconds=60,
        failure_threshold=2,
    )
    assert blocked.state == HEALTH_BLOCKED
    assert blocked.reason_codes == ("RETENTION_CYCLE_ALARM",)


def test_partition_creation_ranges_match_daily_weekly_and_monthly_sql_contracts():
    today = date(2026, 10, 17)
    daily = _partition_ranges("rt.raw_payloads", "DAILY", 14, 3, today)
    weekly = _partition_ranges("rt.option_flow_clusters", "WEEKLY", 14, 28, today)
    monthly = _partition_ranges("rt.equity_evidence", "MONTHLY", 14, 35, today)
    assert (daily[0].lower, daily[-1].lower, len(daily)) == (
        date(2026, 10, 14),
        date(2026, 10, 31),
        18,
    )
    assert (weekly[0].lower, weekly[-1].lower, len(weekly)) == (
        date(2026, 9, 14),
        date(2026, 10, 26),
        7,
    )
    assert [(item.lower, item.upper) for item in monthly] == [
        (date(2026, 9, 1), date(2026, 10, 1)),
        (date(2026, 10, 1), date(2026, 11, 1)),
    ]


def test_partition_creation_plan_reports_only_names_absent_from_to_regclass():
    today = date(2026, 10, 17)

    class Cursor:
        def __init__(self):
            self.answer = [
                ("rt.raw_payloads", "DAILY", 1, 0),
                ("rt.option_flow_clusters", "WEEKLY", 1, 0),
            ]
            self.policies = [
                [
                    ("rt.raw_payloads", "DAILY", 1, 0),
                    ("rt.option_flow_clusters", "WEEKLY", 1, 0),
                ]
            ]
            self.names = []

        def execute(self, statement, parameters=None):
            if parameters:
                self.names = list(parameters[0])
                missing = "rt.raw_payloads_p20261018"
                self.answer = [
                    (name, name != missing)
                    for name in self.names
                ]
            else:
                self.answer = self.policies[0]

        def fetchall(self):
            return self.answer

    cursor = Cursor()
    plan = partition_creation_plan(cursor, today)
    assert [item.partition_name for item in plan.candidates] == [
        "rt.raw_payloads_p20261018"
    ]
    assert plan.as_dict()["would_create"] == 1


def test_selected_partition_maintenance_calls_only_explicit_relations():
    class Cursor:
        def __init__(self):
            self.calls = []
            self.created = iter(((2,), (3,)))

        def execute(self, statement, parameters=None):
            self.calls.append((statement, parameters))

        def fetchone(self):
            return next(self.created)

        def fetchall(self):
            return []

    cursor = Cursor()
    created, alarms = ensure_selected_partitions_and_check(
        cursor,
        date(2026, 10, 17),
        ("rt.raw_payloads", "rt.portal_snapshot_hot"),
    )
    ensure_calls = [
        parameters[0]
        for statement, parameters in cursor.calls
        if statement.startswith("SELECT rt.ensure_partitions(")
    ]
    assert created == 5 and alarms == []
    assert ensure_calls == ["rt.portal_snapshot_hot", "rt.raw_payloads"]
