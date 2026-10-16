"""Offline checks of the ordinary-retention partition executor and its script (fake repository, no database)."""
from __future__ import annotations

import json
import re
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from realtime import partition_retention as retention
from realtime.partition_retention import (
    CATALOG_RULES,
    ELIGIBLE_RELATIONS,
    DetachedTable,
    PartitionRetentionExecutor,
    RetentionPolicy,
    classify_partition,
    policy_refusal,
    retention_cutoff,
    subtract_months,
)
from realtime.relation_registry import PARTITION_POLICY_SEEDS, RELATIONS
from realtime.session_archive_worker import PartitionInfo
from scripts import run_realtime_v2_retention as script

UTC = timezone.utc
AS_OF = date(2026, 10, 13)
CLOCK = lambda: datetime(2026, 10, 13, 9, 30, tzinfo=UTC)  # noqa: E731
SQL_009 = Path(__file__).resolve().parents[2] / "realtime" / "sql_v2" / "009_realtime_partition_maintenance.sql"
POLICY_ROW = re.compile(
    r"\('(rt\.[a-z_]+)', '([A-Z]+)', '([a-z_]+)', '([a-z]+)', '([A-Z_0-9]+)', '([A-Z_]+)', (\d+), \d+, \d+, "
    r"(true|false), ARRAY\[[^\]]*\](?:::text\[\])?, (NULL|'rt\.[a-z_]+')\)")

EXPECTED_ELIGIBLE = {
    "rt.acquisition_facts", "rt.equity_evidence", "rt.equity_evidence_dependencies", "rt.equity_evidence_inputs",
    "rt.equity_evidence_source_windows", "rt.feature_snapshots", "rt.normalization_diagnostics",
    "rt.option_discovery_pages", "rt.portal_releases", "rt.portal_snapshot_hot", "rt.raw_payload_chunks",
    "rt.raw_payloads",
}
NOT_ELIGIBLE = (
    "rt.bar_revisions", "rt.portal_snapshot_kept", "rt.option_flow_events", "rt.option_flow_clusters",
)


def policy_rows() -> dict[str, RetentionPolicy]:
    """The rt.partition_policies rows exactly as 009 seeds them."""
    rows = {}
    for match in POLICY_ROW.finditer(SQL_009.read_text(encoding="utf-8")):
        relation, scheme, key, key_type, klass, kind, value, requires, catalog = match.groups()
        rows[relation] = RetentionPolicy(
            relation, scheme, key, key_type, klass, kind, int(value), requires == "true",
            None if catalog == "NULL" else catalog.strip("'"))
    return rows


def partition(relation: str, lower: date, upper: date) -> PartitionInfo:
    name = relation.split(".")[1] + "_p" + lower.strftime("%Y%m%d")
    return PartitionInfo(relation, name, lower, upper,
                         f"FOR VALUES FROM ('{lower} 00:00:00+00') TO ('{upper} 00:00:00+00')")


def daily(relation: str, first: date, last: date) -> list[PartitionInfo]:
    days = (last - first).days + 1
    return [partition(relation, first + timedelta(days=offset), first + timedelta(days=offset + 1))
            for offset in range(days)]


def monthly(relation: str, first: tuple[int, int], last: tuple[int, int]) -> list[PartitionInfo]:
    result, (year, month) = [], first
    while (year, month) <= last:
        following = (year + month // 12, month % 12 + 1)
        result.append(partition(relation, date(year, month, 1), date(following[0], following[1], 1)))
        year, month = following
    return result


def standard_partitions() -> dict[str, list[PartitionInfo]]:
    attached: dict[str, list[PartitionInfo]] = {}
    for relation, policy in policy_rows().items():
        if policy.scheme == "DAILY":
            attached[relation] = daily(relation, AS_OF - timedelta(days=20), AS_OF + timedelta(days=14))
        elif policy.scheme == "MONTHLY":
            attached[relation] = monthly(relation, (2026, 4), (2026, 12))
        else:
            attached[relation] = []
    return attached


class FakeRepository:
    def __init__(self, attached=None, rows=None, detached=None):
        self.rows = rows if rows is not None else policy_rows()
        self.attached = attached if attached is not None else standard_partitions()
        self.detached = detached or {}
        self.calls: list[tuple] = []
        self.policy_error: Exception | None = None
        self.partition_errors: dict[str, Exception] = {}
        self.detach_errors: dict[str, Exception] = {}
        self.vanish_on_error: set[str] = set()
        self.drop_detached_errors: dict[str, Exception] = {}
        self.prune_errors: dict[str, Exception] = {}

    def policies(self):
        self.calls.append(("policies",))
        if self.policy_error:
            raise self.policy_error
        return list(self.rows.values())

    def partitions(self, relation):
        self.calls.append(("partitions", relation))
        if relation in self.partition_errors:
            raise self.partition_errors[relation]
        return list(self.attached.get(relation, ()))

    def detached_tables(self, policy):
        self.calls.append(("detached_tables", policy.relation))
        return list(self.detached.get(policy.relation, ()))

    def detach_and_drop(self, item):
        self.calls.append(("detach", item.name))
        error = self.detach_errors.get(item.name)
        if error is not None:
            if item.name in self.vanish_on_error:
                self.attached[item.relation] = [p for p in self.attached[item.relation] if p.name != item.name]
            raise error
        self.attached[item.relation] = [p for p in self.attached[item.relation] if p.name != item.name]

    def drop_detached(self, table, policy):
        self.calls.append(("drop_detached", table.name))
        if table.name in self.drop_detached_errors:
            raise self.drop_detached_errors[table.name]
        self.detached[table.relation] = [t for t in self.detached[table.relation] if t.name != table.name]

    def prune_catalog(self, catalog, rule, before):
        self.calls.append(("prune", catalog, rule, before))
        if catalog in self.prune_errors:
            raise self.prune_errors[catalog]
        return 7

    def mutations(self):
        return [call for call in self.calls if call[0] in {"detach", "drop_detached", "prune"}]

    def dropped_names(self):
        return [call[1] for call in self.calls if call[0] == "detach"]


class FakePgError(Exception):
    def __init__(self, message, pgcode):
        super().__init__(message)
        self.pgcode = pgcode


def run(repository, **options):
    return PartitionRetentionExecutor(repository, clock=CLOCK).run(**options)


def codes(report):
    return sorted(alarm.code for alarm in report.all_alarms)


# ---- window arithmetic and the boundary -------------------------------------------------------------------


def test_the_eligible_relations_are_exactly_the_partitioned_hot_ttl_and_kept_3m_relations():
    assert set(ELIGIBLE_RELATIONS) == EXPECTED_ELIGIBLE
    for relation in ELIGIBLE_RELATIONS:
        spec = RELATIONS[relation]
        assert spec.partitioned and spec.retention in {"HOT_TTL", "KEPT_3M"}
        scheme, klass, kind, value = PARTITION_POLICY_SEEDS[relation]
        assert (scheme, kind) == (("DAILY", "CALENDAR_DAYS") if klass == "HOT_TTL" else ("MONTHLY", "MONTHS"))
    for relation in NOT_ELIGIBLE:
        assert relation not in ELIGIBLE_RELATIONS


def test_the_009_policy_rows_are_all_accepted_or_not_ordinary_retention():
    rows = policy_rows()
    assert set(PARTITION_POLICY_SEEDS) == set(rows)
    for relation, policy in rows.items():
        refusal = policy_refusal(policy)
        if relation in EXPECTED_ELIGIBLE:
            assert refusal is None, relation
            assert policy.catalog_relation is None or policy.catalog_relation in CATALOG_RULES
        else:
            assert refusal is not None, relation


def test_every_catalog_rule_names_a_prunable_registry_catalog():
    from realtime.relation_registry import grants

    for catalog in CATALOG_RULES:
        spec = RELATIONS[catalog]
        # Catalogs are append-only for app; admin (the owner) prunes them with DELETE.
        assert not spec.partitioned and spec.writes == "APPEND" and not spec.delete, catalog
        assert "DELETE" not in grants()[catalog], catalog


def test_the_snapshot_catalog_rule_prunes_exactly_the_class_that_has_hot_bodies():
    from realtime.portal_projection_repository import SNAPSHOT_CLASS_EPHEMERAL

    assert CATALOG_RULES["rt.snapshot_catalog"].equals == (("snapshot_class", SNAPSHOT_CLASS_EPHEMERAL),)


@pytest.mark.parametrize("today, window, expected", [
    (date(2026, 10, 13), 14, date(2026, 9, 29)),
    (date(2026, 10, 13), 3, date(2026, 10, 10)),
    (date(2026, 3, 5), 14, date(2026, 2, 19)),
])
def test_calendar_day_windows_subtract_whole_days(today, window, expected):
    assert retention_cutoff("CALENDAR_DAYS", window, today) == expected


@pytest.mark.parametrize("today, expected", [
    (date(2026, 10, 13), date(2026, 7, 13)),
    (date(2026, 5, 31), date(2026, 2, 28)),
    (date(2024, 5, 31), date(2024, 2, 29)),
    (date(2026, 2, 15), date(2025, 11, 15)),
    (date(2026, 1, 1), date(2025, 10, 1)),
    (date(2026, 3, 31), date(2025, 12, 31)),
])
def test_three_month_windows_are_calendar_months_clamped_to_the_month_length(today, expected):
    assert retention_cutoff("MONTHS", 3, today) == expected
    assert subtract_months(today, 0) == today


@pytest.mark.parametrize("kind, value", [("SESSIONS", 26), ("CALENDAR_DAYS", 0), ("MONTHS", -1), ("WEEKS", 2)])
def test_unsupported_windows_are_rejected(kind, value):
    with pytest.raises(ValueError):
        retention_cutoff(kind, value, AS_OF)


@pytest.mark.parametrize("lower, upper, droppable, reason", [
    (date(2026, 9, 28), date(2026, 9, 29), True, "EXPIRED"),                 # ends exactly at the cutoff
    (date(2026, 9, 29), date(2026, 9, 30), False, "WITHIN_WINDOW"),          # starts exactly at the cutoff
    (date(2026, 9, 20), date(2026, 9, 30), False, "STRADDLES_CUTOFF"),
    (date(2026, 9, 1), date(2026, 9, 29), True, "EXPIRED"),
    (date(2026, 10, 12), date(2026, 10, 13), False, "WITHIN_WINDOW"),        # ends today: yesterday's partition
    (date(2026, 10, 13), date(2026, 10, 14), False, "CURRENT_OR_FUTURE"),    # contains today
    (date(2026, 10, 14), date(2026, 10, 15), False, "CURRENT_OR_FUTURE"),
    (date(2026, 9, 29), date(2026, 9, 29), False, "INVALID_RANGE"),
])
def test_a_daily_partition_is_dropped_only_when_its_whole_range_ends_at_or_before_the_cutoff(
    lower, upper, droppable, reason,
):
    assert classify_partition(lower, upper, date(2026, 9, 29), AS_OF) == (droppable, reason)


@pytest.mark.parametrize("as_of, lower, upper, droppable, reason", [
    (date(2026, 10, 13), date(2026, 6, 1), date(2026, 7, 1), True, "EXPIRED"),
    (date(2026, 10, 13), date(2026, 7, 1), date(2026, 8, 1), False, "STRADDLES_CUTOFF"),
    (date(2026, 10, 13), date(2026, 9, 1), date(2026, 10, 1), False, "WITHIN_WINDOW"),
    (date(2026, 10, 13), date(2026, 10, 1), date(2026, 11, 1), False, "CURRENT_OR_FUTURE"),
    (date(2026, 10, 13), date(2026, 11, 1), date(2026, 12, 1), False, "CURRENT_OR_FUTURE"),
    (date(2026, 10, 1), date(2026, 6, 1), date(2026, 7, 1), True, "EXPIRED"),       # cutoff on the 1st
    (date(2026, 10, 1), date(2026, 7, 1), date(2026, 8, 1), False, "WITHIN_WINDOW"),
    (date(2026, 10, 31), date(2026, 7, 1), date(2026, 8, 1), False, "STRADDLES_CUTOFF"),  # up to 4 months online
    (date(2026, 11, 1), date(2026, 7, 1), date(2026, 8, 1), True, "EXPIRED"),       # dropped on the 1st
    (date(2026, 11, 2), date(2026, 7, 1), date(2026, 8, 1), True, "EXPIRED"),
])
def test_a_monthly_partition_is_whole_expired_only_after_three_months_and_straddlers_stay(
    as_of, lower, upper, droppable, reason,
):
    cutoff = retention_cutoff("MONTHS", 3, as_of)
    assert classify_partition(lower, upper, cutoff, as_of) == (droppable, reason)


def test_the_cutoff_never_reaches_the_current_or_a_future_partition():
    for window in (1, 3, 14):
        for offset in range(0, 800, 7):
            as_of = date(2025, 1, 1) + timedelta(days=offset)
            for kind in ("CALENDAR_DAYS", "MONTHS"):
                cutoff = retention_cutoff(kind, window, as_of)
                assert cutoff < as_of
                for lower in (as_of, as_of + timedelta(days=1), as_of - timedelta(days=1)):
                    droppable, _ = classify_partition(lower, lower + timedelta(days=1), cutoff, as_of)
                    assert droppable == (lower + timedelta(days=1) <= cutoff)
                    if lower >= as_of:
                        assert not droppable


# ---- the executor over a fake repository -----------------------------------------------------------------


def test_the_default_run_is_a_dry_run_that_lists_without_any_mutation():
    repository = FakeRepository()
    report = run(repository)
    assert not report.executed and report.ok
    assert repository.mutations() == []
    assert len(report.policies) == len(EXPECTED_ELIGIBLE)
    assert len(report.would_drop) == 3 * 6 + 17 + 8 * 3
    assert not report.dropped and not report.failed
    assert {item.status for item in report.catalogs} == {"PLANNED"}
    assert all(item.rows_deleted is None for item in report.catalogs)


def test_execute_drops_exactly_the_expired_whole_partitions_of_every_eligible_policy():
    repository = FakeRepository()
    report = run(repository, execute=True)
    assert report.executed and report.ok and not report.failed
    by_relation = {result.relation: result for result in report.policies}
    assert set(by_relation) == EXPECTED_ELIGIBLE
    cutoff14, cutoff3, cutoff_month = date(2026, 9, 29), date(2026, 10, 10), date(2026, 7, 13)
    for relation, result in by_relation.items():
        scheme = policy_rows()[relation].scheme
        cutoff = {"MONTHLY": cutoff_month}.get(scheme, cutoff3 if relation == "rt.portal_snapshot_hot" else cutoff14)
        assert result.cutoff == cutoff
        assert result.dropped and all(item.upper <= cutoff for item in result.dropped)
        assert all(item.upper > cutoff for item in result.kept)
        assert {item.partition for item in result.dropped}.isdisjoint(
            {p.name for p in repository.attached[relation]})
    assert len(by_relation["rt.raw_payloads"].dropped) == 6
    assert len(by_relation["rt.portal_snapshot_hot"].dropped) == 17
    assert [item.lower for item in by_relation["rt.equity_evidence"].dropped] == [
        date(2026, 4, 1), date(2026, 5, 1), date(2026, 6, 1)]
    straddler = [item for item in by_relation["rt.equity_evidence"].kept if item.lower == date(2026, 7, 1)]
    assert straddler[0].reason == "STRADDLES_CUTOFF"
    kept_reasons = {item.reason for result in report.policies for item in result.kept}
    assert kept_reasons == {"WITHIN_WINDOW", "STRADDLES_CUTOFF", "CURRENT_OR_FUTURE"}


def test_current_and_future_partitions_are_never_dropped_even_when_every_other_one_is():
    original = standard_partitions()
    original["rt.raw_payloads"].append(partition("rt.raw_payloads", date(2030, 1, 1), date(2030, 1, 2)))
    repository = FakeRepository(attached={relation: list(items) for relation, items in original.items()})
    report = run(repository, execute=True)
    assert report.ok
    for relation, items in original.items():
        remaining = {item.name for item in repository.attached[relation]}
        for item in items:
            if item.upper > AS_OF:
                assert item.name in remaining, item.name
    assert not any(name.endswith(("_p20261013", "_p20261101", "_p20300101")) for name in repository.dropped_names())


def test_other_retention_classes_are_never_touched_and_are_reported_as_skipped():
    attached = standard_partitions()
    attached["rt.bar_revisions"] = monthly("rt.bar_revisions", (2024, 1), (2026, 12))
    attached["rt.portal_snapshot_kept"] = daily("rt.portal_snapshot_kept", date(2026, 1, 1), date(2026, 2, 1))
    attached["rt.option_flow_events"] = daily("rt.option_flow_events", date(2026, 1, 1), date(2026, 2, 1))
    repository = FakeRepository(attached=attached)
    report = run(repository, execute=True)
    touched = {call[1] for call in repository.calls if call[0] in {"partitions", "detached_tables"}}
    assert touched == EXPECTED_ELIGIBLE
    assert not any(name.startswith(("bar_revisions", "portal_snapshot_kept", "option_flow")) for name in
                   repository.dropped_names())
    assert {name for name, _ in report.skipped} == set(NOT_ELIGIBLE)
    reasons = dict(report.skipped)
    assert reasons["rt.bar_revisions"].startswith("HISTORY_PLAN")
    assert reasons["rt.portal_snapshot_kept"].startswith("SESSION_WINDOW")
    assert reasons["rt.option_flow_events"].startswith("ARCHIVE_FIRST")
    assert report.ok


@pytest.mark.parametrize("relation", [
    *NOT_ELIGIBLE, "rt.detector_events", "rt.option_discovery_batches", "rt.snapshot_catalog", "rt.nope",
    "rt.partition_policies", "",
])
def test_an_explicit_relation_that_is_not_ordinary_retention_is_refused_before_any_statement(relation):
    repository = FakeRepository()
    with pytest.raises(ValueError):
        run(repository, execute=True, relations=[relation])
    assert repository.calls == []


def test_a_relation_filter_limits_the_run():
    repository = FakeRepository()
    report = run(repository, execute=True, relations=["rt.option_discovery_pages", "rt.option_discovery_pages"])
    assert [result.relation for result in report.policies] == ["rt.option_discovery_pages"]
    assert {call[1] for call in repository.calls if call[0] == "partitions"} == {"rt.option_discovery_pages"}
    assert report.catalogs == ()


def test_the_window_can_be_evaluated_earlier_but_never_later_than_today():
    repository = FakeRepository()
    earlier = run(repository, as_of_date=date(2026, 10, 1))
    assert earlier.as_of_date == date(2026, 10, 1)
    assert len(earlier.would_drop) < len(run(FakeRepository()).would_drop)
    with pytest.raises(ValueError):
        run(FakeRepository(), execute=True, as_of_date=date(2026, 10, 14))


def test_today_is_the_utc_date_of_the_clock():
    late_evening_utc_minus_five = datetime(2026, 10, 13, 22, 0, tzinfo=timezone(timedelta(hours=-5)))
    report = PartitionRetentionExecutor(FakeRepository(), clock=lambda: late_evening_utc_minus_five).run()
    assert report.as_of_date == date(2026, 10, 14)


@pytest.mark.parametrize("change", [
    dict(window_value=1), dict(scheme="WEEKLY"), dict(retention_class="ARCHIVE_FIRST"),
    dict(window_kind="SESSIONS"), dict(requires_archive=True), dict(catalog_relation="rt.bar_catalog"),
])
def test_a_policy_row_that_differs_from_the_registry_is_refused_and_nothing_is_dropped(change):
    rows = policy_rows()
    relation = "rt.equity_evidence"
    rows[relation] = replace(rows[relation], **change)
    repository = FakeRepository(rows=rows)
    report = run(repository, execute=True)
    assert "PARTITION_POLICY_UNSUPPORTED" in codes(report)
    assert relation not in {result.relation for result in report.policies}
    assert relation not in {call[1] for call in repository.calls if call[0] == "partitions"}
    assert not any(name.startswith("equity_evidence_p") for name in repository.dropped_names())
    assert {result.relation for result in report.policies} == EXPECTED_ELIGIBLE - {relation}


def test_a_missing_policy_row_and_an_unregistered_ordinary_policy_are_refused():
    rows = policy_rows()
    del rows["rt.raw_payloads"]
    rows["rt.surprise"] = RetentionPolicy("rt.surprise", "DAILY", "created_at", "timestamptz", "HOT_TTL",
                                          "CALENDAR_DAYS", 1)
    repository = FakeRepository(rows=rows)
    report = run(repository, execute=True)
    assert codes(report) == ["PARTITION_POLICY_UNSUPPORTED"] * 2
    assert {alarm.relation for alarm in report.alarms} == {"rt.raw_payloads", "rt.surprise"}
    assert not any(call[1] == "rt.surprise" for call in repository.calls if len(call) > 1)


def test_a_failure_on_one_partition_alarms_and_the_run_continues_with_the_next_partition_and_policy():
    repository = FakeRepository()
    boom = "raw_payloads_p20260924"
    repository.detach_errors[boom] = RuntimeError("raw_payloads_p20260924 was detached but not dropped: Lock")
    report = run(repository, execute=True)
    assert not report.ok and codes(report) == ["PARTITION_DROP_FAILED"]
    result = {item.relation: item for item in report.policies}["rt.raw_payloads"]
    assert [item.partition for item in result.failed] == [boom]
    assert len(result.dropped) == 5
    assert report.all_alarms[0].relation == f"rt.raw_payloads/{boom}"
    later = {item.relation: item for item in report.policies}["rt.raw_payload_chunks"]
    assert len(later.dropped) == 6
    assert len({item.relation for item in report.policies if item.dropped}) == len(EXPECTED_ELIGIBLE)


def test_a_partition_that_other_tables_still_reference_fails_closed_with_its_own_alarm():
    repository = FakeRepository()
    referenced = "portal_snapshot_hot_p20260923"
    repository.detach_errors[referenced] = FakePgError(
        'removing partition "portal_snapshot_hot_p20260923" violates foreign key constraint', "23503")
    report = run(repository, execute=True)
    assert codes(report) == ["RETENTION_PARTITION_REFERENCED"]
    result = {item.relation: item for item in report.policies}["rt.portal_snapshot_hot"]
    assert [item.partition for item in result.failed] == [referenced]
    assert len(result.dropped) == 16
    assert referenced in {p.name for p in repository.attached["rt.portal_snapshot_hot"]}


def test_a_partition_that_vanished_between_listing_and_ddl_is_already_dropped_not_an_alarm():
    repository = FakeRepository()
    name = "raw_payloads_p20260925"
    repository.detach_errors[name] = RuntimeError(f"{name} is not a partition of rt.raw_payloads")
    repository.vanish_on_error.add(name)
    report = run(repository, execute=True)
    assert report.ok
    result = {item.relation: item for item in report.policies}["rt.raw_payloads"]
    assert {(item.partition, item.reason) for item in result.dropped} >= {(name, "ALREADY_DROPPED")}
    assert len(result.dropped) == 6


def test_a_partition_lookup_failure_alarms_that_policy_only_and_skips_its_catalog():
    repository = FakeRepository()
    repository.partition_errors["rt.equity_evidence"] = RuntimeError("connection reset")
    report = run(repository, execute=True)
    assert codes(report) == ["PARTITION_LOOKUP_FAILED"]
    failed = {item.relation: item for item in report.policies}["rt.equity_evidence"]
    assert not failed.scanned and not failed.partitions
    assert "rt.evidence_catalog" not in {item.catalog for item in report.catalogs}
    assert {item.catalog for item in report.catalogs} == {"rt.evidence_source_window_catalog", "rt.snapshot_catalog"}
    assert len(report.dropped) == 59 - 3


def test_a_policy_lookup_failure_is_an_alarm_not_a_crash():
    repository = FakeRepository()
    repository.policy_error = RuntimeError("no connection")
    report = run(repository, execute=True)
    assert codes(report) == ["RETENTION_POLICY_LOOKUP_FAILED"] and not report.policies
    assert repository.mutations() == []


def test_a_rerun_is_idempotent_and_finishes_what_a_partial_failure_left():
    repository = FakeRepository()
    name = "raw_payloads_p20260924"
    repository.detach_errors[name] = RuntimeError("lock timeout")
    first = run(repository, execute=True)
    assert len(first.failed) == 1
    del repository.detach_errors[name]
    repository.calls.clear()
    second = run(repository, execute=True)
    assert second.ok and repository.dropped_names() == [name]
    repository.calls.clear()
    third = run(repository, execute=True)
    assert third.ok and repository.dropped_names() == [] and not third.dropped


def test_a_table_left_detached_by_an_interrupted_run_is_finished_when_expired():
    relation = "rt.raw_payloads"
    expired = DetachedTable(relation, "raw_payloads_p20260901", date(2026, 9, 1), date(2026, 9, 2))
    current = DetachedTable(relation, "raw_payloads_p20261012", date(2026, 10, 12), date(2026, 10, 13))
    attached = standard_partitions()
    attached[relation] = []
    repository = FakeRepository(attached=attached, detached={relation: [expired, current]})
    dry = run(repository)
    reasons = {item.partition: (item.status, item.reason)
               for item in {r.relation: r for r in dry.policies}[relation].partitions}
    assert reasons == {"raw_payloads_p20260901": ("WOULD_DROP", "DETACHED_EXPIRED"),
                       "raw_payloads_p20261012": ("KEPT", "DETACHED_NOT_EXPIRED")}
    assert repository.mutations() == []
    report = run(repository, execute=True)
    assert report.ok and ("drop_detached", "raw_payloads_p20260901") in repository.calls
    assert ("drop_detached", "raw_payloads_p20261012") not in repository.calls


def test_a_detached_table_that_cannot_be_dropped_alarms_and_the_run_continues():
    relation = "rt.raw_payloads"
    table = DetachedTable(relation, "raw_payloads_p20260901", date(2026, 9, 1), date(2026, 9, 2))
    repository = FakeRepository(detached={relation: [table]})
    repository.drop_detached_errors[table.name] = RuntimeError("holds rows at or after 2026-09-02; kept")
    report = run(repository, execute=True)
    assert codes(report) == ["RETENTION_DETACHED_DROP_FAILED"]
    assert report.all_alarms[0].relation == f"{relation}/{table.name}"
    assert len(report.dropped) == 59


def test_catalogs_are_pruned_once_after_every_partition_drop_with_the_cutoff_and_their_rule():
    repository = FakeRepository()
    report = run(repository, execute=True)
    prunes = [call for call in repository.calls if call[0] == "prune"]
    first_prune = repository.calls.index(prunes[0])
    assert all(call[0] != "detach" for call in repository.calls[first_prune:])
    assert {call[1]: call[3] for call in prunes} == {
        "rt.evidence_catalog": datetime(2026, 7, 13, tzinfo=UTC),
        "rt.evidence_source_window_catalog": datetime(2026, 7, 13, tzinfo=UTC),
        "rt.snapshot_catalog": datetime(2026, 10, 10, tzinfo=UTC),
    }
    assert {call[1]: call[2] for call in prunes}["rt.snapshot_catalog"].equals == (
        ("snapshot_class", "EPHEMERAL_PORTAL"),)
    assert [(item.catalog, item.status, item.rows_deleted) for item in report.catalogs] == [
        ("rt.evidence_catalog", "PRUNED", 7), ("rt.evidence_source_window_catalog", "PRUNED", 7),
        ("rt.snapshot_catalog", "PRUNED", 7)]
    assert dict((item.catalog, item.relations) for item in report.catalogs)["rt.snapshot_catalog"] == (
        "rt.portal_snapshot_hot",)


def test_a_catalog_failure_alarms_and_does_not_block_the_other_catalogs_or_the_drops():
    repository = FakeRepository()
    repository.prune_errors["rt.evidence_catalog"] = RuntimeError("deadlock detected")
    report = run(repository, execute=True)
    assert codes(report) == ["RETENTION_CATALOG_PRUNE_FAILED"]
    statuses = {item.catalog: item.status for item in report.catalogs}
    assert statuses == {"rt.evidence_catalog": "FAILED", "rt.evidence_source_window_catalog": "PRUNED",
                        "rt.snapshot_catalog": "PRUNED"}
    assert len(report.dropped) == 59


def test_a_catalog_is_still_swept_when_its_partitions_are_already_gone():
    repository = FakeRepository(attached={relation: [] for relation in policy_rows()})
    report = run(repository, execute=True)
    assert report.ok and not report.dropped
    assert {item.catalog for item in report.catalogs} == set(CATALOG_RULES)
    assert all(item.status == "PRUNED" for item in report.catalogs)


def test_the_report_is_json_and_names_dropped_failed_and_kept_partitions_per_policy():
    repository = FakeRepository()
    repository.detach_errors["raw_payloads_p20260924"] = RuntimeError("lock timeout")
    report = run(repository, execute=True)
    text = json.dumps(report.as_dict())
    payload = json.loads(text)
    assert payload["mode"] == "EXECUTE" and not payload["ok"] and payload["failed"] == 1
    entry = {item["relation"]: item for item in payload["policies"]}["rt.raw_payloads"]
    assert entry["window"] == "14 CALENDAR_DAYS" and entry["cutoff"] == "2026-09-29"
    assert entry["failed"] == [{"partition": "raw_payloads_p20260924", "reason": "PARTITION_DROP_FAILED"}]
    assert len(entry["dropped"]) == 5 and entry["kept"] == 29 and "kept_partitions" not in entry
    assert payload["alarms"][0]["code"] == "PARTITION_DROP_FAILED"
    detailed = report.as_dict(include_kept=True)
    assert len({item["relation"]: item for item in detailed["policies"]}["rt.raw_payloads"]["kept_partitions"]) == 29


def test_the_module_never_deletes_partition_rows_or_cascades():
    source = Path(retention.__file__).read_text(encoding="utf-8")
    assert not re.search(r"DROP\s+TABLE[^\n]*CASCADE", source) and "TRUNCATE" not in source
    assert source.count("DELETE FROM") == 1 and "DELETE FROM {} AS c" in source
    assert "ALTER TABLE" not in source  # the detach DDL is the archive repository's, reused


@pytest.mark.parametrize("text, valid", [
    ("FOR VALUES FROM ('2026-10-13 00:00:00+00') TO ('2026-10-14 00:00:00+00')", True),
    ("FOR VALUES FROM ('2026-10-12 20:00:00-04') TO ('2026-10-13 20:00:00-04')", True),
    ("FOR VALUES FROM ('2026-10-13') TO ('2026-10-20')", True),
    ("FOR VALUES FROM ('2026-10-13 04:00:00+00') TO ('2026-10-14 04:00:00+00')", False),
    ("FOR VALUES FROM ('2026-10-13 00:00:00+00') TO ('2026-10-14 12:30:00+00')", False),
    ("DEFAULT", False),
])
def test_a_partition_bound_off_utc_midnight_is_refused_because_dates_would_misread_it(text, valid):
    item = PartitionInfo("rt.raw_payloads", "raw_payloads_p20261013", date(2026, 10, 13), date(2026, 10, 14), text)
    if valid:
        retention._require_whole_utc_days(item)
    else:
        with pytest.raises(ValueError):
            retention._require_whole_utc_days(item)


@pytest.mark.parametrize("scheme, lower, upper", [
    ("DAILY", date(2026, 12, 31), date(2027, 1, 1)),
    ("MONTHLY", date(2026, 11, 1), date(2026, 12, 1)),
    ("MONTHLY", date(2026, 12, 1), date(2027, 1, 1)),
    ("MONTHLY", date(2026, 12, 2), None),
    ("WEEKLY", date(2026, 12, 7), None),
])
def test_the_range_of_a_detached_table_is_derived_from_its_name_and_scheme(scheme, lower, upper):
    assert retention._next_start(scheme, lower) == upper


# ---- the script ------------------------------------------------------------------------------------------


def args(*extra, database="test_realtime_v2_ret_script", confirm=None):
    return ["--database", database, "--confirm-database-name", confirm or database, *extra]


@pytest.mark.parametrize("database", [
    "stocks_realtime_v2", "stocks_realtime_v1", "postgres", "test_realtime_v1_x", "test_realtime_x",
    "test_realtime_v2", "", "prod_test_realtime_v2_x",
])
def test_the_script_refuses_any_database_that_is_not_a_test_realtime_v2_target(database):
    with pytest.raises(SystemExit) as stop:
        script.parse_args(args(database=database))
    assert stop.value.code == 2


def test_the_script_requires_the_database_and_an_exact_confirmation():
    for argv in ([], ["--database", "test_realtime_v2_x"], ["--confirm-database-name", "test_realtime_v2_x"],
                 args(confirm="test_realtime_v2_other"), args("--execute", confirm="test_realtime_v2_ret_x ")):
        with pytest.raises(SystemExit):
            script.parse_args(argv)


def test_the_script_defaults_to_a_dry_run_and_accepts_the_documented_options():
    parsed = script.parse_args(args())
    assert not parsed.execute and parsed.relation is None and parsed.as_of_date is None
    parsed = script.parse_args(args("--execute", "--relation", "rt.raw_payloads", "--relation",
                                    "rt.equity_evidence", "--as-of-date", "2026-10-01", "--include-kept"))
    assert parsed.execute and parsed.relation == ["rt.raw_payloads", "rt.equity_evidence"]
    assert parsed.as_of_date == date(2026, 10, 1) and parsed.include_kept


@pytest.mark.parametrize("relation", ["rt.bar_revisions", "rt.portal_snapshot_kept", "rt.option_flow_events", "rt.nope"])
def test_the_script_only_offers_ordinary_retention_relations(relation):
    with pytest.raises(SystemExit):
        script.parse_args(args("--relation", relation))


def test_the_script_dry_run_prints_the_plan_and_issues_nothing(capsys):
    repository = FakeRepository()
    assert script.main(args(), repository=repository) == 0
    output = capsys.readouterr().out
    assert "No database changes made" in output
    assert json.loads(output[:output.rindex("}") + 1])["mode"] == "DRY_RUN"
    assert repository.mutations() == []


def test_the_script_executes_only_with_the_flag_and_exits_nonzero_on_an_alarm(capsys):
    repository = FakeRepository()
    assert script.main(args("--execute"), repository=repository) == 0
    assert repository.dropped_names()
    capsys.readouterr()
    failing = FakeRepository()
    failing.detach_errors["raw_payloads_p20260924"] = RuntimeError("lock timeout")
    assert script.main(args("--execute", "--relation", "rt.raw_payloads"), repository=failing) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["failed"] == 1 and payload["alarms"][0]["code"] == "PARTITION_DROP_FAILED"


def test_the_script_refuses_a_future_as_of_date_without_issuing_anything(capsys):
    repository = FakeRepository()
    tomorrow = (datetime.now(UTC) + timedelta(days=2)).date().isoformat()
    assert script.main(args("--execute", "--as-of-date", tomorrow), repository=repository) == 2
    assert "refused" in capsys.readouterr().err
    assert repository.mutations() == []
