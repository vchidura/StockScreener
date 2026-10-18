import hashlib
import json
import logging
from collections import Counter
from datetime import date, datetime, timedelta, timezone

import pytest

from realtime.session_archive import (
    ALERT_KINDS,
    DETECTOR_EVALUATIONS,
    FLOW_KINDS,
    KEEP,
    OPTION_ALERTS,
    OPTION_FLOW_CLUSTERS,
    OPTION_FLOW_EVENTS,
    OUTCOMES,
    PIN_ARCHIVE_REFRESH_FAILED,
    PIN_COVERAGE_UNKNOWN,
    PIN_LOOKUP_FAILED,
    PIN_OPEN_HORIZON,
    PIN_OUTCOME_UNVERIFIED,
    PIN_ROWS_OUTSIDE_SESSIONS,
    REASON_ARCHIVE_MISSING,
    REASON_OPEN_PIN,
    SESSION_KINDS,
    STOCK_ALERTS,
    ArchiveIdentity,
    ArchiveRecord,
    ArchiveRow,
)
from realtime.session_archive_repository import _BOUND_PATTERN, _archive_id, _bound_date, _key_part
from realtime.session_archive_worker import (
    ALARM_BUILD_FAILED,
    ALARM_LEDGER_FAILED,
    ALARM_PARTITION_DROP_FAILED,
    ALARM_PARTITION_HEALTH,
    ALARM_PARTITION_HEALTH_UNAVAILABLE,
    ALARM_PARTITION_LOOKUP_FAILED,
    ALARM_PARTITION_POLICY_UNSUPPORTED,
    ALARM_PARTITION_ROWS_OUTSIDE_SESSIONS,
    ALARM_PIN_LOOKUP_FAILED,
    ALARM_SOURCE_READ_FAILED,
    ALARM_VERIFICATION_FAILED,
    ALARM_WRITE_FAILED,
    STATUS_ALREADY_ARCHIVED,
    STATUS_ARCHIVED,
    STATUS_FAILED,
    AlertHorizon,
    PartitionHealth,
    PartitionInfo,
    PartitionPolicy,
    SessionArchiveWorker,
)

UTC = timezone.utc
IDENTITY = ArchiveIdentity("build-1", "policy-1")
VERIFIED_AT = datetime(2026, 10, 5, 22, 0, tzinfo=UTC)
MONDAY = date(2026, 9, 14)
SESSIONS = [MONDAY + timedelta(days=offset) for offset in range(5)]
RELATION = "rt.option_flow_clusters"
PARTITION = PartitionInfo(RELATION, "option_flow_clusters_p20260914", MONDAY, date(2026, 9, 21),
                          "FOR VALUES FROM ('2026-09-14') TO ('2026-09-21')")
AS_OF_PAST_WINDOW = date(2026, 10, 26)  # 26 weekdays back starts on 2026-09-21: the partition end
# Policies as rt.partition_policies seeds them for option flow, plus a stand-in for a future
# alert relation (the baseline's alert tables are not partitioned).
CLUSTER_POLICY = PartitionPolicy("ARCHIVE_FIRST", "SESSIONS", 26, True, (OPTION_FLOW_CLUSTERS,))
EVENT_POLICY = PartitionPolicy("ARCHIVE_FIRST", "SESSIONS", 26, True, (OPTION_FLOW_EVENTS,))
ALERT_POLICY = PartitionPolicy("ARCHIVE_FIRST", "SESSIONS", 26, True, ALERT_KINDS)
_RELATIONS = {
    DETECTOR_EVALUATIONS: "rt.detector_evaluations",
    OPTION_FLOW_EVENTS: "rt.option_flow_events",
    OPTION_FLOW_CLUSTERS: "rt.option_flow_clusters",
}


class FakeRepository:
    """In-memory stand-in for all three ports; weekdays are the exchange sessions."""

    def __init__(self):
        self.rows = {}
        self.count_delta = {}
        self.outcomes = {}
        self.horizons = {}
        self.records = []
        self.failures = {}
        self.partition_list = [PARTITION]
        self.policy = CLUSTER_POLICY
        self.health = []
        self.health_dates = []
        self.coverage = None
        self.dropped = []
        self.root = None

    def _maybe_fail(self, method, session=None, kind=None):
        error = self.failures.get((method, session, kind)) or self.failures.get((method, session)) \
            or self.failures.get((method,))
        if error is not None:
            raise error

    def seed(self, session, kind, *documents):
        relation = _RELATIONS.get(kind, "rt.alert_current")
        self.rows[(session, kind)] = [
            ArchiveRow(relation, (f"{kind}-{session}-{index}",), document)
            for index, document in enumerate(documents)
        ]

    def read_rows(self, session_date, kind):
        self._maybe_fail("read_rows", session_date, kind)
        return list(self.rows.get((session_date, kind), []))

    def count_rows(self, session_date, kind):
        self._maybe_fail("count_rows", session_date, kind)
        counts = Counter(row.relation for row in self.rows.get((session_date, kind), []))
        for relation, delta in self.count_delta.get((session_date, kind), {}).items():
            counts[relation] += delta
        return dict(counts)

    def read_outcomes(self, session_date):
        self._maybe_fail("read_outcomes", session_date)
        return {horizon: list(rows) for horizon, rows in self.outcomes.get(session_date, {}).items()}

    def count_outcomes(self, session_date, horizon_close_date):
        self._maybe_fail("count_outcomes", session_date, horizon_close_date)
        return {"rt.alert_management_assessments": len(self.outcomes[session_date][horizon_close_date])}

    def alert_horizons(self, session_date):
        self._maybe_fail("alert_horizons", session_date)
        return list(self.horizons.get(session_date, []))

    def list_archives(self, session_date):
        self._maybe_fail("list_archives", session_date)
        return [record for record in self.records if record.session_date == session_date]

    def record_archive(self, record):
        self._maybe_fail("record_archive", record.session_date, record.kind)
        assert record.verified_at is not None
        path = self.root / record.session_date.isoformat()
        assert any(hashlib.sha256(file.read_bytes()).hexdigest() == record.sha256
                   for file in path.glob("*.jsonl.gz")), "recorded before the file was on disk"
        if any(
            (
                r.session_date,
                r.kind,
                r.horizon_close_date,
                r.sha256,
                r.build_id,
            )
            == (
                record.session_date,
                record.kind,
                record.horizon_close_date,
                record.sha256,
                record.build_id,
            )
            for r in self.records
        ):
            return False
        self.records.append(record)
        return True

    def partition_policy(self, relation):
        self._maybe_fail("partition_policy")
        if self.policy is None:
            raise LookupError(f"no partition policy for {relation}")
        return self.policy

    def partitions(self, relation):
        self._maybe_fail("partitions")
        return list(self.partition_list)

    def partition_health(self, as_of_date):
        self._maybe_fail("partition_health")
        self.health_dates.append(as_of_date)
        return list(self.health)

    def partition_row_coverage(self, partition, sessions):
        self._maybe_fail("partition_row_coverage")
        return self.coverage if self.coverage is not None else (0, 0)

    def sessions_between(self, start, end):
        self._maybe_fail("sessions_between")
        days = (start + timedelta(days=offset) for offset in range((end - start).days))
        return tuple(day for day in days if day.weekday() < 5)

    def session_window_start(self, as_of_session, window_sessions):
        current, remaining = as_of_session, window_sessions - 1
        while remaining:
            current -= timedelta(days=1)
            if current.weekday() < 5:
                remaining -= 1
        return current

    def detach_and_drop(self, partition):
        self._maybe_fail("detach_and_drop")
        self.dropped.append(partition.name)


@pytest.fixture
def repo(tmp_path):
    repository = FakeRepository()
    repository.root = tmp_path / "archive"
    return repository


def _worker(repo, kinds=ALERT_KINDS):
    return SessionArchiveWorker(repo, repo.root, IDENTITY, kinds=kinds, clock=lambda: VERIFIED_AT)


def _outcome_row(identifier, session=SESSIONS[0]):
    return ArchiveRow("rt.alert_management_assessments", (identifier,), {
        "assessment_id": identifier, "status": "TARGET",
        "assessed_at": datetime(session.year, session.month, session.day, 18, 0, tzinfo=UTC)})


def _seed_session(repo, session, alerts=2):
    repo.seed(session, STOCK_ALERTS, *({"alert_id": f"s{index}", "as_of": VERIFIED_AT} for index in range(alerts)))
    repo.seed(session, OPTION_ALERTS, {"alert_id": "o1", "as_of": VERIFIED_AT})
    repo.seed(session, DETECTOR_EVALUATIONS, {"evaluation_id": "e1", "observed_at": VERIFIED_AT})


def _file(repo, session, name):
    return repo.root / session.isoformat() / name


def _alarm_codes(result):
    return [alarm.code for alarm in result.alarms]


def _status(result, kind, horizon=None):
    [outcome] = [o for o in result.outcomes if o.kind == kind and o.horizon_close_date == horizon]
    return outcome.status


# Archive, verify, record ---------------------------------------------------------------------


def test_worker_requires_an_absolute_archive_directory(repo):
    with pytest.raises(ValueError, match="absolute"):
        SessionArchiveWorker(repo, "relative/archive", IDENTITY)


def test_worker_kinds_must_be_known_session_kinds(repo):
    for kinds in ((), (OUTCOMES,), ("NOT_A_KIND",)):
        with pytest.raises(ValueError, match="kinds"):
            SessionArchiveWorker(repo, repo.root, IDENTITY, kinds=kinds)


def test_every_kind_is_archived_verified_and_then_recorded(repo):
    _seed_session(repo, SESSIONS[0])
    result = _worker(repo).archive_session(SESSIONS[0])
    assert result.ok and result.pins == ()
    assert [(o.kind, o.status, o.row_count) for o in result.outcomes] == [
        (STOCK_ALERTS, STATUS_ARCHIVED, 2), (OPTION_ALERTS, STATUS_ARCHIVED, 1),
        (DETECTOR_EVALUATIONS, STATUS_ARCHIVED, 1)]
    assert sorted(record.kind for record in repo.records) == sorted(ALERT_KINDS)
    for record in repo.records:
        assert record.verified_at == VERIFIED_AT and record.build_id == "build-1"
        assert record.uri.startswith("file:") and record.horizon_close_date is None
    for name in ("stock_alerts", "option_alerts", "detector_evaluations"):
        assert _file(repo, SESSIONS[0], f"{name}.jsonl.gz").is_file()
        manifest = json.loads(_file(repo, SESSIONS[0], f"{name}.manifest.json").read_text(encoding="ascii"))
        assert manifest["build_id"] == "build-1" and manifest["policy_id"] == "policy-1"


def test_option_flow_kinds_are_archived_with_every_revision_by_default(repo):
    session = SESSIONS[0]
    _seed_session(repo, session)
    repo.seed(session, OPTION_FLOW_EVENTS,
              {"event_id": "e1", "revision": 1}, {"event_id": "e2", "revision": 2, "supersedes_event_id": "e1"})
    repo.seed(session, OPTION_FLOW_CLUSTERS,
              {"cluster_id": "c1", "revision": 1, "status": "FINAL"},
              {"cluster_id": "c1", "revision": 2, "status": "RETRACTED", "print_count": 0})
    result = _worker(repo, kinds=SESSION_KINDS).archive_session(session)
    assert result.ok and sorted(record.kind for record in repo.records) == sorted(SESSION_KINDS)
    assert {o.kind: o.row_count for o in result.outcomes} == {
        STOCK_ALERTS: 2, OPTION_ALERTS: 1, DETECTOR_EVALUATIONS: 1, OPTION_FLOW_EVENTS: 2, OPTION_FLOW_CLUSTERS: 2}
    for name in ("option_flow_events", "option_flow_clusters"):
        assert _file(repo, session, f"{name}.jsonl.gz").is_file()
    # A cancel stored after the archive changes the source: a verified revision, never a rewrite.
    repo.seed(session, OPTION_FLOW_EVENTS, {"event_id": "e1", "revision": 1}, {"event_id": "e2", "revision": 2},
              {"event_id": "e3", "revision": 3})
    again = _worker(repo, kinds=FLOW_KINDS).archive_session(session)
    assert _status(again, OPTION_FLOW_EVENTS) == STATUS_ARCHIVED and _status(again, OPTION_FLOW_CLUSTERS) == (
        STATUS_ALREADY_ARCHIVED)
    assert [o.kind for o in again.outcomes] == list(FLOW_KINDS) and again.pins == ()  # no alert work for flow kinds


def test_an_explicit_kind_selection_overrides_the_worker_default(repo):
    result = _worker(repo).archive_session(SESSIONS[1], kinds=(OPTION_FLOW_CLUSTERS,))
    assert [o.kind for o in result.outcomes] == [OPTION_FLOW_CLUSTERS] and result.ok
    assert [record.kind for record in repo.records] == [OPTION_FLOW_CLUSTERS]


def test_an_empty_session_is_archived_with_zero_rows(repo):
    result = _worker(repo).archive_session(SESSIONS[1])
    assert result.ok and [record.row_count for record in repo.records] == [0, 0, 0]
    assert len(repo.records) == 3


def test_rerun_is_idempotent(repo):
    _seed_session(repo, SESSIONS[0])
    worker = _worker(repo)
    worker.archive_session(SESSIONS[0])
    stamps = {path: path.stat().st_mtime_ns for path in repo.root.rglob("*") if path.is_file()}
    again = worker.archive_session(SESSIONS[0])
    assert again.ok and {o.status for o in again.outcomes} == {STATUS_ALREADY_ARCHIVED}
    assert len(repo.records) == 3
    assert stamps == {path: path.stat().st_mtime_ns for path in repo.root.rglob("*") if path.is_file()}


def test_rerun_fails_closed_when_existing_manifest_has_another_identity(repo):
    session = SESSIONS[0]
    prior_identity = ArchiveIdentity("prior-build", IDENTITY.policy_id)
    prior_worker = SessionArchiveWorker(
        repo,
        repo.root,
        prior_identity,
        kinds=(OPTION_FLOW_CLUSTERS,),
        clock=lambda: VERIFIED_AT,
    )
    assert prior_worker.archive_session(session).ok
    manifest_path = _file(repo, session, "option_flow_clusters.manifest.json")
    original_manifest = manifest_path.read_bytes()

    result = _worker(repo, kinds=(OPTION_FLOW_CLUSTERS,)).archive_session(session)

    assert not result.ok
    assert _status(result, OPTION_FLOW_CLUSTERS) == STATUS_FAILED
    assert _alarm_codes(result) == [ALARM_VERIFICATION_FAILED]
    assert "IDENTITY_MISMATCH" in result.alarms[0].detail
    assert len(repo.records) == 1
    assert manifest_path.read_bytes() == original_manifest


def test_a_changed_source_produces_a_new_revision_and_keeps_the_old_file(repo):
    _seed_session(repo, SESSIONS[0])
    worker = _worker(repo)
    worker.archive_session(SESSIONS[0])
    primary = _file(repo, SESSIONS[0], "stock_alerts.jsonl.gz")
    original = primary.read_bytes()
    repo.seed(SESSIONS[0], STOCK_ALERTS, {"alert_id": "s0", "as_of": VERIFIED_AT, "lifecycle": "CLOSED"})
    result = worker.archive_session(SESSIONS[0])
    assert _status(result, STOCK_ALERTS) == STATUS_ARCHIVED
    assert [o.revision for o in result.outcomes if o.kind == STOCK_ALERTS] == [True]
    assert primary.read_bytes() == original
    assert len([r for r in repo.records if r.kind == STOCK_ALERTS]) == 2


# Outcomes: append-only addenda -------------------------------------------------------------------


def test_outcome_addenda_are_written_per_close_date_and_never_rewritten(repo):
    session, closed, later = SESSIONS[0], SESSIONS[2], SESSIONS[3]
    _seed_session(repo, session)
    repo.outcomes[session] = {closed: [_outcome_row("m1"), _outcome_row("m2")]}
    repo.horizons[session] = [AlertHorizon("s0", closed), AlertHorizon("s1", closed)]
    worker = _worker(repo)
    result = worker.archive_session(session)
    assert result.ok and result.pins == ()
    addendum = _file(repo, session, f"outcomes-{closed.isoformat()}.jsonl.gz")
    assert addendum.is_file() and _status(result, OUTCOMES, closed) == STATUS_ARCHIVED
    [record] = [r for r in repo.records if r.kind == OUTCOMES]
    assert record.horizon_close_date == closed and record.row_count == 2
    original = addendum.read_bytes()

    assert _status(worker.archive_session(session), OUTCOMES, closed) == STATUS_ALREADY_ARCHIVED

    repo.outcomes[session][closed].append(_outcome_row("m3"))
    repo.outcomes[session][later] = [_outcome_row("m4")]
    repo.horizons[session].append(AlertHorizon("o1", later))
    result = worker.archive_session(session)
    assert addendum.read_bytes() == original
    revised = [o for o in result.outcomes if o.kind == OUTCOMES and o.horizon_close_date == closed]
    assert revised[0].revision and revised[0].sha256 != record.sha256
    assert _file(repo, session, f"outcomes-{later.isoformat()}.jsonl.gz").is_file()
    assert sorted((r.horizon_close_date, r.row_count) for r in repo.records if r.kind == OUTCOMES) == [
        (closed, 2), (closed, 3), (later, 1)]


def test_open_and_unverified_outcomes_pin_the_session(repo):
    session, closed = SESSIONS[0], SESSIONS[1]
    _seed_session(repo, session)
    repo.outcomes[session] = {closed: [_outcome_row("m1")]}
    repo.horizons[session] = [AlertHorizon("a-open", None), AlertHorizon("a-closed", closed)]
    result = _worker(repo).archive_session(session)
    assert [(pin.reason, pin.ref) for pin in result.pins] == [(PIN_OPEN_HORIZON, "a-open")]

    repo.failures[("count_outcomes", session, closed)] = RuntimeError("count failed")
    result = _worker(repo).archive_session(session)
    assert [(pin.reason, pin.ref) for pin in result.pins] == [
        (PIN_OUTCOME_UNVERIFIED, "a-closed"), (PIN_OPEN_HORIZON, "a-open")]
    assert _status(result, OUTCOMES, closed) == STATUS_FAILED
    assert ALARM_SOURCE_READ_FAILED in _alarm_codes(result)


def test_outcome_read_failure_alarms_and_pins_closed_alerts(repo):
    session = SESSIONS[0]
    repo.horizons[session] = [AlertHorizon("a-closed", SESSIONS[1])]
    repo.failures[("read_outcomes", session)] = RuntimeError("boom")
    result = _worker(repo).archive_session(session)
    assert _alarm_codes(result) == [ALARM_SOURCE_READ_FAILED]
    assert [pin.reason for pin in result.pins] == [PIN_OUTCOME_UNVERIFIED]
    assert len(repo.records) == 3  # the session kinds are unaffected


def test_pin_lookup_failure_alarms_and_fails_closed(repo):
    repo.failures[("alert_horizons", SESSIONS[0])] = RuntimeError("boom")
    result = _worker(repo).archive_session(SESSIONS[0])
    assert _alarm_codes(result) == [ALARM_PIN_LOOKUP_FAILED]
    assert [pin.reason for pin in result.pins] == [PIN_LOOKUP_FAILED]


# Alarm and keep; failure isolation ---------------------------------------------------------------


def test_verification_failure_alarms_keeps_the_file_and_records_nothing(repo, caplog):
    _seed_session(repo, SESSIONS[0])
    repo.count_delta[(SESSIONS[0], STOCK_ALERTS)] = {"rt.alert_current": 1}
    with caplog.at_level(logging.ERROR, logger="realtime.session_archive_worker"):
        result = _worker(repo).archive_session(SESSIONS[0])
    assert not result.ok and _alarm_codes(result) == [ALARM_VERIFICATION_FAILED]
    [alarm] = result.alarms
    assert (alarm.session_date, alarm.kind) == (SESSIONS[0], STOCK_ALERTS)
    assert "SOURCE_COUNT_MISMATCH" in alarm.detail
    assert _status(result, STOCK_ALERTS) == STATUS_FAILED
    assert sorted(r.kind for r in repo.records) == [DETECTOR_EVALUATIONS, OPTION_ALERTS]
    assert _file(repo, SESSIONS[0], "stock_alerts.jsonl.gz").is_file()  # data stays; nothing is deleted
    assert [(pin.reason, pin.ref) for pin in result.pins] == [(PIN_ARCHIVE_REFRESH_FAILED, STOCK_ALERTS)]
    [message] = [r.getMessage() for r in caplog.records if "session_archive_alarm" in r.getMessage()]
    payload = json.loads(message.split(" ", 1)[1])
    assert payload["code"] == ALARM_VERIFICATION_FAILED and payload["kind"] == STOCK_ALERTS
    assert payload["session_date"] == "2026-09-14"


def test_source_read_failure_in_one_kind_does_not_stop_the_others(repo):
    _seed_session(repo, SESSIONS[0])
    repo.failures[("read_rows", SESSIONS[0], OPTION_ALERTS)] = RuntimeError("connection lost")
    result = _worker(repo).archive_session(SESSIONS[0])
    assert _alarm_codes(result) == [ALARM_SOURCE_READ_FAILED]
    assert _status(result, OPTION_ALERTS) == STATUS_FAILED
    assert sorted(r.kind for r in repo.records) == [DETECTOR_EVALUATIONS, STOCK_ALERTS]


def test_a_row_that_cannot_be_serialized_alarms_as_a_build_failure(repo):
    _seed_session(repo, SESSIONS[0])
    repo.seed(SESSIONS[0], STOCK_ALERTS, {"alert_id": "bad", "as_of": datetime(2026, 9, 14, 17, 0)})
    result = _worker(repo).archive_session(SESSIONS[0])
    assert _alarm_codes(result) == [ALARM_BUILD_FAILED]
    assert len(repo.records) == 2


def test_write_failure_is_isolated_to_its_session(repo):
    _seed_session(repo, SESSIONS[0])
    _seed_session(repo, SESSIONS[1])
    repo.root.mkdir(parents=True)
    (repo.root / SESSIONS[0].isoformat()).write_text("a file where the directory should be")
    report = _worker(repo).archive_sessions([SESSIONS[1], SESSIONS[0]])
    assert [result.session_date for result in report.sessions] == [SESSIONS[0], SESSIONS[1]]
    assert [alarm.code for alarm in report.alarms] == [ALARM_WRITE_FAILED] * 3
    assert {alarm.session_date for alarm in report.alarms} == {SESSIONS[0]}
    assert report.sessions[1].ok and len([r for r in repo.records if r.session_date == SESSIONS[1]]) == 3
    assert not report.ok


def test_ledger_failure_alarms_and_a_later_run_records_the_existing_file(repo):
    _seed_session(repo, SESSIONS[0])
    repo.failures[("record_archive", SESSIONS[0], STOCK_ALERTS)] = RuntimeError("database down")
    worker = _worker(repo)
    result = worker.archive_session(SESSIONS[0])
    assert _alarm_codes(result) == [ALARM_LEDGER_FAILED]
    assert sorted(r.kind for r in repo.records) == [DETECTOR_EVALUATIONS, OPTION_ALERTS]
    repo.failures.clear()
    result = worker.archive_session(SESSIONS[0])
    assert result.ok and _status(result, STOCK_ALERTS) == STATUS_ARCHIVED
    assert len(repo.records) == 3


def test_failures_across_sessions_and_kinds_never_raise_and_never_stop_other_work(repo):
    for session in SESSIONS[:3]:
        _seed_session(repo, session)
    repo.failures[("read_rows", SESSIONS[1], STOCK_ALERTS)] = RuntimeError("a")
    repo.failures[("read_rows", SESSIONS[1], DETECTOR_EVALUATIONS)] = RuntimeError("b")
    repo.failures[("read_outcomes", SESSIONS[2])] = RuntimeError("c")
    report = _worker(repo).archive_sessions(SESSIONS[:3])
    assert [len(result.alarms) for result in report.sessions] == [0, 2, 1]
    assert Counter(r.session_date for r in repo.records) == {
        SESSIONS[0]: 3, SESSIONS[1]: 1, SESSIONS[2]: 3}


def test_read_only_session_plan_counts_archive_sources_without_body_reads_or_writes(repo):
    session = SESSIONS[0]
    _seed_session(repo, session)
    _seed_flow(repo, (session,))
    repo.outcomes[session] = {SESSIONS[1]: [_outcome_row("m1", session)]}
    repo.horizons[session] = [
        AlertHorizon("closed", SESSIONS[1]),
        AlertHorizon("open", None),
    ]
    repo.records.extend([
        ArchiveRecord(
            session,
            STOCK_ALERTS,
            "file:///prior-stock.jsonl.gz",
            "1" * 64,
            2,
            "schema-v1",
            "prior-build",
            VERIFIED_AT,
        ),
        ArchiveRecord(
            session,
            STOCK_ALERTS,
            "file:///current-stock.jsonl.gz",
            "2" * 64,
            2,
            "schema-v1",
            IDENTITY.build_id,
            VERIFIED_AT,
        ),
        ArchiveRecord(
            session,
            OUTCOMES,
            "file:///prior-outcomes.jsonl.gz",
            "3" * 64,
            1,
            "schema-v1",
            "prior-build",
            VERIFIED_AT,
            SESSIONS[1],
        ),
    ])
    repo.failures[("read_rows",)] = RuntimeError("must not read archive bodies")
    repo.failures[("record_archive",)] = RuntimeError("must not write archive ledger")
    repo.failures[("detach_and_drop",)] = RuntimeError("must not detach")
    plan = _worker(repo, kinds=SESSION_KINDS).plan_session_archive(session)
    assert plan.ok and plan.open_alerts == 1
    by_kind = {item.kind: item for item in plan.kinds}
    assert by_kind[STOCK_ALERTS].rows == 2
    assert by_kind[OPTION_ALERTS].rows == 1
    assert by_kind[DETECTOR_EVALUATIONS].rows == 1
    assert by_kind[OPTION_FLOW_EVENTS].rows == 1
    assert by_kind[OPTION_FLOW_CLUSTERS].rows == 2
    assert by_kind[STOCK_ALERTS].existing_archives == 2
    assert by_kind[STOCK_ALERTS].matching_build_archives == 1
    assert by_kind[OPTION_ALERTS].matching_build_archives == 0
    assert by_kind[STOCK_ALERTS].as_dict()["matching_build_archives"] == 1
    [outcome] = plan.outcomes
    assert outcome.horizon_close_date == SESSIONS[1] and outcome.rows == 1
    assert outcome.existing_archives == 1 and outcome.matching_build_archives == 0
    assert repo.dropped == []


# Drop pass ------------------------------------------------------------------------------------------


def _drop(repo, *, as_of=AS_OF_PAST_WINDOW, relations=(RELATION,), **options):
    return _worker(repo).drop_archived_partitions(as_of_session=as_of, relations=relations, **options)


def _reasons(report):
    [result] = report.partitions
    return {reason.code for reason in result.decision.reasons}


def _seed_flow(repo, sessions=SESSIONS):
    for session in sessions:
        repo.seed(session, OPTION_FLOW_CLUSTERS, {"cluster_id": "c1", "revision": 1, "status": "FINAL"},
                  {"cluster_id": "c1", "revision": 2, "status": "RETRACTED", "print_count": 0})
        repo.seed(session, OPTION_FLOW_EVENTS, {"event_id": "e1", "revision": 1})


def test_a_flow_partition_needs_only_the_kinds_its_policy_requires(repo):
    _seed_flow(repo)
    report = _drop(repo)
    assert repo.dropped == [PARTITION.name] and report.ok
    [result] = report.dropped
    assert result.decision.drop and (result.lower, result.upper) == (PARTITION.lower, PARTITION.upper)
    assert {record.kind for record in repo.records} == {OPTION_FLOW_CLUSTERS}
    assert len(repo.records) == 5  # every session of the week archived and verified first, nothing else
    assert repo.records[0].row_count == 2  # the RETRACTED revision is part of the file


def test_the_event_policy_requires_the_event_kind(repo):
    repo.policy = EVENT_POLICY
    _seed_flow(repo)
    assert _drop(repo).dropped and {record.kind for record in repo.records} == {OPTION_FLOW_EVENTS}


def test_alert_pins_do_not_gate_a_flow_partition(repo):
    repo.horizons[SESSIONS[2]] = [AlertHorizon("still-open", None)]
    repo.failures[("alert_horizons",)] = RuntimeError("never consulted")
    report = _drop(repo)
    assert repo.dropped == [PARTITION.name] and report.ok


def test_any_required_kind_unarchived_for_any_session_keeps_the_partition(repo):
    repo.policy = PartitionPolicy("ARCHIVE_FIRST", "SESSIONS", 26, True, FLOW_KINDS)
    _seed_flow(repo)
    repo.failures[("read_rows", SESSIONS[3], OPTION_FLOW_EVENTS)] = RuntimeError("down")
    report = _drop(repo)
    assert repo.dropped == [] and not report.ok
    assert _reasons(report) == {REASON_OPEN_PIN, REASON_ARCHIVE_MISSING}
    [missing] = [r for r in report.partitions[0].decision.reasons if r.code == REASON_ARCHIVE_MISSING]
    assert (missing.session_date, missing.kind) == (SESSIONS[3], OPTION_FLOW_EVENTS)
    assert {record.kind for record in repo.records if record.session_date == SESSIONS[3]} == {OPTION_FLOW_CLUSTERS}
    repo.failures.clear()  # both kinds verified for every session: now it drops
    assert _drop(repo).dropped and repo.dropped == [PARTITION.name]
    assert len(repo.records) == 10


def test_a_missing_archive_record_blocks_the_drop(repo):
    original = repo.list_archives
    repo.list_archives = lambda session: [r for r in original(session) if r.kind != OPTION_FLOW_CLUSTERS]
    report = _drop(repo)
    assert repo.dropped == [] and REASON_ARCHIVE_MISSING in _reasons(report)


def test_a_failed_refresh_blocks_the_drop_even_if_an_older_archive_exists(repo):
    repo.records.append(ArchiveRecord(SESSIONS[3], OPTION_FLOW_CLUSTERS, "file:///old", "c" * 64, 0, "v1", "old",
                                      VERIFIED_AT))
    repo.failures[("read_rows", SESSIONS[3], OPTION_FLOW_CLUSTERS)] = RuntimeError("down")
    report = _drop(repo)
    assert repo.dropped == [] and not report.ok
    assert _reasons(report) == {REASON_OPEN_PIN}
    assert report.partitions[0].decision.reasons[0].detail == f"{PIN_ARCHIVE_REFRESH_FAILED}:{OPTION_FLOW_CLUSTERS}"


# A relation whose policy requires the alert kinds is still gated by open and unverified alerts.


def test_an_open_pin_keeps_an_alert_partition_even_when_every_archive_is_verified(repo):
    repo.policy = ALERT_POLICY
    repo.horizons[SESSIONS[2]] = [AlertHorizon("still-open", None)]
    report = _drop(repo)
    assert repo.dropped == [] and _reasons(report) == {REASON_OPEN_PIN}
    assert report.partitions[0].decision.action == KEEP
    assert report.partitions[0].decision.reasons[0].detail == f"{PIN_OPEN_HORIZON}:still-open"
    assert len(repo.records) == 15  # the three alert kinds for each of the five sessions


def test_an_unverified_outcome_addendum_keeps_an_alert_partition(repo):
    repo.policy = ALERT_POLICY
    repo.outcomes[SESSIONS[0]] = {SESSIONS[1]: [_outcome_row("m1")]}
    repo.horizons[SESSIONS[0]] = [AlertHorizon("closed", SESSIONS[1])]
    repo.failures[("count_outcomes", SESSIONS[0], SESSIONS[1])] = RuntimeError("no count")
    report = _drop(repo)
    assert repo.dropped == [] and _reasons(report) == {REASON_OPEN_PIN}
    assert report.partitions[0].decision.reasons[0].detail == f"{PIN_OUTCOME_UNVERIFIED}:closed"
    repo.failures.clear()
    assert _drop(repo).dropped and repo.dropped == [PARTITION.name]


def test_every_alert_kind_is_required_for_an_alert_policy(repo):
    repo.policy = ALERT_POLICY
    original = repo.list_archives
    repo.list_archives = lambda session: [r for r in original(session) if r.kind != OPTION_ALERTS]
    assert _drop(repo).dropped == () and repo.dropped == []


@pytest.mark.parametrize(("as_of", "dropped"), [
    (date(2026, 10, 26), True),    # window starts on the partition's exclusive upper bound
    (date(2026, 10, 23), False),   # window still reaches the partition's last session
    (date(2026, 10, 19), False),
])
def test_only_partitions_older_than_the_online_window_are_considered(repo, as_of, dropped):
    report = _drop(repo, as_of=as_of)
    assert bool(repo.dropped) is dropped
    assert (len(report.partitions) == 1) is dropped
    assert len(repo.records) == (5 if dropped else 0)  # no archive work inside the window


def test_read_only_drop_plan_reports_exact_missing_archives_without_writes(repo):
    repo.failures[("read_rows",)] = RuntimeError("must not read source rows")
    repo.failures[("record_archive",)] = RuntimeError("must not write the ledger")
    repo.failures[("detach_and_drop",)] = RuntimeError("must not detach")
    report = _worker(repo).plan_archived_partitions(
        as_of_session=AS_OF_PAST_WINDOW,
        relations=(RELATION,),
        health_date=date(2026, 10, 26),
    )
    [plan] = report.partitions
    assert report.ok
    assert plan.partition == PARTITION.name
    assert plan.sessions == tuple(SESSIONS)
    assert plan.required_kinds == (OPTION_FLOW_CLUSTERS,)
    assert plan.missing_archives == tuple(
        (session, OPTION_FLOW_CLUSTERS) for session in SESSIONS
    )
    assert plan.partition_rows == plan.covered_rows == 0
    assert not plan.ledger_decision.drop
    assert repo.records == [] and repo.dropped == []


def test_read_only_drop_plan_reports_ledger_ready_but_still_requires_refresh(repo):
    for session in SESSIONS:
        repo.records.append(ArchiveRecord(
            session,
            OPTION_FLOW_CLUSTERS,
            f"file:///archive/{session.isoformat()}.jsonl.gz",
            "c" * 64,
            0,
            "v1",
            "build",
            VERIFIED_AT,
        ))
    report = _worker(repo).plan_archived_partitions(
        as_of_session=AS_OF_PAST_WINDOW,
        relations=(RELATION,),
    )
    [plan] = report.partitions
    assert report.ok and plan.ledger_decision.drop
    assert plan.existing_archives == len(SESSIONS)
    assert plan.missing_archives == ()
    assert plan.as_dict()["refresh_required_before_drop"] is True
    assert repo.dropped == []


def test_detach_failure_alarms_and_keeps_the_data(repo, caplog):
    repo.failures[("detach_and_drop",)] = RuntimeError("lock timeout")
    with caplog.at_level(logging.ERROR, logger="realtime.session_archive_worker"):
        report = _drop(repo)
    [result] = report.partitions
    assert not result.dropped and result.decision.drop
    assert [alarm.code for alarm in result.alarms] == [ALARM_PARTITION_DROP_FAILED]
    assert not report.ok and repo.dropped == []
    assert any("PARTITION_DROP_FAILED" in record.getMessage() for record in caplog.records)


def test_partition_lookup_failures_alarm_without_raising(repo):
    repo.failures[("partitions",)] = RuntimeError("catalog unavailable")
    report = _drop(repo)
    assert [alarm.code for alarm in report.alarms] == [ALARM_PARTITION_LOOKUP_FAILED] and not report.ok
    repo.failures.clear()
    repo.failures[("sessions_between",)] = RuntimeError("calendar unavailable")
    report = _drop(repo)
    assert [alarm.code for alarm in report.partitions[0].alarms] == [ALARM_PARTITION_LOOKUP_FAILED]
    assert repo.dropped == []


def test_only_partitioned_archive_first_relations_may_be_listed(repo):
    for relation in ("rt.raw_payloads", "rt.alert_current", "rt.no_such_table"):
        with pytest.raises(ValueError, match="ARCHIVE_FIRST"):
            _drop(repo, relations=(relation,))
    assert repo.dropped == []


# Policy binding: a missing or non-archive policy never drops --------------------------------------


def test_a_missing_policy_row_fails_closed(repo):
    repo.policy = None
    report = _drop(repo)
    assert [alarm.code for alarm in report.alarms] == [ALARM_PARTITION_LOOKUP_FAILED]
    assert report.partitions == () and repo.dropped == [] and repo.records == []


@pytest.mark.parametrize("policy", [
    PartitionPolicy("HOT_TTL", "SESSIONS", 26, False, ()),                         # ordinary retention
    PartitionPolicy("ARCHIVE_FIRST", "SESSIONS", 26, False, ()),                   # requires_archive off
    PartitionPolicy("ARCHIVE_FIRST", "SESSIONS", 26, True, ()),                    # no kind to verify
    PartitionPolicy("ARCHIVE_FIRST", "SESSIONS", 26, True, (OUTCOMES,)),           # a kind this worker cannot write
    PartitionPolicy("ARCHIVE_FIRST", "SESSIONS", 26, True, ("NEW_KIND",)),
    PartitionPolicy("ARCHIVE_FIRST", "CALENDAR_DAYS", 14, True, (OPTION_FLOW_EVENTS,)),
])
def test_a_policy_that_is_not_archive_gated_here_is_refused_with_an_alarm(repo, policy):
    repo.policy = policy
    report = _drop(repo)
    assert [alarm.code for alarm in report.alarms] == [ALARM_PARTITION_POLICY_UNSUPPORTED]
    assert report.partitions == () and repo.dropped == [] and repo.records == []


# Rows no archived session covers -------------------------------------------------------------------


def test_rows_outside_every_session_of_the_partition_keep_it(repo, caplog):
    repo.coverage = (10, 8)
    with caplog.at_level(logging.ERROR, logger="realtime.session_archive_worker"):
        report = _drop(repo)
    [result] = report.partitions
    assert not result.dropped and repo.dropped == []
    assert [alarm.code for alarm in result.alarms] == [ALARM_PARTITION_ROWS_OUTSIDE_SESSIONS]
    [reason] = result.decision.reasons
    assert (reason.code, reason.session_date) == (REASON_OPEN_PIN, MONDAY)
    assert reason.detail == f"{PIN_ROWS_OUTSIDE_SESSIONS}:{PARTITION.name}:2"
    assert any(ALARM_PARTITION_ROWS_OUTSIDE_SESSIONS in record.getMessage() for record in caplog.records)


def test_an_unknown_row_coverage_fails_closed(repo):
    repo.failures[("partition_row_coverage",)] = RuntimeError("catalog down")
    report = _drop(repo)
    [result] = report.partitions
    assert not result.dropped and [alarm.code for alarm in result.alarms] == [ALARM_PARTITION_LOOKUP_FAILED]
    assert result.decision.reasons[0].detail.startswith(PIN_COVERAGE_UNKNOWN)


# Partition health ------------------------------------------------------------------------------------


def _health(relation, status="OK", days=14):
    return PartitionHealth(relation, "WEEKLY", date(2026, 10, 19), days, status)


def test_every_health_alarm_is_surfaced_and_logged_as_an_error(repo, caplog):
    repo.health = [_health("rt.raw_payloads"), _health("rt.option_flow_events", "ALARM", 3),
                   _health("rt.option_flow_clusters", "ALARM", -1)]
    with caplog.at_level(logging.ERROR, logger="realtime.session_archive_worker"):
        alarms = _worker(repo).check_partition_health(date(2026, 10, 5))
    assert [(alarm.code, alarm.relation) for alarm in alarms] == [
        (ALARM_PARTITION_HEALTH, "rt.option_flow_events"), (ALARM_PARTITION_HEALTH, "rt.option_flow_clusters")]
    assert "3 days ahead" in alarms[0].detail and alarms[0].detail.startswith("ALARM")
    logged = [r for r in caplog.records if r.levelno == logging.ERROR and ALARM_PARTITION_HEALTH in r.getMessage()]
    assert len(logged) == 2
    assert repo.health_dates == [date(2026, 10, 5)]


def test_health_defaults_to_the_clock_date_and_is_returned_with_the_drop_report(repo):
    repo.health = [_health("rt.option_flow_events", "ALARM", 2)]
    _seed_flow(repo)
    report = _drop(repo)
    assert repo.health_dates == [VERIFIED_AT.date()]
    assert [(alarm.code, alarm.relation) for alarm in report.alarms] == [
        (ALARM_PARTITION_HEALTH, "rt.option_flow_events")]
    assert not report.ok and repo.dropped == [PARTITION.name]  # an alarm never blocks the archive work
    _drop(repo, health_date=date(2027, 1, 4))
    assert repo.health_dates[-1] == date(2027, 1, 4)


def test_health_failures_never_raise_or_block(repo):
    repo.failures[("partition_health",)] = RuntimeError("function missing")
    alarms = _worker(repo).check_partition_health()
    assert [alarm.code for alarm in alarms] == [ALARM_PARTITION_HEALTH_UNAVAILABLE]
    _seed_flow(repo)
    report = _drop(repo)
    assert [alarm.code for alarm in report.alarms] == [ALARM_PARTITION_HEALTH_UNAVAILABLE]
    assert repo.dropped == [PARTITION.name]


def test_a_healthy_partition_set_raises_no_alarm(repo):
    repo.health = [_health("rt.option_flow_events"), _health("rt.option_flow_clusters")]
    assert _worker(repo).check_partition_health() == ()


def test_key_parts_sort_in_their_natural_order():
    assert sorted(_key_part(value) for value in (10, 2, 1)) == [_key_part(1), _key_part(2), _key_part(10)]
    early, late = datetime(2026, 10, 5, 14, 30, tzinfo=UTC), datetime(2026, 10, 5, 14, 30, 0, 500, tzinfo=UTC)
    assert _key_part(early) < _key_part(late) and _key_part(early) == "2026-10-05T14:30:00.000000Z"
    assert _key_part("O:SPY") == "O:SPY" and _key_part(date(2026, 10, 5)) == "2026-10-05"


def test_archive_id_includes_build_identity():
    base = ArchiveRecord(
        SESSIONS[0],
        OPTION_FLOW_CLUSTERS,
        "file:///archive.jsonl.gz",
        "a" * 64,
        0,
        "schema-v1",
        "build-1",
        VERIFIED_AT,
    )
    another_build = ArchiveRecord(
        base.session_date,
        base.kind,
        base.uri,
        base.sha256,
        base.row_count,
        base.schema_version,
        "build-2",
        base.verified_at,
    )

    assert _archive_id(base) == _archive_id(base)
    assert _archive_id(base) != _archive_id(another_build)


def test_partition_bounds_are_read_as_session_dates():
    match = _BOUND_PATTERN.fullmatch("FOR VALUES FROM ('2026-09-14') TO ('2026-09-21')")
    assert [_bound_date(part, "date") for part in match.groups()] == [date(2026, 9, 14), date(2026, 9, 21)]
    stamp = _BOUND_PATTERN.fullmatch("FOR VALUES FROM ('2026-09-14 00:00:00+00') TO ('2026-09-21 00:00:00+00')")
    assert [_bound_date(part, "timestamptz") for part in stamp.groups()] == [date(2026, 9, 14), date(2026, 9, 21)]
    assert _BOUND_PATTERN.fullmatch("DEFAULT") is None
    with pytest.raises(ValueError):
        _bound_date("2026-09-14 00:00:00", "timestamptz")
