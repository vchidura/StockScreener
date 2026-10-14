"""Disposable-database check of the session archive worker against the real v2 schema.

Opt in with SESSION_ARCHIVE_POSTGRES_TESTS=1 and run WITHOUT the offline I/O guard plugin. It
creates test_realtime_v2_c1 (never any other database), applies backend/realtime/sql_v2,
archives fixture rows, verifies and records them, and drops a precreated weekly partition
with DETACH ... CONCURRENTLY then DROP. The database is always dropped. No provider is used.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
from contextlib import closing
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest
from dotenv import load_dotenv
from psycopg2 import sql

from realtime.migrations import V2_MIGRATIONS_DIR, load_realtime_migrations, validate_target_database_v2
from realtime.session_archive import (
    ALERT_KINDS,
    DETECTOR_EVALUATIONS,
    FLOW_KINDS,
    OPTION_ALERTS,
    OPTION_FLOW_CLUSTERS,
    OPTION_FLOW_EVENTS,
    OUTCOMES,
    STOCK_ALERTS,
    ArchiveIdentity,
    verify_archive,
)
from realtime.session_archive_repository import PostgresSessionArchiveRepository
from realtime.session_archive_worker import SessionArchiveWorker

pytestmark = pytest.mark.skipif(
    os.getenv("SESSION_ARCHIVE_POSTGRES_TESTS") != "1",
    reason="explicit disposable PostgreSQL opt-in required (SESSION_ARCHIVE_POSTGRES_TESTS=1)",
)

BACKEND_DIR = Path(__file__).resolve().parents[2]
DATABASE = "test_realtime_v2_c1"
RELATION = "rt.option_flow_clusters"
UTC = timezone.utc
IDENTITY = ArchiveIdentity("test-build-c1", "test-policy-c1")


def _hash(number: int) -> str:
    return f"{number:064x}"


@dataclass(frozen=True)
class _Settings:
    host: str
    port: str
    user: str
    password: str = field(repr=False)  # keeps credentials out of pytest failure output


def _settings() -> _Settings:
    load_dotenv(BACKEND_DIR / ".env", override=True)
    names = ("DB_HOST", "DB_PORT", "POSTGRES_ADMIN_USER", "POSTGRES_ADMIN_PASSWORD")
    values = {name: os.getenv(name, "").strip() for name in names}
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise RuntimeError("requires: " + ", ".join(missing))
    return _Settings(values["DB_HOST"], values["DB_PORT"], values["POSTGRES_ADMIN_USER"],
                     values["POSTGRES_ADMIN_PASSWORD"])


def _connect(settings: _Settings, database: str):
    return psycopg2.connect(
        dbname=database, user=settings.user, password=settings.password, host=settings.host,
        port=settings.port, application_name="test_session_archive_postgres", options="-c timezone=UTC",
    )


@pytest.fixture(scope="module")
def settings():
    validate_target_database_v2(DATABASE, DATABASE, allow_disposable=True)
    values = _settings()
    created = False
    try:
        with closing(_connect(values, "postgres")) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname=%s)", (DATABASE,))
                if cursor.fetchone()[0]:
                    raise RuntimeError("disposable database already exists; refusing ownership")
                cursor.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0 ENCODING 'UTF8'")
                               .format(sql.Identifier(DATABASE)))
        created = True
        with closing(_connect(values, DATABASE)) as connection:
            for migration in load_realtime_migrations(V2_MIGRATIONS_DIR):
                with connection.cursor() as cursor:
                    cursor.execute(migration.sql)
                    cursor.execute("INSERT INTO rt.schema_migrations (version, checksum_sha256) VALUES (%s, %s)",
                                   (migration.version, migration.checksum_sha256))
                connection.commit()
        yield values
    finally:
        if created:
            with closing(_connect(values, "postgres")) as connection:
                connection.autocommit = True
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                                   "WHERE datname=%s AND pid <> pg_backend_pid()", (DATABASE,))
                    cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(DATABASE)))


def _at(day: date, hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=UTC)


def _insert_cohort(cursor, first: date, seed: int) -> str:
    """A cohort and its parents; `seed` keeps the unique hashes distinct between fixtures."""
    profile, manifest, cohort = str(uuid4()), str(uuid4()), str(uuid4())
    now = datetime.now(UTC)
    cursor.execute(
        "INSERT INTO rt.capability_profiles (profile_id, config_sha256, schema_sha256, account_alias, "
        "capabilities_sha256, universe_sha256, checked_at, expires_at, status, profile_sha256, "
        "provider_tier, security_policy_sha256, preflight_mode, provider_requests, action_authorized) "
        "VALUES (%s,%s,%s,'fixture',%s,%s,%s,%s,'PASS',%s,'MASSIVE_DEVELOPER',%s,'PASSIVE',false,false)",
        (profile, _hash(seed), _hash(seed), _hash(seed), _hash(seed), now, now + timedelta(hours=1), _hash(seed),
         _hash(seed)))
    cursor.execute(
        "INSERT INTO rt.subscription_manifests (manifest_id, manifest_sha256, universe_sha256, session_date, "
        "effective_at, profile_id, manifest_kind, stock_count, option_underlying_count, member_count, "
        "allocation_policy_sha256) VALUES (%s,%s,%s,%s,%s,%s,'MARKET_OPTIONS',400,50,400,%s)",
        (manifest, _hash(seed), _hash(seed), first, now, profile, _hash(seed)))
    cursor.execute(
        "INSERT INTO rt.cohorts (cohort_id, cohort_sha256, manifest_id, configuration_sha256, "
        "model_bundle_sha256, valuation_sha256, effective_at, mode) VALUES (%s,%s,%s,%s,%s,%s,%s,'SHADOW')",
        (cohort, _hash(seed), manifest, _hash(seed), _hash(seed), _hash(seed), now))
    return cohort


def _insert_alert_fixture(connection, first: date, second: date) -> dict[str, str]:
    """Rows in the real v2 tables: two alerts, five evaluations (every state), assessments."""
    ids = {name: str(uuid4()) for name in ("security", "stock_alert", "option_alert", "dataset")}
    events = [str(uuid4()) for _ in range(5)]
    ids["stock_event"], ids["option_event"] = events[0], events[1]
    now = datetime.now(UTC)
    with connection.cursor() as cursor:
        ids["cohort"] = _insert_cohort(cursor, first, seed=1)
        cursor.execute("INSERT INTO ref.securities (security_id, asset_type, source_origin, created_at) "
                       "VALUES (%s, 'STOCK', 'fixture', %s)", (ids["security"], now))
        cursor.execute("INSERT INTO rt.instruments (asset_class, security_id) VALUES ('EQUITY', %s) "
                       "RETURNING instrument_id", (ids["security"],))
        instrument = cursor.fetchone()[0]
        cursor.execute(
            "INSERT INTO rt.detector_versions (detector_sha256, detector_id, version, code_sha256, "
            "policy_sha256, valuation_sha256, requirements, output_kind) "
            "VALUES (%s, 'S1', 'v1', %s, %s, NULL, '{}'::jsonb, 'ALERT')", (_hash(2), _hash(2), _hash(2)))
        for index, event_id in enumerate(events):
            cursor.execute(
                "INSERT INTO rt.detector_events (event_id, dedup_sha256, cohort_id, provider_tier, "
                "detector_sha256, instrument_id, trigger_identity, trigger_at, decision_at, valid_until, "
                "disposition, direction, reason_codes, readiness_epoch, evidence_sha256, payload_schema, payload) "
                "VALUES (%s,%s,%s,'MASSIVE_DEVELOPER',%s,%s,%s,%s,%s,%s,'EVALUATED',1,%s,1,%s,'event-v1',%s::jsonb)",
                (event_id, _hash(100 + index), ids["cohort"], _hash(2), instrument, f"trigger-{index}",
                 _at(first, 14, index), _at(first, 14, index + 1), _at(first, 20), ["FIXTURE", "ROW"],
                 _hash(200 + index), json.dumps({"index": index, "entry": "101.50"})))
        for event_id, role, key in ((events[0], "bars", "1m"), (events[0], "marks", "o1"), (events[1], "marks", "o2")):
            cursor.execute(
                "INSERT INTO rt.detector_event_inputs (event_id, input_role, input_key, source_kind, "
                "feature_snapshot_id, input_sha256, available_at, received_at, retained_input) "
                "VALUES (%s,%s,%s,'FEATURE_SNAPSHOT',%s,%s,%s,%s,%s::jsonb)",
                (event_id, role, key, str(uuid4()), _hash(300), _at(first, 13, 59), _at(first, 14),
                 json.dumps({"price": 101.5, "bid": 101.4})))
        for alert_id, event_id, kind in ((ids["stock_alert"], events[0], "STOCK"),
                                         (ids["option_alert"], events[1], "OPTION")):
            cursor.execute(
                "INSERT INTO rt.alert_current (alert_id, cohort_id, event_id, alert_kind, lifecycle, as_of, "
                "valid_until, projection_schema, projection_sha256, projection) "
                "VALUES (%s,%s,%s,%s,'OPEN',%s,%s,'alert-v1',%s,%s::jsonb)",
                (alert_id, ids["cohort"], event_id, kind, _at(first, 17), _at(first, 20), _hash(400),
                 json.dumps({"entry": "101.50", "stop": "99.25", "levels": [1, 2.5]})))
        cursor.execute(
            "INSERT INTO rt.detector_datasets (dataset_id, cohort_id, provider_tier, dataset_key, session_date, "
            "status, detector_ids, source_manifest_sha256, started_at, completed_at, summary) "
            "VALUES (%s,%s,'MASSIVE_DEVELOPER','dataset-1',%s,'COMPLETE',ARRAY['S1'],%s,%s,%s,'{}'::jsonb)",
            (ids["dataset"], ids["cohort"], first, _hash(500), _at(first, 14), _at(first, 15)))
        statuses = ("SELECTED", "SELECTED", "NOT_SELECTED", "REPEAT", "OBSERVATION")
        for index, (event_id, status) in enumerate(zip(events, statuses)):
            cursor.execute(
                "INSERT INTO rt.detector_evaluations (evaluation_id, dataset_id, event_id, detector_id, "
                "underlyer, selection_status, observed_at, payload_sha256, payload) "
                "VALUES (%s,%s,%s,'S1','SPY',%s,%s,%s,%s::jsonb)",
                (str(uuid4()), ids["dataset"], event_id, status, _at(first, 14, index), _hash(600 + index),
                 json.dumps({"reason": status.lower(), "score": 0.5 + index})))
        for alert_id, assessed in ((ids["stock_alert"], _at(first, 18)), (ids["option_alert"], _at(first, 18))):
            cursor.execute(
                "INSERT INTO rt.alert_management_assessments (assessment_id, cohort_id, alert_id, assessed_at, "
                "status, payload_sha256, payload) VALUES (%s,%s,%s,%s,'READY',%s,'{}'::jsonb)",
                (str(uuid4()), ids["cohort"], alert_id, assessed, _hash(700)))
        cursor.execute(
            "INSERT INTO rt.alert_management_assessments (assessment_id, cohort_id, alert_id, assessed_at, "
            "status, payload_sha256, payload) VALUES (%s,%s,%s,%s,'TARGET',%s,%s::jsonb)",
            (str(uuid4()), ids["cohort"], ids["stock_alert"], _at(second, 15), _hash(701),
             json.dumps({"exit": "104.00"})))
    connection.commit()
    return ids


def _insert_flow_fixture(connection, first: date, second: date) -> dict[str, str]:
    """Option flow rows in the real tables: a print, its cancel revision, a retracted cluster."""
    ids = {"print": str(uuid4()), "cancel": str(uuid4()), "second": str(uuid4()), "cluster": str(uuid4()),
           "cluster_two": str(uuid4())}
    event = (
        "INSERT INTO rt.option_flow_events (event_id, event_time, cohort_id, session_date, provider_tier, "
        "contract_ticker, underlyer, source_seq, price, size, premium, condition_semantics_version, "
        "trade_treatment, side_class, side_confidence, volume_baseline_kind, revision, supersedes_event_id, "
        "conditions, opening_reasons) VALUES (%s,%s,%s,%s,'MASSIVE_DEVELOPER','O:SPY261016C00500000','SPY',%s,"
        "101.5,25,2537.5,'sem-v1',%s,'NO_QUOTE','NONE','WARMING',%s,%s,%s,%s)")
    cluster = (
        "INSERT INTO rt.option_flow_clusters (cluster_id, revision, session_date, cohort_id, provider_tier, "
        "contract_ticker, underlyer, window_seconds, window_start, window_end, side_class, print_count, "
        "total_size, total_premium, distinct_exchanges, first_event_time, last_event_time, status, "
        "policy_sha256) VALUES (%s,%s,%s,%s,'MASSIVE_DEVELOPER','O:SPY261016C00500000','SPY',60,%s,%s,"
        "'NO_QUOTE',%s,%s,%s,%s,%s,%s,%s,%s)")
    with connection.cursor() as cursor:
        cohort = _insert_cohort(cursor, first, seed=2000)
        print_time = _at(first, 14, 30, 5)
        cursor.execute(event, (ids["print"], print_time, cohort, first, 7, "INCLUDED", 1, None, [37, 12], []))
        # A cancel is a new row of the same print: same time and sequence, revision 2.
        cursor.execute(event, (ids["cancel"], print_time, cohort, first, 7, "CANCELED", 2, ids["print"], [37], []))
        cursor.execute(event, (ids["second"], _at(second, 15, 1), cohort, second, 9, "INCLUDED", 1, None, [], []))
        window = (_at(first, 14, 30), _at(first, 14, 31))
        cursor.execute(cluster, (ids["cluster"], 1, first, cohort, *window, 1, 25, "2537.5", 1,
                                 print_time, _at(first, 14, 30, 9), "FINAL", _hash(801)))
        cursor.execute(cluster, (ids["cluster"], 2, first, cohort, *window, 0, 0, "0", 0,
                                 print_time, _at(first, 14, 30, 9), "RETRACTED", _hash(801)))
        window_two = (_at(second, 15), _at(second, 15, 1))
        cursor.execute(cluster, (ids["cluster_two"], 1, second, cohort, *window_two, 1, 25, "2537.5", 1,
                                 _at(second, 15, 1, 0), _at(second, 15, 1, 0), "PROVISIONAL", _hash(802)))
    connection.commit()
    return ids


def _scalar(settings, statement, parameters=()):
    with closing(_connect(settings, DATABASE)) as connection, connection.cursor() as cursor:
        cursor.execute(statement, parameters)
        return cursor.fetchone()[0]


def _archive_rows(path: Path):
    return [json.loads(line) for line in gzip.decompress(path.read_bytes()).splitlines()]


def test_alert_archives_outcome_addenda_and_pins_on_the_real_tables(settings, tmp_path):
    root = (tmp_path / "archive").resolve()
    repository = PostgresSessionArchiveRepository(lambda: _connect(settings, DATABASE))
    worker = SessionArchiveWorker(repository, root, IDENTITY, kinds=ALERT_KINDS)

    [oldest, *_] = repository.partitions(RELATION)
    sessions = tuple(repository.sessions_between(oldest.lower, oldest.upper))
    first, second = sessions[0], sessions[1]
    with closing(_connect(settings, DATABASE)) as connection:
        ids = _insert_alert_fixture(connection, first, second)

    # 1. The option alert has no outcome yet: its session stays pinned (the stock alert's target closed).
    report = worker.archive_sessions([first, second])
    assert report.ok, [alarm.as_dict() for alarm in report.alarms]
    first_result, second_result = report.sessions
    assert [(pin.reason, pin.ref) for pin in first_result.pins] == [("OPEN_HORIZON", ids["option_alert"])]
    assert second_result.pins == ()

    directory = root / first.isoformat()
    expected = {STOCK_ALERTS: 4, OPTION_ALERTS: 3, DETECTOR_EVALUATIONS: 14}
    for kind, count in expected.items():
        name = kind.lower()
        verified = verify_archive(directory / f"{name}.jsonl.gz", directory / f"{name}.manifest.json",
                                  kind=kind, session_date=first, source_counts=repository.count_rows(first, kind))
        assert verified.row_count == count and verified.build_id == IDENTITY.build_id
        assert _scalar(settings, "SELECT count(*) FROM rt.session_archives WHERE session_date=%s AND kind=%s "
                                 "AND sha256=%s AND row_count=%s AND verified_at IS NOT NULL",
                       (first, kind, verified.sha256, count)) == 1
    assert _scalar(settings, "SELECT count(*) FROM rt.session_archives WHERE session_date=%s AND kind <> %s",
                   (second, OUTCOMES)) == len(ALERT_KINDS)

    # Rows are archived exactly as stored: every state of every evaluation, inputs by value.
    evaluation_rows = [line["row"] for line in _archive_rows(directory / "detector_evaluations.jsonl.gz")
                       if line["relation"] == "rt.detector_evaluations"]
    assert sorted(row["selection_status"] for row in evaluation_rows) == [
        "NOT_SELECTED", "OBSERVATION", "REPEAT", "SELECTED", "SELECTED"]
    stored = _scalar(settings, "SELECT payload::text FROM rt.detector_evaluations WHERE payload_sha256=%s",
                     (_hash(602),))
    assert [row["payload"] for row in evaluation_rows if row["payload_sha256"] == _hash(602)] == [json.loads(stored)]
    stock_lines = _archive_rows(directory / "stock_alerts.jsonl.gz")
    assert [line["relation"] for line in stock_lines] == [
        "rt.alert_current", "rt.detector_events", "rt.detector_event_inputs", "rt.detector_event_inputs"]
    assert stock_lines[0]["row"]["alert_id"] == ids["stock_alert"]
    assert stock_lines[3]["row"]["retained_input"] == {"bid": 101.4, "price": 101.5}
    manifest = json.loads((directory / "stock_alerts.manifest.json").read_text(encoding="ascii"))
    assert manifest["clocks"]["market"]["min"] == _at(first, 13, 59).strftime("%Y-%m-%dT%H:%M:%S.%fZ")  # input available_at
    assert manifest["clocks"]["replay"] is None

    # The stock alert's target closed on the second session: one addendum with its two assessments.
    close_name = f"outcomes-{second.isoformat()}"
    addendum = directory / f"{close_name}.jsonl.gz"
    first_bytes = addendum.read_bytes()
    assert _scalar(settings, "SELECT row_count FROM rt.session_archives WHERE session_date=%s AND kind=%s "
                             "AND horizon_close_date=%s", (first, OUTCOMES, second)) == 2

    # 2. The option alert expires on the same date: a new addendum, the first is never rewritten.
    with closing(_connect(settings, DATABASE)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO rt.alert_management_assessments (assessment_id, cohort_id, alert_id, assessed_at, "
                "status, reason_codes, payload_sha256, payload) VALUES (%s,%s,%s,%s,'EXPIRED',%s,%s,'{}'::jsonb)",
                (str(uuid4()), ids["cohort"], ids["option_alert"], _at(second, 16), ["MAX_HOLD"], _hash(702)))
        connection.commit()
    result = worker.archive_session(first)
    assert result.ok and result.pins == ()
    assert addendum.read_bytes() == first_bytes
    revision = [path for path in directory.glob(f"{close_name}.*.jsonl.gz")]
    assert len(revision) == 1
    assert hashlib.sha256(revision[0].read_bytes()).hexdigest() != hashlib.sha256(first_bytes).hexdigest()
    assert sorted(row[0] for row in _all(settings, "SELECT row_count FROM rt.session_archives WHERE "
                                         "session_date=%s AND kind=%s", (first, OUTCOMES))) == [2, 4]

    # 3. Re-running is idempotent.
    before = _scalar(settings, "SELECT count(*) FROM rt.session_archives")
    again = worker.archive_sessions([first, second])
    assert again.ok and _scalar(settings, "SELECT count(*) FROM rt.session_archives") == before


def _as_of_past_window(repository, partition):
    """A session whose 26-session window starts exactly where the partition ends."""
    return repository.sessions_between(partition.upper, partition.upper + timedelta(days=70))[25]


def test_option_flow_partitions_are_kept_until_archived_then_dropped(settings, tmp_path):
    root = (tmp_path / "archive").resolve()
    repository = PostgresSessionArchiveRepository(lambda: _connect(settings, DATABASE))
    worker = SessionArchiveWorker(repository, root, IDENTITY)
    relations = ("rt.option_flow_events", "rt.option_flow_clusters")

    # Policies come from the database: requires_archive and the kinds are bound per relation.
    assert repository.partition_policy("rt.option_flow_events").archive_kinds == (OPTION_FLOW_EVENTS,)
    assert repository.partition_policy("rt.option_flow_clusters").archive_kinds == (OPTION_FLOW_CLUSTERS,)
    assert all(repository.partition_policy(name).requires_archive for name in relations)

    [events_partition, *_] = repository.partitions("rt.option_flow_events")
    [clusters_partition, *_] = repository.partitions("rt.option_flow_clusters")
    assert (events_partition.lower, events_partition.upper) == (clusters_partition.lower, clusters_partition.upper)
    sessions = tuple(repository.sessions_between(clusters_partition.lower, clusters_partition.upper))
    first, second = sessions[0], sessions[1]
    with closing(_connect(settings, DATABASE)) as connection:
        ids = _insert_flow_fixture(connection, first, second)
    as_of = _as_of_past_window(repository, clusters_partition)
    today = datetime.now(UTC).date()

    # Rows whose session is outside the partition's sessions are noticed (sessions[0] only here).
    assert repository.partition_row_coverage(events_partition, sessions) == (3, 3)
    assert repository.partition_row_coverage(events_partition, sessions[:1]) == (3, 2)
    assert repository.partition_row_coverage(clusters_partition, sessions[1:]) == (3, 1)

    # 1. The second session cannot be written, so it is not archived: both partitions stay online.
    root.mkdir(parents=True)
    (root / second.isoformat()).write_text("blocks the directory")
    report = worker.drop_archived_partitions(as_of_session=as_of, relations=relations, health_date=today)
    assert not report.ok and report.dropped == () and report.alarms == ()
    assert {alarm.code for alarm in report.all_alarms} == {"ARCHIVE_WRITE_FAILED"}
    kept = {result.relation: result for result in report.partitions}
    assert set(kept) == set(relations)
    for relation, kind in zip(relations, (OPTION_FLOW_EVENTS, OPTION_FLOW_CLUSTERS)):
        assert not kept[relation].dropped
        assert ("ARCHIVE_MISSING", second, kind) in [
            (reason.code, reason.session_date, reason.kind) for reason in kept[relation].decision.reasons]
        assert _scalar(settings, "SELECT to_regclass(%s) IS NOT NULL", (f"rt.{kept[relation].partition}",))
    assert _scalar(settings, "SELECT count(*) FROM rt.option_flow_events") == 3
    assert _scalar(settings, "SELECT count(*) FROM rt.option_flow_clusters") == 3
    flow_kinds = list(FLOW_KINDS)
    assert _scalar(settings, "SELECT count(*) FROM rt.session_archives WHERE session_date=%s AND kind = ANY(%s)",
                   (second, flow_kinds)) == 0
    assert _scalar(settings, "SELECT count(*) FROM rt.session_archives WHERE session_date=%s AND kind=%s",
                   (first, OPTION_FLOW_EVENTS)) == 1
    # A flow partition is gated by its own kinds only: no alert file is written for its other sessions.
    assert _scalar(settings, "SELECT count(*) FROM rt.session_archives WHERE session_date = ANY(%s) "
                             "AND kind <> ALL(%s)", (list(sessions[2:]), flow_kinds)) == 0

    # 2. Rows are archived exactly as stored, every revision, in deterministic order.
    directory = root / first.isoformat()
    event_lines = _archive_rows(directory / "option_flow_events.jsonl.gz")
    assert [(line["row"]["revision"], line["row"]["trade_treatment"]) for line in event_lines] == [
        (1, "INCLUDED"), (2, "CANCELED")]
    assert event_lines[1]["row"]["supersedes_event_id"] == ids["print"]
    assert event_lines[0]["row"]["event_id"] == ids["print"] and event_lines[0]["row"]["conditions"] == [37, 12]
    assert event_lines[0]["row"]["price"] == "101.50000000" and event_lines[0]["row"]["premium"] == "2537.50000000"
    cluster_lines = _archive_rows(directory / "option_flow_clusters.jsonl.gz")
    assert [(line["row"]["revision"], line["row"]["status"], line["row"]["print_count"]) for line in cluster_lines] == [
        (1, "FINAL", 1), (2, "RETRACTED", 0)]
    for kind, count in ((OPTION_FLOW_EVENTS, 2), (OPTION_FLOW_CLUSTERS, 2)):
        name = kind.lower()
        verified = verify_archive(directory / f"{name}.jsonl.gz", directory / f"{name}.manifest.json",
                                  kind=kind, session_date=first, source_counts=repository.count_rows(first, kind))
        assert verified.row_count == count
        assert _scalar(settings, "SELECT count(*) FROM rt.session_archives WHERE session_date=%s AND kind=%s "
                                 "AND sha256=%s AND row_count=%s AND verified_at IS NOT NULL",
                       (first, kind, verified.sha256, count)) == 1
    manifest = json.loads((directory / "option_flow_events.manifest.json").read_text(encoding="ascii"))
    assert manifest["clocks"]["market"]["min"] == _at(first, 14, 30, 5).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    assert manifest["clocks"]["durable"] is not None and manifest["clocks"]["replay"] is None

    # 3. Once every session verifies for the kind its relation requires, both partitions are dropped.
    (root / second.isoformat()).unlink()
    report = worker.drop_archived_partitions(as_of_session=as_of, relations=relations, health_date=today)
    assert report.ok, [alarm.as_dict() for alarm in report.alarms]
    assert sorted(result.partition for result in report.dropped) == sorted(
        [events_partition.name, clusters_partition.name])
    for name in (events_partition.name, clusters_partition.name):
        assert not _scalar(settings, "SELECT to_regclass(%s) IS NOT NULL", (f"rt.{name}",))
    assert _scalar(settings, "SELECT count(*) FROM rt.option_flow_events") == 0
    assert _scalar(settings, "SELECT count(*) FROM rt.option_flow_clusters") == 0
    assert _scalar(settings, "SELECT count(*) FROM rt.session_archives WHERE kind=%s",
                   (OPTION_FLOW_EVENTS,)) == len(sessions)  # the ledger outlives the partition
    assert _scalar(settings, "SELECT count(*) FROM rt.session_archives WHERE kind=%s",
                   (OPTION_FLOW_CLUSTERS,)) == len(sessions)
    assert _scalar(settings, "SELECT count(*) FROM rt.session_archives WHERE session_date=%s AND kind=%s",
                   (second, OPTION_FLOW_CLUSTERS)) == 1


def test_partition_health_alarms_are_surfaced(settings, tmp_path):
    repository = PostgresSessionArchiveRepository(lambda: _connect(settings, DATABASE))
    worker = SessionArchiveWorker(repository, (tmp_path / "archive").resolve(), IDENTITY)
    today = datetime.now(UTC).date()
    assert worker.check_partition_health(today) == ()  # partitions are precreated 14 days ahead
    alarms = worker.check_partition_health(today + timedelta(days=400))
    assert {alarm.code for alarm in alarms} == {"PARTITION_HEALTH_ALARM"}
    assert {"rt.option_flow_events", "rt.option_flow_clusters", "rt.raw_payloads"} <= {
        alarm.relation for alarm in alarms}
    assert len(alarms) == _scalar(settings, "SELECT count(*) FROM rt.partition_policies")


def _all(settings, statement, parameters=()):
    with closing(_connect(settings, DATABASE)) as connection, connection.cursor() as cursor:
        cursor.execute(statement, parameters)
        return cursor.fetchall()


def test_an_interrupted_concurrent_detach_is_finalized_then_dropped(settings):
    repository = PostgresSessionArchiveRepository(lambda: _connect(settings, DATABASE))
    [partition, *_] = repository.partitions(RELATION)
    holder = _connect(settings, DATABASE)
    try:
        with holder.cursor() as cursor:  # an open transaction that has read the partition
            cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier("rt", partition.name)))
        with closing(_connect(settings, DATABASE)) as detaching:
            detaching.autocommit = True
            with detaching.cursor() as cursor:
                cursor.execute("SET statement_timeout = '1500ms'")
                with pytest.raises(psycopg2.errors.QueryCanceled):
                    cursor.execute(sql.SQL("ALTER TABLE {} DETACH PARTITION {} CONCURRENTLY").format(
                        sql.Identifier("rt", "option_flow_clusters"), sql.Identifier("rt", partition.name)))
    finally:
        holder.rollback()
        holder.close()
    [pending] = [item for item in repository.partitions(RELATION) if item.name == partition.name]
    assert pending.detach_pending

    repository.detach_and_drop(pending)
    assert not _scalar(settings, "SELECT to_regclass(%s) IS NOT NULL", (f"rt.{partition.name}",))
    assert partition.name not in [item.name for item in repository.partitions(RELATION)]
