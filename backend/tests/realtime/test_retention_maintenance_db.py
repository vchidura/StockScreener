"""Authorized hardened-disposable qualification of the complete retention maintenance cycle."""
from __future__ import annotations

import shutil
from contextlib import closing, contextmanager
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from psycopg2 import sql

from realtime.partition_maintenance import ensure_partitions_and_check
from realtime.partition_retention import PartitionRetentionExecutor, PostgresPartitionRetentionRepository
from realtime.relation_registry import ADMIN_ROLE, APP_ROLE
from realtime.retention_maintenance import (
    STATUS_READY,
    RetentionMaintenanceCycle,
    RetentionMaintenanceRequest,
    read_retention_status,
)
from realtime.security_v2 import build_role_names_v2, drop_disposable_roles_v2, harden_database_v2
from realtime.session_archive import OPTION_FLOW_CLUSTERS, ArchiveIdentity
from realtime.session_archive_repository import PostgresSessionArchiveRepository
from realtime.session_archive_worker import STATUS_ALREADY_ARCHIVED, STATUS_ARCHIVED, SessionArchiveWorker
from scripts.validate_realtime_v2_schema import _connect, _settings
from v2_database import disposable_v2_database, requires_v2_database

pytestmark = requires_v2_database

DATABASE = "test_realtime_v2_retention_q1"
ROLES = build_role_names_v2("test_rt_retention_q1")
AS_OF_DATE = date(2026, 10, 17)
AS_OF_SESSION = date(2026, 10, 16)
ARCHIVE_ROOT = Path(
    r"C:\Users\vachi\.copilot\session-state\58e967d8-cb4c-4781-a8ff-1975aa0a3b8d"
    r"\files\retention-q1-archive"
)
STATUS_PATH = Path(
    r"C:\Users\vachi\.copilot\session-state\58e967d8-cb4c-4781-a8ff-1975aa0a3b8d"
    r"\files\retention-q1-status.json"
)
FUTURE_PARTITION = "rt.raw_payloads_p20261031"
EXPIRED_ORDINARY_PARTITION = "rt.raw_payloads_p20260901"
EXPIRED_ARCHIVE_PARTITION = "rt.option_flow_clusters_p20260831"
UTC = timezone.utc


def _role_connect(role: str):
    connection = _connect(_settings(), DATABASE)
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(ROLES.name(role))))
        connection.autocommit = False
        return connection
    except Exception:
        connection.close()
        raise


@contextmanager
def _role_cursor(role: str):
    with closing(_role_connect(role)) as connection:
        try:
            with connection.cursor() as cursor:
                yield cursor
            connection.commit()
        except BaseException:
            connection.rollback()
            raise


def _partition_names(cursor) -> set[str]:
    cursor.execute(
        "SELECT format('%I.%I', child_ns.nspname, child.relname) "
        "FROM rt.partition_policies AS policy "
        "JOIN pg_class AS parent ON parent.oid=to_regclass(policy.relation_name) "
        "JOIN pg_inherits AS link ON link.inhparent=parent.oid "
        "JOIN pg_class AS child ON child.oid=link.inhrelid "
        "JOIN pg_namespace AS child_ns ON child_ns.oid=child.relnamespace"
    )
    return {row[0] for row in cursor.fetchall()}


class AppPartitionMaintenance:
    def __init__(self):
        self.created: set[str] = set()

    def __call__(self, today: date):
        with _role_cursor(APP_ROLE) as cursor:
            before = _partition_names(cursor)
            result = ensure_partitions_and_check(cursor, today)
            self.created = _partition_names(cursor) - before
            return result


def _exists(cursor, relation: str) -> bool:
    cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (relation,))
    return bool(cursor.fetchone()[0])


@pytest.fixture(scope="module")
def hardened_database():
    roles_are_owned = False
    try:
        with disposable_v2_database(DATABASE) as pool:
            connection = pool.getconn()
            try:
                created, audit = harden_database_v2(connection, ROLES)
                roles_are_owned = True
                assert set(created) == set(ROLES.all_names)
                assert all(status.startswith("PASS") for _, status in audit)
            finally:
                pool.putconn(connection)
            yield
    finally:
        if roles_are_owned:
            with closing(_connect(_settings(), "postgres")) as connection:
                connection.autocommit = True
                drop_disposable_roles_v2(connection, ROLES)


def test_hardened_disposable_retention_cycle(hardened_database):
    if ARCHIVE_ROOT.exists() or STATUS_PATH.exists():
        raise RuntimeError("retention qualification artifacts already exist; refusing ownership")
    try:
        with _role_cursor(ADMIN_ROLE) as cursor:
            assert _exists(cursor, FUTURE_PARTITION)
            cursor.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier("rt", "raw_payloads_p20261031")))
            cursor.execute(
                "CREATE TABLE rt.raw_payloads_p20260901 PARTITION OF rt.raw_payloads "
                "FOR VALUES FROM ('2026-09-01 00:00:00+00') TO ('2026-09-02 00:00:00+00')"
            )
            cursor.execute(
                "CREATE TABLE rt.option_flow_clusters_p20260831 PARTITION OF rt.option_flow_clusters "
                "FOR VALUES FROM ('2026-08-31') TO ('2026-09-07')"
            )

        admin_connect = lambda: _role_connect(ADMIN_ROLE)  # noqa: E731
        archive_repository = PostgresSessionArchiveRepository(admin_connect)
        worker = SessionArchiveWorker(
            archive_repository,
            ARCHIVE_ROOT,
            ArchiveIdentity("retention-q1-build", "retention-q1-policy"),
            kinds=(OPTION_FLOW_CLUSTERS,),
            clock=lambda: datetime(2026, 10, 17, 20, 0, tzinfo=UTC),
        )
        partition_maintenance = AppPartitionMaintenance()
        cycle = RetentionMaintenanceCycle(
            partition_maintenance,
            PartitionRetentionExecutor(
                PostgresPartitionRetentionRepository(admin_connect),
                clock=lambda: datetime(2026, 10, 17, 20, 0, tzinfo=UTC),
            ),
            worker,
            STATUS_PATH,
            clock=lambda: datetime(2026, 10, 17, 20, 0, tzinfo=UTC),
        )
        request = RetentionMaintenanceRequest(
            as_of_date=AS_OF_DATE,
            as_of_session=AS_OF_SESSION,
            archive_session_dates=(AS_OF_SESSION,),
            retention_relations=("rt.raw_payloads",),
            archive_relations=("rt.option_flow_clusters",),
            archive_kinds=(OPTION_FLOW_CLUSTERS,),
        )

        status = cycle.run(request, execute=True)

        assert status.state == STATUS_READY
        assert status.created_partitions == len(partition_maintenance.created)
        assert FUTURE_PARTITION in partition_maintenance.created
        assert status.retention_dropped == 1
        assert status.archive_outcomes == 1
        assert status.archive_partitions_dropped == 1
        assert status.last_success_at == datetime(2026, 10, 17, 20, 0, tzinfo=UTC)
        assert status.consecutive_failures == 0
        assert read_retention_status(STATUS_PATH) == status

        old_sessions = archive_repository.sessions_between(date(2026, 8, 31), date(2026, 9, 7))
        expected_sessions = set(old_sessions) | {AS_OF_SESSION}
        with _role_cursor(ADMIN_ROLE) as cursor:
            assert _exists(cursor, FUTURE_PARTITION)
            assert not _exists(cursor, EXPIRED_ORDINARY_PARTITION)
            assert not _exists(cursor, EXPIRED_ARCHIVE_PARTITION)
            cursor.execute(
                "SELECT session_date FROM rt.session_archives "
                "WHERE kind=%s ORDER BY session_date",
                (OPTION_FLOW_CLUSTERS,),
            )
            assert {row[0] for row in cursor.fetchall()} == expected_sessions

        for session_date in expected_sessions:
            directory = ARCHIVE_ROOT / session_date.isoformat()
            assert len(list(directory.glob("*.jsonl.gz"))) == 1
            assert len(list(directory.glob("*.manifest.json"))) == 1
    finally:
        if STATUS_PATH.exists():
            STATUS_PATH.unlink()
        if ARCHIVE_ROOT.exists():
            shutil.rmtree(ARCHIVE_ROOT)


def test_hardened_disposable_archive_ledger_distinguishes_build_identity(
    hardened_database,
    tmp_path,
):
    session_date = date(2026, 10, 15)
    repository = PostgresSessionArchiveRepository(lambda: _role_connect(ADMIN_ROLE))
    first = SessionArchiveWorker(
        repository,
        (tmp_path / "build-a").resolve(),
        ArchiveIdentity("retention-build-a", "retention-policy-v1"),
        kinds=(OPTION_FLOW_CLUSTERS,),
        clock=lambda: datetime(2026, 10, 17, 21, 0, tzinfo=UTC),
    )
    second = SessionArchiveWorker(
        repository,
        (tmp_path / "build-b").resolve(),
        ArchiveIdentity("retention-build-b", "retention-policy-v1"),
        kinds=(OPTION_FLOW_CLUSTERS,),
        clock=lambda: datetime(2026, 10, 17, 21, 1, tzinfo=UTC),
    )

    [first_outcome] = first.archive_session(session_date).outcomes
    [second_outcome] = second.archive_session(session_date).outcomes
    [rerun_outcome] = second.archive_session(session_date).outcomes

    assert first_outcome.status == second_outcome.status == STATUS_ARCHIVED
    assert rerun_outcome.status == STATUS_ALREADY_ARCHIVED
    assert first_outcome.sha256 == second_outcome.sha256
    with _role_cursor(ADMIN_ROLE) as cursor:
        cursor.execute(
            "SELECT build_id, sha256 FROM rt.session_archives "
            "WHERE session_date=%s AND kind=%s ORDER BY build_id",
            (session_date, OPTION_FLOW_CLUSTERS),
        )
        assert cursor.fetchall() == [
            ("retention-build-a", first_outcome.sha256),
            ("retention-build-b", second_outcome.sha256),
        ]
        cursor.execute(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid='rt.session_archives'::regclass "
            "AND conname='session_archives_build_identity_key'"
        )
        [definition] = cursor.fetchone()
        assert "NULLS NOT DISTINCT" in definition and "build_id" in definition
