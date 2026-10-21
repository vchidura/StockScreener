"""Disposable hardened-PostgreSQL qualification for the O-WP3 resident."""
from __future__ import annotations

import os
import sys
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg2 import Error as PsycopgError

from realtime.advanced_options_service import (
    OPTIONS_NON_SERVING_STAGE_ORDER,
    AdvancedOptionsProcessKind,
    AdvancedOptionsResidentLauncher,
    AdvancedOptionsSubprocessPort,
    build_advanced_options_non_serving_qualification_plan,
    build_owp3_advanced_options_resident_plan,
)
from realtime.database_access_v2 import (
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.orchestration_repository import (
    PostgresOrchestrationRepository,
)
from realtime.security_v2 import (
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from scripts.validate_realtime_v2_schema import _connect, _settings
from v2_database import (
    disposable_v2_database,
    insert_cohort,
    requires_v2_database,
)


pytestmark = requires_v2_database
DATABASE = "test_realtime_v2_options_owp3"
ROLE_PREFIX = "test_rt_options_owp3"
UTC = timezone.utc
FORBIDDEN_RELATIONS = (
    "rt.portal_releases",
    "rt.portal_release_current",
    "rt.alert_outbox",
    "rt.session_archives",
    "rt.archive_segments",
)


class _RetryablePostgresError(PsycopgError):
    @property
    def pgcode(self):
        return "40001"


class _RetryOnceProvider:
    def __init__(self, provider):
        self._provider = provider
        self.attempts = 0

    @contextmanager
    def transaction(self, *args, **kwargs):
        self.attempts += 1
        if self.attempts == 1:
            raise _RetryablePostgresError("qualification serialization")
        with self._provider.transaction(*args, **kwargs) as unit:
            yield unit


def _assert_absent(settings, roles) -> None:
    with closing(_connect(settings, "postgres")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT datname FROM pg_database WHERE datname=%s",
                (DATABASE,),
            )
            databases = cursor.fetchall()
            cursor.execute(
                "SELECT rolname FROM pg_roles WHERE rolname=ANY(%s)",
                (list(roles.all_names),),
            )
            role_rows = cursor.fetchall()
    if databases or role_rows:
        raise RuntimeError(
            "O-WP3 disposable target is not clean: "
            f"databases={databases}, roles={role_rows}"
        )


@contextmanager
def _qualification_database():
    settings = _settings()
    roles = build_role_names_v2(ROLE_PREFIX)
    _assert_absent(settings, roles)
    try:
        with disposable_v2_database(DATABASE) as pool:
            connection = pool.getconn()
            try:
                now = datetime.now(UTC).replace(microsecond=0)
                with connection.cursor() as cursor:
                    cohort_id = insert_cohort(cursor, now)
                connection.commit()
                harden_database_v2(connection, roles)
                connection.commit()
            finally:
                pool.putconn(connection)
            provider = V2ConnectionProvider(
                {APP_ROLE: pool},
                V2AccessConfig(
                    security_mode="hardened",
                    role_prefix=ROLE_PREFIX,
                    allow_disposable=True,
                ),
            )
            assert provider.verify() == DATABASE
            yield provider, cohort_id, now
    finally:
        with closing(_connect(settings, "postgres")) as connection:
            connection.autocommit = True
            drop_disposable_roles_v2(connection, roles)
        _assert_absent(settings, roles)


def _effect_counts(provider) -> dict[str, int]:
    with provider.transaction(read_only=True) as unit:
        result = {}
        for relation in FORBIDDEN_RELATIONS:
            unit.cursor.execute(f"SELECT count(*) FROM {relation}")
            result[relation] = int(unit.cursor.fetchone()[0])
        return result


def _isolated_environment() -> dict[str, str]:
    return {
        name: os.environ[name]
        for name in ("SystemRoot", "WINDIR", "TEMP", "TMP")
        if name in os.environ
    }


def test_owp3_disposable_resident_plan_fencing_restart_and_cleanup(
    tmp_path,
):
    with _qualification_database() as (provider, cohort_id, now):
        repository = PostgresOrchestrationRepository(provider)
        config_sha256 = "9" * 64
        plan_id = repository.create_plan(
            cohort_id,
            now.date(),
            OPTIONS_NON_SERVING_STAGE_ORDER,
            config_sha256=config_sha256,
            deadline=now + timedelta(hours=1),
            scope_reference=(
                "O-WP3:"
                "c1136851-b496-5efe-aaa7-1eb8c8a7efd5:"
                "4b5a24d95fe037d13a54153adbc7f2c0136469946047af41d8b"
                "98225da0f117c"
            ),
            catalog_version="stages_v4",
        )
        qualification = (
            build_advanced_options_non_serving_qualification_plan(
                plan_id=plan_id,
                cohort_id=cohort_id,
                session_date=now.date(),
                config_sha256=config_sha256,
                deadline=now + timedelta(hours=1),
            )
        )
        assert repository.load_plan(plan_id) == qualification.service_plan
        assert _effect_counts(provider) == {
            relation: 0 for relation in FORBIDDEN_RELATIONS
        }

        command = (
            sys.executable,
            "-B",
            "-c",
            "import time; time.sleep(60)",
        )
        processes = AdvancedOptionsSubprocessPort(
            {kind: command for kind in AdvancedOptionsProcessKind},
            cwd=tmp_path,
            environment=_isolated_environment(),
        )
        retrying_provider = _RetryOnceProvider(provider)
        owners = {
            kind: uuid4() for kind in AdvancedOptionsProcessKind
        }
        lock_path = tmp_path / "massive-options.lock"
        launcher = AdvancedOptionsResidentLauncher(
            build_owp3_advanced_options_resident_plan(
                plan_id=plan_id,
                cohort_id=cohort_id,
                session_date=now.date(),
            ),
            retrying_provider,
            processes,
            owner_ids=owners,
            socket_lock_path=lock_path,
            cleanup_host_artifacts=True,
            postgres_retry_initial_seconds=0,
            wait=lambda _seconds: None,
        )
        started_at = datetime.now(UTC).replace(microsecond=0)
        try:
            health = launcher.start(now=started_at)
            assert retrying_provider.attempts == 5
            assert all(
                verdict.status == "READY"
                for verdict in health.values()
            )

            ingress = processes.process_history[
                AdvancedOptionsProcessKind.INGRESS
            ][-1]
            ingress.terminate()
            ingress.wait(timeout=5)
            launcher.tick(now=started_at + timedelta(seconds=1))
            launcher.tick(now=started_at + timedelta(seconds=2))
            assert len(
                processes.process_history[
                    AdvancedOptionsProcessKind.INGRESS
                ]
            ) == 2

            with provider.transaction() as unit:
                unit.cursor.execute(
                    "UPDATE rt.worker_leases SET owner_id=%s,"
                    "owner_epoch=owner_epoch+1,lease_until=%s "
                    "WHERE resource_key=%s",
                    (
                        str(uuid4()),
                        started_at,
                        (
                            f"advanced-options:{cohort_id}:"
                            f"{now.date()}:flow"
                        ),
                    ),
                )
                assert unit.cursor.rowcount == 1
            lost = launcher.tick(
                now=started_at + timedelta(seconds=3)
            )
            assert lost[
                AdvancedOptionsProcessKind.FLOW
            ].status == "UNAVAILABLE"
            launcher.tick(now=started_at + timedelta(seconds=4))
            assert len(
                processes.process_history[
                    AdvancedOptionsProcessKind.FLOW
                ]
            ) == 2
        finally:
            launcher.stop()

        assert all(
            handle.poll() is not None
            for history in processes.process_history.values()
            for handle in history
        )
        assert not lock_path.exists()
        assert not Path(f"{lock_path}.json").exists()
        with provider.transaction(read_only=True) as unit:
            unit.cursor.execute(
                "SELECT count(*) FROM rt.worker_leases "
                "WHERE lease_until>clock_timestamp()"
            )
            assert unit.cursor.fetchone()[0] == 0
        assert _effect_counts(provider) == {
            relation: 0 for relation in FORBIDDEN_RELATIONS
        }
        assert not any(qualification.effect_authorizations.values())
