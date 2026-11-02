"""The live ledger's retired 045 row must migrate forward; history is never rewritten."""
from contextlib import closing
from uuid import uuid4

from psycopg2.extras import RealDictCursor

from realtime.migrations import (
    RETIRED_REALTIME_MIGRATIONS,
    V2_MIGRATIONS_DIR,
    load_realtime_migrations,
    plan_realtime_migrations,
)
from scripts import audit_advanced_options_v3_operations as operations
from v2_database import _disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database
RETIRED_VERSION = "045_advanced_options_detector_allocation"
OLD_TABLE = "rt.advanced_options_detector_allocations"
# The table as recorded from the live catalog. Its original file no longer exists.
OLD_TABLE_DDL = f"""
CREATE TABLE {OLD_TABLE} (
    selection_generation_id uuid PRIMARY KEY
        REFERENCES rt.advanced_options_selection_generations(selection_generation_id),
    allocation_sha256 char(64) NOT NULL UNIQUE
        CHECK (allocation_sha256 ~ '^[0-9a-f]{{64}}$'),
    profile_version text NOT NULL,
    profile_sha256 char(64) NOT NULL
        CHECK (profile_sha256 ~ '^[0-9a-f]{{64}}$'),
    source_capture_sha256 char(64) NOT NULL
        CHECK (source_capture_sha256 ~ '^[0-9a-f]{{64}}$'),
    allocation_document jsonb NOT NULL
        CHECK (jsonb_typeof(allocation_document) = 'object'),
    ready_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
)
"""


def _ledger(cursor):
    cursor.execute(
        "SELECT version,checksum_sha256 FROM rt.schema_migrations "
        "ORDER BY version"
    )
    return {str(row[0]): str(row[1]) for row in cursor.fetchall()}


def test_retired_ledger_row_migrates_forward_and_audits_clean():
    migrations = load_realtime_migrations(V2_MIGRATIONS_DIR)
    through_044 = tuple(
        migration for migration in migrations if migration.sequence <= 44
    )
    with _disposable_v2_database(
        f"test_realtime_v2_retired{uuid4().hex[:12]}",
        preserve=False,
        migrations=through_044,
    ) as pool:
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(OLD_TABLE_DDL)
                cursor.execute(
                    "INSERT INTO rt.schema_migrations "
                    "(version,checksum_sha256) VALUES (%s,%s)",
                    (RETIRED_VERSION, RETIRED_REALTIME_MIGRATIONS[RETIRED_VERSION]),
                )
            connection.commit()
            with connection.cursor() as cursor:
                plan = plan_realtime_migrations(
                    "test",
                    _ledger(cursor),
                    migrations,
                )
                assert [row.version for row in plan.pending] == [
                    migration.version
                    for migration in migrations
                    if migration.sequence >= 45
                ]
            connection.commit()
            for migration in plan.pending:
                with connection.cursor() as cursor:
                    cursor.execute(migration.sql)
                    cursor.execute(
                        "INSERT INTO rt.schema_migrations "
                        "(version,checksum_sha256) VALUES (%s,%s)",
                        (migration.version, migration.checksum_sha256),
                    )
                connection.commit()
            with connection.cursor() as cursor:
                cursor.execute("SELECT to_regclass(%s)", (OLD_TABLE,))
                assert cursor.fetchone()[0] is None
                cursor.execute(
                    "SELECT count(*) FROM information_schema.columns "
                    "WHERE table_schema='rt' "
                    "AND table_name='advanced_options_preparation_work' "
                    "AND column_name='expiration_state'"
                )
                assert cursor.fetchone()[0] == 1
                ledger = _ledger(cursor)
            connection.commit()
            assert (
                ledger[RETIRED_VERSION]
                == RETIRED_REALTIME_MIGRATIONS[RETIRED_VERSION]
            )
            assert (
                plan_realtime_migrations("test", ledger, migrations).pending
                == ()
            )
            connection.set_session(
                readonly=True,
                autocommit=False,
                isolation_level="REPEATABLE READ",
            )
            with closing(
                connection.cursor(cursor_factory=RealDictCursor)
            ) as cursor:
                identity = operations._production_identity(cursor, migrations)
            connection.rollback()
            assert identity["unknown_migrations"] == ()
            assert identity["checksum_drift"] == ()
            assert identity["pending_migrations"] == ()
            assert identity["retired_migrations"] == (RETIRED_VERSION,)
        finally:
            connection.rollback()
            connection.set_session(readonly=False, autocommit=False)
            pool.putconn(connection)
