from uuid import uuid4

from v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database


def test_journal_registry_uses_runtime_polymorphic_plan_authority():
    with disposable_v2_database(
        f"test_realtime_v2_journal_plan_{uuid4().hex[:8]}",
        preserve=False,
    ) as pool:
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM pg_constraint "
                    "WHERE conrelid='rt.advanced_journal_registry'::regclass "
                    "AND conname='advanced_journal_registry_plan_id_fkey'"
                )
                assert cursor.fetchone()[0] == 0
        finally:
            connection.rollback()
            pool.putconn(connection)
