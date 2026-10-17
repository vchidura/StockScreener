from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest

from realtime.advanced_equity_control_plane import (
    build_advanced_equity_control_plane_plan,
    persist_advanced_equity_control_plane_cursor,
)
from realtime.contracts import canonical_sha256
from v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database
NOW = datetime(2026, 10, 16, 16, 0, tzinfo=timezone.utc)


def test_spy_am_control_plane_is_warming_and_unpublished():
    with disposable_v2_database(
        f"test_realtime_v2_advanced_control_{uuid4().hex[:8]}"
    ) as pool:
        security_id = uuid4()
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO ref.securities ("
                    "security_id,asset_type,source_origin,created_at) "
                    "VALUES (%s,'STOCK','fixture',%s)",
                    (str(security_id), NOW - timedelta(days=1)),
                )
                cursor.execute(
                    "INSERT INTO ref.security_revisions ("
                    "revision_id,security_id,ticker,exchange,calendar_id,"
                    "effective_from,observed_at,payload_sha256,"
                    "company_name,security_type,active) VALUES ("
                    "%s,%s,'SPY','ARCX','XNYS',%s,%s,%s,"
                    "'SPY','ETF',true)",
                    (
                        str(uuid4()),
                        str(security_id),
                        NOW - timedelta(days=1),
                        NOW - timedelta(days=1),
                        uuid4().hex + uuid4().hex,
                    ),
                )
                cursor.execute(
                    "SELECT policy_sha256 FROM rt.security_phase_events "
                    "ORDER BY event_id DESC LIMIT 1"
                )
                policy_sha256 = cursor.fetchone()[0]
                cursor.execute(
                    "SELECT version,checksum_sha256 "
                    "FROM rt.schema_migrations ORDER BY version"
                )
                schema_sha256 = canonical_sha256(tuple(cursor.fetchall()))
                plan = build_advanced_equity_control_plane_plan(
                    session_date=date(2026, 10, 16),
                    security_ids={"SPY": security_id},
                    entitlement_evidence_sha256="a" * 64,
                    authorization_reference="fixture-advanced",
                    checked_at=NOW,
                    expires_at=NOW + timedelta(hours=8),
                    schema_sha256=schema_sha256,
                    security_policy_sha256=policy_sha256,
                )
                persist_advanced_equity_control_plane_cursor(cursor, plan)
                seed_id = uuid4()
                cursor.execute(
                    "INSERT INTO rt.seed_manifests ("
                    "seed_id,seed_sha256,universe_sha256,session_date,"
                    "status,member_count,created_at,effective_at,"
                    "valid_until,source_generations) VALUES ("
                    "%s,%s,%s,%s,'READY',1,%s,%s,%s,'{}'::jsonb)",
                    (
                        str(seed_id),
                        uuid4().hex + uuid4().hex,
                        plan.universe.sha256,
                        date(2026, 10, 16),
                        NOW,
                        NOW,
                        NOW + timedelta(hours=1),
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.seed_current ("
                    "scope,seed_id,generation,effective_at) "
                    "VALUES ('ADVANCED_SPY_REFERENCES',%s,1,%s)",
                    (str(seed_id), NOW),
                )
            connection.commit()
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT stocks_tier,options_tier,ready_state "
                    "FROM rt.provider_tier_cohorts WHERE cohort_id=%s",
                    (str(plan.cohort_id),),
                )
                assert cursor.fetchone() == (
                    "MASSIVE_ADVANCED",
                    None,
                    "ADVANCED_READY",
                )
                cursor.execute(
                    "SELECT count(*) FROM rt.portal_cohort_current "
                    "WHERE cohort_id=%s",
                    (str(plan.cohort_id),),
                )
                assert cursor.fetchone()[0] == 0
                cursor.execute(
                    "SELECT generation FROM rt.seed_current "
                    "WHERE scope='ADVANCED_SPY_REFERENCES'"
                )
                assert cursor.fetchone()[0] == 1
        finally:
            pool.putconn(connection)
