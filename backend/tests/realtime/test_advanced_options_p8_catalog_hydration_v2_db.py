from __future__ import annotations

from datetime import datetime, timezone

import pytest
from psycopg2.extras import RealDictCursor

from scripts.apply_advanced_options_p8_catalog_hydration import (
    apply_manifest_cursor,
)
from test_advanced_options_p8_catalog_hydration import _manifest
from v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database
UTC = timezone.utc


def test_p8_catalog_hydration_is_exact_and_one_use():
    manifest = _manifest()
    with disposable_v2_database(
        "test_realtime_v2_p8_catalog_hydration"
    ) as pool:
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                for security_id in (
                    "11111111-1111-1111-1111-111111111111",
                    "22222222-2222-2222-2222-222222222222",
                ):
                    cursor.execute(
                        "INSERT INTO ref.securities ("
                        "security_id,asset_type,source_origin,created_at) "
                        "VALUES (%s,'ETF','P8_HYDRATION_TEST',%s)",
                        (
                            security_id,
                            datetime(2026, 10, 21, tzinfo=UTC),
                        ),
                    )
            connection.commit()

            with connection.cursor(
                cursor_factory=RealDictCursor,
            ) as cursor:
                applied = apply_manifest_cursor(
                    cursor,
                    manifest,
                    expected_contract_inserts=2,
                    expected_version_inserts=2,
                    expected_instrument_inserts=2,
                    expected_candidate_count=2,
                )
            connection.commit()

            assert applied == {
                "contract_inserts": 2,
                "version_inserts": 2,
                "instrument_inserts": 2,
            }
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT "
                    "(SELECT count(*) FROM ref.option_contracts),"
                    "(SELECT count(*) FROM ref.option_contract_versions),"
                    "(SELECT count(*) FROM rt.instruments "
                    "WHERE asset_class='OPTION')"
                )
                assert cursor.fetchone() == (2, 2, 2)

            with connection.cursor(
                cursor_factory=RealDictCursor,
            ) as cursor:
                with pytest.raises(
                    ValueError,
                    match="production diff changed",
                ):
                    apply_manifest_cursor(
                        cursor,
                        manifest,
                        expected_contract_inserts=2,
                        expected_version_inserts=2,
                        expected_instrument_inserts=2,
                        expected_candidate_count=2,
                    )
            connection.rollback()
        finally:
            pool.putconn(connection)
