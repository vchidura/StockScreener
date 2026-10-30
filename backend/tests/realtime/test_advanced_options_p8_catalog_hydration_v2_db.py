from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest
from psycopg2.extras import RealDictCursor

from scripts.apply_advanced_options_p8_catalog_hydration import (
    _entries,
    apply_manifest_cursor,
)
from test_advanced_options_p8_catalog_hydration import _manifest
from v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database
UTC = timezone.utc


def _insert_securities(connection):
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


def test_p8_catalog_hydration_is_exact_and_one_use():
    manifest = _manifest()
    with disposable_v2_database(
        "test_realtime_v2_p8_catalog_hydration"
    ) as pool:
        connection = pool.getconn()
        try:
            _insert_securities(connection)

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


def test_p8_catalog_hydration_promotes_finite_capture_versions():
    manifest = _manifest()
    captured_at = datetime.fromisoformat(manifest["captured_at_utc"])
    finite_manifest = deepcopy(manifest)
    finite_manifest["captured_at_utc"] = (
        captured_at - timedelta(seconds=2)
    ).isoformat()
    finite_entries = _entries(
        finite_manifest,
        expected_candidate_count=2,
    )
    promoted_entries = _entries(
        manifest,
        expected_candidate_count=2,
    )
    boundary = captured_at + timedelta(hours=1)

    with disposable_v2_database(
        "test_realtime_v2_p8_catalog_promotion"
    ) as pool:
        connection = pool.getconn()
        try:
            _insert_securities(connection)
            with connection.cursor() as cursor:
                for entry, security_id in finite_entries:
                    catalog = entry.catalog
                    cursor.execute(
                        "INSERT INTO ref.option_contracts ("
                        "contract_id,underlying_security_id,provider,"
                        "contract_ticker,first_observed_at,source_origin"
                        ") VALUES (%s,%s,'massive',%s,%s,%s)",
                        (
                            catalog.contract_id,
                            security_id,
                            catalog.contract_ticker,
                            catalog.first_observed_at,
                            "MASSIVE_OPTION_REFERENCE_V3",
                        ),
                    )
                    cursor.execute(
                        "INSERT INTO ref.option_contract_versions ("
                        "version_id,contract_id,call_put,expiration,"
                        "strike,exercise_style,multiplier,deliverables,"
                        "eligibility,effective_from,effective_to,"
                        "observed_at,payload_sha256) VALUES ("
                        "%s,%s,%s,%s,%s,%s,%s,'[]'::jsonb,"
                        "'VALIDATED_ACTIVE',%s,%s,%s,%s)",
                        (
                            entry.catalog_version_id,
                            catalog.contract_id,
                            catalog.contract_type.value[0],
                            catalog.expiration_date,
                            catalog.strike,
                            catalog.exercise_style.value,
                            catalog.shares_per_contract,
                            catalog.valid_from,
                            boundary,
                            catalog.first_observed_at,
                            catalog.payload_sha256,
                        ),
                    )
            connection.commit()

            with connection.cursor(
                cursor_factory=RealDictCursor,
            ) as cursor:
                applied = apply_manifest_cursor(
                    cursor,
                    manifest,
                    expected_contract_inserts=0,
                    expected_version_inserts=2,
                    expected_instrument_inserts=2,
                    expected_candidate_count=2,
                )
            connection.commit()

            assert applied == {
                "contract_inserts": 0,
                "version_inserts": 2,
                "instrument_inserts": 2,
            }
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT "
                    "(SELECT count(*) FROM ref.option_contracts),"
                    "(SELECT count(*) FROM ref.option_contract_versions),"
                    "(SELECT count(*) FROM ref.option_contract_versions "
                    "WHERE effective_to=%s),"
                    "(SELECT count(*) FROM ref.option_contract_versions "
                    "WHERE effective_to IS NULL),"
                    "(SELECT count(*) FROM rt.instruments "
                    "WHERE asset_class='OPTION')",
                    (boundary,),
                )
                assert cursor.fetchone() == (2, 4, 2, 2, 2)
                cursor.execute(
                    "SELECT version_id "
                    "FROM ref.option_contract_versions "
                    "WHERE effective_to IS NULL ORDER BY version_id"
                )
                assert tuple(row[0] for row in cursor.fetchall()) == tuple(
                    sorted(
                        entry.catalog_version_id
                        for entry, _ in promoted_entries
                    )
                )
        finally:
            pool.putconn(connection)
