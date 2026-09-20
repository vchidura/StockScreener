from __future__ import annotations

import os
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest
from dotenv import load_dotenv
from psycopg2 import sql

from options.domain import ContractType
from options.outcome_contracts import (
    PACKAGE_ASSESSMENT_POLICY,
    OptionPackageAssessment,
    assess_option_outcome_measurement,
    assess_option_package,
)
from options.repositories.outcome_availability import OptionOutcomeAvailabilityRepository
from options.repositories.package_assessments import OptionPackageAssessmentRepository
from options.strategies.domain import OptionSide, StructureType
from test_outcome_contracts import package_candidate
from test_strategy_payoff import leg


BACKEND_DIR = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(
    os.getenv("OPTION_PACKAGE_POSTGRES_TESTS") != "1",
    reason="explicit isolated PostgreSQL integration opt-in required",
)


def connection(*, admin=False, schema="public"):
    load_dotenv(BACKEND_DIR / ".env", override=True)
    user_key, password_key = (
        ("POSTGRES_ADMIN_USER", "POSTGRES_ADMIN_PASSWORD")
        if admin else ("DB_USER", "DB_PASSWORD")
    )
    return psycopg2.connect(
        dbname=os.environ["DB_NAME"], host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"], user=os.environ[user_key],
        password=os.environ[password_key], connect_timeout=10,
        application_name="option-package-assessment-isolated-test",
        options=(
            f"-c search_path={schema} -c timezone=UTC "
            "-c statement_timeout=15000 -c lock_timeout=10000"
        ),
    )


@pytest.fixture(scope="module")
def isolated_schema():
    schema = "option_package_test_" + uuid4().hex
    migration = (
        BACKEND_DIR / "migrations" / "048_option_package_assessments.sql"
    ).read_text(encoding="utf-8").replace("public.", f'"{schema}".')
    unavailable_migration = (
        BACKEND_DIR / "migrations" / "049_option_outcome_unavailable_evidence.sql"
    ).read_text(encoding="utf-8").replace("public.", f'"{schema}".')
    policy_migration = (
        BACKEND_DIR / "migrations" / "050_option_package_assessment_policy.sql"
    ).read_text(encoding="utf-8").replace("public.", f'"{schema}".')
    leg_migration = (
        BACKEND_DIR / "migrations" / "051_option_outcome_unavailable_leg_contract.sql"
    ).read_text(encoding="utf-8").replace("public.", f'"{schema}".')
    package_contract_migration = (
        BACKEND_DIR / "migrations" / "052_option_package_candidate_contract.sql"
    ).read_text(encoding="utf-8").replace("public.", f'"{schema}".')
    with closing(connection(admin=True)) as admin:
        try:
            with admin.cursor() as cursor:
                cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
                cursor.execute(sql.SQL("""
                    CREATE TABLE {}.option_analysis_runs (matrix_id uuid PRIMARY KEY)
                """).format(sql.Identifier(schema)))
                cursor.execute(sql.SQL("""
                    CREATE TABLE {}.option_strategy_candidates (
                        candidate_id uuid PRIMARY KEY,
                        candidate_identity character(64) NOT NULL,
                        matrix_id uuid NOT NULL REFERENCES {}.option_analysis_runs(matrix_id),
                        underlying varchar(16) NOT NULL,
                        strategy_name varchar(64) NOT NULL,
                        strategy_version varchar(64) NOT NULL,
                        policy_sha256 character(64) NOT NULL,
                        structure_type varchar(64) NOT NULL,
                        status varchar(16) NOT NULL,
                        candidate_kind varchar(32) NOT NULL,
                        market_data_time timestamptz NOT NULL,
                        observed_time timestamptz NOT NULL
                        ,valid_until timestamptz,
                        net_premium numeric,
                        collateral_required numeric,
                        capital_at_risk numeric,
                        maximum_profit numeric,
                        maximum_loss numeric,
                        breakevens numeric[] NOT NULL DEFAULT ARRAY[]::numeric[]
                    )
                """).format(sql.Identifier(schema), sql.Identifier(schema)))
                cursor.execute(sql.SQL("""
                    CREATE TABLE {}.option_candidate_legs (
                        candidate_id uuid NOT NULL REFERENCES {}.option_strategy_candidates(candidate_id),
                        leg_index integer NOT NULL,
                        snapshot_id uuid NOT NULL,
                        contract_id bigint NOT NULL,
                        contract_ticker varchar(64) NOT NULL,
                        side varchar(4) NOT NULL,
                        ratio integer NOT NULL,
                        multiplier integer NOT NULL,
                        expiration_date date NOT NULL,
                        strike numeric NOT NULL,
                        contract_type varchar(4) NOT NULL,
                        spot numeric NOT NULL,
                        time_to_expiration_years double precision NOT NULL,
                        risk_free_rate double precision NOT NULL,
                        dividend_yield double precision NOT NULL,
                        model_mark numeric,
                        local_iv double precision,
                        source_market_time timestamptz NOT NULL,
                        mark_source varchar(40) NOT NULL,
                        model_version varchar(64) NOT NULL,
                        valuation_policy_version varchar(64),
                        valuation_policy_sha256 character(64) NOT NULL,
                        PRIMARY KEY (candidate_id, leg_index)
                    )
                """).format(sql.Identifier(schema), sql.Identifier(schema)))
                cursor.execute(migration)
                cursor.execute(migration)
                cursor.execute(unavailable_migration)
                cursor.execute(unavailable_migration)
                cursor.execute(policy_migration)
                cursor.execute(policy_migration)
                cursor.execute(leg_migration)
                cursor.execute(leg_migration)
                cursor.execute(package_contract_migration)
                cursor.execute(package_contract_migration)
                for statement in (
                    "GRANT USAGE ON SCHEMA {} TO {}",
                    "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {} TO {}",
                    "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {} TO {}",
                ):
                    cursor.execute(sql.SQL(statement).format(
                        sql.Identifier(schema), sql.Identifier(os.environ["DB_USER"]),
                    ))
            admin.commit()
            yield schema
        finally:
            admin.rollback()
            with admin.cursor() as cursor:
                cursor.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema),
                ))
            admin.commit()
            with admin.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=%s)",
                    (schema,),
                )
                assert cursor.fetchone()[0] is False


def repository(schema):
    @contextmanager
    def factory():
        with closing(connection(schema=schema)) as current:
            yield current

    return OptionPackageAssessmentRepository(factory)


def availability_repository(schema):
    @contextmanager
    def factory():
        with closing(connection(schema=schema)) as current:
            yield current

    return OptionOutcomeAvailabilityRepository(factory)


def assessment(schema, *, reason="PACKAGE_LEGS_UNAVAILABLE"):
    matrix_id = uuid4()
    candidate_id = uuid4()
    assessed_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    row = OptionPackageAssessment(
        candidate_id=candidate_id, candidate_identity_sha256="a" * 64,
        option_matrix_id=matrix_id, valuation_policy_sha256="b" * 64,
        assessment_policy_version=PACKAGE_ASSESSMENT_POLICY.version,
        assessment_policy_sha256=PACKAGE_ASSESSMENT_POLICY.sha256,
        assessed_at=assessed_at, status="UNAVAILABLE", reason_codes=(reason,),
    )
    persist_parent(schema, row)
    return row


def persist_parent(schema, row):
    package = row.package
    with closing(connection(schema=schema)) as current:
        with current.cursor() as cursor:
            cursor.execute(
                "INSERT INTO option_analysis_runs(matrix_id) VALUES (%s)",
                (row.option_matrix_id,),
            )
            cursor.execute(
                """
                INSERT INTO option_strategy_candidates (
                    candidate_id, candidate_identity, matrix_id, underlying,
                    strategy_name, strategy_version, policy_sha256,
                    structure_type, status, candidate_kind, market_data_time,
                    observed_time, valid_until, net_premium,
                    collateral_required, capital_at_risk, maximum_profit,
                    maximum_loss, breakevens
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'SELECTED',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    row.candidate_id, row.candidate_identity_sha256,
                    row.option_matrix_id,
                    package.underlyer if package else "TEST",
                    package.strategy_name if package else "WP6_TEST",
                    package.strategy_version if package else "strategy_v1",
                    package.strategy_policy_sha256 if package else "c" * 64,
                    package.structure if package else "LONG_CALL",
                    "SINGLE_CONTRACT" if package and len(package.ordered_legs) == 1
                    else "MULTI_LEG",
                    package.market_time if package else row.assessed_at,
                    row.assessed_at, package.valid_until if package else None,
                    package.net_premium if package else None,
                    package.collateral_required if package else None,
                    package.capital_at_risk if package else None,
                    package.maximum_profit if package else None,
                    package.maximum_loss if package else None,
                    list(package.breakevens) if package else [],
                ),
            )
            package_legs = package.ordered_legs if package is not None else (None, None)
            for leg_index, package_leg in enumerate(package_legs):
                cursor.execute(
                    """
                    INSERT INTO option_candidate_legs (
                        candidate_id, leg_index, snapshot_id, contract_id,
                        contract_ticker, side, ratio, multiplier,
                        expiration_date, strike, contract_type, spot,
                        time_to_expiration_years, risk_free_rate,
                        dividend_yield, model_mark, local_iv,
                        source_market_time, mark_source, model_version,
                        valuation_policy_version, valuation_policy_sha256
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        row.candidate_id, leg_index,
                        package_leg.snapshot_id if package_leg else uuid4(),
                        package_leg.contract_id if package_leg else leg_index + 1,
                        package_leg.contract_ticker if package_leg else f"O:TEST{leg_index}",
                        package_leg.side if package_leg else "BUY",
                        package_leg.ratio if package_leg else 1,
                        package_leg.multiplier if package_leg else 100,
                        package_leg.expiration_date if package_leg else row.assessed_at.date(),
                        package_leg.strike if package_leg else 100,
                        package_leg.contract_type if package_leg else "CALL",
                        package_leg.spot if package_leg else 100,
                        package_leg.time_to_expiration_years if package_leg else 0.1,
                        package_leg.risk_free_rate if package_leg else 0.04,
                        package_leg.dividend_yield if package_leg else 0.0,
                        package_leg.entry_mark if package_leg else 1,
                        package_leg.entry_iv if package_leg else 0.2,
                        package_leg.source_market_time if package_leg else row.assessed_at,
                        package_leg.mark_source if package_leg else "TEST_MARK",
                        package_leg.model_version if package_leg else "test_v1",
                        package_leg.valuation_policy_version if package_leg else "test_v1",
                        row.valuation_policy_sha256,
                    ),
                )
        current.commit()


def ready_assessment(schema):
    candidate = package_candidate(
        (leg(0, "100", "5", OptionSide.BUY, ContractType.CALL),),
        StructureType.LONG_CALL,
    )
    row = assess_option_package(candidate, valuation_policy_sha256="e" * 64)
    persist_parent(schema, row)
    return row


def test_postgres_package_assessment_retry_and_database_clock(isolated_schema):
    row = assessment(isolated_schema)
    repo = repository(isolated_schema)

    first = repo.persist((row,))
    retry = repo.persist((row,))

    assert (first.inserted, first.existing) == (1, 0)
    assert (retry.inserted, retry.existing) == (0, 1)
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            cursor.execute(
                """
                SELECT payload_text, payload_sha256, recorded_at,
                       assessment_status, package_terms_sha256
                FROM option_package_assessments WHERE candidate_id=%s
                """,
                (row.candidate_id,),
            )
            payload, payload_sha256, recorded_at, status, package_hash = cursor.fetchone()
    assert payload == row.canonical_json() and payload_sha256 == row.sha256
    assert recorded_at > row.assessed_at
    assert status == "UNAVAILABLE" and package_hash is None


def test_postgres_ready_package_verifies_canonical_package_payload(isolated_schema):
    row = ready_assessment(isolated_schema)

    result = repository(isolated_schema).persist((row,))

    assert result.inserted == 1
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            cursor.execute(
                """
                SELECT package_terms_sha256, package_payload_text,
                       payload_text::jsonb->'package' = package_payload_text::jsonb
                FROM option_package_assessments WHERE candidate_id=%s
                """,
                (row.candidate_id,),
            )
            package_hash, package_payload, payload_matches = cursor.fetchone()
    assert package_hash == row.package.sha256
    assert package_payload == row.package.canonical_json()
    assert payload_matches is True
    usage = repository(isolated_schema).usage(
        row.assessed_at - timedelta(minutes=1),
        row.assessed_at + timedelta(minutes=1),
        row.assessment_policy_sha256,
    )
    assert usage.assessment_count == 1
    assert usage.payload_bytes == len(row.canonical_json().encode("utf-8"))
    assert usage.unavailable_count == 0


def test_postgres_ready_package_rejects_nested_candidate_drift(isolated_schema):
    row = ready_assessment(isolated_schema)
    forged_package = row.package.model_copy(update={"underlyer": "OTHER"})
    forged = row.model_copy(update={
        "package": forged_package,
        "package_terms_sha256": forged_package.sha256,
    })

    with pytest.raises(psycopg2.Error, match="candidate or package disagrees"):
        repository(isolated_schema).persist((forged,))


def test_postgres_package_assessment_rejects_conflict_and_mutation(isolated_schema):
    row = assessment(isolated_schema)
    repository(isolated_schema).persist((row,))
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor, pytest.raises(psycopg2.Error, match="immutable"):
            cursor.execute(
                "UPDATE option_package_assessments SET payload_text=payload_text "
                "WHERE candidate_id=%s",
                (row.candidate_id,),
            )
        current.rollback()
    for statement in (
        "DELETE FROM option_package_assessments WHERE candidate_id=%s",
        "UPDATE option_package_assessments SET assessment_status=assessment_status WHERE candidate_id=%s",
    ):
        with closing(connection(schema=isolated_schema)) as current:
            with current.cursor() as cursor, pytest.raises(psycopg2.Error, match="immutable"):
                cursor.execute(statement, (row.candidate_id,))
            current.rollback()
    with closing(connection(admin=True, schema=isolated_schema)) as current:
        with current.cursor() as cursor, pytest.raises(psycopg2.Error, match="immutable"):
            cursor.execute("TRUNCATE option_package_assessments")
        current.rollback()


def test_postgres_package_assessment_rejects_conflicting_existing_payload(
    isolated_schema,
):
    from options.repositories.package_assessments import package_assessment_id

    requested = assessment(isolated_schema)
    changed = requested.model_copy(update={"reason_codes": ("CHANGED_REASON",)})
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO option_package_assessments (
                    assessment_id, candidate_id, candidate_identity, matrix_id,
                    valuation_policy_sha256, assessment_policy_version,
                    assessment_policy_sha256, assessment_status,
                    package_terms_sha256, package_payload_text, assessed_at,
                    payload_text, payload_sha256
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    package_assessment_id(requested), changed.candidate_id,
                    changed.candidate_identity_sha256, changed.option_matrix_id,
                    changed.valuation_policy_sha256,
                    changed.assessment_policy_version,
                    changed.assessment_policy_sha256, changed.status,
                    changed.package_terms_sha256, None, changed.assessed_at,
                    changed.canonical_json(), changed.sha256,
                ),
            )
        current.commit()

    with pytest.raises(ValueError, match="differs from requested evidence"):
        repository(isolated_schema).persist((requested,))


def test_postgres_package_assessment_identity_failure_rolls_back(isolated_schema):
    row = assessment(isolated_schema)
    invalid = row.model_copy(update={"candidate_identity_sha256": "f" * 64})

    with pytest.raises(psycopg2.Error, match="candidate or package disagrees"):
        repository(isolated_schema).persist((invalid,))
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM option_package_assessments WHERE candidate_id=%s",
                (row.candidate_id,),
            )
            assert cursor.fetchone()[0] == 0


def test_postgres_outcome_unavailable_retry_clock_and_immutability(isolated_schema):
    package = assessment(isolated_schema)
    checkpoint = package.assessed_at - timedelta(hours=1)
    row = assess_option_outcome_measurement(
        candidate_id=package.candidate_id,
        candidate_identity_sha256=package.candidate_identity_sha256,
        valuation_policy_sha256=package.valuation_policy_sha256,
        measurement_type="30MIN", checkpoint_at=checkpoint,
        evaluated_at=package.assessed_at,
        required_contract_ids=(1, 2), observed_contract_ids=(1,),
        source_batch_id=None,
    )
    repo = availability_repository(isolated_schema)

    first = repo.persist_unavailable((row,))
    retry = repo.persist_unavailable((row,))

    assert (first.inserted, first.existing) == (1, 0)
    assert (retry.inserted, retry.existing) == (0, 1)
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            cursor.execute(
                """
                SELECT missing_contract_ids, recorded_at, payload_text,
                       payload_sha256
                FROM option_outcome_unavailable_evidence WHERE candidate_id=%s
                """,
                (row.candidate_id,),
            )
            missing, recorded_at, payload, payload_sha256 = cursor.fetchone()
    assert tuple(missing) == (2,)
    assert recorded_at >= row.availability_deadline
    assert payload == row.canonical_json() and payload_sha256 == row.sha256
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor, pytest.raises(psycopg2.Error, match="immutable"):
            cursor.execute(
                "DELETE FROM option_outcome_unavailable_evidence WHERE candidate_id=%s",
                (row.candidate_id,),
            )
        current.rollback()


def test_postgres_outcome_unavailable_rejects_candidate_leg_policy_drift(
    isolated_schema,
):
    package = assessment(isolated_schema)
    checkpoint = package.assessed_at - timedelta(hours=1)
    invalid = assess_option_outcome_measurement(
        candidate_id=package.candidate_id,
        candidate_identity_sha256=package.candidate_identity_sha256,
        valuation_policy_sha256="f" * 64,
        measurement_type="30MIN", checkpoint_at=checkpoint,
        evaluated_at=package.assessed_at,
        required_contract_ids=(1, 2), observed_contract_ids=(1,),
        source_batch_id=None,
    )

    with pytest.raises(psycopg2.Error, match="package legs or policy disagree"):
        availability_repository(isolated_schema).persist_unavailable((invalid,))