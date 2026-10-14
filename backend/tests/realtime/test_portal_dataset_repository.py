from datetime import date, datetime, timezone
from unittest.mock import MagicMock
from uuid import UUID

from realtime.portal_projection import (
    build_detector_dataset,
    build_management_assessment,
    build_screening_generation,
)
from realtime.portal_projection_repository import (
    persist_detector_dataset,
    persist_management_assessment,
    persist_screening_generation,
)
from realtime.preflight import ProviderTier


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 15, tzinfo=UTC)
COHORT = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
SECURITY = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
EVENT = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
ALERT = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")


def _values_executor(cursor, statement, rows, *, page_size):
    assert page_size == 100
    cursor.value_statement = statement
    cursor.value_rows = tuple(rows)


def test_screening_generation_rows_and_current_commit_atomically():
    generation = build_screening_generation(
        cohort_id=COHORT,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        session_date=date(2026, 10, 5),
        expected_members=1,
        generated_at=NOW,
        manifest={"source_generation": 7},
        rows=((SECURITY, "SPY", True, {"values": {"close": 600.0}}),),
    )
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        generation.generation_id, generation.cohort_id,
        generation.provider_tier.value, generation.session_date,
        generation.source_manifest_sha256, generation.expected_members,
        len(generation.rows), generation.status,
    )
    cursor.fetchall.return_value = ((
        SECURITY, generation.rows[0].ticker, generation.rows[0].payload_sha256,
    ),)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    persist_screening_generation(
        connection,
        generation,
        values_executor=_values_executor,
    )

    assert "INSERT INTO rt.screening_rows" in cursor.value_statement
    assert any("rt.screening_current.session_date < EXCLUDED.session_date" in call.args[0]
               for call in cursor.execute.call_args_list)
    assert connection.commit.call_count == 1


def test_detector_dataset_and_evaluations_reconcile_immutable_identity():
    dataset = build_detector_dataset(
        cohort_id=COHORT,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        dataset_key="options-canary-2026-10-05",
        session_date=date(2026, 10, 5),
        detector_ids=("O1",),
        source_manifest_sha256="e" * 64,
        started_at=NOW,
        completed_at=NOW,
        summary={"evaluated": 1},
        evaluations=((EVENT, "O1", "SPY", "OBSERVATION", NOW, {"category": "NEUTRAL_FLOW"}),),
    )
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        dataset.dataset_id, dataset.cohort_id, dataset.provider_tier.value,
        dataset.dataset_key, dataset.status, dataset.source_manifest_sha256,
    )
    evaluation = dataset.evaluations[0]
    cursor.fetchall.return_value = ((
        evaluation.evaluation_id, evaluation.event_id, evaluation.payload_sha256,
    ),)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    persist_detector_dataset(
        connection,
        dataset,
        values_executor=_values_executor,
    )

    assert "INSERT INTO rt.detector_evaluations" in cursor.value_statement
    assert connection.commit.call_count == 1


def test_unavailable_management_assessment_remains_explicit_and_monotonic():
    assessment = build_management_assessment(
        cohort_id=COHORT,
        alert_id=ALERT,
        assessed_at=NOW,
        status="UNAVAILABLE",
        reason_codes=("MANAGEMENT_INPUTS_UNAVAILABLE",),
        payload={"management_status_ready": False},
    )
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        assessment.assessment_id, assessment.cohort_id, assessment.alert_id,
        assessment.assessed_at, assessment.status, assessment.payload_sha256,
    )
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    persist_management_assessment(connection, assessment)

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert any("rt.alert_management_current.assessed_at < EXCLUDED.assessed_at"
               in statement for statement in statements)
    insert_parameters = cursor.execute.call_args_list[0].args[1]
    assert insert_parameters[4] == "UNAVAILABLE"
    assert insert_parameters[5] == ["MANAGEMENT_INPUTS_UNAVAILABLE"]
    assert connection.commit.call_count == 1