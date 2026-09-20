from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import execute_values

from options.outcome_contracts import OptionOutcomeMeasurementAssessment

from .base import ConnectionFactory, PostgresRepository


@dataclass(frozen=True, slots=True)
class OutcomeUnavailablePersistResult:
    inserted: int
    existing: int


def outcome_unavailable_id(assessment: OptionOutcomeMeasurementAssessment) -> UUID:
    return uuid5(NAMESPACE_URL, f"option-outcome-unavailable:{assessment.sha256}")


class OptionOutcomeAvailabilityRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def persist_unavailable(
        self, assessments: Sequence[OptionOutcomeMeasurementAssessment],
    ) -> OutcomeUnavailablePersistResult:
        rows = tuple(assessments)
        if not rows:
            return OutcomeUnavailablePersistResult(0, 0)
        if any(row.status != "UNAVAILABLE" for row in rows):
            raise ValueError("only terminal unavailable outcome evidence is persisted")
        ids = tuple(outcome_unavailable_id(row) for row in rows)
        if len(ids) != len(set(ids)):
            raise ValueError("unavailable outcome assessments must be distinct")
        payloads = {identity: row.canonical_json() for identity, row in zip(ids, rows)}
        with self._cursor() as cursor:
            cursor.execute("SET LOCAL lock_timeout = '2s'")
            cursor.execute("SET LOCAL statement_timeout = '10s'")
            cursor.execute(
                "SELECT to_regclass('option_outcome_unavailable_evidence') IS NOT NULL AS ready"
            )
            if not cursor.fetchone()["ready"]:
                raise RuntimeError("outcome unavailable persistence requires migration 049")
            inserted = execute_values(
                cursor,
                """
                INSERT INTO option_outcome_unavailable_evidence (
                    evidence_id, candidate_id, candidate_identity,
                    valuation_policy_sha256, measurement_type, checkpoint_at,
                    availability_deadline, required_contract_ids,
                    observed_contract_ids, missing_contract_ids,
                    availability_policy_version, availability_policy_sha256,
                    payload_text, payload_sha256
                ) VALUES %s
                ON CONFLICT (evidence_id) DO NOTHING
                RETURNING evidence_id
                """,
                [
                    (
                        identity, row.candidate_id, row.candidate_identity_sha256,
                        row.valuation_policy_sha256, row.measurement_type,
                        row.checkpoint_at, row.availability_deadline,
                        list(row.required_contract_ids), list(row.observed_contract_ids),
                        list(row.missing_contract_ids),
                        row.availability_policy_version,
                        row.availability_policy_sha256,
                        payloads[identity], row.sha256,
                    )
                    for identity, row in zip(ids, rows)
                ],
                fetch=True,
            )
            cursor.execute(
                """
                SELECT evidence_id, candidate_id, candidate_identity,
                       valuation_policy_sha256, measurement_type, checkpoint_at,
                       availability_deadline, required_contract_ids,
                       observed_contract_ids, missing_contract_ids,
                       availability_policy_version, availability_policy_sha256,
                       payload_text, payload_sha256
                FROM option_outcome_unavailable_evidence
                WHERE evidence_id = ANY(%s::uuid[])
                """,
                (list(ids),),
            )
            stored = {row["evidence_id"]: row for row in cursor.fetchall()}
            if set(stored) != set(ids):
                raise ValueError("stored outcome unavailable evidence is incomplete")
            for identity, assessment in zip(ids, rows):
                row = stored[identity]
                if (
                    row["candidate_id"] != assessment.candidate_id
                    or row["candidate_identity"] != assessment.candidate_identity_sha256
                    or row["valuation_policy_sha256"] != assessment.valuation_policy_sha256
                    or row["measurement_type"] != assessment.measurement_type
                    or row["checkpoint_at"] != assessment.checkpoint_at
                    or row["availability_deadline"] != assessment.availability_deadline
                    or tuple(row["required_contract_ids"]) != assessment.required_contract_ids
                    or tuple(row["observed_contract_ids"]) != assessment.observed_contract_ids
                    or tuple(row["missing_contract_ids"]) != assessment.missing_contract_ids
                    or row["availability_policy_version"] != assessment.availability_policy_version
                    or row["availability_policy_sha256"] != assessment.availability_policy_sha256
                    or row["payload_text"] != payloads[identity]
                    or row["payload_sha256"] != assessment.sha256
                ):
                    raise ValueError(
                        "stored outcome unavailable evidence differs from requested evidence"
                    )
        return OutcomeUnavailablePersistResult(
            inserted=len(inserted), existing=len(rows) - len(inserted),
        )