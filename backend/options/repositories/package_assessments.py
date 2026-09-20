from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import execute_values

from options.outcome_contracts import OptionPackageAssessment

from .base import ConnectionFactory, PostgresRepository


@dataclass(frozen=True, slots=True)
class PackageAssessmentPersistResult:
    inserted: int
    existing: int


@dataclass(frozen=True, slots=True)
class PackageAssessmentUsage:
    assessment_count: int
    payload_bytes: int
    unavailable_count: int


def package_assessment_id(assessment: OptionPackageAssessment) -> UUID:
    return uuid5(NAMESPACE_URL, f"option-package-assessment:{assessment.sha256}")


class OptionPackageAssessmentRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def usage(
        self, since: datetime, until: datetime, assessment_policy_sha256: str,
    ) -> PackageAssessmentUsage:
        if since.tzinfo is None or until.tzinfo is None or not since < until:
            raise ValueError("package assessment usage requires aware ordered bounds")
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute(
                """
                SELECT COUNT(*) AS assessment_count,
                       COALESCE(SUM(octet_length(payload_text)), 0) AS payload_bytes,
                       COUNT(*) FILTER (
                           WHERE assessment_status='UNAVAILABLE'
                       ) AS unavailable_count
                FROM option_package_assessments
                WHERE assessed_at >= %s AND assessed_at < %s
                  AND assessment_policy_sha256 = %s
                """,
                (since, until, assessment_policy_sha256),
            )
            row = cursor.fetchone()
        return PackageAssessmentUsage(**row)

    def persist(
        self, assessments: Sequence[OptionPackageAssessment],
    ) -> PackageAssessmentPersistResult:
        rows = tuple(assessments)
        if not rows:
            return PackageAssessmentPersistResult(0, 0)
        ids = tuple(package_assessment_id(row) for row in rows)
        if len(ids) != len(set(ids)):
            raise ValueError("package assessments must be distinct")
        payloads = {identity: row.canonical_json() for identity, row in zip(ids, rows)}
        package_payloads = {
            identity: row.package.canonical_json() if row.package is not None else None
            for identity, row in zip(ids, rows)
        }
        with self._cursor() as cursor:
            cursor.execute("SET LOCAL lock_timeout = '2s'")
            cursor.execute("SET LOCAL statement_timeout = '10s'")
            cursor.execute(
                "SELECT to_regclass('option_package_assessments') IS NOT NULL AS ready"
            )
            if not cursor.fetchone()["ready"]:
                raise RuntimeError("package assessment persistence requires migration 048")
            inserted = execute_values(
                cursor,
                """
                INSERT INTO option_package_assessments (
                    assessment_id, candidate_id, candidate_identity, matrix_id,
                    valuation_policy_sha256, assessment_policy_version,
                    assessment_policy_sha256, assessment_status,
                    package_terms_sha256, package_payload_text, assessed_at,
                    payload_text, payload_sha256
                ) VALUES %s
                ON CONFLICT (assessment_id) DO NOTHING
                RETURNING assessment_id
                """,
                [
                    (
                        identity, row.candidate_id, row.candidate_identity_sha256,
                        row.option_matrix_id, row.valuation_policy_sha256,
                        row.assessment_policy_version,
                        row.assessment_policy_sha256,
                        row.status, row.package_terms_sha256,
                        package_payloads[identity], row.assessed_at,
                        payloads[identity], row.sha256,
                    )
                    for identity, row in zip(ids, rows)
                ],
                fetch=True,
            )
            cursor.execute(
                """
                SELECT assessment_id, candidate_id, candidate_identity, matrix_id,
                      valuation_policy_sha256, assessment_policy_version,
                      assessment_policy_sha256, assessment_status,
                       package_terms_sha256, package_payload_text, assessed_at,
                       payload_text, payload_sha256
                FROM option_package_assessments
                WHERE assessment_id = ANY(%s::uuid[])
                """,
                (list(ids),),
            )
            stored = {row["assessment_id"]: row for row in cursor.fetchall()}
            if set(stored) != set(ids):
                raise ValueError("stored package assessments are incomplete")
            for identity, assessment in zip(ids, rows):
                row = stored[identity]
                if (
                    row["candidate_id"] != assessment.candidate_id
                    or row["candidate_identity"] != assessment.candidate_identity_sha256
                    or row["matrix_id"] != assessment.option_matrix_id
                    or row["valuation_policy_sha256"] != assessment.valuation_policy_sha256
                    or row["assessment_policy_version"] != assessment.assessment_policy_version
                    or row["assessment_policy_sha256"] != assessment.assessment_policy_sha256
                    or row["assessment_status"] != assessment.status
                    or row["package_terms_sha256"] != assessment.package_terms_sha256
                    or row["package_payload_text"] != package_payloads[identity]
                    or row["assessed_at"] != assessment.assessed_at
                    or row["payload_text"] != payloads[identity]
                    or row["payload_sha256"] != assessment.sha256
                ):
                    raise ValueError(
                        "stored package assessment differs from requested evidence"
                    )
        return PackageAssessmentPersistResult(
            inserted=len(inserted), existing=len(rows) - len(inserted),
        )