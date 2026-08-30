from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from uuid import UUID

from options.domain import RawFileManifest
from options.repositories.retention import OptionRetentionRepository


@dataclass(frozen=True, slots=True)
class RetentionVerification:
    file_id: UUID
    rollup_reconciled: bool
    accepted_backup_exists: bool
    dependencies_clear: bool


@dataclass(frozen=True, slots=True)
class FileRetentionAssessment:
    manifest: RawFileManifest
    eligible: bool
    blocked_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RetentionDryRunReport:
    assessed_at: datetime
    cutoff_market_date: date
    assessments: tuple[FileRetentionAssessment, ...]

    @property
    def eligible_file_count(self) -> int:
        return sum(assessment.eligible for assessment in self.assessments)

    @property
    def blocked_file_count(self) -> int:
        return len(self.assessments) - self.eligible_file_count

    @property
    def eligible_bytes(self) -> int:
        return sum(
            assessment.manifest.byte_size
            for assessment in self.assessments
            if assessment.eligible
        )


class RetentionReporter:
    def __init__(
        self,
        repository: OptionRetentionRepository,
        *,
        raw_retention_days: int = 30,
    ) -> None:
        if raw_retention_days <= 0:
            raise ValueError("raw_retention_days must be positive")
        self.repository = repository
        self.raw_retention_days = raw_retention_days

    def generate(
        self,
        assessed_at: datetime,
        verifications: tuple[RetentionVerification, ...] = (),
    ) -> RetentionDryRunReport:
        if assessed_at.tzinfo is None or assessed_at.utcoffset() is None:
            raise ValueError("assessed_at must be timezone-aware")
        assessed_at = assessed_at.astimezone(timezone.utc)
        cutoff = (assessed_at - timedelta(days=self.raw_retention_days)).date()
        verification_by_file = {
            verification.file_id: verification for verification in verifications
        }
        assessments = []
        for manifest, active_hold_count in self.repository.list_file_retention_candidates(
            cutoff,
            assessed_at,
        ):
            verification = verification_by_file.get(manifest.file_id)
            reasons = []
            if active_hold_count:
                reasons.append("ACTIVE_RETENTION_HOLD")
            if verification is None or not verification.rollup_reconciled:
                reasons.append("ROLLUP_NOT_RECONCILED")
            if verification is None or not verification.accepted_backup_exists:
                reasons.append("ACCEPTED_BACKUP_NOT_VERIFIED")
            if verification is None or not verification.dependencies_clear:
                reasons.append("DEPENDENCIES_NOT_VERIFIED")
            assessments.append(
                FileRetentionAssessment(
                    manifest=manifest,
                    eligible=not reasons,
                    blocked_reasons=tuple(reasons),
                )
            )
        return RetentionDryRunReport(
            assessed_at=assessed_at,
            cutoff_market_date=cutoff,
            assessments=tuple(assessments),
        )