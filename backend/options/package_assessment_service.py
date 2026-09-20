"""Fail-soft WP6 package-term assessment for existing selected candidates."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Sequence

from options.outcome_contracts import (
    PACKAGE_ASSESSMENT_POLICY,
    OptionPackageAssessmentPolicy,
    assess_option_package,
)
from options.repositories.package_assessments import OptionPackageAssessmentRepository
from options.strategies.domain import CandidateKind, CandidateStatus, OptionCandidate


@dataclass(frozen=True, slots=True)
class PackageAssessmentRunResult:
    status: str
    attempted: int
    ready: int
    unavailable: int
    inserted: int
    existing: int


class OptionPackageAssessmentService:
    def __init__(
        self,
        *,
        valuation_policy_sha256: str,
        repository: OptionPackageAssessmentRepository | None = None,
        policy: OptionPackageAssessmentPolicy = PACKAGE_ASSESSMENT_POLICY,
    ) -> None:
        if len(valuation_policy_sha256) != 64 or any(
            character not in "0123456789abcdef"
            for character in valuation_policy_sha256
        ):
            raise ValueError("valuation_policy_sha256 must be a SHA-256 digest")
        self.valuation_policy_sha256 = valuation_policy_sha256
        self.repository = repository or OptionPackageAssessmentRepository()
        self.policy = policy

    def assess_and_persist(
        self, candidates: Sequence[OptionCandidate],
    ) -> PackageAssessmentRunResult:
        selected = tuple(
            candidate for candidate in candidates
            if candidate.status is CandidateStatus.SELECTED
            and candidate.candidate_kind is not CandidateKind.RESEARCH_ONLY
        )
        if len(selected) > self.policy.maximum_candidates_per_matrix:
            raise ValueError("package assessment candidate bound exceeded")
        if not selected:
            return PackageAssessmentRunResult("NO_APPLICABLE_CANDIDATES", 0, 0, 0, 0, 0)
        assessed_at = max(candidate.observed_time for candidate in selected)
        usage = self.repository.usage(
            assessed_at - timedelta(seconds=self.policy.usage_window_seconds),
            assessed_at,
            self.policy.sha256,
        )
        if usage.assessment_count >= self.policy.maximum_assessments:
            return PackageAssessmentRunResult("ASSESSMENT_LIMIT_PAUSE", 0, 0, 0, 0, 0)
        if usage.payload_bytes >= self.policy.maximum_payload_bytes:
            return PackageAssessmentRunResult("PAYLOAD_LIMIT_PAUSE", 0, 0, 0, 0, 0)
        if usage.assessment_count >= self.policy.minimum_assessments_before_rate_pause:
            unavailable_fraction = usage.unavailable_count / usage.assessment_count
            if unavailable_fraction > self.policy.maximum_unavailable_fraction:
                return PackageAssessmentRunResult("UNAVAILABLE_RATE_PAUSE", 0, 0, 0, 0, 0)
        assessments = tuple(
            assess_option_package(
                candidate,
                valuation_policy_sha256=self.valuation_policy_sha256,
                policy=self.policy,
            )
            for candidate in selected
        )
        payload_bytes = sum(
            len(assessment.canonical_json().encode("utf-8"))
            for assessment in assessments
        )
        if usage.assessment_count + len(assessments) > self.policy.maximum_assessments:
            return PackageAssessmentRunResult("ASSESSMENT_LIMIT_PAUSE", 0, 0, 0, 0, 0)
        if usage.payload_bytes + payload_bytes > self.policy.maximum_payload_bytes:
            return PackageAssessmentRunResult("PAYLOAD_LIMIT_PAUSE", 0, 0, 0, 0, 0)
        persisted = self.repository.persist(assessments)
        return PackageAssessmentRunResult(
            status="RECORDED",
            attempted=len(assessments),
            ready=sum(row.status == "READY" for row in assessments),
            unavailable=sum(row.status == "UNAVAILABLE" for row in assessments),
            inserted=persisted.inserted,
            existing=persisted.existing,
        )