from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from options.domain import ContractType
from options.outcome_contracts import (
    PACKAGE_ASSESSMENT_POLICY,
    OptionPackageAssessment,
    OptionPackageAssessmentPolicy,
)
from options.package_assessment_service import OptionPackageAssessmentService
from options.repositories.package_assessments import package_assessment_id
from options.strategies.domain import CandidateKind, OptionSide, StructureType
from test_outcome_contracts import package_candidate
from test_strategy_payoff import leg


def unavailable_assessment(reason="PACKAGE_LEGS_UNAVAILABLE"):
    return OptionPackageAssessment(
        candidate_id=uuid4(), candidate_identity_sha256="a" * 64,
        option_matrix_id=uuid4(), valuation_policy_sha256="b" * 64,
        assessment_policy_version=PACKAGE_ASSESSMENT_POLICY.version,
        assessment_policy_sha256=PACKAGE_ASSESSMENT_POLICY.sha256,
        assessed_at=datetime(2026, 9, 18, 20, 0, tzinfo=timezone.utc),
        status="UNAVAILABLE", reason_codes=(reason,),
    )


def test_package_assessment_id_is_retry_stable_and_payload_sensitive():
    assessment = unavailable_assessment()

    assert package_assessment_id(assessment) == package_assessment_id(
        OptionPackageAssessment.model_validate(assessment.model_dump())
    )
    changed = assessment.model_copy(update={"reason_codes": ("POLICY_MISMATCH",)})
    assert package_assessment_id(changed) != package_assessment_id(assessment)


def test_package_service_assesses_selected_contract_packages_and_excludes_observations():
    candidate = package_candidate(
        (leg(0, "100", "5", OptionSide.BUY, ContractType.CALL),),
        StructureType.LONG_CALL,
    )

    class Repository:
        def usage(self, *_args):
            return SimpleNamespace(
                assessment_count=0, payload_bytes=0, unavailable_count=0,
            )

        def persist(self, assessments):
            self.assessments = tuple(assessments)
            return SimpleNamespace(inserted=len(self.assessments), existing=0)

    repository = Repository()
    service = OptionPackageAssessmentService(
        valuation_policy_sha256="e" * 64, repository=repository,
    )
    observation = SimpleNamespace(
        status=candidate.status,
        candidate_kind=CandidateKind.RESEARCH_ONLY,
    )

    result = service.assess_and_persist((candidate, observation))

    assert (result.attempted, result.ready, result.unavailable) == (1, 1, 0)
    assert result.inserted == 1
    assert repository.assessments[0].candidate_id == candidate.candidate_id
    assert repository.assessments[0].execution_permission is False


def test_package_service_enforces_per_matrix_bound_before_persistence():
    candidate = package_candidate(
        (leg(0, "100", "5", OptionSide.BUY, ContractType.CALL),),
        StructureType.LONG_CALL,
    )
    service = OptionPackageAssessmentService(
        valuation_policy_sha256="e" * 64,
        repository=SimpleNamespace(
            usage=lambda *_: SimpleNamespace(
                assessment_count=0, payload_bytes=0, unavailable_count=0,
            ),
            persist=lambda _: (_ for _ in ()).throw(
                AssertionError("bounded failure cannot persist")
            )
        ),
        policy=OptionPackageAssessmentPolicy(maximum_candidates_per_matrix=1),
    )

    with pytest.raises(ValueError, match="candidate bound exceeded"):
        service.assess_and_persist((candidate, candidate))


@pytest.mark.parametrize(("usage", "expected_status"), [
    (SimpleNamespace(assessment_count=10, payload_bytes=0, unavailable_count=0),
     "ASSESSMENT_LIMIT_PAUSE"),
    (SimpleNamespace(assessment_count=1, payload_bytes=1000000, unavailable_count=0),
     "PAYLOAD_LIMIT_PAUSE"),
    (SimpleNamespace(assessment_count=4, payload_bytes=0, unavailable_count=2),
     "UNAVAILABLE_RATE_PAUSE"),
])
def test_package_service_pauses_before_assessment_or_write(usage, expected_status):
    candidate = package_candidate(
        (leg(0, "100", "5", OptionSide.BUY, ContractType.CALL),),
        StructureType.LONG_CALL,
    )

    class Repository:
        def usage(self, *_args):
            return usage

        def persist(self, _assessments):
            raise AssertionError("paused package service cannot write")

    service = OptionPackageAssessmentService(
        valuation_policy_sha256="e" * 64,
        repository=Repository(),
        policy=OptionPackageAssessmentPolicy(
            maximum_assessments=10,
            maximum_payload_bytes=1000000,
            minimum_assessments_before_rate_pause=4,
            maximum_unavailable_fraction=0.25,
        ),
    )

    result = service.assess_and_persist((candidate,))

    assert result.status == expected_status
    assert result.attempted == 0 and result.inserted == 0
