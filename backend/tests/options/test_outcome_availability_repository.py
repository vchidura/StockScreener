from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from options.outcome_contracts import assess_option_outcome_measurement
from options.repositories.outcome_availability import (
    OptionOutcomeAvailabilityRepository,
    outcome_unavailable_id,
)


UTC = timezone.utc


def unavailable(*, evaluated_late_seconds=0):
    checkpoint = datetime(2026, 9, 18, 20, 0, tzinfo=UTC)
    return assess_option_outcome_measurement(
        candidate_id=uuid4(), candidate_identity_sha256="a" * 64,
        valuation_policy_sha256="b" * 64, measurement_type="30MIN",
        checkpoint_at=checkpoint,
        evaluated_at=checkpoint + timedelta(minutes=45, seconds=evaluated_late_seconds),
        required_contract_ids=(1, 2), observed_contract_ids=(1,),
        source_batch_id=None,
    )


def test_terminal_unavailable_identity_is_stable_across_later_retries():
    first = unavailable()
    later = first.model_copy()
    checkpoint = first.checkpoint_at
    rebuilt = assess_option_outcome_measurement(
        candidate_id=first.candidate_id,
        candidate_identity_sha256=first.candidate_identity_sha256,
        valuation_policy_sha256=first.valuation_policy_sha256,
        measurement_type=first.measurement_type, checkpoint_at=checkpoint,
        evaluated_at=checkpoint + timedelta(days=1),
        required_contract_ids=first.required_contract_ids,
        observed_contract_ids=first.observed_contract_ids,
        source_batch_id=None,
    )

    assert later == first
    assert rebuilt == first
    assert outcome_unavailable_id(rebuilt) == outcome_unavailable_id(first)


def test_unavailable_repository_rejects_pending_evidence_before_database_access():
    checkpoint = datetime(2026, 9, 18, 20, 0, tzinfo=UTC)
    pending = assess_option_outcome_measurement(
        candidate_id=uuid4(), candidate_identity_sha256="a" * 64,
        valuation_policy_sha256="b" * 64, measurement_type="30MIN",
        checkpoint_at=checkpoint, evaluated_at=checkpoint + timedelta(minutes=1),
        required_contract_ids=(1, 2), observed_contract_ids=(1,),
        source_batch_id=None,
    )
    repository = OptionOutcomeAvailabilityRepository(
        lambda: (_ for _ in ()).throw(
            AssertionError("pending evidence cannot open a database connection")
        )
    )

    with pytest.raises(ValueError, match="terminal unavailable"):
        repository.persist_unavailable((pending,))