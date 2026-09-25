from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from options.calendar import OptionExchangeCalendar
from options.config import ValuationPolicy
from options.outcome_contracts import (
    OptionOutcomeAvailabilityPolicy,
    assess_option_outcome_measurement,
)
from options.outcomes import (
    CURRENT_MARK_MEASUREMENT,
    configured_valuation_policy,
    evaluate_delayed_proxy_outcome,
    measurement_checkpoints,
)
from options.repositories.outcomes import OptionOutcomeRepository
from options.repositories.outcome_availability import OptionOutcomeAvailabilityRepository


@dataclass(frozen=True, slots=True)
class OptionOutcomeRunResult:
    candidates: int
    due_measurements: int
    available_measurements: int
    persisted: int
    pending: int
    current_candidates: int = 0
    current_persisted: int = 0
    unavailable_measurements: int = 0
    unavailable_persisted: int = 0


class OptionOutcomeService:
    def __init__(
        self,
        repository: OptionOutcomeRepository | None = None,
        *,
        calendar: OptionExchangeCalendar | None = None,
        policy: ValuationPolicy | None = None,
        availability_repository: OptionOutcomeAvailabilityRepository | None = None,
        availability_policy: OptionOutcomeAvailabilityPolicy | None = None,
        availability_evidence_enabled: bool = False,
    ) -> None:
        self.repository = repository or OptionOutcomeRepository()
        self.calendar = calendar or OptionExchangeCalendar()
        self.policy = policy or configured_valuation_policy()
        self.availability_repository = (
            availability_repository or OptionOutcomeAvailabilityRepository()
        )
        self.availability_policy = (
            availability_policy or OptionOutcomeAvailabilityPolicy()
        )
        self.availability_evidence_enabled = availability_evidence_enabled

    def mature(
        self,
        *,
        available_by: datetime,
        limit: int = 1000,
    ) -> OptionOutcomeRunResult:
        available_utc = _utc(available_by)
        candidate_limit = (
            min(limit, self.availability_policy.maximum_candidates_per_run)
            if self.availability_evidence_enabled else limit
        )
        pending_arguments = {}
        if self.availability_evidence_enabled:
            pending_arguments.update(
                availability_policy_sha256=self.availability_policy.sha256,
                calendar=self.calendar,
            )
        candidates = self.repository.list_pending_candidates(
            valuation_policy_sha256=self.policy.policy_sha256,
            available_by=available_utc,
            limit=candidate_limit,
            include_incomplete_packages=self.availability_evidence_enabled,
            **pending_arguments,
        )
        outcomes = []
        unavailable_assessments = []
        due = pending = 0
        due_requests = []
        for candidate in candidates:
            completed = set(candidate["completed_measurements"] or ()) | set(
                candidate.get("unavailable_measurements") or ()
            )
            checkpoints = measurement_checkpoints(
                candidate["market_data_time"], calendar=self.calendar
            )
            for measurement_type, checkpoint in checkpoints.items():
                if measurement_type in completed or checkpoint > available_utc:
                    continue
                due += 1
                read_available_by = available_utc
                if self.availability_evidence_enabled:
                    read_available_by = min(
                        available_utc, self.availability_policy.deadline(checkpoint))
                due_requests.append((candidate, measurement_type, checkpoint, read_available_by))
        maximum_mark_lag = timedelta(
            seconds=self.availability_policy.maximum_mark_lag_seconds)
        bulk_checkpoint_legs = None
        if hasattr(self.repository, "checkpoint_legs_bulk"):
            bulk_checkpoint_legs = self.repository.checkpoint_legs_bulk(
                [(candidate["candidate_id"], checkpoint, read_available_by)
                 for candidate, _, checkpoint, read_available_by in due_requests],
                valuation_policy_sha256=self.policy.policy_sha256,
                maximum_mark_lag=maximum_mark_lag,
            )
        for candidate, measurement_type, checkpoint, read_available_by in due_requests:
                legs = (bulk_checkpoint_legs.get((candidate["candidate_id"], checkpoint), ())
                    if bulk_checkpoint_legs is not None else self.repository.checkpoint_legs(
                        candidate["candidate_id"], checkpoint_time=checkpoint,
                        available_by=read_available_by,
                        valuation_policy_sha256=self.policy.policy_sha256,
                        **({"maximum_mark_lag": maximum_mark_lag}
                           if self.availability_evidence_enabled else {})))
                if not legs and not self.availability_evidence_enabled:
                    pending += 1
                    continue
                required_contract_ids = tuple(candidate["required_contract_ids"])
                observed_contract_ids = tuple(leg.contract_id for leg in legs)
                source_batch_id = legs[0].source_batch_id if legs else None
                availability = assess_option_outcome_measurement(
                    candidate_id=candidate["candidate_id"],
                    candidate_identity_sha256=candidate["candidate_identity"],
                    valuation_policy_sha256=self.policy.policy_sha256,
                    measurement_type=measurement_type,
                    checkpoint_at=checkpoint,
                    evaluated_at=available_utc,
                    required_contract_ids=required_contract_ids,
                    observed_contract_ids=observed_contract_ids,
                    source_batch_id=source_batch_id,
                    policy=self.availability_policy,
                )
                if availability.status == "PENDING":
                    pending += 1
                    continue
                if availability.status == "UNAVAILABLE":
                    unavailable_assessments.append(availability)
                    continue
                outcomes.append(evaluate_delayed_proxy_outcome(
                    candidate_id=candidate["candidate_id"],
                    event_id=candidate.get("event_id"),
                    measurement_type=measurement_type,
                    market_time=max(row.source_market_time for row in legs),
                    observed_time=max(row.source_observed_time for row in legs),
                    capital_at_risk=candidate["capital_at_risk"],
                    legs=legs,
                    policy=self.policy,
                ))
        persisted = self.repository.persist_decay_outcomes(outcomes)
        unavailable_persisted = (
            self.availability_repository.persist_unavailable(
                unavailable_assessments
            ).inserted
            if unavailable_assessments and self.availability_evidence_enabled else 0
        )
        current_outcomes = []
        current_persisted = 0
        if self.repository.current_marks_available():
            self.repository.delete_non_causal_current_marks()
            current_candidates = self.repository.list_current_candidates(
                valuation_policy_sha256=self.policy.policy_sha256,
                available_by=available_utc,
                limit=limit,
            )
            bulk_current_legs = None
            if hasattr(self.repository, "current_mark_legs_bulk"):
                bulk_current_legs = {}
                candidate_ids = [candidate["candidate_id"] for candidate in current_candidates]
                for offset in range(0, len(candidate_ids), 1000):
                    bulk_current_legs.update(self.repository.current_mark_legs_bulk(
                        candidate_ids[offset:offset + 1000],
                        available_by=available_utc,
                        valuation_policy_sha256=self.policy.policy_sha256,
                    ))
            for candidate in current_candidates:
                legs = (bulk_current_legs.get(candidate["candidate_id"], ())
                    if bulk_current_legs is not None else self.repository.current_mark_legs(
                        candidate["candidate_id"], available_by=available_utc,
                        valuation_policy_sha256=self.policy.policy_sha256))
                if not legs:
                    continue
                current_outcomes.append(evaluate_delayed_proxy_outcome(
                    candidate_id=candidate["candidate_id"],
                    event_id=candidate.get("event_id"),
                    measurement_type=CURRENT_MARK_MEASUREMENT,
                    market_time=max(row.source_market_time for row in legs),
                    observed_time=max(row.source_observed_time for row in legs),
                    capital_at_risk=candidate["capital_at_risk"], legs=legs,
                    policy=self.policy,
                ))
            current_persisted = self.repository.persist_current_marks(current_outcomes)
        return OptionOutcomeRunResult(
            candidates=len(candidates),
            due_measurements=due,
            available_measurements=len(outcomes),
            persisted=persisted,
            pending=pending,
            current_candidates=len(current_outcomes),
            current_persisted=current_persisted,
            unavailable_measurements=len(unavailable_assessments),
            unavailable_persisted=unavailable_persisted,
        )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("available_by must be timezone-aware")
    return value.astimezone(timezone.utc)