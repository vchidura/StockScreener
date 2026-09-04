from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from options.calendar import OptionExchangeCalendar
from options.outcomes import (
    delayed_proxy_commission_policy,
    evaluate_delayed_proxy_outcome,
    measurement_checkpoints,
)
from options.repositories.outcomes import OptionOutcomeRepository


@dataclass(frozen=True, slots=True)
class OptionOutcomeRunResult:
    candidates: int
    due_measurements: int
    available_measurements: int
    persisted: int
    pending: int


class OptionOutcomeService:
    def __init__(
        self,
        repository: OptionOutcomeRepository | None = None,
        *,
        calendar: OptionExchangeCalendar | None = None,
    ) -> None:
        self.repository = repository or OptionOutcomeRepository()
        self.calendar = calendar or OptionExchangeCalendar()
        self.policy = delayed_proxy_commission_policy()

    def mature(
        self,
        *,
        available_by: datetime,
        limit: int = 1000,
    ) -> OptionOutcomeRunResult:
        available_utc = _utc(available_by)
        candidates = self.repository.list_pending_candidates(
            valuation_policy_sha256=self.policy.policy_sha256,
            available_by=available_utc,
            limit=limit,
        )
        outcomes = []
        due = pending = 0
        for candidate in candidates:
            completed = set(candidate["completed_measurements"] or ())
            checkpoints = measurement_checkpoints(
                candidate["market_data_time"], calendar=self.calendar
            )
            for measurement_type, checkpoint in checkpoints.items():
                if measurement_type in completed or checkpoint > available_utc:
                    continue
                due += 1
                legs = self.repository.checkpoint_legs(
                    candidate["candidate_id"],
                    checkpoint_time=checkpoint,
                    available_by=available_utc,
                )
                if not legs:
                    pending += 1
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
        return OptionOutcomeRunResult(
            candidates=len(candidates),
            due_measurements=due,
            available_measurements=len(outcomes),
            persisted=persisted,
            pending=pending,
        )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("available_by must be timezone-aware")
    return value.astimezone(timezone.utc)