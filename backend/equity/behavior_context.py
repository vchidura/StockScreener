"""Optional descriptive rank attachment; preserves the original behavior profile."""
from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, Field, model_validator

from .behavior import Contract, Sha256, StockBehaviorSnapshot
from .domain import DecisionWatermark
from .stock_context import VERSION, stamp


class DailyRankReference(Contract):
    cohort_snapshot_id: UUID
    cohort_generation: Sha256
    cohort_payload_sha256: Sha256
    source_publication_id: UUID
    universe: str = Field(min_length=1, max_length=100)
    expected_members: int = Field(strict=True, gt=0, le=1000)
    eligible_ranked: int = Field(strict=True, gt=0, le=1000)
    percentile: float = Field(ge=0, le=1)
    momentum: float
    market_time: AwareDatetime
    source_cutoff: AwareDatetime
    available_at: AwareDatetime
    definition: Literal["RAW_12_MINUS_1_MOMENTUM_PERCENTILE"] = "RAW_12_MINUS_1_MOMENTUM_PERCENTILE"
    price_basis: Literal["RAW_ACTION_GATED"] = "RAW_ACTION_GATED"

    @model_validator(mode="after")
    def causal_cohort(self):
        if self.eligible_ranked > self.expected_members or not self.market_time <= self.source_cutoff <= self.available_at:
            raise ValueError("rank cohort coverage or clocks are invalid")
        return self


class StockBehaviorEquityContext(Contract):
    schema_version: Literal["stock_behavior_equity_attachment_v1"] = "stock_behavior_equity_attachment_v1"
    behavior_snapshot_id: UUID
    behavior_payload_sha256: Sha256
    security_id: UUID
    ticker: str = Field(min_length=1, max_length=32)
    market_cutoff: AwareDatetime
    decision_at: AwareDatetime
    original_data_status: Literal["READY", "PARTIAL", "UNAVAILABLE"]
    rank_status: Literal["READY", "STALE", "UNAVAILABLE"]
    rank: DailyRankReference | None = None
    reason: str | None = None
    classification: Literal["DESCRIPTIVE_ONLY"] = "DESCRIPTIVE_ONLY"
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_attachment(self):
        if self.market_cutoff > self.decision_at:
            raise ValueError("market cutoff exceeds observation cutoff")
        if self.rank_status == "READY":
            if self.rank is None or self.reason or self.rank.market_time > self.market_cutoff or self.rank.available_at > self.decision_at:
                raise ValueError("ready rank must be causally bound to its cohort")
        elif self.rank is not None or not self.reason:
            raise ValueError("unavailable rank requires an explicit reason and no value")
        return self


def attach_daily_rank(snapshot: StockBehaviorSnapshot, equity_context, decision: DecisionWatermark):
    if snapshot.market_time > decision.market_time or snapshot.available_at > decision.observed_time:
        raise ValueError("behavior snapshot not available at attachment cutoff")
    if equity_context["schema"] != VERSION or stamp(equity_context["as_of"]) > decision.observed_time:
        raise ValueError("equity context contract or cutoff mismatch")
    rows = [row for row in equity_context["rows"] if row["security_id"] == str(snapshot.security_id)]
    if len(rows) > 1 or rows and rows[0]["ticker"] != snapshot.ticker:
        raise ValueError("rank security identity mismatch")
    cohort = equity_context.get("cohort")
    rank, status, reason = None, "UNAVAILABLE", "SECURITY_NOT_IN_RANK_COHORT"
    if rows and cohort:
        row = rows[0]
        status = "STALE" if cohort["status"] == "STALE" or row["daily"]["status"] == "STALE" else row["daily"]["status"]
        reason = "RANK_STALE" if status == "STALE" else "RANK_INPUT_UNAVAILABLE"
        if status == "READY":
            if cohort["ranking"] != "RAW_12_MINUS_1_MOMENTUM_PERCENTILE":
                raise ValueError("unsupported daily rank definition")
            rank = DailyRankReference(cohort_snapshot_id=cohort["snapshot_id"], cohort_generation=cohort["generation"],
                cohort_payload_sha256=cohort["payload_sha256"], source_publication_id=cohort["source_publication_id"],
                universe=cohort["universe"], expected_members=cohort["expected_members"], eligible_ranked=cohort["eligible_ranked"],
                percentile=row["daily"]["percentile"], momentum=row["daily"]["momentum"], market_time=cohort["market_time"],
                source_cutoff=cohort["source_cutoff"], available_at=cohort["available_at"])
            reason = None
    return StockBehaviorEquityContext(behavior_snapshot_id=snapshot.snapshot_id, behavior_payload_sha256=snapshot.sha256,
        security_id=snapshot.security_id, ticker=snapshot.ticker, market_cutoff=decision.market_time,
        decision_at=decision.observed_time, original_data_status=snapshot.assess_at(decision.observed_time).data_status,
        rank_status=status, rank=rank, reason=reason)