"""Frozen research record -> canonical decision -> serializable artifact.

The caller supplies normalized records and owns any eventual artifact writes.
No replay orchestration or simulated outcomes are imported here.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping, Sequence, cast
from uuid import UUID

from .codec import decode_input
from .contracts import (
    AdjustmentBasis, AdjustmentLineage, CanonicalInput, CompletedBar, Decision,
    ObservationClocks, SourceIdentity, canonical_json, canonical_sha256,
    canonical_value, freeze, nonempty, sha256, utc,
)
from .engine import ModelEvaluator, evaluate
from .policies import BreakoutPolicy


@dataclass(frozen=True, slots=True)
class FrozenResearchRecord:
    run_id: str
    input_payload: Mapping[str, Any]
    payload_sha256: str

    def __post_init__(self) -> None:
        nonempty(self.run_id)
        sha256(self.payload_sha256)
        object.__setattr__(self, "input_payload", cast(Mapping[str, Any], freeze(self.input_payload)))
        if canonical_sha256(self.input_payload) != self.payload_sha256:
            raise ValueError("frozen research payload hash mismatch")

    @classmethod
    def from_research_rows(cls, run_id: str, rows: Sequence[Mapping[str, Any]], *,
                           input_context: CanonicalInput) -> FrozenResearchRecord:
        """Adapt extracted legacy flat bar rows, using explicitly frozen metadata.

        ``input_context`` supplies security/action/model/context/clock/prior-state
        contracts; its bars are replaced by ``rows``. Each row must pin a
        revision, payload hash and adjustment/action lineage. ``visible_at`` is the research track's frozen
        availability, and receipt is max(system_observed_at, created_at). The
        adapter never reconstructs a provider delay or guesses an adjustment.
        Legacy binary prices are converted using their retained decimal spelling.
        """
        bars = []
        for row in rows:
            security_id = UUID(str(row["security_id"]))
            if row["ticker"] != input_context.security.ticker:
                raise ValueError("legacy ticker/security identity mismatch")
            revision = str(row["revision_id"])
            source = SourceIdentity(security_id, str(row.get("source_id", revision)),
                                    str(row.get("source_revision", revision)), row["payload_sha256"])
            def clock(name: str) -> datetime:
                value = row[name]
                return utc(value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00")))
            observed, created = clock("system_observed_at"), clock("created_at")
            clocks = ObservationClocks(clock("bar_start"), clock("bar_end"), clock("visible_at"), max(observed, created))
            prices = []
            for name in ("open", "high", "low", "close", "volume"):
                value = row[name]
                if type(value) not in (str, int, float, Decimal):
                    raise ValueError("unsupported legacy numeric type")
                prices.append(Decimal(str(value)))
            adjustment = AdjustmentLineage(AdjustmentBasis(row["adjustment_basis"]), row["action_sha256"], row["adjustment_version"])
            bars.append(CompletedBar(source, clocks, adjustment, row["interval"], *prices,
                                     completed=row["completed"]))
        packet = replace(input_context, bars=tuple(bars))
        payload = cast(Mapping[str, Any], canonical_value(packet))
        return cls(run_id, payload, canonical_sha256(payload))


@dataclass(frozen=True, slots=True)
class LegacyEvaluationArtifact:
    run_id: str
    source_sha256: str
    canonical_input: CanonicalInput
    decision: Decision

    def to_json(self) -> str:
        return canonical_json({"schema": "equity_legacy_evaluation_v1", "run_id": self.run_id,
                               "source_sha256": self.source_sha256, "canonical_input": self.canonical_input,
                               "decision": self.decision, "decision_sha256": self.decision.decision_sha256,
                               "publication_permission": False, "execution_permission": False})


def run_legacy(record: FrozenResearchRecord, policy: BreakoutPolicy, *,
               evaluator: ModelEvaluator = evaluate) -> LegacyEvaluationArtifact:
    packet = decode_input(record.input_payload)
    return LegacyEvaluationArtifact(record.run_id, record.payload_sha256, packet, evaluator(packet, policy))
