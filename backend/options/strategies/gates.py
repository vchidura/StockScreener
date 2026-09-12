"""Structured execution gates.

`execution_eligibility` used to be hard-coded `None` while two capability strings were
appended to every candidate's reason codes. That recorded *that* a candidate was blocked
but not *which* checks ran, which passed, or which simply could not be evaluated on the
current entitlement.

Here each gate reports `PASS`, `FAIL` or `UNAVAILABLE` with evidence, and eligibility is
derived from the ledger rather than asserted. When quotes and a risk engine arrive, those
gates start returning real verdicts and eligibility follows automatically — no selector
changes.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping

from .domain import CandidateKind, ExecutionEligibility

GATE_LEDGER_VERSION = "gate_ledger_v2"


class ExecutionGate(str, Enum):
    CANDIDATE_KIND = "CANDIDATE_KIND"
    STRATEGY_CONTEXT = "STRATEGY_CONTEXT"
    EQUITY_DIRECTION = "EQUITY_DIRECTION"
    QUOTE_LIQUIDITY = "QUOTE_LIQUIDITY"
    RISK_ENGINE = "RISK_ENGINE"
    READ_ONLY_MODE = "READ_ONLY_MODE"


class GateVerdict(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class GateResult:
    gate: ExecutionGate
    verdict: GateVerdict
    reason_codes: tuple[str, ...] = ()
    evidence: Mapping[str, object] | None = None

    @property
    def blocking(self) -> bool:
        return self.verdict is not GateVerdict.PASS


@dataclass(frozen=True, slots=True)
class ExecutionGateLedger:
    ledger_version: str
    results: tuple[GateResult, ...]

    @property
    def blocking_gates(self) -> tuple[ExecutionGate, ...]:
        return tuple(result.gate for result in self.results if result.blocking)

    @property
    def unavailable_gates(self) -> tuple[ExecutionGate, ...]:
        return tuple(
            result.gate
            for result in self.results
            if result.verdict is GateVerdict.UNAVAILABLE
        )

    @property
    def reason_codes(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                code for result in self.results for code in result.reason_codes
            )
        )

    @property
    def eligibility(self) -> ExecutionEligibility | None:
        """Eligibility is earned by every gate passing, never asserted."""
        if self.blocking_gates:
            return None
        return ExecutionEligibility.LIVE_CANDIDATE

    def verdict_for(self, gate: ExecutionGate) -> GateVerdict | None:
        for result in self.results:
            if result.gate is gate:
                return result.verdict
        return None


def evaluate_execution_gates(
    *,
    candidate_kind: CandidateKind,
    context_reason_codes: tuple[str, ...] = (),
    equity_reason_codes: tuple[str, ...] = (),
    equity_direction_required: bool = False,
    equity_direction_available: bool = False,
    quotes_available: bool = False,
    risk_engine_available: bool = False,
    read_only: bool = True,
) -> ExecutionGateLedger:
    results: list[GateResult] = []

    if candidate_kind is CandidateKind.RESEARCH_ONLY:
        results.append(
            GateResult(
                ExecutionGate.CANDIDATE_KIND,
                GateVerdict.FAIL,
                ("RESEARCH_ONLY_CANDIDATE",),
                {"candidate_kind": candidate_kind.value},
            )
        )
    else:
        results.append(
            GateResult(
                ExecutionGate.CANDIDATE_KIND,
                GateVerdict.PASS,
                (),
                {"candidate_kind": candidate_kind.value},
            )
        )

    results.append(
        GateResult(
            ExecutionGate.STRATEGY_CONTEXT,
            GateVerdict.FAIL if context_reason_codes else GateVerdict.PASS,
            tuple(context_reason_codes),
        )
    )
    if equity_reason_codes:
        equity_direction_result = GateResult(
            ExecutionGate.EQUITY_DIRECTION,
            GateVerdict.FAIL,
            tuple(equity_reason_codes),
            {"required": equity_direction_required},
        )
    elif equity_direction_required and not equity_direction_available:
        equity_direction_result = GateResult(
            ExecutionGate.EQUITY_DIRECTION,
            GateVerdict.UNAVAILABLE,
            ("QUALIFIED_EQUITY_DIRECTION_UNAVAILABLE",),
            {"required": True},
        )
    else:
        equity_direction_result = GateResult(
            ExecutionGate.EQUITY_DIRECTION,
            GateVerdict.PASS,
            (),
            {
                "required": equity_direction_required,
                "available": equity_direction_available,
            },
        )
    results.append(equity_direction_result)
    results.append(
        GateResult(
            ExecutionGate.QUOTE_LIQUIDITY,
            GateVerdict.PASS if quotes_available else GateVerdict.UNAVAILABLE,
            () if quotes_available else ("QUOTE_LIQUIDITY_NOT_AVAILABLE",),
        )
    )
    results.append(
        GateResult(
            ExecutionGate.RISK_ENGINE,
            GateVerdict.PASS if risk_engine_available else GateVerdict.UNAVAILABLE,
            () if risk_engine_available else ("PAPER_RISK_ENGINE_NOT_IMPLEMENTED",),
        )
    )
    results.append(
        GateResult(
            ExecutionGate.READ_ONLY_MODE,
            GateVerdict.FAIL if read_only else GateVerdict.PASS,
            ("READ_ONLY_RESEARCH",) if read_only else (),
        )
    )
    return ExecutionGateLedger(GATE_LEDGER_VERSION, tuple(results))
