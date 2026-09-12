import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.strategies.domain import CandidateKind, ExecutionEligibility
from options.strategies.gates import (
    GATE_LEDGER_VERSION,
    ExecutionGate,
    GateVerdict,
    evaluate_execution_gates,
)
from options.repositories.strategies import OptionStrategyRepository


def _ledger(**overrides):
    values = {"candidate_kind": CandidateKind.MULTI_LEG}
    values.update(overrides)
    return evaluate_execution_gates(**values)


def test_developer_defaults_block_execution():
    ledger = _ledger()
    assert ledger.eligibility is None
    assert ledger.ledger_version == GATE_LEDGER_VERSION


def test_every_named_gate_is_reported():
    reported = {result.gate for result in _ledger().results}
    assert reported == set(ExecutionGate)


def test_quote_and_risk_gates_are_unavailable_not_failed():
    # The distinction matters: UNAVAILABLE means the check could not run on this
    # entitlement, FAIL means it ran and the candidate lost.
    ledger = _ledger()
    assert ledger.verdict_for(ExecutionGate.QUOTE_LIQUIDITY) is GateVerdict.UNAVAILABLE
    assert ledger.verdict_for(ExecutionGate.RISK_ENGINE) is GateVerdict.UNAVAILABLE
    assert set(ledger.unavailable_gates) == {
        ExecutionGate.QUOTE_LIQUIDITY,
        ExecutionGate.RISK_ENGINE,
    }


def test_read_only_mode_is_a_failed_gate():
    assert _ledger().verdict_for(ExecutionGate.READ_ONLY_MODE) is GateVerdict.FAIL
    assert _ledger(read_only=False).verdict_for(
        ExecutionGate.READ_ONLY_MODE
    ) is GateVerdict.PASS


def test_research_only_candidates_fail_the_kind_gate():
    ledger = _ledger(candidate_kind=CandidateKind.RESEARCH_ONLY)
    assert ledger.verdict_for(ExecutionGate.CANDIDATE_KIND) is GateVerdict.FAIL
    assert "RESEARCH_ONLY_CANDIDATE" in ledger.reason_codes


def test_legacy_capability_reason_codes_are_preserved():
    codes = _ledger().reason_codes
    assert "QUOTE_LIQUIDITY_NOT_AVAILABLE" in codes
    assert "PAPER_RISK_ENGINE_NOT_IMPLEMENTED" in codes


def test_context_and_equity_reasons_flow_into_their_own_gates():
    ledger = _ledger(
        context_reason_codes=("BAR_OBSERVATION_TIME_UNAVAILABLE",),
        equity_reason_codes=("EQUITY_DIRECTION_CONFLICT",),
    )
    assert ledger.verdict_for(ExecutionGate.STRATEGY_CONTEXT) is GateVerdict.FAIL
    assert ledger.verdict_for(ExecutionGate.EQUITY_DIRECTION) is GateVerdict.FAIL
    assert "BAR_OBSERVATION_TIME_UNAVAILABLE" in ledger.reason_codes
    assert "EQUITY_DIRECTION_CONFLICT" in ledger.reason_codes


def test_clean_context_passes_its_gates():
    ledger = _ledger()
    assert ledger.verdict_for(ExecutionGate.STRATEGY_CONTEXT) is GateVerdict.PASS
    assert ledger.verdict_for(ExecutionGate.EQUITY_DIRECTION) is GateVerdict.PASS


def test_directional_candidate_reports_equity_direction_unavailable():
    ledger = _ledger(equity_direction_required=True)

    assert ledger.verdict_for(ExecutionGate.EQUITY_DIRECTION) is GateVerdict.UNAVAILABLE
    assert "QUALIFIED_EQUITY_DIRECTION_UNAVAILABLE" in ledger.reason_codes
    assert ExecutionGate.EQUITY_DIRECTION in ledger.unavailable_gates


def test_available_qualified_direction_passes_directional_gate():
    ledger = _ledger(
        equity_direction_required=True,
        equity_direction_available=True,
    )

    assert ledger.verdict_for(ExecutionGate.EQUITY_DIRECTION) is GateVerdict.PASS


def test_eligibility_is_earned_only_when_every_gate_passes():
    ledger = _ledger(
        quotes_available=True, risk_engine_available=True, read_only=False
    )
    assert ledger.blocking_gates == ()
    assert ledger.eligibility is ExecutionEligibility.LIVE_CANDIDATE
    assert ledger.reason_codes == ()


def test_a_single_blocking_gate_withholds_eligibility():
    ledger = _ledger(
        quotes_available=True, risk_engine_available=True, read_only=True
    )
    assert ledger.blocking_gates == (ExecutionGate.READ_ONLY_MODE,)
    assert ledger.eligibility is None


def test_enabling_quotes_alone_does_not_grant_eligibility():
    # The Advanced upgrade flips this gate; the others must still be satisfied.
    ledger = _ledger(quotes_available=True)
    assert ledger.verdict_for(ExecutionGate.QUOTE_LIQUIDITY) is GateVerdict.PASS
    assert "QUOTE_LIQUIDITY_NOT_AVAILABLE" not in ledger.reason_codes
    assert ledger.eligibility is None


def test_reason_codes_are_deduplicated_and_ordered():
    ledger = _ledger(
        context_reason_codes=("SHARED", "CONTEXT_ONLY"),
        equity_reason_codes=("SHARED", "EQUITY_ONLY"),
    )
    codes = ledger.reason_codes
    assert codes.count("SHARED") == 1
    assert codes.index("CONTEXT_ONLY") < codes.index("EQUITY_ONLY")


def test_repository_persists_one_row_per_gate(monkeypatch):
    captured = {}

    def fake_execute_values(cursor, sql, values):
        captured["sql"] = sql
        captured["values"] = values

    monkeypatch.setattr(
        "options.repositories.strategies.execute_values", fake_execute_values
    )
    candidate_id = uuid4()
    evaluated_at = datetime(2026, 9, 7, 12, 0, tzinfo=timezone.utc)
    OptionStrategyRepository._persist_gate_ledger(
        object(), candidate_id, evaluated_at, _ledger()
    )

    assert "option_candidate_execution_gates" in captured["sql"]
    assert len(captured["values"]) == len(ExecutionGate)
    by_gate = {row[2]: row for row in captured["values"]}
    assert by_gate["QUOTE_LIQUIDITY"][3] == "UNAVAILABLE"
    assert by_gate["QUOTE_LIQUIDITY"][4] is True
    assert by_gate["CANDIDATE_KIND"][3] == "PASS"
    assert by_gate["CANDIDATE_KIND"][4] is False
