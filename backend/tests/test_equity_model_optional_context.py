"""Missing optional context reads NOT_APPLICABLE and does not stop an evaluation."""
from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from equity.model_detection.codec import decode_input
from equity.model_detection.contracts import Disposition, GateStatus
from equity.model_detection.engine import evaluate
from equity.model_detection.policies import (
    BreakoutPolicy,
    ContextOptionalBreakoutPolicy,
)

FIXTURE = Path(__file__).parent / "fixtures" / "equity_model_breakout_v1.json"
OPTIONAL = ContextOptionalBreakoutPolicy(optional_context=("event_risk",))
SECTOR_TOO = ContextOptionalBreakoutPolicy()


def _packet(policy, **context):
    packet = decode_input(json.loads(FIXTURE.read_text(encoding="utf-8")))
    return replace(
        packet,
        model=policy.identity,
        context=replace(packet.context, **context),
    )


def _gate(decision, name):
    return next(row for row in decision.gates if row.name == name)


def test_a_breakout_matches_with_no_event_data_and_says_so():
    decision = evaluate(_packet(OPTIONAL, event_risk=None), OPTIONAL)

    assert decision.disposition is Disposition.MATCH
    assert _gate(decision, "event").status is GateStatus.NOT_APPLICABLE
    assert _gate(decision, "sector").status is GateStatus.PASS


def test_a_known_event_still_blocks():
    decision = evaluate(_packet(OPTIONAL, event_risk=True), OPTIONAL)

    assert decision.disposition is Disposition.NO_MATCH
    assert _gate(decision, "event").status is GateStatus.FAIL
    assert "GATE_EVENT_FAILED" in decision.reason_codes


def test_known_clear_event_data_passes_as_before():
    decision = evaluate(_packet(OPTIONAL, event_risk=False), OPTIONAL)

    assert decision.disposition is Disposition.MATCH
    assert _gate(decision, "event").status is GateStatus.PASS


def test_sector_stays_required_unless_named():
    decision = evaluate(_packet(OPTIONAL, sector_trend=None), OPTIONAL)

    assert decision.disposition is Disposition.UNAVAILABLE
    assert decision.reason_codes == ("MISSING_SECTOR_CONTEXT",)


def test_sector_can_be_named_optional():
    decision = evaluate(
        _packet(SECTOR_TOO, sector_trend=None, event_risk=None),
        SECTOR_TOO,
    )

    assert decision.disposition is Disposition.MATCH
    assert _gate(decision, "sector").status is GateStatus.NOT_APPLICABLE
    assert _gate(decision, "event").status is GateStatus.NOT_APPLICABLE


def test_market_context_stays_required():
    decision = evaluate(_packet(SECTOR_TOO, market_trend=None), SECTOR_TOO)

    assert decision.disposition is Disposition.UNAVAILABLE
    assert "MISSING_MARKET_CONTEXT" in decision.reason_codes


def test_only_event_and_sector_can_be_optional():
    with pytest.raises(ValueError, match="optional"):
        ContextOptionalBreakoutPolicy(optional_context=("market_trend",))


def test_the_original_policy_still_refuses_and_keeps_its_hash():
    original = BreakoutPolicy()
    decision = evaluate(_packet(original, event_risk=None), original)

    assert decision.disposition is Disposition.UNAVAILABLE
    assert decision.reason_codes == ("MISSING_EVENT_CONTEXT",)
    assert original.policy_sha256 != OPTIONAL.policy_sha256
    assert original.optional_context_names == frozenset()
    assert OPTIONAL.version == "2"
