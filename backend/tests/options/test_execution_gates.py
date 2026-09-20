import sys
import copy
from dataclasses import FrozenInstanceError
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from decimal import Decimal
from uuid import uuid4
from unittest.mock import MagicMock

import pytest

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
from options.alert_qualification import (
    AlertCapability, AlertInput, AlertInputEvidence, alert_requirements,
    alert_qualification_policy, qualification_policy_sha256, qualify_option_alert,
    retained_alert_evidence, retained_leg, retained_open_interest_evidence, retained_gamma_evidence, retained_event_horizon, validate_alert_package,
)
from options.strategies.domain import StructureType
from options.strategies.registry import STRATEGY_REGISTRY
from options.alert_plans import AlertManagementPolicy, freeze_indicative_alert_plan, preview_indicative_alert_plan
from options.repositories.alert_publications import (
    AlertPublicationEvent, OptionAlertPublicationRepository, publication_exposure_key,
    validate_publication_transition, validated_plan_payload,
)


def _ledger(**overrides):
    values = {"candidate_kind": CandidateKind.MULTI_LEG}
    values.update(overrides)
    return evaluate_execution_gates(**values)


DECISION_AT = datetime(2026, 9, 16, 15, 30, tzinfo=timezone.utc)


def _passing_alert_evidence():
    return {field: AlertInputEvidence(GateVerdict.PASS, (f"retained:{field.value}",), DECISION_AT) for field in AlertInput}


def _capability(result, capability):
    return next(row for row in result["assessments"] if row["capability"] == capability.value)


def test_alert_qualification_all_registered_structures_have_versioned_requirements():
    policy = alert_qualification_policy()
    expected = sum(len(item.allowed_structure_types) for item in STRATEGY_REGISTRY) * len(AlertCapability)
    assert len(policy["contracts"]) == expected
    assert policy["version"] == "option_alert_qualification_v1"
    assert qualification_policy_sha256() == "39ad9b15c6aedf643e2b061e502d13c588d2e24b5ec8fbef12e7d5de2e3f47da"
    for registration in STRATEGY_REGISTRY:
        for structure in registration.allowed_structure_types:
            result = qualify_option_alert(registration.strategy_name, structure, {}, DECISION_AT)
            assert _capability(result, AlertCapability.OBSERVATION)["status"] == "UNAVAILABLE"
            assert result["execution_permission"] is False


def test_activity_observation_does_not_need_quotes_but_cannot_become_a_package():
    evidence = _passing_alert_evidence()
    for field in (AlertInput.QUOTE_LIQUIDITY, AlertInput.EVENT_HORIZON, AlertInput.PAPER_RISK):
        del evidence[field]
    result = qualify_option_alert("VOLUME_OI_ANOMALY", StructureType.VOLUME_OI_ANOMALY, evidence, DECISION_AT)
    assert _capability(result, AlertCapability.OBSERVATION)["status"] == "SATISFIED"
    for capability in (AlertCapability.INDICATIVE, AlertCapability.QUOTE_PAPER, AlertCapability.EXECUTION):
        assert _capability(result, capability)["status"] == "NOT_APPLICABLE"


def test_directional_and_neutral_structures_require_different_context():
    directional = alert_requirements("SPREAD_RANGE_LOCATOR", StructureType.PUT_CREDIT_VERTICAL, AlertCapability.INDICATIVE)
    neutral = alert_requirements("SPREAD_RANGE_LOCATOR", StructureType.IRON_CONDOR, AlertCapability.INDICATIVE)
    assert AlertInput.DIRECTION in directional.required
    assert AlertInput.RANGE_CONTEXT not in directional.required
    assert AlertInput.RANGE_CONTEXT in neutral.required
    assert AlertInput.DIRECTION not in neutral.required


def test_indicative_economics_do_not_assert_paper_fill_or_execution_permission():
    evidence = _passing_alert_evidence()
    del evidence[AlertInput.QUOTE_LIQUIDITY]
    evidence[AlertInput.EXECUTION_LEDGER] = AlertInputEvidence(GateVerdict.FAIL, reasons=("READ_ONLY_RESEARCH",))
    result = qualify_option_alert("INCOME_WHEEL", StructureType.CASH_SECURED_PUT, evidence, DECISION_AT)
    assert _capability(result, AlertCapability.INDICATIVE)["status"] == "SATISFIED"
    assert _capability(result, AlertCapability.QUOTE_PAPER)["status"] == "UNAVAILABLE"
    assert _capability(result, AlertCapability.EXECUTION)["status"] == "BLOCKED"
    assert result["probability"] is None and result["execution_permission"] is False


def test_zero_dte_gamma_confirmation_and_realtime_inputs_are_explicit():
    indicative = alert_requirements("ZERO_DTE_GAMMA_SQUEEZE", StructureType.LONG_CALL, AlertCapability.INDICATIVE)
    paper = alert_requirements("ZERO_DTE_GAMMA_SQUEEZE", StructureType.LONG_CALL, AlertCapability.QUOTE_PAPER)
    assert AlertInput.GAMMA_CONTEXT in indicative.required
    assert AlertInput.REALTIME_INPUTS not in indicative.required
    assert AlertInput.REALTIME_INPUTS in paper.required
    assert AlertInput.ASSIGNMENT_POLICY not in paper.required


def test_missing_or_future_evidence_never_satisfies_required_input():
    evidence = _passing_alert_evidence()
    evidence[AlertInput.VALUATION] = AlertInputEvidence(GateVerdict.PASS, ("later-mark",), DECISION_AT + timedelta(seconds=1))
    result = qualify_option_alert("DIRECTIONAL_LONG_PREMIUM", StructureType.LONG_CALL, evidence, DECISION_AT)
    observation = _capability(result, AlertCapability.OBSERVATION)
    assert observation["status"] == "UNAVAILABLE"
    check = next(row for row in observation["checks"] if row["input"] == "VALUATION")
    assert check["reasons"] == ["EVIDENCE_AFTER_DECISION_CUTOFF"]
    assert check["source_ids"] == []


def test_passing_qualification_evidence_requires_causal_provenance():
    with pytest.raises(ValueError, match="source IDs"):
        AlertInputEvidence(GateVerdict.PASS)
    with pytest.raises(ValueError, match="timezone-aware"):
        AlertInputEvidence(GateVerdict.PASS, ("row",), DECISION_AT.replace(tzinfo=None))
    with pytest.raises(ValueError, match="registered pair"):
        qualify_option_alert("INCOME_WHEEL", StructureType.LONG_CALL, {}, DECISION_AT)


def test_alert_qualification_never_mutates_execution_ledger_or_evidence():
    ledger = _ledger()
    evidence = _passing_alert_evidence()
    before = dict(evidence)
    result = qualify_option_alert("INCOME_WHEEL", StructureType.CASH_SECURED_PUT, evidence, DECISION_AT)
    assert all(row["status"] == "SATISFIED" for row in result["assessments"])
    assert result["execution_permission"] is False
    assert evidence == before
    assert ledger == _ledger()
    assert GATE_LEDGER_VERSION == "gate_ledger_v2"


def retained_wheel_detail():
    candidate_id, snapshot_id, matrix_id = str(uuid4()), str(uuid4()), str(uuid4())
    source_time = DECISION_AT - timedelta(minutes=15)
    expiry = (DECISION_AT + timedelta(days=30)).date().isoformat()
    leg = {
        "leg_index": 0, "snapshot_id": snapshot_id, "contract_id": 1,
        "contract_ticker": "O:SPY261016P00100000", "side": "SELL", "ratio": 1,
        "multiplier": 100, "expiration_date": expiry, "strike": "100", "contract_type": "PUT",
        "spot": "105", "model_mark": "2", "time_to_expiration_years": 30 / 365,
        "risk_free_rate": .04, "dividend_yield": .01, "local_iv": .3,
        "local_delta": -.3, "local_gamma": .03, "local_theta_per_day": -.1,
        "local_vega_per_vol_point": .05, "local_rho_per_rate_point": -.01,
        "source_market_time": source_time, "mark_source": "DEVELOPER_ALIGNED_AGG_CLOSE",
        "model_version": "black_scholes_european_v1", "quality_flags": [],
        "valuation_policy_version": "option_valuation_v1", "valuation_policy_sha256": "a" * 64,
    }
    snapshot = {
        **leg, "underlying": "SPY", "shares_per_contract": 100, "exercise_style": "AMERICAN",
        "market_data_time": source_time, "first_observed_at": DECISION_AT,
        "revised_observed_at": None, "expiration_cutoff": DECISION_AT + timedelta(days=30),
        "iv_converged": True, "day_volume": 400, "open_interest": 100,
    }
    return {
        "candidate": {
            "candidate_id": candidate_id, "matrix_id": matrix_id, "decision_evidence_id": str(uuid4()),
            "identity_sha256": "c" * 64, "underlying": "SPY", "strategy_name": "INCOME_WHEEL",
            "strategy_version": "phase2_v4", "policy_sha256": "b" * 64,
            "model_version": "black_scholes_european_v1", "structure_type": "CASH_SECURED_PUT",
            "candidate_kind": "SINGLE_CONTRACT", "status": "SELECTED", "observed_time": DECISION_AT,
            "market_data_time": source_time, "valid_until": DECISION_AT + timedelta(minutes=5),
            "net_premium": "200", "maximum_profit": "200", "maximum_loss": "9800",
            "capital_at_risk": "10000", "collateral_required": "10000", "breakevens": ["98"],
            "management_policy": {"take_profit_fraction": .5, "stop_loss_multiple": 2, "exit_dte": 21},
            "management_policy_version": "phase2_v4", "decision_context": {}, "primary_evidence": {},
        }, "legs": [leg], "source_snapshots": [snapshot], "execution_gates": [],
        "market_event_evidence": [], "event_coverage_evidence": [], "scenarios": [],
    }


def test_retained_candidate_adapter_recomputes_payoff_without_promoting_paper():
    detail = retained_wheel_detail()
    before = copy.deepcopy(detail)
    evidence = retained_alert_evidence(detail, DECISION_AT)
    result = qualify_option_alert("INCOME_WHEEL", StructureType.CASH_SECURED_PUT, evidence, DECISION_AT)
    assert _capability(result, AlertCapability.INDICATIVE)["status"] == "SATISFIED"
    assert _capability(result, AlertCapability.QUOTE_PAPER)["status"] == "UNAVAILABLE"
    assert evidence[AlertInput.EVENT_HORIZON].verdict is GateVerdict.UNAVAILABLE
    assert evidence[AlertInput.ENTRY_WINDOW].verdict is GateVerdict.UNAVAILABLE
    assert detail == before


@pytest.mark.parametrize("mutation", ["missing_snapshot", "wrong_contract", "late_revision", "wrong_payoff", "wrong_ratio", "missing_policy", "mixed_mark", "wrong_mark_source", "wrong_breakeven", "missing_collateral", "wrong_kind"])
def test_retained_package_defects_prevent_indicative_qualification(mutation):
    detail = retained_wheel_detail()
    if mutation == "missing_snapshot": detail["source_snapshots"] = []
    if mutation == "wrong_contract": detail["source_snapshots"][0]["contract_id"] = 2
    if mutation == "late_revision": detail["source_snapshots"][0]["revised_observed_at"] = DECISION_AT + timedelta(seconds=1)
    if mutation == "wrong_payoff": detail["candidate"]["maximum_loss"] = "50"
    if mutation == "wrong_ratio": detail["legs"][0]["ratio"] = 2
    if mutation == "missing_policy": detail["legs"][0]["valuation_policy_sha256"] = None
    if mutation == "mixed_mark": detail["source_snapshots"][0]["model_mark"] = "3"
    if mutation == "wrong_mark_source": detail["source_snapshots"][0]["mark_source"] = "DISPLAY_DAY_CLOSE"
    if mutation == "wrong_breakeven": detail["candidate"]["breakevens"] = ["105"]
    if mutation == "missing_collateral": detail["candidate"]["collateral_required"] = None
    if mutation == "wrong_kind": detail["candidate"]["candidate_kind"] = "MULTI_LEG"
    evidence = retained_alert_evidence(detail, DECISION_AT)
    result = qualify_option_alert("INCOME_WHEEL", StructureType.CASH_SECURED_PUT, evidence, DECISION_AT)
    assert _capability(result, AlertCapability.INDICATIVE)["status"] != "SATISFIED"


def test_historical_clear_event_and_quotes_do_not_substitute_for_horizon_and_fill_checks():
    detail = retained_wheel_detail()
    detail["candidate"]["decision_context"] = {"earnings_blackout_state": "CLEAR", "fed_blackout_state": "CLEAR"}
    detail["execution_gates"] = [{"gate_name": "QUOTE_LIQUIDITY", "verdict": "PASS"}]
    evidence = retained_alert_evidence(detail, DECISION_AT + timedelta(hours=1))
    assert evidence[AlertInput.EVENT_HORIZON].verdict is GateVerdict.UNAVAILABLE
    assert AlertInput.QUOTE_LIQUIDITY not in evidence
    assert evidence[AlertInput.ENTRY_WINDOW].verdict is GateVerdict.FAIL


def test_registered_structure_validation_rejects_legged_naked_or_mislabeled_packages():
    detail = retained_wheel_detail()
    leg = retained_leg(detail["legs"][0])
    validate_alert_package(StructureType.CASH_SECURED_PUT, (leg,))
    with pytest.raises(ValueError, match="named structure"):
        validate_alert_package(StructureType.PUT_CREDIT_VERTICAL, (leg,))
    with pytest.raises(ValueError, match="distinct listed"):
        validate_alert_package(StructureType.LONG_CALL, ())


def test_equal_decimal_values_with_different_scale_keep_identity_valid():
    detail = retained_wheel_detail()
    detail["source_snapshots"][0]["strike"] = Decimal("100.00000000")
    assert retained_alert_evidence(detail, DECISION_AT)[AlertInput.CONTRACT_IDENTITY].verdict is GateVerdict.PASS


def retained_oi_detail():
    detail = retained_wheel_detail()
    detail["candidate"].update({"strategy_name": "VOLUME_OI_ANOMALY", "structure_type": "VOLUME_OI_ANOMALY", "candidate_kind": "RESEARCH_ONLY", "source_contract_id": 1,
                                "primary_evidence": {"contract_id": 1, "day_volume": 400, "open_interest": 100}})
    detail["legs"] = []
    detail["open_interest_evidence"] = [{
        "contract_id": 1, "underlying": "SPY", "settlement_session": "2026-09-15",
        "open_interest": 100, "open_interest_source": "PROVIDER_CHAIN_SNAPSHOT",
        "open_interest_observed_at": DECISION_AT - timedelta(hours=1),
        "open_interest_observed_session": "2026-09-16", "open_interest_revision_count": 0,
        "open_interest_revised_value": None, "open_interest_revised_observed_at": None,
    }]
    return detail


def test_dated_oi_enables_only_causal_activity_observation():
    detail = retained_oi_detail()
    original = copy.deepcopy(detail)
    evidence = retained_alert_evidence(detail, DECISION_AT)
    assert evidence[AlertInput.OPEN_INTEREST].verdict is GateVerdict.PASS
    result = qualify_option_alert("VOLUME_OI_ANOMALY", StructureType.VOLUME_OI_ANOMALY, evidence, DECISION_AT)
    assert _capability(result, AlertCapability.OBSERVATION)["status"] == "SATISFIED"
    assert _capability(result, AlertCapability.INDICATIVE)["status"] == "NOT_APPLICABLE"
    assert detail == original


@pytest.mark.parametrize(("revisions", "revised_offset", "expected"), [(1, 1, "PASS"), (2, 1, "UNAVAILABLE"), (1, -1, "FAIL"), (2, -1, "FAIL")])
def test_dated_oi_revision_cutoff_never_substitutes_later_or_missing_value(revisions, revised_offset, expected):
    detail = retained_oi_detail()
    detail["open_interest_evidence"][0].update({"open_interest_revision_count": revisions, "open_interest_revised_value": 200,
                                             "open_interest_revised_observed_at": DECISION_AT + timedelta(seconds=revised_offset)})
    assert retained_open_interest_evidence(detail).verdict.value == expected


def test_dated_oi_latest_known_revision_matches_original_snapshot():
    detail = retained_oi_detail()
    detail["open_interest_evidence"][0].update({"open_interest": 80, "open_interest_revision_count": 2, "open_interest_revised_value": 100, "open_interest_revised_observed_at": DECISION_AT - timedelta(seconds=1)})
    result = retained_open_interest_evidence(detail)
    assert result.verdict is GateVerdict.PASS
    assert "revision-2" in result.source_ids[0]


@pytest.mark.parametrize("mutation", ["future", "stale", "wrong_contract", "wrong_underlying", "wrong_detector", "zero", "duplicate"])
def test_dated_oi_invalid_lineage_cannot_pass(mutation):
    detail = retained_oi_detail()
    row = detail["open_interest_evidence"][0]
    if mutation == "future": row["open_interest_observed_at"] = DECISION_AT + timedelta(seconds=1)
    if mutation == "stale": row["settlement_session"] = "2026-09-14"
    if mutation == "wrong_contract": row["contract_id"] = 2
    if mutation == "wrong_underlying": row["underlying"] = "QQQ"
    if mutation == "wrong_detector": detail["candidate"]["primary_evidence"]["open_interest"] = 99
    if mutation == "zero":
        row["open_interest"] = 0
        detail["source_snapshots"][0]["open_interest"] = 0
        detail["candidate"]["primary_evidence"]["open_interest"] = 0
    if mutation == "duplicate": detail["open_interest_evidence"].append(dict(row))
    assert retained_open_interest_evidence(detail).verdict is not GateVerdict.PASS


def test_spread_oi_needs_full_matrix_not_only_selected_legs():
    detail = retained_oi_detail()
    detail["candidate"]["strategy_name"] = "SPREAD_RANGE_LOCATOR"
    assert retained_open_interest_evidence(detail).reasons == ("MATRIX_WIDE_OI_LINEAGE_REQUIRED",)


def retained_gamma_detail():
    detail = retained_wheel_detail()
    detail["legs"][0]["expiration_date"] = DECISION_AT.date().isoformat()
    detail["candidate"].update({"strategy_name": "ZERO_DTE_GAMMA_SQUEEZE", "structure_type": "LONG_PUT",
        "primary_evidence": {"gamma_policy_sha256": "f" * 64, "gamma_scope": "ZERO_DTE", "gamma_wall_strike": "100",
                             "dealer_convention": "DEALER_LONG_CALLS_SHORT_PUTS", "gamma_regime": "NEGATIVE_GAMMA"}})
    detail["gamma_evidence"] = [{
        "gamma_profile_id": str(uuid4()), "matrix_id": detail["candidate"]["matrix_id"], "underlying": "SPY", "scope": "ZERO_DTE",
        "gamma_policy_sha256": "f" * 64, "first_observed_at": DECISION_AT, "market_data_time": DECISION_AT - timedelta(minutes=15),
        "shares_per_contract": 100, "contributing_contract_count": 100, "eligible_contract_count": 105,
        "coverage_fraction": .95, "quality_reasons": [], "dealer_convention": "DEALER_LONG_CALLS_SHORT_PUTS", "regime_at_spot": "NEGATIVE_GAMMA",
    }]
    return detail


def test_gamma_requires_exact_matrix_policy_and_retains_model_caveats():
    result = retained_gamma_evidence(retained_gamma_detail())
    assert result.verdict is GateVerdict.PASS
    assert "MODELED_DEALER_CONVENTION_NOT_OBSERVED_POSITIONING" in result.reasons
    assert "RETAINED_CHAIN_SCOPE_NOT_TOTAL_MARKET" in result.reasons


@pytest.mark.parametrize("version,matrix_binding,expected", [
    ("gamma_wall_evidence_v1", "candidate", GateVerdict.PASS),
    ("gamma_wall_evidence_v1", "wrong", GateVerdict.UNAVAILABLE),
    ("gamma_wall_evidence_v1", None, GateVerdict.UNAVAILABLE),
    ("unknown", "candidate", GateVerdict.UNAVAILABLE),
    (None, "candidate", GateVerdict.UNAVAILABLE),
])
def test_versioned_gamma_evidence_requires_its_candidate_matrix(version, matrix_binding, expected):
    detail = retained_gamma_detail()
    primary = detail["candidate"]["primary_evidence"]
    if version is not None:
        primary["gamma_evidence_version"] = version
    if matrix_binding is not None:
        primary["gamma_matrix_id"] = str(detail["candidate"]["matrix_id"]) if matrix_binding == "candidate" else str(uuid4())
    assert retained_gamma_evidence(detail).verdict is expected


@pytest.mark.parametrize("mutation", ["unlinked", "wrong_matrix", "wrong_policy", "future", "quality", "convention", "scope", "empty", "wall", "not_zero_dte"])
def test_gamma_cannot_use_latest_unlinked_or_unusable_profile(mutation):
    detail = retained_gamma_detail()
    row = detail["gamma_evidence"][0]
    if mutation == "unlinked": del detail["candidate"]["primary_evidence"]["gamma_policy_sha256"]
    if mutation == "wrong_matrix": row["matrix_id"] = str(uuid4())
    if mutation == "wrong_policy": row["gamma_policy_sha256"] = "b" * 64
    if mutation == "future": row["first_observed_at"] = DECISION_AT + timedelta(seconds=1)
    if mutation == "quality": row["quality_reasons"] = ["INSUFFICIENT_GAMMA_CONTRACTS"]
    if mutation == "convention": row["dealer_convention"] = "DEALER_SHORT_CALLS_LONG_PUTS"
    if mutation == "scope": row["scope"] = "TOTAL"
    if mutation == "empty": row["contributing_contract_count"] = 0
    if mutation == "wall": detail["candidate"]["primary_evidence"]["gamma_wall_strike"] = "105"
    if mutation == "not_zero_dte": detail["legs"][0]["expiration_date"] = "2026-10-16"
    assert retained_gamma_evidence(detail).verdict is not GateVerdict.PASS


def retained_event_detail():
    detail = retained_wheel_detail()
    detail["event_coverage_evidence"] = [{"source": "calendar"}]
    detail["holding_event_coverage"] = [{
        "coverage_id": f"coverage-{event_type}", "event_type": event_type, "affected_underlying": underlying,
        "source": "calendar", "source_key": event_type, "first_observed_at": DECISION_AT - timedelta(hours=1),
        "source_observed_at": DECISION_AT - timedelta(hours=1), "window_start": DECISION_AT - timedelta(days=1),
        "window_end": DECISION_AT + timedelta(days=3),
    } for event_type, underlying in (("EARNINGS", "SPY"), ("FED_RATE_DECISION", None))]
    detail["holding_event_evidence"] = []
    return detail


def event_row(**changes):
    return {"market_event_id": "event-original", "event_type": "EARNINGS", "affected_underlying": "SPY", "source": "calendar", "source_key": "earnings",
            "scheduled_time": DECISION_AT + timedelta(hours=1), "status": "SCHEDULED", "confidence": "CONFIRMED", "first_observed_at": DECISION_AT - timedelta(hours=1), **changes}


def test_event_horizon_checks_supported_calendars_without_claiming_full_options_coverage():
    detail = retained_event_detail()
    before = copy.deepcopy(detail)
    report = retained_event_horizon(detail, DECISION_AT, DECISION_AT + timedelta(days=2))
    assert all(check["status"] == "CLEAR" for check in report["checks"])
    assert report["status"] == "UNAVAILABLE" and not report["complete_options_event_coverage"]
    assert report["unavailable_event_types"] == ["EX_DIVIDEND", "CORPORATE_ACTION"]
    assert detail == before


@pytest.mark.parametrize("mutation", ["future_receipt", "short_window", "stale", "wrong_underlying", "wrong_source"])
def test_event_horizon_missing_or_stale_coverage_does_not_become_clear(mutation):
    detail = retained_event_detail()
    row = detail["holding_event_coverage"][0]
    if mutation == "future_receipt": row["first_observed_at"] = DECISION_AT + timedelta(seconds=1)
    if mutation == "short_window": row["window_end"] = DECISION_AT + timedelta(hours=1)
    if mutation == "stale": row["source_observed_at"] = DECISION_AT - timedelta(days=2)
    if mutation == "wrong_underlying": row["affected_underlying"] = "QQQ"
    if mutation == "wrong_source": row["source"] = "later-provider"
    report = retained_event_horizon(detail, DECISION_AT, DECISION_AT + timedelta(days=2))
    assert report["checks"][0]["status"] == "UNAVAILABLE"


def test_event_horizon_known_event_blocks_even_if_coverage_is_missing():
    detail = retained_event_detail()
    detail["holding_event_coverage"] = []
    detail["holding_event_evidence"] = [event_row()]
    report = retained_event_horizon(detail, DECISION_AT, DECISION_AT + timedelta(days=2))
    assert report["status"] == "BLOCKED"
    assert retained_alert_evidence(detail, DECISION_AT, holding_until=DECISION_AT + timedelta(days=2))[AlertInput.EVENT_HORIZON].verdict is GateVerdict.FAIL


def test_event_horizon_cancellation_is_resolved_as_of_candidate_not_today():
    detail = retained_event_detail()
    detail["holding_event_evidence"] = [event_row(), event_row(market_event_id="cancel", status="CANCELED", first_observed_at=DECISION_AT + timedelta(seconds=1))]
    assert retained_event_horizon(detail, DECISION_AT, DECISION_AT + timedelta(days=2))["status"] == "BLOCKED"
    detail["holding_event_evidence"][1]["first_observed_at"] = DECISION_AT
    report = retained_event_horizon(detail, DECISION_AT, DECISION_AT + timedelta(days=2))
    assert report["checks"][0]["status"] == "CLEAR"
    assert "cancel" in report["checks"][0]["source_ids"]


def test_estimated_event_on_same_date_does_not_claim_clear_outside_exact_timestamp():
    detail = retained_event_detail()
    detail["holding_event_evidence"] = [event_row(confidence="ESTIMATED", scheduled_time=DECISION_AT - timedelta(hours=4))]
    assert retained_event_horizon(detail, DECISION_AT, DECISION_AT + timedelta(hours=2))["checks"][0]["status"] == "UNKNOWN"


def test_event_coverage_union_requires_no_gaps():
    detail = retained_event_detail()
    first = detail["holding_event_coverage"][0]
    first["window_end"] = DECISION_AT + timedelta(hours=2)
    second = {**first, "coverage_id": "later-part", "source_key": "other-window", "window_start": first["window_end"], "window_end": DECISION_AT + timedelta(days=3)}
    detail["holding_event_coverage"].append(second)
    assert retained_event_horizon(detail, DECISION_AT, DECISION_AT + timedelta(days=2))["checks"][0]["status"] == "CLEAR"
    second["window_start"] += timedelta(seconds=1)
    assert retained_event_horizon(detail, DECISION_AT, DECISION_AT + timedelta(days=2))["checks"][0]["status"] == "UNAVAILABLE"


def test_event_coverage_newer_narrower_revision_supersedes_old_wide_window():
    detail = retained_event_detail()
    original = detail["holding_event_coverage"][0]
    detail["holding_event_coverage"].append({**original, "coverage_id": "narrow", "first_observed_at": DECISION_AT,
                                            "window_end": DECISION_AT + timedelta(hours=1)})
    assert retained_event_horizon(detail, DECISION_AT, DECISION_AT + timedelta(days=2))["checks"][0]["status"] == "UNAVAILABLE"


def test_event_revision_moved_outside_window_supersedes_old_event():
    detail = retained_event_detail()
    detail["holding_event_evidence"] = [event_row(), event_row(market_event_id="moved", first_observed_at=DECISION_AT, scheduled_time=DECISION_AT + timedelta(days=4), status="REVISED")]
    report = retained_event_horizon(detail, DECISION_AT, DECISION_AT + timedelta(days=2))
    assert report["checks"][0]["status"] == "CLEAR"


def test_event_earnings_exemption_uses_dated_ingestion_type_not_ticker_or_label():
    detail = retained_event_detail()
    detail["holding_event_coverage"] = [row for row in detail["holding_event_coverage"] if row["event_type"] != "EARNINGS"]
    detail["candidate"]["earnings_blackout_state"] = "NOT_APPLICABLE"
    assert retained_event_horizon(detail, DECISION_AT, DECISION_AT + timedelta(days=1))["checks"][0]["status"] == "UNAVAILABLE"
    source = detail["source_snapshots"][0]
    source.update({"underlying_asset_type": "ETF", "underlying_asset_type_observed_at": DECISION_AT, "batch_id": "original-batch"})
    check = retained_event_horizon(detail, DECISION_AT, DECISION_AT + timedelta(days=1))["checks"][0]
    assert check["status"] == "NOT_APPLICABLE" and check["source_ids"] == ["option-ingestion:original-batch"]
    source["underlying_asset_type_observed_at"] = DECISION_AT + timedelta(seconds=1)
    assert retained_event_horizon(detail, DECISION_AT, DECISION_AT + timedelta(days=1))["checks"][0]["status"] == "UNAVAILABLE"


def _freeze(detail, **changes):
    arguments = {"decision_at": DECISION_AT, "entry_deadline": DECISION_AT + timedelta(minutes=2),
                 "exit_deadline": DECISION_AT + timedelta(days=2), "entry_limit": Decimal("200")}
    return freeze_indicative_alert_plan(detail, **{**arguments, **changes})


def test_alert_publication_preview_preserves_package_and_never_publishes():
    detail = retained_wheel_detail()
    original = copy.deepcopy(detail)
    preview = preview_indicative_alert_plan(
        detail, decision_at=DECISION_AT, entry_deadline=DECISION_AT + timedelta(minutes=2),
        exit_deadline=DECISION_AT + timedelta(days=2), entry_limit=Decimal("200"),
    )
    assert preview["status"] == "INDICATIVE_PLAN_VALID"
    assert preview["plan_preview"]["plan_id"] == str(_freeze(detail).plan_id)
    assert preview["plan_preview"]["source_evidence_omitted"] is True
    assert "source_evidence" not in preview["plan_preview"]["plan"]
    assert preview["blockers"] == []
    assert preview["persisted"] is preview["publication_permission"] is preview["execution_permission"] is False
    assert preview["paper_position_created"] is False and preview["fill"] is None
    assert detail == original
    preview["original_package"]["legs"][0]["contract_id"] = 100
    assert detail == original


def test_alert_publication_preview_shows_missing_terms_and_expired_original_window():
    detail = retained_wheel_detail()
    preview = preview_indicative_alert_plan(detail, decision_at=DECISION_AT + timedelta(minutes=6))
    assert preview["status"] == "BLOCKED"
    assert preview["plan_preview"] is None
    assert {row["field"] for row in preview["blockers"] if row["code"] == "PLAN_TERM_REQUIRED"} == {
        "entry_deadline", "exit_deadline", "entry_limit",
    }
    assert "ORIGINAL_ENTRY_WINDOW_ELAPSED" in {row["code"] for row in preview["blockers"]}
    assert preview["original_package"]["legs"][0]["contract_id"] == 1


@pytest.mark.parametrize("mutation", ["worse_entry", "extended_entry", "exit_dte", "missing_management", "observed_later"])
def test_alert_publication_preview_cannot_bypass_frozen_plan_guards(mutation):
    detail = retained_wheel_detail()
    terms = {"entry_deadline": DECISION_AT + timedelta(minutes=2),
             "exit_deadline": DECISION_AT + timedelta(days=2), "entry_limit": Decimal("200")}
    if mutation == "worse_entry": terms["entry_limit"] = Decimal("199")
    if mutation == "extended_entry": terms["entry_deadline"] = DECISION_AT + timedelta(minutes=6)
    if mutation == "exit_dte": terms["exit_deadline"] = DECISION_AT + timedelta(days=10)
    if mutation == "missing_management": detail["candidate"]["management_policy"] = {}
    if mutation == "observed_later": detail["candidate"]["observed_time"] = DECISION_AT + timedelta(seconds=1)
    preview = preview_indicative_alert_plan(detail, decision_at=DECISION_AT, **terms)
    assert preview["status"] == "BLOCKED"
    assert preview["plan_preview"] is None
    assert preview["blockers"]


def test_alert_publication_preview_observation_does_not_become_package():
    detail = retained_wheel_detail()
    detail["candidate"]["candidate_kind"] = "RESEARCH_ONLY"
    preview = preview_indicative_alert_plan(detail, decision_at=DECISION_AT)
    assert preview["status"] == "NOT_APPLICABLE"
    assert preview["plan_preview"] is None


def test_frozen_alert_plan_keeps_exact_package_and_detaches_mutable_inputs():
    detail = retained_wheel_detail()
    original = copy.deepcopy(detail)
    first = _freeze(detail)
    second = _freeze(detail)
    assert first.sha256 == second.sha256 and first.plan_id == second.plan_id
    assert detail == original
    payload = first.to_dict()["plan"]
    assert payload["state"] == "UNPUBLISHED_INDICATIVE_PLAN"
    assert payload["entry_limit_kind"] == "MINIMUM_CREDIT"
    assert payload["published_at"] is None and payload["fill"] is None
    assert payload["execution_permission"] is False
    assert payload["paper_position_created"] is False
    assert payload["legs"][0]["contract_id"] == 1
    detail["legs"][0]["contract_id"] = 100
    detail["candidate"]["management_policy"]["exit_dte"] = 1
    assert first.to_dict()["plan"]["legs"][0]["contract_id"] == 1
    assert first.to_dict()["plan"]["management_policy"]["exit_dte"] == 21
    payload["legs"][0]["contract_id"] = 200
    assert first.to_dict()["plan"]["legs"][0]["contract_id"] == 1
    with pytest.raises(FrozenInstanceError):
        first.payload_json = "changed"


@pytest.mark.parametrize("changes", [
    {"entry_deadline": DECISION_AT},
    {"entry_deadline": DECISION_AT + timedelta(minutes=6)},
    {"decision_at": DECISION_AT - timedelta(seconds=1)},
    {"decision_at": DECISION_AT + timedelta(minutes=6)},
    {"exit_deadline": DECISION_AT + timedelta(days=30)},
    {"exit_deadline": DECISION_AT + timedelta(days=10)},
    {"entry_limit": Decimal("NaN")}, {"entry_limit": Decimal("199")},
])
def test_frozen_alert_plan_never_backdates_extends_deadlines_or_worsens_entry(changes):
    with pytest.raises(ValueError):
        _freeze(retained_wheel_detail(), **changes)


def test_frozen_plan_new_terms_have_new_identity_without_mutating_original():
    detail = retained_wheel_detail()
    first = _freeze(detail)
    changed = _freeze(detail, entry_limit=Decimal("201"))
    assert first.plan_id != changed.plan_id
    assert first.to_dict()["plan"]["entry_limit"] == "200"
    assert changed.to_dict()["plan"]["entry_limit"] == "201"


def test_observation_findings_and_missing_management_cannot_be_frozen_as_packages():
    detail = retained_wheel_detail()
    detail["candidate"]["candidate_kind"] = "RESEARCH_ONLY"
    with pytest.raises(ValueError, match="observation-only"):
        _freeze(detail)
    detail = retained_wheel_detail()
    detail["candidate"]["management_policy"] = {}
    with pytest.raises(ValueError, match="management"):
        _freeze(detail)


@pytest.mark.parametrize("management", [
    {"dte_lane": "SHORT", "maximum_breakeven_expected_move_ratio": .8},
    {"take_profit_fraction": .5}, {"take_profit_fraction": .5, "stop_loss_multiple": .8},
    {"take_profit_fraction": 1.1, "stop_loss_multiple": 2},
])
def test_partial_or_invalid_management_is_not_a_frozen_trade_plan(management):
    detail = retained_wheel_detail()
    detail["candidate"]["management_policy"] = management
    with pytest.raises(ValueError):
        _freeze(detail)


def retained_long_detail():
    detail = retained_wheel_detail()
    for row in (detail["legs"][0], detail["source_snapshots"][0]):
        row.update({"side": "BUY", "contract_type": "CALL", "contract_ticker": "O:SPY261016C00100000", "spot": "95"})
    detail["candidate"].update({"strategy_name": "DIRECTIONAL_LONG_PREMIUM", "structure_type": "LONG_CALL",
        "net_premium": "-200", "maximum_profit": None, "maximum_loss": "200", "capital_at_risk": "200", "collateral_required": None,
        "breakevens": ["102"], "management_policy": {"dte_lane": "SHORT", "maximum_breakeven_expected_move_ratio": 1},
        "decision_context": {"equity_context_snapshot_id": str(uuid4()), "equity_context_status": "COMPLETE", "qualified_direction": "BULLISH"}})
    return detail


def explicit_management(**changes):
    return AlertManagementPolicy(**{"policy_version": "test_alert_management_v1", "strategy_name": "DIRECTIONAL_LONG_PREMIUM",
        "stop_loss_fraction": Decimal("0.35"), "take_profit_fraction": Decimal("0.5"), "maximum_hold_seconds": 3 * 86400, **changes})


def test_explicit_alert_management_completes_debit_plan_without_rewriting_candidate():
    detail = retained_long_detail()
    before = copy.deepcopy(detail)
    with pytest.raises(ValueError, match="management"):
        _freeze(detail)
    policy = explicit_management()
    plan = _freeze(detail, management_policy=policy).to_dict()["plan"]
    assert detail == before
    assert plan["management_policy_sha256"] == policy.sha256
    assert plan["management_source"] == "EXPLICIT_ALERT_POLICY"
    assert plan["original_management_policy"] == before["candidate"]["management_policy"]
    assert plan["management_limits"]["stop_package_value"] == "130.00"
    assert plan["management_limits"]["take_profit_package_value"] == "300.0"
    assert plan["management_limits"]["stop_fill_guaranteed"] is False
    assert plan["published_at"] is None and plan["execution_permission"] is False
    changed = _freeze(detail, management_policy=explicit_management(take_profit_fraction=Decimal("0.6")))
    assert changed.sha256 != _freeze(detail, management_policy=policy).sha256
    incomplete = preview_indicative_alert_plan(detail, decision_at=DECISION_AT + timedelta(minutes=6))
    assert {row["code"] for row in incomplete["blockers"]} >= {"MANAGEMENT_POLICY_REJECTED", "ORIGINAL_ENTRY_WINDOW_ELAPSED"}
    preview = preview_indicative_alert_plan(
        detail, decision_at=DECISION_AT, entry_deadline=DECISION_AT + timedelta(minutes=2),
        exit_deadline=DECISION_AT + timedelta(days=2), entry_limit=Decimal("200"), management_policy=policy,
    )
    assert preview["status"] == "INDICATIVE_PLAN_VALID"
    assert preview["proposed_management_policy_sha256"] == policy.sha256
    assert preview["plan_preview"]["plan"]["management_limits"] == plan["management_limits"]
    assert detail == before


@pytest.mark.parametrize("changes", [
    {"stop_loss_fraction": Decimal("1")}, {"take_profit_fraction": Decimal("NaN")},
    {"maximum_hold_seconds": 0}, {"minimum_exit_dte": 0}, {"strategy_name": "INCOME_WHEEL"},
])
def test_invalid_explicit_alert_management_is_rejected(changes):
    with pytest.raises(ValueError):
        explicit_management(**changes)


def test_explicit_management_preserves_original_rules_and_horizon():
    with pytest.raises(ValueError, match="hold cap"):
        _freeze(retained_long_detail(), management_policy=explicit_management(maximum_hold_seconds=60))
    with pytest.raises(ValueError, match="match"):
        _freeze(retained_long_detail(), management_policy=explicit_management(strategy_name="DIRECTIONAL_DEBIT_SPREAD"))
    detail = retained_long_detail()
    detail["candidate"]["management_policy"]["stop_loss_fraction"] = .2
    with pytest.raises(ValueError, match="overwrite"):
        _freeze(detail, management_policy=explicit_management())


def test_managed_debit_spread_preserves_both_legs_and_caps_profit_target():
    detail = retained_long_detail()
    short = {**detail["legs"][0], "leg_index": 1, "snapshot_id": str(uuid4()), "contract_id": 2,
             "contract_ticker": "O:SPY261016C00102000", "side": "SELL", "strike": "102", "model_mark": ".5"}
    detail["legs"].append(short)
    detail["source_snapshots"].append({**detail["source_snapshots"][0], **short})
    detail["candidate"].update({"candidate_kind": "MULTI_LEG", "strategy_name": "DIRECTIONAL_DEBIT_SPREAD", "structure_type": "CALL_DEBIT_VERTICAL",
        "net_premium": "-150", "maximum_profit": "50", "maximum_loss": "150", "capital_at_risk": "150", "breakevens": ["101.5"]})
    policy = explicit_management(strategy_name="DIRECTIONAL_DEBIT_SPREAD", take_profit_fraction=Decimal(".25"))
    plan = _freeze(detail, entry_limit=Decimal("150"), management_policy=policy).to_dict()["plan"]
    assert [leg["side"] for leg in plan["legs"]] == ["BUY", "SELL"]
    assert plan["management_limits"]["take_profit_package_value"] == "187.50"
    with pytest.raises(ValueError, match="bounded maximum profit"):
        _freeze(detail, entry_limit=Decimal("150"), management_policy=explicit_management(strategy_name="DIRECTIONAL_DEBIT_SPREAD"))


@pytest.mark.parametrize(("previous", "event", "offset", "valid"), [
    (None, "PUBLISHED", 1, True), (None, "PUBLISHED", 121, False),
    (None, "OBSERVED", 1, False), ("PUBLISHED", "OBSERVED", 2, True),
    ("OBSERVED", "OBSERVED", 3, True), ("PUBLISHED", "INVALIDATED", 4, True),
    ("PUBLISHED", "EXPIRED", 1, False), ("OBSERVED", "EXPIRED", 120, True),
    ("EXPIRED", "OBSERVED", 121, False), ("INVALIDATED", "PUBLISHED", 1, False),
    ("OBSERVED", "OBSERVED", 121, False), (None, "PUBLISHED", -1, False),
])
def test_publication_lifecycle_keeps_original_entry_deadline(previous, event, offset, valid):
    payload = _freeze(retained_wheel_detail()).to_dict()["plan"]
    if valid:
        validate_publication_transition(payload, previous, AlertPublicationEvent(event), DECISION_AT + timedelta(seconds=offset))
    else:
        with pytest.raises(ValueError):
            validate_publication_transition(payload, previous, AlertPublicationEvent(event), DECISION_AT + timedelta(seconds=offset))


def test_publication_verifies_frozen_payload_and_stable_exposure_identity():
    detail = retained_long_detail()
    first = _freeze(detail, management_policy=explicit_management())
    second = _freeze(detail, management_policy=explicit_management(), entry_deadline=DECISION_AT + timedelta(minutes=3), entry_limit=Decimal("199"))
    assert validated_plan_payload(first) == first.to_dict()["plan"]
    assert first.plan_id != second.plan_id
    assert publication_exposure_key(first.to_dict()["plan"]) == publication_exposure_key(second.to_dict()["plan"])
    from options.alert_plans import FrozenOptionAlertPlan
    broken = FrozenOptionAlertPlan(first.payload_json.replace('"entry_limit":"200"', '"entry_limit":"999"'))
    with pytest.raises(ValueError):
        validated_plan_payload(broken)


def publication_repository(cursor):
    connection = MagicMock()
    connection.closed = False
    cursor.closed = False
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    return OptionAlertPublicationRepository(factory), connection


def test_publication_writes_plan_and_event_in_one_transaction_with_database_clock():
    plan = _freeze(retained_wheel_detail())
    cursor = MagicMock()
    now = DECISION_AT + timedelta(seconds=1)
    cursor.fetchone.side_effect = [None, {"recorded_at": now}, None, {"plan_sha256": plan.sha256}, {"event_id": uuid4(), "event_type": "PUBLISHED", "recorded_at": now}]
    repository, connection = publication_repository(cursor)
    result = repository.publish(plan, request_key="initial")
    assert result["status"] == "RECORDED"
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert any("pg_advisory_xact_lock" in statement for statement in statements)
    assert sum("INSERT INTO" in statement for statement in statements) == 2
    assert not any("UPDATE option_" in statement or "DELETE FROM" in statement for statement in statements)
    event_sql, values = cursor.execute.call_args.args
    assert "recorded_at)" not in event_sql
    assert values[2] == "PUBLISHED"
    connection.commit.assert_called_once()
    connection.rollback.assert_not_called()


def test_publication_retry_returns_original_event_without_new_plan_or_clock():
    plan = _freeze(retained_wheel_detail())
    request = OptionAlertPublicationRepository._request(plan.plan_id, AlertPublicationEvent.PUBLISHED, "initial", (), None, None)
    cursor = MagicMock()
    cursor.fetchone.return_value = {"event_id": uuid4(), "event_type": "PUBLISHED", "recorded_at": DECISION_AT, "request_text": request}
    repository, connection = publication_repository(cursor)
    result = repository.publish(plan, request_key="initial")
    assert result["status"] == "ALREADY_RECORDED" and result["recorded_at"] == DECISION_AT
    assert not any("INSERT" in call.args[0] or "clock_timestamp" in call.args[0] for call in cursor.execute.call_args_list)
    connection.commit.assert_called_once()


@pytest.mark.parametrize("failure", ["late", "active_exposure", "key_conflict", "insert_failure"])
def test_publication_failure_rolls_back_without_partial_commit(failure):
    plan = _freeze(retained_wheel_detail())
    cursor = MagicMock()
    if failure == "late": cursor.fetchone.side_effect = [None, {"recorded_at": DECISION_AT + timedelta(minutes=3)}]
    if failure == "active_exposure": cursor.fetchone.side_effect = [None, {"recorded_at": DECISION_AT}, {"plan_id": uuid4()}]
    if failure == "key_conflict": cursor.fetchone.return_value = {"request_text": "different"}
    if failure == "insert_failure": cursor.fetchone.side_effect = [None, {"recorded_at": DECISION_AT}, None, {"plan_sha256": plan.sha256}, RuntimeError("insert failed")]
    repository, connection = publication_repository(cursor)
    with pytest.raises((ValueError, RuntimeError)):
        repository.publish(plan, request_key="initial")
    connection.rollback.assert_called_once()
    connection.commit.assert_not_called()


def test_observation_event_has_causal_sources_and_never_updates_plan():
    plan = _freeze(retained_wheel_detail())
    cursor = MagicMock()
    cursor.fetchone.side_effect = [{"exposure_key": "a" * 64}, None, {"payload_text": plan.payload_json}, {"event_type": "PUBLISHED"}, {"recorded_at": DECISION_AT + timedelta(seconds=10)}, {"event_id": uuid4(), "event_type": "OBSERVED", "recorded_at": DECISION_AT + timedelta(seconds=10)}]
    repository, connection = publication_repository(cursor)
    result = repository.append_event(plan.plan_id, AlertPublicationEvent.OBSERVED, request_key="seen-again", source_ids=("new-source",), source_available_at=DECISION_AT + timedelta(seconds=5))
    assert result["event_type"] == "OBSERVED"
    assert sum("INSERT INTO" in call.args[0] for call in cursor.execute.call_args_list) == 1
    connection.commit.assert_called_once()
    with pytest.raises(ValueError, match="causal source"):
        repository.append_event(plan.plan_id, AlertPublicationEvent.OBSERVED, request_key="no-proof")


@pytest.mark.parametrize("source_offset", [-1, 11])
def test_publication_event_rejects_future_or_predecision_source_without_committing(source_offset):
    plan = _freeze(retained_wheel_detail())
    cursor = MagicMock()
    cursor.fetchone.side_effect = [{"exposure_key": "a" * 64}, None, {"payload_text": plan.payload_json}, {"event_type": "PUBLISHED"}, {"recorded_at": DECISION_AT + timedelta(seconds=10)}]
    repository, connection = publication_repository(cursor)
    with pytest.raises(ValueError, match="event source"):
        repository.append_event(plan.plan_id, AlertPublicationEvent.OBSERVED, request_key="bad-time", source_ids=("row",), source_available_at=DECISION_AT + timedelta(seconds=source_offset))
    connection.rollback.assert_called_once()
    assert not any("INSERT INTO" in call.args[0] for call in cursor.execute.call_args_list)


def test_event_history_uses_read_only_select_and_bounds_pagination():
    cursor = MagicMock()
    cursor.fetchall.return_value = [{"event_type": "PUBLISHED", "sequence": 1}]
    repository, connection = publication_repository(cursor)
    assert repository.history(limit=2, offset=5) == ({"event_type": "PUBLISHED", "sequence": 1},)
    sql, parameters = cursor.execute.call_args.args
    assert sql.strip().startswith("SELECT")
    assert parameters == (2, 5)
    for arguments in ({"limit": 201}, {"offset": -1}, {"limit": True}):
        with pytest.raises(ValueError, match="pagination"):
            repository.history(**arguments)


def dry_run_row(detail=None, **terms):
    from options.alert_plans import preview_indicative_alert_plan
    detail = detail or retained_wheel_detail()
    preview = preview_indicative_alert_plan(
        detail, decision_at=DECISION_AT, entry_deadline=DECISION_AT + timedelta(minutes=2),
        exit_deadline=DECISION_AT + timedelta(days=2), entry_limit=Decimal("200"), **terms,
    )
    return {"candidate_id": str(detail["candidate"]["candidate_id"]), "status": preview["status"],
            "preview": preview, "blockers": list(preview["blockers"])}


@pytest.mark.parametrize("state_kind,expected", [
    ("new", "WOULD_PUBLISH_INDICATIVE"), ("terminal", "WOULD_PUBLISH_INDICATIVE"),
    ("active", "BLOCKED"), ("unavailable", "BLOCKED"),
])
def test_dry_run_classifies_exact_exposure_without_publishing(state_kind, expected):
    from options.alert_orchestration import dry_run_exposure_keys, finalize_alert_dry_run
    row = dry_run_row()
    original = copy.deepcopy(row)
    key = dry_run_exposure_keys([row])[0]
    state = {"available": state_kind != "unavailable", "checked_at": DECISION_AT, "rows": []}
    if state_kind in {"terminal", "active"}:
        state["rows"] = [{"exposure_key": key, "active_plan_count": int(state_kind == "active"),
                          "active_plan_id": str(uuid4()), "published_plan_count": 1,
                          "active_entry_deadline": DECISION_AT - timedelta(days=1)}]
    result = finalize_alert_dry_run([row], policy={"version": "test"}, assessed_at=DECISION_AT,
        completed_at=DECISION_AT, expected_slot=DECISION_AT, final_slot=DECISION_AT, publication_state=state)
    assert result["rows"][0]["status"] == expected
    assert result["publication_permission"] is result["persisted"] is result["reservations_created"] is False
    assert row == original


def test_dry_run_collapses_recurrence_in_request_order_without_rewriting_inputs():
    from options.alert_orchestration import dry_run_exposure_keys, finalize_alert_dry_run
    first, second = dry_run_row(), dry_run_row()
    assert first["candidate_id"] != second["candidate_id"]
    assert len(dry_run_exposure_keys([first, second])) == 1
    result = finalize_alert_dry_run([first, second], policy={"version": "test"}, assessed_at=DECISION_AT,
        completed_at=DECISION_AT, expected_slot=DECISION_AT, final_slot=DECISION_AT,
        publication_state={"available": True, "checked_at": DECISION_AT, "rows": []})
    assert [row["status"] for row in result["rows"]] == ["WOULD_PUBLISH_INDICATIVE", "BLOCKED"]
    assert result["rows"][1]["blockers"] == [{"code": "DUPLICATE_EXPOSURE_IN_BATCH", "candidate_id": first["candidate_id"]}]
    assert not first["blockers"] and not second["blockers"]


@pytest.mark.parametrize("elapsed,changed_slot", [(True, False), (False, True)])
def test_dry_run_rechecks_deadline_and_source_slot_at_completion(elapsed, changed_slot):
    from options.alert_orchestration import finalize_alert_dry_run
    row = dry_run_row()
    result = finalize_alert_dry_run([row], policy={}, assessed_at=DECISION_AT,
        completed_at=DECISION_AT + timedelta(minutes=2) if elapsed else DECISION_AT,
        expected_slot=DECISION_AT, final_slot=None if changed_slot else DECISION_AT,
        publication_state={"available": True, "rows": [], "checked_at": DECISION_AT})
    assert result["rows"][0]["status"] == "BLOCKED"
    assert result["counts"] == {"BLOCKED": 1}


def test_exposure_state_is_bounded_read_only_and_preserves_elapsed_active_publications():
    cursor = MagicMock()
    cursor.fetchone.return_value = {"ready": True, "checked_at": DECISION_AT}
    cursor.fetchall.return_value = [{"exposure_key": "a" * 64, "active_plan_count": 1,
                                   "active_entry_deadline": DECISION_AT - timedelta(days=1)}]
    repository, connection = publication_repository(cursor)
    result = repository.exposure_state(("a" * 64, "a" * 64))
    assert result["available"] is True
    assert result["rows"][0]["active_plan_count"] == 1
    statements = [call.args[0].strip() for call in cursor.execute.call_args_list]
    assert statements[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert all(statement.startswith(("SET", "SELECT")) for statement in statements)
    assert "ORDER BY sequence DESC LIMIT 1" in statements[-1]
    assert "plan.entry_deadline >" not in statements[-1]
    assert "pg_advisory" not in statements[-1]
    assert cursor.execute.call_args.args[1] == (["a" * 64],)
    connection.commit.assert_called_once()


def test_exposure_state_missing_schema_is_unavailable_not_empty_clear():
    cursor = MagicMock()
    cursor.fetchone.return_value = {"ready": False, "checked_at": DECISION_AT}
    repository, _ = publication_repository(cursor)
    result = repository.exposure_state(("a" * 64,))
    assert result["available"] is False
    assert result["reason"] == "OPTION_ALERT_PUBLICATION_MIGRATION_REQUIRED"
    cursor.fetchall.assert_not_called()


@pytest.mark.parametrize("keys", [(), ("a" * 64,) * 21, ("x" * 64,), (None,), ["a" * 64]])
def test_exposure_state_rejects_invalid_scope_before_database_access(keys):
    cursor = MagicMock()
    repository, connection = publication_repository(cursor)
    with pytest.raises(ValueError, match="1-20 SHA256"):
        repository.exposure_state(keys)
    connection.cursor.assert_not_called()


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
