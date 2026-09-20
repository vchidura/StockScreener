import json
import os
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from main import app
from options.api import (
    _candidate_research_evidence,
    _event_asset_type,
    _event_window_state,
    _performance_checkpoints,
    option_candidate_detail,
    option_alert_qualification,
    option_alert_qualification_policy,
    option_alert_publications,
    option_alert_publication_preview,
    OptionAlertPreviewRequest,
    option_alert_publications_dry_run,
    _assess_stock_behavior_for_dry_run,
    OptionAlertDryRunRequest,
    option_candidates,
    option_data_quality,
    option_discovery_catalog,
    option_eligible_chain,
    option_event_calendar_reference,
    option_flow,
    option_gamma,
    option_health,
    option_opportunities,
    option_performance,
    option_screener,
    option_signals,
    option_universe,
)
from options.calendar import OptionExchangeCalendar
from options.discovery import EligibleChainQuery, contract_filter_sql


UTC = timezone.utc


@pytest.mark.parametrize(("opened", "verdict", "expected"), [(False, "OUTSIDE_MARKET_HOURS", 2), (True, "NO_SLOT_DUE", 1), (True, "INCOMPLETE_OR_STALE", 1), (True, "CURRENT", 0)])
def test_alert_publications_market_validation_exit_status(monkeypatch, capsys, opened, verdict, expected):
    from scripts import report_option_market_runs as audit
    monkeypatch.setattr(audit, "report", lambda: {"market_open": opened, "market_hours_verdict": verdict, "data_mutated": False})
    monkeypatch.setattr(sys, "argv", ["report_option_market_runs.py", "--require-market-open"])
    assert audit.main() == expected
    assert verdict in capsys.readouterr().out


def test_session_validation_no_slot_does_not_query_or_report_complete():
    from scripts.report_option_market_runs import session_validation
    cursor = MagicMock()
    result = session_validation(cursor, None, datetime(2026, 9, 17, 13, 30, tzinfo=UTC), None, None)
    assert result == {"status": "NO_OBSERVABLE_SLOT", "expected_member_slots": 0}
    cursor.execute.assert_not_called()


@pytest.mark.parametrize("snapshot_count,identity_mismatch,expected_integrity", [
    (0, 0, "NO_RETAINED_SNAPSHOTS"),
    (1, 0, "PASS_CHECKED_INVARIANTS"),
    (1, 1, "REVIEW_REQUIRED"),
])
def test_session_validation_uses_causal_versioned_contracts_and_independent_verdicts(
    snapshot_count, identity_mismatch, expected_integrity,
):
    from scripts.report_option_market_runs import session_validation
    from options.config import load_option_runtime_configuration
    from options.worker import OptionWorkerSettings
    configuration = load_option_runtime_configuration({"POLYGON_API_KEY": "test"}, BACKEND_DIR)
    cursor = MagicMock()
    slot = datetime(2026, 9, 17, 13, 45, tzinfo=UTC)
    snapshot = dict.fromkeys((
        "snapshots", "model_marks", "iv_converged", "diagnostic_only", "missing_contract_reference",
        "identity_mismatch", "invalid_numeric", "invalid_timing", "invalid_valuation_provenance",
        "model_mark_age_or_skew", "converged_missing_greeks",
    ), 0)
    snapshot.update(snapshots=snapshot_count, identity_mismatch=identity_mismatch)
    cursor.fetchall.side_effect = [
        [{"slot": slot, "expected": 13, "ingestion_present": 1, "analysis_complete": 1}],
        [snapshot] if snapshot_count else [], [],
    ]
    cursor.fetchone.side_effect = [
        {"invalid_candidate_time": 0, "observation_with_legs": 0, "gate_ledger_mismatch": 0, "invalid_leg_lineage": 0},
        {"active_queries": 0, "queries": []},
    ]
    report = session_validation(cursor, configuration, slot - timedelta(minutes=15), slot, OptionWorkerSettings())
    assert report["status"] == "INCOMPLETE"
    assert report["missing_member_slots"] == 12
    assert report["retained_integrity_verdict"] == expected_integrity
    assert report["snapshot_totals"]["snapshots"] == snapshot_count
    assert report["provider_price_crosscheck_performed"] is False
    for call in cursor.execute.call_args_list:
        statement = call.args[0]
        assert statement.lstrip().startswith(("WITH", "SELECT"))
        if len(call.args) > 1:
            assert statement.count("%s") == len(call.args[1])
    snapshot_sql = cursor.execute.call_args_list[1].args[0]
    assert "option_contract_catalog_versions" in snapshot_sql
    assert "version.first_observed_at <= snapshot.first_observed_at" in snapshot_sql
    assert "version.valid_to > snapshot.market_data_time" in snapshot_sql
    assert "version.strike" in snapshot_sql


def test_alert_publications_missing_schema_does_not_initialize_or_publish():
    cursor = MagicMock()
    cursor.fetchone.return_value = {"ready": False}
    with patch("options.api.get_db_cursor") as connection, patch("options.api.OptionAlertPublicationRepository") as repository, patch("options.api._configuration", return_value=SimpleNamespace(policy_sha256="a" * 64)):
        connection.return_value.__enter__.return_value = cursor
        result = option_alert_publications(limit=10, offset=0).model_dump(mode="json")
    assert result["available"] is False
    assert result["reason"] == "OPTION_ALERT_PUBLICATION_MIGRATION_REQUIRED"
    assert result["data"]["rows"] == []
    repository.assert_not_called()
    assert cursor.execute.call_args.args[0].strip().startswith("SELECT")


def test_alert_publications_reader_preserves_event_and_separates_elapsed_deadline():
    import json
    now = datetime.now(UTC)
    cursor = MagicMock()
    cursor.fetchone.return_value = {"ready": True}
    plan = {"candidate_id": str(uuid4()), "underlying": "SPY", "strategy": "INCOME_WHEEL", "structure": "CASH_SECURED_PUT", "legs": [],
            "entry_limit": "200", "entry_limit_kind": "MINIMUM_CREDIT", "entry_deadline": (now - timedelta(minutes=1)).isoformat(),
            "exit_deadline": (now + timedelta(days=1)).isoformat(), "original_economics": {}, "management_policy": {}, "source_market_time": now.isoformat()}
    record = {"event_id": uuid4(), "sequence": 4, "plan_id": uuid4(), "plan_sha256": "b" * 64, "event_type": "PUBLISHED", "recorded_at": now - timedelta(minutes=2),
              "payload_text": json.dumps(plan), "request_text": '{"event":"PUBLISHED"}'}
    with patch("options.api.get_db_cursor") as connection, patch("options.api.OptionAlertPublicationRepository") as repository, patch("options.api.OptionOutcomeRepository") as outcomes, patch("options.api._configuration", return_value=SimpleNamespace(policy_sha256="a" * 64)):
        connection.return_value.__enter__.return_value = cursor
        repository.return_value.history.return_value = (record,)
        outcomes.return_value.retained_plan_marks.return_value = {"ready": False, "rows": {}}
        result = option_alert_publications(limit=10, offset=20).model_dump(mode="json")
    repository.return_value.history.assert_called_once_with(limit=10, offset=20)
    row = result["data"]["rows"][0]
    assert row["event_type"] == "PUBLISHED"
    assert row["entry_window_elapsed"] is True
    assert row["execution_permission"] is False
    assert "source_evidence" not in row
    assert record["event_type"] == "PUBLISHED"


@pytest.mark.skipif(os.getenv("OPTION_ALERT_READONLY_TESTS") != "1", reason="explicit read-only PostgreSQL check required")
def test_alert_publications_exposure_reader_live_read_only():
    from dotenv import load_dotenv
    from options.repositories.alert_publications import OptionAlertPublicationRepository

    load_dotenv(BACKEND_DIR / ".env")
    result = OptionAlertPublicationRepository().exposure_state(("0" * 64,))
    assert result["available"] is True
    assert result["checked_at"].utcoffset() is not None
    assert all(row["exposure_key"] == "0" * 64 for row in result["rows"])
    assert all(0 <= row["active_plan_count"] <= row["published_plan_count"] for row in result["rows"])


def dry_run_request_body():
    return {"policy_version": "dry_run_test_v1",
            "strategies": [{"strategy_name": "INCOME_WHEEL", "management_source": "ORIGINAL"}],
            "candidates": [{"candidate_id": str(uuid4())}]}


@pytest.mark.parametrize("mutation", ["empty", "oversized", "duplicate_candidate", "duplicate_rule", "no_rules",
                                      "implicit_management", "conflicting_management", "wrong_strategy",
                                      "decision_time", "source_evidence", "item_management"])
def test_alert_publications_dry_run_rejects_unbounded_or_implicit_scope(mutation):
    from pydantic import ValidationError
    body = dry_run_request_body()
    if mutation == "empty": body["candidates"] = []
    if mutation == "oversized": body["candidates"] = [{"candidate_id": str(uuid4())} for _ in range(21)]
    if mutation == "duplicate_candidate": body["candidates"] *= 2
    if mutation == "duplicate_rule": body["strategies"] *= 2
    if mutation == "no_rules": body["strategies"] = []
    if mutation == "implicit_management": del body["strategies"][0]["management_source"]
    if mutation == "conflicting_management": body["strategies"][0]["management_source"] = "EXPLICIT"
    if mutation == "wrong_strategy":
        body["strategies"][0].update(management_source="EXPLICIT", management_policy={
            "policy_version": "test", "strategy_name": "DIRECTIONAL_LONG_PREMIUM", "stop_loss_fraction": ".35",
            "take_profit_fraction": ".5", "maximum_hold_seconds": 3600})
    if mutation == "decision_time": body["decision_at"] = datetime.now(UTC).isoformat()
    if mutation == "source_evidence": body["candidates"][0]["source_evidence"] = {}
    if mutation == "item_management": body["candidates"][0]["management_policy"] = None
    with pytest.raises(ValidationError):
        OptionAlertDryRunRequest.model_validate(body)


def dry_run_source(configuration, candidate_id, slot):
    return {"candidate_id": candidate_id, "matrix_id": uuid4(), "strategy_name": "INCOME_WHEEL",
            "structure_type": "CASH_SECURED_PUT",
            "strategy_version": configuration.strategy_policy.strategy_version,
            "strategy_policy_sha256": configuration.strategy_policy_sha256,
            "candidate_observed_at": slot, "underlying": "SPY", "configuration_sha256": configuration.configuration_sha256,
            "market_policy_sha256": configuration.policy_sha256, "analysis_policy_sha256": configuration.policy_sha256,
            "scheduled_cycle": slot, "ingestion_status": "COMPLETE", "analysis_status": "COMPLETE",
            "ingestion_completed_at": slot, "analysis_completed_at": slot}


def test_stock_behavior_dry_run_adapter_reads_exact_snapshot_for_directional_scope():
    from test_stock_behavior_gates import current_aapl_snapshot

    snapshot = current_aapl_snapshot()
    source = {
        "candidate_id": uuid4(), "matrix_id": uuid4(),
        "candidate_identity_sha256": "f" * 64,
        "strategy_name": "DIRECTIONAL_LONG_PREMIUM",
        "strategy_version": "phase2_v4",
        "strategy_policy_sha256": "a" * 64,
        "configuration_sha256": "b" * 64,
        "market_policy_sha256": "c" * 64,
        "analysis_policy_sha256": "c" * 64,
        "structure_type": "LONG_CALL", "underlying": "AAPL",
        "scheduled_cycle": snapshot.market_time,
        "option_market_time": snapshot.market_time - timedelta(minutes=15),
        "candidate_observed_at": snapshot.market_time,
    }
    candidate = {"primary_evidence": {"directional_thesis": "BULLISH"}}
    security = SimpleNamespace(security_id=snapshot.security_id)
    decision_at = snapshot.computed_at
    with (
        patch("options.api.EquityReferenceRepository") as references,
        patch("options.api.EquityEvidenceRepository") as evidence,
    ):
        references.return_value.get_security_as_of.return_value = security
        evidence.return_value.get_behavior_as_of.return_value = snapshot
        result = _assess_stock_behavior_for_dry_run(
            source, candidate, decision_at, decision_at + timedelta(days=2),
        )

    assert result["disposition"] == "ELIGIBLE_RESEARCH"
    assert result["stock_snapshot_id"] == str(snapshot.snapshot_id)
    assert result["candidate_identity_sha256"] == "f" * 64
    assert result["option_strategy_policy_sha256"] == "a" * 64
    assert result["stock_market_cutoff"] == snapshot.market_time.isoformat().replace("+00:00", "Z")
    references.return_value.get_security_as_of.assert_called_once()
    evidence.return_value.get_behavior_as_of.assert_called_once()


def test_stock_behavior_dry_run_adapter_skips_reads_outside_scope_and_fails_closed_on_db_error():
    from psycopg2 import OperationalError

    decision_at = datetime(2026, 9, 18, 15, 0, tzinfo=UTC)
    base = {
        "candidate_id": uuid4(), "matrix_id": uuid4(), "underlying": "SPY",
        "scheduled_cycle": decision_at,
    }
    with (
        patch("options.api.EquityReferenceRepository", side_effect=AssertionError("reference read")),
        patch("options.api.EquityEvidenceRepository", side_effect=AssertionError("behavior read")),
    ):
        not_applicable = _assess_stock_behavior_for_dry_run(
            {**base, "strategy_name": "INCOME_WHEEL", "structure_type": "CASH_SECURED_PUT"},
            {"primary_evidence": {}}, decision_at, None,
        )
    assert not_applicable["disposition"] == "NOT_APPLICABLE"

    with patch(
        "options.api.EquityReferenceRepository",
        side_effect=OperationalError("sensitive database message"),
    ):
        unavailable = _assess_stock_behavior_for_dry_run(
            {
                **base, "strategy_name": "DIRECTIONAL_LONG_PREMIUM",
                "structure_type": "LONG_CALL",
            },
            {"primary_evidence": {"directional_thesis": "BULLISH"}},
            decision_at, None,
        )
    assert unavailable["disposition"] == "UNAVAILABLE"
    assert unavailable["gates"][0]["reason_codes"] == ["STOCK_BEHAVIOR_READ_FAILED"]
    assert "sensitive database message" not in str(unavailable)

    with patch(
        "options.api.EquityReferenceRepository",
    ) as references, patch(
        "options.api.EquityEvidenceRepository",
    ) as evidence:
        references.return_value.get_security_as_of.return_value = SimpleNamespace(
            security_id=uuid4(),
        )
        evidence.return_value.get_behavior_as_of.side_effect = ValueError(
            "sensitive malformed payload detail"
        )
        rejected = _assess_stock_behavior_for_dry_run(
            {
                **base, "strategy_name": "DIRECTIONAL_LONG_PREMIUM",
                "structure_type": "LONG_CALL",
            },
            {"primary_evidence": {"directional_thesis": "BULLISH"}},
            decision_at, None,
        )
    assert rejected["disposition"] == "UNAVAILABLE"
    assert rejected["gates"][0]["reason_codes"] == [
        "STOCK_BEHAVIOR_CONTRACT_REJECTED"
    ]
    assert "sensitive malformed payload detail" not in str(rejected)


@pytest.mark.parametrize("mutation,code", [
    ("stale", "SOURCE_SLOT_NOT_CURRENT"), ("future", "SOURCE_NOT_COMPLETE_AT_ASSESSMENT"),
    ("incomplete", "SOURCE_NOT_COMPLETE_AT_ASSESSMENT"), ("policy", "ACTIVE_POLICY_OR_UNIVERSE_MISMATCH"),
    ("unconfigured", "ACTIVE_POLICY_OR_UNIVERSE_MISMATCH"), ("strategy", "STRATEGY_NOT_ALLOWED"),
    ("missing", "CANDIDATE_SOURCE_UNAVAILABLE"),
])
def test_alert_publications_dry_run_blocks_source_before_evidence_or_ledger_reads(mutation, code):
    from options.config import load_option_runtime_configuration
    configuration = load_option_runtime_configuration({"POLYGON_API_KEY": "test"}, BACKEND_DIR)
    request = OptionAlertDryRunRequest.model_validate(dry_run_request_body())
    slot = datetime.now(UTC) - timedelta(minutes=15)
    source = dry_run_source(configuration, request.candidates[0].candidate_id, slot)
    if mutation == "stale": source["scheduled_cycle"] -= timedelta(minutes=15)
    if mutation == "future": source["candidate_observed_at"] = datetime.now(UTC) + timedelta(days=1)
    if mutation == "incomplete": source["analysis_status"] = "RUNNING"
    if mutation == "policy": source["configuration_sha256"] = "b" * 64
    if mutation == "unconfigured": source["underlying"] = "UNCONFIGURED"
    if mutation == "strategy": source["strategy_name"] = "DIRECTIONAL_LONG_PREMIUM"
    cursor = MagicMock()
    cursor.fetchone.return_value = {"ready": True}
    cursor.fetchall.return_value = [] if mutation == "missing" else [source]
    with patch("options.api._configuration", return_value=configuration), patch("options.api.get_db_cursor") as connection, \
            patch("options.api.OptionExchangeCalendar.latest_delayed_slot", return_value=slot), \
            patch("options.api._retained_alert_detail", side_effect=AssertionError("evidence read")), \
            patch("options.api.OptionAlertPublicationRepository", side_effect=AssertionError("ledger read")):
        connection.return_value.__enter__.return_value = cursor
        response = option_alert_publications_dry_run(request)
    assert response.data["counts"] == {"BLOCKED": 1}
    assert code in {row["code"] for row in response.data["rows"][0]["blockers"]}
    assert response.data["publication_state_available"] is None
    assert response.data["publication_permission"] is False
    assert all(call.args[0].strip().startswith(("SET", "SELECT")) for call in cursor.execute.call_args_list)
    assert cursor.execute.call_args.args[1] == ([str(request.candidates[0].candidate_id)],)


@pytest.mark.parametrize("ledger_failure", [False, True])
def test_alert_publications_dry_run_coordinates_preview_and_read_only_exposure_state(ledger_failure):
    from options.config import load_option_runtime_configuration
    from psycopg2 import OperationalError
    configuration = load_option_runtime_configuration({"POLYGON_API_KEY": "test"}, BACKEND_DIR)
    request = OptionAlertDryRunRequest.model_validate(dry_run_request_body())
    now = datetime.now(UTC)
    slot = now - timedelta(minutes=15)
    source = dry_run_source(configuration, request.candidates[0].candidate_id, slot)
    detail = {"candidate": {"matrix_id": source["matrix_id"], "policy_sha256": source["strategy_policy_sha256"]}}
    preview = {"status": "INDICATIVE_PLAN_VALID", "blockers": [], "plan_preview": {"plan": {
        "underlying": "SPY", "strategy": "INCOME_WHEEL", "strategy_version": "test", "strategy_policy_sha256": "a" * 64,
        "structure": "CASH_SECURED_PUT", "management_policy": {"take_profit_fraction": ".5"}, "legs": [],
        "entry_deadline": now + timedelta(minutes=5),
    }}}
    cursor = MagicMock()
    cursor.fetchone.return_value = {"ready": True}
    cursor.fetchall.return_value = [source]
    with patch("options.api._configuration", return_value=configuration), patch("options.api.get_db_cursor") as connection, \
            patch("options.api.OptionExchangeCalendar.latest_delayed_slot", return_value=slot), \
            patch("options.api._retained_alert_detail", return_value=(None, detail)), \
            patch("options.api._assess_stock_behavior_for_dry_run", return_value={
                "disposition": "NOT_APPLICABLE", "execution_permission": False,
            }) as stock_assessment, \
            patch("options.api.preview_indicative_alert_plan", return_value=preview) as build_preview, \
            patch("options.api.OptionAlertPublicationRepository") as repository:
        connection.return_value.__enter__.return_value = cursor
        repository.return_value.exposure_state.return_value = {"available": True, "rows": [], "checked_at": now}
        if ledger_failure:
            repository.return_value.exposure_state.side_effect = OperationalError("sensitive database message")
        response = option_alert_publications_dry_run(request)
    assert response.data["counts"] == {"BLOCKED" if ledger_failure else "WOULD_PUBLISH_INDICATIVE": 1}
    assert build_preview.call_args.kwargs["management_policy"] is None
    assert now <= build_preview.call_args.kwargs["decision_at"] <= datetime.now(UTC)
    repository.return_value.exposure_state.assert_called_once()
    repository.return_value.publish.assert_not_called()
    repository.return_value.append_event.assert_not_called()
    assert response.data["publication_permission"] is False
    assert response.data["version"] == "option_alert_dry_run_v2"
    assert response.data["rows"][0]["stock_behavior_assessment"]["disposition"] == "NOT_APPLICABLE"
    assert response.data["rows"][0]["status"] == (
        "BLOCKED" if ledger_failure else "WOULD_PUBLISH_INDICATIVE"
    )
    assert response.data["policy"]["stock_behavior_effect"] == (
        "ASSESSMENT_ONLY_NO_SELECTION_OR_PUBLICATION_BLOCK"
    )
    stock_assessment.assert_called_once()
    assert "sensitive database message" not in str(response.data)


def test_alert_publications_preview_reuses_retained_reader_and_server_time_without_writes():
    candidate_id = uuid4()
    candidate = {"candidate_id": candidate_id, "matrix_id": uuid4(), "source_contract_id": None}
    original = SimpleNamespace(available=True, data={"candidate": candidate}, as_of=None, observed_at=None,
                               policy_sha256="a" * 64, model_version="test")
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    start = datetime.now(UTC)
    request = OptionAlertPreviewRequest.model_validate({
        "entry_deadline": (start + timedelta(minutes=1)).isoformat(),
        "exit_deadline": (start + timedelta(days=1)).isoformat(), "entry_limit": "200.25",
        "management_policy": {"policy_version": "preview_test", "strategy_name": "DIRECTIONAL_LONG_PREMIUM",
                              "stop_loss_fraction": ".35", "take_profit_fraction": ".5", "maximum_hold_seconds": 172800},
    })
    with patch("options.api.option_candidate_detail", return_value=original), patch("options.api.get_db_cursor") as connection, \
            patch("options.api.preview_indicative_alert_plan", return_value={"status": "BLOCKED", "persisted": False}) as preview, \
            patch("options.api.OptionAlertPublicationRepository", side_effect=AssertionError("publication accessed")), \
            patch("options.api._configuration", return_value=SimpleNamespace(policy_sha256="a" * 64)):
        connection.return_value.__enter__.return_value = cursor
        response = option_alert_publication_preview(candidate_id, request)
    assert response.data == {"status": "BLOCKED", "persisted": False}
    assert start <= preview.call_args.kwargs["decision_at"] <= datetime.now(UTC)
    assert str(preview.call_args.kwargs["entry_limit"]) == "200.25"
    assert preview.call_args.kwargs["management_policy"].policy_version == "preview_test"
    assert preview.call_args.args[0]["source_snapshots"] == []
    assert "source_snapshots" not in original.data
    assert cursor.execute.call_count == 1
    assert cursor.execute.call_args.args[0].strip().startswith("SELECT")


def test_alert_publications_preview_missing_candidate_does_not_read_or_publish_more():
    original = SimpleNamespace(available=False, reason="CANDIDATE_NOT_FOUND")
    with patch("options.api.option_candidate_detail", return_value=original), \
            patch("options.api.get_db_cursor", side_effect=AssertionError("extra database access")):
        assert option_alert_publication_preview(uuid4(), OptionAlertPreviewRequest()) is original


@pytest.mark.parametrize("body", [
    {"decision_at": "2026-09-17T14:00:00Z"}, {"source_evidence": {}}, {"candidate_ids": [str(uuid4())]},
    {"entry_deadline": "2026-09-18T10:00:00"}, {"entry_limit": "NaN"}, {"entry_limit": "Infinity"},
    {"entry_limit": 0}, {"entry_limit": -1},
    {"management_policy": {"policy_version": "test", "strategy_name": "INCOME_WHEEL", "stop_loss_fraction": ".2",
                           "take_profit_fraction": ".5", "maximum_hold_seconds": 100}},
])
def test_alert_publications_preview_request_rejects_backdating_untrusted_evidence_and_bad_terms(body):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        OptionAlertPreviewRequest.model_validate(body)


def test_alert_qualification_policy_is_static_and_separate_from_execution():
    with patch("options.api.get_db_cursor", side_effect=AssertionError("database accessed")):
        policy = option_alert_qualification_policy()
    assert policy["version"] == "option_alert_qualification_v1"
    assert policy["assessment_only"] is True
    assert len(policy["policy_sha256"]) == 64


def test_alert_qualification_not_found_keeps_existing_reader_error():
    original = SimpleNamespace(available=False, reason="CANDIDATE_NOT_FOUND")
    with patch("options.api.option_candidate_detail", return_value=original), patch("options.api.get_db_cursor", side_effect=AssertionError("extra database access")):
        assert option_alert_qualification(uuid4()) is original


def test_alert_qualification_reads_source_snapshots_without_writes_or_overriding_eligibility():
    candidate_id, matrix_id = uuid4(), uuid4()
    candidate = {"candidate_id": candidate_id, "matrix_id": matrix_id, "source_contract_id": None,
                 "strategy_name": "INCOME_WHEEL", "structure_type": "CASH_SECURED_PUT", "execution_eligibility": None}
    original = SimpleNamespace(available=True, data={"candidate": candidate}, as_of=None, observed_at=None, policy_sha256="a" * 64, model_version="test")
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    with patch("options.api.option_candidate_detail", return_value=original), patch("options.api.get_db_cursor") as connection, patch("options.api.retained_alert_evidence", return_value={}), patch("options.api._configuration", return_value=SimpleNamespace(policy_sha256="a" * 64)):
        connection.return_value.__enter__.return_value = cursor
        response = option_alert_qualification(candidate_id).model_dump(mode="json")
    sql, parameters = cursor.execute.call_args.args
    assert sql.strip().startswith("SELECT")
    assert "ingestion.asset_type AS underlying_asset_type" in sql
    assert parameters == (str(candidate_id), None, matrix_id)
    assert response["data"]["persisted"] is False
    assert response["data"]["assessment"]["execution_permission"] is False
    assert response["data"]["original_execution_eligibility"] is None
    assert "source_snapshots" not in original.data


def test_alert_qualification_links_oi_and_gamma_with_original_cutoffs():
    candidate_id, matrix_id = uuid4(), uuid4()
    observed = datetime(2026, 9, 16, 15, 30, tzinfo=UTC)
    candidate = {"candidate_id": candidate_id, "matrix_id": matrix_id, "underlying": "SPY", "observed_time": observed,
                 "market_data_time": observed - timedelta(minutes=15), "source_contract_id": 1,
                 "strategy_name": "INCOME_WHEEL", "structure_type": "CASH_SECURED_PUT", "execution_eligibility": None}
    original = SimpleNamespace(available=True, data={"candidate": candidate}, as_of=observed, observed_at=observed, policy_sha256="a" * 64, model_version="test")
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[{"snapshot_id": str(uuid4()), "contract_id": 1}], [{"open_interest": 100}], [{"gamma_profile_id": "retained"}]]
    with patch("options.api.option_candidate_detail", return_value=original), patch("options.api.get_db_cursor") as connection, patch("options.api.retained_alert_evidence", return_value={}) as adapter, patch("options.api._configuration", return_value=SimpleNamespace(policy_sha256="a" * 64)):
        connection.return_value.__enter__.return_value = cursor
        response = option_alert_qualification(candidate_id).model_dump(mode="json")
    assert len(cursor.execute.call_args_list) == 3
    for call in cursor.execute.call_args_list:
        sql, parameters = call.args
        assert sql.strip().startswith("SELECT")
        assert sql.count("%s") == len(parameters)
    oi_sql, oi_params = cursor.execute.call_args_list[1].args
    assert "open_interest_revised_value" in oi_sql
    assert oi_params == ("SPY", [1], datetime(2026, 9, 15).date(), observed)
    gamma_sql, gamma_params = cursor.execute.call_args_list[2].args
    assert "WHERE matrix_id = %s" in gamma_sql and "first_observed_at <= %s" in gamma_sql
    assert gamma_params == (matrix_id, "SPY", observed)
    assert adapter.call_args.args[0]["open_interest_evidence"] == [{"open_interest": 100}]
    assert response["data"]["evidence_adapter_version"] == "option_alert_retained_evidence_v2"
    assert "open_interest_evidence" not in original.data


@pytest.mark.parametrize("holding_until", [datetime(2026, 9, 16), datetime(2000, 1, 1, tzinfo=UTC), datetime(2100, 1, 1, tzinfo=UTC)])
def test_alert_qualification_rejects_invalid_horizon_before_any_reads(holding_until):
    with patch("options.api.option_candidate_detail", side_effect=AssertionError("unexpected read")), pytest.raises(HTTPException) as error:
        option_alert_qualification(uuid4(), holding_until)
    assert error.value.status_code == 422


def test_alert_qualification_event_queries_keep_original_source_and_resolve_before_scope():
    observed = datetime.now(UTC) - timedelta(hours=1)
    candidate = {"candidate_id": uuid4(), "matrix_id": uuid4(), "underlying": "SPY", "observed_time": observed,
                 "strategy_name": "INCOME_WHEEL", "structure_type": "CASH_SECURED_PUT"}
    original = SimpleNamespace(available=True, data={"candidate": candidate, "event_coverage_evidence": [{"source": "original-calendar"}]},
                               as_of=observed, observed_at=observed, policy_sha256="a" * 64, model_version="test")
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    end = datetime.now(UTC) + timedelta(days=1)
    with patch("options.api.option_candidate_detail", return_value=original), patch("options.api.get_db_cursor") as connection, patch("options.api.retained_alert_evidence", return_value={}) as adapter, patch("options.api._configuration", return_value=SimpleNamespace(policy_sha256="a" * 64)):
        connection.return_value.__enter__.return_value = cursor
        response = option_alert_qualification(candidate["candidate_id"], end).model_dump(mode="json")
    assert len(cursor.execute.call_args_list) == 3
    for call in cursor.execute.call_args_list[1:]:
        sql, parameters = call.args
        assert sql.count("%s") == len(parameters)
        assert sql.index("SELECT DISTINCT ON") < sql.index("SELECT * FROM latest WHERE affected_underlying")
        assert parameters[0] == ["original-calendar"] and parameters[1] == observed
        assert "scheduled_time BETWEEN" not in sql
    assert adapter.call_args.kwargs["holding_until"] == end
    assert response["data"]["event_horizon"]["status"] == "UNAVAILABLE"
    assert response["data"]["persisted"] is False


@pytest.mark.parametrize("offset", [0, 500])
def test_eligible_chain_is_independent_of_detectors_and_keeps_empty_page_totals(offset):
    cursor = MagicMock()
    cursor.fetchone.return_value = {"total": 8, "rows": [], "coverage": [{
        "underlying": "SPY", "market_time": "2026-09-16T19:00:00+00:00", "observed_time": "2026-09-16T19:15:00+00:00",
    }]}
    configuration = SimpleNamespace(policy_sha256="a" * 64, configuration_sha256="b" * 64, settings=SimpleNamespace(underlyers=("SPY", "QQQ")))
    with patch("options.api._configuration", return_value=configuration), patch("options.api._schema_available", return_value=True), patch("options.api.get_db_cursor") as connection:
        connection.return_value.__enter__.return_value = cursor
        payload = option_eligible_chain(EligibleChainQuery(
            session_date="2026-09-16", filters=[{"field": "absolute_delta", "minimum": 0.2, "maximum": 0.4}], offset=offset,
        )).model_dump(mode="json")
    sql, parameters = cursor.execute.call_args.args
    assert sql.count("%s") == len(parameters)
    assert "option_strategy_candidates" not in sql and "option_board" not in sql
    assert "snapshot.iv_converged" in sql and "snapshot.model_mark > 0" in sql
    assert "snapshot.first_observed_at <= latest.observed_time" in sql
    assert "COALESCE(snapshot.revised_observed_at, snapshot.first_observed_at) <= latest.observed_time" in sql
    assert "snapshot.market_data_time <= latest.market_time" in sql
    assert "NULLIF(snapshot.open_interest, 0)" in sql
    assert "absolute_delta >= %s AND absolute_delta <= %s" in sql
    assert "SELECT COUNT(*) FROM filtered" in sql
    assert "jsonb_agg(to_jsonb(page) ORDER BY" in sql
    assert parameters[-4:] == (0.2, 0.4, 100, offset)
    assert payload["data"]["total"] == 8
    assert payload["data"]["missing_underlyers"] == ["QQQ"]
    assert payload["data"]["coverage_status"] == "PARTIAL"


@pytest.mark.parametrize("filters", [
    [{"field": "absolute_delta", "minimum": 1.1}], [{"field": "calendar_dte", "minimum": 1.5}],
    [{"field": "local_iv", "minimum": 0.5, "maximum": 0.2}], [{"field": "local_iv"}],
    [{"field": "local_iv", "minimum": float("nan")}], [{"field": "sql", "minimum": 0}],
    [{"field": "local_iv", "minimum": 0}, {"field": "local_iv", "maximum": 1}],
])
def test_eligible_chain_rejects_invalid_filters(filters):
    with pytest.raises(ValueError):
        EligibleChainQuery(filters=filters)


def test_eligible_chain_zero_and_signed_bounds_are_preserved():
    sql, parameters = contract_filter_sql(EligibleChainQuery(contract_type="PUT", filters=[
        {"field": "local_theta_per_day", "minimum": -0.2, "maximum": 0},
        {"field": "open_interest", "minimum": 0},
    ]))
    assert sql == "contract_type = %s AND local_theta_per_day >= %s AND local_theta_per_day <= %s AND open_interest >= %s"
    assert parameters == ("PUT", -0.2, 0, 0)


def test_options_health_exposes_delayed_read_only_contract():
    payload = option_health().model_dump(mode="json")
    assert payload["data_tier"] == "15-MINUTE DELAYED RESEARCH DATA"
    assert payload["data"]["read_only"] is True
    assert payload["data"]["schema_ready"] is True
    assert payload["data"]["valuation_policy_version"] == "option_valuation_v1"
    assert len(payload["data"]["valuation_policy_sha256"]) == 64
    settlement = payload["data"]["settlement_valuation_policy"]
    assert settlement["version"] == "option_settlement_valuation_v1"
    assert settlement["adjusted"] is False
    assert settlement["iv_context_ready"] is False
    assert payload["data"]["event_calendar"]["coverage_required"] is True
    assert payload["data"]["event_calendar"]["maximum_age_seconds"] == 43200
    assert payload["data"]["event_calendar"]["status"] in {
        "UNCONFIGURED", "CONFIGURED_COVERAGE_REQUIRED",
    }
    workbench = payload["data"]["candidate_workbench"]
    # An empty cohort is a legitimate state after a purge, so assert the shape of the
    # contract rather than the presence of rows.
    assert isinstance(workbench["available"], bool)
    assert workbench["candidate_count"] >= 0
    assert workbench["available"] is (workbench["candidate_count"] > 0)
    publication = payload["data"]["board_publication"]
    assert publication["expected_underlyings"] == 13
    assert len(publication["selector_sha256"]) == 64
    assert publication["publishable"] is (
        publication["covered_underlyings"] == publication["expected_underlyings"]
    )


def test_ticker_event_calendar_fails_closed_when_source_is_unconfigured(monkeypatch):
    monkeypatch.setattr(
        "options.api._configuration",
        lambda: SimpleNamespace(
            settings=SimpleNamespace(
                event_calendar_provider=None,
                event_calendar_max_age_seconds=43200,
                fixed_etf_underlyers=("SPY", "QQQ", "IWM"),
            ),
            policy_sha256="a" * 64,
        ),
    )
    payload = option_event_calendar_reference(
        "AAPL", days_forward=30
    ).model_dump(mode="json")

    assert payload["available"] is False
    assert payload["reason"] == "EVENT_CALENDAR_UNCONFIGURED"
    assert payload["data"]["earnings_state"] == "UNAVAILABLE"
    assert payload["data"]["fed_state"] == "UNAVAILABLE"
    assert payload["data"]["events"] == []


def test_event_window_state_requires_coverage_before_absence_means_clear():
    assert _event_window_state("EARNINGS", [], []) == "UNAVAILABLE"
    coverage = [{"event_type": "EARNINGS"}]
    assert _event_window_state("EARNINGS", coverage, []) == "CLEAR"
    assert _event_window_state(
        "EARNINGS",
        coverage,
        [{"event_type": "EARNINGS", "status": "SCHEDULED"}],
    ) == "BLOCKED"
    assert _event_window_state(
        "EARNINGS",
        coverage,
        [{"event_type": "EARNINGS", "status": "CANCELED"}],
    ) == "CLEAR"


def test_event_asset_type_uses_equity_reference_outside_option_universe(monkeypatch):
    reference = type("Reference", (), {"security_type": "ETF"})()
    monkeypatch.setattr(
        "options.api.EquityReferenceRepository.get_security_as_of",
        lambda self, symbol, context: reference,
    )

    assert _event_asset_type("ARKK", datetime.now(timezone.utc), ()) == "ETF"


def test_options_gamma_returns_profiles_for_the_active_policy():
    payload = option_gamma(
        underlyer=None, scope="TOTAL", include_curve=False
    ).model_dump(mode="json")
    data = payload["data"]
    assert data["scope"] == "TOTAL"
    assert data["gamma_policy_version"]
    assert len(data["gamma_policy_sha256"]) == 64
    assert "does not identify who is long or short" in data["dealer_convention_note"]
    for profile in data["profiles"]:
        assert profile["regime_at_spot"] in {
            "POSITIVE_GAMMA", "NEGATIVE_GAMMA", "UNDETERMINED"
        }
        assert profile["dealer_convention"] in {
            "DEALER_LONG_CALLS_SHORT_PUTS", "DEALER_SHORT_CALLS_LONG_PUTS"
        }
        assert 0 <= profile["coverage_fraction"] <= 1
        assert profile["call_gamma_notional_per_percent"] >= 0
        assert profile["put_gamma_notional_per_percent"] >= 0
        # The board query must not carry the curve payload.
        assert profile["strike_profile"] is None


def test_options_gamma_curve_is_opt_in_and_ordered():
    payload = option_gamma(
        underlyer="SPY", scope="TOTAL", include_curve=True
    ).model_dump(mode="json")
    profiles = payload["data"]["profiles"]
    if not profiles:
        return
    profile = profiles[0]
    curve = profile["strike_profile"]
    assert curve is not None
    assert len(curve) == profile["strike_count"]
    strikes = [float(row["strike"]) for row in curve]
    assert strikes == sorted(strikes)


def test_options_gamma_scope_filter_is_applied():
    payload = option_gamma(
        underlyer=None, scope="ZERO_DTE", include_curve=False
    ).model_dump(mode="json")
    assert payload["data"]["scope"] == "ZERO_DTE"
    assert all(
        profile["scope"] == "ZERO_DTE" for profile in payload["data"]["profiles"]
    )


def test_options_universe_returns_configured_or_persisted_members():
    payload = option_universe().model_dump(mode="json")
    assert len(payload["data"]) == 13
    assert {row["ticker"] for row in payload["data"]} >= {"AAPL", "SPY", "IWM"}


def test_options_flow_exposes_activity_without_inferred_direction():
    payload = option_flow(underlyer="SPY").model_dump(mode="json")
    data = payload["data"]

    if data["session_date"] is None:
        pytest.skip("no completed matrix under the active market-data policy")
    assert data["selected"]
    assert data["session_date"]
    assert data["directional_flow_available"] is False
    assert data["quote_liquidity"] == "NOT_AVAILABLE"
    assert data["trade_tape_scope"] == "EXCLUDED_FROM_TOTALS_WATCHLIST_BIASED"
    assert "not transacted premium or net flow" in data["definitions"]["premium_activity"]
    for row in data["underlyers"]:
        assert row["total_volume"] == row["call_volume"] + row["put_volume"]
        assert row["total_open_interest"] == (
            row["call_open_interest"] + row["put_open_interest"]
        )
        assert 0 <= row["retention_fraction"] <= 1
    if data["selected_summary"]:
        assert data["selected_summary"]["underlying"] == data["selected"]
        assert all(row["contract_count"] > 0 for row in data["expirations"])
        strikes = [
            (row["expiration_date"], float(row["strike"]))
            for row in data["strikes"]
        ]
        assert strikes == sorted(strikes)
        assert len(data["top_contracts"]) <= 24


def test_package_filters_preserve_all_legs_before_counting_and_paging():
    cursor = MagicMock()
    cursor.fetchone.return_value = {"total": 0, "selected": 0, "suppressed": 0, "rejected": 0}
    cursor.fetchall.return_value = []
    configuration = SimpleNamespace(policy_sha256="a" * 64, configuration_sha256="b" * 64, strategy_policy_sha256="c" * 64)
    with patch("options.api._configuration", return_value=configuration), patch("options.api.get_db_cursor") as connection, patch(
        "options.api._strategy_schema_available", return_value=True
    ):
        connection.return_value.__enter__.return_value = cursor
        payload = option_candidates(
            category="DEFINED_RISK_INCOME", structured_only=True, status="SELECTED",
            session_date=datetime(2026, 9, 16).date(), minimum_dte=21, maximum_dte=45,
            maximum_capital=5000, sort="CAPITAL_ASC", limit=5, offset=10,
        ).model_dump(mode="json")
    assert len(cursor.execute.call_args_list) == 2
    for call in cursor.execute.call_args_list:
        query, parameters = call.args
        assert query.count("%s") == len(parameters)
        assert "candidate.structure_type = ANY(%s::text[])" in query
        assert "candidate.candidate_kind <> 'RESEARCH_ONLY'" in query
        assert "COUNT(*) FROM option_candidate_legs AS complete_leg" in query
        assert "ELSE -1 END" in query
        assert "analysis.status = 'COMPLETE'" in query
        assert "NOT EXISTS (SELECT 1 FROM option_candidate_legs AS dated_leg" in query
        assert "candidate.capital_at_risk <= %s" in query
        assert parameters[3] == datetime(2026, 9, 16).date()
        assert set(parameters[5]) == {"PUT_CREDIT_VERTICAL", "CALL_CREDIT_VERTICAL", "IRON_CONDOR"}
        assert parameters[6:9] == (21, 45, 5000)
    query, parameters = cursor.execute.call_args.args
    assert "ORDER BY leg.leg_index" in query
    assert "candidate.capital_at_risk ASC NULLS LAST" in query
    assert parameters[-2:] == (5, 10)
    assert payload["data"]["rows"] == []
    assert payload["data"]["selection_basis"] == "LATEST_COMPLETE_MATRIX_PER_UNDERLYING"
    assert payload["data"]["session_date"] == "2026-09-16"


@pytest.mark.parametrize("filters", [
    {"minimum_dte": -1}, {"minimum_dte": 46, "maximum_dte": 45},
    {"maximum_dte": 366}, {"maximum_capital": float("nan")}, {"maximum_capital": -1},
])
def test_package_invalid_filters_do_not_read_database(filters):
    with patch("options.api.get_db_cursor", side_effect=AssertionError("database accessed")):
        payload = option_candidates(limit=5, offset=0, **filters).model_dump(mode="json")
    assert payload["reason"] == "INVALID_PACKAGE_FILTER"


def test_candidate_workbench_exposes_typed_delayed_research_contract():
    payload = option_candidates(limit=5, offset=0).model_dump(mode="json")
    assert payload["reason"] in {None, "NO_STRATEGY_RESULTS"}
    assert payload["data"]["title"] == "Weekly Research Candidates"
    assert payload["data"]["quote_liquidity"] == "NOT_AVAILABLE"
    assert payload["data"]["execution_mode"] == "READ_ONLY_RESEARCH"
    assert payload["data"]["serving_mode"] in {
        "CURRENT_POLICY", "HISTORICAL_PREVIOUS_POLICY",
    }
    assert payload["data"]["limit"] == 5
    assert set(payload["data"]["status_counts"]) == {
        "selected",
        "suppressed",
        "rejected",
    }
    if payload["data"]["rows"]:
        detail = option_candidate_detail(
            UUID(payload["data"]["rows"][0]["candidate_id"])
        ).model_dump(mode="json")
        assert detail["available"] is True
        assert detail["data"]["execution_mode"] == "READ_ONLY_RESEARCH"
        assert isinstance(detail["data"]["market_event_evidence"], list)
        assert isinstance(detail["data"]["event_coverage_evidence"], list)


def _research_evidence_candidate():
    observed = datetime(2026, 9, 18, 20, tzinfo=UTC)
    return {
        "candidate_id": UUID("00000000-0000-0000-0000-000000000001"),
        "candidate_identity": "f" * 64,
        "matrix_id": UUID("00000000-0000-0000-0000-000000000002"),
        "underlying": "AAPL",
        "policy_sha256": "c" * 64,
        "market_data_time": observed - timedelta(minutes=15),
        "observed_time": observed,
        "valid_until": observed + timedelta(minutes=1),
        "strategy_name": "DIRECTIONAL_LONG_PREMIUM",
        "strategy_version": "directional_long_premium_v1",
        "display_name": "Directional Long Premium",
        "candidate_kind": "MULTI_LEG",
        "structure_type": "LONG_CALL",
    }


def _research_evidence_rows(candidate=None, snapshot=None):
    from dataclasses import replace
    from options.domain import ContractType
    from options.outcome_contracts import assess_option_package
    from options.stock_behavior_gates import evaluate_option_stock_behavior
    from options.strategies.domain import OptionSide, StructureType
    from test_outcome_contracts import package_candidate
    from test_strategy_payoff import leg

    candidate = candidate or _research_evidence_candidate()
    package_candidate_row = replace(
        package_candidate(
            (leg(0, "100", "5", OptionSide.BUY, ContractType.CALL),),
            StructureType.LONG_CALL,
        ),
        candidate_id=candidate["candidate_id"], identity_sha256=candidate["candidate_identity"],
        matrix_id=candidate["matrix_id"], underlyer=candidate["underlying"],
        strategy_name=candidate["strategy_name"], strategy_version=candidate["strategy_version"],
        policy_sha256=candidate["policy_sha256"], market_data_time=candidate["market_data_time"],
        observed_time=candidate["observed_time"], valid_until=candidate["valid_until"],
    )
    package = assess_option_package(package_candidate_row, valuation_policy_sha256="e" * 64)
    stock = evaluate_option_stock_behavior(
        snapshot, candidate_id=candidate["candidate_id"],
        candidate_identity_sha256=candidate["candidate_identity"],
        matrix_id=candidate["matrix_id"], underlyer=candidate["underlying"],
        strategy_name=candidate["strategy_name"], structure_type=StructureType.LONG_CALL,
        directional_thesis="BULLISH", decision_at=candidate["observed_time"],
        option_strategy_version=candidate["strategy_version"],
        option_strategy_policy_sha256=candidate["policy_sha256"],
        option_market_time=candidate["market_data_time"],
        option_observed_at=candidate["observed_time"], entry_deadline=candidate["valid_until"],
    )
    return [dict(
        payload_text=assessment.canonical_json(), payload_sha256=assessment.sha256,
        recorded_at=candidate["observed_time"] + timedelta(seconds=1), **columns,
    ) for assessment, columns in (
        (stock, dict(detector_policy_version=stock.detector_policy_version,
                     detector_policy_sha256=stock.detector_policy_sha256,
                     decision_at=stock.decision_at, disposition=stock.disposition,
                     stock_snapshot_id=stock.stock_snapshot_id)),
        (package, dict(assessment_policy_version=package.assessment_policy_version,
                       assessment_policy_sha256=package.assessment_policy_sha256,
                       assessed_at=package.assessed_at, assessment_status=package.status,
                       package_terms_sha256=package.package_terms_sha256,
                       valuation_policy_sha256=package.valuation_policy_sha256)),
    )]


def test_candidate_research_evidence_reads_persisted_assessments_with_bounds():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        {"stock_behavior_ready": True, "package_assessment_ready": True},
        *_research_evidence_rows(),
    ]

    result = _candidate_research_evidence(cursor, _research_evidence_candidate())

    assert result["version"] == "option_candidate_research_evidence_v1"
    assert result["detector"]["id"] == "DIRECTIONAL_LONG_PREMIUM"
    assert result["category_ids"] == ["MOMENTUM"]
    assert result["structure"] == "LONG_CALL"
    assert result["stock_behavior"]["availability"] == "AVAILABLE"
    assert result["stock_behavior"]["assessment"]["disposition"] == "UNAVAILABLE"
    assert result["package_assessment"]["assessment"]["status"] == "READY"
    assert result["probability"] == {
        "status": "UNAVAILABLE",
        "value": None,
        "reason": "NO_QUALIFIED_CALIBRATION_REPORT",
        "target": None,
        "outcome_basis": None,
        "report_sha256": None,
    }
    assert result["calculation_performed"] is False
    assert result["execution_permission"] is False
    assert "setup_package_binding" not in result
    assessment_calls = cursor.execute.call_args_list[1:]
    assert all("LIMIT 1" in call.args[0] for call in assessment_calls)
    from options.stock_behavior_gates import STOCK_BEHAVIOR_GATE_POLICY
    from options.outcome_contracts import PACKAGE_ASSESSMENT_POLICY
    for call, policy in zip(assessment_calls, (STOCK_BEHAVIOR_GATE_POLICY, PACKAGE_ASSESSMENT_POLICY)):
        assert call.args[1] == (str(_research_evidence_candidate()["candidate_id"]), policy.version, policy.sha256)
        assert "policy_version = %s" in call.args[0] and "policy_sha256 = %s" in call.args[0]


@pytest.mark.parametrize("kind", ["stock_behavior", "package_assessment"])
@pytest.mark.parametrize(("mutation", "reason"), [
    ("policy", "PERSISTED_ASSESSMENT_POLICY_UNSUPPORTED"),
    ("hash", "PERSISTED_ASSESSMENT_HASH_MISMATCH"),
    ("noncanonical", "PERSISTED_ASSESSMENT_HASH_MISMATCH"),
    ("missing", "PERSISTED_ASSESSMENT_PAYLOAD_INVALID"),
    ("candidate", "PERSISTED_ASSESSMENT_CANDIDATE_MISMATCH"),
    ("matrix", "PERSISTED_ASSESSMENT_CANDIDATE_MISMATCH"),
    ("column", "PERSISTED_ASSESSMENT_COLUMN_MISMATCH"),
    ("clock", "PERSISTED_ASSESSMENT_CLOCK_MISMATCH"),
])
def test_candidate_research_evidence_rejects_incompatible_evidence(kind, mutation, reason):
    import hashlib

    rows = _research_evidence_rows()
    row = rows[0 if kind == "stock_behavior" else 1]
    payload = json.loads(row["payload_text"])
    if mutation == "policy":
        field = "detector_policy_sha256" if kind == "stock_behavior" else "assessment_policy_sha256"
        payload[field] = row[field] = "0" * 64
    elif mutation == "missing":
        payload.pop("gates" if kind == "stock_behavior" else "candidate_id")
    elif mutation == "candidate":
        payload["candidate_id"] = str(uuid4())
    elif mutation == "matrix":
        payload["matrix_id" if kind == "stock_behavior" else "option_matrix_id"] = str(uuid4())
    elif mutation == "column":
        row["disposition" if kind == "stock_behavior" else "assessment_status"] = "INVALID"
    elif mutation == "clock":
        row["recorded_at"] = _research_evidence_candidate()["observed_time"] - timedelta(seconds=1)
    row["payload_text"] = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    if mutation == "noncanonical":
        row["payload_text"] += " "
    row["payload_sha256"] = hashlib.sha256(row["payload_text"].encode("ascii")).hexdigest()
    if mutation == "hash":
        row["payload_sha256"] = "0" * 64
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        {"stock_behavior_ready": True, "package_assessment_ready": True}, *rows,
    ]

    result = _candidate_research_evidence(cursor, _research_evidence_candidate())

    assert result[kind] == {"availability": "UNAVAILABLE", "reason": reason, "assessment": None}
    assert cursor.execute.call_count == 3


def test_candidate_research_evidence_preserves_eligible_v1_snapshot():
    from test_stock_behavior_gates import current_aapl_snapshot

    snapshot = current_aapl_snapshot()
    candidate = _research_evidence_candidate()
    candidate.update(
        market_data_time=snapshot.market_time, observed_time=snapshot.available_at,
        valid_until=snapshot.valid_until,
    )
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        {"stock_behavior_ready": True, "package_assessment_ready": True},
        *_research_evidence_rows(candidate, snapshot),
    ]

    result = _candidate_research_evidence(cursor, candidate)

    assert result["stock_behavior"]["availability"] == "AVAILABLE"
    assessment = result["stock_behavior"]["assessment"]
    assert assessment["disposition"] == "ELIGIBLE_RESEARCH"
    assert assessment["stock_payload_sha256"] == snapshot.sha256
    assert assessment["execution_permission"] is False
    assert result["package_assessment"]["availability"] == "AVAILABLE"


@pytest.mark.skipif(os.getenv("OPTION_ALERT_READONLY_TESTS") != "1", reason="explicit read-only PostgreSQL check required")
def test_candidate_research_evidence_retained_payloads_read_only():
    from database import get_db_connection
    from psycopg2.extras import RealDictCursor

    with get_db_connection() as connection:
        connection.rollback()
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SET LOCAL statement_timeout = '5s'")
                cursor.execute("""
                    SELECT candidate.*, registry.display_name
                    FROM option_strategy_candidates AS candidate
                    JOIN option_strategy_registry AS registry
                      ON registry.strategy_name = candidate.strategy_name
                     AND registry.strategy_version = candidate.strategy_version
                    JOIN option_stock_behavior_assessments AS stock USING(candidate_id)
                    JOIN option_package_assessments AS package USING(candidate_id)
                    WHERE stock.recorded_at <= NOW() AND package.recorded_at <= NOW()
                    ORDER BY stock.recorded_at DESC
                    LIMIT 1
                """)
                candidate = cursor.fetchone()
                if candidate is None:
                    pytest.skip("no retained paired assessments")
                evidence = _candidate_research_evidence(cursor, candidate)
                assert evidence["stock_behavior"]["availability"] == "AVAILABLE", evidence["stock_behavior"]["reason"]
                assert evidence["package_assessment"]["availability"] == "AVAILABLE", evidence["package_assessment"]["reason"]
                assert evidence["execution_permission"] is False
                assert evidence["probability"]["value"] is None
        finally:
            connection.rollback()
            connection.set_session(readonly=False, isolation_level="READ COMMITTED")


def test_candidate_research_evidence_reports_missing_schemas_without_queries():
    cursor = MagicMock()
    cursor.fetchone.return_value = {
        "stock_behavior_ready": False,
        "package_assessment_ready": False,
    }

    result = _candidate_research_evidence(cursor, _research_evidence_candidate())

    assert cursor.execute.call_count == 1
    assert result["stock_behavior"]["reason"] == (
        "STOCK_BEHAVIOR_ASSESSMENT_SCHEMA_UNAVAILABLE"
    )
    assert result["package_assessment"]["reason"] == (
        "PACKAGE_ASSESSMENT_SCHEMA_UNAVAILABLE"
    )


def test_candidate_research_evidence_distinguishes_not_recorded_rows():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        {"stock_behavior_ready": True, "package_assessment_ready": True},
        None,
        None,
    ]

    result = _candidate_research_evidence(cursor, _research_evidence_candidate())

    assert result["stock_behavior"]["reason"] == (
        "STOCK_BEHAVIOR_ASSESSMENT_NOT_RECORDED"
    )
    assert result["package_assessment"]["reason"] == (
        "PACKAGE_ASSESSMENT_NOT_RECORDED"
    )


@pytest.mark.parametrize(("payload", "reason"), [
    ("not-json", "PERSISTED_ASSESSMENT_PAYLOAD_INVALID"),
    (json.dumps({"schema_version": "future_v2"}),
     "PERSISTED_ASSESSMENT_VERSION_UNSUPPORTED"),
])
def test_candidate_research_evidence_rejects_invalid_or_unknown_payloads(
    payload, reason,
):
    cursor = MagicMock()
    row = {
        "payload_text": payload,
        "payload_sha256": "a" * 64,
        "recorded_at": datetime(2026, 9, 18, 20, tzinfo=UTC),
    }
    cursor.fetchone.side_effect = [
        {"stock_behavior_ready": True, "package_assessment_ready": False},
        row,
    ]

    result = _candidate_research_evidence(cursor, _research_evidence_candidate())

    assert result["stock_behavior"] == {
        "availability": "UNAVAILABLE", "reason": reason, "assessment": None,
    }


def test_research_findings_expose_distinct_source_contracts():
    payload = option_candidates(
        persona="MOMENTUM",
        status="SELECTED",
        limit=100,
        offset=0,
    ).model_dump(mode="json")
    findings = [
        row for row in payload["data"]["rows"]
        if row["candidate_kind"] == "RESEARCH_ONLY"
    ]
    if not findings:
        pytest.skip("no research-only cohort persisted yet")
    assert all(row["source_contract_id"] for row in findings)
    assert all(row["source_contract_ticker"] for row in findings)
    assert len({row["source_contract_id"] for row in findings}) == len(findings)


def test_recommendation_api_exposes_persisted_signal_contract():
    payload = option_signals(limit=5, offset=0).model_dump(mode="json")

    assert payload["reason"] in {None, "NO_SIGNAL_EVENTS"}
    assert payload["data"]["limit"] == 5
    assert payload["data"]["offset"] == 0
    assert payload["data"]["execution_mode"] == "READ_ONLY_RESEARCH"
    assert set(payload["data"]["status_counts"]) == {
        "pending", "ready", "blocked", "expired",
    }
    for row in payload["data"]["rows"]:
        assert isinstance(row["legs"], list)
        assert row["expected_leg_count"] == len(row["legs"])


def test_discovery_catalog_is_static_and_does_not_claim_market_readiness():
    with patch("options.api.get_db_cursor", side_effect=AssertionError("database accessed")), patch(
        "options.api._configuration", side_effect=AssertionError("runtime configuration accessed")
    ):
        payload = option_discovery_catalog()
    assert payload["version"] == "option_discovery_v1"
    assert len(payload["models"]) == 8
    assert "available" not in payload
    assert "execution_eligibility" not in payload


@pytest.mark.parametrize("scope", ["ALL", "STRUCTURED", "RESEARCH", "BOARD"])
@pytest.mark.parametrize("category", [None, "INCOME", "DEFINED_RISK_INCOME", "MOMENTUM", "NEUTRAL_VOL"])
def test_screener_category_filters_candidates_before_grouping(scope, category):
    configuration = SimpleNamespace(
        policy_sha256="a" * 64,
        configuration_sha256="b" * 64,
        strategy_policy_sha256="c" * 64,
        settings=SimpleNamespace(underlyers=("SPY",)),
    )
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    with patch("options.api._configuration", return_value=configuration), patch(
        "options.api.get_db_cursor"
    ) as connection, patch("options.api._strategy_schema_available", return_value=True), patch(
        "options.api._board_schema_available", return_value=True
    ):
        connection.return_value.__enter__.return_value = cursor
        payload = option_screener(
            scope=scope, category=category, minimum_dte=0, maximum_dte=60,
            minimum_volume=0, minimum_open_interest=0, minimum_volume_oi_ratio=0,
            limit=5, offset=10,
        ).model_dump(mode="json")
    query, parameters = cursor.execute.call_args.args
    assert query.count("%s") == len(parameters)
    assert parameters[18:20] == (configuration.configuration_sha256, scope)
    assert query.index("category_candidates AS MATERIALIZED") < query.index("detected AS MATERIALIZED")
    assert query.count("FROM category_candidates AS candidate") == 2
    assert "structure_type = ANY(%s::text[])" in query
    structures, duplicate_structures = parameters[24:26]
    assert structures == duplicate_structures
    expected = {
        None: None,
        "INCOME": {"CASH_SECURED_PUT"},
        "DEFINED_RISK_INCOME": {"PUT_CREDIT_VERTICAL", "CALL_CREDIT_VERTICAL", "IRON_CONDOR"},
        "MOMENTUM": {"LONG_CALL", "LONG_PUT", "CALL_DEBIT_VERTICAL", "PUT_DEBIT_VERTICAL", "SWEEP_LIKE_CLUSTER", "VOLUME_OI_ANOMALY"},
        "NEUTRAL_VOL": {"IRON_CONDOR", "CALL_BUTTERFLY", "PUT_BUTTERFLY", "SWEEP_LIKE_CLUSTER", "VOLUME_OI_ANOMALY", "VOLATILITY_DISTORTION"},
    }[category]
    assert (set(structures) if structures is not None else None) == expected
    assert parameters[-2:] == (5, 10)
    assert payload["data"]["category"] == category
    assert payload["data"]["rows"] == []


@pytest.mark.parametrize(("structured", "observations", "result_kind"), [
    (1, 0, "STRUCTURE_LEG"), (0, 1, "OBSERVATION"), (1, 1, "MIXED"),
])
def test_screener_category_result_kind_preserves_contract_row_semantics(structured, observations, result_kind):
    cursor = MagicMock()
    cursor.fetchall.return_value = [{
        "filtered_total": 1, "current_policy": True,
        "structure_types": ["IRON_CONDOR"] if structured else ["VOLUME_OI_ANOMALY"],
        "structured_candidate_count": structured, "research_candidate_count": observations,
        "market_data_time": datetime(2026, 9, 16, 19, tzinfo=UTC),
        "first_observed_at": datetime(2026, 9, 16, 19, 15, tzinfo=UTC),
    }]
    configuration = SimpleNamespace(
        policy_sha256="a" * 64, configuration_sha256="b" * 64,
        strategy_policy_sha256="c" * 64, settings=SimpleNamespace(underlyers=("SPY",)),
    )
    with patch("options.api._configuration", return_value=configuration), patch(
        "options.api.get_db_cursor"
    ) as connection, patch("options.api._strategy_schema_available", return_value=True), patch(
        "options.api._board_schema_available", return_value=True
    ):
        connection.return_value.__enter__.return_value = cursor
        payload = option_screener(
            minimum_dte=0, maximum_dte=60, minimum_volume=0,
            minimum_open_interest=0, minimum_volume_oi_ratio=0, limit=5, offset=0,
        ).model_dump(mode="json")
    row = payload["data"]["rows"][0]
    assert row["result_kind"] == result_kind
    assert "NEUTRAL_VOL" in row["category_ids"]
    assert "execution_eligibility" not in row
    assert "success_probability" not in row


def test_options_screener_exposes_detected_contract_metrics():
    payload = option_screener(
        session_date=None,
        scope="ALL",
        underlyer=None,
        contract_type=None,
        strategy=None,
        minimum_dte=0,
        maximum_dte=60,
        minimum_volume=0,
        minimum_open_interest=0,
        minimum_volume_oi_ratio=0,
        sort="PREMIUM_ACTIVITY",
        limit=5,
        offset=0,
    ).model_dump(mode="json")

    assert payload["reason"] in {None, "NO_DETECTED_CONTRACTS"}
    assert payload["data"]["scope"] == "ALL"
    assert payload["data"]["sort"] == "PREMIUM_ACTIVITY"
    assert payload["data"]["directional_flow_available"] is False
    assert payload["data"]["quote_liquidity"] == "NOT_AVAILABLE"
    assert payload["data"]["serving_mode"] in {
        "CURRENT_POLICY", "HISTORICAL_PREVIOUS_POLICY",
    }
    assert "not transacted premium" in payload["data"]["definitions"]["premium_activity"]
    contract_ids = [row["contract_id"] for row in payload["data"]["rows"]]
    assert len(contract_ids) == len(set(contract_ids))
    for row in payload["data"]["rows"]:
        assert row["contract_type"] in {"CALL", "PUT"}
        assert row["calendar_dte"] >= 0
        assert row["strategy_names"]


def test_performance_api_exposes_checkpoint_and_management_contract():
    payload = option_performance(
        cohort="RANK_LEADERS", days=14, limit=5, offset=0
    ).model_dump(mode="json")

    assert payload["reason"] in {None, "NO_SIGNAL_PERFORMANCE"}
    assert payload["data"]["days"] == 14
    assert payload["data"]["cohort"] == "RANK_LEADERS"
    assert payload["data"]["requested_cohort"] == "RANK_LEADERS"
    assert payload["data"]["limit"] == 5
    assert payload["data"]["valuation_mode"] == "RESEARCH_DELAYED_PROXY"
    assert payload["data"]["materialization_owner"] == "OPTION_WORKER"
    assert payload["data"]["entry_basis"] == "FIRST_RAW_RANK_LEADER_OCCURRENCE"
    assert payload["data"]["board_membership_exact"] is False
    assert "does not reconstruct historical Opportunity Board membership" in (
        payload["data"]["cohort_definition"]
    )
    assert payload["data"]["navigation_revalues"] is False
    assert isinstance(payload["data"]["current_mark_included"], bool)
    assert isinstance(payload["data"]["measurement_summary"], list)
    for row in payload["data"]["rows"]:
        assert row["structure_type"]
        assert row["capital_at_risk"] is not None
        assert all(
            checkpoint["status"] in {"AVAILABLE", "PENDING", "NOT_DUE"}
            for checkpoint in row["checkpoints"]
        )
        assert {
            checkpoint["measurement_type"] for checkpoint in row["checkpoints"]
        } <= {"15MIN", "30MIN", "60MIN", "CLOSE", "NEXT_OPEN"}
        if row["current_mark"]:
            assert isinstance(row["current_mark"]["legs"], list)
        assert row["candidate_rank"] == 1


def test_performance_api_can_include_all_structured_signals():
    board = option_performance(
        cohort="RANK_LEADERS", days=14, limit=200, offset=0
    ).model_dump(mode="json")
    all_signals = option_performance(
        cohort="ALL_SIGNALS", days=14, limit=200, offset=0
    ).model_dump(mode="json")

    assert board["data"]["total"] <= all_signals["data"]["total"]
    assert all_signals["data"]["cohort"] == "ALL_SIGNALS"
    assert all_signals["data"]["entry_basis"] == "ORIGINAL_SIGNAL_PACKAGE"


def test_performance_api_can_filter_historical_board_by_strategy():
    payload = option_performance(
        strategy="income_wheel", cohort="RANK_LEADERS",
        days=14, limit=200, offset=0,
    ).model_dump(mode="json")

    assert all(
        row["strategy_name"] == "INCOME_WHEEL"
        for row in payload["data"]["rows"]
    )


def test_legacy_opportunity_board_cohort_is_an_alias_for_rank_leaders():
    payload = option_performance(
        cohort="OPPORTUNITY_BOARD", days=14, limit=1, offset=0
    ).model_dump(mode="json")

    assert payload["data"]["cohort"] == "RANK_LEADERS"
    assert payload["data"]["requested_cohort"] == "OPPORTUNITY_BOARD"
    assert payload["data"]["board_membership_exact"] is False


def test_exact_board_performance_is_prospective_publication_membership():
    payload = option_performance(
        cohort="BOARD_PUBLICATIONS", days=14, limit=1, offset=0
    ).model_dump(mode="json")

    assert payload["data"]["cohort"] == "BOARD_PUBLICATIONS"
    assert payload["data"]["requested_cohort"] == "BOARD_PUBLICATIONS"
    assert payload["data"]["entry_basis"] == "FIRST_PUBLISHED_BOARD_MEMBERSHIP"
    assert payload["data"]["board_membership_exact"] is True
    assert "Coverage begins with the first prospective publication" in (
        payload["data"]["cohort_definition"]
    )


def test_performance_checkpoints_distinguish_available_pending_and_not_due():
    market_time = datetime(2026, 9, 2, 14, 0, tzinfo=UTC)
    outcome = {"measurement_type": "15MIN", "net_pnl": "10.00"}
    row = {"market_data_time": market_time, "outcomes": [outcome]}

    checkpoints = _performance_checkpoints(
        row,
        generated_at=market_time + timedelta(minutes=31),
        exchange=OptionExchangeCalendar(),
    )

    statuses = {
        checkpoint["measurement_type"]: checkpoint["status"]
        for checkpoint in checkpoints
    }
    assert statuses["15MIN"] == "AVAILABLE"
    assert statuses["30MIN"] == "PENDING"
    assert statuses["60MIN"] == "NOT_DUE"
    assert checkpoints[0]["outcome"] == outcome
    assert "outcomes" not in row


def test_opportunity_board_separates_structures_from_research_detectors():
    payload = option_opportunities(per_strategy=1).model_dump(mode="json")

    assert payload["reason"] in {None, "NO_COMPLETE_BOARD_PUBLICATION"}
    assert payload["data"]["selection_basis"] == "IMMUTABLE_BOARD_PUBLICATION"
    assert payload["data"]["execution_mode"] == "READ_ONLY_RESEARCH"
    if payload["available"]:
        assert payload["data"]["serving_mode"] in {
            "CURRENT_POLICY", "HISTORICAL_PREVIOUS_POLICY",
        }
    assert payload["data"]["configured_underlyer_count"] == 13
    assert payload["data"]["covered_underlyer_count"] == len(
        payload["data"]["underlyers"]
    )
    assert all(
        row["candidate_kind"] != "RESEARCH_ONLY"
        for row in payload["data"]["structured"]
    )
    assert all(
        row["candidate_kind"] == "RESEARCH_ONLY"
        for row in payload["data"]["research_highlights"]
    )
    for row in (
        payload["data"]["structured"]
        + payload["data"]["research_highlights"]
    ):
        assert row["strategy_position"] == 1
        assert row["window_state"] in {"ACTIVE", "ELAPSED", "UNBOUNDED"}
        assert isinstance(row["legs"], list)


def test_opportunity_board_reads_immutable_publication_members():
    cursor = MagicMock()
    publication_id = uuid4()
    matrix_id = uuid4()
    cursor.fetchone.side_effect = [
        {"ready": True},
        {"ready": True},
        {
            "publication_id": publication_id,
            "source_matrix_ids": [matrix_id],
            "market_data_time": datetime(2026, 9, 4, 15, 0, tzinfo=UTC),
            "observed_time": datetime(2026, 9, 4, 15, 15, tzinfo=UTC),
            "covered_underlying_count": 1,
            "scheduled_cycle": datetime(2026, 9, 4, 15, 0, tzinfo=UTC),
            "published_at": datetime(2026, 9, 4, 15, 16, tzinfo=UTC),
            "selector_version": "option_board_selector_v1",
            "selector_sha256": "b" * 64,
            "selection_evidence": {"persisted_members": 0},
        },
    ]
    cursor.fetchall.side_effect = [[{
        "underlying": "SPY",
        "matrix_id": matrix_id,
        "analysis_status": "COMPLETE",
        "market_data_time": datetime(2026, 9, 4, 15, 0, tzinfo=UTC),
        "observed_time": datetime(2026, 9, 4, 15, 15, tzinfo=UTC),
        "matrix_age_seconds": 0,
        "structured_count": 0,
        "research_count": 0,
        "suppressed_count": 0,
        "recommendation_count": 0,
        "blocked_count": 0,
    }], []]

    @contextmanager
    def get_cursor():
        yield cursor

    with patch("options.api.get_db_cursor", get_cursor):
        result = option_opportunities(per_strategy=1)

    assert result.data["structured"] == []
    assert result.data["publication_id"] == publication_id
    assert result.data["selection_basis"] == "IMMUTABLE_BOARD_PUBLICATION"
    opportunity_sql = cursor.execute.call_args_list[-1].args[0]
    assert "FROM option_board_members AS member" in opportunity_sql
    assert "member.publication_id = %s" in opportunity_sql
    assert "member.board_position <= %s" in opportunity_sql
    assert "first_selected_contracts" not in opportunity_sql


def test_data_quality_explains_retention_and_unknown_references():
    payload = option_data_quality(limit=5).model_dump(mode="json")

    assert payload["data"]["definitions"]["unknown_references"].startswith(
        "Provider option tickers absent"
    )
    assert len(payload["data"]["retention_criteria"]) == 6
    assert len(payload["data"]["model_eligibility_criteria"]) == 3
    assert payload["data"]["unknown_reference_gate"] == {
        "maximum_count": 20,
        "maximum_fraction": 0.01,
        "rule": "Reference drift fails above the lower of the count and fraction thresholds.",
    }
    for run in payload["data"]["runs"]:
        assert run["excluded_row_count"] == max(
            run["received_row_count"] - run["retained_row_count"],
            0,
        )
        assert 0 <= run["catalog_coverage_fraction"] <= 1
        assert 0 <= run["retention_fraction"] <= 1
        assert all(
            {"code", "label", "count"} <= set(reason)
            for reason in run["exclusion_breakdown"]
        )


def test_options_routes_are_registered_on_main_app():
    paths = {route.path for route in app.routes}
    assert {
        "/api/options/health",
        "/api/options/universe",
        "/api/options/chain/{underlyer}",
        "/api/options/analysis/{underlyer}",
        "/api/options/flow",
        "/api/options/data-quality",
        "/api/options/opportunities",
        "/api/options/screener",
        "/api/options/candidates",
        "/api/options/candidates/{candidate_id}",
        "/api/options/scenarios/{candidate_id}",
        "/api/options/signals",
        "/api/options/performance",
    } <= paths
