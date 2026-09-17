from datetime import date, datetime, timedelta, timezone
from copy import deepcopy
import json
from pathlib import Path
import sys
from unittest.mock import MagicMock
from uuid import uuid4

import exchange_calendars
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from equity.historical_universe import HistoricalUniversePolicy, historical_universe_run_id
from equity.polygon import normalize_security_reference, sha256_json
from scripts.audit_equity_research_inputs import (
    classify_identity_pair, replay_symbol_change_candidate, review_membership_source, summarize_universes, summarize_price_coverage,
)


def universe(session, **changes):
    return {"session_date": session, "policy_sha256": "a" * 64,
            "status": "COMPLETE", "admitted_members": 60,
            "members": 60, "availability_valid": True} | changes


def test_audit_uses_exchange_sessions_not_calendar_days():
    result = summarize_universes([
        universe(date(2026, 9, 4)), universe(date(2026, 9, 8)),
    ], "a" * 64)
    assert result["blockers"] == []
    assert result["memberships"] == 120


def test_duplicate_and_missing_dates_cannot_look_complete():
    result = summarize_universes([
        universe(date(2026, 9, 8)), universe(date(2026, 9, 8)),
        universe(date(2026, 9, 10)),
    ], "a" * 64)
    assert set(result["blockers"]) == {"DUPLICATE_UNIVERSE_SESSION", "MISSING_UNIVERSE_SESSION"}
    assert result["missing_session_examples"] == ["2026-09-09"]


def test_policy_coverage_and_availability_failures_are_explicit():
    result = summarize_universes([
        universe(date(2026, 9, 8), status="DEGRADED", members=59,
                 availability_valid=False, policy_sha256="b" * 64),
    ], "a" * 64)
    assert set(result["blockers"]) == {"UNIVERSE_POLICY_MISMATCH", "INCOMPLETE_UNIVERSE",
                                      "UNIVERSE_MEMBER_COUNT_MISMATCH", "UNIVERSE_AVAILABILITY_INVALID"}
    assert result["member_count_mismatch_examples"] == [{"session": "2026-09-08", "declared": 60, "stored": 59}]


def test_empty_universe_is_not_a_successful_audit():
    assert summarize_universes([], "a" * 64)["blockers"] == ["NO_HISTORICAL_UNIVERSES"]


def test_price_audit_distinguishes_immaturity_from_missing_paths():
    import pandas as pd
    sessions = [value.date() for value in pd.bdate_range("2026-08-03", periods=30)]
    rows = [{"session_date": session, "memberships": 10,
             "with_contiguous_warmup_252": 9 if index >= 2 else 5,
             "with_contiguous_forward_5": 8 if index + 5 < len(sessions) else 0,
             "with_contiguous_forward_10": 7 if index + 10 < len(sessions) else 0,
             "with_contiguous_forward_21": 6 if index + 21 < len(sessions) else 0}
            for index, session in enumerate(sessions)]
    result = summarize_price_coverage(rows, sessions, sessions[-1])
    assert result["maturity"]["21"] == {"mature_memberships": 90, "missing_forward_paths": 36, "immature_memberships": 210}
    assert result["first_session_90pct_warmup"] == sessions[2].isoformat()


def test_audit_enforces_read_only_snapshot_before_querying_data(monkeypatch):
    from scripts import audit_equity_research_inputs as audit

    cursor = MagicMock()
    cursor.fetchone.return_value = {"read_only": "on", "cutoff": "2026-09-12"}
    cursor.fetchall.side_effect = [[], []]
    manager = MagicMock()
    manager.__enter__.return_value = cursor
    monkeypatch.setattr(audit, "get_db_cursor", lambda: manager)

    result = audit.audit_inputs()

    statements = [call.args[0].strip() for call in cursor.execute.call_args_list]
    assert statements[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert "statement_timeout" in statements[1]
    assert all(statement.startswith("SELECT") for statement in statements[2:])
    assert result["transaction"]["read_only"] == "on"
    assert result["readiness"] == "NOT_CERTIFIED"
    assert result["study_settings_status"] == "NOT_FROZEN"


@pytest.mark.parametrize("changes, expected", [
    ({}, "IDENTIFIER_DRIFT_CANDIDATE"),
    ({"bar_cik": "123"}, "IDENTIFIER_DRIFT_CANDIDATE"),
    ({"bar_cik": "999"}, "ISSUER_OR_SHARE_CLASS_CONFLICT"),
    ({"bar_share_class_figi": "OTHER"}, "ISSUER_OR_SHARE_CLASS_CONFLICT"),
    ({"bar_security_type": "ETF"}, "ISSUER_OR_SHARE_CLASS_CONFLICT"),
    ({"bar_cik": None}, "INSUFFICIENT_IDENTITY_EVIDENCE"),
    ({"bar_share_class_figi": ""}, "INSUFFICIENT_IDENTITY_EVIDENCE"),
    ({"bar_security_type": None}, "INSUFFICIENT_IDENTITY_EVIDENCE"),
    ({"membership_cik": "INVALID", "bar_cik": "INVALID"}, "INSUFFICIENT_IDENTITY_EVIDENCE"),
])
def test_identity_classification_never_relies_on_ticker_alone(changes, expected):
    row = {"ticker": "TEST", "membership_cik": "0000123", "bar_cik": "0000123",
           "membership_share_class_figi": "SHARE", "bar_share_class_figi": "SHARE",
           "membership_security_type": "CS", "bar_security_type": "CS"} | changes
    assert classify_identity_pair(row) == expected


@pytest.fixture
def membership_source(tmp_path):
    session = date(2024, 3, 4)
    policy_hash = HistoricalUniversePolicy().policy_sha256
    payload = [{"ticker": ticker, "type": "CS", "active": True,
                "composite_figi": "SHARED_ID"} for ticker in ("DOC", "PEAK")]
    request_hash = sha256_json(payload)
    row = {
        "session_date": session, "policy_sha256": policy_hash,
        "source_request_sha256": request_hash,
        "observed_at": datetime(2026, 9, 4, tzinfo=timezone.utc),
        "admitted_members": 2, "stored_members": 1,
        "stored_tickers": ["PEAK"], "stored_ranks": [1],
        "universe_run_id": historical_universe_run_id(
            signal_date=session, policy_sha256=policy_hash,
            source_request_sha256=request_hash, member_tickers=["DOC", "PEAK"],
        ),
    }
    path = tmp_path / "tickers-CS_2024-03-04.json"
    path.write_text(json.dumps({"payload": payload, "sha256": request_hash}), encoding="utf-8")
    return row, path


@pytest.mark.parametrize("failure, expected", [
    ("missing", "NOT_RETAINED"),
    ("checksum", "CHECKSUM_INVALID"),
    ("request", "REQUEST_HASH_MISMATCH"),
])
def test_membership_review_rejects_missing_or_unverified_source(tmp_path, membership_source, failure, expected):
    row, path = membership_source
    if failure == "missing":
        path.unlink()
    elif failure == "checksum":
        document = json.loads(path.read_text(encoding="utf-8"))
        document["sha256"] = "0" * 64
        path.write_text(json.dumps(document), encoding="utf-8")
    else:
        row["source_request_sha256"] = "0" * 64

    result = review_membership_source(row, tmp_path)

    assert result["source_cache_status"] == expected
    assert "eligibility_replay_status" not in result
    assert "reference_identity_collisions" not in result
    assert result["repair_authorized"] is False
    assert row["stored_tickers"] == ["PEAK"]


@pytest.mark.parametrize("failure, expected", [
    ("missing", "GROUPED_CACHE_NOT_RETAINED"),
    ("checksum", "GROUPED_CHECKSUM_INVALID"),
    ("policy", "UNSUPPORTED_POLICY"),
])
def test_verified_references_do_not_prove_original_eligibility(tmp_path, membership_source, failure, expected):
    row, _ = membership_source
    if failure == "checksum":
        (tmp_path / "grouped-unadjusted_2024-02-02.json").write_text(
            json.dumps({"payload": [], "sha256": "0" * 64}), encoding="utf-8",
        )
    elif failure == "policy":
        row["policy_sha256"] = "0" * 64

    result = review_membership_source(row, tmp_path)

    assert result["source_cache_status"] == "VERIFIED"
    assert result["eligibility_replay_status"] == expected
    assert result["reference_identity_collisions"][0]["source_tickers"] == ["DOC", "PEAK"]
    assert result["reference_identity_collisions"][0]["stored_tickers"] == ["PEAK"]
    assert result["repair_authorized"] is False


@pytest.mark.parametrize("matching_id", [True, False])
def test_membership_replay_uses_only_prior_sessions_and_checks_original_id(tmp_path, membership_source, matching_id):
    row, _ = membership_source
    daily = [{"T": "DOC", "c": 10, "v": 3000000}, {"T": "PEAK", "c": 20, "v": 3000000}]
    document = json.dumps({"payload": daily, "sha256": sha256_json(daily)})
    calendar = exchange_calendars.get_calendar("XNYS")
    for session in calendar.sessions_window("2024-03-01", -20):
        (tmp_path / f"grouped-unadjusted_{session.date().isoformat()}.json").write_text(document, encoding="utf-8")
    (tmp_path / "grouped-unadjusted_2024-03-04.json").write_text("invalid", encoding="utf-8")
    if not matching_id:
        row["universe_run_id"] = uuid4()

    result = review_membership_source(row, tmp_path)

    assert result["eligibility_replay_status"] == ("REPRODUCED" if matching_id else "RUN_ID_MISMATCH")
    assert result["replayed_members"] == 2
    assert result["missing_member_ranks"] == [2]
    assert result["missing_from_storage"] == [{"ticker": "DOC", "member_rank": 2}]
    assert result["extra_in_storage"] == []
    assert result["stored_members_reproduce_run_id"] is False
    assert result["repair_authorized"] is False


def test_identity_review_enforces_read_only_and_cannot_certify_empty_data(monkeypatch, tmp_path):
    from scripts import audit_equity_research_inputs as audit

    cursor = MagicMock()
    cursor.fetchone.return_value = {"read_only": "on", "cutoff": "2026-09-12"}
    cursor.fetchall.side_effect = [[], []]
    manager = MagicMock()
    manager.__enter__.return_value = cursor
    monkeypatch.setattr(audit, "get_db_cursor", lambda: manager)

    result = audit.review_identities("liquid_us_common_stocks_v2", tmp_path)

    statements = [call.args[0].strip() for call in cursor.execute.call_args_list]
    assert statements[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert all(statement.startswith(("SELECT", "WITH")) for statement in statements[2:])
    assert "LEFT JOIN equity_universe_members" in statements[-1]
    assert result["transaction"]["read_only"] == "on"
    assert result["readiness"] == "NOT_CERTIFIED"
    assert result["repair_status"] == "REVIEW_ONLY"
    assert result["review_sha256"] == sha256_json({key: value for key, value in result.items() if key != "review_sha256"})


@pytest.fixture
def transition_inputs():
    manifest = json.loads((Path(__file__).resolve().parents[2] / "backend/research/inputs/equity_doc_peak_transition.json").read_text())
    change = manifest["symbol_change"]
    references = [{"ticker": ticker, "type": "CS", "active": True,
                   "cik": change["cik"] if ticker == "PEAK" else manifest["merger"]["predecessor_cik"],
                   "composite_figi": change["composite_figi"], "share_class_figi": change["share_class_figi"]}
                  for ticker in ("DOC", "PEAK")]
    sessions = [value.date() for value in exchange_calendars.get_calendar("XNYS").sessions_window("2024-03-01", -20)]
    history = {session: {"PEAK": {"close": 20, "volume": 3000000},
                         "DOC": {"close": 10, "volume": 9000000}} for session in sessions}
    return dict(reference_rows=references, daily_history=history, signal_date=date(2024, 3, 4),
                prior_sessions=sessions, policy=HistoricalUniversePolicy(), transition=manifest)


def test_symbol_change_candidate_reuses_only_same_security_history_and_preserves_inputs(transition_inputs):
    original = deepcopy(transition_inputs)
    result = replay_symbol_change_candidate(**transition_inputs)
    assert transition_inputs == original
    assert result["members"] == 1
    assert result["successor_member"] == {"ticker": "DOC", "rank": 1, "latest_price": "20",
                                          "median_dollar_volume": "60000000", "observed_sessions": 20}
    assert result["corrected_reference"]["cik"] == "0000765880"
    assert result["removed_reference_tickers"] == ["PEAK"]
    assert result["repair_authorized"] is False
    assert replay_symbol_change_candidate(**transition_inputs) == result


def test_missing_peak_history_cannot_fall_back_to_old_doc(transition_inputs):
    for row in transition_inputs["daily_history"].values():
        row.pop("PEAK")
    result = replay_symbol_change_candidate(**transition_inputs)
    assert result["successor_member"] is None
    assert result["members"] == 0


@pytest.mark.parametrize("mutation", ["date", "figi", "issuer", "duplicate", "same_day", "sources", "merger", "window"])
def test_symbol_change_candidate_rejects_unreviewed_scope_or_inputs(transition_inputs, mutation):
    if mutation == "date":
        transition_inputs["signal_date"] = date(2024, 3, 5)
    elif mutation == "figi":
        transition_inputs["reference_rows"][0]["share_class_figi"] = "OTHER"
    elif mutation == "issuer":
        transition_inputs["reference_rows"][1]["cik"] = "0000001"
    elif mutation == "duplicate":
        transition_inputs["reference_rows"].append(dict(transition_inputs["reference_rows"][0]))
    elif mutation == "same_day":
        transition_inputs["prior_sessions"].append(date(2024, 3, 4))
    elif mutation == "merger":
        transition_inputs["transition"]["merger"]["successor_cik"] = "0000001"
    elif mutation == "window":
        transition_inputs["prior_sessions"].pop()
    else:
        transition_inputs["transition"]["sources"] = []
    with pytest.raises(ValueError):
        replay_symbol_change_candidate(**transition_inputs)


def test_candidate_identity_changes_when_source_evidence_changes(transition_inputs):
    first = replay_symbol_change_candidate(**transition_inputs)
    transition_inputs["transition"]["transition_version"] = "revised_v2"
    second = replay_symbol_change_candidate(**transition_inputs)
    assert first["candidate_sha256"] != second["candidate_sha256"]
    assert first["proposed_universe_run_id"] != second["proposed_universe_run_id"]


@pytest.mark.parametrize("run_count", [0, 2])
def test_transition_review_rejects_missing_or_ambiguous_original(monkeypatch, tmp_path, transition_inputs, run_count):
    from scripts import audit_equity_research_inputs as audit

    cursor = MagicMock()
    cursor.fetchone.return_value = {"read_only": "on"}
    cursor.fetchall.return_value = [{} for _ in range(run_count)]
    manager = MagicMock()
    manager.__enter__.return_value = cursor
    monkeypatch.setattr(audit, "get_db_cursor", lambda: manager)
    with pytest.raises(ValueError, match="exactly one original"):
        audit.review_transition(transition_inputs["transition"], tmp_path)
    assert cursor.execute.call_args_list[0].args[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert cursor.execute.call_args.args[1] == ("liquid_us_common_stocks_v2", date(2024, 3, 4))


def test_unverified_original_cannot_get_a_transition_candidate(monkeypatch, tmp_path, membership_source, transition_inputs):
    from scripts import audit_equity_research_inputs as audit

    row, cache = membership_source
    cache.unlink()
    cursor = MagicMock()
    cursor.fetchone.return_value = {"read_only": "on"}
    cursor.fetchall.return_value = [row]
    manager = MagicMock()
    manager.__enter__.return_value = cursor
    monkeypatch.setattr(audit, "get_db_cursor", lambda: manager)
    report = audit.review_transition(transition_inputs["transition"], tmp_path)
    assert "correction_candidate" not in report["membership"]
    assert "ORIGINAL_INPUT_REPLAY_NOT_VERIFIED" in report["blockers"]
    assert report["repair_authorized"] is False
    assert report["readiness"] == "NOT_CERTIFIED"


def test_original_run_id_mismatch_cannot_get_correction_candidate(monkeypatch, tmp_path, membership_source, transition_inputs):
    from scripts import audit_equity_research_inputs as audit

    row, _ = membership_source
    row["universe_run_id"] = uuid4()
    daily = [{"T": "DOC", "c": 10, "v": 3000000}, {"T": "PEAK", "c": 20, "v": 3000000}]
    document = json.dumps({"payload": daily, "sha256": sha256_json(daily)})
    for session in transition_inputs["prior_sessions"]:
        (tmp_path / f"grouped-unadjusted_{session.isoformat()}.json").write_text(document, encoding="utf-8")
    propose = MagicMock()
    monkeypatch.setattr(audit, "replay_symbol_change_candidate", propose)
    report = audit.review_membership_source(row, tmp_path, transition=transition_inputs["transition"])
    assert report["eligibility_replay_status"] == "RUN_ID_MISMATCH"
    assert "correction_candidate" not in report
    propose.assert_not_called()


@pytest.fixture
def transition_price_inputs(monkeypatch, tmp_path, transition_inputs):
    from scripts import audit_equity_research_inputs as audit

    candidate = replay_symbol_change_candidate(**transition_inputs)
    observed_at = datetime(2026, 9, 4, tzinfo=timezone.utc)
    membership = {"correction_candidate": candidate, "observed_at": observed_at}
    security_id = normalize_security_reference(candidate["corrected_reference"], observed_at=observed_at).security_id
    session = transition_inputs["signal_date"]
    calendar = exchange_calendars.get_calendar("XNYS")
    rows = []
    for current in transition_inputs["prior_sessions"] + [session]:
        references = transition_inputs["reference_rows"] if current == session else [transition_inputs["reference_rows"][1]]
        (tmp_path / f"tickers-CS_{current.isoformat()}.json").write_text(
            json.dumps({"payload": references, "sha256": sha256_json(references)}), encoding="utf-8",
        )
        end = calendar.session_close(current).to_pydatetime()
        rows.append({"ticker": "DOC" if current == session else "PEAK", "session_date": current,
                     "security_id": security_id, "bar_revision_id": uuid4(), "payload_sha256": "a" * 64,
                     "interval": "1d", "session_scope": "RTH", "adjusted": True, "is_final": True,
                     "quality_codes": ["GROUPED_DAILY_EXACT_TICKER_V2"], "availability_mode": "HISTORICAL_RECONSTRUCTED",
                     "bar_end": end, "replay_available_at": end, "system_observed_at": observed_at, "created_at": observed_at,
                     "open": 20, "high": 21, "low": 19, "close": 20, "volume": 3000000})
    cursor = MagicMock()
    cursor.fetchone.return_value = {"read_only": "on", "cutoff": observed_at + timedelta(days=1)}
    cursor.fetchall.return_value = rows
    manager = MagicMock()
    manager.__enter__.return_value = cursor
    factory = MagicMock(return_value=manager)
    monkeypatch.setattr(audit, "get_db_cursor", factory)
    return membership, cursor, factory, rows


def test_transition_price_review_verifies_dates_identity_and_pin_reread(tmp_path, transition_inputs, transition_price_inputs):
    from scripts import audit_equity_research_inputs as audit

    membership, cursor, _, rows = transition_price_inputs
    original = deepcopy(rows)
    result = audit.review_transition_prices(transition_inputs["transition"], membership, tmp_path)
    assert result["status"] == "IDENTITY_ALIGNED_SLICE"
    assert result["sessions"] == 21
    assert result["source_ticker_counts"] == {"PEAK": 20, "DOC": 1}
    assert result["pinned_reread_matches"] is True
    assert result["repair_authorized"] is False
    assert result["readiness"] == "NOT_CERTIFIED"
    assert cursor.execute.call_args_list[0].args[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert cursor.fetchall.call_count == 2
    assert rows == original


@pytest.mark.parametrize("failure", ["missing", "checksum", "issuer", "transition_hash", "candidate"])
def test_transition_price_review_rejects_unverified_identity_before_price_read(tmp_path, transition_inputs, transition_price_inputs, failure):
    from scripts import audit_equity_research_inputs as audit

    membership, _, factory, _ = transition_price_inputs
    session = transition_inputs["signal_date"] if failure == "transition_hash" else transition_inputs["prior_sessions"][0]
    path = tmp_path / f"tickers-CS_{session.isoformat()}.json"
    if failure == "missing":
        path.unlink()
    elif failure == "candidate":
        membership = {}
    else:
        document = json.loads(path.read_text())
        if failure == "checksum":
            document["sha256"] = "0" * 64
        elif failure == "issuer":
            document["payload"][0]["cik"] = "0009999"
            document["sha256"] = sha256_json(document["payload"])
        else:
            document["payload"][0]["name"] = "CHANGED"
            document["sha256"] = sha256_json(document["payload"])
        path.write_text(json.dumps(document), encoding="utf-8")
    result = audit.review_transition_prices(transition_inputs["transition"], membership, tmp_path)
    assert result["status"] == "BLOCKED"
    assert "price_manifest" not in result
    factory.assert_not_called()


@pytest.mark.parametrize("failure", ["wrong_id", "reread"])
def test_transition_price_review_cannot_hide_conflicts_or_failed_pins(tmp_path, transition_inputs, transition_price_inputs, failure):
    from scripts import audit_equity_research_inputs as audit

    membership, cursor, _, rows = transition_price_inputs
    if failure == "wrong_id":
        rows[0]["security_id"] = uuid4()
    else:
        cursor.fetchall.side_effect = [rows, rows[1:]]
    result = audit.review_transition_prices(transition_inputs["transition"], membership, tmp_path)
    assert result["status"] == "BLOCKED"
    assert result["reason"] == "PRICE_LINEAGE_UNRESOLVED"
    assert "price_manifest" not in result


def test_transition_action_review_checks_calendar_scopes_and_never_certifies_other_events(monkeypatch, transition_inputs):
    from scripts import audit_equity_research_inputs as audit

    cursor = MagicMock()
    cursor.fetchone.return_value = {"read_only": "on", "cutoff": datetime(2026, 9, 12, tzinfo=timezone.utc)}
    cursor.fetchall.return_value = []
    manager = MagicMock()
    manager.__enter__.return_value = cursor
    monkeypatch.setattr(audit, "get_db_cursor", lambda: manager)
    price_review = {"status": "IDENTITY_ALIGNED_SLICE", "security_id": str(uuid4()),
                    "price_manifest": {"revisions": [{"session": "2024-02-02"}, {"session": "2024-03-04"}],
                                       "identity_evidence_sha256": "a" * 64}}
    result = audit.review_transition_actions(transition_inputs["transition"], price_review)
    assert result["status"] == "BLOCKED"
    assert len(result["scope_results"]) == 4
    assert all(row["status"] == "BLOCKED" and "ACTION_COVERAGE_MISSING" in row["detail"] for row in result["scope_results"])
    assert result["scope_results"][0]["scope"]["window_end"] == date(2024, 3, 3)
    assert result["scope_results"][2]["scope"]["window_start"] == date(2024, 3, 4)
    assert "MERGER" in result["unsupported_completeness"]
    assert result["repair_authorized"] is False
    assert cursor.execute.call_args_list[0].args[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"


def test_transition_action_review_requires_validated_price_identities(transition_inputs):
    from scripts import audit_equity_research_inputs as audit
    assert audit.review_transition_actions(transition_inputs["transition"], {"status": "BLOCKED"}) == {
        "status": "BLOCKED", "reason": "PRICE_IDENTITY_REVIEW_REQUIRED",
    }