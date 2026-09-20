from copy import deepcopy
from datetime import timezone

from research.stock_alert_review import build_stock_eod_review
from research.stock_idea_engine import candidate_record
from test_stock_idea_engine import NOW, candidate


def fixture():
    selected = candidate("A", rs_rank=.9)
    quota = candidate("B", rs_rank=.8)
    first = dict(instance_id="instance", stream="intraday", label="Intraday / daily v2", record_id="run-one",
        payload=dict(window_key="run-one", session="2026-08-03", coverage="PUBLISHED", actual_publication_at=NOW.isoformat(),
            selected=[selected.episode_id], candidates={selected.episode_id: candidate_record(selected), quota.episode_id: candidate_record(quota)},
            dispositions=[dict(episode_id=selected.episode_id, security_id="A", model="resumption", interval="30m",
                direction=1, selection="SELECTED", reason=None),
                dict(episode_id=quota.episode_id, security_id="B", model="resumption", interval="30m",
                    direction=1, selection="SUPPRESSED", reason="MODEL_QUOTA")]))
    second = dict(instance_id="instance", stream="intraday", label="Intraday / daily v2", record_id="run-two",
        payload=dict(window_key="run-two", session="2026-08-03", coverage="PUBLISHED",
            actual_publication_at=(NOW.replace(minute=47)).isoformat(), selected=[],
            candidates={selected.episode_id: candidate_record(selected)},
            dispositions=[dict(episode_id=selected.episode_id, security_id="A", model="resumption", interval="30m",
                direction=1, selection="SUPPRESSED", reason="CONTINUING_EPISODE")]))
    outcome = dict(status="CLOSED", paper_return=.03)
    return [first, second], {("instance", selected.episode_id): outcome}


def test_stock_eod_review_reconciles_selection_rank_repeats_and_outcomes():
    publications, outcomes = fixture()
    original = deepcopy((publications, outcomes))
    result = build_stock_eod_review(publications, outcomes, as_of=NOW.astimezone(timezone.utc),
        session_date="2026-08-03", sessions=["2026-08-03"])
    assert result["detected_occurrences"] == 3 and result["unique_candidates"] == 2
    cell = next(cell for cell in result["models"] if cell["model"] == "resumption")
    assert cell == cell | dict(detected_occurrences=3, unique_candidates=2, selected=1,
        not_selected=1, repeats=1, measured=1, positive_rate=1., mean_return=.03)
    selected = next(row for row in result["rows"] if row["ticker"] == "A")
    assert selected["selection_status"] == "SELECTED" and selected["occurrences"] == 2
    assert selected["repeat_occurrences"] == 1 and selected["model_rank"] == 1
    assert selected["priority_rank"] == 1
    assert selected["outcome_status"] == "CLOSED" and selected["paper_return"] == .03
    rejected = next(row for row in result["rows"] if row["ticker"] == "B")
    assert rejected["selection_status"] == "NOT_SELECTED" and rejected["selection_reason"] == "MODEL_QUOTA"
    assert rejected["model_rank"] == 2 and rejected["priority_rank"] == 2 and rejected["paper_return"] is None
    diagnostics = result["diagnostics"]
    assert diagnostics["coverage"] == dict(completed=2, missed=0, completion_rate=1.)
    assert diagnostics["conversion"] == dict(selected=1, entered=1, closed=1, open=0, no_fill=0,
        pending=0, unavailable=0, fill_rate=1., measured=1, positive_rate=1., mean_return=.03, median_return=.03)
    assert diagnostics["rank_buckets"] == [dict(model="resumption", bucket="RANK_1",
        selected=1, entered=1, closed=1, open=0, no_fill=0, pending=0, unavailable=0,
        fill_rate=1., measured=1, positive_rate=1., mean_return=.03, median_return=.03)]
    assert diagnostics["bottlenecks"][0] == dict(reason="MODEL_QUOTA", category="ALLOCATION",
        candidates=1, ranked=1, top_three_priority=1, median_priority=2)
    assert result["review_signals"][0] == dict(code="ALLOCATION_PRESSURE", severity="CAUTION",
        candidates=1, top_three_priority=1)
    assert result["review_signals"][1:] == [
        dict(code="ALLOCATION_CONCENTRATION", severity="REVIEW", dimension="direction", value=1,
            share=1., selected=1),
        dict(code="ALLOCATION_CONCENTRATION", severity="REVIEW", dimension="interval", value="30m",
            share=1., selected=1)]
    assert (publications, outcomes) == original


def test_stock_eod_review_filters_before_pagination_and_rejects_invalid_filters():
    import pytest
    publications, outcomes = fixture()
    result = build_stock_eod_review(publications, outcomes, as_of=NOW, session_date="2026-08-03",
        model="resumption", selection_status="NOT_SELECTED", search="b", direction=1,
        trade_type="INTRADAY", offset=0, limit=1)
    assert result["total"] == 1 and result["rows"][0]["ticker"] == "B"
    with pytest.raises(ValueError, match="filters"):
        build_stock_eod_review(publications, outcomes, as_of=NOW, session_date="2026-08-03", selection_status="INVALID")


def test_stock_eod_review_does_not_treat_no_fill_as_zero_return():
    publications, outcomes = fixture()
    selected_id = publications[0]["payload"]["selected"][0]
    outcomes[("instance", selected_id)] = dict(status="NO_FILL", paper_return=0.)
    result = build_stock_eod_review(publications, outcomes, as_of=NOW, session_date="2026-08-03")
    row = next(row for row in result["rows"] if row["ticker"] == "A")
    assert row["outcome_status"] == "NO_FILL" and row["paper_return"] is None
    assert next(cell for cell in result["models"] if cell["model"] == "resumption")["measured"] == 0


def test_stock_eod_review_excludes_missed_runs_from_selection_statistics():
    publications, outcomes = fixture()
    missed = deepcopy(publications[0])
    missed["record_id"] = missed["payload"]["window_key"] = "missed"
    missed["payload"].update(coverage="MISSED_PUBLICATION", selected=[])
    for disposition in missed["payload"]["dispositions"]:
        disposition.update(selection="SUPPRESSED", reason="MISSED_PUBLICATION")
    result = build_stock_eod_review(publications + [missed], outcomes, as_of=NOW,
        session_date="2026-08-03", sessions=["2026-08-03"])
    assert result["completed_runs"] == 2 and result["missed_runs"] == 1
    assert result["detected_occurrences"] == 3
    assert next(signal for signal in result["review_signals"] if signal["code"] == "COVERAGE_LIMITED") == dict(
        code="COVERAGE_LIMITED", severity="CAUTION", completed=2, missed=1, total_runs=3,
        completion_rate=2 / 3)


def test_stock_eod_review_route_is_read_only_and_forwards_filters(monkeypatch):
    from datetime import date
    from equity.stock_discovery_api import alert_eod_review
    import equity.stock_alert_results as repository

    calls = []
    monkeypatch.setattr(repository, "load_stock_eod_review", lambda **kwargs: calls.append(kwargs) or {
        "schema": "stock_alert_eod_review_v1", "storage_ready": True, "rows": []})
    result = alert_eod_review(session_date=date(2026, 8, 3), search="A", model="resumption",
        selection_status="NOT_SELECTED", direction=1, trade_type="INTRADAY", offset=10, limit=25)
    assert result["storage_ready"] and len(calls) == 1
    assert calls[0]["as_of"].utcoffset() is not None
    assert {key: calls[0][key] for key in ("session_date", "search", "model", "selection_status", "direction",
        "trade_type", "offset", "limit")} == dict(session_date=date(2026, 8, 3), search="A", model="resumption",
            selection_status="NOT_SELECTED", direction=1, trade_type="INTRADAY", offset=10, limit=25)