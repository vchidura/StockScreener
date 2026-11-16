"""G102 OA3 (Nov 16): past sessions on the Options Alerts page from durable rows.

Live Nov 16: the page's index listed only the latest release's session, and a past session's page payload had left the
3-day hot table, so the API answered DETECTOR_ALERTS_UNAVAILABLE for Nov 10-12. The index now lists every session with
durable history and a past session's last window is rebuilt from rt.detector_datasets / detector_evaluations /
option_packages and served by the page publication's own methods.
"""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from realtime.advanced_options_native_loader import options_alert_v1_event_package
from realtime.option_alert_projection import DetectorEvaluationQuery, DetectorRunQuery
from realtime.options_alert_history import (
    AdvancedOptionsAlertsHistoryPublication,
    history_index,
    history_release,
    load_options_alert_history_page_cursor,
    load_options_alert_sessions_cursor,
    with_past_sessions,
)

UTC = timezone.utc
COHORT = UUID("a50deb50-521e-5261-974d-56c3aaaf7114")
DATASET = "11111111-2222-4333-8444-555555555555"
SESSION = date(2026, 11, 12)
CUTOFF = datetime(2026, 11, 12, 21, 0, tzinfo=UTC)
EVENT_SELECTED = uuid5(NAMESPACE_URL, "event:selected")
EVENT_OBSERVED = uuid5(NAMESPACE_URL, "event:observed")


def _sessions():
    return [
        {"session_date": "2026-11-10", "cohort_id": "c10", "dataset_id": "d10", "cutoff": datetime(2026, 11, 10, 21, tzinfo=UTC)},
        {"session_date": "2026-11-11", "cohort_id": "c11", "dataset_id": "d11", "cutoff": datetime(2026, 11, 11, 20, 58, tzinfo=UTC)},
        {"session_date": "2026-11-12", "cohort_id": "c12", "dataset_id": "d12", "cutoff": datetime(2026, 11, 12, 21, tzinfo=UTC)},
        {"session_date": "2026-11-13", "cohort_id": "c13", "dataset_id": "d13", "cutoff": datetime(2026, 11, 13, 19, 58, tzinfo=UTC)},
    ]


def test_index_keeps_the_current_session_and_adds_the_past_ones():
    current = {
        "storage_ready": True, "datasets": ["d13"], "dataset_sessions": {"d13": ["2026-11-13"]},
        "sessions": ["2026-11-13"], "session_datasets": {"2026-11-13": "d13"}, "current_session_date": "2026-11-13",
        "current_dataset_id": "d13", "default_dataset_id": "d13", "configured_dataset_id": "d13",
        "configured_effective_from": "2026-11-13T19:58:40+00:00",
    }
    data = with_past_sessions(current, _sessions())
    # Oldest first, latest last: the header's session picker reads the list this way.
    assert data["sessions"] == ["2026-11-10", "2026-11-11", "2026-11-12", "2026-11-13"]
    assert data["session_datasets"] == {"2026-11-10": "d10", "2026-11-11": "d11", "2026-11-12": "d12", "2026-11-13": "d13"}
    assert data["dataset_sessions"]["d11"] == ["2026-11-11"] and data["datasets"][-1] == "d13"
    for key in ("current_session_date", "current_dataset_id", "default_dataset_id", "configured_dataset_id"):
        assert data[key] == current[key]
    assert current["sessions"] == ["2026-11-13"]  # the release's own index is not changed in place
    assert with_past_sessions(current, []) == current


def test_index_from_durable_sessions_only_when_no_release_page_is_stored():
    data = history_index(_sessions()[:3])
    assert data["current_session_date"] == "2026-11-12" and data["current_dataset_id"] == "d12"
    assert data["sessions"] == ["2026-11-10", "2026-11-11", "2026-11-12"]
    assert history_index([]) is None


class _Cursor:
    def __init__(self, results):
        self.results, self.result, self.statements = results, [], []

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        for marker, rows in self.results:
            if marker in sql:
                self.result = rows(params) if callable(rows) else rows
                return
        raise AssertionError(f"unexpected statement: {sql}")

    def fetchall(self):
        return list(self.result)

    def fetchone(self):
        return self.result[0] if self.result else None


def test_sessions_are_listed_oldest_first_from_the_first_session_list():
    cursor = _Cursor([("FROM rt.detector_datasets", [
        {"session_date": date(2026, 11, 12), "cohort_id": "c12", "dataset_id": "d12", "started_at": CUTOFF},
        {"session_date": date(2026, 11, 11), "cohort_id": "c11", "dataset_id": "d11", "started_at": CUTOFF - timedelta(days=1)},
    ])])
    rows = load_options_alert_sessions_cursor(cursor, cutoff=CUTOFF)
    assert [row["session_date"] for row in rows] == ["2026-11-11", "2026-11-12"]
    sql, params = cursor.statements[0]
    assert "options-alert-session-v1" in params and "MASSIVE_ADVANCED" in sql and "DISTINCT ON" in sql


def _row(event_id, contract_id=101, ticker="O:NVDA261113C00165000"):
    return {
        "event_id": str(event_id), "event_time": CUTOFF - timedelta(minutes=3), "quote_time": CUTOFF - timedelta(minutes=3),
        "quote_bid": Decimal("1.10"), "quote_ask": Decimal("1.20"), "contract_id": contract_id, "contract_ticker": ticker,
        "instrument_id": 9001, "version_id": 77, "strike": Decimal("165"), "multiplier": Decimal("100"),
        "expiration": date(2026, 11, 13), "call_put": "C",
    }


def _history_cursor(*, with_dataset=True, evidence_grant=True):
    package = options_alert_v1_event_package(_row(EVENT_SELECTED), cohort_id=COHORT, matrix_id=COHORT, session_date=SESSION)
    evaluations = [
        {"evaluation_id": str(uuid5(NAMESPACE_URL, "evaluation:selected")), "event_id": str(EVENT_SELECTED),
         "detector_id": "O1", "underlyer": "NVDA", "selection_status": "SELECTED",
         "observed_at": CUTOFF - timedelta(seconds=30), "payload": {"disposition": "DETECTED"}},
        {"evaluation_id": str(uuid5(NAMESPACE_URL, "evaluation:observed")), "event_id": str(EVENT_OBSERVED),
         "detector_id": "O1", "underlyer": "TSLA", "selection_status": "OBSERVATION",
         "observed_at": CUTOFF - timedelta(seconds=20), "payload": {"disposition": "DETECTED"}},
        {"evaluation_id": str(uuid5(NAMESPACE_URL, "evaluation:retired")), "event_id": str(uuid5(NAMESPACE_URL, "x")),
         "detector_id": "RETIRED_DETECTOR", "underlyer": "SPY", "selection_status": "NOT_SELECTED",
         "observed_at": CUTOFF, "payload": {}},
    ]
    lanes = [{"state": "COMPLETE_SELECTED", "detector": "O1", "reason_codes": [], "outcome_sha256": "a" * 64},
             {"state": "UNAVAILABLE", "detector": "O2", "reason_codes": ["O2_MODEL_NOT_ACCEPTED"], "outcome_sha256": "b" * 64}]
    dataset = [{"dataset_id": DATASET, "session_date": SESSION, "started_at": CUTOFF,
                "completed_at": CUTOFF + timedelta(seconds=40), "summary": {"lanes": lanes}}] if with_dataset else []
    import realtime.option_portal_repository as repository

    return _Cursor([
        ("FROM rt.detector_datasets", dataset),
        ("FROM rt.detector_evaluations", evaluations),
        ("FROM rt.option_packages", [{"event_id": str(EVENT_SELECTED), "candidate_id": str(package.candidate_id)}]),
        ("has_table_privilege", [{"allowed": evidence_grant}]),
        ("FROM rt.advanced_options_flow_package_evidence", [{"candidate_id": str(package.candidate_id), "package_mark": {
            "computed_at": CUTOFF.isoformat(),
            "outcome": {"status": "FRESH", "market_time": CUTOFF.isoformat(), "observed_time": CUTOFF.isoformat(),
                        "capital_at_risk": "115.0", "net_return": "0.1",
                        "availability_flag": "REALTIME_ADVANCED_NBBO", "valuation_policy_sha256": "c" * 64,
                        "source_snapshot_ids": [], "gross_pnl": "11.0", "net_pnl": "9.7", "price_return": "0.1",
                        "estimated_cost": "1.30", "exit_net_premium": "-126.0", "entry_net_premium": "-115.0"}}}]),
    ]), package, repository


@pytest.fixture
def history(monkeypatch):
    cursor, package, repository = _history_cursor()
    monkeypatch.setattr(repository, "load_realtime_option_packages_cursor",
                        lambda cursor, ids, cohort_id: (package,) if package.candidate_id in ids else ())
    publication = load_options_alert_history_page_cursor(cursor, cohort_id=COHORT, session_date=SESSION, cutoff=CUTOFF)
    return publication, package, cursor


def test_a_past_session_page_is_rebuilt_from_its_last_window(history):
    publication, package, cursor = history
    assert isinstance(publication, AdvancedOptionsAlertsHistoryPublication)
    assert publication.dataset_id == DATASET and publication.session_date == "2026-11-12"
    stored = publication._payload
    assert [row["selection_status"] for row in stored["evaluation_rows"]] == ["SELECTED", "OBSERVATION"]
    (alert,) = stored["alert_rows"]
    assert alert["candidate_id"] == str(package.candidate_id) and alert["run_id"] == DATASET
    assert alert["original_package"]["legs"][0]["contract_ticker"] == "O:NVDA261113C00165000"
    # The mark the window sealed into its page (flow-package evidence), as on the stored page.
    assert alert["current_mark"]["basis"] == "REALTIME_ADVANCED_NBBO"
    assert set(stored["candidate_details"]) == {str(package.candidate_id)}
    assert stored["run"]["run_id"] == DATASET and stored["run"]["selection_counts"] == {"SELECTED": 1, "OBSERVATION": 1}
    dataset_sql, dataset_params = cursor.statements[0]
    assert dataset_params[0] == str(COHORT) and "2026-11-12" in dataset_params


def test_day_history_latest_run_and_daily_results_read_the_rebuilt_page(history):
    publication, package, _ = history
    session_rows = [{
        "evaluation_id": "earlier", "evaluation_ids": ["earlier"], "candidate_id": "c-earlier", "run_id": "d-earlier",
        "detector_id": "O1", "underlyer": "AAPL", "direction": 1, "triggered_at": "2026-11-12T15:00:00+00:00",
        "scheduled_cycle": "2026-11-12T15:00:00+00:00", "original_package": {"legs": []}, "hit_count": 1,
    }]
    history_rows = publication.detector_runs(
        DetectorRunQuery(dataset_id=DATASET, scope="HISTORY", session_date=SESSION, session_rollup=True),
        session_rows=session_rows, current_session_date="2026-11-16",
    )
    assert history_rows["available"] and history_rows["data"]["session_date"] == "2026-11-12"
    # A past session withholds nothing: the session list and the last window's own alert.
    assert {row["candidate_id"] for row in history_rows["data"]["rows"]} == {"c-earlier", str(package.candidate_id)}
    latest = publication.detector_runs(DetectorRunQuery(dataset_id=DATASET, scope="LATEST"),
                                       session_rows=session_rows, current_session_date="2026-11-16")
    assert [row["candidate_id"] for row in latest["data"]["rows"]] == [str(package.candidate_id)]
    other = publication.detector_runs(DetectorRunQuery(dataset_id="another", scope="LATEST"), session_rows=session_rows)
    assert not other["available"] and other["reason"] == "DETECTOR_DATASET_UNAVAILABLE"
    results = publication.evaluations(DetectorEvaluationQuery(session_date=SESSION))
    assert results["data"]["total"] == 2
    assert {model["detector_id"]: model["lane_state"] for model in results["data"]["models"]} == {
        "O1": "COMPLETE_SELECTED", "O2": "UNAVAILABLE"}
    candidate = publication.candidate(package.candidate_id)
    assert candidate["available"] and candidate["data"]["candidate"]["candidate_id"] == str(package.candidate_id)
    release = history_release(publication, checked_at=CUTOFF + timedelta(days=4), release_id=None)
    assert release["retained_session_date"] == "2026-11-12" and release["cutoff"] == CUTOFF and not release["stale"]


def test_no_window_for_the_session_means_no_history(monkeypatch):
    cursor, _, _ = _history_cursor(with_dataset=False)
    assert load_options_alert_history_page_cursor(cursor, cohort_id=COHORT, session_date=SESSION, cutoff=CUTOFF) is None


PIN = SimpleNamespace(session_date=SESSION, cutoff=CUTOFF, release_id=uuid5(NAMESPACE_URL, "release:nov12"),
                      provider_tier="MASSIVE_ADVANCED")
MONDAY = datetime(2026, 11, 16, 15, 0, tzinfo=UTC)
QUERY = {"dataset_id": DATASET, "scope": "HISTORY", "session_date": SESSION, "session_rollup": True}


class _Savepoints:
    def __init__(self):
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append(sql)


def _repository(monkeypatch, publication, seen):
    import realtime.options_alert_history as module
    from realtime.portal_compatibility_repository import RealtimeOptionsAlertsPageRepository

    monkeypatch.setattr(module, "load_options_alert_history_page_cursor",
                        lambda cursor, **kwargs: seen.setdefault("page", kwargs) and publication)
    repository = RealtimeOptionsAlertsPageRepository.__new__(RealtimeOptionsAlertsPageRepository)
    repository._reads_enabled = True
    repository._advanced_session_alerts = lambda cursor, publication, cohort_id, cutoff: []
    repository._advanced_current_marks_reported = lambda cursor, envelope, cutoff: seen.setdefault("marks", cutoff) and envelope
    repository._advanced_source_facts_reported = lambda cursor, envelope, cutoff: envelope
    repository._advanced_alert_valuation_switched = lambda cursor, envelope: envelope
    return repository


def _history(repository, cursor, **overrides):
    arguments = dict(cohort_id=COHORT, cutoff=MONDAY, active={"provider_tier": "MASSIVE_ADVANCED"},
                     reason="DETECTOR_ALERTS_UNAVAILABLE", resolved_release=PIN, requested_session=SESSION,
                     query=QUERY, candidate_id=None)
    arguments.update(overrides)
    contract = arguments.pop("contract", "detector_runs")
    return repository._advanced_options_history(cursor, contract, **arguments)


def test_repository_serves_the_pinned_session_whose_page_left_the_hot_table(monkeypatch, history):
    publication, package, _ = history
    seen = {}
    repository = _repository(monkeypatch, publication, seen)
    cursor = _Savepoints()
    envelope = _history(repository, cursor)
    assert envelope["available"] and envelope["retained_session_date"] == "2026-11-12"
    assert envelope["release_id"] == str(PIN.release_id)
    assert envelope["data"]["optional_reads"][-1]["serving_source"] == "DURABLE_SESSION_ROWS"
    # The page is rebuilt for the pinned session up to the release cutoff; read-time marks as for a stored page.
    assert seen["page"] == {"cohort_id": COHORT, "session_date": "2026-11-12", "cutoff": CUTOFF}
    assert seen["marks"] == MONDAY
    assert cursor.statements[0] == "SAVEPOINT options_alert_history" and cursor.statements[-1].startswith("RELEASE")
    results = _history(repository, _Savepoints(), contract="evaluations", query={"session_date": SESSION})
    assert results["available"] and results["data"]["total"] == 2
    detail = _history(repository, _Savepoints(), contract="candidate", query=None, candidate_id=package.candidate_id)
    assert detail["available"]


def test_repository_history_keeps_the_read_pin_rule(monkeypatch, history):
    publication, _, _ = history
    repository = _repository(monkeypatch, publication, {})
    for overrides in (
        {"resolved_release": None, "reason": "ADVANCED_OPTIONS_READ_PIN_UNAVAILABLE"},  # no pin: nothing is read
        {"reason": "OPTIONS_SESSION_RELEASE_UNAVAILABLE", "requested_session": date(2026, 11, 11)},
        {"requested_session": date(2026, 11, 11)},
        {"reason": "EQUITY_ONLY_COHORT"},
        {"resolved_release": SimpleNamespace(**{**vars(PIN), "provider_tier": "MASSIVE"})},
    ):
        cursor = _Savepoints()
        assert _history(repository, cursor, **overrides) is None, overrides
        assert cursor.statements == [], overrides
    with pytest.raises(ValueError):
        _history(repository, _Savepoints(), query={**QUERY, "scope": "LATEST"})


def test_repository_history_failure_answers_as_before(monkeypatch):
    import realtime.options_alert_history as module
    from realtime.portal_compatibility_repository import RealtimeOptionsAlertsPageRepository

    def broken(cursor, **kwargs):
        raise RuntimeError("relation does not exist")

    monkeypatch.setattr(module, "load_options_alert_history_page_cursor", broken)
    repository = RealtimeOptionsAlertsPageRepository.__new__(RealtimeOptionsAlertsPageRepository)
    repository._reads_enabled = True
    cursor = _Savepoints()
    assert _history(repository, cursor, contract="evaluations", query={"session_date": SESSION}) is None
    assert cursor.statements == ["SAVEPOINT options_alert_history", "ROLLBACK TO SAVEPOINT options_alert_history"]


def test_repository_index_from_durable_sessions_up_to_the_pinned_session(monkeypatch):
    import realtime.options_alert_history as module
    from realtime.portal_compatibility_repository import RealtimeOptionsAlertsPageRepository

    monkeypatch.setattr(module, "load_options_alert_sessions_cursor", lambda cursor, cutoff: _sessions())
    repository = RealtimeOptionsAlertsPageRepository.__new__(RealtimeOptionsAlertsPageRepository)
    repository._reads_enabled = True
    envelope = _history(repository, _Savepoints(), contract="datasets", reason="DETECTOR_DATASET_INDEX_UNAVAILABLE",
                        requested_session=None, query=None)
    assert envelope["available"] and envelope["data"]["current_session_date"] == "2026-11-12"
    assert envelope["data"]["sessions"] == ["2026-11-10", "2026-11-11", "2026-11-12"]


def test_index_extension_is_additive(monkeypatch):
    import realtime.options_alert_history as module
    from realtime.portal_compatibility_repository import RealtimeOptionsAlertsPageRepository

    envelope = {"available": True, "data": {"sessions": ["2026-11-13"], "session_datasets": {"2026-11-13": "d13"},
                                            "dataset_sessions": {"d13": ["2026-11-13"]}, "datasets": ["d13"],
                                            "current_session_date": "2026-11-13"}}
    statements = []
    cursor = SimpleNamespace(execute=lambda sql, params=None: statements.append(sql))
    monkeypatch.setattr(module, "load_options_alert_sessions_cursor", lambda cursor, cutoff: _sessions())
    extended = RealtimeOptionsAlertsPageRepository._with_past_options_sessions(cursor, envelope, cutoff=CUTOFF)
    assert extended["data"]["sessions"] == ["2026-11-10", "2026-11-11", "2026-11-12", "2026-11-13"]
    assert extended["data"]["optional_reads"][0]["status"] == "AVAILABLE"

    def broken(cursor, cutoff):
        raise RuntimeError("boom")

    monkeypatch.setattr(module, "load_options_alert_sessions_cursor", broken)
    fallback = RealtimeOptionsAlertsPageRepository._with_past_options_sessions(cursor, envelope, cutoff=CUTOFF)
    assert fallback["data"]["sessions"] == ["2026-11-13"]
    assert fallback["data"]["optional_reads"][0]["reason_code"] == "OPTIONS_PAST_SESSIONS_UNAVAILABLE"
    assert RealtimeOptionsAlertsPageRepository._with_past_options_sessions(cursor, {"available": False}, cutoff=CUTOFF) == {
        "available": False}


def test_a_reader_without_the_evidence_grant_gets_the_rows_without_sealed_marks(monkeypatch):
    cursor, package, repository = _history_cursor(evidence_grant=False)
    monkeypatch.setattr(repository, "load_realtime_option_packages_cursor", lambda cursor, ids, cohort_id: (package,))
    publication = load_options_alert_history_page_cursor(cursor, cohort_id=COHORT, session_date=SESSION, cutoff=CUTOFF)
    (alert,) = publication._payload["alert_rows"]
    assert alert["current_mark"]["status"] == "UNAVAILABLE"
    assert not any("evidence_document" in sql for sql, _ in cursor.statements)  # the evidence itself is not read
