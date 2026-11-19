"""G106 D5 (r138, Nov 18): the Options Alerts model filter accepts F0DTE.

F0DTE alerts carry their own detector id on the page (their evaluations ride the O1 lane). API r130 refused
`?detector=F0DTE` with 422, so the portal could not filter to them. Now the three routes, the page queries and the
compatibility repository accept it; the alert list filters on the row's detector id, and the evaluation list serves
F0DTE rows when the O1 lane they ride is on the page. Any other unknown detector is still refused."""
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from realtime.advanced_options_projection import AdvancedOptionsAlertsPagePublication
from realtime.option_alert_projection import DetectorEvaluationQuery, DetectorRunQuery
from realtime.portal_api import RealtimePortalRepositories, create_realtime_portal_app
from realtime.portal_compatibility_repository import RealtimeOptionAlertRepository
from realtime.preflight import ProviderTier

COHORT = UUID(int=7)
SESSION = "2026-11-18"


class _Views:
    """The page repository: records what each route asked for."""

    def __init__(self):
        self.calls = []

    def read(self, contract, **params):
        self.calls.append((contract, params))
        return {"available": True, "reason": None, "data": {}}


class _Alerts:
    def __init__(self):
        self.calls = []

    def option_alert_evaluations(self, **params):
        self.calls.append(params)
        return {"available": True, "data": {}}


def _app():
    from datetime import datetime, timezone

    views, alerts = _Views(), _Alerts()
    app = create_realtime_portal_app(
        RealtimePortalRepositories(None, None, alerts, option_alert_views=views),
        clock=lambda: datetime(2026, 11, 18, 16, 0, tzinfo=timezone.utc),
    )
    return app, views, alerts


@pytest.mark.parametrize("detector", ["F0DTE", "O1", "OS2"])
def test_the_routes_accept_f0dte_and_the_lanes(detector):
    app, views, alerts = _app()
    cohort = {"cohort_id": str(COHORT)}
    with TestClient(app) as client:
        runs = client.get("/api/options/alerts/detector-runs",
                          params={**cohort, "dataset_id": "d1", "scope": "HISTORY", "detector": detector})
        evaluations = client.get("/api/options/alerts/evaluations",
                                 params={**cohort, "dataset_id": "d1", "detector": detector})
        legacy = client.get("/api/realtime/option-alerts", params={**cohort, "detector": detector})
    assert (runs.status_code, evaluations.status_code, legacy.status_code) == (200, 200, 200), runs.text
    assert [call[1]["query"]["detector"] for call in views.calls] == [detector, detector]
    assert alerts.calls[0]["detector"] == detector


@pytest.mark.parametrize("detector", ["F0", "f0dte", "S3", "ODTE"])
def test_any_other_detector_is_still_refused(detector):
    app, views, alerts = _app()
    cohort = {"cohort_id": str(COHORT)}
    with TestClient(app) as client:
        for path, params in (("/api/options/alerts/detector-runs", {"dataset_id": "d1"}),
                             ("/api/options/alerts/evaluations", {"dataset_id": "d1"}),
                             ("/api/realtime/option-alerts", {})):
            assert client.get(path, params={**cohort, **params, "detector": detector}).status_code == 422
    assert views.calls == [] and alerts.calls == []
    with pytest.raises(ValidationError):
        DetectorRunQuery(dataset_id="d1", detector=detector)


def test_the_page_queries_and_the_compatibility_repository_accept_f0dte():
    assert DetectorRunQuery(dataset_id="d1", scope="HISTORY", detector="F0DTE").detector == "F0DTE"
    assert DetectorEvaluationQuery(detector="F0DTE").detector == "F0DTE"

    class Repository(RealtimeOptionAlertRepository):
        def __init__(self):
            pass

        def _cursor(self):
            raise LookupError("reached the read")

    with pytest.raises(LookupError, match="reached the read"):
        Repository().option_alert_evaluations(available_by=_utc(), detector="F0DTE")
    with pytest.raises(ValueError, match="unsupported option detector"):
        Repository().option_alert_evaluations(available_by=_utc(), detector="F1")


def _utc():
    from datetime import datetime, timezone

    return datetime(2026, 11, 18, 16, 0, tzinfo=timezone.utc)


def _page(*, lanes=("O1", "O2", "O3", "OS1", "OS2")):
    evaluation_rows = [
        dict(evaluation_id="o1", run_id="d1", detector_id="O1", underlyer="NVDA", selection_status="SELECTED",
             scheduled_cycle="c1"),
        dict(evaluation_id="f1", run_id="d1", detector_id="F0DTE", underlyer="SPY", selection_status="SELECTED",
             scheduled_cycle="c1"),
        dict(evaluation_id="os", run_id="d1", detector_id="OS1", underlyer="SPY", selection_status="OBSERVATION",
             scheduled_cycle="c1"),
    ]
    alert_rows = [
        dict(evaluation_id="o1", detector_id="O1", underlyer="NVDA", scheduled_cycle="c1",
             original_package={"legs": [{"side": "BUY", "contract_type": "CALL", "ratio": 1}]}),
        dict(evaluation_id="f1", detector_id="F0DTE", underlyer="SPY", scheduled_cycle="c1",
             original_package={"legs": [{"side": "BUY", "contract_type": "PUT", "ratio": 1}]}),
    ]
    payload = dict(
        cohort_id=str(COHORT), provider_tier=ProviderTier.MASSIVE_ADVANCED.value, scope_sha256="s" * 64,
        dataset_id="d1", session_date=SESSION, published_at="2026-11-18T16:00:00+00:00",
        run={"run_id": "d1", "observed_time": "2026-11-18T16:00:00+00:00"}, policy_sha256="p" * 64,
        evaluation_rows=evaluation_rows, alert_rows=alert_rows,
        lanes=[{"detector": lane, "state": "COMPLETE", "reason_codes": []} for lane in lanes],
    )
    identity = dict(cohort_id=COHORT, provider_tier=ProviderTier.MASSIVE_ADVANCED, scope_sha256="s" * 64)
    return AdvancedOptionsAlertsPagePublication(payload), identity


def test_the_alert_list_filters_to_the_f0dte_rows():
    publication, identity = _page()
    rows = publication.detector_runs(DetectorRunQuery(dataset_id="d1", scope="LATEST", detector="F0DTE"),
                                     **identity)["data"]["rows"]
    assert [row["evaluation_id"] for row in rows] == ["f1"]
    rows = publication.detector_runs(DetectorRunQuery(dataset_id="d1", scope="LATEST", detector="O1"),
                                     **identity)["data"]["rows"]
    assert [row["evaluation_id"] for row in rows] == ["o1"]
    every = publication.detector_runs(DetectorRunQuery(dataset_id="d1", scope="LATEST"), **identity)["data"]["rows"]
    assert sorted(row["evaluation_id"] for row in every) == ["f1", "o1"]


def test_the_evaluation_list_serves_f0dte_rows_when_their_o1_lane_is_on_the_page():
    publication, identity = _page()
    result = publication.evaluations(DetectorEvaluationQuery(dataset_id="d1", detector="F0DTE"), **identity)
    assert result["available"] is True
    assert [row["evaluation_id"] for row in result["data"]["rows"]] == ["f1"]
    # The lanes and their counts are unchanged by the filter (the models list is per lane).
    assert [model["detector_id"] for model in result["data"]["models"]] == ["O1", "O2", "O3", "OS1", "OS2"]
    o1 = publication.evaluations(DetectorEvaluationQuery(dataset_id="d1", detector="O1"), **identity)
    assert [row["evaluation_id"] for row in o1["data"]["rows"]] == ["o1"]
    every = publication.evaluations(DetectorEvaluationQuery(dataset_id="d1"), **identity)
    assert sorted(row["evaluation_id"] for row in every["data"]["rows"]) == ["f1", "o1", "os"]


def test_without_the_o1_lane_the_f0dte_filter_is_unavailable_as_any_missing_lane():
    publication, identity = _page(lanes=("O3", "OS1", "OS2"))
    for detector in ("F0DTE", "O1"):
        result = publication.evaluations(DetectorEvaluationQuery(dataset_id="d1", detector=detector), **identity)
        assert (result["available"], result["reason"]) == (False, "DETECTOR_LANE_UNAVAILABLE")
        assert result["data"] == {"available_detectors": ["O3", "OS1", "OS2"]}
    assert publication.evaluations(DetectorEvaluationQuery(dataset_id="d1", detector="OS1"), **identity)["available"]


def test_the_openapi_schema_lists_f0dte_for_the_three_routes():
    app, _, _ = _app()
    schema = app.openapi()

    def detector_enum(path):
        (parameter,) = [item for item in schema["paths"][path]["get"]["parameters"] if item["name"] == "detector"]
        options = parameter["schema"].get("anyOf", [parameter["schema"]])
        return next(option["enum"] for option in options if "enum" in option)

    assert detector_enum("/api/options/alerts/detector-runs")[-1] == "F0DTE"
    assert detector_enum("/api/options/alerts/evaluations")[-1] == "F0DTE"
    assert detector_enum("/api/realtime/option-alerts") == ["O1", "O2", "O3", "OS1", "OS2", "F0DTE"]
