"""Nov 11 (user): Options Alerts.

R45: Latest Run and Day History showed the same rows (the read ignored the scope). Now, as in Stock Alerts, Latest Run
is the alerts triggered or hit in the latest run and Day History the alerts first triggered before it, the latest run
withheld for the current session.

R46: TSLA 261111 390 CALL bullish was listed three times, once per detector (O1 09:41, OS1 10:05, OS2 10:14 ET). Now
one row per contract and side: the first trigger is the alert, later detectors are added to it as confirmations.
"""
from realtime.option_alert_projection import DetectorRunQuery
from tests.realtime.test_options_session_alerts import _page

SESSION = "2026-11-06"
CALL = {"legs": [{"contract_id": 390, "side": "BUY", "contract_type": "CALL", "ratio": 1}], "net_premium": "-307.50"}
PUT = {"legs": [{"contract_id": 372, "side": "BUY", "contract_type": "PUT", "ratio": 1}], "net_premium": "-213.50"}


def alert(evaluation_id, detector, hhmm, *, run, package=CALL, hits=1, underlyer="TSLA", direction=1, extra=()):
    at = f"2026-11-06T{hhmm}:00+00:00"
    return dict(evaluation_id=evaluation_id, evaluation_ids=[evaluation_id, *extra], candidate_id=f"c-{evaluation_id}",
                session_alert_id=f"s-{evaluation_id}", detector_id=detector, underlyer=underlyer, direction=direction,
                hit_count=hits, repeat_count=hits - 1, triggered_at=at, last_seen_at=at, run_id=run,
                scheduled_cycle=f"c{hhmm}", original_package=package)


def session():
    return [
        alert("o1", "O1", "14:41", run="r-early", hits=2, extra=("o1b",)),
        alert("os1", "OS1", "15:05", run="r-early"),
        alert("os2", "OS2", "15:14", run="r1"),  # the latest run
        alert("put", "O1", "15:02", run="r-early", package=PUT, direction=-1),
    ]


def read(scope="LATEST", *, current=SESSION, detector=None, rows=None):
    publication, identity = _page([dict(evaluation_id="os2", detector_id="OS2", scheduled_cycle="c14")])
    request = DetectorRunQuery(dataset_id="d1", scope=scope, detector=detector)
    return publication.detector_runs(request, session_rows=rows if rows is not None else session(),
                                     current_session_date=current, **identity)["data"]


def test_one_row_per_contract_and_side_with_the_later_detectors_as_confirmations():
    data = read("HISTORY", current=None)
    by_contract = {row["original_package"]["legs"][0]["contract_id"]: row for row in data["rows"]}
    assert sorted(by_contract) == [372, 390]
    call = by_contract[390]
    assert call["detector_id"] == "O1" and call["evaluation_id"] == "o1"  # the first trigger stays the alert
    assert call["detectors"] == ["O1", "OS1", "OS2"]
    assert [item["detector_id"] for item in call["confirmations"]] == ["OS1", "OS2"]
    assert [item["net_premium"] for item in call["confirmations"]] == ["-307.50", "-307.50"]
    assert (call["hit_count"], call["repeat_count"]) == (4, 3)
    assert call["last_seen_at"] == "2026-11-06T15:14:00+00:00"
    assert set(call["evaluation_ids"]) == {"o1", "o1b", "os1", "os2"}
    assert by_contract[372]["confirmations"] == []  # the opposite side is its own alert


def test_a_detector_filter_shows_that_detectors_own_rows():
    data = read("HISTORY", current=None, detector="OS2")
    assert [row["evaluation_id"] for row in data["rows"]] == ["os2"]
    assert "confirmations" not in data["rows"][0]


def test_latest_run_shows_the_alerts_triggered_or_hit_in_it():
    data = read("LATEST")
    # The call alert was hit by OS2 in the latest run; the put was not.
    assert [row["original_package"]["legs"][0]["contract_id"] for row in data["rows"]] == [390]
    assert data["new_alerts"] == 0  # the call alert was first triggered in an earlier run


def test_day_history_withholds_the_latest_run_for_the_current_session():
    only_new = [alert("os2", "OS2", "15:14", run="r1"),
                alert("put", "O1", "15:02", run="r-early", package=PUT, direction=-1)]
    data = read("HISTORY", rows=only_new)
    assert [row["evaluation_id"] for row in data["rows"]] == ["put"]  # the new OS2 alert waits for the next run
    assert data["withheld_run"]["run_id"] == "r1"
    past = read("HISTORY", rows=only_new, current="2026-11-07")
    assert sorted(row["evaluation_id"] for row in past["rows"]) == ["os2", "put"] and past["withheld_run"] is None
    latest = read("LATEST", rows=only_new)
    assert [row["evaluation_id"] for row in latest["rows"]] == ["os2"] and latest["new_alerts"] == 1


def test_without_the_session_list_the_release_rows_are_served_unchanged():
    publication, identity = _page([dict(evaluation_id="w", detector_id="O1", scheduled_cycle="c")])
    for scope in ("LATEST", "HISTORY"):
        data = publication.detector_runs(DetectorRunQuery(dataset_id="d1", scope=scope), **identity)["data"]
        assert [row["evaluation_id"] for row in data["rows"]] == ["w"] and data["withheld_run"] is None
