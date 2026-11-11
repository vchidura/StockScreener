"""G102 A0 (G100): the Options Alerts page as a session list on `rt.alert_current`.

Each window evaluated only the last 10 minutes of trades, so an alert left the page after about two windows. The
Options window now merges its v1 single-contract alert rows into one `rt.alert_current` row per session, detector and
contract (hits = distinct trades), and the API serves that list. Offline: `AlertTable` stands in for the table and
checks its constraints (unique event, valid_until after as_of, projection hash).
"""
from datetime import datetime, timedelta, timezone
import json
import logging
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

from realtime.advanced_options_native_loader import OPTIONS_ALERT_V1_VALUATION_SHA256
from realtime.advanced_options_projection import (
    AdvancedOptionsAlertsPagePublication,
    _one_alert_per_contract,
    load_options_session_alert_rows_cursor,
    options_session_alert_id,
    persist_options_session_alerts_cursor,
)
from realtime.contracts import canonical_sha256
from realtime.option_alert_projection import DetectorRunQuery
from realtime.portal_compatibility_repository import RealtimeOptionsAlertsPageRepository
from realtime.preflight import ProviderTier

UTC = timezone.utc
COHORT = UUID(int=7)
SESSION = "2026-11-06"
CLOSE = datetime(2026, 11, 6, 21, 0, tzinfo=UTC)


class AlertTable:
    """`rt.alert_current` in memory, for the statements the session writer and reader run."""

    def __init__(self):
        self.rows, self.result, self.statements = {}, [], []

    def execute(self, sql, params=()):
        self.statements.append(sql)
        if sql.startswith("SELECT alert_id::text AS alert_id,event_id::text"):
            self.result = [dict(alert_id=key, event_id=row["event_id"], projection=json.loads(row["projection"]))
                           for key, row in self.rows.items() if key in set(params[0])]
        elif sql.startswith("SELECT event_id::text AS event_id,alert_id::text"):
            self.result = [dict(event_id=row["event_id"], alert_id=key)
                           for key, row in self.rows.items() if row["event_id"] in set(params[0])]
        elif sql.startswith("INSERT INTO rt.alert_current"):
            alert_id, cohort_id, event_id, as_of, valid_until, schema, sha, projection = params
            assert all(row["event_id"] != event_id for row in self.rows.values()), "UNIQUE (event_id)"
            self._check(as_of, valid_until, sha, projection)
            self.rows.setdefault(alert_id, dict(cohort_id=cohort_id, event_id=event_id, as_of=as_of,
                                                valid_until=valid_until, schema=schema, projection=projection))
        elif sql.startswith("UPDATE rt.alert_current"):
            as_of, valid_until, schema, sha, projection, alert_id = params
            self._check(as_of, valid_until, sha, projection)
            self.rows[alert_id].update(as_of=as_of, valid_until=valid_until, schema=schema, projection=projection)
        elif sql.startswith("SELECT projection,valid_until FROM rt.alert_current"):
            cohort_id, start, end, schema, session = params
            matched = [row for row in self.rows.values()
                       if row["cohort_id"] == cohort_id and start <= row["as_of"] < end and row["schema"] == schema
                       and json.loads(row["projection"])["session_date"] == session]
            matched.sort(key=lambda row: row["as_of"], reverse=True)
            self.result = [dict(projection=json.loads(row["projection"]), valid_until=row["valid_until"])
                           for row in matched]
        elif sql.split()[0] in {"SAVEPOINT", "RELEASE", "ROLLBACK"}:
            self.result = []
        else:
            raise AssertionError(f"unexpected statement: {sql}")

    @staticmethod
    def _check(as_of, valid_until, sha, projection):
        assert valid_until is None or valid_until > as_of, "alert_current_check1"
        assert sha == canonical_sha256(json.loads(projection))

    def fetchall(self):
        return self.result

    def alert(self, detector, contract):
        row = self.rows[str(options_session_alert_id(SESSION, detector, contract))]
        return dict(row, projection=json.loads(row["projection"]))


def evaluation(minute, contract, trade, *, detector="O1", window="w1", policy=OPTIONS_ALERT_V1_VALUATION_SHA256,
               legs=1):
    leg = SimpleNamespace(contract_id=contract, valuation_policy_sha256=policy)
    return SimpleNamespace(
        evaluation_id=uuid5(NAMESPACE_URL, f"{window}:{detector}:{trade}"),
        event_id=uuid5(NAMESPACE_URL, f"trade:{trade}"), detector=SimpleNamespace(value=detector),
        observed_at=datetime(2026, 11, 6, 16, minute, tzinfo=UTC), package=SimpleNamespace(legs=(leg,) * legs),
    )


def write(table, evaluations, mark="first"):
    """One window: its alert rows grouped as the page builder groups them, then the session write."""
    pairs = [(item, dict(
        evaluation_id=str(item.evaluation_id), detector_id=item.detector.value, underlyer="NVDA",
        triggered_at=item.observed_at.isoformat(), first_selected_at=item.observed_at.isoformat(),
        last_seen_at=item.observed_at.isoformat(), hit_count=1, repeat_count=0, scheduled_cycle="c",
        exit_deadline=CLOSE.isoformat(), current_mark={"status": "UNAVAILABLE", "window": mark},
        management_status="UNAVAILABLE",
    )) for item in evaluations]
    return persist_options_session_alerts_cursor(
        table, cohort_id=COHORT, session_date=SESSION, alert_rows=_one_alert_per_contract(pairs),
        evaluations=evaluations,
    )


def test_one_alert_per_contract_for_the_session_counts_distinct_trades_across_overlapping_windows():
    table = AlertTable()
    assert write(table, [evaluation(40, 101, "A"), evaluation(41, 101, "B"), evaluation(41, 202, "C")]) == 2
    first = table.alert("O1", 101)
    assert first["projection"]["hit_count"] == 2 and first["event_id"] == str(uuid5(NAMESPACE_URL, "trade:A"))
    assert first["valid_until"] == CLOSE and first["as_of"] == datetime(2026, 11, 6, 16, 41, tzinfo=UTC)
    # The next window overlaps: it evaluates trade B again (a new evaluation id) and a new trade D.
    later = [evaluation(41, 101, "B", window="w2"), evaluation(45, 101, "D", window="w2")]
    assert write(table, later, mark="second") == 1
    merged = table.alert("O1", 101)["projection"]
    assert merged["hit_count"] == 3 and merged["repeat_count"] == 2
    assert merged["event_ids"] == sorted(str(uuid5(NAMESPACE_URL, f"trade:{trade}")) for trade in "ABD")
    assert len(merged["evaluation_ids"]) == 4
    # The first trigger stays; the latest window's mark and the last-seen time move.
    assert merged["triggered_at"] == first["projection"]["triggered_at"] == "2026-11-06T16:40:00+00:00"
    assert merged["last_seen_at"] == "2026-11-06T16:45:00+00:00" and merged["current_mark"]["window"] == "second"
    assert table.alert("O1", 101)["as_of"] == datetime(2026, 11, 6, 16, 45, tzinfo=UTC)
    assert table.alert("O1", 202)["projection"]["hit_count"] == 1
    # A rerun of the same window writes nothing.
    assert write(table, later, mark="third") == 0
    assert table.alert("O1", 101)["projection"]["current_mark"]["window"] == "second"


def test_stored_and_multi_leg_packages_keep_their_window_rows():
    table = AlertTable()
    assert write(table, [evaluation(40, 303, "S", policy="stored"), evaluation(41, 404, "M", legs=2)]) == 0
    assert table.rows == {} and table.statements == []


def test_two_detectors_on_one_trade_anchor_on_different_events(caplog):
    table = AlertTable()
    window = [evaluation(40, 101, "A"), evaluation(40, 101, "A", detector="O2"),
              evaluation(42, 101, "E", detector="O2"), evaluation(40, 101, "A", detector="O3")]
    with caplog.at_level(logging.WARNING, logger="realtime.advanced_options_projection"):
        assert write(table, window) == 2
    assert table.alert("O1", 101)["event_id"] == str(uuid5(NAMESPACE_URL, "trade:A"))
    second = table.alert("O2", 101)
    assert second["event_id"] == str(uuid5(NAMESPACE_URL, "trade:E")) and second["projection"]["hit_count"] == 2
    assert second["projection"]["triggered_at"] == "2026-11-06T16:40:00+00:00"
    # O3's only trade anchors O1's alert, so it is not stored (and stays on the window's own rows).
    assert str(options_session_alert_id(SESSION, "O3", 101)) not in table.rows
    assert "each of its trades anchors another alert" in caplog.text


def test_the_session_list_is_read_by_cutoff_newest_first_and_closes_at_the_exit_deadline():
    table = AlertTable()
    write(table, [evaluation(40, 101, "A"), evaluation(41, 202, "C")])
    write(table, [evaluation(45, 101, "D", window="w2")])

    def read(cutoff):
        return load_options_session_alert_rows_cursor(table, cohort_id=COHORT, session_date=SESSION, cutoff=cutoff)

    rows = read(datetime(2026, 11, 6, 17, 0, tzinfo=UTC))
    assert [row["contract_id"] for row in rows] == [101, 202]
    assert {row["lifecycle"] for row in rows} == {"OPEN"}
    assert [row["contract_id"] for row in read(datetime(2026, 11, 6, 16, 40, 30, tzinfo=UTC))] == [101]
    assert {row["lifecycle"] for row in read(CLOSE)} == {"CLOSED"}
    assert load_options_session_alert_rows_cursor(
        table, cohort_id=COHORT, session_date="2026-11-09", cutoff=CLOSE + timedelta(days=3)) == []


def _page(alert_rows):
    payload = dict(
        cohort_id=str(COHORT), provider_tier=ProviderTier.MASSIVE_ADVANCED.value, scope_sha256="s" * 64,
        dataset_id="d1", session_date=SESSION, published_at="2026-11-06T16:45:00+00:00",
        run={"run_id": "r1", "observed_time": "2026-11-06T16:45:00+00:00"}, policy_sha256="p" * 64,
        evaluation_rows=[], alert_rows=alert_rows,
    )
    identity = dict(cohort_id=COHORT, provider_tier=ProviderTier.MASSIVE_ADVANCED, scope_sha256="s" * 64)
    return AdvancedOptionsAlertsPagePublication(payload), identity


def test_the_page_serves_the_session_list_and_keeps_the_windows_other_rows():
    table = AlertTable()
    write(table, [evaluation(40, 101, "A"), evaluation(41, 202, "C")])
    latest = [evaluation(45, 101, "D", window="w2")]
    write(table, latest)
    session = load_options_session_alert_rows_cursor(
        table, cohort_id=COHORT, session_date=SESSION, cutoff=datetime(2026, 11, 6, 16, 46, tzinfo=UTC))
    window_v1 = dict(evaluation_id=str(latest[0].evaluation_id), detector_id="O1", scheduled_cycle="c2")
    multi_leg = dict(evaluation_id="multi", detector_id="O3", scheduled_cycle="c2")
    publication, identity = _page([window_v1, multi_leg])
    request = DetectorRunQuery(dataset_id="d1")
    rows = publication.detector_runs(request, session_rows=session, **identity)["data"]["rows"]
    # 202 left the window's own rows two windows ago; the session list keeps it. The window's v1 row is covered.
    assert sorted(str(row.get("contract_id", row["evaluation_id"])) for row in rows) == ["101", "202", "multi"]
    assert next(row for row in rows if row.get("contract_id") == 101)["hit_count"] == 2
    assert publication.detector_runs(request, **identity)["data"]["rows"] == [window_v1, multi_leg]


def test_a_failed_session_lookup_serves_the_release_rows():
    class Broken(AlertTable):
        def execute(self, sql, params=()):
            if sql.startswith("SELECT projection"):
                raise RuntimeError("statement timeout")
            super().execute(sql, params)

    cursor = Broken()
    publication, _ = _page([])
    assert RealtimeOptionsAlertsPageRepository._advanced_session_alerts(
        cursor, publication, cohort_id=COHORT, cutoff=CLOSE) is None
    assert cursor.statements[-1] == "ROLLBACK TO SAVEPOINT options_session_alerts"
    healthy = AlertTable()
    assert RealtimeOptionsAlertsPageRepository._advanced_session_alerts(
        healthy, publication, cohort_id=COHORT, cutoff=CLOSE) == []
    assert healthy.statements[0] == "SAVEPOINT options_session_alerts"
    assert healthy.statements[-1] == "RELEASE SAVEPOINT options_session_alerts" and len(healthy.statements) == 3


def test_a_later_runs_repeat_folds_into_its_alert_and_o1_direction_follows_the_package():
    """Nov 10 (live): TSLA 372.5 PUT was listed twice, the 18:05 session alert and the 18:10 run's row for the same
    candidate, with 0 repeat hits; and the bought put read Bullish (O1 has no direction, the row defaulted to +1)."""
    put = {"legs": [{"side": "BUY", "contract_type": "PUT", "ratio": 1}]}
    call = {"legs": [{"side": "BUY", "contract_type": "CALL", "ratio": 1}]}
    session = [
        dict(evaluation_id="e1", evaluation_ids=["e1"], candidate_id="tsla", detector_id="O1", underlyer="TSLA",
             hit_count=1, repeat_count=0, triggered_at="2026-11-06T16:41:52+00:00",
             last_seen_at="2026-11-06T16:41:52+00:00", scheduled_cycle="c1", direction=1, original_package=put),
        dict(evaluation_id="e2", evaluation_ids=["e2", "e3"], candidate_id="nvda", detector_id="O1",
             underlyer="NVDA", hit_count=2, repeat_count=1, triggered_at="2026-11-06T16:36:40+00:00",
             last_seen_at="2026-11-06T16:40:00+00:00", scheduled_cycle="c1", direction=1, original_package=call),
    ]
    later = dict(evaluation_id="e9", candidate_id="tsla", detector_id="O1", underlyer="TSLA", scheduled_cycle="c2",
                 triggered_at="2026-11-06T16:45:00+00:00", direction=1, original_package=put)
    multi_leg = dict(evaluation_id="multi", detector_id="O3", underlyer="SPY", scheduled_cycle="c2")
    publication, identity = _page([later, multi_leg])
    data = publication.detector_runs(DetectorRunQuery(dataset_id="d1"), session_rows=session, **identity)["data"]
    by_candidate = {row.get("candidate_id", row["evaluation_id"]): row for row in data["rows"]}
    assert sorted(by_candidate) == ["multi", "nvda", "tsla"]
    assert (by_candidate["tsla"]["hit_count"], by_candidate["tsla"]["repeat_count"]) == (2, 1)
    assert by_candidate["tsla"]["last_seen_at"] == "2026-11-06T16:45:00+00:00"
    assert (data["new_alerts"], data["repeat_hits"]) == (3, 2)
    assert by_candidate["tsla"]["direction"] == -1 and by_candidate["nvda"]["direction"] == 1
    assert session[0]["hit_count"] == 1  # the caller's rows are not changed
