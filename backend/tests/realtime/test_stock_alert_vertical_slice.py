from dataclasses import replace
from datetime import timedelta
from functools import lru_cache
import json
from uuid import UUID

from fastapi.testclient import TestClient
import pytest

from realtime.contracts import EQUITY_VALIDATION_UNIVERSE, canonical_json, canonical_sha256
from realtime.portal_api import RealtimePortalRepositories, create_realtime_portal_app
from realtime.stock_alert_projection import (
    STOCK_ALERTS_VERSION, RetainedAlertStream, alert_domain_inputs, evaluate_alert_inputs,
    produce_realtime_stock_alerts,
)
from realtime.stock_alert_repository import RealtimeStockAlertViewRepository
from research.stock_alerts import alert_page
from research.stock_idea_engine import Candidate, digest
from research.stock_idea_forward import forward_config, forward_decision
from research.stock_idea_replay import session_windows, utc
from tests.realtime.test_ticker_vertical_slice import TickerStore, ticker_fixture
from tests.realtime.test_equity_validation_history import COHORT_ID, OBSERVED_AT


@lru_cache(maxsize=1)
def alert_fixture():
    inputs = ticker_fixture()
    return replace(inputs, bars=tuple(dict(row, created_at=OBSERVED_AT) for row in inputs.bars))


@lru_cache(maxsize=1)
def timely_fixture():
    inputs = alert_fixture()
    # Independent synthetic feed clocks, not a repair/backdate of retained data.
    rows = tuple(dict(
        row, observed_at=row["bar_end"] + timedelta(minutes=1),
        created_at=row["bar_end"] + timedelta(minutes=1),
        replay_available_at=row["bar_end"] + timedelta(minutes=1),
        volume=row["volume"] * 1000,
        payload_sha256=canonical_sha256(("synthetic-timely-alert-feed", row["payload_sha256"])),
    ) for row in inputs.bars)
    return replace(inputs, bars=rows, members=tuple(
        dict(member, observed_at=utc("2026-09-30T19:00:00+00:00")) for member in inputs.members
    ))


@lru_cache(maxsize=1)
def retained_streams():
    inputs = timely_fixture()
    bars, actions = alert_domain_inputs(inputs)
    members = [dict(security_id=str(row["security_id"]), ticker=row["ticker"]) for row in inputs.members]
    boundary = session_windows("2026-10-02")[0][1]
    result = []
    for stream in ("intraday", "swing"):
        policy = forward_config(quality_version=2, swing=stream == "swing")
        state = dict(members=members, enrolled_at="2026-09-30T20:00:00+00:00",
                     next_boundary="2026-10-05T14:00:00+00:00", bars={}, actions=actions)
        for bar in bars:
            state["bars"].setdefault(bar["security_id"] + "|" + bar["interval"], {})[bar["bar_start"]] = bar
        packets = []
        for index, member in enumerate(members):
            bar = next(row for row in bars if row["security_id"] == member["security_id"]
                       and row["interval"] == "30m" and utc(row["bar_end"]) == boundary)
            model = ("resumption", "acceptance", "failure")[index % 3]
            price = bar["close"]
            candidate = Candidate(
                member["security_id"], member["ticker"], model, "30m", 1, "INTRADAY",
                "synthetic-retained-trigger", boundary, boundary + timedelta(minutes=30),
                boundary + timedelta(minutes=15), price - 1, price - 5, price + 15, 2, price,
                .9, 0, 3, 30_000_000, .2, (bar["revision_id"],),
                policy_version=policy["models"][model]["intraday_version"],
            )
            packets.append(dict(
                security_id=member["security_id"], interval="30m", market_time=boundary,
                available_at=candidate.available_at, ready=True, candidates=[candidate],
                updates=[], revision_ids=[bar["revision_id"]],
            ))
        publications = []
        if stream == "swing":
            daily_boundary = session_windows("2026-10-01", "1d")[0][1]
            daily_packets = []
            for packet in packets:
                confirmation = packet["candidates"][0]
                daily_bar = next(row for row in bars if row["security_id"] == confirmation.security_id
                                 and row["interval"] == "1d" and utc(row["bar_end"]) == daily_boundary)
                candidate = replace(
                    confirmation, interval="1d",
                    horizon=f"DAILY_{policy['models'][confirmation.model]['daily_horizon_sessions']}",
                    trigger_at=daily_boundary, available_at=daily_boundary + timedelta(minutes=15),
                    expires_at=session_windows("2026-10-02")[0][0],
                    revision_ids=(daily_bar["revision_id"],),
                    policy_version=policy["models"][confirmation.model]["daily_family"],
                )
                daily_packets.extend([
                    dict(packet, interval="1d", market_time=daily_boundary,
                         available_at=candidate.available_at, candidates=[candidate],
                         revision_ids=[daily_bar["revision_id"]]),
                    dict(packet, market_time=daily_boundary, available_at=candidate.available_at,
                         candidates=[], revision_ids=[daily_bar["revision_id"]]),
                ])
            state, publication, _ = forward_decision(
                state, daily_packets, boundary=daily_boundary, actual_time=daily_boundary + timedelta(minutes=17),
                members=members, policy_hash=digest(policy), config=policy,
            )
            publications.append(publication)
        state, publication, _ = forward_decision(
            state, packets, boundary=boundary, actual_time=boundary + timedelta(minutes=17),
            members=members, policy_hash=digest(policy), config=policy,
        )
        assert publication["selected"], f"synthetic {stream} did not execute owning selection"
        publications.append(publication)
        if stream == "intraday":
            later = boundary + timedelta(minutes=30)
            empty = [dict(packet, market_time=later, available_at=later + timedelta(minutes=15),
                          candidates=[], updates=[]) for packet in packets]
            state, publication, _ = forward_decision(
                state, empty, boundary=later, actual_time=later + timedelta(minutes=17),
                members=members, policy_hash=digest(policy), config=policy,
            )
            assert publication["selected"] == []
            publications.append(publication)
        result.append(RetainedAlertStream(COHORT_ID, "a" * 64, stream, policy, state, tuple(publications)))
    return tuple(result)


@lru_cache(maxsize=1)
def retained_snapshot():
    return produce_realtime_stock_alerts(timely_fixture(), streams=retained_streams())


@lru_cache(maxsize=1)
def alert_snapshot():
    return produce_realtime_stock_alerts(alert_fixture())


class AlertStore(TickerStore):
    def __init__(self, references):
        super().__init__()
        self.references = references

    def execute(self, sql, params=()):
        if sql.startswith("SELECT revision_id"):
            self.statements.append(sql)
            reference = next(row for row in self.references if str(row["security_id"]) == params[0])
            self.result = [dict(revision_id="wrong" if self.reference_drift else reference["security_revision_id"],
                                payload_sha256=reference["payload_sha256"])]
        elif sql.startswith("SELECT snapshot.*") and "snapshot.snapshot_key=%s" in sql:
            # The shared fake reads the PLATFORM key parameter of this statement as the cutoff.
            self.statements.append(sql)
            row = self.snapshots.get(params[-1])
            self.result = []
            if (self.screening_available and row and row["generated_at"] <= params[3]
                    and (row["cohort_id"], row["snapshot_type"], row["snapshot_key"]) == tuple(params[:3])):
                self.result = [dict(row, cohort_manifest_sha256="a" * 64)]
        else:
            super().execute(sql, params)


def alert_app(*, snapshot=None, clock=lambda: OBSERVED_AT):
    snapshot = snapshot or alert_snapshot()
    store = AlertStore(snapshot.payload["references"])
    store.publish(snapshot)
    reader = RealtimeStockAlertViewRepository(store.cursor)
    return create_realtime_portal_app(
        RealtimePortalRepositories(None, None, None, stock_alert_views=reader), clock=clock,
    ), store


def test_nine_ticker_real_detectors_do_not_turn_bootstrap_into_forward_publications():
    snapshot = alert_snapshot()
    payload = json.loads(canonical_json(snapshot.payload))
    assert snapshot.snapshot_type == STOCK_ALERTS_VERSION
    assert {row["ticker"] for row in payload["observations"]} == set(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
    assert len(payload["observations"]) == 27
    assert all(row["status"] == "UNAVAILABLE" and not row["selected"] for row in payload["observations"])
    assert payload["shadow"]["status"] == "UNAVAILABLE"
    assert payload["shadow"]["publications"] == payload["shadow"]["alerts"] == []
    assert set(payload["missing_producers"]) == {"forward", "replay", "context"}
    assert payload["provider_fetch_performed"] is payload["execution_permission"] is False


def test_detector_only_bar_input_produces_the_same_alert_snapshot_as_portal_history():
    inputs = timely_fixture()
    detector_bars = tuple(
        row for row in inputs.bars
        if row["interval"] in {"30m", "1d"}
    )

    assert produce_realtime_stock_alerts(replace(inputs, bars=detector_bars)) == (
        produce_realtime_stock_alerts(inputs)
    )


def test_original_alert_and_eod_routes_read_retained_outputs_only():
    app, store = alert_app()
    with TestClient(app) as client:
        for view in ("latest", "history", "open"):
            response = client.get("/api/stocks/alert-view", params=dict(
                cohort_id=str(COHORT_ID), source="SHADOW", view=view,
            ))
            assert response.status_code == 200, response.text
            assert response.json()["rows"] == []
            assert response.json()["status"] == "UNAVAILABLE"
            assert response.json()["source"] == "SHADOW"
        replay = client.get("/api/stocks/alert-view", params=dict(cohort_id=str(COHORT_ID), source="REPLAY"))
        assert replay.status_code == 503 and "REPLAY_COHORT_UNAVAILABLE" in replay.text
        eod = client.get("/api/stocks/alert-eod-review", params=dict(cohort_id=str(COHORT_ID)))
        assert eod.status_code == 200 and eod.json()["storage_ready"] is False
        assert eod.json()["rows"] == []
    assert not any(sql.startswith(("INSERT", "UPDATE", "DELETE")) for sql in store.statements[3:])
    cohort_queries = [sql for sql in store.statements if "SELECT snapshot.*, manifest.manifest_sha256" in sql]
    assert cohort_queries
    assert all("JOIN rt.screening_current" in sql and "screening.status='COMPLETE'" in sql
        and "screening.generated_at<=%s" in sql and "snapshot.provider_tier=screening.provider_tier" in sql
        and "rt.capability_profiles" not in sql and "cohort.profile_id" not in sql for sql in cohort_queries)


def test_retained_original_contracts_match_the_shared_domain_engine():
    snapshot = retained_snapshot()
    app, store = alert_app(snapshot=snapshot)
    stored = json.loads(canonical_json(snapshot.payload))
    assert all(row["status"] == "EVALUATED" for row in stored["observations"])
    with TestClient(app) as client:
        for view in ("latest", "history", "open"):
            for trade_type in (None, "INTRADAY", "SWING"):
                for offset, limit in ((0, 100), (1, 2)):
                    params = dict(cohort_id=str(COHORT_ID), source="SHADOW", view=view,
                                  session_date="2026-10-02", offset=offset, limit=limit,
                                  sort="ticker", descending=False)
                    if trade_type:
                        params["trade_type"] = trade_type
                    response = client.get("/api/stocks/alert-view", params=params)
                    assert response.status_code == 200, response.text
                    expected = alert_page(stored["shadow"], session="2026-10-02", view=view,
                                          offset=offset, limit=limit, sort="ticker", descending=False,
                                          trade_type=trade_type)
                    for key in ("rows", "total", "counts", "run", "runs", "latest_runs", "withheld_run"):
                        assert response.json()[key] == expected[key]
        latest = client.get("/api/stocks/alert-view", params=dict(cohort_id=str(COHORT_ID),
                           source="SHADOW", trade_type="INTRADAY"))
        assert latest.json()["rows"] == [] and latest.json()["latest_runs"]
        history = client.get("/api/stocks/alert-view", params=dict(cohort_id=str(COHORT_ID),
                            source="SHADOW", view="history", trade_type="INTRADAY"))
        assert history.json()["total"] == 9
        assert all(row["status"] == "CLOSED" for row in history.json()["rows"]), [
            (row["ticker"], row["model"], row["status"], row["reason"]) for row in history.json()["rows"]
        ]
        positions = client.get("/api/stocks/alert-view", params=dict(cohort_id=str(COHORT_ID), view="open"))
        assert positions.json()["total"] == 9
        assert all(row["trade_style"] == "SWING" and row["status"] == "OPEN" for row in positions.json()["rows"])
        eod = client.get("/api/stocks/alert-eod-review", params=dict(cohort_id=str(COHORT_ID), session_date="2026-10-02"))
        assert eod.status_code == 200, eod.text
        assert eod.json()["total"] == 18 and eod.json()["storage_ready"]
        for row in positions.json()["rows"]:
            assert row["context"]["status"] == "UNAVAILABLE"
            assert row["stop"] < row["trigger_price"] < row["target"]
            assert row["hits"] is not None
    assert all(not sql.startswith(("INSERT", "UPDATE", "DELETE")) for sql in store.statements[3:])


def test_timely_fixture_executes_unmocked_detector_functions_without_forcing_matches():
    inputs = timely_fixture()
    bars, actions = alert_domain_inputs(inputs)
    packets = evaluate_alert_inputs(inputs, bars, actions, forward_config(quality_version=2))
    assert {packet["security_id"] for packet in packets} == {str(row["security_id"]) for row in inputs.members}
    assert any(packet["ready"] and packet["interval"] == "30m" for packet in packets)
    for packet in packets:
        for candidate in packet["candidates"]:
            assert candidate.available_at >= candidate.trigger_at + timedelta(minutes=15)


@pytest.mark.parametrize("defect", [
    "cohort", "policy", "future-enrollment", "missing-position", "future-publication", "late-source",
])
def test_retained_stream_identity_policy_and_clock_defects_fail_closed(defect):
    from copy import deepcopy
    inputs = timely_fixture()
    stream = deepcopy(retained_streams()[0])
    if defect == "cohort":
        stream = replace(stream, cohort_id=UUID(int=9))
    elif defect == "policy":
        stream = replace(stream, policy=dict(stream.policy, minimum_room_risk=0))
    elif defect == "future-enrollment":
        stream.state["enrolled_at"] = (OBSERVED_AT + timedelta(seconds=1)).isoformat()
    elif defect == "missing-position":
        stream.state["positions"] = {}
    elif defect == "future-publication":
        stream.publications[0]["actual_publication_at"] = (OBSERVED_AT + timedelta(seconds=1)).isoformat()
    else:
        stream.publications[0]["input_deadline"] = stream.publications[0]["window_key"]
    from realtime.stock_alert_projection import _retained_snapshot
    bars, actions = alert_domain_inputs(inputs)
    with pytest.raises(ValueError):
        _retained_snapshot(inputs, stream, bars, actions)


def test_stale_retained_history_is_visible_without_rewriting_outcomes_or_publications():
    snapshot = retained_snapshot()
    app, _ = alert_app(snapshot=snapshot, clock=lambda: OBSERVED_AT + timedelta(days=1))
    original = canonical_json(snapshot.payload)
    with TestClient(app) as client:
        history = client.get("/api/stocks/alert-view", params=dict(
            cohort_id=str(COHORT_ID), source="SHADOW", view="history", session_date="2026-10-02",
        ))
        assert history.status_code == 200, history.text
        assert history.json()["total"] == 18
        assert any("stale" in warning for warning in history.json()["warnings"])
        latest = client.get("/api/stocks/alert-view", params=dict(cohort_id=str(COHORT_ID), source="SHADOW"))
        assert latest.json()["rows"] == []
    assert canonical_json(snapshot.payload) == original


@pytest.mark.parametrize("params", [
    {"sort": "invalid"}, {"session_date": "2020-01-01"}, {"run": "invalid"},
    {"offset": -1}, {"limit": 201}, {"source": "CANONICAL"}, {"direction": 2},
])
def test_invalid_original_query_shapes_are_rejected(params):
    app, _ = alert_app()
    with TestClient(app) as client:
        response = client.get("/api/stocks/alert-view", params=dict(cohort_id=str(COHORT_ID), **params))
        assert response.status_code == 422


@pytest.mark.parametrize("defect", ["checksum", "manifest", "cohort", "reference", "action", "future"])
def test_stored_alert_projection_defects_are_unavailable(defect):
    app, store = alert_app()
    row = next(iter(store.snapshots.values()))
    if defect == "checksum":
        row["payload_sha256"] = "b" * 64
    elif defect == "manifest":
        row["source_manifest_sha256"] = "b" * 64
    elif defect == "cohort":
        row["source_manifest"]["cohort_id"] = str(UUID(int=9))
    elif defect == "reference":
        store.reference_drift = True
    elif defect == "action":
        store.action_drift = True
    else:
        row["generated_at"] = OBSERVED_AT + timedelta(seconds=1)
    with TestClient(app) as client:
        response = client.get("/api/stocks/alert-view", params=dict(cohort_id=str(COHORT_ID)))
        assert response.status_code == 503


def test_expired_alert_projection_remains_browsable_with_stale_warning():
    app, store = alert_app(clock=lambda: OBSERVED_AT + timedelta(hours=1))
    next(iter(store.snapshots.values()))["valid_until"] = OBSERVED_AT
    with TestClient(app) as client:
        response = client.get("/api/stocks/alert-view", params=dict(cohort_id=str(COHORT_ID)))
    assert response.status_code == 200
    assert "Realtime retained alert data stale; no request-time refresh performed" in response.json()["warnings"]


def test_alert_projection_cli_is_nonexecuting_without_exact_target_authorization(monkeypatch, capsys):
    from scripts import project_realtime_screening as cli
    def forbidden(*args, **kwargs):
        raise AssertionError("Nonexecuting alert projection attempted I/O")
    monkeypatch.setattr(cli, "load_dotenv", forbidden)
    monkeypatch.setattr(cli.pool, "SimpleConnectionPool", forbidden)
    monkeypatch.setattr(cli, "materialize_realtime_stock_alerts", forbidden)
    args = ["--cohort-id", str(COHORT_ID), "--session", "2026-10-02",
            "--cutoff", OBSERVED_AT.isoformat(), "--source-generation", "1", "--surface", "alerts"]
    assert cli.main(args) == 0
    assert "NOT_EXECUTED" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        cli.parse_args([*args, "--execute"])


@pytest.mark.parametrize("retained_forward", [True, None])
def test_default_materializer_never_replaces_forward_or_unknown_state(monkeypatch, retained_forward):
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from realtime.database_access import RealtimeDatabaseRole
    from realtime.worker_database import RealtimeProcessDatabase
    from realtime import stock_alert_repository as repository
    from tests.realtime.portal_v2_support import release_rows, snapshot_entry
    database = MagicMock(spec=RealtimeProcessDatabase)
    database.process_plan = SimpleNamespace(role=RealtimeDatabaseRole.COMPUTE)
    cursor = database.transaction.return_value.__enter__.return_value
    pointer, release = release_rows(UUID(int=5), COHORT_ID, [snapshot_entry(alert_snapshot())],
                                    cutoff=OBSERVED_AT, session_date=OBSERVED_AT.date())
    cursor.fetchone.side_effect = [pointer, release,
                                   {"retained_body": True, "retained_forward": retained_forward}]
    def forbidden(*args, **kwargs):
        raise AssertionError("Retained/unknown forward state must not reach production or writes")
    monkeypatch.setattr(repository, "load_realtime_stock_alert_input", forbidden)
    monkeypatch.setattr(repository, "persist_portal_snapshot_cursor", forbidden)
    with pytest.raises(ValueError, match="refusing to replace retained publications"):
        repository.compute_realtime_stock_alerts(database, cohort_id=COHORT_ID)
    assert cursor.execute.call_count == 3
    assert all(call.args[0].startswith("SELECT ") for call in cursor.execute.call_args_list)
    assert database.transaction.call_args.kwargs["read_only"] is True


@pytest.mark.parametrize("previous", ["no_release", "expired_body", "empty_forward"])
def test_materializer_replaces_only_unretained_or_forward_free_state(monkeypatch, previous):
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from realtime.database_access import RealtimeDatabaseRole
    from realtime.worker_database import RealtimeProcessDatabase
    from realtime import stock_alert_repository as repository
    from tests.realtime.portal_v2_support import release_rows, snapshot_entry
    database = MagicMock(spec=RealtimeProcessDatabase)
    database.process_plan = SimpleNamespace(role=RealtimeDatabaseRole.COMPUTE)
    cursor = database.transaction.return_value.__enter__.return_value
    pointer, release = release_rows(UUID(int=5), COHORT_ID, [snapshot_entry(alert_snapshot())],
                                    cutoff=OBSERVED_AT, session_date=OBSERVED_AT.date())
    cursor.fetchone.side_effect = {
        "no_release": [None],
        "expired_body": [pointer, release, {"retained_body": False, "retained_forward": None}],
        "empty_forward": [pointer, release, {"retained_body": True, "retained_forward": False}],
    }[previous]
    monkeypatch.setattr(repository, "load_realtime_stock_alert_input",
                        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("reached production")))
    with pytest.raises(RuntimeError, match="reached production"):
        repository.compute_realtime_stock_alerts(database, cohort_id=COHORT_ID)


def _alert_database(role):
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from realtime.worker_database import RealtimeProcessDatabase
    database = MagicMock(spec=RealtimeProcessDatabase)
    database.process_plan = SimpleNamespace(role=role)
    return database, database.transaction.return_value.__enter__.return_value


def test_alert_compute_and_publication_each_require_their_own_role_and_the_split_never_writes_under_compute(
        monkeypatch):
    from realtime.database_access import RealtimeDatabaseRole
    from realtime import stock_alert_repository as repository
    compute, compute_cursor = _alert_database(RealtimeDatabaseRole.COMPUTE)
    publication, publication_cursor = _alert_database(RealtimeDatabaseRole.PUBLICATION)
    snapshot = alert_snapshot()
    with pytest.raises(ValueError, match="COMPUTE"):
        repository.compute_realtime_stock_alerts(publication, cohort_id=COHORT_ID)
    with pytest.raises(ValueError, match="PUBLICATION"):
        repository.publish_realtime_stock_alerts(compute, snapshot)
    with pytest.raises(ValueError, match="COMPUTE"):
        repository.materialize_realtime_stock_alerts(publication, publication, cohort_id=COHORT_ID)
    with pytest.raises(ValueError, match="PUBLICATION"):
        repository.materialize_realtime_stock_alerts(compute, compute, cohort_id=COHORT_ID)
    persisted = []
    monkeypatch.setattr(repository, "load_current_release", lambda cursor, cohort_id: None)
    monkeypatch.setattr(repository, "load_realtime_stock_alert_input", lambda cursor, **scope: scope)
    monkeypatch.setattr(repository, "produce_realtime_stock_alerts", lambda inputs: snapshot)
    monkeypatch.setattr(repository, "persist_portal_snapshot_cursor",
                        lambda cursor, row, **options: persisted.append((cursor, options)) or row)
    published = []
    monkeypatch.setattr(repository, "publish_portal_release_cursor",
                        lambda cursor, receipt, rows, **options: published.append((cursor, rows, options)))
    assert repository.materialize_realtime_stock_alerts(
        compute, publication, receipt="receipt", cohort_id=COHORT_ID) is snapshot
    assert persisted == [(publication_cursor, {})] and not compute_cursor.method_calls
    assert published == [(publication_cursor, (snapshot,), {})]
    assert compute.transaction.call_args.kwargs["read_only"] is True
    assert publication.transaction.call_args.kwargs.get("read_only") is None


def test_alert_session_close_persists_the_kept_variant_and_publishes_the_kept_release(monkeypatch):
    from realtime.database_access import RealtimeDatabaseRole
    from realtime import stock_alert_repository as repository
    from realtime.portal_projection_repository import RELEASE_CLASS_SESSION_CLOSE, RETENTION_HOT_AND_KEPT
    publication, cursor = _alert_database(RealtimeDatabaseRole.PUBLICATION)
    snapshot, kept = alert_snapshot(), object()
    persisted, published = [], []
    monkeypatch.setattr(repository, "persist_portal_snapshot_cursor",
                        lambda cur, row, **options: persisted.append(options) or kept)
    monkeypatch.setattr(repository, "publish_portal_release_cursor",
                        lambda cur, receipt, rows, **options: published.append((rows, options)))
    repository.publish_realtime_stock_alerts(publication, snapshot, receipt="receipt", session_close=True)
    assert persisted == [{"retention": RETENTION_HOT_AND_KEPT}]
    assert published[0] == ((snapshot,), {})
    assert published[1][0] == (kept,) and published[1][1]["release_class"] == RELEASE_CLASS_SESSION_CLOSE
    persisted.clear(), published.clear()
    repository.publish_realtime_stock_alerts(publication, snapshot, receipt=None, session_close=True)
    assert published == [] and persisted == [{"retention": RETENTION_HOT_AND_KEPT}]


@pytest.mark.parametrize("defect", ["clock", "identity", "lineage", "coverage", "native-window"])
def test_alert_input_defects_fail_closed(defect):
    inputs = alert_fixture()
    bars = list(inputs.bars)
    native = next(index for index, row in enumerate(bars) if row["interval"] == "30m")
    if defect == "clock":
        bars[native] = dict(bars[native], created_at=OBSERVED_AT + timedelta(seconds=1))
    elif defect == "identity":
        bars[native] = dict(bars[native], security_id=COHORT_ID)
    elif defect == "lineage":
        bars[native] = dict(bars[native], source_revision_ids=())
    elif defect == "coverage":
        inputs = replace(inputs, coverage=())
    else:
        bars.pop(native)
    with pytest.raises(ValueError):
        produce_realtime_stock_alerts(replace(inputs, bars=tuple(bars)))
