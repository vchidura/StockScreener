from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
import json
import math
from unittest.mock import MagicMock
from uuid import UUID
from uuid import NAMESPACE_URL, uuid5

from fastapi.testclient import TestClient
import pytest

from equity.portal_scanners import compute_default_scanner_snapshots
from realtime.contracts import EQUITY_VALIDATION_UNIVERSE, canonical_json, canonical_sha256
from realtime.dashboard_projection import dashboard_frames, produce_realtime_dashboard
from realtime.dashboard_repository import RealtimeDashboardRepository
from realtime.portal_api import RealtimePortalRepositories, create_realtime_portal_app
from realtime.screening_projection import SCREENING_BASIS
from tests.realtime.test_screening_vertical_slice import COHORT_ID, OBSERVED_AT, screening_fixture
from tests.realtime.test_ticker_vertical_slice import TickerStore


@lru_cache(maxsize=1)
def dashboard_fixture():
    inputs = screening_fixture()
    reflected = set(EQUITY_VALIDATION_UNIVERSE.equity_tickers[1::2])
    sessions = sorted({row["session_date"] for row in inputs.bars if row["interval"] == "1d"})
    ordinal = {session: index for index, session in enumerate(sessions)}
    bars = []
    for original in inputs.bars:
        row = dict(original, created_at=original["observed_at"], availability_mode="HISTORICAL_RECONSTRUCTED",
                   ingestion_segment_id=uuid5(NAMESPACE_URL, "synthetic-dashboard-segment"))
        index = ordinal[row["session_date"]]
        offset = index * .04 + 7 * math.sin(index / 8) + (3 if row["session_date"] == inputs.session else 0)
        for key in ("open", "high", "low", "close"):
            row[key] += offset
        if row["ticker"] in reflected:
            row.update(open=500 - row["open"], close=500 - row["close"],
                       high=500 - row["low"], low=500 - row["high"])
        if row["ticker"] == "AAPL" and row["session_date"] == inputs.session:
            row["volume"] *= 10
        row["payload_sha256"] = canonical_sha256(("synthetic-dashboard", row["payload_sha256"], row["ticker"]))
        bars.append(row)
    return replace(inputs, bars=tuple(bars), members=tuple(
        dict(member, exchange="XNYS", effective_from=member["observed_at"]) for member in inputs.members
    ))


@lru_cache(maxsize=1)
def dashboard_snapshot():
    return produce_realtime_dashboard(dashboard_fixture())


class DashboardStore(TickerStore):
    def execute(self, sql, params=()):
        if sql.startswith("SELECT snapshot.*"):
            self.statements.append(sql)
            self.result = []
            key = params[:3]
            identity = params[8] or self.current.get(key)
            row = self.snapshots.get(identity)
            if row and (row["cohort_id"], row["snapshot_type"], row["snapshot_key"]) == key and row["generated_at"] <= params[3]:
                self.result = [dict(row, cohort_manifest_sha256="a" * 64, stock_count=9)]
        elif sql.startswith("SELECT revision_id"):
            self.statements.append(sql)
            reference = next(row for row in dashboard_fixture().members if str(row["security_id"]) == params[0])
            self.result = [dict(revision_id="wrong" if self.reference_drift else reference["security_revision_id"],
                                payload_sha256=reference["payload_sha256"])]
        else:
            super().execute(sql, params)


def test_dashboard_reader_uses_reader_visible_screening_tier_identity():
    source = __import__("pathlib").Path(__file__).parents[2].joinpath(
        "realtime", "dashboard_repository.py",
    ).read_text(encoding="utf-8")

    assert "rt.screening_current AS screening_current" in source
    assert "rt.screening_generations AS screening" in source
    assert "snapshot.provider_tier=screening.provider_tier" in source
    assert "cohort.profile_id" not in source


def dashboard_app(*, snapshot=None, clock=lambda: OBSERVED_AT):
    store = DashboardStore()
    store.publish(snapshot or dashboard_snapshot())
    reader = RealtimeDashboardRepository(store.cursor)
    app = create_realtime_portal_app(
        RealtimePortalRepositories(None, None, None, dashboard=reader), clock=clock,
    )
    return app, store


def test_exact_nine_real_domain_outputs_and_immutable_writer():
    inputs, snapshot = dashboard_fixture(), dashboard_snapshot()
    assert tuple(sorted(row["ticker"] for row in snapshot.payload["overview"])) == tuple(sorted(EQUITY_VALIDATION_UNIVERSE.equity_tickers))
    frames, _ = dashboard_frames(inputs)
    owning = compute_default_scanner_snapshots(
        sorted(frames), frames, {key: frames[key] for key in ("SPY", "QQQ")}, observed_at=inputs.cutoff,
    )["SCAN_ALL_1D_5"]
    assert canonical_json(snapshot.payload["scan"]) == canonical_json(owning)
    assert owning["ma_crossover"]["total_signals"] == 9
    assert owning["fibonacci"]["total_signals"] == 5
    assert owning["market_regime"]["spy"]["wsma_200"] is None
    assert snapshot.payload["context"]["cohort"]["eligible_ranked"] == 9
    assert all(row["sector"] is None for row in snapshot.payload["overview"])
    assert any(row["chg_pct"] > 0 for row in snapshot.payload["overview"])
    assert any(row["chg_pct"] < 0 for row in snapshot.payload["overview"])
    aapl = next(row for row in snapshot.payload["overview"] if row["ticker"] == "AAPL")
    own = [row for row in inputs.bars if row["ticker"] == "AAPL" and row["interval"] == "1d"
           and row["adjustment_basis"] == "RAW_UNADJUSTED"]
    assert aapl["rel_vol"] == round(own[-1]["volume"] / (sum(row["volume"] for row in own[-20:]) / 20), 2)
    assert aapl["rel_vol"] >= 2
    store = DashboardStore()
    store.publish(snapshot)
    store.publish(snapshot)
    assert len(store.snapshots) == 1


def test_dashboard_accepts_covered_cash_dividends_but_rejects_other_actions():
    inputs = dashboard_fixture()
    base = dict(inputs.actions[0])
    dividend = {
        **base,
        "action_type": "DIVIDEND",
        "effective_date": inputs.session,
    }
    produce_realtime_dashboard(replace(inputs, actions=(*inputs.actions, dividend)))

    other = {**dividend, "action_type": "OTHER"}
    with pytest.raises(ValueError, match="non-split corporate action"):
        produce_realtime_dashboard(replace(inputs, actions=(*inputs.actions, other)))


def test_original_dashboard_requests_read_one_pinned_publication_without_compute(monkeypatch):
    app, store = dashboard_app()
    import realtime.dashboard_projection as producer
    prohibited = MagicMock(side_effect=AssertionError("GET must not produce"))
    monkeypatch.setattr(producer, "compute_default_scanner_snapshots", prohibited)
    before = len(store.statements)
    with TestClient(app) as client:
        params = dict(cohort_id=str(COHORT_ID), refresh=True)
        latest = client.get("/api/latest-price-date", params=params)
        assert latest.status_code == 200, latest.text
        publication = latest.json()["publication"]
        assert publication["status"] == "READY"
        assert publication["generated_at"] == OBSERVED_AT.isoformat()
        params["snapshot_id"] = publication["snapshot_id"]
        for route, key in (("/api/tickers/overview", "overview"), ("/api/scan/all", "scan"), ("/api/stocks/context", "context")):
            response = client.get(route, params=params)
            assert response.status_code == 200, response.text
            value = response.json()
            expected = json.loads(canonical_json(dashboard_snapshot().payload[key]))
            if key == "scan":
                assert value.pop("publication") == publication
            if key == "context":
                assert value["cohort"].pop("snapshot_id") == publication["snapshot_id"]
                value["cohort"].pop("payload_sha256")
            assert value == expected
    assert all(sql.startswith(("SELECT", "SET")) for sql in store.statements[before:])
    prohibited.assert_not_called()


def test_materialization_status_reads_only_the_pinned_daily_publication():
    app, store = dashboard_app(clock=lambda: OBSERVED_AT + timedelta(minutes=1))
    original = next(iter(store.snapshots.values()))
    original["valid_until"] = OBSERVED_AT
    newer = produce_realtime_dashboard(replace(
        dashboard_fixture(), source_generation=4, cutoff=OBSERVED_AT + timedelta(minutes=1),
    ))
    store.publish(newer)
    with TestClient(app) as client:
        current = client.get(
            "/api/equity/materialization-status", params={"cohort_id": str(COHORT_ID)},
        )
        assert current.status_code == 200, current.text
        assert current.json()["status"] == "READY"
        pinned = client.get("/api/equity/materialization-status", params={
            "cohort_id": str(COHORT_ID), "snapshot_id": str(original["snapshot_id"]),
        })
        assert pinned.status_code == 200, pinned.text
        status = pinned.json()
        assert status["status"] == "EXPIRED"
        assert status["pipeline_active"] is False
        assert status["active_intervals"] == []
        assert status["stale_intervals"] == ["1d"]
        assert status["intervals"]["1d"]["is_serveable"] is False
        assert status["last_market_time"] == dashboard_snapshot().payload["market_time"]
        retained = client.get("/api/latest-price-date", params={
            "cohort_id": str(COHORT_ID), "snapshot_id": str(original["snapshot_id"]),
        })
        assert retained.status_code == 200
        assert retained.json()["publication"]["status"] == "EXPIRED"


@pytest.mark.parametrize("days,status", [(1, "READY"), (3, "STALE"), (10, "EXPIRED")])
def test_materialization_status_reports_retained_freshness_without_empty_success(days, status):
    app, _ = dashboard_app(clock=lambda: OBSERVED_AT + timedelta(days=days))
    with TestClient(app) as client:
        response = client.get(
            "/api/equity/materialization-status", params={"cohort_id": str(COHORT_ID)},
        )
        assert response.status_code == 200, response.text
        result = response.json()
        assert result["status"] == status
        assert result["intervals"]["1d"]["status"] == status
        assert result["pipeline_active"] is False
        assert result["active_runs"] == []
        assert result["stale_intervals"] == ([] if status == "READY" else ["1d"])


@pytest.mark.parametrize("params,status", [({}, 422), ({"snapshot_id": str(UUID(int=9))}, 503)])
def test_materialization_status_requires_exact_publication_identity(params, status):
    app, _ = dashboard_app()
    with TestClient(app) as client:
        query = {} if not params else {"cohort_id": str(COHORT_ID), **params}
        assert client.get("/api/equity/materialization-status", params=query).status_code == status


@pytest.mark.parametrize("params,status", [
    ({}, 422), ({"min_swing_pct": 6}, 422), ({"scan_date": "2026-10-01"}, 503),
    ({"snapshot_id": str(UUID(int=9))}, 503), ({"cohort_id": str(UUID(int=8))}, 503),
    ({"scan_date": "invalid"}, 422),
    ({"tickers": "SPY"}, 422),
])
def test_unsupported_requests_are_not_empty_success(params, status):
    app, _ = dashboard_app()
    with TestClient(app) as client:
        query = {} if not params else {"cohort_id": str(COHORT_ID), **params}
        assert client.get("/api/scan/all", params=query).status_code == status


@pytest.mark.parametrize("fault", ["checksum", "manifest", "identity", "generation", "clock", "reference", "action"])
def test_stored_drift_fails_closed(fault):
    app, store = dashboard_app()
    row = next(iter(store.snapshots.values()))
    if fault in {"checksum", "identity", "clock"}:
        row["payload"] = deepcopy(row["payload"])
        if fault == "checksum":
            row["payload"]["overview"][0]["close"] += 1
        elif fault == "identity":
            row["payload"]["cohort_id"] = str(UUID(int=9))
            row["payload_sha256"] = canonical_sha256(row["payload"])
        else:
            row["payload"]["generated_at"] = (OBSERVED_AT + timedelta(minutes=1)).isoformat()
            row["payload_sha256"] = canonical_sha256(row["payload"])
    elif fault == "manifest":
        row["source_manifest_sha256"] = "b" * 64
    elif fault == "generation":
        row["source_generation"] += 1
    elif fault == "reference":
        store.reference_drift = True
    else:
        store.action_drift = True
    with TestClient(app) as client:
        assert client.get("/api/latest-price-date", params={"cohort_id": str(COHORT_ID)}).status_code == 503


@pytest.mark.parametrize("days,status", [(1, "READY"), (3, "STALE"), (10, "EXPIRED")])
def test_provider_delay_staleness_and_expiry_keep_original_projection_clocks(days, status):
    app, _ = dashboard_app(clock=lambda: OBSERVED_AT + timedelta(days=days))
    with TestClient(app) as client:
        response = client.get("/api/latest-price-date", params={"cohort_id": str(COHORT_ID)})
        assert response.status_code == 200
        assert response.json()["publication"]["status"] == status
        assert response.json()["publication"]["generated_at"] == OBSERVED_AT.isoformat()


@pytest.mark.parametrize("fault", ["created", "published", "delay", "scale", "coverage", "membership", "action"])
def test_source_identity_availability_and_action_gates(fault):
    inputs = dashboard_fixture()
    bars = [dict(row) for row in inputs.bars]
    if fault == "created":
        bars[0]["created_at"] = OBSERVED_AT + timedelta(seconds=1)
    elif fault == "published":
        bars[0]["provider_published_at"] = OBSERVED_AT + timedelta(seconds=1)
    elif fault == "delay":
        cutoff = datetime(2026, 10, 2, 20, 10, tzinfo=timezone.utc)
        for row in bars:
            row.update(created_at=row["bar_end"] + timedelta(minutes=1),
                       observed_at=row["bar_end"] + timedelta(minutes=1),
                       replay_available_at=row["bar_end"] + timedelta(minutes=1))
        inputs = replace(inputs, cutoff=cutoff,
                         members=tuple(dict(row, observed_at=cutoff) for row in inputs.members),
                         actions=tuple(dict(row, received_at=cutoff) for row in inputs.actions),
                         coverage=tuple(dict(row, received_at=cutoff) for row in inputs.coverage))
    elif fault == "scale":
        current = next(row for row in bars if row["interval"] == "1d" and row["adjustment_basis"] == "ADJUSTED"
                       and row["session_date"] == inputs.session)
        current["close"] *= 1.01
        current["high"] = max(current["high"], current["close"])
    elif fault == "coverage":
        inputs = replace(inputs, coverage=())
    elif fault == "membership":
        inputs = replace(inputs, members=inputs.members[:-1])
    else:
        inputs = replace(inputs, actions=(*inputs.actions, dict(
            inputs.actions[0], action_type="MERGER", effective_date=date(2026, 9, 1),
        )))
    match = "availability is beyond cutoff" if fault == "delay" else None
    with pytest.raises(ValueError, match=match):
        produce_realtime_dashboard(replace(inputs, bars=tuple(bars)))


def test_derived_effective_availability_propagates_later_source_clock_before_cutoff():
    inputs = dashboard_fixture()
    bars = [dict(row) for row in inputs.bars]
    derived = next(row for row in bars if row["source_kind"] == "DERIVED")
    source = next(row for row in bars if str(row["bar_revision_id"]) == str(derived["source_revision_ids"][0]))
    source["created_at"] = inputs.cutoff

    snapshot = produce_realtime_dashboard(replace(inputs, bars=tuple(bars)))

    assert snapshot.generated_at == inputs.cutoff


def test_current_scale_gate_accepts_bounded_provider_close_correction():
    inputs = dashboard_fixture()
    bars = [dict(row) for row in inputs.bars]
    current = next(
        row for row in bars
        if row["interval"] == "1d"
        and row["adjustment_basis"] == "ADJUSTED"
        and row["session_date"] == inputs.session
    )
    current["close"] *= 1.0005
    current["high"] = max(current["high"], current["close"])

    result = produce_realtime_dashboard(replace(inputs, bars=bars))

    assert SCREENING_BASIS["current_scale_max_relative_difference"] == 0.001
    assert result.payload["overview"]


def test_publication_pin_survives_current_pointer_rotation():
    app, store = dashboard_app()
    original = dashboard_snapshot()
    newer = produce_realtime_dashboard(replace(dashboard_fixture(), source_generation=4,
                                              cutoff=OBSERVED_AT + timedelta(minutes=1)))
    store.publish(newer)
    with TestClient(app) as client:
        params = {"cohort_id": str(COHORT_ID), "snapshot_id": str(original.snapshot_id)}
        response = client.get("/api/scan/all", params=params)
        assert response.status_code == 200, response.text
        assert response.json()["publication"]["snapshot_id"] == str(original.snapshot_id)
        assert response.json()["scan_datetime"] == original.generated_at.isoformat()


def test_missing_index_inputs_do_not_invent_a_market_regime():
    inputs = dashboard_fixture()
    members = tuple(row for row in inputs.members if row["ticker"] != "QQQ")
    missing = produce_realtime_dashboard(replace(inputs, members=members,
                                               bars=tuple(row for row in inputs.bars if row["ticker"] != "QQQ")))
    assert missing.payload["scan"]["market_regime"] is None
    assert missing.payload["missing_producers"]["market_regime"] == "SPY_QQQ_EXACT_COHORT_INPUTS_UNAVAILABLE"


def test_dashboard_cli_plan_performs_zero_io_and_requires_target_confirmation(monkeypatch, capsys):
    from scripts import project_realtime_screening as cli
    reject = MagicMock(side_effect=AssertionError("No DB/environment access in planning"))
    monkeypatch.setattr(cli, "load_dotenv", reject)
    monkeypatch.setattr(cli.pool, "SimpleConnectionPool", reject)
    args = ["--surface", "dashboard", "--cohort-id", str(COHORT_ID), "--session", "2026-10-02",
            "--cutoff", OBSERVED_AT.isoformat(), "--source-generation", "3"]
    assert cli.main(args) == 0
    assert "NOT_EXECUTED" in capsys.readouterr().out
    reject.assert_not_called()
    with pytest.raises(SystemExit):
        cli.parse_args([*args, "--execute"])
