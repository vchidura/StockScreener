"""Behavioral tests for roadmap detectors, point-in-time replay and price labels."""
from copy import deepcopy
from datetime import timedelta
import json

import numpy as np
import pandas as pd
import pytest

from research.equity_models.catalog import CATALOG, DEFAULT_MODELS
from research.equity_models.data import ResearchData, Snapshot, Unavailable, indicators, stamp, wilder
from research.equity_models.detectors import evaluate_research, ofi_windows
from research.equity_detection_backtest import run_detection_backtest, write_run, measure


def empty_payload():
    return dict(schema="equity_research_dataset_v1", availability_mode="OBSERVED",
                evaluation_start="2026-09-30T13:30:00Z", evaluation_end="2026-09-30T20:00:01Z",
                as_of="2026-11-30T22:00:00Z", evaluation_securities=["A"], bars=[], facts=[], quotes=[])


def bar(data, session, price=100., security="A", interval="1d", offset=0, volume=1e6):
    opening, closing = data.session_bounds(session)
    if interval != "1d":
        seconds = {"5m": 300, "30m": 1800, "1h": 3600}[interval]
        opening += timedelta(seconds=seconds * offset)
        closing = opening + timedelta(seconds=seconds)
    key = f"{security}:{interval}:{opening.isoformat()}"
    return dict(security_id=security, source_id=key, revision="1", start=opening.isoformat(),
                market_at=closing.isoformat(), available_at=closing.isoformat(), received_at=closing.isoformat(),
                interval=interval, completed=True, adjustment_id="split-basis-v1", segment_id="continuous-v1",
                open=price - .1, high=price + .5, low=price - .5, close=price, volume=volume)


def fact(kind, values, at="2026-09-30T12:00:00Z", until="2027-01-01T00:00:00Z"):
    return dict(security_id="A", source_id=kind, revision="1", kind=kind, values=values,
                market_at=at, available_at=at, received_at=at, valid_until=until,
                methodology="test-fixture-v1", input_ids=["test-source:1"])


def common_facts():
    return [fact("eligibility", dict(member=True, actions_covered=True, price_basis="SPLIT_ADJUSTED")),
            fact("events", dict(macro_covered=True, macro_blocked=False,
                                earnings_covered_through="2027-01-01", earnings_sessions=[]))]


def position_payload():
    payload = empty_payload()
    data = ResearchData(payload)
    sessions = data.calendar.sessions_window("2026-09-30", -299)
    payload["bars"] = [bar(data, str(s.date()), 50 + i * .2) for i, s in enumerate(sessions)]
    payload["facts"] = common_facts() + [
        fact("fundamentals", dict(common_stock=True, sector="TECHNOLOGY", assets=1e9,
                                  operating_cash_flow=1e8, capital_expenditure=1e7, enterprise_value=2e9,
                                  interest_coverage=5, operating_margin=.3, prior_operating_margin=.2)),
        fact("fundamental_ranks", dict(cohort_id="dated-all-members", coverage_complete=True,
                                       total_return_12_minus_1_percentile=.9, sector_quality_percentile=.9,
                                       sector_value_percentile=.9))]
    return payload


def test_position_end_to_end_and_idempotent_artifacts(tmp_path):
    payload = position_payload()
    result = run_detection_backtest(payload, models=["P1", "P2"])
    assert [r["status"] for r in result["evaluations"]] == ["MATCH", "MATCH"]
    assert all(r["signal"]["stop"] is None for r in result["evaluations"])
    assert all(r["status"] in {"PENDING", "UNAVAILABLE"} for r in result["outcomes"])
    write_run(result, tmp_path)
    write_run(result, tmp_path)
    changed = deepcopy(result)
    changed["summary"]["P1"]["evaluations"] += 1
    with pytest.raises(ValueError, match="refusing to overwrite"):
        write_run(changed, tmp_path)
    assert json.loads((tmp_path / "manifest.json").read_text())["execution_permission"] is False


@pytest.mark.parametrize("model", ["P1", "P2"])
def test_position_missing_fundamentals_and_future_rank_are_unavailable(model):
    payload = position_payload()
    payload["facts"] = [f for f in payload["facts"] if f["kind"] != "fundamentals"]
    result = run_detection_backtest(payload, models=[model])
    assert result["evaluations"][0]["status"] == "UNAVAILABLE"
    payload = position_payload()
    for f in payload["facts"]:
        if f["kind"] == "fundamental_ranks":
            f["available_at"] = f["received_at"] = "2026-10-01T12:00:00Z"
    result = run_detection_backtest(payload, models=[model])
    assert result["evaluations"][0]["status"] == "UNAVAILABLE"


def test_later_revision_cannot_rewrite_earlier_detection():
    payload = position_payload()
    first = run_detection_backtest(payload, models=["P1"])
    correction = deepcopy(payload["bars"][-1])
    correction.update(revision="2", open=1, high=1, low=1, close=1,
                      available_at="2026-10-01T12:00:00Z", received_at="2026-10-01T12:00:00Z")
    payload["bars"].append(correction)
    second = run_detection_backtest(payload, models=["P1"])
    assert first["evaluations"] == second["evaluations"]
    assert first["manifest"]["dataset_sha256"] != second["manifest"]["dataset_sha256"]


def test_exchange_horizon_missing_pending_and_adjustment_mismatch():
    payload = position_payload()
    data = ResearchData(payload)
    result = run_detection_backtest(payload, models=["P1"])
    endpoint = data.session_offset("2026-09-30", 5)
    payload["bars"].append(bar(data, endpoint, 120))
    result = run_detection_backtest(payload, models=["P1"])
    five = next(r for r in result["outcomes"] if r["horizon"] == 5)
    assert five["status"] == "AVAILABLE"
    assert five["value"] == pytest.approx(120 / result["evaluations"][0]["signal"]["reference"] - 1)
    payload["bars"][-1]["adjustment_id"] = "post-split-different-basis"
    result = run_detection_backtest(payload, models=["P1"])
    assert next(r for r in result["outcomes"] if r["horizon"] == 5)["reason"] == "ADJUSTMENT_COMPARABILITY_UNPROVEN"


def test_all_models_explicitly_report_missing_decision_inputs():
    result = run_detection_backtest(empty_payload())
    assert set(result["summary"]) == set(DEFAULT_MODELS)
    assert len(result["evaluations"]) == len(DEFAULT_MODELS)
    assert all(r["reasons"] == ["NO_DECISION_INPUTS"] for r in result["evaluations"])


def test_wilder_arithmetic_seed_and_no_future_dependency():
    series = pd.Series(range(1, 31), dtype=float)
    smooth = wilder(series)
    assert smooth.iloc[:13].isna().all()
    assert smooth.iloc[13] == pytest.approx(7.5)
    assert smooth.iloc[14] == pytest.approx((13 * 7.5 + 15) / 14)
    frame = pd.DataFrame(dict(open=series + 50, high=series + 51, low=series + 49,
                              close=series + 50, volume=100))
    prefix = indicators(frame.iloc[:25])
    full = indicators(frame)
    pd.testing.assert_frame_equal(prefix, full.iloc[:25])


def test_missing_bar_resets_history_and_half_day_full_hours_only():
    payload = empty_payload()
    data = ResearchData(payload)
    payload["bars"] = [bar(data, "2026-09-30", interval="1h", offset=i) for i in (0, 1, 3, 4)]
    history = ResearchData(payload).history("A", "1h", stamp("2026-09-30T20:00:00Z"))
    assert len(history) == 2
    payload["bars"] = [bar(data, "2026-11-27", interval="1h", offset=3)]
    with pytest.raises(ValueError, match="full RTH"):
        ResearchData(payload)


@pytest.mark.parametrize("mutation", ["naive", "future_market", "nan", "forming", "missing_adjustment"])
def test_bad_inputs_fail_closed(mutation):
    payload = empty_payload()
    row = bar(ResearchData(payload), "2026-09-30")
    if mutation == "naive":
        row["received_at"] = "2026-09-30T20:00:00"
    elif mutation == "future_market":
        row["market_at"] = "2026-10-01T20:00:00Z"
    elif mutation == "nan":
        row["close"] = float("nan")
    elif mutation == "forming":
        row["completed"] = False
    else:
        row.pop("adjustment_id")
    payload["bars"] = [row]
    with pytest.raises(ValueError):
        ResearchData(payload)


class ModelSnapshot:
    """Explicit feature fixtures isolate structural state machines from indicator tests."""
    def __init__(self, interval="1h", session="2026-09-30"):
        self.data = ResearchData(empty_payload())
        self.security, self.interval, self.session = "A", interval, session
        self.at = self.data.session_bounds(session)[0] + timedelta(hours=2)
        self.lineage = set()
        n = 300
        times = pd.date_range(end=self.at, periods=n, freq={"1h": "h", "30m": "30min", "5m": "5min", "1d": "D"}.get(interval, "h"))
        self.frame = pd.DataFrame(dict(market_at=times, session=session, start=times - timedelta(hours=1),
            open=100., high=101., low=99., close=100., volume=2e6, atr_prior=1., ema20=99., ema50=98.,
            ema200=95., ema50_prior10=97., liquidity20=60e6, liquidity60=60e6, compression=.5,
            adx=30., plus_di=30., minus_di=10., adx_change3=2., slot_rvol=2., cumulative_rvol=2.,
            volume_median20=1e6, return63=.1, return20=.05, sma200=98., sma200_prior20=97., sma100=98.,
            session_vwap=99., source_id=[f"bar:{i}" for i in range(n)], revision="1", segment_id="a", adjustment_id="a"))
        self.daily = self.frame.copy()
        self.daily["session"] = self.data.session_offset(session, -1)
        self.values = {f["kind"]: f["values"] for f in common_facts()}
        self.values.update(relative_strength=dict(rs63=.1, percentile63=.9, sector_rs20=.1, sector_rs63=.1),
                           benchmarks=dict(market_security_id="SPY", sector_security_id="XLK"),
                           reversal_context=dict(aligned_breakout=False, material_repricing_event=False))

    def history(self, interval=None, security=None, prior_day=False):
        if interval == "1d":
            return self.daily
        return self.frame

    def facts(self, kind):
        if kind not in self.values:
            raise Unavailable("MISSING_" + kind.upper())
        return self.values[kind]

    def require(self, kind, *keys):
        values = self.facts(kind)
        if any(k not in values for k in keys):
            raise Unavailable("INCOMPLETE_" + kind.upper())
        return [values[k] for k in keys]

    def next(self, **values):
        result = deepcopy(self)
        minutes = {"1h": 60, "30m": 30, "5m": 5}[self.interval]
        result.at += timedelta(minutes=minutes)
        row = result.frame.iloc[-1].copy()
        row["market_at"], row["start"] = result.at, result.at - timedelta(minutes=minutes)
        row["source_id"] = result.at.isoformat()
        for key, value in values.items():
            row[key] = value
        result.frame = pd.concat([result.frame, row.to_frame().T], ignore_index=True)
        return result


@pytest.mark.parametrize("model", ["SW3_B", "B3_1h_structure", "B3_1h_combined", "B3_1h_retest"])
def test_breakout_setup_later_confirmation_and_frozen_target(model):
    snap = ModelSnapshot()
    snap.frame.loc[299, ["open", "high", "low", "close"]] = [100.8, 101.5, 100.8, 101.2]
    if "retest" in model:
        snap.frame.loc[299, "high"] = 101.3
    first = evaluate_research(snap, model)
    assert first["status"] == "NO_MATCH" and first["reasons"] == ["SETUP_DEVELOPING"]
    pending = first["state"]["pending"]
    next_snap = snap.next(open=101.2, high=101.7, low=100.95, close=101.6)
    if "combined" in model:
        next_snap.frame.loc[300, "close"] = 101.5
        next_snap.frame.loc[300, "high"] = 101.55
    if "retest" in model:
        next_snap.frame.loc[300, ["close", "high"]] = [101.45, 101.5]
    second = evaluate_research(next_snap, model, first["state"])
    assert second["status"] == "MATCH", second
    assert second["signal"]["target"] == pending["target"] == 103
    retry = evaluate_research(next_snap, model, second["state"])
    assert retry["reasons"] == ["ALREADY_EVALUATED"]


def test_sw3_cannot_confirm_next_session_or_wrong_interval():
    snap = ModelSnapshot()
    snap.frame.loc[299, ["high", "close"]] = [101.5, 101.2]
    first = evaluate_research(snap, "SW3_B")
    tomorrow = snap.next(close=102., high=102.2)
    tomorrow.session = "2026-10-01"
    tomorrow.at = tomorrow.data.session_bounds(tomorrow.session)[0] + timedelta(hours=1)
    tomorrow.frame.loc[300, "session"] = tomorrow.session
    tomorrow.frame.loc[300, "market_at"] = tomorrow.at
    result = evaluate_research(tomorrow, "SW3_B", first["state"])
    assert result["status"] != "MATCH"
    wrong = ModelSnapshot("30m")
    assert evaluate_research(wrong, "SW3_B")["reasons"] == ["POLICY_INTERVAL_MISMATCH"]


@pytest.mark.parametrize("model", ["SW3_R", "R2_1h_combined"])
def test_resumption_requires_two_later_bars(model):
    snap = ModelSnapshot()
    snap.frame.loc[296, "high"] = 104.
    snap.frame.loc[297:299, "low"] = 102.
    snap.frame.loc[299, ["open", "high", "low", "close"]] = [102.2, 103., 102., 102.5]
    first = evaluate_research(snap, model)
    assert first["reasons"] == ["SETUP_DEVELOPING"], first
    middle = snap.next(open=102.5, high=103.1, low=102.3, close=103.)
    second = evaluate_research(middle, model, first["state"])
    assert second["status"] == "NO_MATCH"
    last = middle.next(open=103., high=103.5, low=102.9, close=103.4)
    third = evaluate_research(last, model, second["state"])
    assert third["status"] == "MATCH", third
    assert third["signal"]["target"] == 104.


@pytest.mark.parametrize("model", ["SW3_F", "F2_1h_combined"])
def test_reversal_extra_confirmation_and_frozen_midpoint(model):
    snap = ModelSnapshot()
    snap.daily["adx"] = 15.
    snap.frame.loc[299, ["open", "high", "low", "close"]] = [99., 99.2, 98.2, 98.8]
    first = evaluate_research(snap, model)
    assert first["reasons"] == ["SETUP_DEVELOPING"]
    middle = snap.next(open=98.9, high=99.3, low=98.8, close=99.25)
    second = evaluate_research(middle, model, first["state"])
    if model == "SW3_F":
        assert second["status"] == "MATCH", second
    else:
        assert second["status"] == "NO_MATCH"
        third = evaluate_research(middle.next(open=99.25, high=99.7, low=99.1, close=99.65), model, second["state"])
        assert third["status"] == "MATCH", third
        assert third["signal"]["target"] == 100.


def test_missing_data_holds_state_and_confirmation_gap_consumes_it():
    snap = ModelSnapshot()
    snap.frame.loc[299, ["high", "close"]] = [101.5, 101.2]
    first = evaluate_research(snap, "SW3_B")
    missing = snap.next(close=101.6, high=101.7)
    del missing.values["eligibility"]
    result = evaluate_research(missing, "SW3_B", first["state"])
    assert result["status"] == "UNAVAILABLE" and result["state"] == first["state"]
    later = snap.next().next(close=101.6, high=101.7)
    result = evaluate_research(later, "SW3_B", first["state"])
    assert result["reasons"] == ["SETUP_CONFIRMATION_GAP_INVALIDATED"]
    assert "pending" not in result["state"]


def test_swing_breakout_retest_daily_geometry():
    snap = ModelSnapshot()
    snap.daily.loc[299, ["open", "high", "low", "close", "volume"]] = [101., 102., 100.8, 101.5, 2e6]
    snap.frame.loc[298, "high"] = 101.1
    snap.frame.loc[299, ["open", "high", "low", "close"]] = [101., 101.6, 101.1, 101.4]
    result = evaluate_research(snap, "SW1")
    assert result["status"] == "MATCH", result
    assert result["signal"]["stop"] == pytest.approx(101.)
    assert result["signal"]["target"] == pytest.approx(102.2)


def test_earnings_requires_original_consensus_and_next_session():
    snap = ModelSnapshot()
    snap.values["earnings_reaction"] = dict(announcement_at="2026-09-28T21:00:00Z",
        consensus_available_at="2026-09-28T19:00:00Z", actual_eps=1.2, consensus_eps=1.,
        reaction_session="2026-09-29", pre_announcement_close=100., eps_basis_compatible=True)
    snap.daily.loc[299, ["open", "high", "low", "close", "volume"]] = [100.5, 102., 100., 101.8, 3e6]
    snap.frame.loc[299, ["open", "high", "low", "close"]] = [101.9, 102.3, 101.8, 102.2]
    result = evaluate_research(snap, "SW2")
    assert result["status"] == "MATCH", result
    snap.values["earnings_reaction"]["consensus_available_at"] = "2026-09-29T12:00:00Z"
    assert evaluate_research(snap, "SW2")["status"] == "UNAVAILABLE"


def quote(security, at, seq, bid=100., ask=100.01, bid_size=100., ask_size=100.):
    return dict(security_id=security, source_id=f"quote:{seq}", revision="1", market_at=at.isoformat(),
                available_at=at.isoformat(), received_at=at.isoformat(), bid=bid, ask=ask,
                bid_size=bid_size, ask_size=ask_size, tick_size=.01, sequence=seq, venue="X",
                adjustment_id="a")


def test_real_ofi_windows_exclude_future_events_and_reject_sequence_gaps():
    payload = empty_payload()
    at = stamp("2026-09-30T15:00:00Z")
    payload["facts"] = [fact("quote_coverage", dict(continuous_from=(at - timedelta(seconds=400)).isoformat(), venue="X"))]
    rows = []
    for i in range(307):
        t = at - timedelta(seconds=306 - i, milliseconds=50)
        rows.append(quote("A", t, i, bid_size=100 + (i % 3) * 10))
    payload["quotes"] = rows
    snapshot = Snapshot(ResearchData(payload), "A", at, "2026-09-30", "event")
    z, _ = ofi_windows(snapshot)
    assert z.tail(5).notna().all()
    payload["quotes"].append(quote("A", at + timedelta(seconds=1), 307, bid_size=1e9))
    again, _ = ofi_windows(Snapshot(ResearchData(payload), "A", at, "2026-09-30", "event"))
    pd.testing.assert_series_equal(z, again)
    payload["quotes"].pop(100)
    with pytest.raises(Unavailable, match="SEQUENCE_GAP"):
        ofi_windows(Snapshot(ResearchData(payload), "A", at, "2026-09-30", "event"))


def intraday_snapshot(model):
    snap = ModelSnapshot("5m")
    opening, _ = snap.data.session_bounds(snap.session)
    count = 6 if model == "D1" else 8
    snap.at = opening + timedelta(minutes=count * 5)
    snap.frame["session"] = "2026-09-29"
    for i in range(count):
        idx = 300 - count + i
        snap.frame.loc[idx, ["session", "start", "market_at"]] = [
            snap.session, opening + timedelta(minutes=i * 5), opening + timedelta(minutes=(i + 1) * 5)]
        snap.frame.loc[idx, "session_vwap"] = 98.9 + i * .03
    snap.daily["atr_prior"] = 2.
    if model == "D1":
        snap.frame.loc[300-count:300-count+2, ["high", "low"]] = [101., 100.]
        snap.frame.loc[299, ["open", "high", "low", "close"]] = [101., 101.4, 101., 101.2]
    else:
        snap.frame.loc[296:298, ["high", "low", "close"]] = [99.7, 99.4, 99.5]
        snap.frame.loc[299, ["open", "high", "low", "close"]] = [99.3, 99.3, 99.05, 99.15]
    raw = quote("A", snap.at, 1)
    snap.data.quotes = tuple(ResearchData(dict(empty_payload(), quotes=[raw])).quotes)
    return snap


@pytest.mark.parametrize("model", ["D1", "D2"])
def test_intraday_requires_later_retest_and_preserves_stop(model):
    snap = intraday_snapshot(model)
    first = evaluate_research(snap, model)
    assert first["reasons"] == ["SETUP_DEVELOPING"], first
    if model == "D1":
        later = snap.next(open=101.1, high=101.4, low=100.98, close=101.3, session_vwap=99.1)
    else:
        later = snap.next(open=99.2, high=99.45, low=99.1, close=99.4, session_vwap=99.12)
    raw = quote("A", later.at, 2)
    later.data.quotes = tuple(ResearchData(dict(empty_payload(), quotes=[raw])).quotes)
    result = evaluate_research(later, model, first["state"])
    assert result["status"] == "MATCH", result
    assert result["signal"]["stop"] < result["signal"]["reference"] < result["signal"]["target"]
    if model == "D1":
        after = later.next()
        after.data.quotes = tuple(ResearchData(dict(empty_payload(), quotes=[quote("A", after.at, 3)])).quotes)
        assert evaluate_research(after, model, result["state"])["reasons"] == ["SESSION_EPISODE_ALREADY_CONFIRMED"]


def test_d2_half_day_entry_cutoff_and_no_native_vwap_proxy():
    snap = intraday_snapshot("D2")
    snap.frame.loc[299, "session_vwap"] = np.nan
    assert evaluate_research(snap, "D2")["status"] == "UNAVAILABLE"
    snap = intraday_snapshot("D2")
    snap.session = "2026-11-27"
    snap.at = stamp("2026-11-27T17:00:00Z")  # noon ET, too late for a 90-minute diagnostic plan
    snap.frame.loc[299, ["market_at", "session"]] = [snap.at, snap.session]
    snap.data.quotes = tuple(ResearchData(dict(empty_payload(), quotes=[quote("A", snap.at, 1)])).quotes)
    assert evaluate_research(snap, "D2")["reasons"] == ["ENTRY_WINDOW"]


@pytest.mark.parametrize("model", ["C1", "C2"])
def test_scalping_conditions_are_detections_never_fills(monkeypatch, model):
    import research.equity_models.detectors as module
    snap = ModelSnapshot("event")
    snap.values["quote_coverage"] = dict(venue="X", continuous_from="2026-09-30T13:00:00Z")
    snap.values["microstructure_readiness"] = dict(trades_covered=True, fees_qualified=True,
                                                 acknowledgements_qualified=True, halted=False, material_news=False)
    snap.values["microstructure_model"] = dict(forecast_move=.05, roundtrip_cost=.01,
                                              trained_until="2026-09-01T00:00:00Z", loss_budget=.02)
    snap.values["passive_execution"] = dict(venue_depth_qualified=True, execution_reports_qualified=True,
        cancel_latency_qualified=True, inventory_units=0, side="BUY", expected_capture=.015,
        cost_and_adverse_selection=.005, trained_until="2026-09-01T00:00:00Z")
    raw = quote("A", snap.at, 1, ask=100.03 if model == "C2" else 100.01)
    snap.data.quotes = tuple(ResearchData(dict(empty_payload(), quotes=[raw])).quotes)
    monkeypatch.setattr(module, "ofi_windows", lambda _: (pd.Series([3.] * 5 if model == "C1" else [0.] * 5), pd.Series([100.] * 6)))
    result = evaluate_research(snap, model)
    assert result["status"] == "MATCH", result
    assert result["execution_permission"] is False and result["publication_permission"] is False
    assert result["signal"]["reference"] == pytest.approx((raw["bid"] + raw["ask"]) / 2)
    if model == "C2":
        snap.values["passive_execution"]["venue_depth_qualified"] = False
        assert evaluate_research(snap, model)["status"] == "UNAVAILABLE"
    else:
        snap.values["microstructure_model"]["trained_until"] = snap.at.isoformat()
        assert evaluate_research(snap, model)["status"] == "UNAVAILABLE"


def test_session_vwap_and_slot_volume_use_only_prior_comparable_sessions():
    payload = empty_payload()
    data = ResearchData(payload)
    sessions = data.calendar.sessions_window("2026-09-30", -17)
    rows = []
    for session in sessions:
        opening, closing = data.session_bounds(str(session.date()))
        for i in range(int((closing - opening).total_seconds() / 300)):
            item = bar(data, str(session.date()), interval="5m", offset=i, volume=100)
            item["vwap"] = 100.
            rows.append(item)
    rows[-1]["volume"] = 200
    payload["bars"] = rows
    frame = ResearchData(payload).history("A", "5m", stamp("2026-09-30T21:00:00Z"))
    assert frame.slot_rvol.iloc[-1] == pytest.approx(2)
    assert frame.session_vwap.iloc[-1] == pytest.approx(100)
    assert frame.iloc[:78 * 15].slot_rvol.isna().all()
    payload["bars"][-1].pop("vwap")
    frame = ResearchData(payload).history("A", "5m", stamp("2026-09-30T21:00:00Z"))
    assert pd.isna(frame.session_vwap.iloc[-1])


def test_fundamental_rank_builder_no_price_proxy_or_missing_report_imputation():
    from research.equity_models.fundamentals import build_rank_facts
    payload = position_payload()
    for item in payload["bars"]:
        item.update(total_return_close=item["close"], total_return_basis_id="synthetic-dividend-inclusive-v1")
    for item in payload["facts"]:
        if item["kind"] == "fundamentals":
            item["values"].update(gross_profit=3e8, net_debt=1e7, equity_market_value=2e9, ebit=1e8)
    data = ResearchData(payload)
    ranks = build_rank_facts(data, ["A"], stamp("2026-09-30T20:00:00Z"))
    assert len(ranks) == 1
    assert ranks[0]["values"]["sector_quality_percentile"] == 1.
    assert ranks[0]["values"]["coverage_complete"] is True
    payload["bars"][-253].pop("total_return_close")
    with pytest.raises(ValueError, match="nonfinite"):
        build_rank_facts(ResearchData(payload), ["A"], stamp("2026-09-30T20:00:00Z"))
    with pytest.raises(Unavailable):
        build_rank_facts(data, ["A", "MISSING_MEMBER"], stamp("2026-09-30T20:00:00Z"))
