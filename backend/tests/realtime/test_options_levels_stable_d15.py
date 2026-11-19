"""G106 D15 stable walls (Nov 19): `REALTIME_V3_OPTIONS_LEVELS_STABLE` OFF (default) = the levels recorder and the
scheduler exactly as r140; RECORD (with the levels switch RECORD or ON) = beside each `OPTIONS_LEVELS_RECORD` line one
`OPTIONS_LEVELS_STABLE_RECORD` line with the v1 walls and the stable walls (call wall above spot, put wall below, never
the same strike, a held wall kept unless beaten by the margin or spot moved through it beyond the band), and one
`OPTIONS_LEVELS_STABLE_WINDOW` line; no extra SQL; a failure is that name's stable line only."""
from __future__ import annotations

import importlib
import json
from datetime import date, timedelta

import pytest

from realtime import advanced_options_levels as levels
from realtime import advanced_options_levels_stable as stable
from realtime.options_exposure_read import build_exposure_chain, compute_exposure
from test_advanced_options_levels import (
    CUTOFF,
    OBSERVED,
    SESSION,
    Cursor,
    Lines,
    Provider,
    _attached_cycle,
    _service,
    chain_rows,
    recorder,
    window_script,
)

SWITCH = stable.STABLE_SETTING


def profile(spot, rows):
    """A hand-made per-strike profile: rows of (strike, call gamma, put gamma)."""
    return stable.StrikeGamma(spot=float(spot), strikes=tuple(float(strike) for strike, _c, _p in rows),
                              texts=tuple(str(strike) for strike, _c, _p in rows),
                              call=tuple(float(call) for _s, call, _p in rows),
                              put=tuple(float(put) for _s, _c, put in rows))


# ---- the switch ---------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [(None, "OFF"), ("", "OFF"), (" ", "OFF"), ("off", "OFF"),
                                           ("record", "RECORD"), (" RECORD ", "RECORD")])
def test_mode_values(raw, expected):
    environ = {} if raw is None else {SWITCH: raw}
    assert stable.levels_stable_mode(environ).value == expected


@pytest.mark.parametrize("raw", ["ON", "SHADOW", "1", "true", "RECORD_ONLY"])
def test_an_invalid_value_raises_naming_the_key(raw):
    with pytest.raises(ValueError, match=SWITCH):
        stable.levels_stable_mode({SWITCH: raw})
    with pytest.raises(ValueError, match=SWITCH):
        stable.start_levels_stable(raw, levels_mode="RECORD")
    with pytest.raises(ValueError, match=SWITCH):
        stable.parse_levels_stable_mode(1)


def test_the_options_scheduler_refuses_to_start_on_an_invalid_value(monkeypatch):
    from realtime.migrations import EXPECTED_V2_DATABASE_NAME

    script = importlib.import_module("scripts.run_advanced_options_v3_scheduler")
    argv = ["--database", EXPECTED_V2_DATABASE_NAME, "--confirm-database-name", EXPECTED_V2_DATABASE_NAME,
            "--continuous", "--execute"]
    monkeypatch.setenv(SWITCH, "ON")
    with pytest.raises(ValueError, match=SWITCH):
        script.parse_args(argv)
    for good in ("", "OFF", "RECORD", "record"):
        monkeypatch.setenv(SWITCH, good)
        assert script.parse_args(argv).continuous


def test_start_off_is_none_and_silent_and_record_needs_the_levels_recorder():
    lines = Lines()
    assert stable.start_levels_stable("OFF", levels_mode="RECORD", emit=lines.emit) is None
    assert lines.lines == []
    assert stable.start_levels_stable("RECORD", levels_mode="OFF", emit=lines.emit) is None
    assert lines.lines == [(stable.STABLE_SETTING_EVENT, {
        "active": False, "applied": False, "band": stable.BAND, "margin": stable.MARGIN, "mode": "RECORD",
        "reason": "LEVELS_OFF", "setting": SWITCH, "version": stable.VERSION})]
    tracker = stable.start_levels_stable("record", levels_mode="ON", emit=lines.emit)
    assert isinstance(tracker, stable.StableLevels)
    assert lines.lines[-1][1]["active"] is True and "reason" not in lines.lines[-1][1]


# ---- the per-strike numbers equal the page's ----------------------------------------------------------------------------

@pytest.mark.parametrize("call_oi, put_oi", [
    (None, None),
    ({97: 30000}, None),                       # the call wall below spot (in the money): v1 takes it
    (None, {104: 40000}),                      # the put wall above spot
    ({97: 30000}, {97: 30000}),                # call wall = put wall below spot (the SPY 730/730 case)
])
def test_strike_gamma_reproduces_the_pages_walls(call_oi, put_oi):
    rows = chain_rows("SPY", call_oi=call_oi, put_oi=put_oi)
    chain = build_exposure_chain(rows, underlyer="SPY", session_date=SESSION, observed_at=OBSERVED)
    gamma = compute_exposure(chain, greek="gamma", weight="oi", scope="all")
    found = stable.strike_gamma(chain)
    assert stable.unconstrained_wall(found, "call") == gamma["levels"]["call_wall"]["strike"]
    assert stable.unconstrained_wall(found, "put") == gamma["levels"]["put_wall"]["strike"]
    by_strike = {item["strike"]: item for item in gamma["strikes"]}
    for text, call, put in zip(found.texts, found.call, found.put):
        assert round(call) == pytest.approx(abs(by_strike[text]["call"]), abs=1)
        assert round(put) == pytest.approx(abs(by_strike[text]["put"]), abs=1)
    # Without today's expiry: the 7-day contracts only (the page's 7d scope minus 0 DTE).
    week = compute_exposure(chain, greek="gamma", weight="oi", scope="7d")
    today = compute_exposure(chain, greek="gamma", weight="oi", scope="0dte")
    today_by_strike = {item["strike"]: item for item in today["strikes"]}
    week_by_strike = {item["strike"]: item for item in week["strikes"]}
    for text, call, put in zip(found.texts, found.call_ex0dte, found.put_ex0dte):
        expected_call = abs(week_by_strike[text]["call"]) - abs(today_by_strike.get(text, {}).get("call", 0))
        expected_put = abs(week_by_strike[text]["put"]) - abs(today_by_strike.get(text, {}).get("put", 0))
        assert call == pytest.approx(expected_call, abs=2) and put == pytest.approx(expected_put, abs=2)


# ---- the rule -------------------------------------------------------------------------------------------------------------

def test_a_new_wall_is_strictly_on_its_side_and_the_walls_never_coincide():
    # The largest call and put gamma both sit at 98, below spot 100 (v1: call wall = put wall = 98).
    found = profile(100, [(96, 1, 40), (98, 90, 95), (100, 50, 50), (102, 60, 5), (104, 70, 2)])
    assert stable.unconstrained_wall(found, "call") == stable.unconstrained_wall(found, "put") == "98"
    walls = stable.choose_walls(found, None, None)
    assert walls == {"call": ("104", "FRESH"), "put": ("98", "FRESH")}  # 100 = spot is on neither side
    assert stable.best_on_side(found, "put", exclude="98") == "96"


def test_a_held_wall_survives_a_challenger_within_the_margin_and_yields_beyond_it():
    held = "103"
    within = profile(100, [(103, 100, 0), (104, 120, 0)])     # +20% < 25%
    beyond = profile(100, [(103, 100, 0), (104, 126, 0)])     # +26% > 25%
    assert stable.choose_wall(within, "call", held) == ("103", "HELD")
    assert stable.choose_wall(beyond, "call", held) == ("104", "SWITCHED")
    assert stable.choose_wall(within, "call", "104") == ("104", "BEST")
    assert stable.choose_wall(within, "call", held, margin=0.1) == ("104", "SWITCHED")


def test_spot_moving_through_a_held_wall_moves_it_only_beyond_the_band():
    rows = [(99, 10, 80), (100, 90, 10), (101, 40, 5), (102, 30, 1)]
    # Spot 100.3: the held call wall 100 is 0.3% below spot (inside the 0.5% band): kept.
    assert stable.choose_wall(profile(100.3, rows), "call", "100") == ("100", "HELD")
    # Spot 100.6: 0.6% through it: replaced by the best strike above spot.
    assert stable.choose_wall(profile(100.6, rows), "call", "100") == ("101", "WRONG_SIDE")
    assert stable.choose_wall(profile(100.3, rows), "call", "100", band=0.0) == ("101", "WRONG_SIDE")
    # Put side mirrored: held put wall 99 with spot 98.7 (0.3% below it) is kept.
    assert stable.choose_wall(profile(98.7, rows), "put", "99") == ("99", "BEST")
    assert stable.choose_wall(profile(98.4, rows), "put", "99") == (None, "NONE_ON_SIDE")


def test_a_held_wall_that_lost_its_gamma_or_was_taken_is_replaced():
    found = profile(100, [(97, 0, 50), (98, 0, 40), (103, 0, 0), (104, 30, 0)])
    assert stable.choose_wall(found, "call", "103") == ("104", "NO_GAMMA")
    assert stable.choose_wall(found, "call", "999") == ("104", "NO_GAMMA")
    assert stable.choose_wall(found, "put", "97", exclude="97") == ("98", "TAKEN")
    assert stable.choose_wall(profile(100, [(101, 0, 5)]), "call", None) == (None, "NONE_ON_SIDE")
    # A put wall still held inside the band is not taken by the call side.
    inside = profile(99.8, [(98, 0, 50), (100, 90, 95), (101, 10, 0)])
    assert stable.choose_walls(inside, None, "100") == {"call": ("101", "FRESH"), "put": ("100", "HELD")}


# ---- the tracker ------------------------------------------------------------------------------------------------------

def _levels_line(found):
    return {"available": True, "observed_at": OBSERVED.isoformat(), "spot": str(found.spot),
            "call_wall": stable.unconstrained_wall(found, "call"), "put_wall": stable.unconstrained_wall(found, "put")}


def test_near_equal_strikes_flap_in_v1_and_not_in_the_stable_walls():
    tracker = stable.StableLevels(profile=lambda chain: chain)
    tracker.begin_window(SESSION)
    documents = []
    for index in range(10):  # IWM-like: the call wall alternates between two strikes 2% apart in gamma
        lead = 1.01 if index % 2 else 0.99
        found = profile(100, [(97, 0, 80), (98, 0, 60), (103, 100, 0), (104, 100 * lead, 0)])
        tracker.begin_window(SESSION)
        documents.append(tracker.observe("IWM", found, _levels_line(found)))
    last = documents[-1]
    assert last["v1"]["changes"] == {"call": 9, "put": 0}
    assert last["stable"]["changes"] == {"call": 0, "put": 0}
    assert last["stable"]["call_wall"] == "103" and last["stable"]["call_basis"] in ("BEST", "HELD")
    assert [document["stable"]["call_basis"] for document in documents[:3]] == ["FRESH", "HELD", "BEST"]
    assert last["windows"] == 10 and last["call_top"][0][0] in ("103", "104") and len(last["call_top"]) == 2
    assert last["call_top_ex0dte"] == last["call_top"]  # a hand-made profile has no 0 DTE split
    summary = tracker.window_summary()
    assert summary["session_changes"] == {"v1": {"call": 9, "put": 0}, "stable": {"call": 0, "put": 0}}
    assert summary["v1_changed"] == ["IWM"] and summary["stable_changed"] == []


def test_the_coincident_v1_walls_are_labelled_and_split():
    tracker = stable.StableLevels(profile=lambda chain: chain)
    tracker.begin_window(SESSION)
    found = profile(731.2, [(720, 10, 60), (730, 300, 400), (735, 50, 3), (750, 80, 1)])
    document = tracker.observe("SPY", found, _levels_line(found))
    assert document["v1"]["call_wall"] == document["v1"]["put_wall"] == "730"
    assert document["v1"]["coincide"] is True and document["v1"]["wrong_side"] == ["call"]
    assert document["stable"]["call_wall"] == "750" and document["stable"]["put_wall"] == "730"
    assert document["stable"]["call_side"] == "ABOVE" and document["stable"]["put_side"] == "BELOW"
    assert document["stable"]["call_best"] == "750" and document["stable"]["call_best_vs_wall"] == 1.0
    assert tracker.window_summary()["v1_coincide"] == ["SPY"]


def test_a_new_session_starts_fresh_and_an_unavailable_name_keeps_no_state():
    tracker = stable.StableLevels(profile=lambda chain: chain)
    tracker.begin_window(SESSION)
    first = profile(100, [(97, 0, 80), (103, 100, 0)])
    tracker.observe("SPY", first, _levels_line(first))
    assert tracker.observe("QQQ", None, {"available": False, "reason": "NO_EXPOSURE_IN_SCOPE"}) == {
        "underlying": "QQQ", "available": False, "reason": "NO_EXPOSURE_IN_SCOPE"}
    tracker.begin_window(SESSION + timedelta(days=1))
    second = profile(100, [(97, 0, 80), (104, 100, 0)])
    document = tracker.observe("SPY", second, _levels_line(second))
    assert document["windows"] == 1 and document["stable"]["call_basis"] == "FRESH"
    assert document["stable"]["changes"] == {"call": 0, "put": 0} and document["v1"]["changes"] == {"call": 0, "put": 0}


# ---- the recorder: OFF is r140, RECORD adds lines only ------------------------------------------------------------------

def _window(record, chains):
    provider = Provider(Cursor(window_script(chains)))
    result = record.record_window(provider, session_date=SESSION, equity_cutoff=CUTOFF, window_id="w-1",
                                  breadth={name: {"state": "COMPLETE"} for name in chains})
    return result, provider


def _without_ms(lines):
    return [(event, {key: value for key, value in document.items() if key not in ("ms", "seconds", "read_seconds",
                                                                                     "compute_seconds")})
            for event, document in lines]


def test_with_the_tracker_the_levels_lines_and_reads_are_unchanged_and_stable_lines_follow():
    chains = {"SPY": chain_rows("SPY", call_oi={97: 30000}, put_oi={97: 30000}), "QQQ": chain_rows("QQQ")}
    plain_lines, stable_lines = Lines(), Lines()
    plain_result, plain_provider = _window(recorder("RECORD", plain_lines), chains)
    tracker = stable.StableLevels()
    result, provider = _window(recorder("RECORD", stable_lines, stable=tracker), chains)
    assert {key: value for key, value in result.items() if key != "seconds"} == {
        key: value for key, value in plain_result.items() if key != "seconds"}
    assert provider.transactions == plain_provider.transactions == [("app", {"read_only": True})]
    assert provider.cursor.statements == plain_provider.cursor.statements
    v1 = [line for line in stable_lines.lines if not line[0].startswith("OPTIONS_LEVELS_STABLE")]
    assert _without_ms(v1) == _without_ms(plain_lines.lines)
    assert stable_lines.events() == [
        levels.LEVELS_RECORD_EVENT, stable.STABLE_RECORD_EVENT, levels.LEVELS_RECORD_EVENT, stable.STABLE_RECORD_EVENT,
        levels.LEVELS_WINDOW_EVENT, stable.STABLE_WINDOW_EVENT]
    named = {document["underlying"]: document for event, document in stable_lines.lines
             if event == stable.STABLE_RECORD_EVENT}
    spy = named["SPY"]
    assert spy["v1"]["call_wall"] == spy["v1"]["put_wall"] == "97" and spy["v1"]["coincide"] is True
    assert float(spy["stable"]["call_wall"]) > 100 and spy["stable"]["put_wall"] == "97"
    assert spy["mode"] == "RECORD" and spy["levels_mode"] == "RECORD" and spy["version"] == stable.VERSION
    assert spy["window_id"] == "w-1" and spy["applied"] is False and spy["execution_permission"] is False
    assert named["QQQ"]["stable"]["call_wall"] == named["QQQ"]["v1"]["call_wall"] == "103"
    window = [document for event, document in stable_lines.lines if event == stable.STABLE_WINDOW_EVENT][0]
    assert window["recorded"] == ["QQQ", "SPY"] and window["v1_coincide"] == ["SPY"]


def test_off_tracker_none_is_byte_identical_to_r140():
    chains = {"SPY": chain_rows("SPY")}
    first, second = Lines(), Lines()
    _window(recorder("RECORD", first), chains)
    _window(recorder("RECORD", second, stable=None), chains)
    assert _without_ms(first.lines) == _without_ms(second.lines)
    assert not any(event.startswith("OPTIONS_LEVELS_STABLE") for event in second.events())


def test_a_failing_tracker_is_its_stable_line_only():
    class Broken(stable.StableLevels):
        def observe(self, name, chain, document):
            raise RuntimeError("boom")

        def window_summary(self):
            raise RuntimeError("summary boom")

    lines = Lines()
    result, _provider = _window(recorder("RECORD", lines, stable=Broken()), {"SPY": chain_rows("SPY")})
    assert result["status"] == "RECORDED" and result["recorded"] == 1 and result["failed"] == 0
    stable_lines = [document for event, document in lines.lines if event == stable.STABLE_RECORD_EVENT]
    assert stable_lines[0]["available"] is False and stable_lines[0]["error"] == "RuntimeError: boom"
    window = [document for event, document in lines.lines if event == stable.STABLE_WINDOW_EVENT][0]
    assert window["error"] == "RuntimeError: summary boom"
    assert lines.named()["SPY"]["available"] is True and lines.faults == []


def test_a_stable_emit_that_raises_never_escapes():
    seen = []

    def emit(event, document):
        if event.startswith("OPTIONS_LEVELS_STABLE"):
            raise OSError("pipe closed")
        seen.append(event)

    record = levels.OptionsLevelsRecorder("RECORD", emit=emit, fault=lambda **_: None, stable=stable.StableLevels())
    result = record.record_window(Provider(Cursor(window_script({"SPY": chain_rows("SPY")}))), session_date=SESSION,
                                  equity_cutoff=CUTOFF, window_id="w", breadth={"SPY": {"state": "COMPLETE"}})
    assert result["status"] == "RECORDED" and seen == [levels.LEVELS_RECORD_EVENT, levels.LEVELS_WINDOW_EVENT]


def test_a_failed_levels_computation_gives_an_unavailable_stable_line():
    def flaky(chain):
        raise ValueError("bad chain")

    lines = Lines()
    record = recorder("RECORD", lines, levels=flaky, stable=stable.StableLevels())
    result, _provider = _window(record, {"SPY": chain_rows("SPY")})
    assert result["failed"] == 1
    (document,) = [document for event, document in lines.lines if event == stable.STABLE_RECORD_EVENT]
    assert document["available"] is False and document["reason"] == "LEVELS_FAILED"


def test_the_stable_line_stays_under_the_docker_limit():
    tracker = stable.StableLevels(profile=lambda chain: chain)
    tracker.begin_window(SESSION)
    found = profile(100, [(50 + index * 0.5, index, 200 - index) for index in range(200)])
    document = tracker.observe("SPY", found, _levels_line(found))
    line = levels.render_line(stable.STABLE_RECORD_EVENT, document)
    assert len(line.encode("utf-8")) < levels.LINE_BYTE_LIMIT and "truncated" not in line
    assert len(document["call_top"]) == len(document["put_top"]) == stable.TOP_STRIKES


# ---- the scheduler service ------------------------------------------------------------------------------------------

@pytest.mark.parametrize("value", [None, "", "OFF"])
def test_the_service_with_the_switch_off_starts_the_levels_recorder_as_before(monkeypatch, capsys, value):
    if value is None:
        monkeypatch.delenv(SWITCH, raising=False)
    else:
        monkeypatch.setenv(SWITCH, value)
    monkeypatch.delenv("REALTIME_V3_OPTIONS_OI_CHANGE", raising=False)
    calls = _service(monkeypatch, "RECORD")
    assert calls[0]["levels"]._stable is None
    assert "OPTIONS_LEVELS_STABLE" not in capsys.readouterr().err


def test_the_service_in_record_attaches_one_tracker(monkeypatch, capsys):
    monkeypatch.setenv(SWITCH, "RECORD")
    monkeypatch.delenv("REALTIME_V3_OPTIONS_OI_CHANGE", raising=False)
    calls = _service(monkeypatch, "RECORD")
    assert isinstance(calls[0]["levels"]._stable, stable.StableLevels)
    err = capsys.readouterr().err
    assert err.count("OPTIONS_LEVELS_STABLE_SETTING ") == 1 and '"active":true' in err


def test_the_service_in_record_with_levels_off_is_inactive(monkeypatch, capsys):
    monkeypatch.setenv(SWITCH, "RECORD")
    monkeypatch.delenv("REALTIME_V3_OPTIONS_OI_CHANGE", raising=False)
    calls = _service(monkeypatch, None)
    assert calls[0]["levels"] is None
    err = capsys.readouterr().err
    assert err.count("OPTIONS_LEVELS_STABLE_SETTING ") == 1 and '"reason":"LEVELS_OFF"' in err


def test_the_cycle_with_a_tracker_keeps_the_levels_key_shape(monkeypatch):
    lines = Lines()
    observed = OBSERVED
    script = window_script({"SPY": chain_rows("SPY", observed=observed)}, captures=[("SPY", "COMPLETE", observed)])
    result, provider, _cursor = _attached_cycle(monkeypatch, script=script,
                                                levels=recorder("RECORD", lines, stable=stable.StableLevels()))
    assert result["levels"]["status"] == "RECORDED" and "oi_change" not in result
    assert stable.STABLE_RECORD_EVENT in lines.events() and stable.STABLE_WINDOW_EVENT in lines.events()
    assert json.dumps(result, sort_keys=True, default=str)


def test_late_in_the_day_the_ex0dte_diagnostics_show_the_wall_behind_the_pin():
    # Today's expiry piles gamma on 101 (next to spot); the 7-day wall sits at 105.
    found = stable.StrikeGamma(spot=100.4, strikes=(99.0, 101.0, 105.0), texts=("99", "101", "105"),
                               call=(5.0, 300.0, 120.0), put=(90.0, 10.0, 0.0),
                               call_ex0dte=(5.0, 20.0, 120.0), put_ex0dte=(90.0, 0.0, 0.0))
    tracker = stable.StableLevels(profile=lambda chain: chain)
    tracker.begin_window(SESSION)
    document = tracker.observe("SPY", found, _levels_line(found))
    assert document["stable"]["call_wall"] == "101" and document["stable"]["call_best_ex0dte"] == "105"
    assert document["call_top_ex0dte"] == [["105", 120], ["101", 20], ["99", 5]]
    assert document["call_top"][0] == ["101", 300]
