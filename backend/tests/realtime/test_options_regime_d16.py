"""G106 D16 R-REGIME (Nov 19): `REALTIME_V3_REGIME` OFF (default) = the levels recorder and the scheduler exactly as
r140; RECORD (with the levels switch RECORD or ON) = after each window's levels one read-only transaction for the SPY
and QQQ IV summaries and one `OPTIONS_REGIME_RECORD` line (gamma sign of SPY/QQQ and of every name, SPY's distance to
its flip, SPY's 7d/30d ATM IV ratio, universe put/call, and a regime label); never fails the levels or the cycle."""
from __future__ import annotations

import importlib

import pytest

from realtime import advanced_options_levels as levels
from realtime import advanced_options_regime as regime
from realtime.options_exposure_read import build_exposure_chain
from test_advanced_options_levels import (
    CUTOFF,
    OBSERVED,
    SESSION,
    Cursor,
    Lines,
    Provider,
    _service,
    chain_rows,
    recorder,
    window_script,
)

SWITCH = regime.REGIME_SETTING


def summary(front, back, *, rr=-0.04, volume=1.3, oi=1.6):
    return {"available": True, "observed_at": OBSERVED.isoformat(),
            "tenors": [{"days": 7, "atm_iv": front}, {"days": 30, "atm_iv": back, "risk_reversal_25_delta": rr},
                       {"days": 60, "atm_iv": back}],
            "put_call": {"volume_ratio": volume, "open_interest_ratio": oi}}


def level(sign, flip_pct=2.0, *, available=True):
    return {"available": available, "gex": -5 if sign == "NEGATIVE" else 5, "gex_sign": sign, "spot": "100",
            "flip": None if flip_pct is None else str(100 + flip_pct), "flip_pct": flip_pct,
            "regime_at_spot": f"{sign}_GAMMA", "observed_at": OBSERVED.isoformat(), "call_wall_pct": 3.0,
            "put_wall_pct": -3.0, "zero_dte_share": 0.1, "dex_tilt": 0.2}


# ---- the switch ---------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [(None, "OFF"), ("", "OFF"), ("Off", "OFF"), (" record", "RECORD")])
def test_mode_values(raw, expected):
    assert regime.regime_mode({} if raw is None else {SWITCH: raw}).value == expected


@pytest.mark.parametrize("raw", ["ON", "SHADOW", "1"])
def test_an_invalid_value_raises_and_the_scheduler_refuses_to_start(monkeypatch, raw):
    from realtime.migrations import EXPECTED_V2_DATABASE_NAME

    with pytest.raises(ValueError, match=SWITCH):
        regime.regime_mode({SWITCH: raw})
    with pytest.raises(ValueError, match=SWITCH):
        regime.parse_regime_mode(3)
    script = importlib.import_module("scripts.run_advanced_options_v3_scheduler")
    argv = ["--database", EXPECTED_V2_DATABASE_NAME, "--confirm-database-name", EXPECTED_V2_DATABASE_NAME,
            "--continuous", "--execute"]
    monkeypatch.setenv(SWITCH, raw)
    with pytest.raises(ValueError, match=SWITCH):
        script.parse_args(argv)
    monkeypatch.setenv(SWITCH, "RECORD")
    assert script.parse_args(argv).continuous


def test_start_off_is_silent_and_record_needs_the_levels_recorder():
    lines = Lines()
    assert regime.start_regime("OFF", levels_mode="RECORD", emit=lines.emit) is None and lines.lines == []
    assert regime.start_regime("RECORD", levels_mode="OFF", emit=lines.emit) is None
    assert lines.lines[0][1]["active"] is False and lines.lines[0][1]["reason"] == "LEVELS_OFF"
    assert isinstance(regime.start_regime("record", levels_mode="ON", emit=lines.emit), regime.RegimeRecorder)
    assert lines.lines[1][1]["active"] is True and lines.lines[1][1]["iv_names"] == ["SPY", "QQQ"]


# ---- the pure parts -------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("flip_pct, state", [(2.4, "BELOW_FLIP"), (-3.0, "ABOVE_FLIP"), (0.5, "NEAR_FLIP"),
                                             (-0.2, "NEAR_FLIP"), (None, "NO_FLIP")])
def test_index_entry_flip_state(flip_pct, state):
    entry = regime.index_entry(level("NEGATIVE", flip_pct))
    assert entry["flip_state"] == state and entry["gex_sign"] == "NEGATIVE" and entry["available"] is True
    assert regime.index_entry(None) == {"available": False, "reason": None}
    assert regime.index_entry({"available": False, "reason": "SPOT_UNAVAILABLE"})["reason"] == "SPOT_UNAVAILABLE"


@pytest.mark.parametrize("front, back, term, ratio", [(0.30, 0.25, "INVERTED", 1.2), (0.25, 0.25, "INVERTED", 1.0),
                                                      (0.24, 0.25, "FLAT", 0.96), (0.20, 0.25, "CONTANGO", 0.8),
                                                      (None, 0.25, "UNKNOWN", None)])
def test_iv_entry_term(front, back, term, ratio):
    entry = regime.iv_entry(summary(front, back))
    assert entry["term"] == term and entry["term_ratio"] == ratio and entry["rr25_30d"] == -0.04
    assert entry["put_call_volume"] == 1.3 and entry["put_call_oi"] == 1.6
    assert regime.iv_entry(None) == {"available": False, "reason": "NOT_READ"}
    assert regime.iv_entry("READ_FAILED: OperationalError")["reason"] == "READ_FAILED: OperationalError"
    assert regime.iv_entry({"available": False, "reason": "NO_CHAIN_CAPTURE"})["reason"] == "NO_CHAIN_CAPTURE"


@pytest.mark.parametrize("spy, qqq, term, expected", [
    (None, "NEGATIVE", "INVERTED", ("UNKNOWN", "UNKNOWN")),
    (("NEGATIVE", 0.3), "NEGATIVE", "INVERTED", ("NEGATIVE", "TRANSITION")),
    (("NEGATIVE", 2.0), "NEGATIVE", "INVERTED", ("NEGATIVE", "STRESS")),
    (("NEGATIVE", 2.0), "NEGATIVE", "CONTANGO", ("NEGATIVE", "FRAGILE")),
    (("NEGATIVE", 2.0), None, "UNKNOWN", ("NEGATIVE", "FRAGILE")),
    (("POSITIVE", -2.0), "NEGATIVE", "INVERTED", ("MIXED", "FRAGILE")),
    (("POSITIVE", -2.0), "NEGATIVE", "CONTANGO", ("MIXED", "NEUTRAL")),
    (("POSITIVE", -2.0), "POSITIVE", "CONTANGO", ("POSITIVE", "CALM")),
    (("POSITIVE", -2.0), "POSITIVE", "FLAT", ("POSITIVE", "NEUTRAL")),
    (("POSITIVE", None), "POSITIVE", "UNKNOWN", ("POSITIVE", "NEUTRAL")),
])
def test_classify(spy, qqq, term, expected):
    index = {"SPY": regime.index_entry(None if spy is None else level(spy[0], spy[1])),
             "QQQ": regime.index_entry(None if qqq is None else level(qqq))}
    labels = regime.classify(index, {"SPY": {"term": term}})
    assert (labels["gamma"], labels["regime"]) == expected and labels["term"] == term


def test_universe_put_call_over_the_chains():
    chains = {name: build_exposure_chain(chain_rows(name), underlyer=name, session_date=SESSION, observed_at=OBSERVED)
              for name in ("SPY", "QQQ")}
    found = regime.universe_put_call({**chains, "BROKEN": object()})
    assert found["names"] == 2 and found["volume_ratio"] == 1.0 and found["put_volume"] == found["call_volume"] > 0
    assert found["oi_ratio"] == round(found["put_oi"] / found["call_oi"], 4)
    assert regime.universe_put_call({})["volume_ratio"] is None


def test_regime_document_counts_every_names_gamma():
    documents = {"SPY": level("NEGATIVE"), "QQQ": level("NEGATIVE"), "AAPL": level("POSITIVE", -4.0),
                 "MSTR": level("NEGATIVE", None), "COIN": {"available": False, "reason": "SPOT_UNAVAILABLE"}}
    document = regime.regime_document(documents, {"SPY": summary(0.3, 0.25), "QQQ": "NO_LEVELS_CAPTURE"}, {})
    assert document["labels"] == {"gamma": "NEGATIVE", "flip": "BELOW_FLIP", "term": "INVERTED", "regime": "STRESS"}
    assert document["gamma_breadth"] == {"names": 4, "negative": ["MSTR", "QQQ", "SPY"], "positive": ["AAPL"],
                                         "zero": [], "negative_share": 0.75}
    assert document["index"]["IWM"] == {"available": False, "reason": None}
    assert document["iv"]["QQQ"] == {"available": False, "reason": "NO_LEVELS_CAPTURE"}


# ---- the recorder ---------------------------------------------------------------------------------------------------------

def test_record_reads_the_iv_names_in_one_read_only_transaction():
    seen = []

    def read(cursor, *, underlyer, available_by, session_date):
        seen.append((underlyer, available_by, session_date))
        return summary(0.2, 0.25)

    provider = Provider(Cursor([]))
    document = regime.RegimeRecorder(read=read).record(
        provider, head={"window_id": "w"}, session_date=SESSION,
        documents={"SPY": level("POSITIVE", -2.0), "QQQ": level("POSITIVE", -1.0)}, chains={})
    assert provider.transactions == [("app", {"read_only": True})]
    statements = [statement for statement, _ in provider.cursor.statements]
    assert statements == ["SET LOCAL statement_timeout = '10s'", "SAVEPOINT regime_name", "RELEASE SAVEPOINT regime_name",
                          "SAVEPOINT regime_name", "RELEASE SAVEPOINT regime_name"]
    assert seen == [("SPY", OBSERVED, SESSION), ("QQQ", OBSERVED, SESSION)]
    assert document["labels"]["regime"] == "CALM" and document["window_id"] == "w" and document["read_error"] is None
    assert document["version"] == regime.VERSION and document["mode"] == "RECORD"


def test_one_names_failed_read_and_a_failed_transaction():
    def read(cursor, *, underlyer, available_by, session_date):
        if underlyer == "QQQ":
            raise RuntimeError("canceling statement due to statement timeout")
        return summary(0.3, 0.25)

    provider = Provider(Cursor([]))
    documents = {"SPY": level("NEGATIVE"), "QQQ": level("NEGATIVE")}
    document = regime.RegimeRecorder(read=read).record(provider, head={}, session_date=SESSION, documents=documents,
                                                       chains={})
    assert "ROLLBACK TO SAVEPOINT regime_name" in [statement for statement, _ in provider.cursor.statements]
    assert document["iv"]["QQQ"] == {"available": False, "reason": "READ_FAILED: RuntimeError"}
    assert document["labels"]["regime"] == "STRESS"
    failed = regime.RegimeRecorder(read=read).record(Provider(error=RuntimeError("pool exhausted")), head={},
                                                     session_date=SESSION, documents=documents, chains={})
    assert failed["read_error"] == "RuntimeError: pool exhausted" and failed["labels"]["term"] == "UNKNOWN"
    assert failed["labels"]["regime"] == "FRAGILE"
    # No SPY or QQQ levels capture: nothing is read.
    empty = Provider(Cursor([]))
    nothing = regime.RegimeRecorder(read=read).record(empty, head={}, session_date=SESSION, documents={}, chains={})
    assert empty.transactions == [] and nothing["labels"]["regime"] == "UNKNOWN"


# ---- with the levels recorder ---------------------------------------------------------------------------------------------

def _window(record, chains):
    provider = Provider(Cursor(window_script(chains)))
    result = record.record_window(provider, session_date=SESSION, equity_cutoff=CUTOFF, window_id="w-1",
                                  breadth={name: {"state": "COMPLETE"} for name in chains})
    return result, provider


def _stripped(lines):
    return [(event, {key: value for key, value in document.items()
                     if key not in ("ms", "seconds", "read_seconds", "compute_seconds")}) for event, document in lines]


def test_with_a_regime_recorder_the_levels_lines_are_unchanged_and_one_regime_line_follows():
    chains = {"SPY": chain_rows("SPY"), "QQQ": chain_rows("QQQ")}
    plain, lines = Lines(), Lines()
    plain_result, plain_provider = _window(recorder("RECORD", plain), chains)
    read_calls = []

    def read(cursor, **kwargs):
        read_calls.append(kwargs["underlyer"])
        return summary(0.3, 0.25)

    result, provider = _window(recorder("RECORD", lines, regime=regime.RegimeRecorder(read=read)), chains)
    assert {key: value for key, value in result.items() if key != "seconds"} == {
        key: value for key, value in plain_result.items() if key != "seconds"}
    assert provider.transactions == plain_provider.transactions + [("app", {"read_only": True})]
    assert _stripped(lines.lines[:-1]) == _stripped(plain.lines)
    event, document = lines.lines[-1]
    assert event == regime.REGIME_RECORD_EVENT and read_calls == ["SPY", "QQQ"]
    assert document["window_id"] == "w-1" and document["levels_mode"] == "RECORD" and document["applied"] is False
    assert document["gamma_breadth"]["names"] == 2 and document["put_call"]["names"] == 2
    assert document["labels"]["gamma"] in ("NEGATIVE", "POSITIVE") and document["labels"]["term"] == "INVERTED"


def test_a_raising_regime_recorder_is_its_line_only():
    class Broken:
        def record(self, provider, **kwargs):
            raise RuntimeError("boom")

    lines = Lines()
    result, _provider = _window(recorder("RECORD", lines, regime=Broken()), {"SPY": chain_rows("SPY")})
    assert result["status"] == "RECORDED" and result["recorded"] == 1
    event, document = lines.lines[-1]
    assert event == regime.REGIME_RECORD_EVENT and document["status"] == "FAILED"
    assert document["error"] == "RuntimeError: boom" and lines.faults == []


def test_the_regime_line_stays_under_the_docker_limit():
    documents = {f"N{index:02d}": level("NEGATIVE" if index % 2 else "POSITIVE") for index in range(400)}
    document = regime.regime_document(documents, {"SPY": summary(0.3, 0.2)}, {})
    assert len(levels.render_line(regime.REGIME_RECORD_EVENT, document).encode("utf-8")) <= levels.LINE_BYTE_LIMIT


# ---- the scheduler service ------------------------------------------------------------------------------------------

def _clear(monkeypatch):
    for name in ("REALTIME_V3_OPTIONS_OI_CHANGE", "REALTIME_V3_OPTIONS_LEVELS_STABLE"):
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize("value", [None, "", "OFF"])
def test_the_service_off_starts_the_levels_recorder_as_before(monkeypatch, capsys, value):
    _clear(monkeypatch)
    if value is None:
        monkeypatch.delenv(SWITCH, raising=False)
    else:
        monkeypatch.setenv(SWITCH, value)
    calls = _service(monkeypatch, "RECORD")
    assert calls[0]["levels"]._regime is None and calls[0]["levels"]._stable is None
    assert "OPTIONS_REGIME" not in capsys.readouterr().err


def test_the_service_in_record_attaches_one_regime_recorder(monkeypatch, capsys):
    _clear(monkeypatch)
    monkeypatch.setenv(SWITCH, "RECORD")
    calls = _service(monkeypatch, "RECORD")
    assert isinstance(calls[0]["levels"]._regime, regime.RegimeRecorder) and calls[0]["levels"]._stable is None
    assert capsys.readouterr().err.count("OPTIONS_REGIME_SETTING ") == 1


def test_the_service_with_all_three_new_switches(monkeypatch, capsys):
    _clear(monkeypatch)
    monkeypatch.setenv(SWITCH, "RECORD")
    monkeypatch.setenv("REALTIME_V3_OPTIONS_LEVELS_STABLE", "RECORD")
    monkeypatch.setenv("REALTIME_V3_OPTIONS_OI_CHANGE", "RECORD")
    calls = _service(monkeypatch, "RECORD")
    assert calls[0]["levels"]._regime is not None and calls[0]["levels"]._stable is not None
    assert calls[0]["oi_change"] is not None
    err = capsys.readouterr().err
    for line in ("OPTIONS_REGIME_SETTING ", "OPTIONS_LEVELS_STABLE_SETTING ", "OPTIONS_OI_CHANGE_SETTING ",
                 "OPTIONS_LEVELS_SETTING "):
        assert err.count(line) == 1, line
