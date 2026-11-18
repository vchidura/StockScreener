"""G106 levels and context (Nov 18): dealer gamma and delta levels per name per Options window, RECORD only.

`REALTIME_V3_OPTIONS_LEVELS` (options-scheduler): OFF (default) = no SQL, no line, the cycle result unchanged; RECORD =
after each window's chain capture one read-only transaction and one `OPTIONS_LEVELS_RECORD` line per name (the OV1
page's numbers) plus one `OPTIONS_LEVELS_RECORD_WINDOW` line; ON = accepted, behaves as RECORD (no consumer yet) with an
ON_UNAVAILABLE line and a DEGRADED fault; anything else refuses start."""
from __future__ import annotations

import importlib
import json
import threading
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pytest

from realtime import advanced_options_continuous_scheduler as scheduler
from realtime import advanced_options_levels as levels
from realtime.options_exposure_read import _ROW_NAMES, build_exposure_chain, compute_exposure

UTC = timezone.utc
SESSION = date(2026, 11, 17)
SWITCH = levels.LEVELS_SETTING
CUTOFF = datetime(2026, 11, 17, 18, 30, tzinfo=UTC)
OBSERVED = CUTOFF + timedelta(seconds=70)


# ---- a synthetic chain ------------------------------------------------------------------------------------------------

def chain_rows(underlying="SPY", *, spot=100.0, observed=OBSERVED, call_oi=None, put_oi=None):
    """Calls and puts 80..120 for today (0 DTE, 100 open interest each) and 7 days out (heavier on puts below spot and
    calls above it, walls at 97 and 103), so the chain has walls on both sides and a flip inside the search range."""
    rows = []
    for expiry in (SESSION, SESSION + timedelta(days=7)):
        for strike in range(80, 121):
            for kind in ("call", "put"):
                if kind == "call":
                    oi = (call_oi or {}).get(strike, 9000 if strike == 103 else 1500 if strike >= spot else 300)
                else:
                    oi = (put_oi or {}).get(strike, 9000 if strike == 97 else 1800 if strike <= spot else 200)
                if expiry == SESSION:
                    oi = 100  # today's expiry: small open interest everywhere (its gamma is all at the money)
                rows.append({
                    "contract_type": kind, "expiration_date": expiry, "strike_price": Decimal(strike),
                    "open_interest": oi, "day_volume": 50, "local_iv": 0.32, "implied_volatility": 0.33,
                    "gamma": None, "delta": None, "risk_free_rate": 0.04, "dividend_yield": 0.0,
                    "spot": Decimal(str(spot)), "underlying_price": Decimal(str(spot)), "observed_at": observed,
                })
    return rows


def as_tuples(rows):
    return [tuple(row[name] for name in _ROW_NAMES) for row in rows]


class Cursor:
    """Answers each statement by the first matching needle; a value may be rows, an exception or a function of the
    parameters."""

    def __init__(self, script):
        self.script = list(script)
        self.statements = []
        self._rows = []

    def execute(self, sql, params=()):
        self.statements.append((" ".join(sql.split()), params))
        for needle, answer in self.script:
            if needle in sql:
                if callable(answer) and not isinstance(answer, Exception):
                    answer = answer(params)
                if isinstance(answer, Exception):
                    raise answer
                self._rows = list(answer)
                return
        self._rows = []

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows


class Provider:
    def __init__(self, cursor=None, error=None):
        self.cursor = cursor
        self.error = error
        self.transactions = []

    @contextmanager
    def transaction(self, role, **kwargs):
        self.transactions.append((role, kwargs))
        if self.error is not None:
            raise self.error
        yield SimpleNamespace(cursor=self.cursor)


def window_script(chains, *, captures=None, earlier=None, rows_error=None):
    """`chains`: {name: rows}. Captures default to COMPLETE at OBSERVED for every name."""
    captures = captures if captures is not None else [(name, "COMPLETE", OBSERVED) for name in sorted(chains)]

    def groups(params):
        _session, name, observed = params
        found = [(observed, len(chains.get(name, ())))] if chains.get(name) else []
        if earlier and name in earlier:
            found.append((observed - timedelta(minutes=30), earlier[name]))
        return found

    def rows(params):
        _session, name, _observed = params
        if rows_error and name in rows_error:
            return rows_error[name]
        return as_tuples(chains.get(name, ()))

    return [
        ("rt.option_breadth_captures", captures),
        ("GROUP BY observed_at", groups),
        ("SELECT contract_type", rows),
    ]


class Lines:
    def __init__(self):
        self.lines = []
        self.faults = []

    def emit(self, event, document):
        line = levels.render_line(event, document)
        assert len(line.encode("utf-8")) < 16 * 1024
        self.lines.append((event, json.loads(line.split(" ", 1)[1])))

    def fault(self, **kwargs):
        self.faults.append(kwargs)

    def events(self):
        return [event for event, _ in self.lines]

    def named(self):
        return {document["underlying"]: document for event, document in self.lines
                if event == levels.LEVELS_RECORD_EVENT}

    def window(self):
        (document,) = [document for event, document in self.lines if event == levels.LEVELS_WINDOW_EVENT]
        return document


def recorder(mode, lines, **kwargs):
    return levels.OptionsLevelsRecorder(mode, emit=lines.emit, fault=lines.fault, **kwargs)


# ---- the switch ---------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    (None, "OFF"), ("", "OFF"), ("  ", "OFF"), ("off", "OFF"), (" Record ", "RECORD"), ("on", "ON"), ("ON", "ON"),
])
def test_mode_values(raw, expected):
    environ = {} if raw is None else {SWITCH: raw}
    assert levels.options_levels_mode(environ).value == expected


@pytest.mark.parametrize("raw", ["SHADOW", "1", "true", "RECORD_ONLY", "ENABLED"])
def test_invalid_mode_raises_naming_the_key(raw):
    with pytest.raises(ValueError, match=SWITCH):
        levels.options_levels_mode({SWITCH: raw})
    with pytest.raises(ValueError, match=SWITCH):
        levels.start_options_levels(raw)


def test_a_non_text_value_is_refused():
    with pytest.raises(ValueError, match=SWITCH):
        levels.parse_options_levels_mode(1)


def test_the_options_scheduler_refuses_to_start_on_an_invalid_value(monkeypatch):
    from realtime.migrations import EXPECTED_V2_DATABASE_NAME

    script = importlib.import_module("scripts.run_advanced_options_v3_scheduler")
    argv = ["--database", EXPECTED_V2_DATABASE_NAME, "--confirm-database-name", EXPECTED_V2_DATABASE_NAME,
            "--continuous", "--execute"]
    monkeypatch.setenv(SWITCH, "SHADOW")
    with pytest.raises(ValueError, match=SWITCH):
        script.parse_args(argv)
    for good in ("", "OFF", "RECORD", "ON", "record"):
        monkeypatch.setenv(SWITCH, good)
        assert script.parse_args(argv).continuous


# ---- OFF ----------------------------------------------------------------------------------------------------------------

def test_off_runs_nothing_and_logs_nothing(monkeypatch, capsys):
    monkeypatch.delenv(SWITCH, raising=False)
    assert levels.start_options_levels("OFF") is None
    assert levels.start_options_levels(None) is None
    monkeypatch.setenv(SWITCH, " off ")
    assert levels.start_options_levels(None) is None
    lines = Lines()
    off = recorder("OFF", lines)
    off.start()
    provider = Provider(Cursor([]))
    assert off.record_window(provider, session_date=SESSION, equity_cutoff=CUTOFF, window_id="w") is None
    assert provider.transactions == [] and lines.lines == [] and lines.faults == []
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == ""


# ---- RECORD -------------------------------------------------------------------------------------------------------------

def test_record_logs_the_pages_levels_per_name_from_one_read_only_transaction():
    lines = Lines()
    record = recorder("RECORD", lines)
    record.start()
    assert lines.events() == [levels.LEVELS_SETTING_EVENT]
    assert lines.lines[0][1] == {"applied": False, "mode": "RECORD", "scope": "all", "setting": SWITCH,
                                 "version": levels.VERSION, "weight": "oi"}
    spy, qqq = chain_rows("SPY"), chain_rows("QQQ", spot=200.0)
    qqq = [{**row, "strike_price": row["strike_price"] * 2} for row in qqq]
    cursor = Cursor(window_script({"SPY": spy, "QQQ": qqq}, earlier={"SPY": 7}))
    provider = Provider(cursor)
    result = record.record_window(provider, session_date=SESSION, equity_cutoff=CUTOFF, window_id="window-1",
                                  breadth={"SPY": {"state": "COMPLETE"}, "QQQ": {"state": "COMPLETE"}})
    assert provider.transactions == [("app", {"read_only": True})]
    assert result == {"status": "RECORDED", "mode": "RECORD", "applied": False, "complete": 2, "recorded": 2,
                      "failed": 0, "seconds": result["seconds"]}
    sql = [statement for statement, _ in cursor.statements]
    assert sql[0] == "SET LOCAL statement_timeout = '20s'"
    assert all(statement.startswith(("SET LOCAL", "SELECT", "SAVEPOINT", "RELEASE")) for statement in sql)
    assert sum("SAVEPOINT levels_name" == statement for statement in sql) == 2
    captures_params = next(params for statement, params in cursor.statements if "option_breadth_captures" in statement)
    assert captures_params == (SESSION, CUTOFF)
    rows_params = [params for statement, params in cursor.statements if statement.startswith("SELECT contract_type")]
    assert rows_params == [(SESSION, "QQQ", OBSERVED), (SESSION, "SPY", OBSERVED)]

    named = lines.named()
    assert sorted(named) == ["QQQ", "SPY"] and lines.events()[-1] == levels.LEVELS_WINDOW_EVENT
    document = named["SPY"]
    # The page's own computation on the same rows (weight oi, scope all).
    chain = build_exposure_chain(spy, underlyer="SPY", session_date=SESSION, observed_at=OBSERVED,
                                 earlier_capture_rows=7)
    gamma = compute_exposure(chain, greek="gamma", weight="oi", scope="all")
    delta = compute_exposure(chain, greek="delta", weight="oi", scope="all")
    assert document["available"] is True and document["spot"] == gamma["spot"] == "100"
    assert document["gex"] == gamma["totals"]["net"] and document["gex_sign"] == ("POSITIVE" if gamma["totals"]["net"] > 0
                                                                                  else "NEGATIVE")
    assert document["gex_call"] == gamma["totals"]["call"] and document["gex_put"] == gamma["totals"]["put"]
    assert document["regime_at_spot"] == gamma["levels"]["regime_at_spot"]
    assert document["flip"] == gamma["levels"]["flip_spot"] is not None
    assert document["flip_pct"] == pytest.approx(float(gamma["levels"]["flip_spot"]) - 100.0, abs=0.001)
    assert document["call_wall"] == gamma["levels"]["call_wall"]["strike"] == "103"
    assert document["put_wall"] == gamma["levels"]["put_wall"]["strike"] == "97"
    assert document["call_wall_pct"] == 3.0 and document["put_wall_pct"] == -3.0
    assert document["dex"] == delta["totals"]["net"] and document["dex"] > 0  # the convention: never negative
    assert document["dex_call"] == delta["totals"]["call"] and document["dex_put"] == delta["totals"]["put"]
    assert document["dex_tilt"] == round((abs(document["dex_call"]) - abs(document["dex_put"]))
                                         / (abs(document["dex_call"]) + abs(document["dex_put"])), 4)
    assert [item["strike"] for item in document["top_gamma"]] == [
        item["strike"] for item in gamma["largest_levels"][:3]]
    assert document["zero_dte_share"] == gamma["expiring_today_share"] > 0
    # 2.5 hours before the close, today's gamma sits at the money, not at the walls 3% away.
    assert document["zero_dte_peak"]["strike"] == "100" and 0 < document["zero_dte_peak"]["share_of_0dte"] <= 1
    assert document["rows"]["capture"] == len(spy) and document["rows"]["earlier_excluded"] == 7
    assert document["rows"]["contributing"] == gamma["coverage"]["contributing_contracts"]
    assert document["quality"] == gamma["quality_reasons"]
    assert document["observed_at"] == OBSERVED.isoformat() and document["equity_cutoff"] == CUTOFF.isoformat()
    assert document["window_id"] == "window-1" and document["applied"] is False
    assert document["execution_permission"] is False and document["convention"] == "DEALER_LONG_CALLS_SHORT_PUTS"

    window = lines.window()
    assert window["captures"] == 2 and window["complete"] == 2 and window["recorded"] == 2
    assert window["failed"] == [] and window["unavailable"] == {} and window["not_complete"] == {}
    assert window["rows"] == len(spy) + len(qqq) and window["read"] == "READ"
    assert set(window) >= {"read_seconds", "compute_seconds", "seconds"}
    assert sorted(name for names in window["regimes"].values() for name in names) == ["QQQ", "SPY"]
    assert lines.faults == []


def test_a_name_whose_capture_is_not_complete_is_not_read():
    lines = Lines()
    record = recorder("RECORD", lines)
    cursor = Cursor(window_script({"SPY": chain_rows("SPY"), "QQQ": chain_rows("QQQ")}, captures=[
        ("QQQ", "PARTIAL", OBSERVED), ("SPY", "COMPLETE", OBSERVED), ("SPY", "FAILED", OBSERVED - timedelta(seconds=9)),
    ]))
    record.record_window(Provider(cursor), session_date=SESSION, equity_cutoff=CUTOFF, window_id="w")
    assert sorted(lines.named()) == ["SPY"]
    assert [params[1] for statement, params in cursor.statements if statement.startswith("SELECT contract_type")] == [
        "SPY"]
    assert lines.window()["not_complete"] == {"QQQ": "PARTIAL"}


def test_rows_overwritten_by_a_newer_capture_are_unavailable_not_mixed():
    lines = Lines()
    record = recorder("RECORD", lines)
    script = window_script({"SPY": chain_rows("SPY")})
    script[1] = ("GROUP BY observed_at", [(OBSERVED + timedelta(minutes=5), 10)])
    cursor = Cursor(script)
    record.record_window(Provider(cursor), session_date=SESSION, equity_cutoff=CUTOFF, window_id="w")
    assert lines.named()["SPY"]["available"] is False and lines.named()["SPY"]["reason"] == "CAPTURE_ROWS_MISSING"
    assert not any(statement.startswith("SELECT contract_type") for statement, _ in cursor.statements)
    assert lines.window()["unavailable"] == {"SPY": "CAPTURE_ROWS_MISSING"}


def test_a_capture_without_spot_is_unavailable():
    lines = Lines()
    record = recorder("RECORD", lines)
    rows = [{**row, "spot": None, "underlying_price": None} for row in chain_rows("SPY")]
    record.record_window(Provider(Cursor(window_script({"SPY": rows}))), session_date=SESSION, equity_cutoff=CUTOFF,
                         window_id="w")
    assert lines.named()["SPY"]["available"] is False and lines.named()["SPY"]["reason"] == "SPOT_UNAVAILABLE"


def test_a_chain_without_open_interest_is_the_pages_unavailable_reason():
    lines = Lines()
    record = recorder("RECORD", lines)
    rows = [{**row, "open_interest": 0} for row in chain_rows("SPY")]
    record.record_window(Provider(Cursor(window_script({"SPY": rows}))), session_date=SESSION, equity_cutoff=CUTOFF,
                         window_id="w")
    document = lines.named()["SPY"]
    assert document["available"] is False and document["reason"] == "NO_EXPOSURE_IN_SCOPE"
    assert lines.window()["unavailable"] == {"SPY": "NO_EXPOSURE_IN_SCOPE"} and lines.window()["recorded"] == 0


def test_a_window_without_a_complete_capture_reads_nothing():
    lines = Lines()
    record = recorder("RECORD", lines)
    provider = Provider(Cursor([]))
    result = record.record_window(provider, session_date=SESSION, equity_cutoff=CUTOFF, window_id="w",
                                  breadth={"status": "FAILED", "reason": "x"})
    assert provider.transactions == [] and result["status"] == "RECORDED" and result["recorded"] == 0
    assert lines.events() == [levels.LEVELS_WINDOW_EVENT]
    assert lines.window()["read"] == "SKIPPED_NO_COMPLETE_CAPTURE"


def test_on_is_accepted_but_records_only():
    lines = Lines()
    on = recorder("ON", lines)
    on.start()
    assert lines.events() == [levels.LEVELS_SETTING_EVENT, levels.LEVELS_ON_UNAVAILABLE_EVENT]
    assert lines.lines[1][1] == {"behaves_as": "RECORD", "mode": "ON", "reason": "NO_CONSUMER"}
    (fault,) = lines.faults
    assert fault["severity"] == "DEGRADED" and fault["kind"] == "OPTIONS_LEVELS_ON_UNAVAILABLE"
    provider = Provider(Cursor(window_script({"SPY": chain_rows("SPY")})))
    result = on.record_window(provider, session_date=SESSION, equity_cutoff=CUTOFF, window_id="w")
    assert result["mode"] == "ON" and result["applied"] is False and result["recorded"] == 1
    assert lines.named()["SPY"]["mode"] == "ON" and provider.transactions == [("app", {"read_only": True})]


# ---- failures -----------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("error, reason", [
    (RuntimeError("database is gone"), "RuntimeError"),
    (type("PoolError", (Exception,), {})("connection pool exhausted"), "NO_CONNECTION"),
])
def test_a_failed_transaction_is_one_failed_line_and_one_fault_and_never_raises(error, reason):
    lines = Lines()
    record = recorder("RECORD", lines)
    result = record.record_window(Provider(error=error), session_date=SESSION, equity_cutoff=CUTOFF, window_id="w")
    assert result["status"] == "FAILED" and result["reason"] == reason
    assert lines.events() == [levels.LEVELS_RECORD_FAILED_EVENT]
    assert lines.lines[0][1]["reason"] == reason and lines.lines[0][1]["applied"] is False
    (fault,) = lines.faults
    assert fault["kind"] == "OPTIONS_LEVELS_RECORD_FAILED" and fault["key"] == reason
    assert fault["severity"] == "DEGRADED" and fault["service"] == "options-scheduler"


def test_a_failed_captures_read_is_a_failed_window():
    lines = Lines()
    record = recorder("RECORD", lines)
    cursor = Cursor([("rt.option_breadth_captures", RuntimeError("canceling statement due to statement timeout"))])
    result = record.record_window(Provider(cursor), session_date=SESSION, equity_cutoff=CUTOFF, window_id="w")
    assert result["status"] == "FAILED" and lines.events() == [levels.LEVELS_RECORD_FAILED_EVENT]


def test_one_names_failed_read_rolls_back_to_its_savepoint_and_the_others_are_recorded():
    lines = Lines()
    record = recorder("RECORD", lines)
    cursor = Cursor(window_script({"SPY": chain_rows("SPY"), "QQQ": chain_rows("QQQ")},
                                  rows_error={"QQQ": RuntimeError("canceling statement due to statement timeout")}))
    result = record.record_window(Provider(cursor), session_date=SESSION, equity_cutoff=CUTOFF, window_id="w")
    assert ("ROLLBACK TO SAVEPOINT levels_name", ()) in cursor.statements
    assert result["status"] == "RECORDED" and result["recorded"] == 1 and result["failed"] == 1
    named = lines.named()
    assert named["SPY"]["available"] is True
    assert named["QQQ"]["available"] is False and named["QQQ"]["reason"] == "FAILED"
    assert "statement timeout" in named["QQQ"]["error"]
    assert lines.window()["failed"] == ["QQQ"]
    (fault,) = lines.faults
    assert fault["kind"] == "OPTIONS_LEVELS_NAME_FAILED" and fault["context"]["names"] == ["QQQ"]


def test_one_names_failed_computation_is_that_names_line_only():
    lines = Lines()
    real = levels.levels_document

    def flaky(chain):
        if chain.underlyer == "QQQ":
            raise FloatingPointError("bad chain")
        return real(chain)

    record = recorder("RECORD", lines, levels=flaky)
    result = record.record_window(Provider(Cursor(window_script({"SPY": chain_rows("SPY"), "QQQ": chain_rows("QQQ")}))),
                                  session_date=SESSION, equity_cutoff=CUTOFF, window_id="w")
    assert result["recorded"] == 1 and result["failed"] == 1
    assert lines.named()["QQQ"]["error"].startswith("COMPUTE: FloatingPointError")
    assert [fault["kind"] for fault in lines.faults] == ["OPTIONS_LEVELS_NAME_FAILED"]


def test_an_emit_that_raises_never_escapes():
    faults = []

    def broken(event, document):
        raise OSError("stderr closed")

    record = levels.OptionsLevelsRecorder("RECORD", emit=broken, fault=lambda **kwargs: faults.append(kwargs))
    record.start()  # the start line is best effort
    result = record.record_window(Provider(Cursor(window_script({"SPY": chain_rows("SPY")}))), session_date=SESSION,
                                  equity_cutoff=CUTOFF, window_id="w")
    assert result["status"] == "FAILED" and result["reason"] == "OSError"
    assert [fault["kind"] for fault in faults] == ["OPTIONS_LEVELS_RECORD_FAILED"]


# ---- lines --------------------------------------------------------------------------------------------------------------

def test_every_line_stays_under_the_docker_limit():
    names = ["AAPL", "AMD", "AMZN", "AVGO", "COIN", "GOOGL", "IWM", "MSFT", "MSTR", "MU", "NFLX", "NVDA", "PLTR",
             "QQQ", "SPY", "TSLA"]
    lines = Lines()
    record = recorder("RECORD", lines)
    record.record_window(Provider(Cursor(window_script({name: chain_rows(name) for name in names}))),
                         session_date=SESSION, equity_cutoff=CUTOFF, window_id="w")
    assert len(lines.named()) == 16 and lines.window()["recorded"] == 16
    huge = {"mode": "RECORD", "items": [{"c": "x" * 50, "n": index} for index in range(5_000)], "other": list(range(9))}
    line = levels.render_line(levels.LEVELS_RECORD_EVENT, huge)
    document = json.loads(line.split(" ", 1)[1])
    assert len(line.encode("utf-8")) <= levels.LINE_BYTE_LIMIT and document["truncated"] is True


def test_the_default_line_goes_to_stderr(capsys):
    levels._print_line(levels.LEVELS_RECORD_EVENT, {"mode": "RECORD"})
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == 'OPTIONS_LEVELS_RECORD {"mode":"RECORD"}\n'


# ---- the scheduler hook -----------------------------------------------------------------------------------------------

NOW = datetime(2026, 11, 17, 18, 31, 20, tzinfo=UTC)
RECEIPT = ("0f7f1d0e-8a51-4d55-9a3e-6c2b1d2f3a40", datetime(2026, 11, 17, 18, 30, tzinfo=UTC),
           NOW - timedelta(seconds=15), "a" * 64)
AUTHORIZATION_ID = UUID("5b0d8f8e-3f43-4a51-9a64-2f1f6f3c9b10")
COHORT_ID = UUID("8e2f4a1c-7c55-4d0e-a0f1-0b3c5d7e9f21")


def _attached_cycle(monkeypatch, *, script=(), **cycle_kwargs):
    """`_run_schedule_cycle` through to ATTACHED with every collaborator faked (no database, no provider); the same
    harness as the AL10 tests."""
    production = importlib.import_module("scripts.run_advanced_options_v3_production")
    acquisition = SimpleNamespace(
        authority=SimpleNamespace(acquisition_stop=NOW + timedelta(hours=2), authorization_id=AUTHORIZATION_ID,
                                  session_date=SESSION, cohort_id=COHORT_ID),
        contract_tickers=("O:SPY261127C00100000", "O:SPY261127P00100000"),
    )
    state = SimpleNamespace(
        load_package=lambda: {"authorization_reference": "ref", "cohort": {"underlyings": ["SPY"]}},
        load_bundle_document=lambda: {}, load_preopen_session_binding=lambda bundle: "session",
        load_downstream_bindings=lambda session: (), load_acquisition_binding=lambda: acquisition,
        attach_downstream_binding=lambda *args: None,
    )
    capture = SimpleNamespace(
        selection=SimpleNamespace(reviewed_at=NOW, selected=(), valid_until=NOW + timedelta(seconds=60),
                                  policy=SimpleNamespace(maximum_snapshot_age_seconds=60)),
        evidence={"selection_sha256": "b" * 64},
    )

    class Factory:
        production_binding_payload = {}

        def __init__(self, *args):
            pass

        def bind_continuous_selection(self, **kwargs):
            return SimpleNamespace(history=SimpleNamespace(binding_sha256="c" * 64))

        def persist_selection_cursor(self, *args, **kwargs):
            return None

    monkeypatch.setattr(scheduler, "PostgresContinuousSessionStateRepository", lambda *a, **k: state)
    monkeypatch.setattr(scheduler, "_scheduler_budget", lambda *a, **k: "budget")
    monkeypatch.setattr(scheduler, "_capture", lambda *a, **k: capture)
    monkeypatch.setattr(scheduler, "p8_options_cycle_cutoff_from_capture", lambda *a, **k: NOW + timedelta(seconds=200))
    monkeypatch.setattr(production, "_authorize_window", lambda *a, **k: {
        "window_id": "6c1b3c3a-58c2-4bd0-bd52-3b1f6b0f6c11", "window_cycle_authorization_sha256": "d" * 64,
        "publication_authorization_reference": "ref:PUBLICATION"})
    monkeypatch.setattr(scheduler, "load_advanced_options_equity_input_binding_cursor",
                        lambda *a, **k: SimpleNamespace(binding_sha256="e" * 64))
    monkeypatch.setattr(scheduler, "_P8ProductionCapture", Factory)
    monkeypatch.setattr(scheduler, "build_p8_downstream_binding", lambda **k: SimpleNamespace(sequence=7))
    monkeypatch.setattr(scheduler, "_capture_breadth", lambda *a, **k: {
        "SPY": {"state": "COMPLETE", "contracts": 1, "pages": 1, "seconds": 1.0}})
    cursor = Cursor([
        ("rt.advanced_options_continuous_sessions", [("cohort",)]),
        ("rt.cycle_receipts", [RECEIPT]),
        *script,
    ])
    provider = Provider(cursor)
    result = scheduler._run_schedule_cycle(
        provider, session_date=SESSION, api_key="k", clock=lambda: NOW, transport_factory=None,
        skipped_receipts=frozenset(), **cycle_kwargs,
    )
    return result, provider, cursor


def test_off_leaves_the_cycle_byte_identical(monkeypatch, capsys):
    baseline, base_provider, base_cursor = _attached_cycle(monkeypatch)
    assert baseline["status"] == "ATTACHED"
    off, off_provider, off_cursor = _attached_cycle(monkeypatch, levels=levels.start_options_levels("OFF"))
    assert json.dumps(off, sort_keys=True) == json.dumps(baseline, sort_keys=True)
    assert list(off) == list(baseline) and "levels" not in off
    assert off_cursor.statements == base_cursor.statements
    assert off_provider.transactions == base_provider.transactions
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == ""


def test_record_adds_one_key_and_one_read_only_transaction(monkeypatch):
    baseline, base_provider, base_cursor = _attached_cycle(monkeypatch)
    lines = Lines()
    observed = NOW + timedelta(seconds=60)
    script = window_script({"SPY": chain_rows("SPY", observed=observed)}, captures=[("SPY", "COMPLETE", observed)])
    result, provider, cursor = _attached_cycle(monkeypatch, script=script, levels=recorder("RECORD", lines))
    assert {key: value for key, value in result.items() if key != "levels"} == baseline
    assert result["levels"]["status"] == "RECORDED" and result["levels"]["recorded"] == 1
    assert provider.transactions[len(base_provider.transactions):] == [("app", {"read_only": True})]
    assert cursor.statements[:len(base_cursor.statements)] == base_cursor.statements
    window = lines.window()
    assert window["window_id"] == baseline["window_id"] and window["equity_cutoff"] == RECEIPT[1].isoformat()
    assert lines.named()["SPY"]["observed_at"] == observed.isoformat()


def test_levels_run_after_the_slot_rotation_and_both_keys_are_kept(monkeypatch):
    order = []

    class Rotation:
        def record_window(self, *args, **kwargs):
            order.append("slot_rotation")
            return {"status": "RECORDED"}

    class Levels:
        def record_window(self, provider, **kwargs):
            order.append("levels")
            assert set(kwargs) == {"session_date", "equity_cutoff", "window_id", "breadth"}
            assert kwargs["breadth"] == {"SPY": {"state": "COMPLETE", "contracts": 1, "pages": 1, "seconds": 1.0}}
            return {"status": "RECORDED"}

    result, _provider, _cursor = _attached_cycle(monkeypatch, slot_rotation=Rotation(), levels=Levels())
    assert order == ["slot_rotation", "levels"]
    assert result["slot_rotation"] == {"status": "RECORDED"} and result["levels"] == {"status": "RECORDED"}


def test_a_recorder_that_raises_never_fails_the_cycle(monkeypatch):
    class Broken:
        def record_window(self, *args, **kwargs):
            raise RuntimeError("unexpected")

    result, _provider, _cursor = _attached_cycle(monkeypatch, levels=Broken())
    assert result["status"] == "ATTACHED"
    assert result["levels"] == {"status": "FAILED", "reason": "RuntimeError: unexpected"}


def test_a_failing_read_is_a_failed_line_and_the_cycle_continues(monkeypatch):
    lines = Lines()
    script = [("rt.option_breadth_captures", RuntimeError("canceling statement due to statement timeout"))]
    result, _provider, _cursor = _attached_cycle(monkeypatch, script=script, levels=recorder("RECORD", lines))
    assert result["status"] == "ATTACHED" and result["levels"]["status"] == "FAILED"
    assert lines.events() == [levels.LEVELS_RECORD_FAILED_EVENT]
    assert lines.faults[0]["kind"] == "OPTIONS_LEVELS_RECORD_FAILED"


def test_the_public_cycle_passes_the_recorder_through(monkeypatch):
    seen = {}

    def fake_cycle(provider, **kwargs):
        seen.update(kwargs)
        return {"status": "ATTACHED"}

    monkeypatch.setattr(scheduler, "_run_schedule_cycle", fake_cycle)
    provider = Provider(Cursor([("pg_try_advisory_xact_lock", [(True,)])]))
    marker = object()
    scheduler.run_continuous_options_schedule_cycle(provider, session_date=SESSION, api_key="k", levels=marker)
    assert seen["levels"] is marker
    scheduler.run_continuous_options_schedule_cycle(provider, session_date=SESSION, api_key="k")
    assert seen["levels"] is None


# ---- the scheduler service --------------------------------------------------------------------------------------------

def _service(monkeypatch, value):
    script = importlib.import_module("scripts.run_advanced_options_v3_scheduler")
    stop = threading.Event()
    if value is None:
        monkeypatch.delenv(SWITCH, raising=False)
    else:
        monkeypatch.setenv(SWITCH, value)
    monkeypatch.delenv("REALTIME_V3_OPTIONS_SLOT_ROTATION", raising=False)
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    monkeypatch.setattr(script, "validate_target_database_v2", lambda *a, **k: None)
    pool = SimpleNamespace(closeall=lambda: None)
    monkeypatch.setattr(script, "_provider", lambda: (object(), pool))
    monkeypatch.setattr(script.signal, "signal", lambda *a: None)
    monkeypatch.setattr(script.threading, "Event", lambda: stop)
    monkeypatch.setattr(script, "AdvancedEquityLifecyclePlanner", lambda: SimpleNamespace(
        plan=lambda **_: SimpleNamespace(processing_session_date=SESSION)))
    calls = []

    def cycle(*_args, **kwargs):
        calls.append(kwargs)
        stop.set()
        return {"status": "WAITING_FOR_SESSION"}

    monkeypatch.setattr(script, "run_continuous_options_schedule_cycle", cycle)
    argv = ["--database", "stocks_realtime_v2", "--confirm-database-name", "stocks_realtime_v2",
            "--poll-seconds", "0.001", "--continuous", "--execute"]
    assert script.main(argv) == 0
    return calls


@pytest.mark.parametrize("value", [None, "", "OFF"])
def test_the_service_off_passes_no_recorder_and_logs_no_line(monkeypatch, capsys, value):
    calls = _service(monkeypatch, value)
    assert calls[0]["levels"] is None and calls[0]["slot_rotation"] is None
    assert "OPTIONS_LEVELS" not in capsys.readouterr().err


def test_the_service_in_record_starts_one_recorder(monkeypatch, capsys):
    calls = _service(monkeypatch, "RECORD")
    assert isinstance(calls[0]["levels"], levels.OptionsLevelsRecorder)
    assert calls[0]["levels"].mode is levels.LevelsMode.RECORD and calls[0]["slot_rotation"] is None
    err = capsys.readouterr().err
    assert err.count("OPTIONS_LEVELS_SETTING ") == 1 and "ON_UNAVAILABLE" not in err


def test_the_service_in_on_records_and_says_so(monkeypatch, capsys):
    calls = _service(monkeypatch, "on")
    assert calls[0]["levels"].mode is levels.LevelsMode.ON
    err = capsys.readouterr().err
    assert err.count("OPTIONS_LEVELS_SETTING ") == 1 and err.count("OPTIONS_LEVELS_ON_UNAVAILABLE ") == 1
    assert '"kind": "OPTIONS_LEVELS_ON_UNAVAILABLE"' in err  # the DEGRADED fault line
