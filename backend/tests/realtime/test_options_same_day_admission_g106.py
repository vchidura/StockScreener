"""G106 0DTE admission (Nov 18-19): the DTE-0 criterion's spread / strike-distance limits in the Options resolution.

`REALTIME_V3_OPTIONS_SAME_DAY_ADMISSION` (options-lifecycle, CAPACITY_SHARDS stage, around AL8 / D5 / AL11): OFF (default)
= today's resolution and allocation byte for byte; RECORD = today's stored, the limited resolution's allocation computed
and logged only; ON = the limited one used (today's on any failure or a per-name count change); invalid values refuse to
start."""
from __future__ import annotations

import io
import json
from collections import Counter
from contextlib import redirect_stderr
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from realtime import advanced_options_same_day_admission as admission_module
from realtime.advanced_options_activity_momentum_c6 import (
    LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    load_activity_momentum_profile,
    resolve_activity_momentum_selection,
    role_admission_limit_reason,
    same_day_admission_limit_reason,
)
from realtime.advanced_options_automatic_preparation import PostgresAutomaticOptionsPreparationExecutor
from realtime.advanced_options_same_day_admission import (
    SAME_DAY_ADMISSION_POLICY,
    SAME_DAY_ADMISSION_RECORD_EVENT,
    SAME_DAY_ADMISSION_RECORD_FAILED_EVENT,
    SAME_DAY_ADMISSION_SETTING,
    SameDayAdmissionMode,
    allocate_with_same_day_admission,
    parse_same_day_admission_mode,
    same_day_admission_mode,
)
from realtime.advanced_options_slot_selection import (
    SLOT_SELECTION_RECORD_EVENT,
    SLOT_SELECTION_SETTING,
    SlotSelectionMode,
)
from realtime.advanced_options_slot_side_balance import (
    SLOT_SIDE_BALANCE_RECORD_EVENT,
    SLOT_SIDE_BALANCE_SETTING,
    SlotSideBalanceMode,
)
from realtime.advanced_options_stock_same_day import (
    STOCK_SAME_DAY_RECORD_EVENT,
    STOCK_SAME_DAY_SETTING,
    StockSameDayMode,
)
from realtime.contracts import canonical_sha256
from tests.realtime.test_options_stock_same_day_d5 import (
    REVISION_ID,
    WEDNESDAY,
    _arguments,
    _executor,
    _expiry_calendar,
    _observed,
    _raw_capture,
    _resolution,
    _rows,
)

WIDE_BID, WIDE_ASK = 1.0, 1.6  # 46% relative spread, over the DTE-0 criterion's 20%


@pytest.fixture(scope="module")
def profile():
    return load_activity_momentum_profile(version=V4_ACTIVITY_MOMENTUM_PROFILE_VERSION)


# --- switch ---------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    (None, SameDayAdmissionMode.OFF), ("", SameDayAdmissionMode.OFF), ("  ", SameDayAdmissionMode.OFF),
    ("off", SameDayAdmissionMode.OFF), ("Record", SameDayAdmissionMode.RECORD), (" On ", SameDayAdmissionMode.ON),
    (SameDayAdmissionMode.ON, SameDayAdmissionMode.ON)])
def test_mode_values(raw, expected):
    assert parse_same_day_admission_mode(raw) is expected
    if not isinstance(raw, SameDayAdmissionMode):
        environ = {} if raw is None else {SAME_DAY_ADMISSION_SETTING: raw}
        assert same_day_admission_mode(environ) is expected


@pytest.mark.parametrize("raw", ["YES", "1", "true", "ACT", "RECORD_ONLY", "OFF,ON", 1, True])
def test_invalid_mode_raises_naming_the_key(raw):
    with pytest.raises(ValueError, match=SAME_DAY_ADMISSION_SETTING):
        parse_same_day_admission_mode(raw)
    with pytest.raises(ValueError, match=SAME_DAY_ADMISSION_SETTING):
        allocate_with_same_day_admission(mode=raw, profile=None, session_date=WEDNESDAY, run=None)


def test_setting_line(capsys):
    admission_module.log_same_day_admission_setting(SameDayAdmissionMode.RECORD)
    event, _, body = capsys.readouterr().err.strip().partition(" ")
    assert event == "OPTIONS_SAME_DAY_ADMISSION_SETTING"
    assert json.loads(body) == {"mode": "RECORD", "setting": SAME_DAY_ADMISSION_SETTING,
                                "policy_version": SAME_DAY_ADMISSION_POLICY["version"]}


# --- the c6 admission: the gap and the flag -------------------------------------------------------------------------

def _wide_same_day(session, *, near=2):
    """The fixture's DTE-0 rows within `near` strikes of spot get a 46% spread (the Nov 18 AAPL 357.5P shape)."""
    def adjust(row):
        if row.dte_for(session) == 0 and abs(row.strike - row.spot) <= near:
            return replace(row, bid=Decimal("1.00"), ask=Decimal("1.60"))
        return row
    return adjust


def test_the_gap_dte0_skips_the_role_limits_and_the_flag_applies_them(profile):
    rows = _rows(WEDNESDAY, adjust=_wide_same_day(WEDNESDAY))
    wide = {row.contract_ticker for row in rows
            if row.dte_for(WEDNESDAY) == 0 and row.relative_spread > Decimal("0.20")}
    assert wide
    off = _resolution(profile, WEDNESDAY, rows)
    admitted_off = {row.contract_ticker for row in off.shortlisted_candidates}
    assert wide & admitted_off, "today's chain admits a DTE-0 contract over the spread limit (the gap)"
    limited = resolve_activity_momentum_selection(
        profile, session_date=WEDNESDAY, candidates=rows, required_contract_tickers=(),
        preparation_at=_calendar_args()["preparation_at"],
        expiry_calendars=_calendar_args()["expiry_calendars"], same_day_admission_limits=True)
    admitted_on = {row.contract_ticker for row in limited.shortlisted_candidates}
    assert not wide & admitted_on
    # DTE 1+ keeps every contract it had (the candidate pool only gains room).
    today = f"{WEDNESDAY:%y%m%d}"
    assert {t for t in admitted_off if today not in t} <= {t for t in admitted_on if today not in t}
    delta = Counter(dict(limited.rejection_counts))
    delta.subtract(Counter(dict(off.rejection_counts)))
    assert delta["ROLE_SPREAD_LIMIT"] == len(wide)
    # Explicit False is today's resolution.
    explicit = resolve_activity_momentum_selection(
        profile, session_date=WEDNESDAY, candidates=rows, required_contract_tickers=(),
        preparation_at=_calendar_args()["preparation_at"],
        expiry_calendars=_calendar_args()["expiry_calendars"], same_day_admission_limits=False)
    assert explicit == off


def _calendar_args(session=WEDNESDAY):
    """The preparation clock and expiry calendars the D5 fixture's `_resolution` uses (built the same way)."""
    friday = session + timedelta(days=4 - session.weekday())
    rows = _rows(session)
    calendars = []
    for underlying in sorted({row.underlying for row in rows}):
        expiries = tuple(sorted({row.expiration for row in rows
                                 if row.underlying == underlying and (row.expiration - session).days <= 5}))
        calendars.append(_expiry_calendar(
            underlying, session_date=session, week_end_session_date=friday, expiries=expiries,
            last_trading_at=datetime(session.year, session.month, session.day, 21, 0, tzinfo=timezone.utc)))
    return {"preparation_at": _observed(session), "expiry_calendars": tuple(calendars)}


def test_flag_is_boolean_and_refused_by_the_legacy_profile(profile):
    rows = _rows(WEDNESDAY)
    args = _calendar_args()
    with pytest.raises(TypeError, match="same_day_admission_limits must be boolean"):
        resolve_activity_momentum_selection(profile, session_date=WEDNESDAY, candidates=rows, **args,
                                            same_day_admission_limits="ON")
    legacy = load_activity_momentum_profile(version=LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION)
    with pytest.raises(ValueError, match="legacy activity/momentum selection inputs changed"):
        resolve_activity_momentum_selection(legacy, session_date=WEDNESDAY, candidates=rows,
                                            same_day_admission_limits=True)


def test_limit_reason_helpers(profile):
    rows = _rows(WEDNESDAY)
    same_day = next(row for row in rows if row.dte_for(WEDNESDAY) == 0 and row.strike == row.spot)
    later = next(row for row in rows if row.dte_for(WEDNESDAY) == 2)
    criterion = next(c for c in profile.selection_criteria if c.minimum_dte <= 0 <= c.maximum_dte)
    assert same_day_admission_limit_reason(same_day, profile, WEDNESDAY) is None
    assert same_day_admission_limit_reason(replace(same_day, bid=Decimal("1.00"), ask=Decimal("1.60")), profile,
                                           WEDNESDAY) == "ROLE_SPREAD_LIMIT"
    assert same_day_admission_limit_reason(replace(same_day, spot=None), profile,
                                           WEDNESDAY) == "SPOT_UNAVAILABLE_FOR_STRIKE_FILTER"
    far = replace(same_day, spot=same_day.strike * Decimal("1.10"))
    assert same_day_admission_limit_reason(far, profile, WEDNESDAY) == "ROLE_STRIKE_DISTANCE_LIMIT"
    assert role_admission_limit_reason(far, criterion) == "ROLE_STRIKE_DISTANCE_LIMIT"
    # Not a same-day contract: never this rule's business.
    assert same_day_admission_limit_reason(replace(later, bid=Decimal("1.00"), ask=Decimal("1.60")), profile,
                                           WEDNESDAY) is None


# --- the helper -----------------------------------------------------------------------------------------------------

class _Run:
    """A fake `run(limits, sink)`: returns marked results and sends one sub-switch line to the sink."""

    def __init__(self, profile, *, fail_limited=False, short_limited=False, fail_today=False):
        self.profile, self.calls = profile, []
        self.fail_limited, self.short_limited, self.fail_today = fail_limited, short_limited, fail_today
        rows = _rows(WEDNESDAY, adjust=_wide_same_day(WEDNESDAY))
        self.off = _resolution(profile, WEDNESDAY, rows)
        args = _calendar_args()
        self.limited = resolve_activity_momentum_selection(profile, session_date=WEDNESDAY, candidates=rows,
                                                           **args, same_day_admission_limits=True)

    def __call__(self, limits, sink):
        from realtime.advanced_options_activity_momentum_c6 import allocate_activity_momentum
        self.calls.append((limits, sink is None))
        line = ("OPTIONS_SLOT_SIDE_BALANCE_RECORD", {"part": "summary", "limited": limits})
        if sink is None:
            admission_module._print_line(*line)
        else:
            sink(*line)
        if limits and self.fail_limited:
            raise RuntimeError("limited resolution failed")
        if not limits and self.fail_today:
            raise RuntimeError("today failed")
        resolution = self.limited if limits else self.off
        arguments = _arguments(WEDNESDAY, resolution)
        if limits and self.short_limited:
            arguments = {**arguments, "per_underlying_contract_limit": 20, "active_contract_budget": 40}
        return resolution, allocate_activity_momentum(self.profile, **arguments)


def _lines(text):
    out = []
    for line in text.splitlines():
        event, _, body = line.partition(" ")
        if event.startswith("OPTIONS_") and body.startswith("{"):
            out.append((event, json.loads(body)))
    return out


def _call(profile, mode, run):
    stderr = io.StringIO()
    with redirect_stderr(stderr):
        allocation = allocate_with_same_day_admission(mode=mode, profile=profile, session_date=WEDNESDAY, run=run)
    return allocation, _lines(stderr.getvalue())


def test_helper_off_is_one_plain_run_and_no_line(profile):
    run = _Run(profile)
    allocation, lines = _call(profile, SameDayAdmissionMode.OFF, run)
    assert run.calls == [(False, True)]
    assert [event for event, _doc in lines] == ["OPTIONS_SLOT_SIDE_BALANCE_RECORD"]
    from realtime.advanced_options_activity_momentum_c6 import allocate_activity_momentum
    assert allocation.allocation_sha256 == allocate_activity_momentum(
        profile, **_arguments(WEDNESDAY, run.off)).allocation_sha256


def test_helper_record_returns_today_and_logs_refusals_and_replacements(profile):
    run = _Run(profile)
    allocation, lines = _call(profile, SameDayAdmissionMode.RECORD, run)
    off_allocation = _Run(profile)(False, lambda *_: None)[1]
    assert allocation.allocation_sha256 == off_allocation.allocation_sha256
    assert run.calls == [(False, True), (True, False)]
    # Today's sub-switch line once; the limited run's lines are not printed.
    assert [doc["limited"] for event, doc in lines if event == "OPTIONS_SLOT_SIDE_BALANCE_RECORD"] == [False]
    record = [doc for event, doc in lines if event == SAME_DAY_ADMISSION_RECORD_EVENT]
    summary = record[0]
    assert summary["part"] == "summary" and summary["applied"] is False and summary["mode"] == "RECORD"
    assert summary["off_allocation_sha256"] == allocation.allocation_sha256
    assert summary["refused_watched"] >= 1 and summary["refused_reasons"].get("ROLE_SPREAD_LIMIT", 0) >= 1
    assert summary["would_add"] == summary["would_drop"] >= 1 and summary["per_name_counts_equal"] is True
    assert summary["lines"] == len(record)
    for doc in record[1:]:
        for contract in doc["refused_watched"]:
            assert Decimal(contract["spread"]) > Decimal("0.20") and contract["dte"] == 0
        assert len(json.dumps(doc)) < 15_000


def test_helper_record_never_fails_the_stage(profile):
    allocation, lines = _call(profile, SameDayAdmissionMode.RECORD, _Run(profile, fail_limited=True))
    assert [event for event, _doc in lines] == ["OPTIONS_SLOT_SIDE_BALANCE_RECORD",
                                                SAME_DAY_ADMISSION_RECORD_FAILED_EVENT]
    assert lines[1][1]["stage"] == "RECORD" and "limited resolution failed" in lines[1][1]["error"]


def test_helper_on_uses_the_limited_run_and_prints_its_lines_once(profile):
    run = _Run(profile)
    allocation, lines = _call(profile, SameDayAdmissionMode.ON, run)
    assert run.calls == [(False, False), (True, False)]
    assert allocation.allocation_sha256 == run(True, lambda *_: None)[1].allocation_sha256
    assert [doc["limited"] for event, doc in lines if event == "OPTIONS_SLOT_SIDE_BALANCE_RECORD"] == [True]
    record = [doc for event, doc in lines if event == SAME_DAY_ADMISSION_RECORD_EVENT]
    assert record[0]["applied"] is True and record[0]["rule_allocation_sha256"] == allocation.allocation_sha256
    for ticker in allocation.selected_contract_tickers:
        assert ticker not in {c["contract"] for doc in record[1:] for c in doc["refused_watched"]}


@pytest.mark.parametrize("failure", ["fail_limited", "short_limited"])
def test_helper_on_keeps_today_on_failure_with_todays_lines(profile, failure):
    run = _Run(profile, **{failure: True})
    allocation, lines = _call(profile, SameDayAdmissionMode.ON, run)
    assert allocation.allocation_sha256 == _Run(profile)(False, lambda *_: None)[1].allocation_sha256
    assert [(event, doc.get("limited")) for event, doc in lines] == [
        ("OPTIONS_SLOT_SIDE_BALANCE_RECORD", False), (SAME_DAY_ADMISSION_RECORD_FAILED_EVENT, None)]
    assert lines[1][1]["kept"] == "TODAY"
    if failure == "short_limited":
        assert "per-name contract counts changed" in lines[1][1]["error"]


def test_helper_on_today_failing_fails_as_today_with_its_lines(profile):
    with pytest.raises(RuntimeError, match="today failed"):
        _call(profile, SameDayAdmissionMode.ON, _Run(profile, fail_today=True))


def test_helper_needs_the_v4_profile(profile):
    v3 = load_activity_momentum_profile(version=SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION)
    for mode in ("RECORD", "ON"):
        with pytest.raises(ValueError, match="v4 admission profile"):
            allocate_with_same_day_admission(mode=mode, profile=v3, session_date=WEDNESDAY, run=_Run(profile))


# --- the executor: _allocate_capture end to end, with AL8 / D5 / AL11 -----------------------------------------------

def _wide_capture():
    """The D5 Wednesday capture (SPY daily, AAPL Mon / Wed / Fri) with the DTE-0 contracts within 2% of spot quoted
    46% wide: today's admission takes them, the DTE-0 criterion refuses them."""
    capture = _raw_capture()
    for ticker, raw in capture["selected_snapshots"].items():
        details = raw["details"]
        spot = raw["underlying_asset"]["price"]
        if (details["expiration_date"] == WEDNESDAY.isoformat()
                and abs(details["strike_price"] - spot) / spot <= 0.02 + 1e-9):
            raw["last_quote"] = {**raw["last_quote"], "bid": WIDE_BID, "ask": WIDE_ASK}
    return capture


def _run_capture(capture, **attributes):
    executor = _executor(capture, **attributes)
    claim = SimpleNamespace(revision_id=REVISION_ID, session_date=WEDNESDAY, option_underlyings=("AAPL", "SPY"))
    stderr = io.StringIO()
    with redirect_stderr(stderr):
        filtered, allocation = executor._allocate_capture(claim, json.loads(json.dumps(capture)))
    return filtered, allocation, _lines(stderr.getvalue()), executor._provider.previous_reads


def _wide(capture):
    return {ticker for ticker, raw in capture["selected_snapshots"].items()
            if raw["last_quote"]["bid"] == WIDE_BID and raw["last_quote"]["ask"] == WIDE_ASK}


@pytest.fixture(scope="module")
def executor_runs():
    capture = _wide_capture()
    plans = (
        ("legacy", {}),
        ("OFF", {"_same_day_admission": SameDayAdmissionMode.OFF}),
        ("RECORD", {"_same_day_admission": SameDayAdmissionMode.RECORD}),
        ("ON", {"_same_day_admission": SameDayAdmissionMode.ON}),
        ("AL8REC", {"_slot_side_balance": SlotSideBalanceMode.RECORD}),
        ("RECORD+AL8REC", {"_same_day_admission": SameDayAdmissionMode.RECORD,
                           "_slot_side_balance": SlotSideBalanceMode.RECORD}),
        ("ON+AL8REC", {"_same_day_admission": SameDayAdmissionMode.ON,
                       "_slot_side_balance": SlotSideBalanceMode.RECORD}),
        ("AL11ON+D5ON", {"_slot_selection": SlotSelectionMode.ON, "_stock_same_day": StockSameDayMode.ON}),
        ("RECORD+AL11ON+D5ON", {"_same_day_admission": SameDayAdmissionMode.RECORD,
                                "_slot_selection": SlotSelectionMode.ON, "_stock_same_day": StockSameDayMode.ON}),
        ("ON+AL11ON+D5ON", {"_same_day_admission": SameDayAdmissionMode.ON, "_slot_selection": SlotSelectionMode.ON,
                            "_stock_same_day": StockSameDayMode.ON}),
        ("AL11REC+D5REC", {"_slot_selection": SlotSelectionMode.RECORD, "_stock_same_day": StockSameDayMode.RECORD}),
        ("RECORD+AL11REC+D5REC", {"_same_day_admission": SameDayAdmissionMode.RECORD,
                                  "_slot_selection": SlotSelectionMode.RECORD,
                                  "_stock_same_day": StockSameDayMode.RECORD}),
    )
    return capture, {label: _run_capture(capture, **attributes) for label, attributes in plans}


def test_executor_the_fixture_shows_the_gap(executor_runs):
    capture, runs = executor_runs
    _f, legacy, _lines_, _reads = runs["legacy"]
    assert _wide(capture) & set(legacy.selected_contract_tickers)


@pytest.mark.parametrize("off, record", [("legacy", "OFF"), ("legacy", "RECORD"), ("AL8REC", "RECORD+AL8REC"),
                                         ("AL11ON+D5ON", "RECORD+AL11ON+D5ON"),
                                         ("AL11REC+D5REC", "RECORD+AL11REC+D5REC")])
def test_executor_off_and_record_are_byte_identical_with_the_same_sub_switch_lines(executor_runs, off, record):
    _capture, runs = executor_runs
    off_filtered, off_allocation, off_lines, off_reads = runs[off]
    filtered, allocation, lines, reads = runs[record]
    assert allocation.allocation_sha256 == off_allocation.allocation_sha256
    assert canonical_sha256(filtered) == canonical_sha256(off_filtered)
    assert reads == off_reads  # the previous-session chain is read once (AL11) or not at all
    assert [line for line in lines if line[0] != SAME_DAY_ADMISSION_RECORD_EVENT] == off_lines
    if record == "OFF":
        assert lines == off_lines == []
    else:
        assert lines[-1][0] == SAME_DAY_ADMISSION_RECORD_EVENT or any(
            event == SAME_DAY_ADMISSION_RECORD_EVENT for event, _doc in lines)


def test_executor_record_lines(executor_runs):
    capture, runs = executor_runs
    _f, legacy, _l, _r = runs["legacy"]
    lines = [doc for event, doc in runs["RECORD"][2] if event == SAME_DAY_ADMISSION_RECORD_EVENT]
    summary = lines[0]
    watched_wide = _wide(capture) & set(legacy.selected_contract_tickers)
    assert summary["refused_watched"] == len(watched_wide)
    assert {c["contract"] for doc in lines[1:] for c in doc["refused_watched"]} == watched_wide
    assert summary["would_drop"] >= len(watched_wide) and summary["per_name_counts_equal"] is True
    assert summary["same_day"]["off"] >= summary["same_day"]["rule"]
    assert summary["contracts"] == {"off": len(legacy.selected_contract_tickers),
                                    "rule": len(legacy.selected_contract_tickers)}


@pytest.mark.parametrize("label, base", [("ON", "legacy"), ("ON+AL8REC", "AL8REC"),
                                         ("ON+AL11ON+D5ON", "AL11ON+D5ON")])
def test_executor_on_drops_the_refused_and_keeps_every_count(executor_runs, label, base):
    capture, runs = executor_runs
    _bf, base_allocation, base_lines, base_reads = runs[base]
    filtered, allocation, lines, reads = runs[label]
    selected = set(allocation.selected_contract_tickers)
    assert not _wide(capture) & selected
    assert _wide(capture) & set(base_allocation.selected_contract_tickers)
    assert Counter(t[2:5] for t in selected) == Counter(t[2:5] for t in base_allocation.selected_contract_tickers)
    assert set(filtered["selected_snapshots"]) == selected
    assert filtered["activity_momentum_allocation"]["allocation_sha256"] == allocation.allocation_sha256
    assert reads == base_reads
    assert not any(event == SAME_DAY_ADMISSION_RECORD_FAILED_EVENT for event, _doc in lines)
    # Each sub-switch prints its lines once (the used run's).
    for event in (SLOT_SIDE_BALANCE_RECORD_EVENT, STOCK_SAME_DAY_RECORD_EVENT, SLOT_SELECTION_RECORD_EVENT):
        assert sum(1 for e, doc in lines if e == event and doc.get("part") == "summary") == sum(
            1 for e, doc in base_lines if e == event and doc.get("part") == "summary")
    record = [doc for event, doc in lines if event == SAME_DAY_ADMISSION_RECORD_EVENT]
    assert record[0]["applied"] is True and record[0]["rule_allocation_sha256"] == allocation.allocation_sha256


def test_executor_al11_on_never_brings_a_refused_contract_back(executor_runs):
    capture, runs = executor_runs
    _f, allocation, lines, reads = runs["ON+AL11ON+D5ON"]
    al11 = [doc for event, doc in lines if event == SLOT_SELECTION_RECORD_EVENT and doc.get("part") == "underlying"]
    assert al11 and reads == 1
    assert not _wide(capture) & set(allocation.selected_contract_tickers)


def test_executor_on_keeps_today_when_the_limited_run_fails(monkeypatch):
    from realtime import advanced_options_automatic_preparation as preparation

    capture = _wide_capture()
    _f, today, today_lines, _r = _run_capture(capture, _slot_side_balance=SlotSideBalanceMode.RECORD)
    real = preparation.resolve_activity_momentum_selection

    def failing(*args, **kwargs):
        if kwargs.get("same_day_admission_limits"):
            raise RuntimeError("limited resolution failed")
        return real(*args, **kwargs)

    monkeypatch.setattr(preparation, "resolve_activity_momentum_selection", failing)
    filtered, allocation, lines, _reads = _run_capture(capture, _same_day_admission=SameDayAdmissionMode.ON,
                                                       _slot_side_balance=SlotSideBalanceMode.RECORD)
    assert allocation.allocation_sha256 == today.allocation_sha256
    assert lines[:-1] == today_lines
    assert lines[-1][0] == SAME_DAY_ADMISSION_RECORD_FAILED_EVENT and lines[-1][1]["kept"] == "TODAY"


def test_allocate_capture_routes_through_the_switch():
    import inspect

    from realtime import advanced_options_automatic_preparation as preparation

    source = inspect.getsource(preparation.PostgresAutomaticOptionsPreparationExecutor._allocate_capture)
    assert '"_same_day_admission"' in source and "SameDayAdmissionMode.OFF" in source
    assert "if same_day_admission is SameDayAdmissionMode.OFF:" in source
    assert source.index("def allocate_resolved(") < source.index("allocate_with_side_balance(") < source.index(
        "self._slot_selection_allocation(") < source.index("self._same_day_admission_allocation(")


# --- service: constructor and lifecycle main ------------------------------------------------------------------------

def test_executor_refuses_an_invalid_value_and_a_non_v4_profile():
    with pytest.raises(ValueError, match=SAME_DAY_ADMISSION_SETTING):
        PostgresAutomaticOptionsPreparationExecutor(
            None, api_key="k", backend_image_id="i", admission_profile_version=V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
            same_day_admission="MAYBE")
    for mode in ("RECORD", "ON"):
        with pytest.raises(ValueError, match=f"{SAME_DAY_ADMISSION_SETTING} requires the v4"):
            PostgresAutomaticOptionsPreparationExecutor(
                None, api_key="k", backend_image_id="i",
                admission_profile_version=SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION, same_day_admission=mode)
    with pytest.raises(ValueError, match=r"configuration is invalid$"):
        PostgresAutomaticOptionsPreparationExecutor(
            None, api_key="k", backend_image_id="i", admission_profile_version=V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
            same_day_admission="ON")


class _Constructed(Exception):
    pass


def _lifecycle(monkeypatch, value):
    from scripts import run_advanced_options_v3_lifecycle as resident

    seen = {}

    def executor(*_args, **kwargs):
        seen.update(kwargs)
        raise _Constructed

    monkeypatch.setenv("POLYGON_API_KEY", "offline-fixture")
    monkeypatch.setenv("REALTIME_V3_OPTIONS_ADMISSION_PROFILE", V4_ACTIVITY_MOMENTUM_PROFILE_VERSION)
    for key in (SLOT_SIDE_BALANCE_SETTING, SLOT_SELECTION_SETTING, STOCK_SAME_DAY_SETTING):
        monkeypatch.delenv(key, raising=False)
    if value is None:
        monkeypatch.delenv(SAME_DAY_ADMISSION_SETTING, raising=False)
    else:
        monkeypatch.setenv(SAME_DAY_ADMISSION_SETTING, value)
    monkeypatch.setattr(resident, "dotenv_values", lambda *_: {})
    monkeypatch.setattr(resident, "_provider", lambda: (SimpleNamespace(), SimpleNamespace(closeall=lambda: None)))
    monkeypatch.setattr(resident, "PostgresAutomaticOptionsPreparationExecutor", executor)
    args = ["--database", "stocks_realtime_v2", "--confirm-database-name", "stocks_realtime_v2", "--continuous",
            "--execute"]
    return resident, seen, args


@pytest.mark.parametrize("value, expected", [
    (None, SameDayAdmissionMode.OFF), ("", SameDayAdmissionMode.OFF), ("record", SameDayAdmissionMode.RECORD),
    (" ON ", SameDayAdmissionMode.ON)])
def test_lifecycle_service_passes_the_switch(monkeypatch, value, expected):
    resident, seen, args = _lifecycle(monkeypatch, value)
    with pytest.raises(_Constructed):
        resident.main(args)
    assert seen["same_day_admission"] is expected
    assert seen["stock_same_day"] is StockSameDayMode.OFF


def test_lifecycle_service_refuses_to_start_on_an_invalid_value(monkeypatch):
    resident, seen, args = _lifecycle(monkeypatch, "LIMITS")
    with pytest.raises(ValueError, match=SAME_DAY_ADMISSION_SETTING):
        resident.main(args)
    assert seen == {}
