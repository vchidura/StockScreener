"""G102 R83-0 (Nov 17): REALTIME_V2_EQUITY_INTRADAY_HISTORY_SESSIONS - the Equity window carries a shorter
1m/5m/15m history.

OFF (unset): no depth reaches the loader or the projection, the statements and parameters are today's, and the
pinned golden fingerprint of the nine-name fixture is unchanged (test_ticker_projection_golden.py).
ON: the loader keeps only the depth's 1m/5m/15m sessions and closes the lineage of the re-verified derived bars only;
the projection charts the bounded intervals and computes the same screening rows, setups, Pattern Watch, evidence
and channels as the full history (the golden digests), with different lineage heads and input digests.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import replace
from datetime import date, timedelta
from types import SimpleNamespace
from uuid import UUID

import exchange_calendars
import pytest

import realtime.ticker_repository as ticker_repository
from realtime.contracts import canonical_sha256
from realtime.equity_intraday_history import (
    BAR_DEPTH_PREDICATE, INTRADAY_HISTORY_SWITCH, BoundedIntradayCounts, IntradayHistoryDepth,
    intraday_history_depth, parse_intraday_history_depth, resolve_intraday_history_window,
)
from realtime.equity_portal_history import build_realtime_equity_portal_history_plan
from realtime.screening_delta_loader import ScreeningInputLoader
from realtime.screening_projection import SOURCE_LINEAGE_LOADED, STORED_LINEAGE_ACCEPTED
from realtime.screening_repository import load_realtime_screening_input
from realtime.ticker_projection import _intraday_expected_count, project_realtime_ticker_workspace
from tests.realtime.test_screening_delta_loader import (
    ADJ, MINUTE, OPEN, PREVIOUS_OPEN, RAW, SESSION, FakeCursor, FakeDatabase, outcome, seeded,
)
from tests.realtime.test_ticker_projection_golden import GOLDEN, fingerprint
from tests.realtime.test_ticker_vertical_slice import (
    _corrupt_stream_five, advanced_next_session_fixture, ticker_fixture,
)

DEPTH = IntradayHistoryDepth(2, 10, 22)
CALENDAR = exchange_calendars.get_calendar("XNYS")


# --- the switch --------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("value", [None, "", "  ", "OFF", "off", "FULL", " full "])
def test_unset_and_off_values_mean_the_full_history(value):
    assert parse_intraday_history_depth(value) is None
    environ = {} if value is None else {INTRADAY_HISTORY_SWITCH: value}
    assert intraday_history_depth(environ) is None


@pytest.mark.parametrize("value, expected", [
    ("1m:2,5m:10,15m:22", (2, 10, 22)),
    (" 15m:20 , 1m:1 ,5M:6 ", (1, 6, 20)),
    ("1m:5,5m:90,15m:90", (5, 90, 90)),
    ("1m:1,5m:6,15m:14", (1, 6, 14)),
])
def test_a_depth_names_each_bounded_interval_once(value, expected):
    depth = parse_intraday_history_depth(value)
    assert (depth.one_minute, depth.five_minute, depth.fifteen_minute) == expected
    assert intraday_history_depth({INTRADAY_HISTORY_SWITCH: value}) == depth
    assert parse_intraday_history_depth(depth.label) == depth


@pytest.mark.parametrize("value", [
    "ON", "2,10,22", "1m:2,5m:10", "1m:2,5m:10,15m:22,30m:20", "1m:2,1m:3,5m:10,15m:22", "1m:x,5m:10,15m:22",
    "1m:-1,5m:10,15m:22", "1m:0,5m:10,15m:22", "1m:6,5m:10,15m:22", "1m:2,5m:5,15m:22", "1m:2,5m:91,15m:22",
    "1m:2,5m:10,15m:13", "1m:2;5m:10;15m:22", "1m=2,5m=10,15m=22",
])
def test_an_invalid_value_is_rejected_by_the_parser_and_ignored_by_the_switch(value, caplog):
    with pytest.raises(ValueError):
        parse_intraday_history_depth(value)
    with caplog.at_level(logging.WARNING):
        assert intraday_history_depth({INTRADAY_HISTORY_SWITCH: value}) is None
    assert INTRADAY_HISTORY_SWITCH in caplog.text


def test_the_switch_reads_the_process_environment(monkeypatch):
    monkeypatch.delenv(INTRADAY_HISTORY_SWITCH, raising=False)
    assert intraday_history_depth() is None
    monkeypatch.setenv(INTRADAY_HISTORY_SWITCH, "1m:2,5m:10,15m:22")
    assert intraday_history_depth() == DEPTH


# --- the resolved window ---------------------------------------------------------------------------------------

def test_the_window_counts_sessions_back_from_the_window_session_inclusive():
    session = date(2026, 11, 13)
    window = resolve_intraday_history_window(session, DEPTH)
    assert window.one_minute_sessions == (date(2026, 11, 12), session)
    assert len(window.five_minute_sessions) == 10 and window.five_minute_sessions[-1] == session
    assert window.five_minute_sessions[0] == date(2026, 11, 2)
    assert len(window.fifteen_minute_sessions) == 22 and window.fifteen_minute_sessions[-1] == session
    assert window.params == (date(2026, 11, 12), date(2026, 11, 2), window.fifteen_minute_sessions[0])
    plan = build_realtime_equity_portal_history_plan(session)
    for interval in ("1m", "5m", "15m"):
        assert window.sessions(interval) == plan.raw_sessions[-DEPTH.sessions(interval):]
    with pytest.raises(ValueError):
        resolve_intraday_history_window(date(2026, 11, 14), DEPTH)  # a Saturday


@pytest.mark.parametrize("interval, basis, session, root", [
    ("1m", RAW, date(2026, 10, 1), True),     # native
    ("5m", RAW, date(2026, 11, 12), True),    # inside the 1m sessions: re-derived from its minutes
    ("5m", RAW, date(2026, 11, 11), False),   # older: its minutes are not loaded
    ("15m", RAW, date(2026, 11, 2), True),    # inside the 5m sessions
    ("30m", RAW, date(2026, 10, 30), False),
    ("1h", RAW, date(2026, 11, 2), True),
    ("1h", RAW, date(2026, 7, 1), False),
    ("1d", RAW, date(2026, 10, 30), False),
    ("1d", RAW, date(2026, 11, 3), True),
    ("1d", ADJ, date(2025, 11, 3), True),     # adjusted daily, weekly and monthly lineage as today
    ("1wk", ADJ, date(2022, 1, 7), True),
])
def test_closure_roots_are_the_derived_bars_inside_the_verified_depth(interval, basis, session, root):
    window = resolve_intraday_history_window(date(2026, 11, 13), DEPTH)
    assert window.closure_root(dict(interval=interval, adjustment_basis=basis, session_date=session)) is root


@pytest.mark.parametrize("cutoff_hour, cutoff_minute", [(21, 0), (14, 35), (19, 15)])
def test_bounded_counts_match_the_plan_formula_over_fewer_sessions(cutoff_hour, cutoff_minute):
    from datetime import datetime, timezone

    session = date(2026, 10, 15)
    cutoff = datetime(2026, 10, 15, cutoff_hour, cutoff_minute, tzinfo=timezone.utc)
    plan = build_realtime_equity_portal_history_plan(session)
    counts = BoundedIntradayCounts(plan, resolve_intraday_history_window(session, DEPTH))
    elapsed = {(21, 0): 390, (14, 35): 65, (19, 15): 345}[(cutoff_hour, cutoff_minute)]  # open 13:30 UTC
    expected = {
        "1m": 390 * 1 + elapsed, "5m": 78 * 9 + elapsed // 5, "15m": 26 * 21 + elapsed // 15,
        "30m": _intraday_expected_count(CALENDAR, plan, session, cutoff, "30m"),
        "1h": _intraday_expected_count(CALENDAR, plan, session, cutoff, "1h"),
    }
    assert {interval: _intraday_expected_count(CALENDAR, counts, session, cutoff, interval)
            for interval in expected} == expected
    assert counts.expected_count("1d", "ADJUSTED") == plan.expected_count("1d", "ADJUSTED")
    with pytest.raises(ValueError):
        BoundedIntradayCounts(build_realtime_equity_portal_history_plan(date(2026, 10, 14)),
                              resolve_intraday_history_window(session, DEPTH))


# --- the loader ----------------------------------------------------------------------------------------------------

LOADER_DEPTH = IntradayHistoryDepth(2, 6, 14)
SESSIONS = tuple(stamp.date() for stamp in CALENDAR.sessions_in_range("2026-08-20", str(SESSION)))


class DepthCursor(FakeCursor):
    """The fake database with the R83-0 predicate: three more parameters after the portal-history window."""

    def _depth(self, sql, params, size):
        if BAR_DEPTH_PREDICATE not in sql:
            return params, None
        assert sql.count(BAR_DEPTH_PREDICATE) == (2 if " AS visible_total" in sql or "counted.visible_total" in sql
                                                   else 1)
        return params, size + 3

    def _candidates(self, params, portal):
        if len(params) == 1 + 5 + 13:
            starts = dict(zip(("1m", "5m", "15m"), params[-3:]))
            return [row for row in super()._candidates(params[:-3], portal)
                    if not (row["adjustment_basis"] == RAW and row["interval"] in starts
                            and row["session_date"] < starts[row["interval"]])]
        return super()._candidates(params, portal)

    def _winners(self, sql, params):
        if BAR_DEPTH_PREDICATE not in sql:
            return super()._winners(sql, params)
        size = 1 + 5 + 13
        with_state = " AS visible_total" in sql
        assert len(params) == size * (2 if with_state else 1)
        candidates = self._candidates(params[:size], True)
        from tests.realtime.test_screening_delta_loader import order_key, rank_of
        best = {}
        for row in candidates:
            key = (row["ticker"], row["interval"], row["adjustment_basis"], row["bar_start"])
            if key not in best or order_key(row, True) < order_key(best[key], True):
                best[key] = row
        out = []
        for row in sorted(best.values(), key=lambda row: (self.db.collation(row), row["bar_start"])):
            item = self._project(row, True)
            if with_state:
                item["src_rank"] = rank_of(row, True)
                item["visible_total"] = len(candidates)
            out.append(item)
        return out

    _winners_state = _winners

    def _delta(self, sql, params):
        if BAR_DEPTH_PREDICATE not in sql:
            return super()._delta(sql, params)
        from tests.realtime.test_screening_delta_loader import BASE_COLUMNS, PORTAL_COLUMNS, rank_of, visible_at
        size = 1 + 5 + 13
        assert len(params) == 2 * size + 5 and params[:size] == params[size:2 * size]
        previous_cutoff, previous_known_by = self._clocks(params[2 * size:])
        current = self._candidates(params[:size], True)
        new = [row for row in current if not visible_at(row, previous_cutoff, previous_known_by)]
        if not new:
            empty = {name: None for name in (*BASE_COLUMNS, *PORTAL_COLUMNS, "created_at")}
            return [{"visible_total": len(current), **empty, "src_rank": None}]
        return [{"visible_total": len(current), **self._project(row, True), "src_rank": rank_of(row, True)}
                for row in new]


class DepthDatabase(FakeDatabase):
    def cursor(self):
        return DepthCursor(self)


def session_open(session):
    return CALENDAR.session_open(str(session)).to_pydatetime()


@pytest.fixture
def history():
    """Per ticker: 1m, 5m and 15m bars on every session of the last ~two months, a stream-like 5m bar derived from
    two 1m bars and a 30m bar derived from 5m bars on each session (superseded 5m sources included), plus today."""
    database = DepthDatabase()
    seeded(database)
    for ticker in database.tickers:
        for session in SESSIONS[:-1]:
            start = session_open(session)
            minutes = [database.add(ticker=ticker, interval="1m", start=start + index * MINUTE, session=session)
                       for index in range(2)]
            five = database.add(ticker=ticker, interval="5m", start=start + 60 * MINUTE, session=session,
                                kind="DERIVED", sources=[row["bar_revision_id"] for row in minutes])
            loser = database.add(ticker=ticker, interval="5m", start=start + 70 * MINUTE, session=session,
                                 kind="DERIVED", observed=start + 71 * MINUTE)
            winner = database.add(ticker=ticker, interval="5m", start=start + 70 * MINUTE, session=session,
                                  kind="NATIVE_REST", observed=start + 80 * MINUTE)
            database.add(ticker=ticker, interval="15m", start=start + 90 * MINUTE, session=session)
            database.add(ticker=ticker, interval="30m", start=start + 60 * MINUTE, session=session, kind="DERIVED",
                         sources=[five["bar_revision_id"], loser["bar_revision_id"], winner["bar_revision_id"]])
    return database


def depth_scope(database, cutoff, depth=LOADER_DEPTH, **kw):
    return dict(database.scope(cutoff, **kw), intraday_depth=depth)


def test_off_issues_today_statements_and_parameters(history):
    plain, explicit = history.cursor(), history.cursor()
    first = load_realtime_screening_input(plain, **history.scope(OPEN + 30 * MINUTE))
    second = load_realtime_screening_input(explicit, **history.scope(OPEN + 30 * MINUTE), intraday_depth=None)
    assert plain.statements == explicit.statements and plain.params == explicit.params
    assert all(BAR_DEPTH_PREDICATE not in sql for sql in plain.statements)
    assert first == second and first.source_lineage == SOURCE_LINEAGE_LOADED
    loader = ScreeningInputLoader()
    cursor = history.cursor()
    assert loader.load(cursor, **history.scope(OPEN + 30 * MINUTE), intraday_depth=None) == first
    assert all(BAR_DEPTH_PREDICATE not in sql for sql in cursor.statements)


def test_on_keeps_the_depth_sessions_and_closes_only_the_reverified_lineage(history):
    cutoff = OPEN + 30 * MINUTE
    full = load_realtime_screening_input(history.cursor(), **history.scope(cutoff))
    cursor = history.cursor()
    bounded = load_realtime_screening_input(cursor, **depth_scope(history, cutoff))
    window = resolve_intraday_history_window(SESSION, LOADER_DEPTH)
    assert bounded.source_lineage == STORED_LINEAGE_ACCEPTED
    assert any(BAR_DEPTH_PREDICATE in sql for sql in cursor.statements)
    by_id = {row["bar_revision_id"]: row for row in history.rows}
    kept = {row["bar_revision_id"] for row in bounded.bars}
    winners = {row["bar_revision_id"] for row in full.bars if row["interval"] in ("1m", "5m", "15m", "30m")}
    for revision in winners:
        row = by_id[revision]
        inside = row["interval"] == "30m" or row["session_date"] >= window.start(row["interval"])
        if inside:
            assert revision in kept, (row["interval"], row["session_date"])
    # Closure rows are same-session sources of roots inside the depth, so nothing older is read.
    assert all(row["session_date"] >= window.start(row["interval"])
               for row in bounded.bars if row["interval"] in ("1m", "5m", "15m"))
    # The 30m bars inside the 5m sessions bring their superseded 5m source (a closure row); older ones do not.
    losers = [row for row in history.rows if row["interval"] == "5m" and row["source_kind"] == "DERIVED"
              and not row["source_revision_ids"]]
    for loser in losers:
        assert (loser["bar_revision_id"] in kept) is (loser["session_date"] >= window.start("5m"))
    # The 5m bars derived from minutes keep their minutes only inside the 1m sessions.
    streamed = [row for row in history.rows if row["interval"] == "5m" and row["source_revision_ids"]]
    for five in streamed:
        minutes = {str(source) for source in five["source_revision_ids"]}
        if five["session_date"] >= window.start("1m"):
            assert minutes <= kept
        elif five["session_date"] >= window.start("5m"):
            assert five["bar_revision_id"] in kept and not minutes & kept
    assert len(bounded.bars) < len(full.bars)
    # Everything outside the bounded intervals is the full load's.
    def other(result):
        return [row for row in result.bars if row["interval"] not in ("1m", "5m", "15m")]
    assert other(bounded) == other(full)


def test_on_the_delta_loader_equals_the_bounded_full_load_across_cutoffs(history):
    loader = ScreeningInputLoader()
    for minutes in (30, 31, 45, 90, 200, 385):  # before the close (the close rolls the daily window)
        cutoff = OPEN + minutes * MINUTE
        if minutes == 45:
            history.add(ticker="AAPL", start=OPEN + 40 * MINUTE)
        if minutes == 90:
            first = history.add(ticker="MSFT", interval="1m", start=OPEN + 60 * MINUTE)
            history.add(ticker="MSFT", start=OPEN + 60 * MINUTE, kind="DERIVED", sources=[first["bar_revision_id"]],
                        observed=OPEN + 89 * MINUTE)
        cursor = history.cursor()
        delta = outcome(lambda: loader.load(cursor, **depth_scope(history, cutoff)))
        full = outcome(lambda: load_realtime_screening_input(history.cursor(), **depth_scope(history, cutoff)))
        assert delta == full
        assert delta[0] == "ok"
        if minutes > 30:
            assert loader.last_report.mode == "delta", (minutes, loader.last_report)
    for state in loader._entries.values():
        state.check_invariants()


def test_switching_the_depth_is_a_full_reload_of_another_identity(history):
    loader = ScreeningInputLoader()
    cutoff = OPEN + 30 * MINUTE
    loader.load(history.cursor(), **history.scope(cutoff))
    loader.load(history.cursor(), **depth_scope(history, cutoff))
    assert (loader.last_report.mode, loader.last_report.reason) == ("full", "WINDOW_CHANGED")
    loader.load(history.cursor(), **history.scope(cutoff))
    assert (loader.last_report.mode, loader.last_report.reason) == ("full", "WINDOW_CHANGED")


def test_a_depth_bounds_only_the_portal_history_load(history):
    with pytest.raises(ValueError, match="portal-history"):
        load_realtime_screening_input(history.cursor(), **depth_scope(history, OPEN + 30 * MINUTE, portal=False))


# --- the projection --------------------------------------------------------------------------------------------------

def loaded_with(inputs, depth):
    """What the bounded load returns for a fixture whose sources are all winners: the rows outside the depth's
    1m/5m/15m sessions are not read and no closure row is added."""
    window = resolve_intraday_history_window(inputs.session, depth)
    return replace(inputs, source_lineage=STORED_LINEAGE_ACCEPTED, bars=tuple(
        row for row in inputs.bars
        if not (row["adjustment_basis"] == RAW and row["interval"] in ("1m", "5m", "15m")
                and row["session_date"] < window.start(row["interval"]))
    ))


@pytest.mark.parametrize("depth", [DEPTH, IntradayHistoryDepth(1, 6, 14)])
def test_on_projection_keeps_every_analysis_digest_of_the_golden_fixture(depth):
    inputs = loaded_with(ticker_fixture(), depth)
    projection = project_realtime_ticker_workspace(inputs, intraday_depth=depth)
    print_ = fingerprint(projection, inputs)
    assert print_["screening"]["rows"] == GOLDEN["screening"]["rows"]
    assert print_["screening"]["lineage"] == GOLDEN["screening"]["lineage"]
    assert print_["screening"]["hourly_lineage"] == GOLDEN["screening"]["hourly_lineage"]
    assert print_["screening"]["input_sha256"] != GOLDEN["screening"]["input_sha256"]
    for ticker, expected in GOLDEN["workspaces"].items():
        assert print_["workspaces"][ticker]["payload"] == expected["payload"], ticker
        assert print_["workspaces"][ticker]["evidence"] == expected["evidence"], ticker
        assert print_["workspaces"][ticker]["manifest"] != expected["manifest"], ticker
    assert print_["summary_charts"] == GOLDEN["summary_charts"]
    assert print_["summary"] == GOLDEN["summary"]
    assert print_["pattern_watch"] == GOLDEN["pattern_watch"]
    window = resolve_intraday_history_window(inputs.session, depth)
    full = {snapshot.snapshot_key: snapshot.source_manifest["bar_lineage"]["groups"]
            for snapshot in project_realtime_ticker_workspace(ticker_fixture()).snapshots}
    for snapshot in projection.snapshots:
        groups = snapshot.source_manifest["bar_lineage"]["groups"]
        assert "chart:1m" not in snapshot.payload["missing_producers"]
        assert set(snapshot.payload["chart_refs"]) == {"1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"}
        for interval, bars in (("1m", 390), ("5m", 78), ("15m", 26)):
            group = groups[f"{interval}:RAW_UNADJUSTED"]
            assert group["count"] == bars * depth.sessions(interval)
            assert group["first_bar_start"].startswith(str(window.start(interval)))
            assert group["last_bar_end"] == full[snapshot.snapshot_key][f"{interval}:RAW_UNADJUSTED"]["last_bar_end"]
        # Every other group (30m, 1h, daily, weekly, monthly) is the full history's.
        assert {key: value for key, value in groups.items() if not key.startswith(("1m:", "5m:", "15m:"))} == {
            key: value for key, value in full[snapshot.snapshot_key].items()
            if not key.startswith(("1m:", "5m:", "15m:"))}


def test_on_projection_with_the_full_input_is_the_bounded_projection():
    """The projection bounds its groups itself: a full-history input projects like the bounded one (only the input
    digests differ), so a load and a projection that disagree cannot publish a mixed chart."""
    full = ticker_fixture()
    bounded = loaded_with(full, DEPTH)
    left = fingerprint(project_realtime_ticker_workspace(bounded, intraday_depth=DEPTH), bounded)
    right = fingerprint(project_realtime_ticker_workspace(replace(full, source_lineage=STORED_LINEAGE_ACCEPTED),
                                                          intraday_depth=DEPTH), full)
    assert {ticker: (item["payload"], item["evidence"], item["manifest"])
            for ticker, item in left["workspaces"].items()} == {
        ticker: (item["payload"], item["evidence"], item["manifest"]) for ticker, item in right["workspaces"].items()}


def test_off_projection_of_a_bounded_input_does_not_publish_short_5m_history():
    inputs = loaded_with(ticker_fixture(("AAPL",)), DEPTH)
    with pytest.raises(ValueError):
        project_realtime_ticker_workspace(inputs)


def test_on_stream_five_inside_the_1m_sessions_is_still_reverified():
    inputs = advanced_next_session_fixture(sessions_back=1)
    prior = CALENDAR.previous_session(str(inputs.session)).date()
    assert project_realtime_ticker_workspace(loaded_with(inputs, DEPTH), intraday_depth=DEPTH).snapshots
    for corruption in ("source_ids", "aggregate", "missing_minutes"):
        broken = loaded_with(_corrupt_stream_five(inputs, prior, corruption), DEPTH)
        with pytest.raises(ValueError, match="(?i)(causal|lineage|aggregate|minute|source)"):
            project_realtime_ticker_workspace(broken, intraday_depth=DEPTH)


def test_on_stream_five_older_than_the_1m_sessions_keeps_its_stored_lineage():
    inputs = advanced_next_session_fixture(sessions_back=4)
    session = CALENDAR.session_offset(str(inputs.session), -4).date()
    bounded = loaded_with(inputs, DEPTH)
    assert not any(row["interval"] == "1m" and row["session_date"] == session for row in bounded.bars)
    assert any(row["interval"] == "5m" and row["session_date"] == session and row["source_kind"] == "DERIVED"
               for row in bounded.bars)
    projection = project_realtime_ticker_workspace(bounded, intraday_depth=DEPTH)
    assert projection.snapshots and "raw_history" not in projection.snapshots[0].payload["missing_producers"]
    # Its 15m/30m/1h/daily bars are still re-derived from those 5m bars: a 5m aggregate that disagrees with them
    # is caught there.
    with pytest.raises(ValueError, match="(?i)(lineage|aggregate)"):
        project_realtime_ticker_workspace(loaded_with(_corrupt_stream_five(inputs, session, "aggregate"), DEPTH),
                                          intraday_depth=DEPTH)


# --- the window cycle wiring --------------------------------------------------------------------------------------

class _Database:
    @contextmanager
    def transaction(self, **_kwargs):
        yield object()


@pytest.mark.parametrize("value", [None, "1m:2,5m:10,15m:22", "bogus"])
def test_the_ticker_cycle_passes_the_switch_to_the_load_and_the_projection(monkeypatch, value):
    if value is None:
        monkeypatch.delenv(INTRADAY_HISTORY_SWITCH, raising=False)
    else:
        monkeypatch.setenv(INTRADAY_HISTORY_SWITCH, value)
    calls = {}
    inputs = SimpleNamespace(members=(), cutoff=OPEN, bars=())

    class Loader:
        def load(self, _cursor, **scope):
            calls["load"] = scope
            return inputs

    def project(_inputs, **kwargs):
        calls["project"] = kwargs
        return SimpleNamespace(snapshots=(), unavailable_members=(), summary_charts={})

    monkeypatch.setattr(ticker_repository, "project_realtime_ticker_workspace", project)
    monkeypatch.setattr(ticker_repository, "_load_ticker_calendar_inputs", lambda *_args: ((), ()))
    monkeypatch.setattr(ticker_repository, "_load_ticker_fundamental_inputs", lambda *_args: ((), ()))
    monkeypatch.setattr(ticker_repository, "produce_realtime_ticker_workspace_summary", lambda *a, **k: "summary")
    monkeypatch.setattr(ticker_repository, "produce_realtime_pattern_watch_publication", lambda *a, **k: "pattern")
    scope = dict(cohort_id=UUID(int=1), session=SESSION, cutoff=OPEN, source_generation=1)
    ticker_repository._project_ticker_cycle(_Database(), state=None, loader=Loader(), scope=scope, should_stop=None)
    if value == "1m:2,5m:10,15m:22":
        assert calls["load"] == dict(scope, include_portal_history=True, intraday_depth=DEPTH)
        assert calls["project"]["intraday_depth"] == DEPTH
    else:
        assert calls["load"] == dict(scope, include_portal_history=True)
        assert "intraday_depth" not in calls["project"]
