"""R78 (Nov 14): the daily products job - the daily strategies and the Dashboard built outside the Equity window from a
narrow input (`realtime/daily_products.py`, plan `docs/g102/G102_DAILY_PRODUCTS_REFACTOR_20261114.md` section 5)."""
from contextlib import contextmanager
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import exchange_calendars
import numpy as np
import pytest

import realtime.daily_products as job
import realtime.daily_products_launcher as launcher
from realtime.dashboard_projection import produce_realtime_dashboard
from realtime.lineage_head import inputs_sha256
from realtime.preflight import ProviderTier
from realtime.screening_projection import (
    ENDPOINT_CLOSE_SCALE_MAX_RELATIVE_DIFFERENCE, SOURCE_LINEAGE_LOADED, STORED_LINEAGE_ACCEPTED, _validate_bars,
)
from realtime.screening_repository import BarRequest, BarScope, daily_product_bar_window, select_daily_product_bars
from realtime.setup_summary_projection import produce_daily_setup_summary
from realtime.strategy_projection import (
    HISTORY_VERSION, _validate_strategy_inputs, project_realtime_daily_strategies, strategy_action_context,
)
from tests.realtime.test_daily_scanner_vertical_slice import daily_scanner_fixture
from tests.realtime.test_dashboard_vertical_slice import dashboard_fixture

UTC = timezone.utc
SESSION = date(2026, 11, 13)


def _narrow(inputs):
    """What `select_daily_product_bars` keeps of a full input: daily bars and raw 1h, no 5m/1m sources."""
    bars = tuple(row for row in inputs.bars if row["interval"] == "1d"
                 or (row["interval"] == "1h" and row["adjustment_basis"] == "RAW_UNADJUSTED"))
    return replace(inputs, bars=bars, source_lineage=STORED_LINEAGE_ACCEPTED)


# --- narrow loader scope ---------------------------------------------------------------------------------------------

def test_daily_product_bar_window_reads_plan_adjusted_90_raw_and_20_hourly_sessions():
    from realtime.equity_portal_history import build_realtime_equity_portal_history_plan
    calendar = exchange_calendars.get_calendar("XNYS")
    adjusted, raw, hourly, last = daily_product_bar_window(SESSION)
    assert adjusted == build_realtime_equity_portal_history_plan(SESSION).adjusted_sessions[0]
    assert len(calendar.sessions_in_range(str(raw), str(SESSION))) == 90
    assert len(calendar.sessions_in_range(str(hourly), str(SESSION))) == 20
    assert last == SESSION
    with pytest.raises(ValueError):
        daily_product_bar_window(date(2026, 11, 14))  # Saturday


class _BarCursor:
    def __init__(self, rows, sources=()):
        self.rows, self.sources, self.statements = rows, {str(row["bar_revision_id"]): row for row in sources}, []
        self._result = []

    def execute(self, sql, params=()):
        self.statements.append((sql, params))
        if "bar_revision_id=ANY" in sql:
            self._result = [dict(self.sources[key]) for key in params[0] if key in self.sources]
        else:
            self._result = [dict(row) for row in self.rows]

    def fetchall(self):
        return self._result


def _request():
    scope = BarScope(cohort_id=UUID(int=1), provider_tier="MASSIVE_ADVANCED", tickers=("AAPL",),
                     include_portal_history=True, history_params=())
    cutoff = datetime(2026, 11, 13, 21, 30, tzinfo=UTC)
    return BarRequest(scope=scope, cutoff=cutoff, known_by=cutoff, identity=(), slot=())


def _row(key, interval, basis, sources=()):
    return dict(bar_revision_id=key, ticker="AAPL", interval=interval, adjustment_basis=basis,
                source_revision_ids=list(sources))


def test_narrow_loader_reads_only_daily_and_hourly_and_no_sources_by_default():
    cursor = _BarCursor([_row("d", "1d", "ADJUSTED"), _row("r", "1d", "RAW_UNADJUSTED", ("m5",)),
                         _row("h", "1h", "RAW_UNADJUSTED", ("m5",))], sources=[_row("m5", "5m", "RAW_UNADJUSTED")])
    rows = select_daily_product_bars(cursor, _request(), session=SESSION)
    assert [row["bar_revision_id"] for row in rows] == ["d", "r", "h"]
    assert len(cursor.statements) == 1
    sql, params = cursor.statements[0]
    for clause in ("interval='1d' AND adjustment_basis='ADJUSTED'",
                   "interval='1d' AND adjustment_basis='RAW_UNADJUSTED'",
                   "interval='1h' AND adjustment_basis='RAW_UNADJUSTED'",
                   "DISTINCT ON (ticker, interval, adjustment_basis, bar_start)"):
        assert clause in sql
    where = sql.split("ORDER BY")[0]  # the ORDER BY's source-precedence CASE names every interval
    for absent in ("'1m'", "'5m'", "'15m'", "'30m'", "'1wk'", "'1mo'"):
        assert absent not in where
    adjusted, raw, hourly, last = daily_product_bar_window(SESSION)
    assert params[-6:] == (adjusted, last, raw, last, hourly, last)


def test_narrow_loader_with_source_closure_reads_the_sources_as_the_window_load_does():
    cursor = _BarCursor([_row("r", "1d", "RAW_UNADJUSTED", ("m5",))],
                        sources=[_row("m5", "5m", "RAW_UNADJUSTED", ("m1",)), _row("m1", "1m", "RAW_UNADJUSTED")])
    rows = select_daily_product_bars(cursor, _request(), session=SESSION, source_closure=True)
    assert [row["bar_revision_id"] for row in rows] == ["r", "m5", "m1"]


# --- lineage acceptance ----------------------------------------------------------------------------------------------

def _member_rows(inputs, ticker="AAPL"):
    member = next(row for row in inputs.members if row["ticker"] == ticker)
    return member, [row for row in inputs.bars if row["ticker"] == ticker]


def test_derived_bars_without_sources_are_refused_when_loaded_and_accepted_by_stored_lineage():
    inputs = daily_scanner_fixture()
    member, rows = _member_rows(inputs)
    kept = [row for row in rows if row["interval"] in {"1d", "1h"}]
    assert any(row["source_kind"] == "DERIVED" for row in kept), "fixture must hold derived daily/hourly bars"
    with pytest.raises(ValueError, match="causal source is missing"):
        _validate_bars(kept, member, inputs)
    _validate_bars(kept, member, replace(inputs, source_lineage=STORED_LINEAGE_ACCEPTED))
    assert inputs.source_lineage == SOURCE_LINEAGE_LOADED


def test_accepted_lineage_still_requires_named_sources_and_verifies_sources_that_are_present():
    inputs = replace(daily_scanner_fixture(), source_lineage=STORED_LINEAGE_ACCEPTED)
    member, rows = _member_rows(inputs)
    derived = next(row for row in rows if row["source_kind"] == "DERIVED")
    unnamed = [dict(row, source_revision_ids=[]) if row is derived else row for row in rows]
    with pytest.raises(ValueError, match="lineage is missing"):
        _validate_bars(unnamed, member, inputs)
    bumped = derived["close"] * 1.01
    wrong = [dict(row, close=bumped, high=max(row["high"], bumped)) if row is derived else row for row in rows]
    with pytest.raises(ValueError, match="aggregate drift"):
        _validate_bars(wrong, member, inputs)


def test_source_lineage_mode_is_validated_and_out_of_every_hash():
    from realtime.contracts import canonical_sha256
    inputs = daily_scanner_fixture()
    accepted = replace(inputs, source_lineage=STORED_LINEAGE_ACCEPTED)
    assert canonical_sha256(accepted) == canonical_sha256(inputs) and accepted == inputs
    assert inputs_sha256(accepted) == inputs_sha256(inputs)
    assert replace(accepted, session=accepted.session).source_lineage == STORED_LINEAGE_ACCEPTED
    with pytest.raises(ValueError, match="lineage mode"):
        replace(inputs, source_lineage="TRUSTED")


# --- grouped lookups equal the per-member scans ----------------------------------------------------------------------

# The validation as it was before R78 (git ea76dd5), kept verbatim as the oracle for the grouped version.
OLD_VALIDATE = (
    'def _old_validate_strategy_inputs(inputs):\n'
    '    if (len(inputs.source_manifest_sha256) != 64\n'
    '            or any(value not in "0123456789abcdef" for value in inputs.source_manifest_sha256)\n'
    '            or type(inputs.source_generation) is not int or inputs.source_generation <= 0):\n'
    '        raise ValueError("Strategy source manifest/generation is invalid")\n'
    '    calendar = exchange_calendars.get_calendar("XNYS")\n'
    '    market_time = calendar.session_close(str(inputs.session)).to_pydatetime()\n'
    '    if market_time > inputs.cutoff:\n'
    '        raise ValueError("Strategy session is not completed at cutoff")\n'
    '    members = inputs.members\n'
    '    if (not 0 < len(members) <= 1000 or len({row["ticker"] for row in members}) != len(members)\n'
    '            or len({str(row["security_id"]) for row in members}) != len(members)\n'
    '            or set(row["ticker"] for row in inputs.bars) - {row["ticker"] for row in members}):\n'
    '        raise ValueError("Strategy exact unique cohort membership is required")\n'
    '    delay = timedelta(minutes=15 if inputs.provider_tier is ProviderTier.MASSIVE_DEVELOPER else 0)\n'
    '    unavailable_members = {}\n'
    '    action_context = {}\n'
    '    for member in members:\n'
    '        if (not member.get("security_revision_id") or member.get("security_type") not in {"CS", "ETF", "ETV"}\n'
    '                or member["observed_at"] > inputs.cutoff):\n'
    '            raise ValueError("Strategy reference identity/availability drift")\n'
    '        own = [row for row in inputs.bars if row["ticker"] == member["ticker"]\n'
    '               and row["interval"] not in {"1wk", "1mo"}]\n'
    '        _validate_bars(own, member, inputs)\n'
    '        for row in own:\n'
    '            clocks = [row["bar_end"] + delay, row["observed_at"], row["created_at"]]\n'
    '            clocks.extend(row[key] for key in ("replay_available_at", "provider_published_at")\n'
    '                          if row.get(key) is not None)\n'
    '            if any(clock.tzinfo is None or clock.utcoffset() is None for clock in clocks):\n'
    '                raise ValueError("Strategy source clocks must be timezone-aware")\n'
    '            if max(clocks) > inputs.cutoff:\n'
    '                raise ValueError("Strategy source availability is beyond cutoff")\n'
    '        daily = sorted((row for row in own if row["interval"] == "1d"\n'
    '                        and row["adjustment_basis"] == "ADJUSTED"), key=lambda row: row["session_date"])\n'
    '        actions = [row for row in inputs.actions if str(row["security_id"]) == str(member["security_id"])]\n'
    '        if any(row["ticker"] != member["ticker"] or row["received_at"] > inputs.cutoff for row in actions):\n'
    '            raise ValueError("Strategy action identity/availability drift")\n'
    '        if not daily:\n'
    '            continue\n'
    '        sessions = [stamp.date() for stamp in calendar.sessions_in_range(str(daily[0]["session_date"]), str(inputs.session))]\n'
    '        if [row["session_date"] for row in daily] != sessions:\n'
    '            raise ValueError("Strategy retained daily window is incomplete or ambiguous")\n'
    '        review_actions = [row for row in actions if daily[0]["session_date"] < row["effective_date"] <= inputs.session\n'
    '                          and row["action_type"] != "SPLIT"]\n'
    '        if review_actions:\n'
    '            refs = [strategy_action_context(row) for row in review_actions]\n'
    '            reason = "CORPORATE_ACTION_REVIEW" if any(row["handling"] == "CORPORATE_ACTION_REVIEW" for row in refs) else None\n'
    '            context = dict(reason=reason, available_bars=len(daily), action_refs=refs)\n'
    '            action_context[member["ticker"]] = context\n'
    '            if reason:\n'
    '                unavailable_members[member["ticker"]] = context\n'
    '        coverage = [row for row in inputs.coverage if str(row["security_id"]) == str(member["security_id"])\n'
    '                    and row["action_type"] == "SPLIT" and row["window_start"] <= sessions[0]\n'
    '                    and row["window_end"] >= inputs.session and row["received_at"] <= inputs.cutoff\n'
    '                    and row["response_action_count"] == sum(action["action_type"] == "SPLIT"\n'
    '                        and row["window_start"] <= action["effective_date"] <= row["window_end"] for action in actions)\n'
    '                    and all(action["received_at"] <= row["received_at"] for action in actions\n'
    '                            if action["action_type"] == "SPLIT" and row["window_start"] <= action["effective_date"] <= row["window_end"])]\n'
    '        if not coverage:\n'
    '            raise ValueError("Strategy split/action coverage is unavailable or inconsistent")\n'
    '        splits = [row for row in actions if row["action_type"] == "SPLIT"]\n'
    '        if any(not np.isfinite([float(row["split_from"]), float(row["split_to"])]).all()\n'
    '               or float(row["split_from"]) <= 0 or float(row["split_to"]) <= 0 for row in splits):\n'
    '            raise ValueError("Strategy split evidence is invalid")\n'
    '        for previous, current in zip(daily, daily[1:]):\n'
    '            change = float(current["close"]) / float(previous["close"]) - 1\n'
    '            if current["session_date"] in {row["effective_date"] for row in splits} and (change <= -.38 or change >= .60):\n'
    '                raise ValueError("Strategy adjusted split/action drift")\n'
    '        raw = [row for row in own if row["interval"] == "1d" and row["adjustment_basis"] == "RAW_UNADJUSTED"\n'
    '               and row["session_date"] == inputs.session]\n'
    '        if len(raw) != 1 or abs(float(raw[0]["close"]) / float(daily[-1]["close"]) - 1) > ENDPOINT_CLOSE_SCALE_MAX_RELATIVE_DIFFERENCE:\n'
    '            raise ValueError("Strategy current adjusted/raw scale drift")\n'
    '    return market_time, unavailable_members, action_context\n'
)
_namespace = {}
exec(OLD_VALIDATE, dict(
    exchange_calendars=exchange_calendars, np=np, timedelta=timedelta, ProviderTier=ProviderTier,
    _validate_bars=_validate_bars, strategy_action_context=strategy_action_context,
    ENDPOINT_CLOSE_SCALE_MAX_RELATIVE_DIFFERENCE=ENDPOINT_CLOSE_SCALE_MAX_RELATIVE_DIFFERENCE,
), _namespace)
_old_validate_strategy_inputs = _namespace["_old_validate_strategy_inputs"]


def _with_dividend(inputs, verified=True):
    from realtime.contracts import canonical_sha256
    member = next(row for row in inputs.members if row["ticker"] == "MSFT")
    ex_date = inputs.session - timedelta(days=3)
    raw = dict(ticker="MSFT", distribution_type="recurring" if verified else "special", currency="USD",
               cash_amount=0.83, ex_dividend_date=str(ex_date))
    action = dict(action_id=uuid4(), security_id=member["security_id"], ticker="MSFT", action_type="DIVIDEND",
                  effective_date=ex_date, split_from=None, split_to=None, payload_sha256=canonical_sha256(raw),
                  received_at=inputs.cutoff - timedelta(days=1), source="test", source_key="div",
                  observed_at=inputs.cutoff - timedelta(days=1), cash_amount="0.83", ex_date=ex_date,
                  new_ticker=None, raw_payload=raw)
    return replace(inputs, actions=(*inputs.actions, action))


@pytest.mark.parametrize("variant", ["plain", "dividend", "review", "narrow"])
def test_grouped_strategy_validation_equals_the_per_member_scans(variant):
    inputs = daily_scanner_fixture()
    if variant == "dividend":
        inputs = _with_dividend(inputs)
    elif variant == "review":
        inputs = _with_dividend(inputs, verified=False)
    elif variant == "narrow":
        inputs = _narrow(inputs)
    assert _validate_strategy_inputs(inputs) == _old_validate_strategy_inputs(inputs)


def test_grouped_strategy_validation_refuses_what_the_scans_refused():
    inputs = daily_scanner_fixture()
    member = inputs.members[0]
    late = replace(inputs, coverage=tuple(dict(row, received_at=inputs.cutoff + timedelta(seconds=1))
                                          if str(row["security_id"]) == str(member["security_id"]) else row
                                          for row in inputs.coverage))
    for function in (_validate_strategy_inputs, _old_validate_strategy_inputs):
        with pytest.raises(ValueError, match="coverage"):
            function(late)


def test_strategy_manifest_uses_the_memoized_input_digest():
    inputs = daily_scanner_fixture()
    assert project_realtime_daily_strategies(inputs).snapshot.source_manifest["input_sha256"] == inputs_sha256(inputs)


# --- coverage rule ---------------------------------------------------------------------------------------------------

def test_coverage_rule_accepts_a_refresh_received_before_the_cutoff_covering_the_whole_window():
    """The documented rule: a SPLIT coverage row per member received by the cutoff, its window from the member's first
    adjusted daily session through the session, its response count equal to the split actions in it. The live
    preparation refresh (Nov 10-14) writes exactly that: window from 283 sessions back, end one year ahead."""
    inputs = daily_scanner_fixture()
    calendar = exchange_calendars.get_calendar("XNYS")
    first = min(row["session_date"] for row in inputs.bars
                if row["interval"] == "1d" and row["adjustment_basis"] == "ADJUSTED")
    refreshed = tuple(dict(row, window_start=first, window_end=inputs.session + timedelta(days=365),
                           received_at=inputs.cutoff - timedelta(hours=8)) for row in inputs.coverage)
    _validate_strategy_inputs(replace(inputs, coverage=refreshed))
    short = tuple(dict(row, window_end=calendar.previous_session(str(inputs.session)).date()) for row in refreshed)
    with pytest.raises(ValueError, match="coverage"):
        _validate_strategy_inputs(replace(inputs, coverage=short))
    late_start = tuple(dict(row, window_start=first + timedelta(days=1)) for row in refreshed)
    with pytest.raises(ValueError, match="coverage"):
        _validate_strategy_inputs(replace(inputs, coverage=late_start))


# --- the products over the narrow input equal the window-input products ----------------------------------------------

def test_dashboard_over_the_narrow_input_equals_the_window_input_dashboard():
    full = dashboard_fixture()
    base = produce_realtime_dashboard(full).payload
    narrow = produce_realtime_dashboard(_narrow(full)).payload
    for key in ("overview", "scan", "scanners", "tickers", "unavailable_members", "missing_producers"):
        assert narrow[key] == base[key], key
    assert narrow["setup_summary"]["results"] == base["setup_summary"]["results"]
    assert [row["daily"] for row in narrow["context"]["rows"]] == [row["daily"] for row in base["context"]["rows"]]


def test_strategies_over_the_narrow_input_equal_the_window_input_strategies():
    full = daily_scanner_fixture()
    base = project_realtime_daily_strategies(full).snapshot.payload
    narrow = project_realtime_daily_strategies(_narrow(full)).snapshot.payload
    for key in ("scanners", "analysis", "overview"):
        assert narrow[key] == base[key], key
    assert narrow["setup_summary"]["results"] == base["setup_summary"]["results"]


def test_dashboard_reuses_setups_only_when_they_cover_exactly_its_members(monkeypatch):
    import realtime.dashboard_projection as dashboard
    inputs = dashboard_fixture()
    base = produce_realtime_dashboard(inputs)
    rows, evidence, missing = produce_daily_setup_summary(
        replace(inputs, source_generation=inputs.source_generation + 4), materializations=[])
    assert "dependency_evidence_ids" in evidence[0]
    calls = []
    original = dashboard.produce_daily_setup_summary
    monkeypatch.setattr(dashboard, "produce_daily_setup_summary", lambda *a, **k: calls.append(1) or original(*a, **k))
    reused = produce_realtime_dashboard(inputs, setups=(rows, evidence, missing))
    assert calls == []
    assert reused.payload["setup_summary"]["results"] == base.payload["setup_summary"]["results"]
    assert all("dependency_evidence_ids" not in row for row in reused.payload["setup_evidence"])
    assert reused.payload["setup_summary"]["analysis_run_id"] == evidence[0]["analysis_run_id"]
    for key in ("overview", "scan", "scanners", "context"):
        assert reused.payload[key] == base.payload[key], key
    produce_realtime_dashboard(inputs, setups=(rows[1:], evidence[1:], missing))
    assert calls == [1]


@pytest.mark.parametrize("kind", ["thread", "spawn"])
def test_setup_executor_returns_the_in_process_results_in_member_order(kind):
    """R78: the job's optional worker pool (`REALTIME_V2_DAILY_PRODUCTS_WORKERS`) changes where the per-member setup
    evidence is computed, not what it is."""
    import multiprocessing
    from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
    from realtime.setup_summary_projection import setup_executor
    inputs = dashboard_fixture()
    if kind == "spawn":  # a spawned process proves the requests and results pickle; two members keep it quick
        tickers = sorted(member["ticker"] for member in inputs.members)[:2]
        inputs = replace(inputs, members=tuple(m for m in inputs.members if m["ticker"] in tickers),
                         bars=tuple(row for row in inputs.bars if row["ticker"] in tickers))
    base_materializations, pooled_materializations = [], []
    base = produce_daily_setup_summary(inputs, materializations=base_materializations)
    executor = (ThreadPoolExecutor(3) if kind == "thread"
                else ProcessPoolExecutor(2, mp_context=multiprocessing.get_context("spawn")))
    with executor, setup_executor(executor):
        pooled = produce_daily_setup_summary(inputs, materializations=pooled_materializations)
    assert pooled == base
    assert [m.input_sha256 for m in pooled_materializations] == [m.input_sha256 for m in base_materializations]
    assert [[e.payload_sha256 for e in m.materialization.evidence] for m in pooled_materializations] == [
        [e.payload_sha256 for e in m.materialization.evidence] for m in base_materializations]


# --- the job: identity, idempotency, supersede, publication ----------------------------------------------------------

def test_input_identity_ignores_the_clock_and_tracks_inputs_and_mode():
    inputs = daily_scanner_fixture()
    later = replace(inputs, cutoff=inputs.cutoff + timedelta(hours=2), source_generation=inputs.source_generation + 3)
    assert job.daily_products_input_sha256(inputs, job.MODE_JOB) == job.daily_products_input_sha256(later, job.MODE_JOB)
    assert (job.daily_products_input_sha256(inputs, job.MODE_JOB)
            != job.daily_products_input_sha256(inputs, job.MODE_STRATEGIES))
    changed = replace(inputs, bars=inputs.bars[:-1])
    assert job.daily_products_input_sha256(changed, job.MODE_JOB) != job.daily_products_input_sha256(inputs, job.MODE_JOB)


@pytest.mark.parametrize("value,mode,window", [
    (None, "OFF", True), ("", "OFF", True), ("off", "OFF", True), ("strategies", "STRATEGIES", True),
    ("JOB", "JOB", False), (" job ", "JOB", False), ("YES", "OFF", True),
])
def test_switch_values(value, mode, window):
    environ = {} if value is None else {job.DAILY_PRODUCTS_SWITCH: value}
    assert job.daily_products_mode(environ) == mode
    assert job.window_builds_dashboard(environ) is window


def _compute_db():
    from realtime.database_access import RealtimeDatabaseRole
    from realtime.stage_database_v2 import V2WorkerDatabase

    class Provider:
        @contextmanager
        def transaction(self, **_options):
            yield SimpleNamespace(cursor="cursor")

    return V2WorkerDatabase(Provider(), RealtimeDatabaseRole.COMPUTE)


def _patch_load(monkeypatch, inputs, stored):
    monkeypatch.setattr(job, "next_source_generation_cursor", lambda cursor, cohort: inputs.source_generation)
    monkeypatch.setattr(job, "load_daily_products_input", lambda cursor, **scope: inputs)
    monkeypatch.setattr(job, "stored_daily_products_cursor", lambda cursor, cohort, mode, **_: stored)
    monkeypatch.setattr(job, "prepared_daily_products_cursor", lambda cursor, cohort, mode, identity, **_: {})


def _refuse(*_args, **_kwargs):
    raise RuntimeError("computing")


def test_job_is_already_stored_when_current_products_carry_the_same_input_identity(monkeypatch):
    inputs = _narrow(daily_scanner_fixture())
    identity = job.daily_products_input_sha256(inputs, job.MODE_JOB)
    stored = {product: (dict(snapshot_id=str(uuid4())), dict(input_sha256=identity), str(SESSION))
              for product in job.PRODUCTS[job.MODE_JOB]}
    _patch_load(monkeypatch, inputs, stored)
    monkeypatch.setattr(job, "project_realtime_strategy_history", _refuse)
    products, outcome = job.compute_daily_products(_compute_db(), cohort_id=inputs.cohort_id, session=inputs.session,
                                                   cutoff=inputs.cutoff, mode=job.MODE_JOB)
    assert products is None and outcome.status == job.ALREADY_STORED and outcome.input_sha256 == identity
    # A product missing, or one built from other inputs, is recomputed.
    for changed in (dict(list(stored.items())[:1]),
                    {key: (entry, dict(input_sha256="0" * 64), session) for key, (entry, _m, session) in stored.items()}):
        _patch_load(monkeypatch, inputs, changed)
        with pytest.raises(RuntimeError, match="computing"):
            job.compute_daily_products(_compute_db(), cohort_id=inputs.cohort_id, session=inputs.session,
                                       cutoff=inputs.cutoff, mode=job.MODE_JOB)


@pytest.fixture(scope="module")
def job_products():
    inputs = _narrow(daily_scanner_fixture())
    saved = (job.next_source_generation_cursor, job.load_daily_products_input, job.stored_daily_products_cursor,
             job.prepared_daily_products_cursor)
    job.next_source_generation_cursor = lambda cursor, cohort: inputs.source_generation
    job.load_daily_products_input = lambda cursor, **scope: inputs
    job.stored_daily_products_cursor = lambda cursor, cohort, mode, **_: {}
    job.prepared_daily_products_cursor = lambda cursor, cohort, mode, identity, **_: {}
    try:
        products, stored = job.compute_daily_products(_compute_db(), cohort_id=inputs.cohort_id, session=inputs.session,
                                                      cutoff=inputs.cutoff, mode=job.MODE_JOB)
    finally:
        (job.next_source_generation_cursor, job.load_daily_products_input, job.stored_daily_products_cursor,
         job.prepared_daily_products_cursor) = saved
    assert stored is None
    return products


def test_job_products_are_a_reconstructed_history_and_a_dashboard_stamped_with_the_input_identity(job_products):
    from realtime.strategy_repository import _validate_strategy_snapshot
    strategy, dashboard = job_products.strategy.snapshot, job_products.dashboard
    assert strategy.snapshot_type == HISTORY_VERSION and dashboard.snapshot_type == "DASHBOARD_V1"
    for snapshot in (strategy, dashboard):
        assert snapshot.source_manifest[job.DAILY_PRODUCTS_MARKER] == dict(
            version=job.DAILY_PRODUCTS_VERSION, input_sha256=job_products.input_sha256, mode=job.MODE_JOB,
            strategy_sessions=5, source_lineage=STORED_LINEAGE_ACCEPTED)
    _validate_strategy_snapshot(strategy.source_manifest, strategy.payload)
    latest = strategy.source_manifest["history"][-1]
    assert dashboard.payload["setup_summary"]["results"] == latest["payload"]["setup_summary"]["results"]
    assert len(strategy.payload["evaluation_sessions"]) == 5
    assert strategy.source_generation == dashboard.source_generation + 4  # five evaluation generations


class _PublishCursor:
    def __init__(self, *, current, stored_rows=()):
        self.current, self.stored_rows, self.statements = current, list(stored_rows), []
        self._one = self._all = None

    def execute(self, sql, params=()):
        self.statements.append(sql)
        if "FROM rt.portal_release_current" in sql:
            self._one = self.current
        elif "snapshot.source_manifest->" in sql:
            self._all = self.stored_rows
        else:
            self._one = None

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


def _current(cutoff, entries=(), tier=None):
    tier = tier or daily_scanner_fixture().provider_tier.value
    return dict(release_id=uuid4(), cutoff=cutoff, session_date=SESSION, provider_tier=tier,
                receipt_id=uuid4(), universe_sha256="a" * 64, desired_revision_id=uuid4(),
                desired_snapshot_sha256="b" * 64, membership_sha256="c" * 64, manifest=dict(snapshots=list(entries)))


def _patch_publication(monkeypatch, published):
    monkeypatch.setattr(job, "persist_strategy_snapshot_cursor",
                        lambda cursor, snapshot: published.append(("strategy", snapshot)))
    monkeypatch.setattr(job, "persist_realtime_dashboard_cursor",
                        lambda cursor, snapshot, session_close: published.append(("dashboard", snapshot, session_close)))

    def release(cursor, receipt, snapshots, session_date=None):
        published.append(("release", receipt, tuple(snapshots), session_date))
        return SimpleNamespace(pointer_swapped=True, release_id=uuid4(), cutoff=receipt.cutoff,
                               publication_disposition="CURRENT")
    monkeypatch.setattr(job, "publish_portal_release_cursor", release)


def test_job_publishes_both_products_in_one_release_on_the_current_pointer(monkeypatch, job_products):
    published = []
    _patch_publication(monkeypatch, published)
    current = _current(datetime(2026, 11, 13, 21, 0, tzinfo=UTC))
    status, release, reason = job.publish_daily_products_cursor(_PublishCursor(current=current), job_products)
    assert status == job.PUBLISHED and reason is None
    assert [item[0] for item in published] == ["strategy", "dashboard", "release"]
    assert published[1][2] is False  # the Dashboard is HOT, as a window's
    _kind, receipt, snapshots, session_date = published[2]
    assert receipt.cutoff == current["cutoff"] and str(receipt.receipt_id) == str(current["receipt_id"])
    assert receipt.desired_snapshot_sha256 == current["desired_snapshot_sha256"]
    assert receipt.membership_sha256 == current["membership_sha256"]
    assert receipt.source_generation is None and session_date == SESSION
    assert [snapshot.snapshot_type for snapshot in snapshots] == [HISTORY_VERSION, "DASHBOARD_V1"]


def test_a_late_job_never_replaces_a_newer_publication(monkeypatch, job_products):
    published = []
    _patch_publication(monkeypatch, published)
    session = job_products.inputs.session
    newer = job_products.dashboard.generated_at + timedelta(minutes=5)
    entry = dict(snapshot_id=str(uuid4()), snapshot_type="DASHBOARD_V1", snapshot_key="COHORT",
                 generated_at=newer.isoformat(), source_generation=99)
    cursor = _PublishCursor(current=_current(newer, [entry]), stored_rows=[
        dict(snapshot_id=entry["snapshot_id"], marker=dict(input_sha256="x"), session=str(session))])
    status, release, reason = job.publish_daily_products_cursor(cursor, job_products)
    assert (status, release, reason) == (job.SUPERSEDED, None, "NEWER_DASHBOARD_V1_IS_CURRENT")
    assert published == []
    # A later session's product supersedes too, whatever its clock.
    entry = dict(entry, generated_at=(job_products.dashboard.generated_at - timedelta(days=1)).isoformat())
    cursor = _PublishCursor(current=_current(newer, [entry]), stored_rows=[
        dict(snapshot_id=entry["snapshot_id"], marker=None, session=str(session + timedelta(days=3)))])
    assert job.publish_daily_products_cursor(cursor, job_products)[0] == job.SUPERSEDED
    # An older one of the same session (the window's Dashboard) is replaced.
    entry = dict(entry, generated_at=(job_products.dashboard.generated_at - timedelta(minutes=30)).isoformat())
    cursor = _PublishCursor(current=_current(newer, [entry]), stored_rows=[
        dict(snapshot_id=entry["snapshot_id"], marker=None, session=str(session))])
    assert job.publish_daily_products_cursor(cursor, job_products)[0] == job.PUBLISHED


def test_a_release_of_another_tier_is_refused(monkeypatch, job_products):
    published = []
    _patch_publication(monkeypatch, published)
    other = "MASSIVE_ADVANCED" if job_products.inputs.provider_tier.value != "MASSIVE_ADVANCED" else "MASSIVE_DEVELOPER"
    with pytest.raises(ValueError, match="tier"):
        job.publish_daily_products_cursor(_PublishCursor(current=_current(datetime(2026, 11, 13, 21, tzinfo=UTC),
                                                                          tier=other)), job_products)
    assert published == []


def test_job_without_a_current_release_publishes_nothing(monkeypatch, job_products):
    published = []
    _patch_publication(monkeypatch, published)
    assert job.publish_daily_products_cursor(_PublishCursor(current=None), job_products) == (
        job.NO_RELEASE, None, "COHORT_HAS_NO_CURRENT_RELEASE")
    assert published == []


def test_a_failed_publication_writes_no_release(monkeypatch, job_products):
    """Forced failure: the products are persisted in the release's transaction, so an error before the release (here
    the Dashboard write) leaves no release; the transaction rolls back and the previous release stays current."""
    published = []
    _patch_publication(monkeypatch, published)

    def boom(*_args, **_kwargs):
        raise ValueError("boom")
    monkeypatch.setattr(job, "persist_realtime_dashboard_cursor", boom)
    with pytest.raises(ValueError, match="boom"):
        job.publish_daily_products_cursor(_PublishCursor(current=_current(datetime(2026, 11, 13, 21, tzinfo=UTC))),
                                          job_products)
    assert [item[0] for item in published] == ["strategy"]


# --- the launcher: retries, deadline, idempotent keys ----------------------------------------------------------------

class _Process:
    def __init__(self, codes):
        self.codes, self.killed = list(codes), False

    def poll(self):
        return self.codes.pop(0) if self.codes else None

    def kill(self):
        self.killed = True

    def wait(self, timeout=None):
        return -9


def _launcher(tmp_path, processes, mode="JOB", **options):
    started = []

    def popen(command, env=None, close_fds=True):
        started.append((command, env))
        return processes.pop(0)
    instance = launcher.DailyProductsLauncher(report_directory=tmp_path, environ={job.DAILY_PRODUCTS_SWITCH: mode},
                                              popen=popen, python="python", **options)
    return instance, started


DUE = launcher.DueProducts(str(UUID(int=7)), SESSION, launcher.AFTER_CLOSE)
T0 = datetime(2026, 11, 13, 21, 20, tzinfo=UTC)


def test_launcher_starts_one_limited_job_and_marks_its_key_done(tmp_path, monkeypatch):
    faults = []
    monkeypatch.setattr(launcher, "emit_fault", lambda **record: faults.append(record))
    instance, started = _launcher(tmp_path, [_Process([None, 0])])
    assert instance.tick(T0, lambda now: [DUE]) == DUE
    command, env = started[0]
    assert command[command.index("--mode") + 1] == "JOB" and command[command.index("--trigger") + 1] == "AFTER_CLOSE"
    assert command[command.index("--memory-limit-mb") + 1] == "3072" and command[command.index("--nice") + 1] == "10"
    assert command[command.index("--workers") + 1] == "1"
    assert env["OPENBLAS_NUM_THREADS"] == "1"
    assert instance.tick(T0 + timedelta(seconds=30), lambda now: [DUE]) is None  # still running
    assert instance.tick(T0 + timedelta(seconds=90), lambda now: [DUE]) is None  # exited 0: done, not restarted
    assert DUE.key in instance.done and len(started) == 1 and faults == []


def test_launcher_retries_a_failed_key_with_spacing_and_gives_up_with_a_fault(tmp_path, monkeypatch):
    faults = []
    monkeypatch.setattr(launcher, "emit_fault", lambda **record: faults.append(record))
    instance, started = _launcher(tmp_path, [_Process([1]), _Process([1]), _Process([1])], check_seconds=0)
    instance.tick(T0, lambda at: [DUE])
    instance.tick(T0 + timedelta(seconds=1), lambda at: [DUE])  # polls: failed, retry spaced
    assert len(started) == 1 and faults[-1]["kind"] == "DAILY_PRODUCTS_ATTEMPT_FAILED"
    instance.tick(T0 + timedelta(minutes=4), lambda at: [DUE])
    assert len(started) == 1
    for minutes in (6, 7, 13, 14):
        instance.tick(T0 + timedelta(minutes=minutes), lambda at: [DUE])
    assert len(started) == 3
    assert faults[-1]["kind"] == "DAILY_PRODUCTS_GAVE_UP" and faults[-1]["severity"] == "ERROR"
    instance.tick(T0 + timedelta(hours=1), lambda at: [DUE])
    assert len(started) == 3  # attempts spent


def test_launcher_kills_a_job_past_its_deadline(tmp_path, monkeypatch):
    faults = []
    monkeypatch.setattr(launcher, "emit_fault", lambda **record: faults.append(record))
    process = _Process([])
    instance, _started = _launcher(tmp_path, [process], deadline=timedelta(minutes=10))
    instance.tick(T0, lambda at: [DUE])
    instance.tick(T0 + timedelta(minutes=9), lambda at: [DUE])
    assert not process.killed
    instance.tick(T0 + timedelta(minutes=11), lambda at: [DUE])
    assert process.killed and instance.running is None and "DEADLINE" in faults[-1]["message"]


def test_launcher_waits_without_spending_attempts_when_the_cohort_has_no_release(tmp_path, monkeypatch):
    def no_fault(**_record):
        raise AssertionError("no fault for NO_RELEASE")
    monkeypatch.setattr(launcher, "emit_fault", no_fault)
    instance, started = _launcher(tmp_path, [_Process([3]) for _ in range(6)], check_seconds=0)
    for second in range(6):  # each check polls the last run (NO_RELEASE) and starts the next
        instance.tick(T0 + timedelta(seconds=second), lambda at: [DUE])
    assert len(started) == 6 and instance.attempts[DUE.key] == 1 and DUE.key not in instance.done


def test_launcher_does_nothing_with_the_switch_off_and_never_raises(tmp_path, monkeypatch):
    instance, started = _launcher(tmp_path, [], mode="")
    assert instance.tick(T0, lambda at: [DUE]) is None and started == []
    faults = []
    monkeypatch.setattr(launcher, "emit_fault", lambda **record: faults.append(record))
    instance, started = _launcher(tmp_path, [])

    def broken(_at):
        raise RuntimeError("db down")
    assert instance.tick(T0, broken) is None
    assert faults[-1]["kind"] == "DAILY_PRODUCTS_LAUNCHER_FAILED"


def test_discovery_orders_the_session_cohort_before_the_closed_session():
    from options.calendar import OptionExchangeCalendar
    from realtime.advanced_equity_lifecycle import AdvancedEquityLifecyclePhase

    class Cursor:
        pointer = False

        def execute(self, sql, params=()):
            self.pointer = params[0] == "target-cohort"

        def fetchone(self):
            return (1,) if self.pointer else None

    class Provider:
        @contextmanager
        def transaction(self, role, read_only=False):
            yield SimpleNamespace(cursor=Cursor())

    calendar = OptionExchangeCalendar("XNYS")
    target = date(2026, 11, 16)
    scopes = {target: dict(cohort_id="target-cohort"), SESSION: dict(cohort_id="closed-cohort")}
    complete = []

    def discover(phase):
        return launcher.discover_due_daily_products(
            Provider(), lifecycle=SimpleNamespace(target_session_date=target, phase=phase), calendar=calendar, now=T0,
            load_scope=lambda cursor, session_date: scopes.get(session_date),
            recovery_complete=lambda cursor, **options: complete.append(options) or True, app_role="app")
    due = discover(AdvancedEquityLifecyclePhase.IDLE)
    assert [(item.cohort_id, item.session, item.trigger) for item in due] == [
        ("target-cohort", SESSION, launcher.SESSION_COHORT), ("closed-cohort", SESSION, launcher.AFTER_CLOSE)]
    assert complete[0]["cutoff"] == calendar.session_close(SESSION)
    active = next(phase for phase in AdvancedEquityLifecyclePhase
                  if phase not in {AdvancedEquityLifecyclePhase.IDLE, AdvancedEquityLifecyclePhase.BEFORE_OPEN})
    assert [item.trigger for item in discover(active)] == [launcher.SESSION_COHORT]
    scopes.pop(target)
    assert [item.trigger for item in discover(AdvancedEquityLifecyclePhase.BEFORE_OPEN)] == [launcher.AFTER_CLOSE]


# --- the window with the switch set ----------------------------------------------------------------------------------

def test_window_never_builds_the_dashboard_in_job_mode(monkeypatch):
    import importlib
    script = importlib.import_module("scripts.run_realtime_v2_advanced_equity_window")
    at = datetime(2026, 11, 13, 16, 35, tzinfo=UTC)
    monkeypatch.delenv(job.DAILY_PRODUCTS_SWITCH, raising=False)
    assert script.dashboard_due(at, warm=False) is True
    monkeypatch.setenv(job.DAILY_PRODUCTS_SWITCH, "STRATEGIES")
    assert script.dashboard_due(at, warm=False) is True
    monkeypatch.setenv(job.DAILY_PRODUCTS_SWITCH, "JOB")
    assert script.dashboard_due(at, warm=False) is False and script.dashboard_due(at, warm=True) is False


def test_only_the_dashboard_opens_the_second_loader_slot():
    """One copy of the bars: a loader slot is (cohort, tier, portal history, strategy details, binding); the window's
    projection loads without strategy details and only `compute_realtime_dashboard` loads with them, so with no
    Dashboard in the window (JOB mode) the scheduler retains one slot."""
    import inspect
    from realtime import dashboard_repository, stage_runners_equity, ticker_repository
    assert "include_strategy_action_details=True" in inspect.getsource(dashboard_repository.compute_realtime_dashboard)
    for module in (ticker_repository, stage_runners_equity):
        assert "include_strategy_action_details=True" not in inspect.getsource(module)


# --- round 2: prepared before the open, published after the first window ----------------------------------------------

class _CohortCursor:
    def __init__(self, effective_at):
        self.effective_at = effective_at

    def execute(self, sql, params=()):
        self.sql = sql

    def fetchone(self):
        return None if self.effective_at is None else dict(
            effective_at=self.effective_at, created_at=getattr(self, "created_at", self.effective_at - timedelta(minutes=15)))


def test_a_run_before_the_open_uses_the_cohorts_effective_time_as_its_cutoff():
    opened = datetime(2026, 11, 16, 14, 30, tzinfo=UTC)
    before = datetime(2026, 11, 16, 14, 16, tzinfo=UTC)
    after = datetime(2026, 11, 16, 15, 0, tzinfo=UTC)
    assert job.default_cutoff_cursor(_CohortCursor(opened), UUID(int=1), before) == opened
    assert job.default_cutoff_cursor(_CohortCursor(opened), UUID(int=1), after) == after
    late = _CohortCursor(opened)
    late.created_at = datetime(2026, 11, 16, 14, 35, 26, tzinfo=UTC)  # Nov 13: activation after the open
    assert job.default_cutoff_cursor(late, UUID(int=1), datetime(2026, 11, 16, 14, 35, tzinfo=UTC)) == late.created_at
    with pytest.raises(ValueError, match="cohort"):
        job.default_cutoff_cursor(_CohortCursor(None), UUID(int=1), before)


class _PreparedCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, sql, params=()):
        self.params = params

    def fetchall(self):
        return self.rows


def _stored_row(snapshot, **changes):
    from realtime.contracts import canonical_json
    import json
    row = dict(snapshot_id=snapshot.snapshot_id, cohort_id=snapshot.cohort_id, provider_tier=snapshot.provider_tier.value,
               snapshot_type=snapshot.snapshot_type, snapshot_key=snapshot.snapshot_key,
               source_generation=snapshot.source_generation, source_manifest_sha256=snapshot.source_manifest_sha256,
               payload_sha256=snapshot.payload_sha256, generated_at=snapshot.generated_at,
               valid_until=snapshot.valid_until,
               source_manifest=json.loads(canonical_json(snapshot.source_manifest), parse_float=lambda t: float(t) + 0.0),
               payload=json.loads(canonical_json(snapshot.payload), parse_float=lambda t: float(t) + 0.0))
    row.update(changes)
    return row


def test_prepared_products_are_found_by_input_identity_and_proven_equal_to_their_stored_rows(job_products):
    strategy, dashboard = job_products.strategy.snapshot, job_products.dashboard
    rows = [_stored_row(dashboard), _stored_row(strategy)]
    found = job.prepared_daily_products_cursor(_PreparedCursor(rows), strategy.cohort_id, job.MODE_JOB,
                                               job_products.input_sha256)
    assert {key: snapshot.snapshot_id for key, snapshot in found.items()} == {
        (HISTORY_VERSION, "COHORT"): strategy.snapshot_id, ("DASHBOARD_V1", "COHORT"): dashboard.snapshot_id}
    assert job.prepared_daily_products_cursor(_PreparedCursor(rows[:1]), strategy.cohort_id, job.MODE_JOB,
                                              job_products.input_sha256) == {}
    tampered = [rows[0], _stored_row(strategy, payload_sha256="0" * 64)]
    with pytest.raises(ValueError, match="Prepared daily product"):
        job.prepared_daily_products_cursor(_PreparedCursor(tampered), strategy.cohort_id, job.MODE_JOB,
                                           job_products.input_sha256)


def test_compute_returns_prepared_products_without_computing(monkeypatch, job_products):
    inputs = job_products.inputs
    _patch_load(monkeypatch, inputs, {})
    prepared = {(HISTORY_VERSION, "COHORT"): job_products.strategy.snapshot,
                ("DASHBOARD_V1", "COHORT"): job_products.dashboard}
    monkeypatch.setattr(job, "prepared_daily_products_cursor", lambda cursor, cohort, mode, identity, **_: prepared)
    monkeypatch.setattr(job, "project_realtime_strategy_history", _refuse)
    products, stored = job.compute_daily_products(_compute_db(), cohort_id=inputs.cohort_id, session=inputs.session,
                                                  cutoff=inputs.cutoff, mode=job.MODE_JOB)
    assert stored is None and products.prepared is True
    assert products.strategy.snapshot is prepared[(HISTORY_VERSION, "COHORT")]
    assert products.dashboard is prepared[("DASHBOARD_V1", "COHORT")]


def _run_databases():
    from realtime.database_access import RealtimeDatabaseRole
    from realtime.stage_database_v2 import V2WorkerDatabase

    class Provider:
        def __init__(self):
            self.transactions = []

        @contextmanager
        def transaction(self, **options):
            self.transactions.append(options)
            yield SimpleNamespace(cursor="cursor")

    provider = Provider()
    return (V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE),
            V2WorkerDatabase(provider, RealtimeDatabaseRole.PUBLICATION), provider)


@pytest.mark.parametrize("prepared", [False, True])
def test_prepare_stores_without_a_release_and_publish_only_releases_without_writing_evidence(
        monkeypatch, job_products, prepared):
    from dataclasses import replace as dc_replace
    compute, publication, provider = _run_databases()
    products = dc_replace(job_products, prepared=prepared)
    calls = []
    monkeypatch.setattr(job, "compute_daily_products", lambda *a, **k: (products, None))
    monkeypatch.setattr(job, "write_strategy_evidence", lambda database, projection: calls.append("evidence") or 7)
    monkeypatch.setattr(job, "store_daily_products_cursor", lambda cursor, products: calls.append("store"))
    monkeypatch.setattr(job, "publish_daily_products_cursor",
                        lambda cursor, products: calls.append("release") or (job.PUBLISHED, None, None))
    when = products.inputs.cutoff
    outcome = job.run_daily_products(compute, publication, cohort_id=products.inputs.cohort_id,
                                     session=products.inputs.session, cutoff=when, mode=job.MODE_JOB, prepare=True)
    if prepared:
        assert outcome.status == job.ALREADY_PREPARED and calls == []
    else:
        assert outcome.status == job.PREPARED and calls == ["evidence", "store"]
    calls.clear()
    outcome = job.run_daily_products(compute, publication, cohort_id=products.inputs.cohort_id,
                                     session=products.inputs.session, cutoff=when, mode=job.MODE_JOB)
    assert outcome.status == job.PUBLISHED
    assert calls == (["release"] if prepared else ["evidence", "release"])


def test_launcher_prepares_before_the_open_and_publishes_after_the_first_window(tmp_path, monkeypatch):
    monkeypatch.setattr(launcher, "emit_fault", lambda **record: pytest.fail(str(record)))
    prepare = launcher.DueProducts(str(UUID(int=9)), SESSION, launcher.PRE_OPEN)
    publish = launcher.DueProducts(str(UUID(int=9)), SESSION, launcher.SESSION_COHORT)
    assert prepare.key != publish.key
    instance, started = _launcher(tmp_path, [_Process([0]), _Process([0])])
    instance.tick(T0, lambda at: [prepare])
    assert "--prepare" in started[0][0]
    instance.tick(T0 + timedelta(seconds=1), lambda at: [prepare])  # done; the next check waits 60 s
    assert len(started) == 1 and prepare.key in instance.done
    instance.check_soon()  # a window just finished
    instance.tick(T0 + timedelta(seconds=2), lambda at: [publish, prepare])
    assert len(started) == 2 and "--prepare" not in started[1][0]
    assert started[1][0][started[1][0].index("--trigger") + 1] == launcher.SESSION_COHORT


def test_launcher_deadline_comes_from_the_environment(tmp_path):
    instance = launcher.DailyProductsLauncher(report_directory=tmp_path,
                                              environ={"REALTIME_V2_DAILY_PRODUCTS_DEADLINE_SECONDS": "1500"})
    assert instance.deadline == timedelta(seconds=1500)
    assert launcher.DailyProductsLauncher(report_directory=tmp_path, environ={}).deadline == timedelta(minutes=10)


def test_discovery_prepares_a_new_cohort_only_before_its_open_and_without_a_release():
    from options.calendar import OptionExchangeCalendar
    from realtime.advanced_equity_lifecycle import AdvancedEquityLifecyclePhase

    class Cursor:
        def execute(self, sql, params=()):
            pass

        def fetchone(self):
            return None  # no release yet

    class Provider:
        @contextmanager
        def transaction(self, role, read_only=False):
            yield SimpleNamespace(cursor=Cursor())

    calendar = OptionExchangeCalendar("XNYS")
    target = date(2026, 11, 16)

    def discover(now):
        return launcher.discover_due_daily_products(
            Provider(), lifecycle=SimpleNamespace(target_session_date=target,
                                                  phase=AdvancedEquityLifecyclePhase.BEFORE_OPEN),
            calendar=calendar, now=now, load_scope=lambda cursor, session_date: (
                dict(cohort_id="new-cohort") if session_date == target else None),
            recovery_complete=lambda cursor, **options: False, app_role="app")
    assert [(d.cohort_id, d.session, d.trigger) for d in discover(datetime(2026, 11, 16, 14, 16, tzinfo=UTC))] == [
        ("new-cohort", SESSION, launcher.PRE_OPEN)]
    assert discover(datetime(2026, 11, 16, 14, 31, tzinfo=UTC)) == []


# --- round 2: Pattern Watch light 30m/1h frames ------------------------------------------------------------------------

def _two_names():
    from tests.realtime.test_ticker_projection_state import two_names
    return two_names()


@pytest.fixture(scope="module")
def pattern_projections():
    import os
    from realtime.ticker_projection import (
        PATTERN_WATCH_SWITCH, TickerProjectionState, produce_realtime_pattern_watch_publication,
        project_realtime_ticker_workspace,
    )
    inputs = _two_names()
    saved = os.environ.pop(PATTERN_WATCH_SWITCH, None)
    try:
        full = project_realtime_ticker_workspace(inputs, state=TickerProjectionState())
        full_watch = produce_realtime_pattern_watch_publication(inputs, full.snapshots)
        os.environ[PATTERN_WATCH_SWITCH] = "LIGHT"
        light = project_realtime_ticker_workspace(inputs, state=TickerProjectionState())
        light_watch = produce_realtime_pattern_watch_publication(inputs, light.snapshots)
    finally:
        os.environ.pop(PATTERN_WATCH_SWITCH, None)
        if saved is not None:
            os.environ[PATTERN_WATCH_SWITCH] = saved
    return full, full_watch, light, light_watch


def test_light_pattern_rows_equal_the_full_pipeline_rows(pattern_projections):
    full, full_watch, light, light_watch = pattern_projections
    for full_snapshot, light_snapshot in zip(full.snapshots, light.snapshots):
        assert full_snapshot.snapshot_key == light_snapshot.snapshot_key
        for interval in ("30m", "1h", "1d", "1wk", "1mo"):
            assert light_snapshot.payload["patterns"].get(interval) == full_snapshot.payload["patterns"].get(interval)
            assert light_snapshot.payload["channels"].get(interval) == full_snapshot.payload["channels"].get(interval)
        assert set(full_snapshot.payload["pattern_evidence_refs"]) - set(light_snapshot.payload["pattern_evidence_refs"]) \
            <= {"30m", "1h"}
        assert not {"30m", "1h"} & set(light_snapshot.payload["pattern_evidence_refs"])
        assert light_snapshot.payload["multi"] == full_snapshot.payload["multi"]  # setups untouched
    for interval in ("30m", "1h", "1d", "1wk", "1mo"):
        full_view, light_view = full_watch.payload["views"][interval], light_watch.payload["views"][interval]
        assert light_view["results"] == full_view["results"], interval
        assert light_view["market_time"] == full_view["market_time"] and light_view["status"] == full_view["status"]
    assert light_watch.payload["views"]["30m"]["evidence_storage"] == "RELEASE_ONLY"
    assert light_watch.payload["views"]["1h"]["evidence_storage"] == "RELEASE_ONLY"
    assert light_watch.payload["views"]["1d"]["evidence_storage"] == "IMMUTABLE_EVIDENCE"
    assert light_watch.payload["views"]["30m"]["analysis_refs"] == {}


def test_light_frames_store_no_pattern_evidence_and_the_evidence_parity_holds(pattern_projections):
    from realtime.ticker_repository import _verify_ticker_evidence_parity
    full, _full_watch, light, _light_watch = pattern_projections
    assert len(light.pattern_materializations) < len(full.pattern_materializations)
    assert {m.interval for m in light.pattern_materializations} <= {"1d", "1wk", "1mo"}
    assert [m.input_sha256 for m in light.materializations] == [m.input_sha256 for m in full.materializations]
    _verify_ticker_evidence_parity(light)
    _verify_ticker_evidence_parity(full)


def test_strategy_reader_accepts_the_published_reference_among_overlapping_current_revisions():
    """R78 live check, Nov 14: after a reference refresh 10+ tickers had two current revisions (overlapping validity),
    and the strategy reader refused the release; the Dashboard reader has accepted that since G97b (Nov 6)."""
    from realtime.strategy_repository import _reference_matches
    reference = dict(security_revision_id="r1", payload_sha256="a" * 64)
    assert _reference_matches(reference, [dict(revision_id="r1", payload_sha256="a" * 64)])
    assert _reference_matches(reference, [dict(revision_id="r2", payload_sha256="b" * 64),
                                          dict(revision_id="r1", payload_sha256="a" * 64)])
    assert not _reference_matches(reference, [dict(revision_id="r2", payload_sha256="b" * 64)])
    assert not _reference_matches(reference, [dict(revision_id="r1", payload_sha256="c" * 64)])
    assert not _reference_matches(reference, [])


def test_latest_session_strategy_release_is_selectable_and_feeds_the_dashboard(monkeypatch):
    """REALTIME_V2_DAILY_STRATEGY_SESSIONS=1: DAILY_STRATEGIES_V1 (the latest session, ~a fifth of the cost and the size
    of the 5-session history) instead of DAILY_STRATEGY_HISTORY_V1; the Dashboard reuses its setups."""
    from realtime.strategy_repository import _validate_strategy_snapshot
    assert job.strategy_sessions({}) == 5 and job.strategy_sessions({job.STRATEGY_SESSIONS_SWITCH: "1"}) == 1
    assert job.strategy_sessions({job.STRATEGY_SESSIONS_SWITCH: "3"}) == 5
    assert job.products_for(job.MODE_JOB, 1)[0] == ("DAILY_STRATEGIES_V1", "COHORT")
    assert job.products_for(job.MODE_STRATEGIES, 5) == ((HISTORY_VERSION, "COHORT"),)
    inputs = _narrow(daily_scanner_fixture())
    assert (job.daily_products_input_sha256(inputs, job.MODE_JOB, 1)
            != job.daily_products_input_sha256(inputs, job.MODE_JOB, 5))
    _patch_load(monkeypatch, inputs, {})
    products, stored = job.compute_daily_products(_compute_db(), cohort_id=inputs.cohort_id, session=inputs.session,
                                                  cutoff=inputs.cutoff, mode=job.MODE_JOB, sessions=1)
    strategy = products.strategy.snapshot
    assert stored is None and products.sessions == 1 and strategy.snapshot_type == "DAILY_STRATEGIES_V1"
    assert strategy.source_manifest[job.DAILY_PRODUCTS_MARKER]["strategy_sessions"] == 1
    _validate_strategy_snapshot(strategy.source_manifest, strategy.payload)
    assert products.dashboard.payload["setup_summary"]["results"] == strategy.payload["setup_summary"]["results"]
