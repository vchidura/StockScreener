import pytest
from pydantic import ValidationError

from research.screening import Predicate, Query, VERSION, combine, evaluate, predicate_hash, query_generation


def test_source_gap_repair_rejects_bny_fund_and_wrong_session():
    from datetime import datetime, timezone
    from scripts.repair_screening_source_gaps import check_identity, selected_daily, targets

    with pytest.raises(ValueError, match='identity mismatch'):
        check_identity('BNY', dict(ticker='BNY', composite_figi='BBG000BZF6G2', type='FUND'),
                       datetime.now(timezone.utc), '2025-09-05')
    with pytest.raises(ValueError, match='gaps remain'):
        selected_daily('ARKW', [], ['2026-09-09'])
    with pytest.raises(ValueError, match='Incomplete'):
        selected_daily('ARKW', [dict(t=1788926400000, o=158.77, h=159.06, l=156.11, c=156.11)], ['2026-09-09'])
    assert targets()['BK'][-1] == '2026-05-20'
    assert set(targets()) == {'ARKW', 'BKNG', 'BK'}


def test_source_repair_retains_bk_lineage_and_actual_availability(monkeypatch):
    from datetime import datetime, timezone
    from uuid import uuid4
    from scripts import repair_screening_source_gaps as repair

    session = '2025-09-05'
    observed = '2026-09-13T23:00:00+00:00'
    raw = dict(t=1757044800000, o=105.65, h=106.056, l=102.895, c=103.69, v=2780540)
    monkeypatch.setattr(repair, 'targets', lambda: {'BK': [session]})
    envelope = dict(sha256='e' * 64, evidence=dict(daily=dict(BK=dict(adjusted=False, observed_at=observed, rows={session: raw}))))
    old_id = uuid4()
    old = {('BNY', session): [dict(bar_revision_id=old_id, source_kind='DERIVED')]}
    segment, native, continuity = repair.prepare_bars(envelope, old)
    assert len(native) == len(continuity) == 1
    assert native[0].ticker == 'BK' and continuity[0].ticker == 'BNY'
    assert native[0].security_id == continuity[0].security_id
    assert continuity[0].source_bar_revision_ids == (native[0].bar_revision_id,)
    assert continuity[0].supersedes_bar_revision_id == old_id
    assert continuity[0].reconciliation_status == 'CORRECTED'
    assert continuity[0].source_kind.value == 'RECONCILED'
    assert continuity[0].close_price == native[0].close_price
    assert all(bar.system_observed_at == datetime.fromisoformat(observed) and bar.replay_available_at is None
               and bar.availability_mode.value == 'LIVE_OBSERVED' for bar in native + continuity)
    assert repair.prepare_bars(envelope, old) == (segment, native, continuity)
    _, _, missing = repair.prepare_bars(envelope, {})
    assert missing[0].supersedes_bar_revision_id is None
    assert missing[0].reconciliation_status == 'DERIVED_MISSING'


def test_source_repair_shared_transaction_defers_commits_and_rolls_back(monkeypatch):
    from contextlib import contextmanager
    from scripts import repair_screening_source_gaps as repair

    calls = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, *args):
            pass

    class Connection:
        def cursor(self, **kwargs):
            return Cursor()

        def commit(self):
            calls.append('commit')

        def rollback(self):
            calls.append('rollback')

    @contextmanager
    def connection():
        yield Connection()

    monkeypatch.setattr(repair, 'get_db_connection', connection)
    with repair.shared_transaction() as factory:
        with factory() as borrowed:
            borrowed.commit()
            assert calls == []
    assert calls == ['commit']
    with pytest.raises(RuntimeError):
        with repair.shared_transaction():
            raise RuntimeError('interrupted')
    assert calls == ['commit', 'rollback']


def row(identity="a", **values):
    return dict(security_id=identity, ticker=identity.upper(), eligible=True, values=values, missing={}, patterns={})


def test_field_level_unknown_and_known_failure():
    recent = row(price=10., volume=100., momentum_12_1=None)
    price = Predicate(filters=[{"field": "price", "min": 0.}])
    assert evaluate(recent, price)[0] == "MATCH"
    price.filters.append(Predicate(filters=[{"field": "momentum_12_1", "min": 0.}]).filters[0])
    assert evaluate(recent, price)[0] == "UNKNOWN"
    price.filters[0].min = 11.
    assert evaluate(recent, price)[0] == "NO_MATCH"


@pytest.mark.parametrize("condition", [
    {"field": "price", "min": 2., "max": 1.}, {"field": "price", "min": float("nan")},
    {"field": "price", "max": float("inf")}, {"field": "price"},
    {"field": "sql", "min": 0.}, {"field": "price", "min": "0"},
    {"field": "instrument_type", "values": ["MADE_UP"]},
])
def test_invalid_filters(condition):
    with pytest.raises(ValidationError):
        Predicate(filters=[condition])


def test_inclusive_zero_and_three_valued_groups():
    assert evaluate(row(change=0.), Predicate(filters=[{"field": "change", "min": 0., "max": 0.}]))[0] == "MATCH"
    assert combine(["UNKNOWN", "MATCH"], "ANY") == "MATCH"
    assert combine(["UNKNOWN", "NO_MATCH"], "ALL") == "NO_MATCH"
    assert combine(["UNKNOWN", "NO_MATCH"], "NONE") == "UNKNOWN"


def test_pagination_counts_ties_and_original_ranks():
    generation = dict(version=VERSION, generation="fixed", rows=[row("b", price=3., momentum_percentile=.7), row("a", price=3., momentum_percentile=.2), row("c", price=None)])
    query = Query(predicate=Predicate(filters=[{"field": "price", "min": 0.}]), sort="price", descending=True, limit=1)
    result = query_generation(generation, query)
    assert (result["matched_count"], result["unknown_count"]) == (2, 1)
    assert result["rows"][0]["security_id"] == "a"
    assert result["rows"][0]["values"]["momentum_percentile"] == .2
    query.offset = 1
    assert query_generation(generation, query)["rows"][0]["security_id"] == "b"
    query.generation = "missing"
    with pytest.raises(ValueError):
        query_generation(generation, query)


def test_predicate_hash_is_order_independent():
    filters = [{"field": "price", "min": 0.}, {"field": "volume", "min": 1.}]
    assert predicate_hash(Predicate(filters=filters)) == predicate_hash(Predicate(filters=list(reversed(filters))))


def comparable_generations(prior_values, current_values):
    from research.screening import FIELD_SET_VERSION, UNIVERSE
    shared = dict(version=VERSION, universe=UNIVERSE, interval="1d", field_set_version=FIELD_SET_VERSION,
                  contract_hash="contract", capture_mode="RECONSTRUCTED_FROM_RETAINED_PUBLICATION",
                  code_hashes={"technicals.py": "candles", "stock_discovery.py": "states"})
    previous = dict(shared, generation="prior", session="2026-09-10", source_cutoff="2026-09-11T03:00:00+00:00", rows=[], lineage={})
    current = dict(shared, generation="current", session="2026-09-11", previous_expected_session="2026-09-10", source_cutoff="2026-09-11T20:35:00+00:00", rows=[], lineage={})
    for generation, values, latest in ((previous, prior_values, "prior"), (current, current_values, "current")):
        for identity, price in values.items():
            generation["rows"].append(row(identity, price=price, instrument_type="CS") | {"source_bar_id": f"{identity}-{latest}"})
            generation["lineage"][identity] = dict(bars=[f"{identity}-prior", f"{identity}-current"] if latest == "current" else [f"{identity}-prior"], actions=[])
    return previous, current


def test_new_filter_compares_same_rules_before_pagination_without_changing_base_counts():
    previous, current = comparable_generations(dict(a=9., b=11., c=None, d=8.), dict(a=10., b=12., c=10., d=11., e=15., f=7.))
    request = Query(predicate=Predicate(filters=[dict(field="price", min=10.)]), new_only=True, limit=1)
    result = query_generation(current, request, previous)
    assert result["matched_count"] == 5 and result["result_count"] == 2
    assert result["comparison"] == dict(status="READY", current_session="2026-09-11", previous_session="2026-09-10",
        previous_generation="prior", new_count=2, unavailable_count=2)
    assert result["rows"][0]["security_id"] == "a" and result["rows"][0]["new_status"] == "NEW"
    request.offset = 1
    assert query_generation(current, request, previous)["rows"][0]["security_id"] == "d"
    request.new_only, request.offset, request.limit = False, 0, 100
    assert query_generation(current, request, previous)["matched_count"] == 5
    request.predicate = Predicate(filters=[dict(field="price", min=8.)])
    assert query_generation(current, request, previous)["comparison"]["new_count"] == 0


@pytest.mark.parametrize("defect", ["missing_day", "same_day", "contract", "detector", "correction_mode", "late_revision", "identity", "eligibility", "unknown", "lineage", "actions"])
def test_new_never_labels_missing_ineligible_incompatible_or_corrected_data(defect):
    previous, current = comparable_generations(dict(a=9.), dict(a=10.))
    if defect == "missing_day":
        previous = None
    elif defect == "same_day":
        previous["session"] = current["session"]
    elif defect == "contract":
        previous["contract_hash"] = "other"
    elif defect == "detector":
        previous["code_hashes"] = dict(previous["code_hashes"], **{"technicals.py": "different"})
    elif defect == "correction_mode":
        current["capture_mode"] = "CORRECTED"
    elif defect == "late_revision":
        previous["source_cutoff"] = "2026-09-13T20:00:00+00:00"
    elif defect == "identity":
        previous["rows"][0]["security_id"] = "other"
    elif defect == "eligibility":
        previous["rows"][0]["eligible"] = False
    elif defect == "unknown":
        previous["rows"][0]["values"]["price"] = None
    elif defect == "lineage":
        current["lineage"]["a"]["bars"][0] = "corrected-prior"
    else:
        current["lineage"]["a"]["actions"] = ["new-split"]
    request = Query(predicate=Predicate(filters=[dict(field="price", min=10.)]))
    result = query_generation(current, request, previous)
    assert result["rows"][0]["new_status"] == "UNAVAILABLE"
    assert result["comparison"]["unavailable_count"] == 1
    request.new_only = True
    assert query_generation(current, request, previous)["rows"] == []


def test_reader_resolves_exact_previous_session_and_never_substitutes_revision(monkeypatch):
    from contextlib import contextmanager
    from equity import screening_api
    previous, current = comparable_generations(dict(a=9.), dict(a=10.))
    current["market_time"] = "2026-09-11T20:00:00+00:00"
    publications = [dict(snapshot_id="current", source_manifest=current), dict(snapshot_id="prior", source_manifest=previous)]
    loaded = []
    statements = []

    def load(identity):
        loaded.append(identity)
        return {"current": current, "prior": previous, "correction": previous | {"contract_hash": "changed"}}[identity]

    class Cursor:
        def execute(self, sql, parameters=None):
            statements.append((sql, parameters))

        def fetchone(self):
            return None

    @contextmanager
    def cursor():
        yield Cursor()

    monkeypatch.setattr(screening_api, "load_generation", load)
    monkeypatch.setattr(screening_api, "publication_index", lambda: publications)
    monkeypatch.setattr(screening_api, "get_db_cursor", cursor)
    request = Query(predicate=Predicate(filters=[dict(field="price", min=10.)]), new_only=True)
    assert screening_api.query(request)["result_count"] == 1
    assert loaded == ["current", "prior"]
    publications.insert(1, dict(snapshot_id="correction", source_manifest=previous))
    assert screening_api.query(request)["comparison"]["status"] == "INCOMPATIBLE_PUBLICATIONS"
    publications[:] = publications[:1]
    response = screening_api.query(request)
    assert response["comparison"]["status"] == "PRIOR_SESSION_UNAVAILABLE" and response["rows"] == []
    assert statements[-1][1] == (screening_api.SNAPSHOT_TYPE, "2026-09-10")
    current["session"], current["previous_expected_session"] = "2026-09-08", "2026-09-04"
    assert screening_api.previous_generation(current, [dict(snapshot_id="prior", source_manifest=previous | {"session": "2026-09-04"})]) == previous
    current["previous_expected_session"] = "2026-09-03"
    assert screening_api.previous_generation(current, publications) is None


def test_pattern_none_never_treats_unknown_as_absent():
    rule = Predicate(patterns=[{"id": "bullish_engulfing", "direction": 1}], pattern_mode="NONE")
    assert evaluate(row(), rule)[0] == "UNKNOWN"
    known = row() | {"patterns": {"bullish_engulfing": {"present": False}}}
    assert evaluate(known, rule)[0] == "MATCH"
    with pytest.raises(ValidationError):
        Predicate(patterns=[{"id": "bullish_engulfing", "direction": -1}])


def test_gap_projection_has_stable_episodes_fixed_boundaries_and_nonterminal_failed():
    import pandas as pd
    from equity.screening_gaps import project_gaps

    dates = pd.bdate_range("2026-08-01", periods=23)
    bars = [dict(session_date=stamp.date(), bar_revision_id=f"bar-{index}", open=100., high=101., low=99., close=100., volume=100.)
            for index, stamp in enumerate(dates)]
    bars[-3].update(open=105., high=107., low=104., close=106.)
    bars[-2].update(open=104., high=105., low=98., close=99.)
    first = project_gaps(pd.DataFrame(bars[:-1]), "bank")
    gap = next(episode for episode in first if episode["values"]["direction"] == "UP")
    assert gap["values"]["state"] == "FAILED"
    assert (gap["zone_lower"], gap["zone_upper"], gap["fill_target"]) == (101., 104., 100.)
    bars[-1].update(open=100., high=103., low=99., close=102.)
    following = project_gaps(pd.DataFrame(bars[1:]), "bank")
    recovered = next(episode for episode in following if episode["episode_id"] == gap["episode_id"])
    assert recovered["values"]["state"] == "FILLED"
    assert recovered["values"]["formation_age"] == gap["values"]["formation_age"] + 1
    assert recovered["values"]["fill_fraction"] == 1.
    assert recovered["values"]["price_location"] == "INSIDE"
    assert recovered["values"]["distance_fraction"] == pytest.approx(1 / 101)
    assert recovered["formation_bar_id"] == gap["formation_bar_id"]
    assert recovered["zone_lower"] == gap["zone_lower"] and recovered["zone_upper"] == gap["zone_upper"]
    assert project_gaps(pd.DataFrame(bars[1:]), "other-security")[0]["episode_id"] != following[0]["episode_id"]
    with pytest.raises(ValueError, match="22 verified"):
        project_gaps(pd.DataFrame(bars[:10]), "bank")


def test_gap_conditions_require_one_episode_and_preserve_unknown_coverage():
    from research.screening import GAP_VERSION, GapPredicate, evaluate_gaps

    def episode(identity, direction, age, fill):
        return dict(episode_id=identity, values=dict(direction=direction, formation_age=age, fill_fraction=fill, distance_fraction=.01))

    context = dict(version=GAP_VERSION, status="READY", episodes=[episode("old-up", "UP", 15, .1), episode("new-down", "DOWN", 2, .2)])
    gap = GapPredicate(filters=[dict(field="direction", values=["UP"]), dict(field="formation_age", max=5.)])
    assert evaluate_gaps(context, gap)[0] == "NO_MATCH"
    context["episodes"].append(episode("new-up", "UP", 3, .4))
    assert [item["episode_id"] for item in evaluate_gaps(context, gap)[1]] == ["new-up"]
    assert evaluate_gaps(dict(version=GAP_VERSION, status="UNAVAILABLE", reason="MISSING_EXPECTED_SESSION", episodes=[]), gap)[0] == "UNKNOWN"
    assert evaluate_gaps(dict(version=GAP_VERSION, status="READY", episodes=[]), gap)[0] == "NO_MATCH"
    assert evaluate(row(price=10.), Predicate())[0] == "MATCH"
    assert evaluate(row(price=10.), Predicate(gap=gap))[0] == "UNKNOWN"
    assert evaluate(row(price=1.), Predicate(filters=[dict(field="price", min=10.)], gap=gap))[0] == "NO_MATCH"
    for filters in ([dict(field="formation_age", max=21.)], [dict(field="formation_age", min=1.5)],
                    [dict(field="fill_fraction", max=1.1)], [dict(field="distance_fraction", min=-1.)],
                    [dict(field="state", values=["PERMANENTLY_INVALIDATED"])], [dict(field="price", min=10.)]):
        with pytest.raises(ValidationError):
            GapPredicate(filters=filters)


def test_gap_predicate_hash_is_optional_order_independent_and_old_generations_fail_closed():
    from research.screening import digest
    original = Predicate().model_dump(exclude={"gap", "hourly"})
    assert predicate_hash(Predicate()) == digest(original)
    first = Predicate(gap=dict(filters=[dict(field="direction", values=["UP", "DOWN"]), dict(field="formation_age", max=5.)]))
    second = Predicate(gap=dict(filters=[dict(field="formation_age", max=5.), dict(field="direction", values=["DOWN", "UP"])]))
    assert predicate_hash(first) == predicate_hash(second)
    with pytest.raises(ValueError, match="gap context unavailable"):
        query_generation(dict(version=VERSION, generation="old", rows=[row(price=10.)]), Query(predicate=first))


def test_projection_field_windows_future_bars_actions_and_identity():
    from datetime import date, timedelta
    from equity.screening_projection import project_security
    start = date(2026, 1, 1)
    bars = [dict(session_date=start + timedelta(days=index), security_id="a", bar_revision_id=str(index),
                 open=10., high=12., low=9., close=11., volume=100.) for index in range(30)]
    session = bars[20]["session_date"]
    member = dict(security_id="a", ticker="A", selected_bar_revision_id="20", security_type="CS")
    ordinals = {bar["session_date"]: index for index, bar in enumerate(bars)}
    result = project_security(member, bars, [], session, ordinals)
    assert result["eligible"] and result["values"]["price"] == 11.
    assert result["values"]["dollar_volume_20"] == 1100.
    assert result["values"]["relative_volume_20"] == 1.
    assert result["values"]["momentum_12_1"] is None
    assert project_security(member, bars[:21], [], session, ordinals) == result
    action = dict(effective_date=session, action_type="SPLIT")
    action_result = project_security(member, bars, [action], session, ordinals)
    assert action_result["values"]["price"] == 11.
    assert action_result["values"]["change"] is None
    assert action_result["patterns"] == {}
    bars[19]["security_id"] = "other"
    assert project_security(member, bars, [], session, ordinals)["values"]["change"] is None
    bars[19]["security_id"] = "a"
    assert project_security(member, bars[:19] + bars[20:], [], session, ordinals)["values"]["relative_volume_20"] is None


@pytest.mark.parametrize("field,size", [("momentum_12_1", 253), ("momentum_6_1", 127), ("momentum_3_1", 64)])
def test_momentum_horizons_exact_windows_and_skipped_month(field, size):
    from datetime import date, timedelta
    from equity.screening_projection import project_security
    from research.screening import FIELDS
    bars = [dict(session_date=date(2025, 1, 1) + timedelta(days=index), security_id="a", bar_revision_id=str(index),
                 open=100. + index, high=102. + index, low=99. + index, close=100. + index, volume=100.) for index in range(254)]
    session = bars[252]["session_date"]
    member = dict(security_id="a", ticker="A", selected_bar_revision_id="252", security_type="CS")
    ordinals = {bar["session_date"]: index for index, bar in enumerate(bars)}
    assert FIELDS[field]["warmup_sessions"] == size
    result = project_security(member, bars, [], session, ordinals)
    expected = bars[231]["close"] / bars[253 - size]["close"] - 1
    assert result["values"][field] == pytest.approx(expected)
    assert project_security(member, bars[253 - size:253], [], session, ordinals)["values"][field] == pytest.approx(expected)
    short = project_security(member, bars[254 - size:253], [], session, ordinals)
    assert short["values"][field] is None and short["missing"][field] == "INSUFFICIENT_HISTORY"
    for bar in bars[232:]:
        bar.update(open=50., high=51., low=49., close=50.)
    assert project_security(member, bars, [], session, ordinals)["values"][field] == pytest.approx(expected)


@pytest.mark.parametrize("issue", ["missing", "identity", "action", "invalid"])
def test_momentum_horizons_history_gates_are_independent(issue):
    from datetime import date, timedelta
    from equity.screening_projection import project_security
    bars = [dict(session_date=date(2025, 1, 1) + timedelta(days=index), security_id="a", bar_revision_id=str(index),
                 open=100., high=101., low=99., close=100., volume=100.) for index in range(253)]
    session = bars[-1]["session_date"]
    member = dict(security_id="a", ticker="A", selected_bar_revision_id="252", security_type="CS")
    ordinals = {bar["session_date"]: index for index, bar in enumerate(bars)}
    actions = []
    if issue == "missing":
        bars.pop(160)
    elif issue == "identity":
        bars[160]["security_id"] = "other"
    elif issue == "action":
        actions.append(dict(effective_date=bars[160]["session_date"], action_type="SPLIT"))
    else:
        bars[160]["close"] = float("nan")
    result = project_security(member, bars, actions, session, ordinals)
    assert result["values"]["momentum_12_1"] is None
    assert result["values"]["momentum_6_1"] is None
    assert result["values"]["momentum_3_1"] == 0.
    assert result["values"]["price"] == 100.
    predicate = Predicate(filters=[dict(field="momentum_6_1", min=0.)])
    assert evaluate(result, predicate)[0] == "UNKNOWN"
    assert evaluate(result, Predicate(filters=[dict(field="momentum_3_1", min=0., max=0.)]))[0] == "MATCH"


def test_gap_coverage_requires_complete_contiguous_identity_and_action_safe_window():
    from datetime import date, timedelta
    from equity.screening_projection import project_security
    bars = [dict(session_date=date(2026, 1, 1) + timedelta(days=index), security_id="a", bar_revision_id=str(index),
                 open=100., high=101., low=99., close=100., volume=100.) for index in range(22)]
    session = bars[-1]["session_date"]
    member = dict(security_id="a", ticker="A", selected_bar_revision_id="21", security_type="CS")
    ordinals = {bar["session_date"]: index for index, bar in enumerate(bars)}
    complete = project_security(member, bars, [], session, ordinals)
    assert complete["gaps"]["status"] == "READY" and complete["gaps"]["episodes"] == []
    for source, actions in ((bars[1:], []), (bars[:5] + bars[6:], []),
                            ([dict(bar, security_id="other") if index == 10 else bar for index, bar in enumerate(bars)], []),
                            (bars, [dict(effective_date=session, action_type="SPLIT")])):
        result = project_security(member, source, actions, session, ordinals)
        assert result["eligible"] and result["values"]["price"] == 100.
        assert result["gaps"]["status"] == "UNAVAILABLE" and result["gaps"]["episodes"] == []


def test_hourly_slots_include_short_close_holidays_and_partial_session():
    from datetime import datetime, timedelta, timezone
    import exchange_calendars
    from equity.screening_hourly import hourly_slots
    calendar = exchange_calendars.get_calendar("XNYS")
    close = datetime(2026, 11, 27, 18, tzinfo=timezone.utc)
    slots = hourly_slots(calendar, close)
    assert len(slots) == 23 and slots[-1] == (close - timedelta(minutes=30), close)
    assert not any(start.date().isoformat() == "2026-11-26" for start, _ in slots)
    partial = hourly_slots(calendar, close - timedelta(minutes=15))
    assert partial[-1][1] == close - timedelta(minutes=30)
    assert all(end <= close - timedelta(minutes=15) for _, end in partial)


def test_hourly_fields_use_exact_slots_and_field_level_unavailability():
    from datetime import datetime, timezone
    import exchange_calendars
    import pandas as pd
    from equity.screening_hourly import hourly_slots, project_hourly
    slots = hourly_slots(exchange_calendars.get_calendar("XNYS"), datetime(2026, 9, 11, 20, tzinfo=timezone.utc))
    bars = [dict(bar_start=start, bar_end=end, security_id="a", bar_revision_id=str(index),
                 open=100. + index, high=102. + index, low=99. + index, close=101. + index, volume=100.) for index, (start, end) in enumerate(slots)]
    result = project_hourly("a", "22", bars, [], slots)
    assert result["missing"] == {} and result["values"]["close"] == 123.
    assert result["values"]["change"] == pytest.approx(123 / 122 - 1)
    closes = pd.Series([bar["close"] for bar in bars])
    assert result["values"]["vs_ema20"] == pytest.approx(123 / closes.tail(20).ewm(span=20, adjust=False).mean().iloc[-1] - 1)
    average = closes.ewm(span=20, adjust=False).mean()
    assert result["values"]["ema20_change_3"] == pytest.approx(average.iloc[-1] / average.iloc[-4] - 1)
    missing = project_hourly("a", "22", bars[1:], [], slots)
    assert missing["values"]["vs_ema20"] is not None and missing["values"]["ema20_change_3"] is None
    for bad in (bars[:-1], [dict(bar, security_id="other") for bar in bars]):
        assert project_hourly("a", "22", bad, [], slots)["values"]["close"] is None
    assert project_hourly("a", None, bars, [], slots)["values"]["close"] is None
    assert project_hourly("a", "22", bars, [], slots, source_ready=False)["values"]["close"] is None
    action = dict(effective_date=slots[-1][1].date(), action_type="SPLIT")
    gated = project_hourly("a", "22", bars, [action], slots)
    assert gated["values"]["close"] == 123. and gated["values"]["change"] is None


@pytest.mark.parametrize('refreshed', [False, True])
def test_hourly_predicate_is_separate_strict_and_never_claims_daily_new(refreshed):
    from research.screening import HOURLY_CATALOG, REFRESHED_HOURLY_CATALOG, HourlyPredicate
    predicate = Predicate(hourly=dict(filters=[dict(field="vs_ema20", min=0.)]))
    current = row(price=10.) | {"hourly": dict(values=dict(vs_ema20=.01), missing={})}
    assert evaluate(current, predicate)[0] == "MATCH"
    assert evaluate(row(price=10.), predicate)[0] == "UNKNOWN"
    generation = dict(version=VERSION, generation="hourly", rows=[current], hourly_contract=REFRESHED_HOURLY_CATALOG if refreshed else HOURLY_CATALOG)
    result = query_generation(generation, Query(predicate=predicate))
    assert result["comparison"]["status"] == "HOURLY_COMPARISON_NOT_ENABLED"
    assert result["rows"][0]["explanations"][-1]["conditions"][0]["field"] == "vs_ema20"
    assert query_generation(generation, Query(predicate=predicate, new_only=True))["rows"] == []
    with pytest.raises(ValueError, match="Hourly context unavailable"):
        query_generation(dict(version=VERSION, generation="old", rows=[current]), Query(predicate=predicate))
    for invalid in (dict(filters=[]), dict(interval="1d", filters=[dict(field="close", min=1.)]),
                    dict(filters=[dict(field="discovery_state", min=0.)]), dict(filters=[dict(field="close", min=2., max=1.)]),
                    dict(filters=[dict(field="close", min=1.), dict(field="close", max=2.)])):
        with pytest.raises(ValidationError):
            HourlyPredicate(**invalid)


def test_hourly_reconstruction_uses_complete_retained_sources_never_latest_slot():
    from dataclasses import replace
    from datetime import timedelta
    import exchange_calendars
    from test_equity_bar_derivation import security, intraday_30m
    from equity.screening_hourly import complete_retained_slots, hourly_slots
    item = security()
    sources = intraday_30m(item)
    cutoff = sources[-1].bar_end + timedelta(minutes=20)
    slots = hourly_slots(exchange_calendars.get_calendar("XNYS"), sources[-1].bar_end)
    derived, lineage = complete_retained_slots(item, [], sources, slots, cutoff)
    assert len(derived) == 6 and len(lineage) == 6
    assert all(bar["bar_end"] < slots[-1][1] for bar in derived)
    assert set(lineage.values().__iter__().__next__()) == {str(sources[0].bar_revision_id), str(sources[1].bar_revision_id)}
    incomplete, _ = complete_retained_slots(item, [], sources[1:], slots, cutoff)
    assert len(incomplete) == 5
    late = [replace(bar, system_observed_at=cutoff + timedelta(minutes=1)) for bar in sources]
    assert complete_retained_slots(item, [], late, slots, cutoff) == ([], {})
    retained = derived[:1]
    completed, _ = complete_retained_slots(item, retained, sources, slots, cutoff)
    assert completed[0] == retained[0] and len(completed) == 6


def test_refreshed_hourly_context_preserves_daily_facts_and_separate_cutoffs():
    from copy import deepcopy
    from datetime import datetime, timezone
    import exchange_calendars
    from equity.screening_hourly import attach_hourly, hourly_slots
    from research.screening import REFRESHED_HOURLY_CATALOG, HOURLY_REFRESH_POLICY
    calendar = exchange_calendars.get_calendar("XNYS")
    daily = dict(market_time=datetime(2026, 9, 11, 20, tzinfo=timezone.utc), published_at=datetime(2026, 9, 11, 20, 35, tzinfo=timezone.utc))
    hourly = dict(publication_id="hour", interval="1h", session_scope="RTH", adjusted=False, status="COMPLETE", expected_members=1, selected_members=1,
                  market_time=datetime(2026, 9, 14, 14, 30, tzinfo=timezone.utc), published_at=datetime(2026, 9, 14, 14, 55, tzinfo=timezone.utc))
    slots = hourly_slots(calendar, hourly["market_time"])
    bars = [dict(ticker="A", security_id="a", bar_revision_id=str(index), bar_start=start, bar_end=end,
                 open=100.+index, high=102.+index, low=99.+index, close=101.+index, volume=100.) for index, (start, end) in enumerate(slots)]
    actions = []

    class Cursor:
        def execute(self, statement, parameters):
            self.statement = statement
            assert "INSERT" not in statement and "UPDATE" not in statement
        def fetchall(self):
            if "equity_bar_publication_members" in self.statement:
                return [dict(ticker="A", security_id="a", status="SELECTED", selected_bar_revision_id="22")]
            return actions if "equity_corporate_actions" in self.statement else bars

    original = dict(generation="daily-generation", session="2026-09-11", source_cutoff=daily["published_at"].isoformat(),
                    rows=[row("a", price=99.)], lineage={"a": {"bars": ["daily"]}})
    refreshed = attach_hourly(Cursor(), daily, [dict(ticker="A", security_id="a")], deepcopy(original), calendar, hourly_publication=hourly)
    assert refreshed["rows"][0]["values"] == original["rows"][0]["values"]
    assert refreshed["lineage"] == original["lineage"] and refreshed["source_cutoff"] == original["source_cutoff"]
    assert refreshed["hourly_source"]["snapshot_cutoff"] == daily["published_at"].isoformat()
    assert refreshed["hourly_source"]["source_cutoff"] == hourly["published_at"].isoformat()
    assert refreshed["hourly_contract"] == REFRESHED_HOURLY_CATALOG and refreshed["hourly_refresh_policy"] == HOURLY_REFRESH_POLICY
    assert refreshed["rows"][0]["hourly"]["values"]["close"] == 123.
    actions.append(dict(ticker="A", effective_date=hourly["market_time"].date(), action_type="SPLIT"))
    gated = attach_hourly(Cursor(), daily, [dict(ticker="A", security_id="a")], deepcopy(original), calendar, hourly_publication=hourly)
    assert all(value is None for value in gated["rows"][0]["hourly"]["values"].values())
    assert gated["rows"][0]["values"]["price"] == 99.
    with pytest.raises(ValueError, match="precede"):
        attach_hourly(Cursor(), daily, [], deepcopy(original), calendar, hourly_publication=dict(hourly, market_time=datetime(2026, 9, 10, 20, tzinfo=timezone.utc)))


def test_hourly_freshness_respects_provider_delay_and_original_cutoffs(monkeypatch):
    from datetime import datetime, timezone
    from equity import screening_api
    from research.screening import REFRESHED_HOURLY_CATALOG
    current = dict(version=VERSION, generation='pair', session='2026-09-11', market_time='2026-09-11T20:00:00+00:00',
        source_cutoff='2026-09-11T20:35:00+00:00', rows=[row(price=10.)], hourly_contract=REFRESHED_HOURLY_CATALOG,
        hourly_source=dict(market_time='2026-09-14T14:30:00+00:00'))

    class Clock:
        stamp = datetime(2026, 9, 14, 15, 40, tzinfo=timezone.utc)
        @classmethod
        def now(cls, zone):
            return cls.stamp
        fromisoformat = staticmethod(datetime.fromisoformat)

    monkeypatch.setattr(screening_api, 'datetime', Clock)
    monkeypatch.setenv('EQUITY_PROVIDER_DELAY_MINUTES', '15')
    monkeypatch.setattr(screening_api, 'publication_index', lambda: [dict(snapshot_id='stored', source_manifest=current, generated_at=Clock.stamp)])
    monkeypatch.setattr(screening_api, 'load_generation', lambda identity: current)
    assert screening_api.catalog()['hourly'] == REFRESHED_HOURLY_CATALOG
    before = screening_api.query(Query())
    assert before['hourly_stale'] is False and before['stale'] is False
    assert before['hourly_expected_market_time'] == '2026-09-14T14:30:00+00:00'
    Clock.stamp = datetime(2026, 9, 14, 15, 46, tzinfo=timezone.utc)
    after = screening_api.query(Query())
    assert after['hourly_stale'] is True and after['stale'] is False
    assert current['source_cutoff'] == '2026-09-11T20:35:00+00:00'


def test_screening_worker_waits_and_does_not_rebuild_unchanged_daily_anchor(monkeypatch):
    from scripts import run_screening_worker as worker
    calls = []
    monkeypatch.setattr(worker, 'build_latest', lambda: calls.append('build') or dict(source_publication_id='daily'))
    monkeypatch.setattr(worker, 'publish', lambda *args: calls.append('publish') or ['snapshot'])
    monkeypatch.setattr(worker, 'prepare_pair', lambda anchor, now: (None, dict(status='ALREADY_PUBLISHED')))
    monkeypatch.setattr(worker, 'load_daily_anchor', lambda now: (None, None))
    assert worker.run_once()['status'] == 'WAITING_FOR_DAILY_PUBLICATION' and not calls
    monkeypatch.setattr(worker, 'load_daily_anchor', lambda now: ('daily', dict(source_publication_id='daily')))
    assert worker.run_once()['status'] == 'ALREADY_PUBLISHED' and not calls
    monkeypatch.setattr(worker, 'load_daily_anchor', lambda now: ('new', dict(source_publication_id='old')))
    worker.run_once(publish_result=False)
    assert calls == ['build']
    calls.clear()
    worker.run_once()
    assert calls == ['build', 'publish']


def test_screening_pair_is_idempotent_uses_retained_sources_and_preserves_anchor(monkeypatch):
    from copy import deepcopy
    from contextlib import contextmanager
    from datetime import datetime, timezone
    from scripts import run_screening_worker as worker
    from research.screening import HOURLY_REFRESH_POLICY
    stamp = datetime(2026, 9, 14, 15, 55, tzinfo=timezone.utc)
    anchor = dict(generation='daily', source_cutoff='2026-09-11T20:35:00+00:00', market_time='2026-09-11T20:00:00+00:00',
                  session='2026-09-11', rows=[row('a', price=100.)], expected_members=1)
    original = deepcopy(anchor)
    hourly = dict(publication_id='hour', market_time=datetime(2026, 9, 14, 15, 30, tzinfo=timezone.utc))
    retained = []
    statements = []

    class Cursor:
        def execute(self, statement, parameters=None):
            self.statement = statement
            statements.append((statement, parameters))
            assert statement.startswith('SET ') or statement.startswith('SELECT ')
            self.parameters = parameters
        def fetchone(self):
            if 'equity_bar_publications' in self.statement:
                return hourly
            return retained[0] if retained and self.parameters[1] == retained[0]['pair_key'] else None

    @contextmanager
    def cursor():
        yield Cursor()

    def attach(cursor, daily, members, payload, calendar, *, hourly_publication):
        assert hourly_publication is hourly and members[0]['security_id'] == 'a'
        payload.update(generation='pair', hourly_refresh_policy=HOURLY_REFRESH_POLICY, hourly_source={}, hourly_coverage={})
        return payload

    monkeypatch.setattr(worker, 'get_db_cursor', cursor)
    monkeypatch.setattr(worker, 'attach_hourly', attach)
    first, report = worker.prepare_pair(anchor, stamp)
    assert report['status'] == 'PREPARED' and anchor == original and first['daily_generation'] == 'daily'
    assert 'created_at<=%s' in statements[2][0] and statements[2][1] == (stamp, stamp, stamp)
    retained.append(dict(snapshot_id='snapshot', source_manifest=dict(generation='pair'), pair_key=first['hourly_pair_key']))
    second, report = worker.prepare_pair(anchor, stamp)
    assert second is None and report['status'] == 'ALREADY_PUBLISHED'
    hourly['publication_id'] = 'next-hour'
    next_pair, report = worker.prepare_pair(anchor, stamp)
    assert report['status'] == 'PREPARED' and next_pair['hourly_pair_key'] != first['hourly_pair_key']
    assert next_pair['daily_generation'] == first['daily_generation']
    assert anchor == original


@pytest.mark.parametrize('refreshed', [False, True])
def test_hourly_detail_reads_only_stored_snapshot(monkeypatch, refreshed):
    from equity import screening_api
    from research.screening import HOURLY_CATALOG, REFRESHED_HOURLY_CATALOG
    generation = dict(hourly_contract=REFRESHED_HOURLY_CATALOG if refreshed else HOURLY_CATALOG, hourly_source=dict(market_time="retained"),
        hourly_lineage={"a": dict(selected_bar_id="selected", reconstructed_from_30m={})})
    current = dict(hourly=dict(values=dict(close=10.), missing={}))
    monkeypatch.setattr(screening_api, "snapshot_row", lambda request: (generation, current))
    response = screening_api.hourly_details(screening_api.SnapshotRowRequest(generation="a" * 64, security_id="a"))
    assert response["values"] == dict(close=10.) and response["lineage"]["selected_bar_id"] == "selected"


def test_gap_summary_and_details_use_same_matching_episode_and_snapshot_only(monkeypatch):
    from contextlib import contextmanager
    from equity import screening_api
    from research.screening import GAP_VERSION, GAP_CATALOG
    episode = dict(episode_id="gap", formation_session="2026-09-10", formation_bar_id="formation",
        values=dict(direction="UP", formation_age=1, fill_fraction=.25, distance_fraction=.01))
    projected = row(price=10.) | {"gaps": dict(version=GAP_VERSION, status="READY", episodes=[episode])}
    generation = dict(version=VERSION, generation="a" * 64, session="2026-09-11", source_cutoff="cutoff", gap_contract=GAP_CATALOG, rows=[projected])
    predicate = Predicate(gap=dict(filters=[dict(field="direction", values=["UP"])]))
    result = query_generation(generation, Query(predicate=predicate))
    assert result["matched_count"] == 1 and "gaps" not in result["rows"][0]
    assert result["rows"][0]["gap_summary"]["representative"]["episode_id"] == "gap"
    statements = []

    class Cursor:
        def execute(self, statement, parameters=None):
            assert statement.startswith("SET ") or "equity_portal_snapshots" in statement
            statements.append(statement)

        def fetchone(self):
            return dict(snapshot_id="stored")

    @contextmanager
    def cursor():
        yield Cursor()

    monkeypatch.setattr(screening_api, "get_db_cursor", cursor)
    monkeypatch.setattr(screening_api, "load_generation", lambda identity: generation)
    detail = screening_api.gap_details(screening_api.GapDetailRequest(generation="a" * 64, security_id="a", gap=predicate.gap))
    assert detail["matching_episodes"] == [episode] and statements


def test_full_membership_required_and_idempotent_generation():
    from equity.screening_projection import finalize
    manifest = dict(expected_members=2)
    with pytest.raises(ValueError):
        finalize([row()], manifest, {})
    rows = [row("a", momentum_12_1=.1), row("b", momentum_12_1=.2)]
    assert finalize(rows, manifest, {}) == finalize(rows, manifest, {})


def test_historical_publication_inserts_snapshot_without_promoting_any_pointer(monkeypatch):
    from contextlib import contextmanager
    from equity import portal_snapshots
    statements = []

    class Cursor:
        def execute(self, sql, parameters):
            statements.append(sql)

    @contextmanager
    def cursor():
        yield Cursor()

    monkeypatch.setattr(portal_snapshots, "get_db_cursor", cursor)
    payloads = {"SCREENING_DAILY_V1": {"session": "2026-09-10", "rows": []}}
    manifest = {"source_generation": 0, "session": "2026-09-10"}
    first = portal_snapshots.publish(payloads, manifest, promote_current=False)
    assert len(statements) == 1 and "INSERT INTO equity_portal_snapshots" in statements[0]
    assert not any("equity_portal_current_projections" in sql for sql in statements)
    repeated = portal_snapshots.publish(payloads, manifest, promote_current=False)
    assert repeated == first
    statements.clear()
    assert portal_snapshots.publish(payloads, manifest) == first
    assert len(statements) == 2 and "equity_portal_current_projections" in statements[1]


def test_missing_source_member_is_retained_as_unknown_not_filled_from_other_bars():
    from datetime import date
    from equity.screening_projection import project_security
    session = date(2026, 9, 9)
    member = dict(security_id="arkw", ticker="ARKW", selected_bar_revision_id=None, status="MISSING", security_type="ETF")
    bars = [dict(session_date=session, security_id="arkw", bar_revision_id="unselected", open=10., high=12., low=9., close=11., volume=100.)]
    projected = project_security(member, bars, [], session, {session: 0})
    assert not projected["eligible"] and projected["source_bar_id"] is None
    assert projected["values"]["price"] is None and projected["patterns"] == {}
    assert projected["values"]["instrument_type"] == "ETF"
    assert projected["missing"]["price"] == "SOURCE_PUBLICATION_MEMBER_UNAVAILABLE"
    assert evaluate(projected, Predicate())[0] == "UNKNOWN"


def partial_anchor_fixture():
    from datetime import datetime, timezone
    from scripts.prepare_stock_screening import approve_partial_source
    missing = dict(security_id="oke", ticker="OKE", status="MISSING")
    payload = dict(session="2026-09-16", market_time="2026-09-16T20:00:00+00:00", source_publication_id="partial-source",
        source_publication_status="DEGRADED", source_selected_members=385, expected_members=386,
        source_unavailable_members=[missing], generation="original",
        rows=[dict(security_id="oke", eligible=False, source_bar_id=None, values=dict(price=None))])
    return approve_partial_source(payload, ["OKE"], 385, datetime(2026, 9, 17, 17, tzinfo=timezone.utc))


def test_partial_current_approval_keeps_unknown_member_and_does_not_approve_other_coverage():
    from copy import deepcopy
    from datetime import datetime, timezone
    from scripts.prepare_stock_screening import approve_partial_source
    anchor = partial_anchor_fixture()
    original = deepcopy(anchor)
    for changes in (dict(source_selected_members=384), dict(source_unavailable_members=[]), dict(source_publication_status="COMPLETE"),
                    dict(rows=[dict(security_id="oke", eligible=True, source_bar_id="wrong", values=dict(price=10.))])):
        with pytest.raises(ValueError):
            approve_partial_source(anchor | changes, ["OKE"], 385, datetime(2026, 9, 17, 17, tzinfo=timezone.utc))
    assert anchor == original and anchor["rows"][0]["values"]["price"] is None


def test_worker_uses_only_exact_approved_partial_anchor_until_a_complete_newer_source():
    from copy import deepcopy
    from datetime import datetime, timezone
    from scripts.run_screening_worker import approved_anchor_source
    anchor = partial_anchor_fixture()
    now = datetime(2026, 9, 17, 18, tzinfo=timezone.utc)
    old = dict(publication_id="old", market_time=datetime(2026, 9, 15, 20, tzinfo=timezone.utc))
    assert approved_anchor_source(old, anchor, now) == "partial-source"
    assert approved_anchor_source(None, anchor, now) == "partial-source"
    assert approved_anchor_source(old | dict(publication_id="complete", market_time=datetime(2026, 9, 16, 20, tzinfo=timezone.utc)), anchor, now) == "complete"
    unapproved = {key: value for key, value in anchor.items() if key != "partial_source_approval"}
    assert approved_anchor_source(old, unapproved, now) == "old"
    for changes in (dict(session="2026-09-17"), dict(source_publication_id="other"), dict(source_unavailable_members=[])):
        with pytest.raises(ValueError):
            approved_anchor_source(old, anchor | changes, now)
    tampered = deepcopy(anchor)
    tampered["partial_source_approval"]["selected_members"] = 384
    with pytest.raises(ValueError):
        approved_anchor_source(old, tampered, now)


def test_source_membership_requires_every_expected_member_and_consistent_selected_ids():
    from equity.screening_projection import validate_publication_members
    publication = dict(expected_members=2, selected_members=1)
    members = [dict(security_id="a", ticker="A", status="SELECTED", selected_bar_revision_id="bar"),
               dict(security_id="b", ticker="B", status="MISSING", selected_bar_revision_id=None)]
    validate_publication_members(publication, members)
    for invalid in (members[:1], [members[0], members[0]], [members[0] | {"selected_bar_revision_id": None}, members[1]]):
        with pytest.raises(ValueError):
            validate_publication_members(publication, invalid)


def test_historical_builder_rejects_holiday_and_implicit_degraded_build():
    from datetime import date
    from equity.screening_projection import build_latest
    with pytest.raises(ValueError, match="exchange session"):
        build_latest(date(2026, 9, 7))
    with pytest.raises(ValueError, match="explicit session"):
        build_latest(allow_degraded=True)


def test_historical_preparation_is_date_bounded_and_skips_only_exchange_holidays():
    from datetime import date
    from scripts.prepare_stock_screening import preparation_sessions
    assert preparation_sessions() == [None]
    assert preparation_sessions(date(2026, 9, 10)) == [date(2026, 9, 10)]
    assert preparation_sessions(start=date(2026, 9, 3), end=date(2026, 9, 11)) == [date(2026, 9, day) for day in (3, 4, 8, 9, 10, 11)]
    for arguments in (dict(start=date(2026, 9, 3)), dict(start=date(2026, 9, 11), end=date(2026, 9, 3)),
                      dict(start=date(2026, 9, 2), end=date(2026, 9, 11)), dict(session=date(2026, 9, 7)),
                      dict(session=date(2026, 9, 3), start=date(2026, 9, 3), end=date(2026, 9, 4))):
        with pytest.raises(ValueError):
            preparation_sessions(**arguments)


def test_historical_cli_never_promotes_and_measurement_never_writes(monkeypatch):
    from datetime import date
    from scripts import prepare_stock_screening as command
    calls = []
    payload = dict(version=VERSION, generation="fixed", session="2026-09-10", source_cutoff="cutoff", source_publication_id="source",
                   source_publication_status="DEGRADED", source_unavailable_members=[], expected_members=1,
                   field_coverage={}, pattern_coverage={}, rows=[row(price=10.)])
    monkeypatch.setattr(command, "build_latest", lambda *args, **kwargs: payload)
    monkeypatch.setattr(command, "publish", lambda *args, **kwargs: calls.append(kwargs) or ["snapshot"])
    measured = command.prepare_one(date(2026, 9, 10))
    assert measured["status"] == "MEASURED_NO_WRITES" and not calls
    command.prepare_one(date(2026, 9, 10), publish_result=True, allow_degraded=True)
    assert calls == [{"promote_current": False}]


@pytest.mark.parametrize("state", ["PULLBACK", "BOUNCE", "RESUMING_UP", "RESUMING_DOWN"])
def test_published_discovery_state_matches_original_and_stays_field_scoped(state):
    from datetime import date, timedelta
    import pandas as pd
    from equity.screening_projection import project_security
    from equity.stock_discovery import stock_features
    start = date(2025, 1, 1)
    bullish = state in ("PULLBACK", "RESUMING_UP")
    closes = [100 + index * (.2 if bullish else -.2) for index in range(253)]
    anchor = closes[-6]
    for index in range(5):
        closes[-5 + index] = anchor + (-.3 if bullish else .3) * (index + 1)
    if state.startswith("RESUMING"):
        closes[-1] = closes[-2] + (1 if bullish else -1)
    bars = [dict(session_date=start + timedelta(days=index), security_id="a", bar_revision_id=str(index),
                 open=close, high=close + .2, low=close - .2, close=close, volume=2_000_000.) for index, close in enumerate(closes)]
    session = bars[-1]["session_date"]
    ordinals = {bar["session_date"]: index for index, bar in enumerate(bars)}
    member = dict(security_id="a", ticker="A", selected_bar_revision_id="252", security_type="CS")
    frame = pd.DataFrame(bars)
    frame["ordinal"] = frame.session_date.map(ordinals)
    frame["raw_close"], frame["raw_volume"] = frame.close, frame.volume
    original = stock_features(frame, session, "a")
    assert original["state"] == state
    projected = project_security(member, bars, [], session, ordinals)
    assert projected["values"]["discovery_state"] == original["state"]
    assert projected["values"]["discovery_trend"] == original["trend"]
    rule = Predicate(filters=[{"field": "discovery_state", "values": [state]}])
    assert evaluate(projected, rule)[0] == "MATCH"
    for unavailable_bars, actions in [(bars[-30:], []), (bars, [{"effective_date": session, "action_type": "SPLIT"}]),
                                     ([dict(bar, volume=1.) for bar in bars], [])]:
        unavailable = project_security(member, unavailable_bars, actions, session, ordinals)
        assert unavailable["eligible"]
        assert unavailable["values"]["price"] is not None
        assert unavailable["values"]["discovery_state"] is None
        assert evaluate(unavailable, rule)[0] == "UNKNOWN"


def test_extracted_state_provenance_does_not_trust_compatibility_shim():
    from copy import deepcopy
    from research.screening import comparison_status
    previous = dict(version=VERSION, universe="fixed", interval="1d", field_set_version="fields",
                    contract_hash="contract", capture_mode="RECONSTRUCTED_FROM_RETAINED_PUBLICATION",
                    session="2026-09-10", source_cutoff="2026-09-10T21:00:00+00:00",
                    daily_state_contract="daily_state_extracted_v1",
                    code_hashes={"technicals.py": "candles", "daily_state.py": "implementation"})
    current = deepcopy(previous)
    current.update(session="2026-09-11", previous_expected_session="2026-09-10",
                   source_cutoff="2026-09-11T21:00:00+00:00")
    assert comparison_status(current, previous) == "READY"
    current["code_hashes"]["daily_state.py"] = "changed"
    assert comparison_status(current, previous) == "INCOMPATIBLE_PUBLICATIONS"
    current["code_hashes"]["daily_state.py"] = "implementation"
    previous.pop("daily_state_contract")
    assert comparison_status(current, previous) == "INCOMPATIBLE_PUBLICATIONS"


def test_new_state_fields_cannot_be_requested_from_old_publication():
    generation = dict(version=VERSION, generation="old", rows=[row(price=10.)])
    assert query_generation(generation, Query())["matched_count"] == 1
    with pytest.raises(ValueError, match="Fields unavailable in this publication"):
        query_generation(generation, Query(predicate=Predicate(filters=[{"field": "discovery_state", "values": ["PULLBACK"]}])))


@pytest.mark.parametrize("field", ["momentum_6_1", "momentum_3_1"])
def test_momentum_publication_availability_filter_sort_and_unknown(field):
    generation = dict(version=VERSION, generation="old", rows=[row(price=10.)])
    predicate = Predicate(filters=[dict(field=field, min=0., max=.2)])
    for query in (Query(predicate=predicate), Query(sort=field)):
        with pytest.raises(ValueError, match="Fields unavailable in this publication"):
            query_generation(generation, query)
    generation.update(field_coverage={field: 3}, rows=[row("a", **{field: .2}), row("b", **{field: 0.}),
                     row("c", **{field: -.1}), row("d", **{field: None})])
    result = query_generation(generation, Query(predicate=predicate, sort=field, descending=True))
    assert (result["matched_count"], result["nonmatch_count"], result["unknown_count"]) == (2, 1, 1)
    assert [item["security_id"] for item in result["rows"]] == ["a", "b"]
    assert result["rows"][0]["explanations"][0]["unit"] == "fraction"


def test_momentum_catalog_requires_published_fields_without_changing_existing_fields(monkeypatch):
    from datetime import datetime, timezone
    from equity import screening_api
    from research.screening import FIELDS, SHORT_MOMENTUM_FIELDS
    manifest = dict(generation="old", session="2026-09-11", field_coverage={"discovery_state": 1})
    monkeypatch.setattr(screening_api, "publication_index", lambda: [dict(source_manifest=manifest, generated_at=datetime.now(timezone.utc))])
    original = screening_api.catalog()["fields"]
    assert not SHORT_MOMENTUM_FIELDS & original.keys()
    assert "momentum_12_1" in original and "momentum_percentile" in original
    manifest["field_coverage"].update(momentum_6_1=1, momentum_3_1=0)
    updated = screening_api.catalog()["fields"]
    assert all(updated[name] == spec for name, spec in original.items())
    assert all(updated[name] == FIELDS[name] for name in SHORT_MOMENTUM_FIELDS)


def test_reader_wire_contract_and_no_source_work(monkeypatch):
    import asyncio
    import json
    from contextlib import contextmanager
    from datetime import datetime, timezone
    from fastapi import FastAPI
    from equity import screening_api, screening_projection, technicals
    stamp = datetime(2026, 9, 11, 20, tzinfo=timezone.utc)
    snapshot_id = "00000000-0000-0000-0000-000000000001"
    generation = dict(version=VERSION, generation="fixed", market_time=stamp.isoformat(), session="2026-09-11", rows=[row(price=10.)])
    prior_generation = None
    prior_snapshot_id = "00000000-0000-0000-0000-000000000002"
    statements = []

    class Cursor:
        def execute(self, sql, params=None):
            statements.append(sql)
            assert not any(source in sql for source in ("equity_bar_revisions", "equity_evidence", "INSERT", "UPDATE"))
            self.sql, self.params = sql, params

        def fetchall(self):
            return [dict(snapshot_id=snapshot_id, source_manifest=generation, generated_at=stamp)] + ([dict(snapshot_id=prior_snapshot_id, source_manifest=prior_generation, generated_at=stamp)] if prior_generation else [])

        def fetchone(self):
            return None if self.params[-1] == "missing" else dict(payload=prior_generation if str(self.params[0]) == prior_snapshot_id else generation, generated_at=stamp)

    @contextmanager
    def cursor():
        yield Cursor()

    def forbidden(*args, **kwargs):
        raise AssertionError("Reader attempted preparation")

    monkeypatch.setattr(screening_api, "get_db_cursor", cursor)
    monkeypatch.setattr(screening_projection, "build_latest", forbidden)
    monkeypatch.setattr(screening_projection, "stock_features", forbidden)
    monkeypatch.setattr(technicals, "detect_setup_candlesticks", forbidden)
    screening_api.load_generation.cache_clear()
    app = FastAPI()
    app.include_router(screening_api.router)

    def request(path, body=None):
        messages = []

        async def receive():
            return dict(type="http.request", body=json.dumps(body).encode(), more_body=False)

        async def send(message):
            messages.append(message)

        scope = dict(type="http", http_version="1.1", method="GET" if body is None else "POST", scheme="http",
                     path="/api/stocks/screening/" + path, root_path="", query_string=b"",
                     headers=[(b"content-type", b"application/json")], server=("test", 80), client=("test", 123))
        asyncio.run(app(scope, receive, send))
        return (next(message["status"] for message in messages if message["type"] == "http.response.start"),
                json.loads(b"".join(message.get("body", b"") for message in messages if message["type"] == "http.response.body")))

    assert request("catalog")[0] == 200
    status, response = request("query", {"predicate": {"filters": [{"field": "price", "min": 10.}]}})
    assert status == 200, response
    assert response["matched_count"] == 1
    assert request("query", {"generation": "missing"})[0] == 404
    assert request("query", {"predicate": {"filters": [{"field": "price", "min": "10"}]}})[0] == 422
    assert request("query", {"predicate": {"filters": [{"field": "price", "min": 11, "max": 10}]}})[0] == 422
    assert request("query", {"limit": 201})[0] == 422
    assert request("query", {"new_only": "true"})[0] == 422
    monkeypatch.setattr(screening_api, "latest_expected_market_time", lambda now, interval: datetime(2026, 9, 14, 20, tzinfo=timezone.utc))
    assert request("query", {})[1]["stale"] is True
    prior_generation, generation = comparable_generations(dict(a=9., b=11.), dict(a=10., b=12.))
    generation["market_time"] = stamp.isoformat()
    screening_api.load_generation.cache_clear()
    status, response = request("query", {"new_only": True, "predicate": {"filters": [{"field": "price", "min": 10.}]}})
    assert status == 200 and response["matched_count"] == 2 and response["result_count"] == 1
    assert response["rows"][0]["new_status"] == "NEW" and response["comparison"]["previous_generation"] == "prior"
    assert statements and all(sql.startswith("SET ") or "equity_portal_snapshots" in sql for sql in statements)
    screening_api.load_generation.cache_clear()