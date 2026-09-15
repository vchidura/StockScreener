from datetime import timedelta

import pytest

from research.stock_idea_forward import ForwardStore, forward_config, forward_decision, next_boundary, window_clock
from test_stock_idea_engine import NOW, candidate


def packet(model="resumption"):
    original = candidate(model=model)
    return dict(security_id="A", interval="30m", market_time=original.trigger_at, available_at=original.available_at,
                ready=True, candidates=[original], updates=[], revision_ids=["one"])


def test_forward_actual_clock_and_no_backdated_outage_alerts():
    config = forward_config()
    boundary = candidate().trigger_at
    state = dict(enrolled_at=(boundary - timedelta(minutes=30)).isoformat())
    args = dict(boundary=boundary, members=[dict(security_id="A")], policy_hash="policy", config=config)
    state, publication, _ = forward_decision(state, [packet()], actual_time=NOW + timedelta(seconds=1), **args)
    assert publication["coverage"] == "PUBLISHED"
    assert publication["actual_publication_at"] == (NOW + timedelta(seconds=1)).isoformat()
    assert state["positions"][candidate().episode_id]["publication_at"] == publication["actual_publication_at"]
    _, missed, outbox = forward_decision(dict(enrolled_at=state["enrolled_at"]), [packet()], actual_time=NOW + timedelta(minutes=1), **args)
    assert missed["coverage"] == "MISSED_PUBLICATION" and not missed["selected"] and not outbox
    with pytest.raises(ValueError, match="before"):
        forward_decision(state, [packet()], actual_time=NOW - timedelta(seconds=1), **args)


def test_late_inputs_and_pre_enrollment_triggers_never_enter():
    config = forward_config()
    boundary = candidate().trigger_at
    args = dict(boundary=boundary, actual_time=NOW, members=[dict(security_id="A"), dict(security_id="B")], policy_hash="policy", config=config)
    late = dict(packet(), available_at=NOW + timedelta(seconds=1))
    _, result, _ = forward_decision(dict(enrolled_at=(boundary - timedelta(minutes=30)).isoformat()), [late], **args)
    assert not result["selected"] and result["missing_members"] == ["A", "B"]
    with pytest.raises(ValueError, match="pre-enrollment"):
        forward_decision(dict(enrolled_at=NOW.isoformat()), [], **args)


def decision_bar(plan, boundary, price, **changes):
    return dict(security_id=plan.security_id, ticker=plan.ticker, interval="30m", session=str(boundary.date()),
        bar_start=(boundary - timedelta(minutes=30)).isoformat(), bar_end=boundary.isoformat(),
        open=price, high=price + .1, low=price - .1, close=price, volume=1000., revision_id="decision-" + plan.security_id,
        system_observed_at=(boundary + timedelta(minutes=15)).isoformat(), created_at=(boundary + timedelta(minutes=15)).isoformat()) | changes


def test_quality_gate_uses_current_price_before_quota_and_preserves_original_plan():
    from copy import deepcopy
    from research.stock_idea_forward import READINESS_POLICY
    config = forward_config(quality_version=2)
    boundary = candidate().trigger_at
    plans = [candidate(security=name, model="acceptance", interval="1h", trigger_at=boundary - timedelta(minutes=30),
        expires_at=boundary + timedelta(minutes=30), extension=index / 10) for index, name in enumerate(["A", "B", "C", "D"])]
    state = dict(enrolled_at=(boundary - timedelta(hours=1)).isoformat(),
        bars={plan.security_id + "|30m": {"bar": decision_bar(plan, boundary, 99. if plan.security_id == "A" else 101.)} for plan in plans})
    original = deepcopy(state)
    packets = [dict(packet(), security_id=plan.security_id, candidates=[plan]) for plan in plans]
    changed, result, _ = forward_decision(state, packets, boundary=boundary, actual_time=NOW,
        members=[dict(security_id=plan.security_id) for plan in plans], policy_hash="v2", config=config)
    assert len(result["selected"]) == 3 and plans[0].episode_id not in result["selected"]
    assert result["decision_prices"][plans[0].episode_id]["status"] == "ENTRY_CHASE_OR_BOUNDARY_FAILED"
    for plan in plans[1:]:
        assert result["candidates"][plan.episode_id]["price"] == plan.price
        assert changed["positions"][plan.episode_id]["candidate"]["price"] == plan.price
        assert result["decision_prices"][plan.episode_id]["price"] == 101.
        assert result["decision_prices"][plan.episode_id]["reward_risk"] == 3.
    assert state == original


@pytest.mark.parametrize("changes,status", [({"ticker": "REUSED"}, "UNAVAILABLE"),
    ({"created_at": (NOW + timedelta(seconds=1)).isoformat()}, "UNAVAILABLE"),
    ({"volume": 0}, "INVALID_DECISION_BAR"), ({"bar_end": (NOW + timedelta(minutes=13)).isoformat()}, "UNAVAILABLE")])
def test_quality_decision_price_fails_closed(changes, status):
    from research.stock_idea_forward import decision_candidate
    plan = candidate()
    boundary = plan.trigger_at
    checked, evidence = decision_candidate(plan, dict(bars={"A|30m": {"bar": decision_bar(plan, boundary, 100., **changes)}}),
        boundary=boundary, cutoff=NOW, actual_time=NOW, config=forward_config(quality_version=2))
    assert checked.selection_block and evidence["status"] == status


def test_quality_rejects_last_slot_without_extending_candidate_expiry():
    from research.stock_idea_replay import utc
    from research.stock_idea_forward import decision_candidate
    boundary = utc("2026-09-14T19:30:00Z")
    plan = candidate(trigger_at=boundary, expires_at=utc("2026-09-14T20:00:00Z"))
    checked, evidence = decision_candidate(plan, {}, boundary=boundary, cutoff=utc("2026-09-14T19:55:00Z"),
        actual_time=utc("2026-09-14T19:58:46Z"), config=forward_config(quality_version=2))
    assert checked.selection_block == "NO_REMAINING_ENTRY_SLOT" and evidence["expected_entry_at"] is None
    assert checked.expires_at == plan.expires_at


@pytest.mark.parametrize("direction", [1, -1])
def test_quality_acceptance_requires_margin_body_followthrough_and_close_location(direction):
    from types import SimpleNamespace
    from research.stock_idea_models import ACCEPTANCE_CONFIRMATION_V2, acceptance_confirmed
    def row(opening=100.4, closing=100.8, high=101., low=100.2):
        return SimpleNamespace(open=opening if direction == 1 else 200 - opening,
            close=closing if direction == 1 else 200 - closing,
            high=high if direction == 1 else 200 - low, low=low if direction == 1 else 200 - high)
    previous = row(closing=100.5)
    geometry = dict(reference=100., atr=2.)
    assert acceptance_confirmed(row(), previous, geometry, direction, 1, ACCEPTANCE_CONFIRMATION_V2)
    for weak in (row(opening=100.9), row(closing=100.49), row(opening=100.1, closing=100.2), row(high=103.)):
        assert acceptance_confirmed(weak, previous, geometry, direction, 1)
        assert not acceptance_confirmed(weak, previous, geometry, direction, 1, ACCEPTANCE_CONFIRMATION_V2)
    assert not acceptance_confirmed(row(), previous, geometry, direction, 2, ACCEPTANCE_CONFIRMATION_V2)
    with pytest.raises(ValueError, match="unsupported"):
        acceptance_confirmed(row(), previous, geometry, direction, 1, dict(ACCEPTANCE_CONFIRMATION_V2, boundary_margin_atr=0))


@pytest.mark.parametrize("opening,high,low,closing,previous_close,reference,atr", [
    (418.545, 420., 417.5, 417.5, 418.31, 416.38, 3.0055733894244514),
    (222.925, 223.58, 221.3, 221.355, 223.13, 221.29, 1.5985716837345372)])
def test_quality_rejects_reviewed_spgi_rmd_confirmation_without_changing_v1(opening, high, low, closing, previous_close, reference, atr):
    from types import SimpleNamespace
    from research.stock_idea_models import acceptance_confirmed
    row = SimpleNamespace(open=opening, high=high, low=low, close=closing)
    previous = SimpleNamespace(close=previous_close)
    geometry = dict(reference=reference, atr=atr)
    assert acceptance_confirmed(row, previous, geometry, 1, 1)
    assert not acceptance_confirmed(row, previous, geometry, 1, 1,
        forward_config(quality_version=2)["models"]["acceptance"]["confirmation_policy"])


def test_quality_model_config_is_versioned_and_cannot_reuse_old_store(tmp_path):
    from research.stock_idea_forward import QUALITY_VERSION
    old = forward_config()
    revised = forward_config(quality_version=2)
    assert old["models"]["acceptance"]["intraday_version"] == "range_breakout_acceptance_intraday_v1"
    assert "confirmation_policy" not in old["models"]["acceptance"]
    assert revised["policy_version"] == QUALITY_VERSION
    assert revised["models"]["acceptance"]["intraday_version"].endswith("_v2")
    store = ForwardStore(tmp_path / "forward.sqlite", old)
    original = store.path.read_bytes()
    with pytest.raises(ValueError, match="policy changed"):
        ForwardStore(store.path, revised)
    assert store.path.read_bytes() == original


def test_quality_plan_is_offline_separate_and_refuses_overwriting_older_view(tmp_path, capsys):
    import json
    from scripts.run_stock_idea_worker import main, validate_view_policy
    from research.stock_idea_forward import QUALITY_VERSION
    assert main(["--plan"]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["policy"]["policy_version"] == QUALITY_VERSION
    assert plan["state_dir"].endswith("stock-ideas-forward-v2")
    path = tmp_path / "view.json"
    path.write_text(json.dumps(dict(source="SHADOW", source_id=forward_config()["policy_version"])))
    before = path.read_bytes()
    with pytest.raises(ValueError, match="different policy"):
        validate_view_policy(path, forward_config(quality_version=2))
    assert path.read_bytes() == before


@pytest.mark.parametrize("strong,expected", [(False, 0), (True, 1)])
def test_quality_confirmation_controls_actual_incremental_detector(strong, expected):
    from copy import deepcopy
    import pandas as pd
    import exchange_calendars
    from research.stock_idea_models import feature_frames, intraday_observations
    from test_stock_idea_replay import model_fixture, CONFIG
    frame = feature_frames(model_fixture(), CONFIG)[("A", "30m")].iloc[:202].copy()
    last = frame.iloc[-1]
    reference = float(last.close - 1.)
    frame.loc[frame.index[-2], "close"] = last.close - .1 if strong else last.close + .1
    frame.loc[frame.index[-1], ["open", "high", "low"]] = [last.close - .2 if strong else last.close + .2, last.close + .1, last.close - .4]
    geometry = dict(reference=reference, target=last.close + 3, atr=1., low=reference - 2., high=reference,
        anchor="test-range", anchor_ordinal=int(last.ordinal) - 1, extreme=float(last.close - .4), source_ids=["one"])
    initial = {("acceptance", 1): dict(lifecycle=None, geometry=geometry)}
    baseline = deepcopy(initial)
    prior_session = str(exchange_calendars.get_calendar("XNYS").previous_session(last.session).date())
    context = dict(rs63=.1, rs_percentile=.8, close=100., ema50=99., ema50_prior10=98., momentum=.2,
        liquidity=50_000_000., visible_at=pd.Timestamp("2026-06-01T20:15:00Z"), rank_revision_ids=[])
    result = intraday_observations(frame, "30m", {("A", prior_session): context}, forward_config(quality_version=2),
        initial_state=initial, after_ordinal=int(last.ordinal) - 1)
    candidates = [plan for observation in result for plan in observation["candidates"] if plan.model == "acceptance" and plan.direction == 1]
    assert len(candidates) == expected and initial == baseline
    if candidates:
        assert candidates[0].policy_version == "range_breakout_acceptance_intraday_v2"


def test_forward_store_retry_does_not_duplicate_positions_or_outbox(tmp_path):
    import sqlite3
    config = forward_config()
    store = ForwardStore(tmp_path / "forward.sqlite", config)
    state = dict(enrolled_at=(candidate().trigger_at - timedelta(minutes=30)).isoformat())
    state, publication, outbox = forward_decision(state, [packet()], boundary=candidate().trigger_at, actual_time=NOW,
        members=[dict(security_id="A")], policy_hash=store.policy_hash, config=config)
    store.save(state, publication, outbox)
    restored = store.load()
    store.save(dict(state, selected=[]), publication, outbox)
    assert store.load() == restored and len(store.publications()) == 1
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM forward_outbox").fetchone()[0] == 1
    with pytest.raises(ValueError, match="policy changed"):
        ForwardStore(store.path, dict(config, dispatch_grace_seconds=90))


def test_schedule_obeys_half_day_and_does_not_enroll_an_old_boundary():
    from research.stock_idea_replay import utc
    assert next_boundary(utc("2026-11-27T17:31:00Z")) == utc("2026-11-27T18:00:00Z")
    assert next_boundary(utc("2026-11-27T18:00:00Z")) == utc("2026-11-30T15:00:00Z")
    cutoff, latest = window_clock(candidate().trigger_at)
    assert cutoff == NOW and latest == NOW + timedelta(seconds=5)


def test_explicit_source_ready_policy_accepts_actual_late_arrival_without_backdating():
    from research.stock_idea_forward import READINESS_POLICY, readiness_deadline
    config = forward_config()
    boundary = candidate().trigger_at
    arrived = NOW + timedelta(minutes=2)
    state = dict(enrolled_at=(boundary - timedelta(minutes=30)).isoformat(), dispatch_policy=READINESS_POLICY)
    late_packet = dict(packet(), available_at=arrived)
    readiness = dict(input_cutoff=arrived.isoformat(), publications=["complete-source"])
    changed, result, _ = forward_decision(state, [late_packet], boundary=boundary, actual_time=arrived,
        members=[dict(security_id="A")], policy_hash="base", config=config, readiness=readiness)
    assert result["selected"] and result["input_deadline"] == arrived.isoformat()
    assert changed["positions"][candidate().episode_id]["publication_at"] == arrived.isoformat()
    assert result["policy_hash"] != "base" and result["source_readiness"] == readiness
    assert readiness_deadline(boundary) == boundary + timedelta(minutes=30, seconds=-5)
    _, missed, _ = forward_decision(state, [late_packet], boundary=boundary,
        actual_time=readiness_deadline(boundary) + timedelta(seconds=1), members=[dict(security_id="A")],
        policy_hash="base", config=config, readiness=readiness)
    assert missed["coverage"] == "MISSED_PUBLICATION" and not missed["selected"]
    with pytest.raises(ValueError, match="explicitly enabled"):
        forward_decision({}, [], boundary=boundary, actual_time=arrived, members=[], policy_hash="base", config=config, readiness=readiness)


def test_incremental_detector_state_matches_single_prefix_processing():
    from copy import deepcopy
    import pandas as pd
    import exchange_calendars
    from test_stock_idea_replay import model_fixture, CONFIG
    from research.stock_idea_models import feature_frames, intraday_observations
    frame = feature_frames(model_fixture(), CONFIG)[("A", "30m")].iloc[:220]
    context = dict(rs63=.1, rs_percentile=.9, close=100., ema50=99., ema50_prior10=98., momentum=.2,
        liquidity=100_000_000., visible_at=pd.Timestamp("2026-06-01T20:15:00Z"), rank_revision_ids=["rank"])
    calendar = exchange_calendars.get_calendar("XNYS")
    contexts = {("A", str(calendar.previous_session(session).date())): context for session in frame.session.unique()}
    sink = {}
    first = intraday_observations(frame.iloc[:201], "30m", contexts, CONFIG, state_sink=sink)
    before = deepcopy(sink)
    second = intraday_observations(frame, "30m", contexts, CONFIG, initial_state=sink, after_ordinal=200)
    full = intraday_observations(frame, "30m", contexts, CONFIG)
    assert first + second == full and sink == before


def test_bootstrap_checkpoint_roundtrip_and_forward_reader_not_replay(tmp_path):
    import json
    from test_stock_idea_replay import full_fixture
    from research.stock_idea_replay import utc
    from research.stock_idea_forward import advance_detectors, publish_view, shadow_snapshot
    from research.stock_alerts import load_snapshot
    fixture, _ = full_fixture()
    config = forward_config()
    now = utc("2026-08-04T13:31:00Z")
    state = dict(enrolled_at=now.isoformat(), members=[dict(security_id="A", ticker="A")], next_boundary=next_boundary(now).isoformat())
    state, packets = advance_detectors(state, fixture, config, now, bootstrap=True)
    assert not packets and state["detectors"]
    store = ForwardStore(tmp_path / "forward.sqlite", config)
    store.save(state)
    restored = store.load()
    assert restored["detectors"] and restored["contexts"]
    view = shadow_snapshot(restored, [], config, now)
    publish_view(tmp_path / "view.json", view)
    assert load_snapshot(tmp_path / "view.json", "SHADOW")["source"] == "SHADOW"
    with pytest.raises(ValueError, match="source mismatch"):
        load_snapshot(tmp_path / "view.json", "REPLAY")
    assert json.loads((tmp_path / "view.json").read_text())["status"] == "WAITING_FOR_PUBLICATION"


def test_actions_reset_feature_warmup_and_source_corrections_are_quarantined():
    from test_stock_idea_replay import full_fixture
    from research.stock_idea_replay import utc
    from research.stock_idea_forward import advance_detectors, build_frame
    fixture, _ = full_fixture()
    config = forward_config()
    now = utc("2026-08-04T13:31:00Z")
    native = [bar for bar in fixture["bars"] if bar["interval"] == "30m"]
    action = dict(security_id="A", ticker="A", action_type="SPLIT", effective_date="2026-08-03")
    frame = build_frame(native, config, actions=[action])
    assert not frame.ready.iloc[-1]
    state = dict(enrolled_at=now.isoformat(), members=[dict(security_id="A", ticker="A")], next_boundary=next_boundary(now).isoformat())
    state, _ = advance_detectors(state, fixture, config, now, bootstrap=True)
    correction = dict(native[-1], revision_id="corrected", close=100.01)
    changed, _ = advance_detectors(state, dict(bars=[correction], actions=[]), config, now)
    assert changed["identity_breaks"]["A"] == "INPUT_CORRECTION_REVIEW"
    assert changed["bars"]["A|30m"][native[-1]["bar_start"]]["revision_id"] == native[-1]["revision_id"]


def test_resident_cycle_publishes_once_and_records_real_late_window(tmp_path):
    from scripts.run_stock_idea_worker import cycle
    from research.stock_idea_forward import packet_record
    from research.stock_idea_replay import utc
    config = forward_config()
    boundary = candidate().trigger_at
    store = ForwardStore(tmp_path / "forward.sqlite", config)
    state = dict(enrolled_at=(boundary - timedelta(minutes=30)).isoformat(), members=[dict(security_id="A", ticker="A")],
                 next_boundary=boundary.isoformat(), last_source_read=(NOW - timedelta(seconds=3)).isoformat(),
                 bars={}, prepared_packets=[packet_record(packet())])
    state, result = cycle(store, state, config, tmp_path / "view.json", clock=lambda: NOW + timedelta(seconds=1),
                          read_inputs=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("no fetch at publication")))
    assert result["status"] == "PUBLISHED" and result["selected"] == 1
    assert len(store.publications()) == 1
    state, result = cycle(store, state, config, tmp_path / "view.json", clock=lambda: NOW + timedelta(seconds=2),
                          read_inputs=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("no duplicate cycle")))
    assert result["status"] == "WAITING_FOR_FINAL_BARS" and len(store.publications()) == 1
    late = window_clock(utc(state["next_boundary"]))[1] + timedelta(seconds=1)
    state, result = cycle(store, state, config, tmp_path / "view.json", clock=lambda: late)
    assert result["status"] == "MISSED_PUBLICATION" and len(store.publications()) == 2


def test_shadow_mark_updates_keep_terminal_exits_frozen():
    from research.stock_idea_forward import update_positions
    original = dict(state="CLOSED", candidate={"security_id": "A"}, exit_price=101., gross=.01)
    state = dict(positions={"plan": original}, bars={}, actions=[])
    assert update_positions(state, forward_config(), NOW)["positions"]["plan"] == original


def test_daily_split_context_uses_adjusted_features_and_raw_execution_scale():
    from test_stock_idea_replay import full_fixture
    from research.stock_idea_forward import build_frame
    fixture, _ = full_fixture()
    bars = [bar for bar in fixture["bars"] if bar["interval"] == "1d" and bar["security_id"] == "A"]
    action = dict(security_id="A", action_type="SPLIT", effective_date="2026-08-03", split_from=1., split_to=2.)
    frame = build_frame(bars, forward_config(), daily=True, actions=[action])
    assert frame.close.iloc[-2] == bars[-2]["close"] / 2
    assert frame.close.iloc[-1] == bars[-1]["close"]
    assert frame.execution_scale.iloc[-2] == 2.
    assert frame.execution_scale.iloc[-1] == 1.
    assert frame.segment.nunique() == 1


def test_resident_does_not_begin_a_heavy_read_too_close_to_deadline(tmp_path):
    from scripts.run_stock_idea_worker import cycle
    config = forward_config()
    store = ForwardStore(tmp_path / "forward.sqlite", config)
    state = dict(enrolled_at=(NOW - timedelta(hours=1)).isoformat(), members=[dict(security_id="A", ticker="A")],
        next_boundary=candidate().trigger_at.isoformat(), last_source_read=(NOW - timedelta(minutes=1)).isoformat(),
        preparation_seconds=20.)
    _, result = cycle(store, state, config, tmp_path / "view.json", clock=lambda: NOW - timedelta(seconds=10),
                     read_inputs=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("would overrun deadline")))
    assert result["status"] == "WAITING_FOR_PUBLICATION"


def test_late_daily_inputs_refresh_context_without_reopening_old_publications(tmp_path, monkeypatch):
    from scripts import run_stock_idea_worker as worker
    from research.stock_idea_forward import packet_record
    from research.stock_idea_replay import utc
    config = forward_config()
    now = utc("2026-09-14T20:40:00Z")
    store = ForwardStore(tmp_path / "forward.sqlite", config)
    old_packet = dict(packet(), interval="1d", market_time=utc("2026-09-14T20:00:00Z"), available_at=now)
    state = dict(enrolled_at="2026-09-14T14:50:00Z", members=[dict(security_id="A", ticker="A")],
        next_boundary=next_boundary(now).isoformat(), last_source_read="2026-09-14T20:16:00Z", bars={}, positions={})
    reads = []
    def read_inputs(members, deadline, **kwargs):
        reads.append((deadline, kwargs))
        return dict(bars=[], actions=[])
    def advance(current, batch, policy, deadline, **kwargs):
        return dict(current, last_source_read=deadline.isoformat(), contexts={"A|2026-09-14": {}}), [old_packet]
    monkeypatch.setattr(worker, "advance_detectors", advance)
    refreshed, result = worker.cycle(store, state, config, tmp_path / "view.json", clock=lambda: now, read_inputs=read_inputs)
    assert result["status"] == "CONTEXT_REFRESHED" and reads[0][1]["include_daily"]
    assert store.load()["last_daily_read"] == now.isoformat() and not store.publications()
    assert refreshed["prepared_packets"] == [packet_record(old_packet)]
    deadline, _ = window_clock(utc(refreshed["next_boundary"]))
    _, result = worker.cycle(store, refreshed, config, tmp_path / "view.json", clock=lambda: deadline, read_inputs=read_inputs)
    assert result["selected"] == 0 and len(reads) == 1


def test_correction_quarantine_blocks_daily_plans_and_records_runtime_sources():
    from dataclasses import replace
    from research.stock_idea_forward import runtime_sources
    config = forward_config()
    original = replace(candidate(), interval="1d", horizon="DAILY")
    state = dict(enrolled_at=(original.trigger_at - timedelta(hours=1)).isoformat(),
        identity_breaks={"A": "INPUT_CORRECTION_REVIEW"}, runtime_sources=runtime_sources())
    daily = dict(packet(), interval="1d", candidates=[original])
    _, publication, _ = forward_decision(state, [daily], boundary=original.trigger_at, actual_time=NOW,
        members=[dict(security_id="A")], policy_hash="policy", config=config)
    assert not publication["selected"]
    assert publication["candidates"][original.episode_id]["health"] == "STALE"
    assert publication["runtime_sources"] == state["runtime_sources"]
    assert len(publication["runtime_sources"]) == 6


def test_publication_reuses_committed_inputs_without_serializing_them_again(tmp_path, monkeypatch):
    config = forward_config()
    store = ForwardStore(tmp_path / "forward.sqlite", config)
    state = dict(enrolled_at=(candidate().trigger_at - timedelta(minutes=30)).isoformat(),
        bars={"A|30m": {}}, detectors={}, contexts={})
    store.save(state)
    encode = store.encode
    def encode_small(value):
        assert "bars" not in value and "detectors" not in value and "contexts" not in value
        return encode(value)
    monkeypatch.setattr(store, "encode", encode_small)
    committed, publication, outbox = forward_decision(state, [packet()], boundary=candidate().trigger_at,
        actual_time=NOW, members=[dict(security_id="A")], policy_hash=store.policy_hash, config=config)
    assert committed["bars"] is state["bars"] and "positions" not in state
    store.save(committed, publication, outbox)
    assert store.load() == store.decode(encode(committed)) and len(store.publications()) == 1


def test_dispatch_deadline_rolls_back_publication_positions_and_outbox(tmp_path):
    from research.stock_idea_forward import ForwardPublicationLate
    config = forward_config()
    store = ForwardStore(tmp_path / "forward.sqlite", config)
    baseline = dict(enrolled_at=(candidate().trigger_at - timedelta(minutes=30)).isoformat(), bars={})
    store.save(baseline)
    state, publication, outbox = forward_decision(baseline, [packet()], boundary=candidate().trigger_at,
        actual_time=NOW, members=[dict(security_id="A")], policy_hash=store.policy_hash, config=config)
    times = iter([NOW + timedelta(seconds=1), NOW + timedelta(seconds=6)])
    with pytest.raises(ForwardPublicationLate, match="rolled back"):
        store.save(state, publication, outbox, clock=lambda: next(times))
    import sqlite3
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM forward_outbox").fetchone()[0] == 0
    assert store.load() == baseline and not store.publications()


def test_resident_records_missed_run_when_persistence_cannot_meet_dispatch_deadline(tmp_path):
    from scripts.run_stock_idea_worker import cycle
    from research.stock_idea_forward import packet_record
    config = forward_config()
    store = ForwardStore(tmp_path / "forward.sqlite", config)
    boundary = candidate().trigger_at
    state = dict(enrolled_at=(boundary - timedelta(minutes=30)).isoformat(), members=[dict(security_id="A", ticker="A")],
        next_boundary=boundary.isoformat(), last_source_read=NOW.isoformat(), bars={}, prepared_packets=[packet_record(packet())])
    store.save(state)
    times = iter([NOW, NOW + timedelta(seconds=6), NOW + timedelta(seconds=7), NOW + timedelta(seconds=8)])
    state, result = cycle(store, state, config, tmp_path / "view.json", clock=lambda: next(times))
    assert result["status"] == "MISSED_PUBLICATION" and not state["positions"]
    assert len(store.publications()) == 1 and not store.publications()[0]["selected"]


def test_source_ready_cycle_waits_then_retries_without_replacing_history_or_schedule(tmp_path, monkeypatch):
    from scripts import run_stock_idea_worker as worker
    from research.stock_idea_forward import READINESS_POLICY, packet_record
    config = forward_config()
    store = ForwardStore(tmp_path / "forward.sqlite", config)
    boundary = candidate().trigger_at
    state = dict(enrolled_at=(boundary - timedelta(minutes=30)).isoformat(), members=[dict(security_id="A", ticker="A")], bars={})
    state, original, outbox = forward_decision(state, [], boundary=boundary, actual_time=NOW,
        members=state["members"], policy_hash=store.policy_hash, config=config)
    state.update(next_boundary=next_boundary(boundary).isoformat(), last_source_read=NOW.isoformat(),
        dispatch_policy=READINESS_POLICY, retry_boundary=boundary.isoformat(), prepared_packets=[packet_record(packet())])
    store.save(state, original, outbox)
    retained_original = store.publications()[0]
    scheduled = state["next_boundary"]
    now = NOW + timedelta(minutes=2)
    state, result = worker.readiness_cycle(store, state, config, tmp_path / "view.json", clock=lambda: now,
        read_inputs=lambda *args, **kwargs: pytest.fail("must wait for complete inputs"), read_readiness=lambda *args, **kwargs: None)
    assert result["status"] == "WAITING_FOR_SOURCE_PUBLICATION" and len(store.publications()) == 1
    monkeypatch.setattr(worker, "advance_detectors", lambda current, *args, **kwargs: (dict(current), []))
    monkeypatch.setattr(worker, "update_positions", lambda current, *args: current)
    now += timedelta(seconds=11)
    state, result = worker.readiness_cycle(store, state, config, tmp_path / "view.json", clock=lambda: now,
        read_inputs=lambda *args, **kwargs: dict(bars=[], actions=[]),
        read_readiness=lambda *args, **kwargs: dict(publications=["complete"]))
    assert result["selected"] == 1 and len(store.publications()) == 2
    assert store.publications()[0] == retained_original and state["next_boundary"] == scheduled
    assert "retry_boundary" not in state
    assert state["positions"][candidate().episode_id]["publication_at"] == now.isoformat()


@pytest.mark.parametrize("source_ids,ready", [([], False), (["B"], False), (["A"], True)])
def test_readiness_checks_selected_security_membership_not_just_complete_label(monkeypatch, source_ids, ready):
    from contextlib import contextmanager
    from equity import stock_idea_forward_source as source
    class Cursor:
        def execute(self, query, parameters=None):
            if "FROM equity_bar_publications" in query:
                assert "published_at<=%s AND created_at<=%s" in query
                assert parameters[-2:] == (NOW, NOW)
        def fetchone(self):
            return dict(publication_id="complete", published_at=NOW, created_at=NOW)
        def fetchall(self):
            return [dict(security_id=security) for security in source_ids]
    @contextmanager
    def cursor():
        yield Cursor()
    monkeypatch.setattr(source, "get_db_cursor", cursor)
    result = source.forward_input_readiness([dict(security_id="A")], candidate().trigger_at, NOW)
    assert bool(result) is ready


def test_closing_readiness_requires_daily_publication_and_uses_exchange_close(monkeypatch):
    from contextlib import contextmanager
    from equity import stock_idea_forward_source as source
    from research.stock_idea_forward import readiness_deadline
    from research.stock_idea_replay import utc
    intervals = []
    class Cursor:
        def execute(self, query, parameters=None):
            if "FROM equity_bar_publications" in query:
                intervals.append(parameters[0])
        def fetchone(self):
            return dict(publication_id="native", published_at=NOW, created_at=NOW) if intervals[-1] == "30m" else None
        def fetchall(self):
            return [dict(security_id="A")]
    @contextmanager
    def cursor():
        yield Cursor()
    monkeypatch.setattr(source, "get_db_cursor", cursor)
    assert source.forward_input_readiness([dict(security_id="A")], candidate().trigger_at, NOW, include_daily=True) is None
    assert intervals == ["30m", "1d"]
    assert readiness_deadline(utc("2026-11-27T18:00:00Z")) == utc("2026-11-27T18:59:55Z")


def test_read_only_geometry_audit_distinguishes_weak_acceptance_from_broken_rules():
    from copy import deepcopy
    from dataclasses import replace
    import pandas as pd
    from scripts.audit_stock_alert_session import geometry_review
    rows = [dict(bar_start=pd.Timestamp("2026-08-03T10:00:00Z") + pd.Timedelta(minutes=30 * index),
        bar_end=pd.Timestamp("2026-08-03T10:30:00Z") + pd.Timedelta(minutes=30 * index),
        open=99., high=100., low=98., close=99., volume=100., atr_prior=1., range_ratio=.7,
        ema20=99., ema50=105., revision_id=str(index), segment=1, ordinal=index)
        for index in range(12)]
    rows[10].update(open=100., high=101.5, low=99.8, close=101.)
    rows[11].update(open=101., high=101.2, low=100.01, close=100.05)
    frame = pd.DataFrame(rows)
    original = frame.copy(deep=True)
    plan = candidate(model="acceptance", trigger_at=rows[-1]["bar_end"].to_pydatetime(),
        anchor=f"{frame.bar_start.iloc[0]}:{frame.bar_start.iloc[9]}", price=100.05,
        reference=100., stop=99.7, target=102., activation_atr=1.)
    result = geometry_review(plan, frame, dict(stop_buffer_atr=.1))
    assert all(result["checks"].values()) and result["trigger_bar_change_pct"] < 0
    assert not geometry_review(replace(plan, target=103.), frame, dict(stop_buffer_atr=.1))["checks"]["target"]
    broken = deepcopy(rows)
    broken[-1]["close"] = 99.99
    assert not geometry_review(plan, pd.DataFrame(broken), dict(stop_buffer_atr=.1))["checks"]["next_bar_confirmation"]
    pd.testing.assert_frame_equal(frame, original)