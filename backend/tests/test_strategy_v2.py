import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from research.strategy_v2 import CONFIGURATIONS, StrategyConfig, detect, entry_status


def frame_base():
    frame = pd.DataFrame(dict(open=[100.] * 25, high=[102.] * 25, low=[98.] * 25, close=[100.] * 25,
                              volume=[1000.] * 25, atr_prior=[2.] * 25, range_ratio=[.5] * 25,
                              ema20=[100.] * 25, ema50=[99.] * 25, ema50_prior10=[98.] * 25,
                              rs63=[.1] * 25, rs_percentile=[.9] * 25, ready=[True] * 25,
                              ticker=["TEST"] * 25, security_id=["identity"] * 25, segment=[1] * 25,
                              bar_revision_id=[str(index) for index in range(25)]), index=pd.bdate_range("2026-01-01", periods=25).date)
    return frame


def mirror(frame):
    result = frame.copy()
    for column in ("open", "close", "ema20", "ema50", "ema50_prior10"):
        result[column] = 200 - frame[column]
    result["high"], result["low"] = 200 - frame.low, 200 - frame.high
    result["rs63"] = -frame.rs63
    result["rs_percentile"] = 1 - frame.rs_percentile
    return result


def test_range_acceptance_freezes_boundary_and_mirrors():
    frame = frame_base()
    frame.iloc[20, frame.columns.get_indexer(["open", "high", "low", "close"])] = [101, 104, 101, 103]
    frame.iloc[21, frame.columns.get_indexer(["open", "high", "low", "close"])] = [103, 104, 102.5, 103.5]
    config = StrategyConfig("range_breakout_acceptance_v1", "chase1", 10)
    events, transitions = detect(frame, config)
    assert len(events) == 1 and events[0]["direction"] == 1
    assert events[0]["reference"] == 102 and events[0]["target"] == 106
    assert events[0]["stop"] == 100.8
    assert [row["status"] for row in transitions[:2]] == ["SETUP", "TRIGGERED"]
    assert detect(frame.iloc[:22], config)[0] == events
    reflected = detect(mirror(frame), config)[0]
    assert len(reflected) == 1 and reflected[0]["direction"] == -1
    assert reflected[0]["stop"] == 200 - events[0]["stop"]


def test_failed_extension_waits_for_return_and_expires():
    frame = frame_base()
    frame.iloc[20, frame.columns.get_indexer(["open", "high", "low", "close"])] = [101, 104, 101, 103]
    frame.iloc[21, frame.columns.get_indexer(["open", "high", "low", "close"])] = [103, 103.5, 100, 101]
    config = StrategyConfig("failed_extension_reversal_v1", "deadline3", 5)
    events, _ = detect(frame, config)
    assert len(events) == 1 and events[0]["direction"] == -1
    assert events[0]["target"] == 100 and events[0]["stop"] == 104.2
    assert len(detect(mirror(frame), config)[0]) == 1
    frame.loc[frame.index[21:], "close"] = 103.5
    assert not detect(frame, config)[0]
    assert any(row["status"] == "EXPIRED" for row in detect(frame, config)[1])


def test_resumption_requires_actual_turn_and_one_event_per_anchor():
    frame = frame_base()
    frame.iloc[18, frame.columns.get_indexer(["high", "close"])] = [105, 104]
    frame.iloc[19, frame.columns.get_indexer(["open", "high", "low", "close"])] = [104, 104.5, 102.5, 103.2]
    frame.iloc[20, frame.columns.get_indexer(["open", "high", "low", "close"])] = [103, 103, 101, 102]
    frame.iloc[21, frame.columns.get_indexer(["open", "high", "low", "close"])] = [102, 104, 102, 103.5]
    config = StrategyConfig("relative_trend_resumption_v1", "rs70", 21)
    assert not detect(frame.iloc[:21], config)[0]
    events, _ = detect(frame, config)
    assert len(events) == 1 and events[0]["target"] == 105
    assert len(detect(mirror(frame), config)[0]) == 1
    assert detect(frame.iloc[:22], config)[0] == events


def test_input_break_invalidates_setup_and_entry_gates_are_explicit():
    frame = frame_base()
    frame.iloc[20, frame.columns.get_indexer(["open", "high", "low", "close"])] = [101, 104, 101, 103]
    frame.iloc[21, frame.columns.get_loc("segment")] = 2
    config = StrategyConfig("range_breakout_acceptance_v1", "chase1", 10)
    events, transitions = detect(frame, config)
    assert not events and any(row["status"] == "INVALIDATED" for row in transitions)
    event = dict(family=config.family, direction=1, reference=102, stop=100, target=106,
                 atr_at_activation=2, configuration={"chase_atr": .5})
    assert entry_status(event, 103, 100) == "FILLED"
    assert entry_status(event, 103.1, 100) == "INSUFFICIENT_TARGET_ROOM"
    assert entry_status(event, 103, 0) == "ZERO_VOLUME_NO_FILL"
    assert entry_status(event, np.nan, 100) == "UNRESOLVED_ENTRY"
    assert len(CONFIGURATIONS) == 9


def test_config_file_and_implementation_have_same_nine_trials():
    import json
    import pytest
    from scripts.run_equity_strategy_v2 import validate_configuration
    config = json.loads((Path(__file__).resolve().parents[2] / "docs" / "equity_strategy_v2_config.json").read_text())
    validate_configuration(config)
    for family, values in config["parameter_grid"].items():
        trials = [trial for trial in CONFIGURATIONS if trial.family == family]
        assert len(trials) == 3 and {trial.horizon for trial in trials} == {values["horizon"]}
        parameter = next(name for name in values if name != "horizon")
        assert [getattr(trial, parameter) for trial in trials] == values[parameter]
    config["parameter_grid"]["relative_trend_resumption_v1"]["rs_threshold"] = [.5, .6, .7]
    with pytest.raises(ValueError, match="nine-trial"):
        validate_configuration(config)


def test_enrichment_is_prefix_causal():
    from research.strategy_v2 import enrich_prices
    frame = frame_base()
    frame["clock_valid"] = True
    frame["identity_disagreement"] = False
    returns = pd.Series(.01, index=frame.index)
    ranks = pd.Series(.8, index=frame.index)
    full = enrich_prices(frame, returns, ranks)
    prefix = enrich_prices(frame.iloc[:22], returns.iloc[:22], ranks.iloc[:22])
    for column in ("ema20", "ema50", "ema50_prior10", "atr_prior", "range_ratio"):
        pd.testing.assert_series_equal(full[column].iloc[:22], prefix[column])


def test_walk_forward_never_selects_on_test_or_second_sample_returns():
    from scripts.run_equity_strategy_v2 import walk_forward
    from copy import deepcopy
    sessions = [str(value.date()) for value in pd.bdate_range("2026-01-01", periods=15)]
    config = dict(training_sessions=8, test_sessions=4, label_purge_sessions=2,
                  selection_benchmark="MOMENTUM_12_1", selection_cost_bps=10,
                  minimum_training_dates=2, minimum_training_signals=2, minimum_pair_coverage=.8)
    rows = [dict(family="range_breakout_acceptance_v1", direction=1, variant="chase1", sample="sample-1",
                 control="MOMENTUM_12_1", cost_bps=10, session=session, maturity_session=sessions[index + 1],
                 complete=True, compared_signals=2, mean_signal=.02, lift=.01)
            for index, session in enumerate(sessions[:-1])]
    first, _ = walk_forward(rows, sessions, config)
    changed = deepcopy(rows)
    for row in changed:
        if row["session"] >= sessions[8]:
            row["lift"] = 99
    changed.extend(dict(row, sample="sample-2", lift=-99) for row in rows)
    repeated, _ = walk_forward(changed, sessions, config)
    assert first[:6] == repeated[:6]
    assert next(row for row in first if row["family"] == "range_breakout_acceptance_v1" and row["direction"] == 1)["chosen"] == "chase1"
    assert next(row for row in first if row["direction"] == -1)["chosen"] is None


def test_calendar_bootstrap_is_deterministic_and_counts_missing_dates():
    from scripts.run_equity_strategy_v2 import bootstrap_summary
    sessions = [str(value.date()) for value in pd.bdate_range("2026-01-01", periods=100)]
    rows = [dict(session=session, lift=.01, mean_signal=.02, complete=index % 10 != 0,
                 compared_signals=2, original_signals=2) for index, session in enumerate(sessions)]
    config = dict(bootstrap_block_sessions=20, bootstrap_seed=2, bootstrap_replicates=100)
    first = bootstrap_summary(rows, sessions, config)
    assert first == bootstrap_summary(rows, sessions, config)
    assert first["measured_dates"] == 90 and first["coverage"] == .9
    assert abs(first["mean_lift"] - .01) < 1e-12 and first["ci_low"] > 0


def test_v2_trade_plan_uses_actual_entry_bracket_and_existing_exit_engine():
    from scripts.run_equity_strategy_v2 import evaluate_event
    from test_equity_outcomes import bar
    from dataclasses import replace
    from decimal import Decimal
    from types import SimpleNamespace
    from uuid import uuid4
    import exchange_calendars

    calendar = exchange_calendars.get_calendar("XNYS")
    identity = uuid4()
    sessions = calendar.sessions_in_range("2026-08-31", "2026-09-03")
    bars = tuple(replace(bar(calendar.session_open(session).to_pydatetime(), 103, 107, 102, 104),
                         interval="1d", security_id=identity, bar_end=calendar.session_close(session).to_pydatetime(),
                         system_observed_at=calendar.session_close(session).to_pydatetime()) for session in sessions)
    reader = SimpleNamespace(source_cutoff=bars[-1].system_observed_at, list_final_after=lambda *args, **kwargs: bars)
    event = dict(event_id=str(uuid4()), family="range_breakout_acceptance_v1", variant="chase1", ticker="AAPL",
                 security_id=str(identity), direction=1, session="2026-08-28", horizon=10, setup_id="test",
                 stop=100., target=106., reference=102., atr_at_activation=2., configuration={"chase_atr": 1.})
    result = evaluate_event(event, reader, sessions[-1].date())
    assert result["entry_state"] == "FILLED" and result["state"] == "ENTERED"
    assert abs(result["gross"] - (106 / 103 - 1)) < 1e-12
    changed = (replace(bars[0], open_price=Decimal("105")),) + bars[1:]
    reader.list_final_after = lambda *args, **kwargs: changed
    assert evaluate_event(event, reader, sessions[-1].date())["entry_state"] == "INSUFFICIENT_TARGET_ROOM"
    assert evaluate_event(event, reader, sessions[-1].date())["gross"] == 0
    reader.list_final_after = lambda *args, **kwargs: bars[1:]
    assert evaluate_event(event, reader, sessions[-1].date())["gross"] is None


def test_control_selection_excludes_family_signals_and_future_returns():
    from scripts.run_equity_strategy_v2 import control_names
    pool = pd.DataFrame(dict(ticker=[f"T{index:02d}" for index in range(20)], momentum=np.arange(20),
                             rs63=np.arange(20), volatility20=np.arange(20))).set_index("ticker", drop=False)
    config = dict(momentum_top_fraction=.1, matched_control_caliper=.2)
    choices = control_names(pool, {"T10", "T11"}, 1, config)
    assert choices["MOMENTUM_12_1"] == ["T19", "T18"]
    assert all(control not in {"T10", "T11"} for _, control in choices["MATCHED_STRENGTH_VOLATILITY"])
    pool["future_return"] = np.arange(20)[::-1]
    assert control_names(pool, {"T10", "T11"}, 1, config) == choices


def test_opposed_signals_are_recorded_without_combining_votes():
    from scripts.run_equity_strategy_v2 import signal_conflicts
    events = [dict(ticker="TEST", session="2026-09-01", direction=direction, event_id=str(index))
              for index, direction in enumerate((1, 1, -1))]
    assert signal_conflicts(events[:2]) == []
    conflict = signal_conflicts(events)
    assert len(conflict) == 1 and conflict[0]["state"] == "OPPOSED_SIGNALS_ABSTAIN"
    assert conflict[0]["event_ids"] == ["0", "1", "2"]


def test_comparisons_keep_nonfills_cash_and_missing_positions_unresolved():
    from scripts.run_equity_strategy_v2 import build_comparisons
    frame = frame_base()
    frame["momentum"], frame["volatility20"] = .1, .2
    frame["session"] = frame.index
    for horizon in (5, 10, 21):
        frame[f"gross_{horizon}"], frame[f"state_{horizon}"] = .02, "OBSERVED"
    other = frame.copy()
    other["ticker"] = "CONTROL"
    frames = {"TEST": frame, "CONTROL": other}
    event = dict(event_id="event", ticker="TEST", family="range_breakout_acceptance_v1", variant="chase1",
                 direction=1, session=str(frame.index[-1]), horizon=10)
    outcome = dict(event_id="event", gross=0., cost_applies=False, maturity_session="2026-03-01")
    config = dict(momentum_top_fraction=.1, matched_control_caliper=.2, cost_scenarios_bps=[4, 10, 25])
    rows = build_comparisons([event], [outcome], frames, {"sample-1": ["TEST", "CONTROL"]}, config, lambda row: None)
    momentum = [row for row in rows if row["control"] == "MOMENTUM_12_1"]
    assert len(momentum) == 3 and all(row["mean_signal"] == 0 and row["filled"] == 0 for row in momentum)
    assert abs(momentum[-1]["mean_control"] - .0175) < 1e-12
    outcome["gross"] = None
    missing = build_comparisons([event], [outcome], frames, {"sample-1": ["TEST", "CONTROL"]}, config, lambda row: None)
    assert all(row["lift"] is None for row in missing)