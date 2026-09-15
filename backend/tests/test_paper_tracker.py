from datetime import datetime, timezone
import json

import pytest

from research.paper_tracker import (
    PaperLedger, capture_state, plan_cohorts, build_signal, covered_splits,
    simulate_fill, mark_portfolios, run_once, report_study, digest, ReadOnlyInputs,
    attach_native_split_coverage,
)


def test_enrollment_never_uses_a_completed_decision_close():
    cohorts = plan_cohorts(datetime(2026, 9, 12, 12, tzinfo=timezone.utc))
    assert len(cohorts) == 6 and cohorts[0]["session"] == "2026-09-14"
    assert cohorts[0]["entry_session"] == "2026-09-15"
    assert cohorts[1]["session"] == cohorts[0]["exit_session"]
    assert capture_state(cohorts[0], datetime(2026, 9, 14, 20, 14, tzinfo=timezone.utc)) == "WAITING_FOR_DECISION"
    assert capture_state(cohorts[0], datetime(2026, 9, 14, 20, 16, tzinfo=timezone.utc)) == "CAPTURE_ALLOWED"
    assert capture_state(cohorts[0], datetime(2026, 9, 15, 13, 25, tzinfo=timezone.utc)) == "MISSED_SIGNAL_WINDOW"
    assert plan_cohorts(datetime(2026, 9, 14, 20, tzinfo=timezone.utc))[0]["session"] == "2026-09-15"


def test_ledger_is_immutable_idempotent_and_checksum_checked(tmp_path):
    ledger = PaperLedger(tmp_path)
    with ledger.locked():
        assert ledger.append("signal_1", {"score": 1})
        assert not ledger.append("signal_1", {"score": 1})
        with pytest.raises(ValueError, match="immutable"):
            ledger.append("signal_1", {"score": 2})
        with pytest.raises(RuntimeError, match="busy"):
            with ledger.locked():
                pass
    path = tmp_path / "signal_1.json"
    document = json.loads(path.read_text())
    document["payload"]["score"] = 2
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="checksum"):
        ledger.read("signal_1")
    assert not (tmp_path / ".lock").exists()


@pytest.fixture
def signal_inputs():
    import exchange_calendars
    calendar = exchange_calendars.get_calendar("XNYS")
    cohort = plan_cohorts(datetime(2026, 9, 12, 12, tzinfo=timezone.utc))[0]
    now = datetime(2026, 9, 14, 20, 20, tzinfo=timezone.utc)
    dates = [calendar.session_offset(cohort["session"], offset).date().isoformat() for offset in range(-252, 1)]
    members = [dict(ticker="TEST", security_id="stock")]
    bars = []
    for index, session in enumerate(dates):
        close = calendar.session_close(session).isoformat()
        bars.append(dict(ticker="TEST", security_id="stock", session_date=session, bar_revision_id=str(index),
                         open=100 + index, high=101 + index, low=99 + index, close=100 + index, volume=1000,
                         bar_end=close, system_observed_at=close, created_at=close, availability_mode="LIVE_OBSERVED"))
    bars.append(dict(bars[-1], ticker="SPY", security_id="market"))
    actions = dict(coverage=[dict(ticker="TEST", action_type="SPLIT", security_id="stock", coverage_id="coverage",
                                 window_start=dates[0], window_end=dates[-1], first_observed_at=now.isoformat(),
                                 created_at=now.isoformat(), response_action_count=0)], members=[], other=[])
    manifest = dict(minimum_eligible=1, minimum_feature_coverage=.9,
                    model=dict(model_sha256="model", features=["mom_12_1", "rev_5"], feature_mean=[0, 0],
                               feature_scale=[1, 1], coefficients=[1, 1], label_mean=0))
    return manifest, cohort, now, {}, members, bars, actions


def test_signal_uses_frozen_model_and_excludes_future_prices(signal_inputs):
    result = build_signal(*signal_inputs)
    candidate = result["candidates"][0]
    assert candidate["mom_12_1"] == pytest.approx(331 / 100 - 1)
    assert candidate["rev_5"] == pytest.approx(-(352 / 347 - 1))
    assert result["portfolios"]["RIDGE"][0]["weight"] == .75
    assert result["broker_orders"] == 0
    signal_inputs[5].append(dict(signal_inputs[5][0], session_date="2026-09-15", close=999999))
    assert build_signal(*signal_inputs)["candidates"] == result["candidates"]
    with pytest.raises(ValueError, match="backdate"):
        build_signal(signal_inputs[0], signal_inputs[1], datetime(2026, 9, 15, 14, tzinfo=timezone.utc), *signal_inputs[3:])


def test_signal_blocks_missing_or_late_inputs(signal_inputs):
    signal_inputs[5][0]["created_at"] = "2026-09-15T15:00:00+00:00"
    with pytest.raises(ValueError, match="insufficient"):
        build_signal(*signal_inputs)


def test_missing_split_coverage_never_means_no_splits(signal_inputs):
    manifest, cohort, now, run, members, bars, actions = signal_inputs
    actions["coverage"] = []
    with pytest.raises(ValueError, match="insufficient"):
        build_signal(manifest, cohort, now, run, members, bars, actions)


def test_split_coverage_composes_windows_but_rejects_gaps(signal_inputs):
    actions = signal_inputs[-1]
    now = signal_inputs[2]
    original = actions["coverage"][0]
    original["window_end"] = "2026-08-31"
    actions["coverage"].append(dict(original, coverage_id="new", window_start="2026-09-01", window_end="2026-09-14"))
    splits, ids = covered_splits(actions, "TEST", "stock", "2026-08-01", "2026-09-14", now)
    assert splits == [] and ids == ["coverage", "new"]
    actions["coverage"][1]["window_start"] = "2026-09-02"
    with pytest.raises(ValueError, match="unavailable"):
        covered_splits(actions, "TEST", "stock", "2026-08-01", "2026-09-14", now)


def test_simulated_mark_costs_and_missing_entries(signal_inputs):
    import exchange_calendars
    manifest, cohort, _, _, _, bars, actions = signal_inputs
    manifest["cost_bps"] = 10
    entry_day = cohort["entry_session"]
    close = exchange_calendars.get_calendar("XNYS").session_close(entry_day).isoformat()
    now = datetime(2026, 9, 15, 21, tzinfo=timezone.utc)
    entry = dict(bars[0], session_date=entry_day, open=100, close=110, high=111, low=99,
                 bar_end=close, system_observed_at=close, created_at=close)
    actions["coverage"][0].update(window_start=entry_day, window_end=cohort["exit_session"])
    position = dict(ticker="TEST", security_id="stock", weight=.75)
    signal = dict(portfolios={name: [position] for name in ("RIDGE", "MOMENTUM", "SPY")})
    fill = simulate_fill(position, cohort, [entry], now)
    mark, issues = mark_portfolios(manifest, cohort, signal, {"stock": fill}, [entry], actions, entry_day, now)
    assert not issues and fill["status"] == "SIMULATED_DAILY_OPEN"
    assert mark["portfolios"]["RIDGE"]["relative_equity"] == pytest.approx(1.075 - .000375)
    assert mark["portfolios"]["RIDGE"]["entry_relative_equity"] == .999625
    missing, issues = mark_portfolios(manifest, cohort, signal, {}, [entry], actions, entry_day, now)
    assert missing is None and len(issues) == 3
    zero = simulate_fill(position, cohort, [dict(entry, volume=0)], now)
    mark, _ = mark_portfolios(manifest, cohort, signal, {"stock": zero}, [], actions, entry_day, now)
    assert mark["portfolios"]["RIDGE"]["relative_equity"] == 1
    assert mark["portfolios"]["RIDGE"]["exposure"] == 0


def test_split_mark_preserves_shares_and_exit_fee(signal_inputs):
    import exchange_calendars
    manifest, cohort, _, _, _, bars, actions = signal_inputs
    manifest["cost_bps"] = 10
    session = cohort["exit_session"]
    close = exchange_calendars.get_calendar("XNYS").session_close(session).isoformat()
    now = datetime.fromisoformat(close)
    mark_bar = dict(bars[0], session_date=session, open=50, close=50, high=51, low=49,
                    bar_end=close, system_observed_at=close, created_at=close)
    coverage = actions["coverage"][0]
    coverage.update(window_start=cohort["entry_session"], window_end=session, response_action_count=1)
    actions["members"] = [dict(coverage_id="coverage", corporate_action_id="split", ticker="TEST", security_id="stock",
                                action_type="SPLIT", effective_date=session, split_from=1, split_to=2,
                                first_observed_at=close, created_at=close)]
    position = dict(ticker="TEST", security_id="stock", weight=.75)
    signal = dict(portfolios={name: [position] for name in ("RIDGE", "MOMENTUM", "SPY")})
    fill = dict(ticker="TEST", security_id="stock", entry_session=cohort["entry_session"], price=100, status="SIMULATED_DAILY_OPEN")
    mark, issues = mark_portfolios(manifest, cohort, signal, {"stock": fill}, [mark_bar], actions, session, now)
    assert not issues and mark["portfolios"]["RIDGE"]["relative_equity"] == pytest.approx(.99925)
    actions["other"] = [dict(ticker="TEST", effective_date=session, action_type="MERGER")]
    assert mark_portfolios(manifest, cohort, signal, {"stock": fill}, [mark_bar], actions, session, now)[0] is None


def test_missed_signal_is_not_backfilled_and_waiting_does_not_read_database(tmp_path):
    from unittest.mock import Mock
    ledger = PaperLedger(tmp_path)
    enrolled = datetime(2026, 9, 12, 12, tzinfo=timezone.utc)
    manifest = dict(study_id="test", enrolled_at=enrolled.isoformat(), cohorts=plan_cohorts(enrolled), model={"model_sha256": "fixed"})
    with ledger.locked():
        ledger.append("manifest", manifest)
        inputs = Mock()
        assert run_once(ledger, now=enrolled, inputs=inputs)["updates"][0]["status"] == "WAITING_FOR_DECISION"
        assert not inputs.mock_calls
        now = datetime(2026, 9, 15, 14, tzinfo=timezone.utc)
        run_once(ledger, now=now, inputs=inputs)
        run_once(ledger, now=now, inputs=inputs)
        assert ledger.read("missed_1")["status"] == "MISSED_SIGNAL_WINDOW"
        assert ledger.read("signal_1") is None and not inputs.mock_calls
    report = report_study(ledger)
    assert report["drawdown_breached"] is None and report["cumulative_return_through_contiguous_marks_pct"] is None
    assert report["completed_cohorts"] == 0


def test_daily_sql_uses_actual_observation_and_creation_cutoffs():
    from unittest.mock import MagicMock
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    now = datetime(2026, 9, 14, 21, tzinfo=timezone.utc)
    ReadOnlyInputs(cursor).bars(["TEST"], "2025-09-11", "2026-09-14", now)
    query, parameters = cursor.execute.call_args.args
    assert "system_observed_at<=%s AND created_at<=%s" in query
    assert "adjusted=FALSE AND is_final" in query
    assert "replay_available_at" not in query
    assert parameters[-3:] == (now, now, now)


def test_native_split_evidence_bridges_only_visible_exact_response_scope(signal_inputs):
    now = signal_inputs[2]
    empty = dict(coverage=[], members=[], other=[], native_split_evidence=dict(start="2026-08-01", end="2026-09-14",
                    observed_at=now.isoformat(), rows=[dict(ticker="TEST", execution_date="2026-09-10", split_from=1, split_to=2)]))
    bundle = attach_native_split_coverage(empty, {"TEST": "stock"}, "2026-08-01", "2026-09-14", now)
    splits, ids = covered_splits(bundle, "TEST", "stock", "2026-08-01", "2026-09-14", now)
    assert len(splits) == len(ids) == 1 and splits[0]["split_to"] == 2
    assert empty["coverage"] == []
    with pytest.raises(ValueError, match="scope or availability"):
        attach_native_split_coverage(empty, {"TEST": "stock"}, "2026-08-01", "2026-09-15", now)
    empty["native_split_evidence"]["observed_at"] = "2026-09-15T21:00:00+00:00"
    with pytest.raises(ValueError, match="scope or availability"):
        attach_native_split_coverage(empty, {"TEST": "stock"}, "2026-08-01", "2026-09-14", now)


def test_observer_records_before_entry_then_marks_idempotently(tmp_path, signal_inputs):
    from unittest.mock import MagicMock
    import exchange_calendars

    manifest, cohort, now, run, members, bars, actions = signal_inputs
    manifest.update(study_id="test", enrolled_at="2026-09-12T12:00:00+00:00", cohorts=plan_cohorts(datetime(2026, 9, 12, 12, tzinfo=timezone.utc)), cost_bps=10)
    inputs = MagicMock()
    inputs.universe.return_value = (run, members)
    inputs.bars.return_value = bars
    inputs.actions.return_value = actions
    ledger = PaperLedger(tmp_path)
    with ledger.locked():
        ledger.append("manifest", manifest)
        result = run_once(ledger, now=now, inputs=inputs)
        signal = ledger.read("signal_1")
        assert result["report"]["contiguous_marked_sessions"] == 0
        assert signal["recorded_at"] == now.isoformat()
        first_hash = digest(signal)
        entry_day = cohort["entry_session"]
        close = exchange_calendars.get_calendar("XNYS").session_close(entry_day).isoformat()
        entry = dict(bars[0], session_date=entry_day, open=100, close=110, high=111, low=99,
                     bar_end=close, system_observed_at=close, created_at=close)
        inputs.bars.return_value = [entry, dict(entry, ticker="SPY", security_id="market")]
        stock_coverage = dict(actions["coverage"][0], window_start=entry_day, window_end=cohort["exit_session"])
        inputs.actions.return_value = dict(coverage=[stock_coverage, dict(stock_coverage, ticker="SPY", security_id="market", coverage_id="spy")], members=[], other=[])
        mark_time = datetime(2026, 9, 15, 21, tzinfo=timezone.utc)
        result = run_once(ledger, now=mark_time, inputs=inputs)
        assert result["report"]["contiguous_marked_sessions"] == 1
        assert result["report"]["cumulative_return_through_contiguous_marks_pct"]["RIDGE"] == pytest.approx(7.4625)
        assert result["report"]["drawdown_through_contiguous_marks_pct"]["RIDGE"] == pytest.approx(-.0375)
        files_before = sorted(path.name for path in tmp_path.glob("*.json"))
        run_once(ledger, now=mark_time, inputs=inputs)
        assert sorted(path.name for path in tmp_path.glob("*.json")) == files_before
        assert digest(ledger.read("signal_1")) == first_hash
        assert ledger.read("mark_1_2026-09-15")["broker_orders"] == 0


def test_split_adjusted_feature_matches_original_definition(signal_inputs):
    manifest, cohort, now, run, members, bars, actions = signal_inputs
    baseline = build_signal(*signal_inputs)["candidates"][0]
    for row in bars:
        if row["ticker"] == "TEST" and row["session_date"] < "2026-09-14":
            for field in ("open", "high", "low", "close"):
                row[field] *= 2
    coverage = actions["coverage"][0]
    coverage["response_action_count"] = 1
    actions["members"] = [dict(coverage_id="coverage", corporate_action_id="split", ticker="TEST", security_id="stock",
                                action_type="SPLIT", effective_date="2026-09-14", split_from=1, split_to=2,
                                first_observed_at=now.isoformat(), created_at=now.isoformat())]
    current = build_signal(manifest, cohort, now, run, members, bars, actions)["candidates"][0]
    assert current["mom_12_1"] == pytest.approx(baseline["mom_12_1"])
    assert current["rev_5"] == pytest.approx(baseline["rev_5"])