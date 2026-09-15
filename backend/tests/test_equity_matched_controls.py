import sys
from pathlib import Path

import numpy as np
import pandas as pd
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.report_equity_matched_controls import compare_day, fixed_sessions, plan_comparison, portfolio_result, price_panel


def test_matching_uses_only_prior_momentum_and_excludes_active_signals():
    pool = pd.DataFrame({"ticker": [f"T{index:02d}" for index in range(20)], "momentum": np.arange(20) / 100}).set_index("ticker", drop=False)
    selection = plan_comparison(pool, ["T10", "T11"], {"T10", "T11", "T12"}, 1)
    assert [row["control"] for row in selection["matched_pairs"]] == ["T09", "T09"]
    assert selection["momentum_controls"] == ["T19", "T18"]
    assert not set(selection["no_signal_controls"]) & {"T10", "T11", "T12"}
    short = plan_comparison(pool, ["T10"], {"T10"}, -1)
    assert short["momentum_controls"] == ["T00", "T01"]


def test_missing_returns_never_change_selection_or_renormalize_weights():
    result = portfolio_result(["A", "B", "B"], {"A": (0.1, "OBSERVED"), "B": (None, "MISSING")}, 1)
    assert result["planned"] == 3 and result["missing"] == 2
    assert not result["complete"] and result["net_return"] is None
    no_fill = portfolio_result(["A"], {"A": (0.0, "NO_FILL")}, -1)
    assert no_fill["complete"] and no_fill["net_return"] == 0 and no_fill["no_fill"] == 1


def test_unmatched_signals_and_unknown_features_are_explicit():
    pool = pd.DataFrame({"ticker": ["A", "B", "C"], "momentum": [0.0, 1.0, np.nan]}).set_index("ticker", drop=False)
    selection = plan_comparison(pool, ["A", "C"], {"A", "C"}, 1)
    assert selection["feature_excluded"] == 1 and selection["unmatched_signals"] == 1
    result = compare_day(selection, {"A": (0.1, "OBSERVED"), "B": (0.2, "OBSERVED")}, 1, "MOMENTUM_MATCHED")
    assert not result["complete"] and not result["decision_covered"]


def test_common_schedule_does_not_depend_on_signals_or_outcomes():
    dates = fixed_sessions("2026-08-03", "2026-09-03", 5)
    assert sorted(map(str, dates)) == ["2026-08-03", "2026-08-10", "2026-08-17", "2026-08-24"]


def test_price_panel_has_causal_momentum_and_strict_forward_paths():
    import exchange_calendars
    from test_equity_outcomes import bar, subject
    from equity.outcomes import default_directional_policy, evaluate_directional_outcome
    from uuid import uuid4

    calendar = exchange_calendars.get_calendar("XNYS")
    sessions = calendar.sessions_in_range("2025-01-02", "2026-03-02")
    identity = uuid4()
    bars = tuple(replace(bar(calendar.session_open(session).to_pydatetime(), 100 + index, 102 + index, 99 + index, 101 + index),
                         interval="1d", security_id=identity, bar_end=calendar.session_close(session).to_pydatetime(),
                         system_observed_at=calendar.session_close(session).to_pydatetime(),
                         replay_available_at=calendar.session_close(session).to_pydatetime())
                 for index, session in enumerate(sessions))
    panel = price_panel("AAPL", bars, [])
    assert panel.momentum.iloc[:252].isna().all()
    assert panel.momentum.iloc[252] == (101 + 231) / 101 - 1
    assert panel.gross_5.iloc[252] == (101 + 257) / (100 + 253) - 1
    signal = replace(subject(), security_id=identity, interval="1d", market_time=bars[252].bar_end,
                     observed_at=bars[252].bar_end, valid_until=bars[252].bar_end + timedelta(days=40))
    for horizon in (5, 10, 21):
        policy = default_directional_policy(source_name=signal.source_name, source_version=signal.source_version,
                                            interval="1d", horizons={f"{horizon}d": horizon}, effective_from=signal.observed_at)
        reference = evaluate_directional_outcome(signal, policy, f"{horizon}d", bars[253:])
        assert reference.entry_status == "ENTERED"
        assert abs(reference.gross_return - panel[f"gross_{horizon}"].iloc[252]) < 1e-12
    prefix = price_panel("AAPL", bars[:253], [])
    assert prefix.momentum.iloc[-1] == panel.momentum.iloc[252]
    missing = price_panel("AAPL", bars[:255] + bars[256:], [])
    assert np.isnan(missing.gross_5.iloc[252])
    action = price_panel("AAPL", bars, [bars[255].session_date])
    assert np.isnan(action.gross_5.iloc[252])
    no_fill = price_panel("AAPL", bars[:253] + (replace(bars[253], volume=Decimal("0")),) + bars[254:], [])
    assert no_fill.gross_5.iloc[252] == 0 and no_fill.state_5.iloc[252] == "NO_FILL"


def test_summaries_use_identical_dates_in_both_samples_and_count_missing():
    from scripts.report_equity_matched_controls import summarize_comparisons, CONTROLS

    plan = dict(start="2026-08-03", end="2026-09-03", groups=[{"name": "sample-1/ma-crossover-9-21-v1"}, {"name": "sample-2/ma-crossover-9-21-v1"}])
    rows = []
    for sample in ("sample-1", "sample-2"):
        for session in ("2026-08-03", "2026-08-10", "2026-08-17"):
            complete = not (sample == "sample-2" and session == "2026-08-10")
            for control in CONTROLS:
                rows.append(dict(source_name="MA_CROSSOVER_9_21", source_version="ma_crossover_9_21_v1", direction=1,
                                 horizon=5, control=control, sample=sample, session=session, complete=complete,
                                 decision_covered=True, feature_excluded=0, unmatched=0, incremental_return=.01 if complete else None,
                                 signal={"net_return": .02, "planned": 2}, comparator={"net_return": .01}))
    result = summarize_comparisons(rows, plan)
    selected = [row for row in result if row["direction"] == 1 and row["horizon"] == 5 and row["control"] == CONTROLS[0]]
    assert len(selected) == 2
    assert selected[0]["measured_dates_sha256"] == selected[1]["measured_dates_sha256"]
    assert all(row["statistics"]["periods"] == 2 and row["missing_pair_dates"] == 1 and row["common_signal_dates"] == 3 for row in selected)