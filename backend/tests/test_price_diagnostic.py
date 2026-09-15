import numpy as np

import pandas as pd

import pytest

from research.price_diagnostic import score_causal_ridge

@pytest.fixture
def ridge_training_panel():
    records = []
    for session, exit_session in (("2024-01-02", "2024-02-01"), ("2024-02-02", "2024-03-01"),
                                  ("2024-03-04", "2024-04-02"), ("2024-04-02", "2024-05-01")):
        for index in range(6):
            records.append(dict(ticker=f"T{index}", session=session, entry_session=(pd.Timestamp(session) + pd.Timedelta(days=1)).date().isoformat(),
                                exit_session=exit_session, feature_ready=True, mom_12_1=index / 10,
                                rev_5=(index % 3) / 10, forward_return=index / 100, outcome_status="OBSERVED"))
    return pd.DataFrame(records)

def test_ridge_ties_use_ticker_order_without_forward_returns(ridge_training_panel):
    group = ridge_training_panel.loc[ridge_training_panel.session == "2024-04-02"].iloc[::-1]
    model = dict(session="2024-04-02", features=["mom_12_1", "rev_5"], feature_mean=[0, 0], feature_scale=[1, 1],
                 coefficients=[0, 0], label_mean=0)
    ranked = score_causal_ridge(group, model).sort_values(["ridge_score", "ticker"], ascending=[False, True])
    assert ranked["ticker"].iloc[0] == "T0"


@pytest.fixture
def scoring_inputs():
    group = pd.DataFrame(dict(ticker=["FIRST", "SECOND", "THIRD"], session="2026-09-14",
                              feature_ready=True, mom_12_1=[1., 3., 5.], rev_5=[2., 6., 10.]))
    model = dict(session="2026-09-14", features=["mom_12_1", "rev_5"], feature_mean=[1., 2.],
                 feature_scale=[2., 4.], coefficients=[3., -2.], label_mean=.5)
    return group, model


def test_frozen_scorer_preserves_model_inputs_and_ignores_outcome_columns(scoring_inputs):
    from copy import deepcopy
    group, model = scoring_inputs
    original_group, original_model = group.copy(deep=True), deepcopy(model)
    result = score_causal_ridge(group, model)
    np.testing.assert_array_equal(result.ridge_score, [.5, 1.5, 2.5])
    pd.testing.assert_frame_equal(group, original_group)
    assert model == original_model
    changed = group.assign(forward_return=[np.nan, 999., -999.], outcome_status="UNKNOWN")
    np.testing.assert_array_equal(score_causal_ridge(changed, model).ridge_score, result.ridge_score)


@pytest.mark.parametrize("defect", ["empty", "session", "not_ready", "nan", "infinite"])
def test_frozen_scorer_rejects_invalid_decision_inputs(scoring_inputs, defect):
    group, model = scoring_inputs
    if defect == "empty":
        group = group.iloc[:0]
    elif defect == "session":
        group.loc[0, "session"] = "2026-09-15"
    elif defect == "not_ready":
        group.loc[0, "feature_ready"] = False
    else:
        group.loc[0, "rev_5"] = np.nan if defect == "nan" else np.inf
    with pytest.raises(ValueError, match="feature-ready universe|invalid ridge features"):
        score_causal_ridge(group, model)


def test_frozen_scorer_rejects_nonfinite_model_output(scoring_inputs):
    group, model = scoring_inputs
    model["coefficients"] = [np.nan, -2.]
    with pytest.raises(ValueError, match="invalid scores"):
        score_causal_ridge(group, model)


def test_generic_alpha_cli_kept_but_closed_diagnostic_option_retired(monkeypatch, capsys):
    import sys
    from scripts import run_alpha_research
    monkeypatch.setattr(sys, "argv", ["run_alpha_research", "--help"])
    with pytest.raises(SystemExit) as help_exit:
        run_alpha_research.main()
    assert help_exit.value.code == 0
    help_text = capsys.readouterr().out
    assert "--features" in help_text and "--model" in help_text
    assert "--price-diagnostic-config" not in help_text
    monkeypatch.setattr(sys, "argv", ["run_alpha_research", "--price-diagnostic-config", "retired.json"])
    with pytest.raises(SystemExit) as retired_exit:
        run_alpha_research.main()
    assert retired_exit.value.code == 2
    assert "unrecognized arguments" in capsys.readouterr().err
