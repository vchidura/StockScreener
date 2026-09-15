import sys
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pandas as pd
import pytest


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.outcomes import default_directional_policy
from equity.qualification import qualify_option_conditioning, qualify_outcomes


UTC = timezone.utc


def observations(periods=60, names=("AAA", "BBB"), alpha=0.01):
    rows = []
    start = datetime(2026, 1, 2, 14, 30, tzinfo=UTC)
    for period in range(periods):
        for position, ticker in enumerate(names):
            variation = ((period % 5) - 2) * 0.0005 + position * 0.0001
            rows.append({
                "ticker": ticker,
                "source_name": "breakout_expansion",
                "source_version": "1.0",
                "interval": None,
                "direction": 1,
                "horizon_key": "30m",
                "horizon_bars": 1,
                "policy_key": "breakout_expansion:1.0:30m:SIGNED",
                "signal_time": start + timedelta(days=period),
                "net_return": alpha + variation,
                "net_alpha": alpha + variation,
                "mae_pct": -0.02 - position * 0.001,
                "mfe_pct": 0.03 + position * 0.001,
                "first_hit": "TARGET" if period % 2 == 0 else "STOP",
                "stop_hit": period % 2 == 1,
                "target_hit": period % 2 == 0,
            })
    return pd.DataFrame(rows)


def test_qualification_aggregates_same_time_names_and_promotes_robust_group():
    result = qualify_outcomes(
        observations(),
        effective_from=datetime(2026, 8, 30, tzinfo=UTC),
        minimum_events=100,
        minimum_independent_periods=40,
    )

    assert len(result) == 1
    row = result[0]
    assert row.sample_size == 120
    assert row.independent_periods == 60
    assert row.qualification_state == "ROBUST_PASS"
    assert row.alpha_fdr_q is not None and row.alpha_fdr_q <= 0.05
    assert row.calibrated_probability is not None
    assert row.brier_score is not None
    assert len(row.report_identity) == 64
    metrics = __import__("json").loads(row.metrics_json)
    assert metrics["research_scope"] == "EQUITY_SIGNAL"
    assert metrics["qualification_metrics_version"] == "equity_qualification_metrics_v3"
    assert len(metrics["cohort_sha256"]) == 64
    assert metrics["mean_net_return"] > 0
    assert metrics["hit_rate"] == 1.0
    assert metrics["mean_mae_pct"] == pytest.approx(-0.0205)
    assert metrics["mean_mfe_pct"] == pytest.approx(0.0305)
    assert metrics["stop_first_rate"] == 0.5
    assert metrics["target_first_rate"] == 0.5
    assert metrics["stop_hit_rate"] == 0.5
    assert metrics["target_hit_rate"] == 0.5


def test_qualification_metrics_survive_without_the_underlying_rows():
    """The verdict is retained after outcomes are purged, so it must stand alone."""
    result = qualify_outcomes(
        observations(),
        effective_from=datetime(2026, 8, 30, tzinfo=UTC),
        minimum_events=100,
        minimum_independent_periods=40,
    )
    metrics = __import__("json").loads(result[0].metrics_json)

    assert metrics["distinct_tickers"] == 2
    assert metrics["top5_concentration"] == pytest.approx(1.0)
    assert metrics["first_signal_time"].startswith("2026-01-02T14:30")
    assert metrics["last_signal_time"].startswith("2026-03-02T14:30")
    # Every period wins here, so Wilson keeps the upper bound at 1 and pulls the
    # lower bound below it rather than collapsing to a zero-width Wald interval.
    assert metrics["hit_rate_ci_high"] == pytest.approx(1.0)
    assert 0.9 < metrics["hit_rate_ci_low"] < 1.0
    # No sector benchmark in this cohort, so the sector t-stat is absent, not zero.
    assert metrics["sector_alpha_t_stat"] is None


def test_qualification_keeps_underpowered_group_unranked():
    result = qualify_outcomes(
        observations(periods=10, names=("AAA",), alpha=0.01),
        effective_from=datetime(2026, 8, 30, tzinfo=UTC),
        minimum_events=100,
        minimum_independent_periods=40,
    )

    assert result[0].qualification_state == "UNRANKED"
    assert result[0].independent_periods == 10


def test_daily_qualification_spaces_sparse_signals_by_exchange_sessions():
    frame = observations(periods=2, names=tuple(f"T{index}" for index in range(50)))
    frame["interval"] = "1d"
    frame["horizon_key"] = "5d"
    frame["horizon_bars"] = 5
    frame["signal_time"] = frame["signal_time"].map(
        lambda value: datetime(2026, 1, 2, 21, tzinfo=UTC)
        if value.date() == datetime(2026, 1, 2).date()
        else datetime(2026, 1, 9, 21, tzinfo=UTC)
    )

    result = qualify_outcomes(
        frame,
        effective_from=datetime(2026, 8, 30, tzinfo=UTC),
        minimum_events=100,
        minimum_independent_periods=2,
    )

    assert result[0].independent_periods == 2
    assert result[0].qualification_state == "ROBUST_PASS"


def test_intraday_qualification_spaces_sparse_signals_by_exchange_bars():
    frame = observations(periods=3, names=tuple(f"T{index}" for index in range(50)))
    frame["interval"] = "30m"
    frame["horizon_key"] = "120m"
    frame["horizon_bars"] = 4
    source_times = sorted(frame["signal_time"].unique())
    target_times = (
        datetime(2026, 1, 2, 15, 0, tzinfo=UTC),
        datetime(2026, 1, 2, 16, 0, tzinfo=UTC),
        datetime(2026, 1, 2, 17, 0, tzinfo=UTC),
    )
    mapping = dict(zip(source_times, target_times))
    frame["signal_time"] = frame["signal_time"].map(mapping)

    result = qualify_outcomes(
        frame,
        effective_from=datetime(2026, 8, 30, tzinfo=UTC),
        minimum_events=100,
        minimum_independent_periods=2,
    )

    assert result[0].independent_periods == 2
    assert result[0].qualification_state == "ROBUST_PASS"


def timed_observations(periods=60, names=("AAA", "BBB")):
    frame = observations(periods=periods, names=names)
    frame["signal_market_time"] = frame["signal_time"]
    frame["signal_time"] += pd.Timedelta(seconds=2)
    frame["entry_time"] = frame["signal_time"] + pd.Timedelta(minutes=30)
    frame["exit_time"] = frame["entry_time"] + pd.Timedelta(minutes=30)
    frame["outcome_available_at"] = frame["exit_time"] + pd.Timedelta(minutes=15)
    return frame


def test_intraday_qualification_preserves_delayed_decision_time():
    frame = timed_observations(periods=1)
    frame["interval"] = "30m"
    frame["signal_market_time"] = pd.Timestamp("2026-09-01 20:00Z")
    frame["signal_time"] = pd.Timestamp("2026-09-02 08:05:00.888689Z")
    frame["entry_time"] = pd.Timestamp("2026-09-02 13:30Z")
    frame["exit_time"] = pd.Timestamp("2026-09-02 14:30Z")
    frame["outcome_available_at"] = pd.Timestamp("2026-09-02 14:45Z")

    result = qualify_outcomes(frame, effective_from=datetime(2026, 9, 12, tzinfo=UTC))[0]
    metrics = json.loads(result.metrics_json)

    assert result.independent_periods == 1
    assert metrics["first_signal_time"] == "2026-09-02T08:05:00.888689+00:00"
    assert metrics["first_market_time"] == "2026-09-01T20:00:00+00:00"
    assert metrics["qualification_metrics_version"] == "equity_qualification_metrics_v5"
    assert metrics["probability_target"] == "EQUAL_WEIGHT_SIGNAL_COHORT_NET_WIN"


def test_qualification_does_not_count_overlapping_delayed_entries_as_independent():
    frame = timed_observations(periods=3, names=("AAA",))
    frame["interval"] = "30m"
    frame["horizon_bars"] = 2
    frame["signal_market_time"] = pd.to_datetime([
        "2026-09-01 14:00Z", "2026-09-01 15:00Z", "2026-09-01 16:00Z",
    ])
    frame["signal_time"] = frame["signal_market_time"] + pd.Timedelta(seconds=2)
    frame["entry_time"] = pd.to_datetime([
        "2026-09-01 15:30Z", "2026-09-01 15:30Z", "2026-09-01 16:30Z",
    ])
    frame["exit_time"] = frame["entry_time"] + pd.Timedelta(hours=1)
    frame["outcome_available_at"] = frame["exit_time"] + pd.Timedelta(minutes=15)

    result = qualify_outcomes(frame, effective_from=datetime(2026, 9, 12, tzinfo=UTC))[0]

    assert result.independent_periods == 2


def test_qualification_calibration_waits_for_the_entire_signal_cohort():
    frame = timed_observations(periods=45)
    slow = frame["ticker"].eq("BBB")
    frame.loc[slow, "signal_time"] += pd.Timedelta(seconds=1)
    frame.loc[slow, "outcome_available_at"] += pd.Timedelta(days=10)
    arguments = {"effective_from": datetime(2026, 9, 12, tzinfo=UTC)}

    result = qualify_outcomes(frame, **arguments)[0]
    repeated = qualify_outcomes(frame.sample(frac=1, random_state=7), **arguments)[0]
    metrics = json.loads(result.metrics_json)

    assert result.independent_periods == 45
    assert metrics["calibration_oos_periods"] == 0
    assert result.calibrated_probability is None
    assert result.report_identity == repeated.report_identity


def test_timed_qualification_uses_finite_sample_p_values():
    frame = timed_observations(periods=40, names=("AAA", "BBB", "CCC"))
    variation = pd.Series([-1.0, 1.0] * 20)
    returns = 0.01 * (variation + 2.01 * variation.std(ddof=1) / (40 ** 0.5))
    frame["net_return"] = returns.repeat(3).to_numpy()
    frame["net_alpha"] = frame["net_return"]

    result = qualify_outcomes(frame, effective_from=datetime(2026, 9, 12, tzinfo=UTC))[0]
    metrics = json.loads(result.metrics_json)

    assert result.independent_periods == 40
    assert result.alpha_t_stat == pytest.approx(2.01)
    assert result.alpha_fdr_q == pytest.approx(0.05138461735384954)
    assert result.qualification_state == "MONITOR_ONLY"
    assert metrics["net_return_p_value"] == pytest.approx(result.alpha_fdr_q)
    assert metrics["inference_method"] == "STUDENT_T_TWO_SIDED"
    assert metrics["inference_degrees_of_freedom"] == 39


def test_qualification_rejects_a_cohort_known_after_its_first_entry():
    frame = timed_observations(periods=1)
    frame.loc[1, "signal_time"] = frame.loc[0, "exit_time"]
    frame.loc[1, "entry_time"] = frame.loc[0, "exit_time"] + pd.Timedelta(minutes=30)
    frame.loc[1, "exit_time"] = frame.loc[1, "entry_time"] + pd.Timedelta(minutes=30)
    frame.loc[1, "outcome_available_at"] = frame.loc[1, "exit_time"]

    with pytest.raises(ValueError, match="cohort must be fully observed"):
        qualify_outcomes(frame, effective_from=datetime(2026, 9, 12, tzinfo=UTC))


@pytest.mark.parametrize("column", [
    "signal_market_time", "entry_time", "exit_time", "outcome_available_at",
])
def test_qualification_requires_complete_explicit_timing(column):
    frame = timed_observations().drop(columns=column)
    with pytest.raises(ValueError, match="timing columns are missing"):
        qualify_outcomes(frame, effective_from=datetime(2026, 9, 12, tzinfo=UTC))


@pytest.mark.parametrize("column, reference", [
    ("entry_time", "signal_time"), ("exit_time", "entry_time"),
    ("outcome_available_at", "signal_time"), ("signal_time", "entry_time"),
])
def test_qualification_rejects_invalid_clock_order(column, reference):
    frame = timed_observations()
    frame[column] = frame[reference]
    with pytest.raises(ValueError, match="market <= decision < entry < exit <= availability"):
        qualify_outcomes(frame, effective_from=datetime(2026, 9, 12, tzinfo=UTC))


def test_persisted_qualification_requires_timing_as_well_as_policy_identity():
    frame = observations()
    frame["outcome_id"] = UUID(int=1)
    frame["outcome_policy_id"] = UUID(int=2)
    with pytest.raises(ValueError, match="require explicit timing"):
        qualify_outcomes(frame, effective_from=datetime(2026, 9, 12, tzinfo=UTC))


def test_unbracketed_qualification_does_not_claim_zero_hit_rates():
    frame = observations()
    frame["has_bracket"] = False

    result = qualify_outcomes(
        frame,
        effective_from=datetime(2026, 8, 30, tzinfo=UTC),
    )
    metrics = __import__("json").loads(result[0].metrics_json)

    assert metrics["stop_hit_rate"] is None
    assert metrics["target_hit_rate"] is None
    assert metrics["stop_first_rate"] is None
    assert metrics["target_first_rate"] is None


def test_qualification_publication_identity_is_shared_and_deterministic():
    bullish = observations()
    bearish = bullish.copy()
    bearish["direction"] = -1
    combined = pd.concat((bullish, bearish), ignore_index=True)
    effective_from = datetime(2026, 8, 30, tzinfo=UTC)
    metadata = {"detector_sha256": "a" * 64}

    first = qualify_outcomes(
        combined,
        effective_from=effective_from,
        publication_metadata=metadata,
    )
    repeated = qualify_outcomes(
        combined.sample(frac=1, random_state=7),
        effective_from=effective_from,
        publication_metadata=metadata,
    )

    assert len(first) == 2
    assert {row.report_identity for row in first} == {first[0].report_identity}
    assert [row.report_identity for row in repeated] == [
        row.report_identity for row in first
    ]
    assert [row.qualification_revision_id for row in repeated] == [
        row.qualification_revision_id for row in first
    ]
    metrics = __import__("json").loads(first[0].metrics_json)
    assert metrics["publication_metadata"] == metadata


@pytest.mark.parametrize("missing_alpha", [False, True])
def test_qualification_rejects_mixed_cost_policy_revisions(missing_alpha):
    arguments = {
        "source_name": "breakout_expansion",
        "source_version": "1.0",
        "interval": "30m",
        "horizons": {"30m": 1},
        "effective_from": datetime(2026, 1, 1, tzinfo=UTC),
    }
    cheap = default_directional_policy(**arguments, round_trip_cost_bps=4)
    expensive = default_directional_policy(**arguments, round_trip_cost_bps=40)
    assert cheap.policy_key == expensive.policy_key
    assert cheap.outcome_policy_id != expensive.outcome_policy_id
    first = observations()
    first["policy_key"] = cheap.policy_key
    first["outcome_policy_id"] = cheap.outcome_policy_id
    second = first.copy()
    second["outcome_policy_id"] = expensive.outcome_policy_id
    second["net_return"] -= 0.0036
    second["net_alpha"] = float("nan") if missing_alpha else second["net_alpha"] - 0.0036

    with pytest.raises(ValueError, match="one outcome_policy_id per policy_key"):
        qualify_outcomes(
            pd.concat((first, second), ignore_index=True),
            effective_from=datetime(2026, 9, 12, tzinfo=UTC),
        )


def test_qualification_retains_exact_policy_and_deterministic_identity():
    frame = observations()
    policy_id = UUID(int=1)
    frame["outcome_policy_id"] = policy_id
    arguments = {"effective_from": datetime(2026, 9, 12, tzinfo=UTC)}

    first = qualify_outcomes(frame, **arguments)[0]
    shuffled = frame.sample(frac=1, random_state=7)
    shuffled["outcome_policy_id"] = str(policy_id)
    repeated = qualify_outcomes(shuffled, **arguments)[0]

    assert first.sample_size == 120
    assert first.report_identity == repeated.report_identity
    assert first.qualification_revision_id == repeated.qualification_revision_id
    metrics = json.loads(first.metrics_json)
    assert metrics["outcome_policy_id"] == str(policy_id)
    assert metrics["qualification_metrics_version"] == "equity_qualification_metrics_v4"
    assert frame["outcome_policy_id"].iloc[0] == policy_id

    changed = frame.copy()
    changed["outcome_policy_id"] = UUID(int=2)
    other = qualify_outcomes(changed, **arguments)[0]
    assert first.report_identity != other.report_identity
    assert metrics["cohort_sha256"] != json.loads(other.metrics_json)["cohort_sha256"]


@pytest.mark.parametrize("invalid_id", [None, "", "not-a-uuid"])
def test_qualification_rejects_unknown_policy_identity(invalid_id):
    frame = observations()
    frame["outcome_policy_id"] = str(UUID(int=1))
    frame.loc[0, "outcome_policy_id"] = invalid_id

    with pytest.raises(ValueError, match="outcome_policy_id"):
        qualify_outcomes(frame, effective_from=datetime(2026, 9, 12, tzinfo=UTC))


@pytest.mark.parametrize("column", ["outcome_id", "subject_evidence_id"])
def test_persisted_qualification_requires_exact_policy_identity(column):
    frame = observations()
    frame[column] = UUID(int=1)

    with pytest.raises(ValueError, match="persisted qualification observations require"):
        qualify_outcomes(frame, effective_from=datetime(2026, 9, 12, tzinfo=UTC))


@pytest.mark.parametrize("policy_key", [None, "", " "])
def test_qualification_rejects_unknown_policy_key(policy_key):
    frame = observations()
    frame["outcome_policy_id"] = UUID(int=1)
    frame.loc[0, "policy_key"] = policy_key

    with pytest.raises(ValueError, match="policy_key.*must be known"):
        qualify_outcomes(frame, effective_from=datetime(2026, 9, 12, tzinfo=UTC))


def test_qualification_allows_distinct_policy_keys_without_pooling():
    first = observations()
    first["outcome_policy_id"] = UUID(int=1)
    second = first.copy()
    second["policy_key"] = "breakout_expansion:1.0:30m:RECOMMENDATION_PLAN"
    second["outcome_policy_id"] = UUID(int=2)

    result = qualify_outcomes(
        pd.concat((first, second), ignore_index=True),
        effective_from=datetime(2026, 9, 12, tzinfo=UTC),
    )

    assert len(result) == 2
    assert {row.sample_size for row in result} == {120}
    assert {json.loads(row.metrics_json)["outcome_policy_id"] for row in result} == {
        str(UUID(int=1)), str(UUID(int=2)),
    }


def test_qualification_rejects_unknown_research_scope():
    with pytest.raises(ValueError, match="research_scope"):
        qualify_outcomes(
            observations(),
            effective_from=datetime(2026, 8, 30, tzinfo=UTC),
            research_scope="UNKNOWN",
        )


def test_sector_primary_can_pass_when_stock_profits_but_trails_spy():
    frame = observations(alpha=0.01)
    variation = frame["net_alpha"] - frame["net_alpha"].mean()
    frame["net_alpha"] = -0.005 + variation
    frame["sector_net_alpha"] = 0.008 + variation
    frame["primary_benchmark"] = "SECTOR"

    result = qualify_outcomes(
        frame,
        effective_from=datetime(2026, 8, 30, tzinfo=UTC),
    )

    assert result[0].qualification_state == "ROBUST_PASS"
    metrics = __import__("json").loads(result[0].metrics_json)
    assert metrics["primary_benchmark"] == "SECTOR"
    assert metrics["mean_net_return"] > 0
    assert metrics["mean_market_net_alpha"] < 0
    assert metrics["mean_sector_net_alpha"] > 0
    assert metrics["net_return_t_stat"] > 2


def test_positive_alpha_cannot_pass_with_negative_absolute_return():
    frame = observations(alpha=0.01)
    variation = frame["net_return"] - frame["net_return"].mean()
    frame["net_return"] = -0.01 + variation
    frame["net_alpha"] = 0.01 + variation

    result = qualify_outcomes(
        frame,
        effective_from=datetime(2026, 8, 30, tzinfo=UTC),
    )

    assert result[0].qualification_state == "UNRANKED"
    metrics = __import__("json").loads(result[0].metrics_json)
    assert metrics["mean_net_return"] < 0
    assert metrics["mean_market_net_alpha"] > 0
    assert metrics["raw_pass"] is False


def option_observations(periods=60, names=("AAA", "BBB"), lift=0.01):
    rows = []
    start = datetime(2026, 1, 2, 14, 30, tzinfo=UTC)
    for period in range(periods):
        for position, ticker in enumerate(names):
            variation = ((period % 7) - 3) * 0.0004 + position * 0.0001
            rows.append({
                "ticker": ticker,
                "source_name": "GAP_BREAKAWAY_CONFIRMATION",
                "source_version": "gap_breakaway_confirmation_v2",
                "interval": None,
                "direction": 1,
                "horizon_key": "30MIN",
                "horizon_bars": 1,
                "policy_key": "SPREAD_RANGE_LOCATOR:phase2_v1:30MIN",
                "signal_time": start + timedelta(days=period),
                "conditioned_return": 0.005 + lift + variation,
                "control_return": 0.005,
            })
    return pd.DataFrame(rows)


def test_option_conditioning_qualifies_paired_incremental_return():
    result = qualify_option_conditioning(
        option_observations(),
        effective_from=datetime(2026, 8, 30, tzinfo=UTC),
        publication_metadata={"matching_policy": "same_strategy_dte_regime_v1"},
    )

    assert len(result) == 1
    row = result[0]
    assert row.qualification_state == "ROBUST_PASS"
    assert row.sample_size == 120
    assert row.independent_periods == 60
    assert row.mean_net_alpha == pytest.approx(0.01, abs=0.001)
    assert row.alpha_t_stat is not None and row.alpha_t_stat > 2
    assert len(row.report_identity) == 64
    metrics = __import__("json").loads(row.metrics_json)
    assert metrics["research_scope"] == "OPTION_CONDITIONING"
    assert metrics["option_conditioning"]["incremental_mean_return"] > 0
    assert metrics["publication_metadata"]["matching_policy"] == (
        "same_strategy_dte_regime_v1"
    )


def test_option_conditioning_keeps_underpowered_lift_unranked():
    result = qualify_option_conditioning(
        option_observations(periods=10, names=("AAA",)),
        effective_from=datetime(2026, 8, 30, tzinfo=UTC),
    )

    assert result[0].qualification_state == "UNRANKED"
    assert result[0].independent_periods == 10