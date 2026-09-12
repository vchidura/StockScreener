from datetime import date, datetime, timedelta, timezone

import pytest

from options.analytics.iv_context import summarize_iv_context
from scripts import materialize_option_iv_context


def test_iv_context_reports_range_and_empirical_percentile():
    history = tuple(
        (date(2026, 1, day), 0.10 + day / 100)
        for day in range(1, 11)
    )

    result = summarize_iv_context(
        0.155,
        history,
        lookback_sessions=10,
        minimum_sample_sessions=8,
        minimum_coverage_fraction=0.8,
    )

    assert result.null_reason_codes == ()
    assert result.range_position_rank == pytest.approx(0.5)
    assert result.empirical_percentile == pytest.approx(0.5)


def test_iv_context_fails_closed_on_thin_history():
    result = summarize_iv_context(
        0.20,
        ((date(2026, 1, 1), 0.19),),
        lookback_sessions=10,
        minimum_sample_sessions=8,
        minimum_coverage_fraction=0.8,
    )

    assert "INSUFFICIENT_IV_SAMPLE_SESSIONS" in result.null_reason_codes
    assert "INSUFFICIENT_IV_COVERAGE" in result.null_reason_codes
    assert result.empirical_percentile is None


def test_iv_context_materializer_defaults_to_dry_run():
    args = materialize_option_iv_context.parser().parse_args([])

    assert args.apply is False
    assert args.output == materialize_option_iv_context.DEFAULT_OUTPUT


def test_validation_overrides_cannot_persist():
    args = materialize_option_iv_context.parser().parse_args([
        "--apply",
        "--validation-latest-complete",
    ])

    with pytest.raises(ValueError, match="cannot persist"):
        materialize_option_iv_context._validate_args(args)

    sensitivity = materialize_option_iv_context.parser().parse_args([
        "--apply",
        "--validation-latest-complete",
        "--validation-maximum-log-moneyness",
        "0.06",
    ])
    with pytest.raises(ValueError, match="cannot persist"):
        materialize_option_iv_context._validate_args(sensitivity)


def test_current_expiration_uses_one_matrix_relative_maturity():
    matrix_time = datetime(2026, 9, 11, 19, 45, tzinfo=timezone.utc)
    cutoff = matrix_time + timedelta(days=21)
    rows = (
        {
            "expiration_date": cutoff.date(),
            "expiration_cutoff": cutoff,
            "contract_type": "CALL",
            "strike": 100,
            "spot": 101,
            "local_iv": 0.20,
        },
        {
            "expiration_date": cutoff.date(),
            "expiration_cutoff": cutoff,
            "contract_type": "PUT",
            "strike": 100,
            "spot": 101,
            "local_iv": 0.22,
        },
    )

    observations = materialize_option_iv_context._current_observations(
        rows, matrix_time
    )

    assert len({row.maturity_years for row in observations}) == 1
    assert observations[0].maturity_years == pytest.approx(21 / 365)


def test_iv_context_coverage_uses_expected_exchange_sessions():
    expected = tuple(date(2026, 1, day) for day in range(1, 11))
    history = tuple((session, 0.20) for session in expected[-8:]) + (
        (date(2025, 12, 31), 0.20),
    )

    result = summarize_iv_context(
        0.21,
        history,
        lookback_sessions=10,
        minimum_sample_sessions=8,
        minimum_coverage_fraction=0.8,
        expected_sessions=expected,
    )

    assert result.sample_count == 8
    assert result.coverage_fraction == pytest.approx(0.8)
    assert result.lookback_start_date == expected[0]
    assert result.lookback_end_date == expected[-1]


def test_materializer_builds_strict_trailing_exchange_window():
    sessions = materialize_option_iv_context.OptionExchangeCalendar().trailing_sessions_before(
        date(2026, 9, 11),
        3,
    )

    assert sessions == (
        date(2026, 9, 8),
        date(2026, 9, 9),
        date(2026, 9, 10),
    )