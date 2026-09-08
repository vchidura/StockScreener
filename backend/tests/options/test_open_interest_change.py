from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from options.analytics.open_interest_change import (
    MINIMUM_PRIOR_OPEN_INTEREST,
    OpenInterestChange,
    OpenInterestFlow,
    OpenInterestObservation,
    VolumeConfirmation,
    classify_open_interest_change,
    rank_open_interest_builds,
    summarize_underlying,
)
from options.calendar import OptionExchangeCalendar


def _observation(**overrides) -> OpenInterestObservation:
    base = dict(
        contract_id=1,
        underlying="SPY",
        contract_type="CALL",
        strike=Decimal("700"),
        expiration_date=date(2026, 10, 16),
        settlement_session=date(2026, 9, 3),
        prior_settlement_session=date(2026, 9, 2),
        open_interest=5000,
        prior_open_interest=1000,
        session_volume=None,
        session_volume_is_final=True,
        sessions_spanned=1,
    )
    base.update(overrides)
    return OpenInterestObservation(**base)


def test_rising_open_interest_is_opening():
    result = classify_open_interest_change(_observation())
    assert result.flow is OpenInterestFlow.OPENING
    assert result.change == 4000
    assert result.change_fraction == pytest.approx(4.0)


def test_falling_open_interest_is_unwinding():
    result = classify_open_interest_change(
        _observation(open_interest=400, prior_open_interest=1000)
    )
    assert result.flow is OpenInterestFlow.UNWINDING
    assert result.change == -600


def test_unchanged_open_interest():
    result = classify_open_interest_change(
        _observation(open_interest=1000, prior_open_interest=1000)
    )
    assert result.flow is OpenInterestFlow.UNCHANGED
    assert result.change_fraction == pytest.approx(0.0)


def test_small_prior_open_interest_suppresses_the_percentage():
    """Two contracts becoming eight is a 300% rise and means nothing."""
    result = classify_open_interest_change(
        _observation(open_interest=8, prior_open_interest=2)
    )
    assert result.change == 6
    assert result.change_fraction is None
    assert "PRIOR_OPEN_INTEREST_BELOW_FLOOR" in result.reasons


def test_floor_is_inclusive():
    result = classify_open_interest_change(
        _observation(
            open_interest=MINIMUM_PRIOR_OPEN_INTEREST + 50,
            prior_open_interest=MINIMUM_PRIOR_OPEN_INTEREST,
        )
    )
    assert result.change_fraction is not None
    assert "PRIOR_OPEN_INTEREST_BELOW_FLOOR" not in result.reasons


def test_volume_close_to_the_change_is_predominantly_opening():
    result = classify_open_interest_change(
        _observation(open_interest=5000, prior_open_interest=1000, session_volume=4200)
    )
    assert result.confirmation is VolumeConfirmation.PREDOMINANTLY_OPENING
    assert result.opening_share == pytest.approx(4000 / 4200)


def test_volume_far_above_the_change_is_churn():
    """The false positive the volume-over-open-interest ratio cannot exclude."""
    result = classify_open_interest_change(
        _observation(open_interest=1100, prior_open_interest=1000, session_volume=50000)
    )
    assert result.confirmation is VolumeConfirmation.PREDOMINANTLY_CHURN
    assert result.opening_share == pytest.approx(100 / 50000)


def test_change_exceeding_volume_is_capped_and_flagged():
    result = classify_open_interest_change(
        _observation(open_interest=9000, prior_open_interest=1000, session_volume=500)
    )
    assert result.opening_share == pytest.approx(1.0)
    assert "CHANGE_EXCEEDS_SESSION_VOLUME" in result.reasons


def test_open_interest_moving_without_volume_is_flagged():
    """Exercise, assignment and expiration move open interest with no print."""
    result = classify_open_interest_change(
        _observation(open_interest=200, prior_open_interest=1000, session_volume=0)
    )
    assert result.confirmation is VolumeConfirmation.NO_VOLUME_RECORDED
    assert "OPEN_INTEREST_MOVED_WITHOUT_VOLUME" in result.reasons


def test_missing_volume_reports_unavailable_not_churn():
    result = classify_open_interest_change(_observation(session_volume=None))
    assert result.confirmation is VolumeConfirmation.VOLUME_UNAVAILABLE
    assert result.opening_share is None


def test_multi_session_span_withdraws_volume_confirmation():
    """One session's volume cannot explain a move spanning several sessions."""
    result = classify_open_interest_change(
        _observation(
            settlement_session=date(2026, 9, 3),
            prior_settlement_session=date(2026, 9, 1),
            session_volume=4200,
            sessions_spanned=2,
        )
    )
    assert "MULTI_SESSION_OPEN_INTEREST_CHANGE" in result.reasons
    assert result.confirmation is VolumeConfirmation.VOLUME_UNAVAILABLE
    assert result.opening_share is None


def test_consecutive_span_keeps_confirmation():
    result = classify_open_interest_change(
        _observation(session_volume=4200, sessions_spanned=1)
    )
    assert "MULTI_SESSION_OPEN_INTEREST_CHANGE" not in result.reasons
    assert result.confirmation is VolumeConfirmation.PREDOMINANTLY_OPENING


def test_revised_open_interest_is_exposed_as_data_quality():
    result = classify_open_interest_change(
        _observation(
            open_interest_revision_count=1,
            prior_open_interest_revision_count=2,
        )
    )
    assert "OPEN_INTEREST_REVISED" in result.reasons
    assert "PRIOR_OPEN_INTEREST_REVISED" in result.reasons


def test_sessions_must_be_ordered():
    with pytest.raises(ValueError):
        classify_open_interest_change(
            _observation(
                settlement_session=date(2026, 9, 1),
                prior_settlement_session=date(2026, 9, 3),
            )
        )


def test_sessions_spanned_must_be_positive():
    with pytest.raises(ValueError):
        classify_open_interest_change(_observation(sessions_spanned=0))


def test_ranking_treats_unwinding_as_significant_as_opening():
    changes = tuple(
        classify_open_interest_change(_observation(contract_id=index, **payload))
        for index, payload in enumerate(
            [
                {"open_interest": 1500, "prior_open_interest": 1000},
                {"open_interest": 200, "prior_open_interest": 9000},
                {"open_interest": 1100, "prior_open_interest": 1000},
            ]
        )
    )
    ranked = rank_open_interest_builds(changes)
    assert ranked[0].change == -8800
    assert ranked[1].change == 500


def test_ranking_applies_the_minimum_change_floor():
    changes = tuple(
        classify_open_interest_change(_observation(contract_id=index, **payload))
        for index, payload in enumerate(
            [
                {"open_interest": 1500, "prior_open_interest": 1000},
                {"open_interest": 1010, "prior_open_interest": 1000},
            ]
        )
    )
    ranked = rank_open_interest_builds(changes, minimum_absolute_change=100)
    assert len(ranked) == 1


def test_summary_separates_calls_from_puts():
    changes = (
        classify_open_interest_change(
            _observation(contract_id=1, contract_type="CALL", open_interest=3000)
        ),
        classify_open_interest_change(
            _observation(
                contract_id=2,
                contract_type="PUT",
                open_interest=400,
                prior_open_interest=1000,
            )
        ),
    )
    summary = summarize_underlying("SPY", changes)
    assert summary.call_change == 2000
    assert summary.put_change == -600
    assert summary.opening_contract_count == 1
    assert summary.unwinding_contract_count == 1


def test_summary_is_none_for_an_unrepresented_underlying():
    changes = (classify_open_interest_change(_observation()),)
    assert summarize_underlying("QQQ", changes) is None


def test_calendar_counts_consecutive_sessions_as_one():
    calendar = OptionExchangeCalendar()
    # Friday to Monday is consecutive despite three calendar days.
    assert calendar.sessions_between(date(2026, 9, 4), date(2026, 9, 8)) == 1


def test_calendar_detects_a_missed_session():
    """The defect this replaced: 09-01 to 09-03 is two sessions, not a weekend gap."""
    calendar = OptionExchangeCalendar()
    assert calendar.sessions_between(date(2026, 9, 1), date(2026, 9, 3)) == 2


def test_calendar_rejects_reversed_range():
    calendar = OptionExchangeCalendar()
    with pytest.raises(ValueError):
        calendar.sessions_between(date(2026, 9, 3), date(2026, 9, 1))


def test_provisional_volume_is_flagged_as_an_upper_bound():
    """A running intraday total understates the denominator of the opening share."""
    result = classify_open_interest_change(
        _observation(
            open_interest=5000,
            prior_open_interest=1000,
            session_volume=4200,
            session_volume_is_final=False,
        )
    )
    assert "PROVISIONAL_SESSION_VOLUME" in result.reasons
    assert result.session_volume_is_final is False
    # The share is still computed; the caller is told not to trust it as settled.
    assert result.opening_share == pytest.approx(4000 / 4200)


def test_settled_volume_carries_no_provisional_flag():
    result = classify_open_interest_change(
        _observation(session_volume=4200, session_volume_is_final=True)
    )
    assert "PROVISIONAL_SESSION_VOLUME" not in result.reasons
    assert result.session_volume_is_final is True


def test_provisional_flag_is_absent_when_no_volume_exists():
    result = classify_open_interest_change(
        _observation(session_volume=None, session_volume_is_final=False)
    )
    assert result.confirmation is VolumeConfirmation.VOLUME_UNAVAILABLE
    assert "PROVISIONAL_SESSION_VOLUME" not in result.reasons
