"""Nov 12 2026: Treasury publishes no par yield curve on Veterans Day (Nov 11) or Columbus Day, when stocks and listed
options trade. The Options reference state required the curve of the previous XNYS session (Nov 11) and raised
"Treasury curve identity, freshness, or clocks changed" for the Nov 12 session, in every Options window. The session
now values with the previous publication, stepping back over Treasury market holidays only."""
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from options.calendar import OptionExchangeCalendar
from options.model_inputs import bond_market_holiday, treasury_rate_date_for_session
from realtime import advanced_options_reference_producer as producer
from realtime.treasury_refresh import required_treasury_rate_date

UTC = timezone.utc


@pytest.mark.parametrize("day, closed", [
    (date(2026, 11, 11), True),   # Veterans Day, a Wednesday
    (date(2026, 10, 12), True),   # Columbus Day, second Monday of October
    (date(2026, 10, 5), False), (date(2026, 10, 19), False), (date(2026, 11, 10), False), (date(2026, 11, 12), False),
    (date(2028, 11, 10), True),   # Nov 11 2028 is a Saturday: observed Friday
    (date(2029, 11, 12), True),   # Nov 11 2029 is a Sunday: observed Monday
    (date(2029, 11, 11), False),
])
def test_treasury_market_holidays(day, closed):
    assert bond_market_holiday(day) is closed


@pytest.mark.parametrize("session, expected", [
    (date(2026, 11, 12), date(2026, 11, 10)),  # the day after Veterans Day values with Nov 10
    (date(2026, 10, 13), date(2026, 10, 9)),   # the day after Columbus Day values with the Friday before
    (date(2026, 11, 11), date(2026, 11, 10)),  # Veterans Day itself: the previous session, as before
    (date(2026, 11, 10), date(2026, 11, 9)),
    (date(2026, 11, 9), date(2026, 11, 6)),    # Monday: the Friday, as before
])
def test_the_session_values_with_the_previous_publication(session, expected):
    assert treasury_rate_date_for_session(session, OptionExchangeCalendar()) == expected
    assert required_treasury_rate_date(session) == expected


class _Cursor:
    """The reference producer's curve query: the latest stored curve at or before the session (Nov 10 here)."""

    def __init__(self, rate_date):
        self.rate_date = rate_date

    def execute(self, *_args):
        pass

    def fetchall(self):
        observed = datetime.combine(self.rate_date, datetime.min.time(), UTC) + timedelta(hours=22)
        return [dict(observation_id="00000000-0000-0000-0000-%012d" % tenor, rate_date=self.rate_date, tenor_days=tenor,
                     annual_rate=0.04, source="us_treasury_par_yield_v1",
                     source_key=f"{self.rate_date.isoformat()}:{tenor}d", source_observed_at=observed,
                     received_at=observed + timedelta(minutes=5), payload_sha256="0" * 64)
                for tenor in (30, 91, 365)]


def _plan(session):
    known = datetime.combine(session, datetime.min.time(), UTC) + timedelta(hours=14, minutes=35)
    return SimpleNamespace(control_plane=SimpleNamespace(session_date=session), known_at=known,
                           valid_until=known + timedelta(hours=7))


def test_the_window_curve_load_accepts_nov_10_for_nov_12():
    curve, rate_date = producer._load_treasury_curve(_Cursor(date(2026, 11, 10)), _plan(date(2026, 11, 12)), [])
    assert rate_date == date(2026, 11, 10) and len(curve) == 3


def test_the_expected_curve_is_not_reported_as_stale(capsys):
    producer._load_treasury_curve(_Cursor(date(2026, 11, 10)), _plan(date(2026, 11, 12)), [])
    assert "TREASURY_CURVE_STALE" not in capsys.readouterr().err  # the day after a holiday is normal, not stale


@pytest.mark.parametrize("stored, session, behind", [
    (date(2026, 11, 9), date(2026, 11, 12), 1),   # Nov 10's curve missing: Nov 9's is used
    (date(2026, 11, 6), date(2026, 11, 10), 1),   # Nov 9's missing on a Tuesday: the Friday's is used
    (date(2026, 11, 3), date(2026, 11, 12), 5),   # five sessions older is the limit
])
def test_a_missing_curve_uses_the_latest_stored_one_and_reports_it(capsys, stored, session, behind):
    """R52 (user, Nov 12): reference data is used when present; a missing curve never stops every Options window."""
    curve, rate_date = producer._load_treasury_curve(_Cursor(stored), _plan(session), [])
    assert rate_date == stored and len(curve) == 3
    err = capsys.readouterr().err
    assert '"kind": "TREASURY_CURVE_STALE"' in err and f'"sessions_behind": {behind}' in err
    assert '"severity": "DEGRADED"' in err


@pytest.mark.parametrize("stored, session", [
    (date(2026, 11, 2), date(2026, 11, 12)),   # six sessions older than the expected Nov 10
    (date(2026, 10, 30), date(2026, 11, 12)),
    (date(2026, 11, 11), date(2026, 11, 12)),  # newer than the expected publication (none exists for Nov 11)
])
def test_a_curve_too_old_or_newer_than_expected_is_refused(stored, session):
    with pytest.raises(ValueError, match="Treasury curve identity, freshness, or clocks changed"):
        producer._load_treasury_curve(_Cursor(stored), _plan(session), [])
