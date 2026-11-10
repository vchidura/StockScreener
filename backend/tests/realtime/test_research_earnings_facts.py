"""G92 (Nov 10): stored Finnhub earnings reach the swing rules' future_earnings gate; a stock without coverage reads
N/A and is still evaluated."""
from datetime import date, datetime, timezone

import exchange_calendars
import pytest

from realtime.research_rule_input import RESEARCH_EARNINGS_POLICY, RESEARCH_MACRO_POLICY, ResearchMember, earnings_facts
from research.equity_models.catalog import research_policy
from research.equity_models.data import Unavailable
from research.equity_models.detectors import Checks, NoMatch, common

UTC = timezone.utc
CALENDAR = exchange_calendars.get_calendar("XNYS")
SESSION = date(2026, 12, 10)
PRIOR = datetime(2026, 12, 9, 21, 0, tzinfo=UTC)
VALID = datetime(2026, 12, 10, 22, 0, tzinfo=UTC)
STOCK = ResearchMember("s-aapl", 1, "AAPL", "CS", None, True)
OTHER = ResearchMember("s-crwd", 2, "CRWD", "CS", None, True)
ETF = ResearchMember("s-spy", 3, "SPY", "ETF", None, False)


def _facts(records):
    return earnings_facts((STOCK, OTHER, ETF), records, session=SESSION, prior_close=PRIOR, valid_until=VALID,
                          calendar=CALENDAR)


def test_an_earnings_date_marks_its_session_and_the_next():
    facts = _facts({"s-aapl": {"covered_through": date(2027, 1, 24), "event_dates": [date(2026, 12, 11)],
                               "coverage_ids": ["c1"]}})
    (fact,) = facts
    assert fact["kind"] == "events" and fact["security_id"] == "s-aapl"
    assert fact["values"] == {"earnings_covered_through": "2027-01-24",
                              "earnings_sessions": ["2026-12-11", "2026-12-14"]}
    assert fact["input_ids"] == ["c1"]


def test_a_weekend_date_marks_only_the_next_session():
    (fact,) = _facts({"s-aapl": {"covered_through": date(2027, 1, 24), "event_dates": [date(2026, 12, 12)]}})
    assert fact["values"]["earnings_sessions"] == ["2026-12-14"]


def test_no_coverage_and_etfs_get_no_fact():
    assert _facts({}) == []
    assert _facts({"s-spy": {"covered_through": date(2027, 1, 24), "event_dates": []}}) == []


class _Data:
    def session_offset(self, session, count):
        return CALENDAR.session_offset(session, count).date().isoformat()


class _Snapshot:
    def __init__(self, events):
        self.events = events
        self.session = SESSION.isoformat()
        self.data = _Data()

    def require(self, kind, *keys):
        if kind == "eligibility":
            return [True, True, "RAW_CONTINUOUS"]
        if self.events is None:
            raise Unavailable("MISSING_OR_STALE_EVENTS")
        return [self.events[key] for key in keys]


def _gate(events):
    checks = Checks()
    policy = research_policy("SW3_B", RESEARCH_MACRO_POLICY, RESEARCH_EARNINGS_POLICY)
    try:
        common(_Snapshot(events), policy, checks)
    except NoMatch:
        pass
    return next(row for row in checks.values if row["name"] == "future_earnings")


def test_the_swing_gate_uses_stored_earnings_and_reads_na_without_them():
    clear = {"earnings_covered_through": "2027-01-24", "earnings_sessions": ["2027-01-20"]}
    inside = {"earnings_covered_through": "2027-01-24", "earnings_sessions": ["2026-12-11"]}
    assert _gate(clear)["status"] == "PASS"
    assert _gate(inside)["status"] == "FAIL"
    assert _gate(None)["status"] == "NOT_APPLICABLE"


def test_coverage_that_ends_before_the_horizon_reads_na():
    short = {"earnings_covered_through": "2026-12-11", "earnings_sessions": []}
    assert _gate(short)["status"] == "NOT_APPLICABLE"


def test_policy_is_optional():
    assert RESEARCH_EARNINGS_POLICY == "optional"
    with pytest.raises(ValueError):
        research_policy("SW3_B", RESEARCH_MACRO_POLICY, "sometimes")
