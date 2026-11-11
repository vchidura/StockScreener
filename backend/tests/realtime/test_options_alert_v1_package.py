"""Options alerts v1 (Nov 6): a detected event gets a single-contract package from its own contract and quote."""
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import UUID, uuid4

from realtime.advanced_options_native_loader import (
    OPTIONS_ALERT_V1_VALUATION_SHA256,
    is_options_alert_v1_package,
    options_alert_v1_event_package,
    session_valuation_policy_sha256,
)

COHORT = UUID("b096f5b7-2371-5522-925d-cf4db3912b9c")
MATRIX = uuid4()
EVENT = UUID("00000000-0000-4000-8000-000000000001")
SESSION = date(2026, 11, 6)


def _row(**overrides):
    row = {
        "event_id": str(EVENT),
        "event_time": datetime(2026, 11, 6, 16, 30, 5, tzinfo=timezone.utc),
        "quote_time": datetime(2026, 11, 6, 16, 30, 4, tzinfo=timezone.utc),
        "quote_bid": Decimal("2.10"),
        "quote_ask": Decimal("2.30"),
        "contract_id": 101,
        "contract_ticker": "O:AAPL261113C00345000",
        "instrument_id": 9001,
        "version_id": 77,
        "strike": Decimal("345"),
        "multiplier": Decimal("100"),
        "expiration": date(2026, 11, 13),
        "call_put": "C",
    }
    row.update(overrides)
    return row


def _build(row, unmarked=()):
    return options_alert_v1_event_package(
        row, cohort_id=COHORT, matrix_id=MATRIX, session_date=SESSION, unmarked_instrument_ids=unmarked,
    )


def test_single_contract_package_from_event_quote():
    package = _build(_row())
    assert package is not None
    assert package.event_id == EVENT and package.cohort_id == COHORT and package.matrix_id == MATRIX
    (leg,) = package.legs
    assert leg.entry_mark == Decimal("2.20")
    assert leg.side.value == "BUY" and leg.ratio == 1 and leg.multiplier == 100
    assert leg.contract_type.value == "CALL" and leg.strike == Decimal("345")
    # Nov 10: valued under the session's policy, the one the window marks with (user's decision, Nov 9).
    assert leg.valuation_policy_sha256 == session_valuation_policy_sha256()
    assert leg.valuation_policy_sha256 != OPTIONS_ALERT_V1_VALUATION_SHA256
    assert is_options_alert_v1_package(package)
    # Debit is negative by the package convention (SELL +, BUY -); capital at risk is the premium paid.
    assert package.entry_net_premium == Decimal("-220.00")
    assert package.capital_at_risk == Decimal("220.00")
    assert package.exit_deadline == datetime(2026, 11, 6, 21, 0, tzinfo=timezone.utc)
    assert package.entry_market_time == datetime(2026, 11, 6, 16, 30, 5, tzinfo=timezone.utc)
    # Same event, same package identity: a re-evaluated window reuses it.
    assert _build(_row()).candidate_id == package.candidate_id


class _QuoteCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, sql, params):
        assert "rt.quote_current" in sql
        self.contract_ids = params[0]

    def fetchall(self):
        return [row for row in self.rows if row["contract_id"] in self.contract_ids]


def _alert_row(mark=None):
    return {
        "candidate_id": "c1",
        "current_mark": mark or {"status": "UNAVAILABLE", "reason": "CURRENT_MARK_UNAVAILABLE"},
        "original_package": {
            "net_premium": "-220.00",
            "capital_at_risk": "220.00",
            "legs": [{"contract_id": 101, "side": "BUY", "ratio": 1, "multiplier": 100}],
        },
    }


def test_read_time_current_mark_from_stored_quotes():
    from realtime.portal_compatibility_repository import RealtimeOptionsAlertsPageRepository

    cutoff = datetime(2026, 11, 6, 17, 0, tzinfo=timezone.utc)
    quote = {"contract_id": 101, "bid": Decimal("2.50"), "ask": Decimal("2.70"),
             "event_at": datetime(2026, 11, 6, 16, 59, 30, tzinfo=timezone.utc),
             "received_at": datetime(2026, 11, 6, 16, 59, 31, tzinfo=timezone.utc)}
    envelope = {"data": {"rows": [_alert_row(), _alert_row({"status": "FRESH", "price_return": "0.5"})]}}
    repository = RealtimeOptionsAlertsPageRepository.__new__(RealtimeOptionsAlertsPageRepository)
    result = repository._advanced_current_marks(_QuoteCursor([quote]), envelope, cutoff=cutoff)
    mark = result["data"]["rows"][0]["current_mark"]
    assert mark["status"] == "FRESH" and mark["basis"] == "STORED_NBBO_MIDPOINT"
    assert Decimal(mark["signed_package_mark"]) == Decimal("-260.00")
    assert Decimal(mark["gross_pnl"]) == Decimal("40.00")
    assert abs(Decimal(mark["price_return"]) - Decimal("0.181818")) < Decimal("0.000001")
    # R33: age is from the quote's market time at the read.
    assert mark["age_seconds"] == 30 and mark["maximum_age_seconds"] == 900
    # A stored mark with no market time is replaced by the read-time mark; a missing quote leaves the row as it was.
    assert result["data"]["rows"][1]["current_mark"]["price_return"] != "0.5"
    missing = repository._advanced_current_marks(_QuoteCursor([]), {"data": {"rows": [_alert_row()]}}, cutoff=cutoff)
    assert missing["data"]["rows"][0]["current_mark"]["status"] == "UNAVAILABLE"


def test_one_alert_per_contract_in_a_window():
    """Nov 6 live: 24 large trades on one NVDA contract each became an alert; v1 shows one alert per contract."""
    from types import SimpleNamespace

    from realtime.advanced_options_projection import _one_alert_per_contract

    def pair(minute, contract_id, evaluation_id, policy=OPTIONS_ALERT_V1_VALUATION_SHA256, legs=1):
        at = datetime(2026, 11, 6, 16, minute, tzinfo=timezone.utc)
        leg = SimpleNamespace(contract_id=contract_id, valuation_policy_sha256=policy)
        evaluation = SimpleNamespace(observed_at=at, package=SimpleNamespace(legs=(leg,) * legs))
        row = {"detector_id": "O1", "evaluation_id": evaluation_id, "hit_count": 1, "repeat_count": 0,
               "last_seen_at": at.isoformat()}
        return evaluation, row

    rows = _one_alert_per_contract([
        pair(42, 101, "e3"), pair(40, 101, "e1"), pair(41, 101, "e2"),  # one contract, three trades, out of order
        pair(41, 202, "e4"),  # another contract
        pair(43, 303, "e5", policy="stored"), pair(44, 303, "e6", policy="stored"),  # stored packages: unchanged
        pair(45, 404, "e7", legs=2), pair(46, 404, "e8", legs=2),  # multi-leg: unchanged
    ])
    by_id = {row["evaluation_id"]: row for row in rows}
    assert sorted(by_id) == ["e1", "e4", "e5", "e6", "e7", "e8"]
    assert by_id["e1"]["hit_count"] == 3 and by_id["e1"]["repeat_count"] == 2
    assert by_id["e1"]["last_seen_at"] == "2026-11-06T16:42:00+00:00"
    assert by_id["e4"]["hit_count"] == 1 and by_id["e5"]["hit_count"] == 1 and by_id["e7"]["hit_count"] == 1


def test_session_policy_packages_still_group_as_v1_alerts():
    from types import SimpleNamespace

    from realtime.advanced_options_projection import _session_alert_contract

    package = _build(_row())
    assert _session_alert_contract(SimpleNamespace(package=package)) == 101
    stored = SimpleNamespace(
        legs=package.legs, candidate_id=uuid4(), cohort_id=COHORT, event_id=EVENT,
    )
    assert _session_alert_contract(SimpleNamespace(package=stored)) is None


def test_package_with_no_mark_is_not_marked_instead_of_failing_the_window():
    from realtime.advanced_options_p8_window import _package_mark
    from test_option_portal_adapter import _policy

    package = _build(_row())
    cutoff = datetime(2026, 11, 6, 17, 0, tzinfo=timezone.utc)
    assert _package_mark(package, (), _policy(), cutoff) is None


def test_put_and_unusable_quotes():
    assert _build(_row(call_put="P")).legs[0].contract_type.value == "PUT"
    assert _build(_row(quote_bid=None)) is None
    assert _build(_row(quote_bid=Decimal("0"))) is None
    assert _build(_row(quote_ask=Decimal("2.00"))) is None  # crossed
    assert _build(_row(), unmarked=(9001,)) is None
    assert _build(_row(instrument_id=None)) is None


def test_current_marks_report_what_they_served():
    """INV-9 (Nov 10 review R7): a failed quote read serves the release's marks, rolls back its savepoint and says so."""
    from realtime.portal_compatibility_repository import RealtimeOptionsAlertsPageRepository

    class Recording(_QuoteCursor):
        def __init__(self, rows, broken=False):
            super().__init__(rows)
            self.statements, self.broken = [], broken

        def execute(self, sql, params=None):
            self.statements.append(sql)
            if sql.startswith(("SAVEPOINT", "RELEASE", "ROLLBACK TO")):
                return
            if self.broken:
                raise RuntimeError("relation does not exist")
            super().execute(sql, params)

    cutoff = datetime(2026, 11, 6, 17, 0, tzinfo=timezone.utc)
    repository = RealtimeOptionsAlertsPageRepository.__new__(RealtimeOptionsAlertsPageRepository)
    broken = Recording([], broken=True)
    result = repository._advanced_current_marks_reported(broken, {"data": {"rows": [_alert_row()]}}, cutoff=cutoff)
    assert result["data"]["rows"][0]["current_mark"]["status"] == "UNAVAILABLE"
    assert result["data"]["optional_reads"] == [{"read": "current_marks", "status": "FALLBACK",
                                                 "reason_code": "CURRENT_MARKS_UNAVAILABLE",
                                                 "serving_source": "RELEASE_MARKS"}]
    assert broken.statements[-1] == "ROLLBACK TO SAVEPOINT options_current_marks"
    healthy = Recording([])
    result = repository._advanced_current_marks_reported(healthy, {"data": {"rows": [_alert_row()]}}, cutoff=cutoff)
    assert result["data"]["optional_reads"][0]["status"] == "AVAILABLE"
    assert healthy.statements[-1] == "RELEASE SAVEPOINT options_current_marks"


def test_stored_window_mark_is_refreshed_from_newer_quotes():
    """R33 (Nov 10 live): the session list kept the 18:05 window mark as FRESH, so at 19:10 Day History still showed
    the 18:05 price and "0.0 hours old". A newer stored quote now replaces it, keeping the window's cost; a stored
    mark at least as new as the quotes is kept and aged at the read; past 15 minutes it reads STALE."""
    from realtime.portal_compatibility_repository import RealtimeOptionsAlertsPageRepository

    repository = RealtimeOptionsAlertsPageRepository.__new__(RealtimeOptionsAlertsPageRepository)
    window_mark = {
        "status": "FRESH", "reason": None, "market_time": "2026-11-10T18:04:59.856000+00:00", "age_seconds": 0,
        "signed_package_mark": "-193.50", "package_price": "193.50", "gross_pnl": "-6.00", "price_return": "-0.03",
        "estimated_cost": "1.30", "net_pnl": "-7.30", "net_return": "-0.0366", "net_return_basis": "NET_PNL_OVER_CAPITAL_AT_RISK",
        "source_snapshot_ids": ["26164a30-db79-57a1-a45d-2d81e5a6f19d"],
    }
    cutoff = datetime(2026, 11, 10, 19, 10, tzinfo=timezone.utc)
    newer = {"contract_id": 101, "bid": Decimal("2.40"), "ask": Decimal("2.50"),
             "event_at": datetime(2026, 11, 10, 19, 9, 58, tzinfo=timezone.utc),
             "received_at": datetime(2026, 11, 10, 19, 9, 58, tzinfo=timezone.utc)}
    result = repository._advanced_current_marks(_QuoteCursor([newer]), {"data": {"rows": [_alert_row(dict(window_mark))]}}, cutoff=cutoff)
    mark = result["data"]["rows"][0]["current_mark"]
    assert mark["status"] == "FRESH" and mark["market_time"] == "2026-11-10T19:09:58+00:00" and mark["age_seconds"] == 2
    assert Decimal(mark["signed_package_mark"]) == Decimal("-245.00") and Decimal(mark["gross_pnl"]) == Decimal("25.00")
    assert Decimal(mark["estimated_cost"]) == Decimal("1.30") and Decimal(mark["net_pnl"]) == Decimal("23.70")
    assert mark["net_return_basis"] == "NET_PNL_OVER_CAPITAL_AT_RISK" and "source_snapshot_ids" not in mark
    # The quotes are no newer than the stored mark: it is kept, aged at the read (65 minutes: STALE).
    older = dict(newer, event_at=datetime(2026, 11, 10, 18, 0, tzinfo=timezone.utc),
                 received_at=datetime(2026, 11, 10, 18, 0, tzinfo=timezone.utc))
    kept = repository._advanced_current_marks(_QuoteCursor([older]), {"data": {"rows": [_alert_row(dict(window_mark))]}}, cutoff=cutoff)
    mark = kept["data"]["rows"][0]["current_mark"]
    assert mark["signed_package_mark"] == "-193.50" and mark["age_seconds"] == 3900
    assert mark["status"] == "STALE" and mark["reason"] == "CURRENT_MARK_OLDER_THAN_15_MINUTES"
    # No quote at all: the stored mark is kept and aged too.
    aged = repository._advanced_current_marks(_QuoteCursor([]), {"data": {"rows": [_alert_row(dict(window_mark))]}}, cutoff=cutoff)
    assert aged["data"]["rows"][0]["current_mark"]["status"] == "STALE"


class _FactsCursor:
    """The two trigger-time reads: the alert's flow events, then the stock's 1-minute close at the trigger."""

    def __init__(self, events, spots, broken=False):
        self.events, self.spots, self.broken, self.statements = events, spots, broken, []

    def execute(self, sql, params=None):
        self.statements.append(sql)
        if sql.startswith(("SAVEPOINT", "RELEASE", "ROLLBACK TO")):
            return
        if self.broken:
            raise RuntimeError("canceling statement due to statement timeout")
        if "rt.option_flow_events" in sql:
            wanted = set(params[0])
            self.result = [row for row in self.events if row["event_id"] in wanted]
        else:
            assert "rt.bar_revisions" in sql
            self.result = [row for row in self.spots if (row["ticker"], row["at"]) in set(zip(*params))]

    def fetchall(self):
        return self.result


def _facts_row(evaluation_id):
    return {
        "evaluation_id": evaluation_id, "underlyer": "NVDA", "session_date": "2026-11-10",
        "triggered_at": "2026-11-10T17:57:58.826525+00:00",
        "event_ids": ["322b985f-1114-53ee-a15d-9f39670591dc", "39532f87-a155-5e19-82a1-9d9ed6ec4e83"],
        "original_package": {
            "calendar_dte": None, "expiration_date": "2026-11-11",
            "legs": [{"contract_ticker": "O:NVDA261111C00170000", "contract_type": "CALL", "strike": "170", "side": "BUY"}],
        },
    }


def test_source_facts_fill_stock_volume_oi_and_dte_from_the_trigger():
    """R33 (Nov 10 live): O1 Day History showed Stock at source, % OTM, Volume, OI and DTE as Unavailable. They come
    from the alert's own flow event at the trigger and the stock's last 1-minute close by then."""
    from realtime.portal_compatibility_repository import RealtimeOptionsAlertsPageRepository

    trigger = datetime(2026, 11, 10, 17, 57, 58, 826525, tzinfo=timezone.utc)
    events = [
        {"event_id": "39532f87-a155-5e19-82a1-9d9ed6ec4e83", "contract_ticker": "O:NVDA261111C00170000",
         "event_time": datetime(2026, 11, 10, 17, 57, 58, 280000, tzinfo=timezone.utc),
         "cumulative_volume_after": 96208, "prior_open_interest": 1586},
        # A later repeat of the same alert: not the value at the trigger.
        {"event_id": "322b985f-1114-53ee-a15d-9f39670591dc", "contract_ticker": "O:NVDA261111C00170000",
         "event_time": datetime(2026, 11, 10, 17, 59, 52, tzinfo=timezone.utc),
         "cumulative_volume_after": 97298, "prior_open_interest": 1586},
    ]
    spots = [{"ticker": "NVDA", "at": trigger, "close_price": Decimal("167.81000000"),
              "bar_end": datetime(2026, 11, 10, 17, 57, tzinfo=timezone.utc)}]
    repository = RealtimeOptionsAlertsPageRepository.__new__(RealtimeOptionsAlertsPageRepository)
    cutoff = datetime(2026, 11, 10, 19, 10, tzinfo=timezone.utc)
    evaluation_id = str(uuid4())
    cursor = _FactsCursor(events, spots)
    result = repository._advanced_source_facts_reported(cursor, {"data": {"rows": [_facts_row(evaluation_id)]}}, cutoff=cutoff)
    package = result["data"]["rows"][0]["original_package"]
    (leg,) = package["legs"]
    assert package["calendar_dte"] == 1
    assert leg["day_volume"] == 96208 and leg["open_interest"] == 1586
    assert leg["spot"] == "167.81000000" and leg["spot_market_data_time"] == "2026-11-10T17:57:00+00:00"
    assert leg["market_facts_basis"] == "TRIGGER_FLOW_EVENT_AND_1M_CLOSE"
    assert result["data"]["optional_reads"] == [{"read": "source_facts", "status": "AVAILABLE", "reason_code": None,
                                                 "serving_source": "TRIGGER_EVENTS"}]
    # Complete facts are kept: the next read makes no queries.
    again = _FactsCursor([], [])
    repeat = repository._advanced_source_facts_reported(again, {"data": {"rows": [_facts_row(evaluation_id)]}}, cutoff=cutoff)
    assert repeat["data"]["rows"][0]["original_package"]["legs"][0]["day_volume"] == 96208
    assert not [sql for sql in again.statements if "rt." in sql]
    # A failed read serves the rows as they were and says so; DTE needs no read.
    broken = _FactsCursor([], [], broken=True)
    failed = repository._advanced_source_facts_reported(broken, {"data": {"rows": [_facts_row(str(uuid4()))]}}, cutoff=cutoff)
    assert failed["data"]["optional_reads"][0]["status"] == "FALLBACK"
    assert failed["data"]["optional_reads"][0]["reason_code"] == "SOURCE_FACTS_UNAVAILABLE"
    assert "day_volume" not in failed["data"]["rows"][0]["original_package"]["legs"][0]
    assert broken.statements[-1] == "ROLLBACK TO SAVEPOINT options_source_facts"
    # An incomplete lookup (no stock bar) fills what it found and is read again next time.
    partial_id = str(uuid4())
    partial = repository._advanced_source_facts(_FactsCursor(events, []), {"data": {"rows": [_facts_row(partial_id)]}}, cutoff=cutoff)
    leg = partial["data"]["rows"][0]["original_package"]["legs"][0]
    assert leg["day_volume"] == 96208 and "spot" not in leg
    retry = _FactsCursor(events, spots)
    repository._advanced_source_facts(retry, {"data": {"rows": [_facts_row(partial_id)]}}, cutoff=cutoff)
    assert [sql for sql in retry.statements if "rt.bar_revisions" in sql]
