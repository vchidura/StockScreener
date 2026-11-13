"""R44 night 2 (Nov 13): the O3 input producer builds exactly what the O3 lane reads and O3 accepts.

End to end without a database: a put credit vertical from two watched contracts, the window's O1 activity source on a
leg, the canonical 30m trend evidence (real `build_canonical_trend_evidence`), the real decision
(`assess_options_credit_canonical_trend`), structural levels, the retained invocation document, and its decode back
into the lane's invocation, which `assess_o3_credit` accepts."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest

from options.dual_origin import ActivitySource
from realtime.advanced_options_o3_inputs import LegQuote, WatchedContract
from realtime.advanced_options_o3_producer import (
    LegMark,
    ValuationInput,
    assemble_o3_inputs,
    build_o3_parts,
    occ_underlying,
    value_leg,
)

UTC = timezone.utc
NOW = datetime(2026, 9, 18, 15, 20, tzinfo=UTC)
MARKET = NOW - timedelta(minutes=20)  # 15:00 UTC: the O1 activity event
CUTOFF = NOW - timedelta(minutes=15)  # 15:05 UTC: the window cutoff
SESSION = MARKET.date()
EXPIRY = date(2026, 10, 16)  # 28 DTE
SECURITY = uuid4()


def _contract(instrument_id, contract_id, strike, side="P"):
    ticker = f"O:AAPL261016{side}{int(Decimal(strike) * 1000):08d}"
    return WatchedContract(instrument_id=instrument_id, contract_id=contract_id, catalog_version_id=contract_id + 1000,
                           contract_ticker=ticker, underlying="AAPL", call_put=side, strike=Decimal(strike),
                           expiration=EXPIRY, multiplier=100)


SHORT = _contract(11, 101, "97")
LONG = _contract(12, 102, "96")
CONTRACTS = (SHORT, LONG)


def _mark(contract, bid, ask):
    bid, ask = Decimal(bid), Decimal(ask)
    return LegMark(evidence_id=uuid4(), instrument_id=contract.instrument_id, bid=bid, ask=ask, mark=(bid + ask) / 2,
                   market_time=CUTOFF - timedelta(minutes=1), observed_time=CUTOFF - timedelta(seconds=55),
                   payload_sha256="d" * 64)


MARKS = {SHORT.instrument_id: _mark(SHORT, "1.15", "1.25"), LONG.instrument_id: _mark(LONG, "0.85", "0.95")}
QUOTES = {
    row.instrument_id: LegQuote(row.instrument_id, MARKS[row.instrument_id].bid, MARKS[row.instrument_id].ask,
                                MARKS[row.instrument_id].market_time, 600, 1000)
    for row in CONTRACTS
}
VALUATION = {("AAPL", EXPIRY): ValuationInput(Decimal("0.04"), Decimal("0.005"), "bsm-v1")}
SPOTS = {"AAPL": (Decimal("100"), CUTOFF - timedelta(minutes=1))}


def _source(contract=SHORT, **changes):
    values = dict(security_id=SECURITY, underlyer="AAPL", contract_id=contract.contract_id,
                  contract_ticker=contract.contract_ticker, contract_type="PUT" if contract.call_put == "P" else "CALL",
                  expiration_date=EXPIRY, expiration_cutoff=NOW + timedelta(days=28), snapshot_id=uuid4(),
                  snapshot_payload_sha256="a" * 64, batch_id=uuid4(), matrix_id=uuid4(), configuration_sha256="b" * 64,
                  market_policy_sha256="c" * 64, volume_session=SESSION, day_volume=600, open_interest=200,
                  oi_settlement_session=SESSION - timedelta(days=1), oi_observed_at=NOW - timedelta(minutes=3),
                  market_time=MARKET, observed_at=NOW - timedelta(minutes=3), recorded_at=NOW - timedelta(minutes=2),
                  received_at=NOW - timedelta(minutes=1))
    values.update(changes)
    return ActivitySource(**values)


def _trend_rows(*, end, count=202, step=0.05):
    rows = []
    for index in range(count):
        bar_end = end - timedelta(minutes=30 * (count - 1 - index))
        close = 100 + step * index
        rows.append(dict(bar_revision_id=uuid4(), bar_end=bar_end, session_date=bar_end.date(),
                         high_price=close + 0.2, low_price=close - 0.2, close_price=close, volume=1000,
                         created_at=bar_end + timedelta(seconds=30), system_observed_at=bar_end + timedelta(seconds=20)))
    return rows


def _daily_rows(count=22):
    return [dict(bar_revision_id=uuid4(), bar_end=datetime.combine(SESSION - timedelta(days=count - index),
                 datetime.min.time(), UTC) + timedelta(hours=20), session_date=SESSION - timedelta(days=count - index),
                 high_price=101, low_price=99, close_price=100, volume=1_000_000, created_at=NOW - timedelta(days=1),
                 system_observed_at=NOW - timedelta(days=1)) for index in range(count)]


def _stock_factory(step=0.05):
    from options.intraday_participation import build_canonical_trend_evidence

    def stock(underlyer, required_window):
        return build_canonical_trend_evidence(
            security_id=SECURITY, underlyer=underlyer, session_date=SESSION, required_window=required_window,
            trend_bars=_trend_rows(end=required_window, step=step), daily_bars=_daily_rows(), split_clear=True,
            received_at=NOW - timedelta(seconds=30), session_close=NOW + timedelta(hours=4),
        )

    return stock


def _levels(stop="98", target="110"):
    def levels(underlyer, stock):
        return ({"interval": "30m", "market_time": stock.required_window, "computed_at": NOW,
                 "bar_revision_ids": (uuid4(),),
                 "levels": {1: {"stops": [{"price": stop, "source": "Price Action"}],
                                "targets": [{"price": target, "source": "Price Action"}], "atr": "1.5"},
                            -1: {"stops": [{"price": target, "source": "Price Action"}],
                                 "targets": [{"price": stop, "source": "Price Action"}], "atr": "1.5"}}},
                SimpleNamespace(security_id=SECURITY, ticker=underlyer))

    return levels


def _assemble(*, sources=None, stock=None, levels=None):
    return assemble_o3_inputs(
        contracts=CONTRACTS, quotes=QUOTES, marks=MARKS,
        activity_sources=(_source(),) if sources is None else sources, spot_by_underlying=SPOTS,
        valuation_inputs=VALUATION, stock_evidence=stock or _stock_factory(),
        structural_levels=levels or _levels(), cohort_id=uuid4(), matrix_id=uuid4(), session_date=SESSION,
        cutoff=CUTOFF, decision_at=NOW, session_close=NOW + timedelta(hours=4), batch_id=uuid4(),
    )


def test_leg_valuation_recovers_the_volatility_that_priced_the_mark():
    from options.analytics.greeks import price_and_greeks
    from options.domain import ContractType

    years = (EXPIRY - SESSION).days / 365.0
    price = price_and_greeks(ContractType.PUT, spot=100.0, strike=97.0, maturity=years, rate=0.04, dividend=0.005,
                             volatility=0.30)[0]
    valued = value_leg(SHORT, Decimal(str(round(price, 6))), spot=Decimal("100"), at=CUTOFF,
                       valuation=ValuationInput(Decimal("0.04"), Decimal("0.005"), "bsm-v1"))
    assert not isinstance(valued, str)
    assert valued.local_iv == pytest.approx(0.30, abs=1e-4)
    assert valued.local_delta < 0 and valued.local_gamma > 0 and valued.time_to_expiration_years == pytest.approx(years)
    expired = _contract(13, 103, "97")
    assert value_leg(expired, Decimal("1"), spot=Decimal("100"), at=datetime(2026, 10, 17, 15, tzinfo=UTC),
                     valuation=VALUATION[("AAPL", EXPIRY)]) == "O3_LEG_EXPIRED_AT_MARK"


def test_parts_are_a_bounded_put_credit_vertical_with_nbbo_marks():
    from realtime.advanced_options_o3_inputs import select_o3_candidates

    selection = select_o3_candidates(CONTRACTS, QUOTES, session_date=SESSION, cutoff=CUTOFF,
                                     spot_by_underlying={"AAPL": Decimal("100")})
    (vertical,) = selection.candidates
    valuations = {row.instrument_id: value_leg(row, MARKS[row.instrument_id].mark, spot=Decimal("100"),
                                               at=MARKS[row.instrument_id].market_time,
                                               valuation=VALUATION[("AAPL", EXPIRY)]) for row in CONTRACTS}
    parts = build_o3_parts(vertical, cohort_id=uuid4(), matrix_id=uuid4(), cutoff=CUTOFF, decision_at=NOW,
                           spot=Decimal("100"), spot_time=SPOTS["AAPL"][1], marks=MARKS, quotes=QUOTES,
                           valuations=valuations, session_close=NOW + timedelta(hours=4), batch_id=uuid4())
    candidate = parts.candidate
    assert candidate.strategy_name == "SPREAD_RANGE_LOCATOR" and candidate.structure_type.value == "PUT_CREDIT_VERTICAL"
    assert [(leg.side.value, leg.strike) for leg in candidate.legs] == [("SELL", Decimal("97")), ("BUY", Decimal("96"))]
    assert candidate.net_premium == Decimal("30.00")  # (1.20 - 0.90) x 100
    assert candidate.maximum_profit == candidate.net_premium and candidate.maximum_loss == Decimal("70.00")
    assert candidate.breakevens == (Decimal("96.70"),)
    assert {snapshot.mark_source.value for snapshot in parts.snapshots} == {"ADVANCED_NBBO_MIDPOINT"}
    assert [snapshot.snapshot_id for snapshot in parts.snapshots] == [leg.snapshot_id for leg in candidate.legs]
    package = parts.package
    assert package.candidate_id == candidate.candidate_id and package.event_id == parts.event_id
    assert package.entry_net_premium == Decimal("30.00") and package.capital_at_risk == Decimal("70.00")
    assert [(leg.side.value, leg.entry_mark) for leg in package.legs] == [("SELL", Decimal("1.20")), ("BUY", Decimal("0.90"))]


def test_end_to_end_record_decodes_into_an_invocation_o3_accepts():
    from options.credit_detection import assess_o3_credit
    from options.dual_origin import CanonicalTrendSignalDecision
    from realtime.advanced_options_native_adapters import decode_advanced_o3_invocation_document

    build = _assemble()
    assert build.candidates_considered == 1, build.rejections
    (record,) = build.records
    assert isinstance(record.decision, CanonicalTrendSignalDecision) and record.decision.disposition == "CONFIRMED"
    assert record.activity_contract_id == SHORT.contract_id and record.structural_invalidation == Decimal("98")
    document = record.document
    assert document["contract"] == "advanced-options-o3-invocation-v1"
    assert document["decision_model"] == "LATEST_COMPLETED_SIGNAL_DECISION"
    assert document["decision"]["schema_version"] == "dual_origin_signal_decision_v6"
    decoded = decode_advanced_o3_invocation_document(document, package=record.parts.package)
    assert decoded.decision == record.decision
    # The lane's own scope checks (`_load_o3`): mark source, OI / volume present, 7-30 DTE, scheduled cycle.
    assert decoded.scheduled_cycle == CUTOFF
    assert {str(row.mark_source) for row in decoded.snapshots} == {"ADVANCED_NBBO_MIDPOINT"}
    assert all(row.open_interest is not None and row.day_volume is not None for row in decoded.snapshots)
    observation = assess_o3_credit(decoded.candidate, decoded.decision, activity_contract_id=decoded.activity_contract_id,
                                   snapshots=decoded.snapshots, structural_invalidation=decoded.structural_invalidation,
                                   scheduled_cycle=decoded.scheduled_cycle)
    assert observation.structure_type == "PUT_CREDIT_VERTICAL" and observation.net_credit == Decimal("30.00")
    assert not observation.execution_permission and not observation.publication_permission


def test_no_activity_source_on_a_leg_writes_nothing():
    other = _contract(14, 104, "90")
    build = _assemble(sources=(_source(other),))
    assert build.records == () and dict(build.rejections)["O3_DATED_ACTIVITY_SOURCE_UNAVAILABLE"] == 1


def test_a_falling_trend_contradicts_a_bullish_put_credit():
    build = _assemble(stock=_stock_factory(step=-0.05))
    assert build.records == ()
    assert any(reason.startswith("O3_DECISION:") for reason, _ in build.rejections)


def test_a_short_strike_inside_the_invalidation_is_refused_by_o3_and_not_stored():
    build = _assemble(levels=_levels(stop="96.5"))  # bullish stop 96.5 < short strike 97
    assert build.records == ()
    assert any(reason.startswith("O3_ASSESSMENT:") for reason, _ in build.rejections)


def test_missing_levels_or_valuation_inputs_are_reasons_not_errors():
    assert dict(_assemble(levels=lambda underlyer, stock: None).rejections)["O3_STRUCTURAL_LEVELS_UNAVAILABLE"] == 1
    build = assemble_o3_inputs(
        contracts=CONTRACTS, quotes=QUOTES, marks=MARKS, activity_sources=(_source(),), spot_by_underlying=SPOTS,
        valuation_inputs={}, stock_evidence=_stock_factory(), structural_levels=_levels(), cohort_id=uuid4(),
        matrix_id=uuid4(), session_date=SESSION, cutoff=CUTOFF, decision_at=NOW,
        session_close=NOW + timedelta(hours=4), batch_id=uuid4(),
    )
    assert dict(build.rejections)["O3_VALUATION_INPUT_UNAVAILABLE"] == 1


def test_retry_builds_the_same_event_and_candidate_ids():
    cohort, matrix = uuid4(), uuid4()

    def once():
        return assemble_o3_inputs(
            contracts=CONTRACTS, quotes=QUOTES, marks=MARKS, activity_sources=(_source(),), spot_by_underlying=SPOTS,
            valuation_inputs=VALUATION, stock_evidence=_stock_factory(), structural_levels=_levels(), cohort_id=cohort,
            matrix_id=matrix, session_date=SESSION, cutoff=CUTOFF, decision_at=NOW,
            session_close=NOW + timedelta(hours=4), batch_id=uuid4(),
        ).records[0]

    first, second = once(), once()
    assert first.parts.event_id == second.parts.event_id
    assert first.parts.candidate.candidate_id == second.parts.candidate.candidate_id


def test_occ_underlying_parses_provider_tickers():
    assert occ_underlying("O:SPY261120C00650000") == "SPY"
    assert occ_underlying("O:BRK.B261120P00400000") == "BRK.B"
    assert occ_underlying("SPY") is None


def test_judged_leg_gate_reads_only_the_packages_legs(monkeypatch):
    from realtime import advanced_options_native_loader as loader

    candidate = uuid4()

    class Cursor:
        def execute(self, sql, params):
            self.params = params

        def fetchall(self):
            return [{"event_id": str(uuid4()), "retained_input": {"package_candidate_id": str(candidate)}}]

    package = SimpleNamespace(legs=(SimpleNamespace(instrument_id=12), SimpleNamespace(instrument_id=11)))
    monkeypatch.setattr(loader, "load_realtime_option_packages_cursor", lambda cursor, ids, cohort_id: (package,))
    plan = SimpleNamespace(control_plane=SimpleNamespace(cohort_id=uuid4()), cutoff=CUTOFF)
    assert loader._o3_judged_instrument_ids(Cursor(), plan) == (11, 12)


def test_switch_is_off_by_default(monkeypatch):
    from realtime.advanced_options_o3_inputs import o3_inputs_enabled

    monkeypatch.delenv("REALTIME_V3_O3_INPUTS", raising=False)
    assert not o3_inputs_enabled()
    monkeypatch.setenv("REALTIME_V3_O3_INPUTS", "on")
    assert o3_inputs_enabled()


def test_outcomes_name_each_underlyings_reasons():
    build = _assemble(sources=(_source(_contract(14, 104, "90")),))
    (outcome,) = build.outcomes
    assert (outcome.underlying, outcome.considered, outcome.confirmed) == ("AAPL", 1, 0)
    assert dict(outcome.rejections) == {"O3_DATED_ACTIVITY_SOURCE_UNAVAILABLE": 1}
    (confirmed,) = _assemble().outcomes
    assert (confirmed.considered, confirmed.confirmed) == (1, 1)


class _LaneCursor:
    """Answers the three O3 lane reads by statement: marks, accepted inputs, NO_INPUT rows."""

    def __init__(self, no_input_rows):
        self.no_input_rows = no_input_rows
        self.sql = ""

    def execute(self, sql, params=None):
        self.sql = sql

    def fetchall(self):
        if "disposition='NO_INPUT'" in self.sql:
            return list(self.no_input_rows)
        return []


def _lane_plan():
    return SimpleNamespace(control_plane=SimpleNamespace(cohort_id=uuid4(), slot_set=SimpleNamespace(members=())),
                           cutoff=CUTOFF, source_flow=SimpleNamespace(journal_generation=uuid4(), stream_session_ids=()),
                           unmarked_instrument_ids=())


def test_lane_with_switch_on_and_no_input_rows_is_evaluated_with_zero_inputs(monkeypatch):
    from realtime import advanced_options_native_loader as loader

    monkeypatch.setenv("REALTIME_V3_O3_INPUTS", "ON")
    evidence = []
    lane, identity, accepted_unavailable = loader._load_o3(
        _LaneCursor([{"event_id": str(uuid4()), "evidence_sha256": "e" * 64}]), _lane_plan(), evidence)
    assert lane.invocations == () and lane.blocked_reason_codes == () and not accepted_unavailable
    assert identity["o3_inputs"] == "NO_QUALIFYING_INPUT"
    assert [(row.member_key, row.available) for row in evidence] == [("ACCEPTED_INPUTS", True)]


def test_lane_without_any_producer_row_stays_blocked_as_today(monkeypatch):
    from realtime import advanced_options_native_loader as loader

    monkeypatch.setenv("REALTIME_V3_O3_INPUTS", "ON")
    evidence = []
    lane, _identity, accepted_unavailable = loader._load_o3(_LaneCursor([]), _lane_plan(), evidence)
    assert lane.blocked_reason_codes == ("O3_ACCEPTED_INPUT_EVIDENCE_ABSENT",) and accepted_unavailable
