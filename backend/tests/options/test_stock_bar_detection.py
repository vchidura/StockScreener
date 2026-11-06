from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

from equity.domain import BarAvailabilityMode, BarSessionScope, BarSourceKind, EquityBarRevision
from options.stock_bar_detection import CanonicalStockSignal, detect_canonical_stock_signals


UTC = timezone.utc
SESSION = date(2026, 9, 28)
OPEN = datetime(2026, 9, 28, 13, 30, tzinfo=UTC)
SECURITY = uuid4()


def bar(
    interval,
    start,
    minutes,
    close,
    *,
    session=SESSION,
    observed=None,
    availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
):
    close = Decimal(str(close))
    return EquityBarRevision(bar_revision_id=uuid4(), security_id=SECURITY, ticker="AAPL",
        interval=interval, session_date=session, bar_start=start, bar_end=start + timedelta(minutes=minutes),
        open_price=close, high_price=close + 1, low_price=close - 1, close_price=close,
        volume=Decimal("1000"), vwap=None, transaction_count=None, source_kind=BarSourceKind.NATIVE_REST,
        availability_mode=availability_mode, is_final=True,
        system_observed_at=observed or start + timedelta(minutes=minutes), replay_available_at=None,
        adjusted=False, payload_sha256="a" * 64, session_scope=BarSessionScope.RTH)


def daily_history():
    start = datetime(2026, 9, 7, 20, tzinfo=UTC)
    rows = []
    for index in range(15):
        session = date(2026, 9, 7) + timedelta(days=index)
        rows.append(bar("1d", start + timedelta(days=index), 1, 100, session=session))
    return rows


def inputs(*intraday):
    rows = [*daily_history(), *intraday]
    return tuple((item, item.system_observed_at) for item in rows)


def test_s1_uses_completed_5m_fallback_then_prefers_15m_when_evaluable():
    five = (bar("5m", OPEN, 5, 100), bar("5m", OPEN + timedelta(minutes=5), 5, 101.1))
    signals = detect_canonical_stock_signals(security_id=SECURITY, underlyer="AAPL", bars=inputs(*five),
        market_cutoff=five[-1].bar_end, decision_at=five[-1].bar_end + timedelta(minutes=15),
        session_close=datetime(2026, 9, 28, 20, tzinfo=UTC))
    s1 = next(signal for signal in signals if signal.detector_id == "S1")
    assert s1.interval == "5m" and s1.direction == 1 and s1.stop == Decimal("100")
    assert s1.target == Decimal("102.2")
    fifteen = (bar("15m", OPEN, 15, 100), bar("15m", OPEN + timedelta(minutes=15), 15, 98.9))
    preferred = detect_canonical_stock_signals(security_id=SECURITY, underlyer="AAPL",
        bars=inputs(*five, *fifteen), market_cutoff=fifteen[-1].bar_end,
        decision_at=fifteen[-1].bar_end + timedelta(minutes=15),
        session_close=datetime(2026, 9, 28, 20, tzinfo=UTC))
    s1 = next(signal for signal in preferred if signal.detector_id == "S1")
    assert s1.interval == "15m" and s1.direction == -1


def test_reconstructed_daily_history_is_allowed_but_intraday_must_be_live():
    daily = tuple(
        replace(
            item,
            availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
            quality_codes=(
                ("DERIVED_FROM_CANONICAL_5M",)
                if index % 2
                else ("GROUPED_DAILY_EXACT_TICKER_V2",)
            ),
        )
        for index, item in enumerate(daily_history())
    )
    five = (
        bar("5m", OPEN, 5, 100),
        bar("5m", OPEN + timedelta(minutes=5), 5, 101.1),
    )

    signals = detect_canonical_stock_signals(
        security_id=SECURITY,
        underlyer="AAPL",
        bars=tuple(
            (item, item.system_observed_at)
            for item in (*daily, *five)
        ),
        market_cutoff=five[-1].bar_end,
        decision_at=five[-1].bar_end + timedelta(minutes=15),
        session_close=datetime(2026, 9, 28, 20, tzinfo=UTC),
    )

    assert next(signal for signal in signals if signal.detector_id == "S1")
    reconstructed_intraday = replace(
        five[-1],
        availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
    )
    assert not detect_canonical_stock_signals(
        security_id=SECURITY,
        underlyer="AAPL",
        bars=tuple(
            (item, item.system_observed_at)
            for item in (*daily, five[0], reconstructed_intraday)
        ),
        market_cutoff=five[-1].bar_end,
        decision_at=five[-1].bar_end + timedelta(minutes=15),
        session_close=datetime(2026, 9, 28, 20, tzinfo=UTC),
    )


def test_s2_detects_outside_pullback_resume_and_prefers_highest_evaluable_timeframe():
    five = (bar("5m", OPEN, 5, 101.2), bar("5m", OPEN + timedelta(minutes=5), 5, 100.6),
        bar("5m", OPEN + timedelta(minutes=10), 5, 101.1))
    thirty = (bar("30m", OPEN, 30, 98.8), bar("30m", OPEN + timedelta(minutes=30), 30, 99.4),
        bar("30m", OPEN + timedelta(minutes=60), 30, 98.9))
    signals = detect_canonical_stock_signals(security_id=SECURITY, underlyer="AAPL",
        bars=inputs(*five, *thirty), market_cutoff=thirty[-1].bar_end,
        decision_at=thirty[-1].bar_end + timedelta(minutes=15),
        session_close=datetime(2026, 9, 28, 20, tzinfo=UTC))
    s2 = next(signal for signal in signals if signal.detector_id == "S2")
    assert s2.interval == "30m" and s2.direction == -1
    assert CanonicalStockSignal.model_validate_json(s2.canonical_json()) == s2


def test_signal_rejects_forming_future_and_noncausal_bars():
    values = (bar("5m", OPEN, 5, 100), bar("5m", OPEN + timedelta(minutes=5), 5, 101.1))
    forming = replace(values[-1], is_final=False)
    future = replace(values[-1], system_observed_at=values[-1].bar_end + timedelta(hours=1))
    for changed in (forming, future):
        signals = detect_canonical_stock_signals(security_id=SECURITY, underlyer="AAPL",
            bars=inputs(values[0], changed), market_cutoff=values[-1].bar_end,
            decision_at=values[-1].bar_end + timedelta(minutes=15),
            session_close=datetime(2026, 9, 28, 20, tzinfo=UTC))
        assert not signals


def test_os_signals_read_v3_coded_intraday_bars_and_s_signals_still_do_not():
    # Every V3 5m bar records that it was built from the AM 1m stream. Before Nov 5 OS1 and OS2
    # dropped them all and could never fire (G81).
    coded = tuple(
        replace(item, quality_codes=("DERIVED_FROM_ADVANCED_AM_1M",))
        for item in (bar("5m", OPEN, 5, 100), bar("5m", OPEN + timedelta(minutes=5), 5, 101.1))
    )
    scope = dict(security_id=SECURITY, underlyer="AAPL", market_cutoff=coded[-1].bar_end,
        decision_at=coded[-1].bar_end + timedelta(minutes=15), session_close=datetime(2026, 9, 28, 20, tzinfo=UTC))

    signals = detect_canonical_stock_signals(**scope, bars=inputs(*coded), detector_ids=("OS1", "OS2"))
    assert [(signal.detector_id, signal.interval, signal.direction) for signal in signals] == [("OS1", "5m", 1)]

    assert not detect_canonical_stock_signals(**scope, bars=inputs(*coded)), "S1 and S2 keep the legacy rule"
    unknown = tuple(replace(item, quality_codes=("UNREVIEWED_DERIVATION",)) for item in coded)
    assert not detect_canonical_stock_signals(**scope, bars=inputs(*unknown), detector_ids=("OS1", "OS2")), (
        "only the listed V3 derivations are accepted"
    )


def test_os_policy_identity_records_the_accepted_intraday_codes():
    from options.stock_bar_detection import (
        ADVANCED_OPTIONS_STOCK_BAR_SIGNAL_POLICY,
        STOCK_BAR_SIGNAL_POLICY,
    )

    assert ["DERIVED_FROM_ADVANCED_AM_1M"] in ADVANCED_OPTIONS_STOCK_BAR_SIGNAL_POLICY["intraday_quality_codes"]["5m"]
    assert "intraday_quality_codes" not in STOCK_BAR_SIGNAL_POLICY