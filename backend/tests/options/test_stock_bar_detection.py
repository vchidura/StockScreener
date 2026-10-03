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


def bar(interval, start, minutes, close, *, session=SESSION, observed=None):
    close = Decimal(str(close))
    return EquityBarRevision(bar_revision_id=uuid4(), security_id=SECURITY, ticker="AAPL",
        interval=interval, session_date=session, bar_start=start, bar_end=start + timedelta(minutes=minutes),
        open_price=close, high_price=close + 1, low_price=close - 1, close_price=close,
        volume=Decimal("1000"), vwap=None, transaction_count=None, source_kind=BarSourceKind.NATIVE_REST,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED, is_final=True,
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