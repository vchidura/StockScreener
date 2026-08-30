import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics import (
    DteBucket,
    FilterReason,
    UnderlyingMinuteBar,
    calculate_contract_economics,
    filter_contract,
    select_developer_marks,
)
from options.domain import ContractType, DataQualityFlag, ExerciseStyle


UTC = timezone.utc
MARKET_TIME = datetime(2026, 8, 31, 14, 0, tzinfo=UTC)
CUTOFF = datetime(2026, 8, 31, 20, 0, tzinfo=UTC)


def _filter(expiration_days, **overrides):
    values = {
        "contract_type": ContractType.CALL,
        "exercise_style": ExerciseStyle.AMERICAN,
        "shares_per_contract": 100,
        "has_additional_deliverables": False,
        "expiration_date": date(2026, 8, 31) + timedelta(days=expiration_days),
        "expiration_cutoff": CUTOFF + timedelta(days=expiration_days),
        "market_time": MARKET_TIME,
        "strike": Decimal("100"),
        "spot": Decimal("100"),
        "day_volume": 20,
        "open_interest": 100,
    }
    values.update(overrides)
    return filter_contract(**values)


@pytest.mark.parametrize(
    ("dte", "eligible", "bucket"),
    [
        (-1, False, None),
        (0, True, DteBucket.ZERO_DTE),
        (1, True, DteBucket.WEEKLY),
        (14, True, DteBucket.WEEKLY),
        (15, True, DteBucket.MONTHLY),
        (45, True, DteBucket.MONTHLY),
        (46, False, None),
    ],
)
def test_dte_boundaries(dte, eligible, bucket):
    result = _filter(dte)
    assert result.eligible is eligible
    assert result.dte_bucket is bucket


@pytest.mark.parametrize(
    ("strike", "eligible"),
    [
        ("84.999", False),
        ("85.000", True),
        ("115.000", True),
        ("115.001", False),
    ],
)
def test_moneyness_corridor_is_inclusive(strike, eligible):
    assert _filter(7, strike=Decimal(strike)).eligible is eligible


@pytest.mark.parametrize(
    ("volume", "open_interest", "eligible"),
    [(19, 99, False), (20, 99, True), (19, 100, True), (None, None, False)],
)
def test_liquidity_floor_is_literal_or_rule(volume, open_interest, eligible):
    result = _filter(7, day_volume=volume, open_interest=open_interest)
    assert result.eligible is eligible
    if volume is None:
        assert DataQualityFlag.MISSING_DAY_VOLUME in result.quality_flags
    if open_interest is None:
        assert DataQualityFlag.MISSING_OPEN_INTEREST in result.quality_flags


def test_expiration_cutoff_rejects_zero_dte_at_cutoff():
    result = _filter(0, market_time=CUTOFF)
    assert result.eligible is False
    assert FilterReason.EXPIRED_CONTRACT in result.reasons


def test_developer_mark_uses_backward_bar_with_inclusive_sixty_second_skew():
    mark_time = MARKET_TIME
    bars = (
        UnderlyingMinuteBar(Decimal("100"), mark_time - timedelta(seconds=60)),
        UnderlyingMinuteBar(Decimal("101"), mark_time + timedelta(seconds=1)),
    )
    result = select_developer_marks(
        day_close=Decimal("2.50"),
        day_vwap=Decimal("2.40"),
        option_mark_time=mark_time,
        underlying_bars=bars,
        observed_at=mark_time + timedelta(minutes=15),
    )
    assert result.model_mark == Decimal("2.50")
    assert result.aligned_spot == Decimal("100")
    assert result.source_skew_seconds == 60


def test_day_vwap_is_display_only_and_never_a_model_mark():
    result = select_developer_marks(
        day_close=None,
        day_vwap=Decimal("2.40"),
        option_mark_time=MARKET_TIME,
        underlying_bars=(UnderlyingMinuteBar(Decimal("100"), MARKET_TIME),),
        observed_at=MARKET_TIME + timedelta(minutes=15),
    )
    assert result.display_mark == Decimal("2.40")
    assert result.model_mark is None
    assert DataQualityFlag.FALLBACK_MARK in result.quality_flags


def test_below_intrinsic_mark_is_not_clamped_and_is_invalidated():
    result = calculate_contract_economics(
        contract_type=ContractType.CALL,
        strike=Decimal("90"),
        spot=Decimal("100"),
        model_mark=Decimal("9.98"),
        intrinsic_price_tolerance=Decimal("0.01"),
    )
    assert result.intrinsic_value == Decimal("10")
    assert result.extrinsic_value == Decimal("-0.02")
    assert result.model_mark is None
    assert result.single_contract_breakeven is None
    assert result.quality_flags == (DataQualityFlag.BELOW_INTRINSIC_MARK,)