from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
import hashlib
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5
from zoneinfo import ZoneInfo

from pydantic import AwareDatetime, Field, model_validator

from equity.behavior import Contract, Name, Sha256
from equity.domain import BarAvailabilityMode, BarSessionScope, EquityBarRevision
from options.strategies.domain import canonical_json


STOCK_BAR_SIGNAL_POLICY = {
    "version": "canonical_stock_bar_signal_v1",
    "universe": "CONFIGURED_OPTION_UNDERLYERS",
    "timeframe_precedence": ["1h", "30m", "15m", "5m"],
    "minimum_bars": {"S1": 2, "S2": 3},
    "threshold_atr_fraction": "0.5",
    "atr": "SIMPLE_TRUE_RANGE_14_THROUGH_PRIOR_SESSION",
    "S1": "PREVIOUS_INSIDE_LATEST_OUTSIDE",
    "S2": "OUTSIDE_DIRECTIONAL_PULLBACK_RESUME_OUTSIDE",
    "invalidation": "PRIOR_DAILY_CLOSE",
    "target": "SYMMETRIC_TRIGGER_DISTANCE_FROM_PRIOR_CLOSE",
    "maximum_validity_seconds": 1800,
    "completed_bars_only": True,
    "execution_permission": False,
}
STOCK_BAR_SIGNAL_POLICY_SHA256 = hashlib.sha256(
    canonical_json(STOCK_BAR_SIGNAL_POLICY).encode("ascii")
).hexdigest()


class CanonicalStockBarPoint(Contract):
    bar_revision_id: UUID
    interval: Literal["5m", "15m", "30m", "1h", "1d"]
    session_date: date
    bar_start: AwareDatetime
    bar_end: AwareDatetime
    open: Decimal = Field(gt=0, allow_inf_nan=False)
    high: Decimal = Field(gt=0, allow_inf_nan=False)
    low: Decimal = Field(gt=0, allow_inf_nan=False)
    close: Decimal = Field(gt=0, allow_inf_nan=False)
    system_observed_at: AwareDatetime
    created_at: AwareDatetime
    payload_sha256: Sha256
    source_kind: Name = "NATIVE_REST"
    availability_mode: Literal["LIVE_OBSERVED"] = "LIVE_OBSERVED"
    quality_codes: tuple[Name, ...] = ()

    @model_validator(mode="after")
    def validate_bar(self):
        if (not self.bar_start < self.bar_end <= self.system_observed_at <= self.created_at
                or self.high < max(self.open, self.close, self.low)
            or self.low > min(self.open, self.close, self.high)
            or (self.interval in ("1d", "1h") and self.quality_codes not in ((), ("DERIVED_FROM_CANONICAL_30M",)))
            or (self.interval not in ("1d", "1h") and self.quality_codes)):
            raise ValueError("canonical stock signal bar is invalid")
        return self


class CanonicalStockSignal(Contract):
    schema_version: Literal["option_canonical_stock_signal_v1"] = "option_canonical_stock_signal_v1"
    policy_version: Literal["canonical_stock_bar_signal_v1"] = "canonical_stock_bar_signal_v1"
    policy_sha256: Sha256 = STOCK_BAR_SIGNAL_POLICY_SHA256
    signal_id: UUID
    detector_id: Literal["S1", "S2"]
    security_id: UUID
    underlyer: Name
    direction: Literal[-1, 1]
    interval: Literal["5m", "15m", "30m", "1h"]
    session_date: date
    prior_close: Decimal = Field(gt=0, allow_inf_nan=False)
    prior_atr14: Decimal = Field(gt=0, allow_inf_nan=False)
    threshold: Decimal = Field(gt=0, allow_inf_nan=False)
    trigger_price: Decimal = Field(gt=0, allow_inf_nan=False)
    stop: Decimal = Field(gt=0, allow_inf_nan=False)
    target: Decimal = Field(gt=0, allow_inf_nan=False)
    daily_bars: tuple[CanonicalStockBarPoint, ...] = Field(min_length=15, max_length=15)
    bars: tuple[CanonicalStockBarPoint, ...] = Field(min_length=2, max_length=3)
    market_time: AwareDatetime
    available_at: AwareDatetime
    received_at: AwareDatetime
    valid_until: AwareDatetime
    disposition: Literal["DETECTED"] = "DETECTED"
    evidence_mode: Literal["PROSPECTIVE_RECEIPT"] = "PROSPECTIVE_RECEIPT"
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_signal(self):
        expected_count = 2 if self.detector_id == "S1" else 3
        expected_id = uuid5(NAMESPACE_URL, ":".join(("canonical-stock-signal", self.policy_sha256,
            self.detector_id, str(self.security_id), self.session_date.isoformat(), str(self.direction),
            str(self.bars[-1].bar_revision_id))))
        if (self.policy_sha256 != STOCK_BAR_SIGNAL_POLICY_SHA256 or len(self.bars) != expected_count
                or self.signal_id != expected_id or any(bar.interval != self.interval for bar in self.bars)
                or tuple(bar.bar_end for bar in self.bars) != tuple(sorted(bar.bar_end for bar in self.bars))
                or any(bar.session_date != self.session_date for bar in self.bars)
                or self.threshold != self.prior_atr14 * Decimal("0.5")
                or self.stop != self.prior_close
                or self.target != self.trigger_price + self.direction * abs(self.trigger_price - self.prior_close)
                or self.market_time != self.bars[-1].bar_end
                or self.available_at != max(max(bar.system_observed_at, bar.created_at) for bar in self.bars)
                or not self.market_time <= self.available_at <= self.received_at < self.valid_until):
            raise ValueError("canonical stock signal identity, geometry or clocks mismatch")
        daily = tuple(sorted(self.daily_bars, key=lambda bar: bar.bar_end))
        true_ranges = tuple(max(current.high - current.low,
            abs(current.high - previous.close), abs(current.low - previous.close))
            for previous, current in zip(daily, daily[1:]))
        if (any(bar.interval != "1d" or bar.session_date >= self.session_date for bar in daily)
                or self.prior_close != daily[-1].close
                or self.prior_atr14 != sum(true_ranges, Decimal("0")) / Decimal(14)):
            raise ValueError("canonical stock signal daily ATR lineage mismatch")
        closes = tuple(bar.close for bar in self.bars)
        upper, lower = self.prior_close + self.threshold, self.prior_close - self.threshold
        if self.detector_id == "S1":
            valid_pattern = lower <= closes[0] <= upper and (
                self.direction == 1 and closes[1] > upper or self.direction == -1 and closes[1] < lower)
        else:
            distances = tuple(self.direction * (close - self.prior_close) for close in closes)
            boundary = upper if self.direction == 1 else lower
            valid_pattern = (self.direction * (closes[0] - boundary) > 0 and distances[1] > 0
                and distances[1] < distances[0] and distances[2] > distances[1]
                and self.direction * (closes[2] - boundary) > 0)
        if not valid_pattern:
            raise ValueError("canonical stock signal pattern mismatch")
        return self


def _point(bar: EquityBarRevision, created_at: datetime) -> CanonicalStockBarPoint:
    return CanonicalStockBarPoint(bar_revision_id=bar.bar_revision_id, interval=bar.interval,
        session_date=bar.session_date, bar_start=bar.bar_start, bar_end=bar.bar_end,
        open=bar.open_price, high=bar.high_price, low=bar.low_price, close=bar.close_price,
        system_observed_at=bar.system_observed_at, created_at=created_at,
        payload_sha256=bar.payload_sha256, source_kind=bar.source_kind.value,
        availability_mode=bar.availability_mode.value, quality_codes=bar.quality_codes)


def _atr14(daily: tuple[tuple[EquityBarRevision, datetime], ...]):
    ordered = tuple(sorted(daily, key=lambda value: value[0].bar_end))
    if len(ordered) < 15:
        raise ValueError("canonical stock signal requires 15 completed daily bars")
    window = ordered[-15:]
    true_ranges = []
    for (previous, _), (current, _) in zip(window, window[1:]):
        true_ranges.append(max(current.high_price - current.low_price,
            abs(current.high_price - previous.close_price), abs(current.low_price - previous.close_price)))
    atr = sum(true_ranges, Decimal("0")) / Decimal(14)
    if atr <= 0:
        raise ValueError("canonical stock signal prior ATR is unavailable")
    return window[-1][0].close_price, atr, window


def detect_canonical_stock_signals(*, security_id: UUID, underlyer: str,
                                   bars: tuple[tuple[EquityBarRevision, datetime], ...],
                                   market_cutoff: datetime, decision_at: datetime,
                                   session_close: datetime) -> tuple[CanonicalStockSignal, ...]:
    if (market_cutoff.utcoffset() is None or decision_at.utcoffset() is None
            or session_close.utcoffset() is None or market_cutoff > decision_at):
        raise ValueError("canonical stock signal requires causal aware cutoffs")
    eligible = []
    signal_session = market_cutoff.astimezone(ZoneInfo("America/New_York")).date()
    for bar, created_at in bars:
        quality_eligible = not bar.quality_codes or (
            bar.interval in ("1d", "1h") and bar.quality_codes == ("DERIVED_FROM_CANONICAL_30M",))
        if (bar.security_id != security_id or bar.ticker != underlyer or not bar.is_final
                or bar.adjusted or bar.availability_mode is not BarAvailabilityMode.LIVE_OBSERVED
            or bar.session_scope is not BarSessionScope.RTH or not quality_eligible
                or bar.bar_end > market_cutoff or bar.system_observed_at > decision_at
                or created_at > decision_at):
            continue
        eligible.append((bar, created_at))
    daily = tuple((bar, created) for bar, created in eligible
        if bar.interval == "1d" and bar.session_date < signal_session)
    prior_close, prior_atr, daily_window = _atr14(daily)
    threshold = prior_atr * Decimal("0.5")
    upper, lower = prior_close + threshold, prior_close - threshold
    outputs = []
    for detector_id, required in (("S1", 2), ("S2", 3)):
        selected = None
        for interval in STOCK_BAR_SIGNAL_POLICY["timeframe_precedence"]:
            values = sorted(((bar, created) for bar, created in eligible
                if bar.interval == interval and bar.session_date == signal_session), key=lambda value: value[0].bar_end)
            if len(values) >= required:
                selected = tuple(values[-required:])
                break
        if selected is None:
            continue
        closes = tuple(value[0].close_price for value in selected)
        direction = None
        if detector_id == "S1":
            if lower <= closes[0] <= upper and closes[1] > upper:
                direction = 1
            elif lower <= closes[0] <= upper and closes[1] < lower:
                direction = -1
        else:
            for side, boundary in ((1, upper), (-1, lower)):
                distances = tuple(side * (close - prior_close) for close in closes)
                if (side * (closes[0] - boundary) > 0 and distances[1] > 0
                        and distances[1] < distances[0] and distances[2] > distances[1]
                        and side * (closes[2] - boundary) > 0):
                    direction = side
                    break
        if direction is None:
            continue
        points = tuple(_point(bar, created) for bar, created in selected)
        available_at = max(max(point.system_observed_at, point.created_at) for point in points)
        valid_until = min(points[-1].bar_end + timedelta(seconds=STOCK_BAR_SIGNAL_POLICY["maximum_validity_seconds"]), session_close)
        if decision_at >= valid_until:
            continue
        signal_id = uuid5(NAMESPACE_URL, ":".join(("canonical-stock-signal",
            STOCK_BAR_SIGNAL_POLICY_SHA256, detector_id, str(security_id),
            signal_session.isoformat(), str(direction), str(points[-1].bar_revision_id))))
        outputs.append(CanonicalStockSignal(signal_id=signal_id, detector_id=detector_id,
            security_id=security_id, underlyer=underlyer, direction=direction,
            interval=points[-1].interval, session_date=signal_session,
            prior_close=prior_close, prior_atr14=prior_atr, threshold=threshold,
            trigger_price=points[-1].close, stop=prior_close,
            target=points[-1].close + direction * abs(points[-1].close - prior_close),
            daily_bars=tuple(_point(bar, created) for bar, created in daily_window),
            bars=points, market_time=points[-1].bar_end, available_at=available_at,
            received_at=decision_at, valid_until=valid_until))
    return tuple(outputs)