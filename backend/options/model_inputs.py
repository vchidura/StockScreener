from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from xml.etree import ElementTree
from uuid import NAMESPACE_URL, UUID, uuid5


TREASURY_CURVE_SOURCE = "us_treasury_par_yield_v1"
TREASURY_CURVE_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
    "pages/xml?data=daily_treasury_yield_curve&field_tdr_date_value={year}"
)
TREASURY_TENORS = {
    "BC_1MONTH": 30,
    "BC_1_5MONTH": 45,
    "BC_2MONTH": 60,
    "BC_3MONTH": 91,
    "BC_4MONTH": 122,
    "BC_6MONTH": 183,
    "BC_1YEAR": 365,
    "BC_2YEAR": 730,
    "BC_3YEAR": 1095,
    "BC_5YEAR": 1825,
    "BC_7YEAR": 2555,
    "BC_10YEAR": 3650,
    "BC_20YEAR": 7300,
    "BC_30YEAR": 10950,
}
_ATOM = "{http://www.w3.org/2005/Atom}"
_DATA = "{http://schemas.microsoft.com/ado/2007/08/dataservices}"
_METADATA = "{http://schemas.microsoft.com/ado/2007/08/dataservices/metadata}"


@dataclass(frozen=True, slots=True)
class RiskFreeRateObservation:
    rate_observation_id: UUID
    rate_date: date
    tenor_days: int
    annual_rate: float
    source: str
    source_key: str
    source_observed_at: datetime | None
    first_observed_at: datetime
    availability_mode: str
    replay_available_at: datetime | None
    payload_sha256: str


@dataclass(frozen=True, slots=True)
class DividendCashFlow:
    ex_date: date
    cash_amount: Decimal

    def __post_init__(self) -> None:
        if self.cash_amount <= 0:
            raise ValueError("dividend cash amount must be positive")


@dataclass(frozen=True, slots=True)
class ExpirationValuationCandidate:
    valuation_date: date
    expiration_date: date
    spot: Decimal
    risk_free_rate: float
    dividend_yield: float
    rate_date: date
    rate_observation_ids: tuple[UUID, ...]
    rate_payload_sha256: tuple[str, ...]
    dividend_cash_flows: tuple[DividendCashFlow, ...]
    dividend_coverage_start: date
    dividend_coverage_end: date
    dividend_source_key: str
    dividend_payload_sha256: str
    selected_at: datetime


@dataclass(frozen=True, slots=True)
class DefaultedExpirationValuationInput:
    risk_free_rate: float
    dividend_yield: float

    def __post_init__(self) -> None:
        if (not math.isfinite(self.risk_free_rate)
                or not math.isfinite(self.dividend_yield)
                or not 0 <= self.dividend_yield <= 1):
            raise ValueError("defaulted expiration valuation inputs must be finite")


def prepare_expiration_valuation_candidate(
    *, curve: tuple[RiskFreeRateObservation, ...], expected_rate_date: date,
    valuation_date: date, expiration_date: date, spot: Decimal, selected_at: datetime,
    maximum_age_seconds: int, cash_flows: tuple[DividendCashFlow, ...],
    dividend_coverage_start: date, dividend_coverage_end: date,
    dividend_coverage_complete: bool, dividend_received_at: datetime,
    dividend_source_key: str, dividend_payload_sha256: str,
) -> ExpirationValuationCandidate:
    selected = _utc(selected_at)
    dividend_received = _utc(dividend_received_at)
    if (type(maximum_age_seconds) is not int or maximum_age_seconds <= 0
            or valuation_date > expiration_date or expected_rate_date > valuation_date
            or valuation_date > selected.date()
            or not spot.is_finite() or spot <= 0):
        raise ValueError("valuation candidate requires finite spot, valid dates and explicit freshness")
    if (dividend_coverage_complete is not True
            or not dividend_coverage_start <= valuation_date <= expiration_date <= dividend_coverage_end):
        raise ValueError("dividend/distribution coverage is unavailable for the full expiry window")
    if (not dividend_source_key.strip() or len(dividend_payload_sha256) != 64
            or any(character not in "0123456789abcdef" for character in dividend_payload_sha256)):
        raise ValueError("dividend/distribution provenance is missing")
    if not 0 <= (selected - dividend_received).total_seconds() <= maximum_age_seconds:
        raise ValueError("dividend/distribution observation is stale or future")
    rates = tuple(row for row in curve if row.rate_date == expected_rate_date)
    if not rates or len({row.tenor_days for row in rates}) != len(rates):
        raise ValueError("required Treasury curve is missing or ambiguous")
    for row in rates:
        if (row.source != TREASURY_CURVE_SOURCE or row.availability_mode != "LIVE_OBSERVED"
            or row.source_key != f"{expected_rate_date.isoformat()}:{row.tenor_days}d"
                or row.replay_available_at is not None or row.tenor_days <= 0
                or not math.isfinite(row.annual_rate) or row.source_observed_at is None
                or not _utc(row.source_observed_at) <= _utc(row.first_observed_at) <= selected
                or not 0 <= (selected - _utc(row.first_observed_at)).total_seconds() <= maximum_age_seconds
                or len(row.payload_sha256) != 64
                or any(character not in "0123456789abcdef" for character in row.payload_sha256)):
            raise ValueError("Treasury source/availability/freshness provenance is invalid")
    if any(not flow.cash_amount.is_finite() or flow.cash_amount <= 0 for flow in cash_flows):
        raise ValueError("dividend/distribution amount must be finite and positive")
    rate = interpolate_rate(rates, max((expiration_date - valuation_date).days, 1))
    yield_value = equivalent_continuous_dividend_yield(spot=spot, valuation_date=valuation_date,
        expiration_date=expiration_date, risk_free_rate=rate, cash_flows=cash_flows)
    return ExpirationValuationCandidate(valuation_date, expiration_date, spot, rate, yield_value,
        expected_rate_date, tuple(row.rate_observation_id for row in rates),
        tuple(row.payload_sha256 for row in rates), cash_flows,
        dividend_coverage_start, dividend_coverage_end, dividend_source_key,
        dividend_payload_sha256, selected)


def parse_treasury_curve(
    xml: str,
    *,
    received_at: datetime,
) -> tuple[RiskFreeRateObservation, ...]:
    received = _utc(received_at)
    root = ElementTree.fromstring(xml)
    updated_node = root.find(f"{_ATOM}updated")
    source_observed_at = (
        _parse_datetime(updated_node.text) if updated_node is not None else None
    )
    parsed: list[tuple[date, int, float, dict[str, object]]] = []
    for entry in root.findall(f"{_ATOM}entry"):
        properties = entry.find(f"{_ATOM}content/{_METADATA}properties")
        if properties is None:
            continue
        date_node = properties.find(f"{_DATA}NEW_DATE")
        if date_node is None or not date_node.text:
            continue
        rate_date = datetime.fromisoformat(
            date_node.text.replace("Z", "+00:00")
        ).date()
        for field, tenor_days in TREASURY_TENORS.items():
            node = properties.find(f"{_DATA}{field}")
            if node is None or not node.text:
                continue
            annual_rate = float(node.text) / 100.0
            if not math.isfinite(annual_rate):
                raise ValueError("Treasury rate must be finite")
            parsed.append((
                rate_date,
                tenor_days,
                annual_rate,
                {"rate_date": rate_date.isoformat(), "field": field, "value": node.text},
            ))
    if not parsed:
        raise ValueError("Treasury curve response contains no rates")
    latest_date = max(row[0] for row in parsed)
    results = []
    for rate_date, tenor_days, annual_rate, payload in parsed:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(canonical.encode("ascii")).hexdigest()
        source_key = f"{rate_date.isoformat()}:{tenor_days}d"
        availability_mode = (
            "LIVE_OBSERVED" if rate_date == latest_date else "HISTORICAL_RECONSTRUCTED"
        )
        results.append(RiskFreeRateObservation(
            rate_observation_id=uuid5(
                NAMESPACE_URL,
                f"option-rate:{TREASURY_CURVE_SOURCE}:{source_key}:{received.isoformat()}:{digest}",
            ),
            rate_date=rate_date,
            tenor_days=tenor_days,
            annual_rate=annual_rate,
            source=TREASURY_CURVE_SOURCE,
            source_key=source_key,
            source_observed_at=source_observed_at,
            first_observed_at=received,
            availability_mode=availability_mode,
            replay_available_at=(received if availability_mode == "HISTORICAL_RECONSTRUCTED" else None),
            payload_sha256=digest,
        ))
    return tuple(results)


def interpolate_rate(
    curve: tuple[RiskFreeRateObservation, ...],
    maturity_days: int,
) -> float:
    if maturity_days <= 0:
        raise ValueError("maturity_days must be positive")
    if not curve:
        raise ValueError("risk-free curve is empty")
    dates = {row.rate_date for row in curve}
    if len(dates) != 1:
        raise ValueError("risk-free curve must share one rate date")
    ordered = sorted(curve, key=lambda row: row.tenor_days)
    if maturity_days <= ordered[0].tenor_days:
        return ordered[0].annual_rate
    if maturity_days >= ordered[-1].tenor_days:
        return ordered[-1].annual_rate
    lower = max(
        (row for row in ordered if row.tenor_days <= maturity_days),
        key=lambda row: row.tenor_days,
    )
    upper = min(
        (row for row in ordered if row.tenor_days >= maturity_days),
        key=lambda row: row.tenor_days,
    )
    if lower.tenor_days == upper.tenor_days:
        return lower.annual_rate
    weight = (maturity_days - lower.tenor_days) / (
        upper.tenor_days - lower.tenor_days
    )
    return lower.annual_rate + weight * (upper.annual_rate - lower.annual_rate)


def equivalent_continuous_dividend_yield(
    *,
    spot: Decimal,
    valuation_date: date,
    expiration_date: date,
    risk_free_rate: float,
    cash_flows: tuple[DividendCashFlow, ...],
    maturity_years: float | None = None,
) -> float:
    """Continuous yield q with S*(1-exp(-q*T)) equal to the present value of the cash dividends.

    By default T is the date-based maturity (legacy, `black_scholes_european_v1`). Pass the solver's own
    timestamp-based `maturity_years` so q is applied over exactly the T it was derived for; with the
    date-based T the dividend effect is mis-scaled by up to a day over T (about 2 volatility points at
    one day to expiry for an index ETF, 0.06 at ten days).
    """
    if spot <= 0:
        raise ValueError("spot must be positive")
    maturity_days = (expiration_date - valuation_date).days
    if maturity_days <= 0:
        return 0.0
    if maturity_years is not None and maturity_years <= 0:
        return 0.0
    maturity_years = maturity_days / 365.0 if maturity_years is None else maturity_years
    present_value = 0.0
    for flow in cash_flows:
        days = (flow.ex_date - valuation_date).days
        if not 0 < days <= maturity_days:
            continue
        present_value += float(flow.cash_amount) * math.exp(
            -risk_free_rate * days / 365.0
        )
    if present_value <= 0:
        return 0.0
    ratio = present_value / float(spot)
    if ratio >= 1:
        raise ValueError("dividend present value must be below spot")
    return -math.log1p(-ratio) / maturity_years


def _parse_datetime(value: str | None) -> datetime:
    if not value:
        raise ValueError("Treasury timestamp is missing")
    return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)