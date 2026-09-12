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
) -> float:
    if spot <= 0:
        raise ValueError("spot must be positive")
    maturity_days = (expiration_date - valuation_date).days
    if maturity_days <= 0:
        return 0.0
    maturity_years = maturity_days / 365.0
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