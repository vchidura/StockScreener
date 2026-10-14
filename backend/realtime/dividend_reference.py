"""Immutable, known-by dividend context shared by equity and option research."""
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Mapping
from uuid import UUID

from .contracts import canonical_sha256


DISTRIBUTION_TYPES = frozenset({"recurring", "special", "supplemental", "irregular", "unknown"})


def _utc(value, name):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _amount(value):
    if value is None:
        return None
    try:
        amount = Decimal(str(value))
    except InvalidOperation as error:
        raise ValueError("Dividend amount is invalid") from error
    if not amount.is_finite() or amount < 0:
        raise ValueError("Dividend amount must be finite and nonnegative")
    return amount


def _date(value):
    return None if value is None else value if isinstance(value, date) and not isinstance(value, datetime) else date.fromisoformat(str(value))


@dataclass(frozen=True, slots=True)
class DividendObservation:
    action_id: UUID
    security_id: UUID
    ticker: str
    provider_id: str | None
    source: str
    original_cash_amount: Decimal | None
    split_adjusted_cash_amount: Decimal | None
    currency: str | None
    distribution_type: str
    frequency: int | None
    declaration_date: date | None
    ex_dividend_date: date
    record_date: date | None
    pay_date: date | None
    observed_at: datetime
    received_at: datetime
    payload_sha256: str
    quality_codes: tuple[str, ...]


def dividend_observation(row: Mapping, *, known_by: datetime) -> DividendObservation:
    cutoff = _utc(known_by, "known_by")
    observed = _utc(row["observed_at"], "observed_at")
    received = _utc(row["received_at"], "received_at")
    raw = row["raw_payload"]
    if (row["action_type"] != "DIVIDEND" or not isinstance(raw, Mapping)
            or canonical_sha256(raw) != row["payload_sha256"]
            or raw.get("ticker") != row["ticker"] or observed > cutoff or received > cutoff):
        raise ValueError("Dividend identity/hash/availability drift")
    ex_date = _date(raw.get("ex_dividend_date"))
    if ex_date is None or ex_date != _date(row["effective_date"]) or ex_date != _date(row["ex_date"]):
        raise ValueError("Dividend ex-date drift")
    original = _amount(raw.get("cash_amount"))
    if original != _amount(row["cash_amount"]):
        raise ValueError("Dividend original cash amount drift")
    codes = []
    currency = raw.get("currency")
    if not isinstance(currency, str) or len(currency) != 3 or not currency.isalpha() or currency != currency.upper():
        currency = None
        codes.append("DIVIDEND_CURRENCY_UNAVAILABLE")
    distribution = raw.get("distribution_type")
    if not isinstance(distribution, str) or distribution not in DISTRIBUTION_TYPES:
        distribution = "unknown"
    if distribution == "unknown":
        codes.append("DIVIDEND_CLASSIFICATION_UNAVAILABLE")
    frequency = raw.get("frequency")
    if type(frequency) is not int or frequency < 0:
        frequency = None
    if original is None:
        codes.append("DIVIDEND_CASH_AMOUNT_UNAVAILABLE")
    dates = {}
    for field in ("declaration_date", "record_date", "pay_date"):
        value = _date(raw.get(field))
        if value != _date(row.get(field)):
            raise ValueError(f"Dividend {field} drift")
        dates[field] = value
    return DividendObservation(
        action_id=UUID(str(row["action_id"])), security_id=UUID(str(row["security_id"])), ticker=row["ticker"],
        provider_id=str(raw["id"]) if raw.get("id") else None, source=row["source"],
        original_cash_amount=original, split_adjusted_cash_amount=_amount(raw.get("split_adjusted_cash_amount")),
        currency=currency, distribution_type=distribution, ex_dividend_date=ex_date,
        frequency=frequency,
        observed_at=observed, received_at=received, payload_sha256=row["payload_sha256"],
        quality_codes=tuple(codes), **dates)


@dataclass(frozen=True, slots=True)
class DividendReferenceWindow:
    security_id: UUID
    ticker: str
    window_start: date
    window_end: date
    known_by: datetime
    status: str
    dividends: tuple[DividendObservation, ...] = ()
    reason_codes: tuple[str, ...] = ()
    coverage_id: UUID | None = None
    coverage_payload_sha256: str | None = None
    response_sha256: str | None = None
    checked_at: datetime | None = None
    refresh_due_at: datetime | None = None
    future_coverage_complete: bool = False
    version: str = "DIVIDEND_REFERENCE_WINDOW_V1"


def dividend_reference_window(*, security_id: UUID, ticker: str, window_start: date, window_end: date,
                              known_by: datetime, coverage: Mapping | None, rows=(), coverage_id=None):
    cutoff = _utc(known_by, "known_by")
    if not isinstance(security_id, UUID) or not ticker or ticker != ticker.upper() or window_end < window_start:
        raise ValueError("Dividend reference window identity or dates are invalid")
    identity = dict(security_id=security_id, ticker=ticker, window_start=window_start, window_end=window_end, known_by=cutoff)
    if coverage is None:
        return DividendReferenceWindow(**identity, status="UNAVAILABLE" if coverage_id else "NOT_CHECKED",
            reason_codes=("EXACT_DIVIDEND_COVERAGE_UNAVAILABLE" if coverage_id else "DIVIDEND_QUERY_NOT_RETAINED",))
    receipt = dict(coverage)
    observed = _utc(receipt["observed_at"], "coverage observed_at")
    received = _utc(receipt["received_at"], "coverage received_at")
    payload = dict(source_key=receipt["source_key"], response_action_count=receipt["response_action_count"],
                   response_sha256=receipt["response_sha256"])
    if (str(receipt["security_id"]) != str(security_id) or receipt["action_type"] != "DIVIDEND"
            or coverage_id is not None and str(receipt["coverage_id"]) != str(coverage_id)
            or observed > received or received > cutoff or receipt["window_start"] > window_start
            or receipt["window_end"] < window_end or canonical_sha256(payload) != receipt["payload_sha256"]):
        return DividendReferenceWindow(**identity, status="UNAVAILABLE", reason_codes=("DIVIDEND_RECEIPT_SCOPE_HASH_OR_CLOCK_DRIFT",))
    records = tuple(rows)
    if (type(receipt["response_action_count"]) is not int or receipt["response_action_count"] < 0
            or len(records) != receipt["response_action_count"]
            or len({str(row["action_id"]) for row in records}) != len(records)
            or len({row["source_key"] for row in records}) != len(records)
            or any(str(row["security_id"]) != str(security_id) or row["ticker"] != ticker
                or row["source"] != receipt["source"] or row["observed_at"] != observed or row["received_at"] != received
                or not receipt["window_start"] <= row["effective_date"] <= receipt["window_end"] for row in records)
            or canonical_sha256(tuple(row["raw_payload"] for row in records)) != receipt["response_sha256"]):
        return DividendReferenceWindow(**identity, status="UNAVAILABLE", reason_codes=("DIVIDEND_RECEIPT_FACT_PARITY_UNVERIFIED",))
    try:
        dividends = tuple(dividend_observation(row, known_by=cutoff) for row in records)
    except ValueError:
        return DividendReferenceWindow(**identity, status="UNAVAILABLE", reason_codes=("DIVIDEND_FACT_IDENTITY_HASH_OR_METADATA_DRIFT",))
    dividends = tuple(sorted((row for row in dividends if window_start <= row.ex_dividend_date <= window_end),
                             key=lambda row: (row.ex_dividend_date, str(row.action_id))))
    refresh_due = received + timedelta(days=1)
    stale = cutoff >= refresh_due
    status = ("STALE" if stale else "PARTIAL" if any(row.quality_codes for row in dividends)
              else "AVAILABLE" if dividends else "KNOWN_NO_ANNOUNCED_DIVIDEND")
    return DividendReferenceWindow(**identity, status=status, dividends=dividends,
        reason_codes=("DIVIDEND_QUERY_REFRESH_DUE",) if stale else (), coverage_id=UUID(str(receipt["coverage_id"])),
        coverage_payload_sha256=receipt["payload_sha256"], response_sha256=receipt["response_sha256"],
        checked_at=received, refresh_due_at=refresh_due)