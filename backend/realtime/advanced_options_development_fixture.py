from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from .advanced_options_validation_mode import (
    AdvancedOptionsValidationMode,
    advanced_options_validation_mode,
)
from .contracts import EQUITY_VALIDATION_UNIVERSE, canonical_sha256


DEVELOPMENT_FIXTURE_E_SCHEMA = (
    "advanced-options-v3-development-fixture-e-v1"
)
DEVELOPMENT_FIXTURE_E_POLICY = (
    "advanced-options-v3-development-fixture-e-policy-v1"
)
_BASE_PRICES = {
    "QQQ": Decimal("704.75"),
    "SPY": Decimal("761.52"),
}


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _bar(
    ticker: str,
    *,
    session_date: date,
    bar_end: datetime,
    index: int,
    authorization_reference: str,
) -> dict[str, object]:
    base = _BASE_PRICES[ticker] + Decimal(index) / Decimal("10")
    identity = {
        "ticker": ticker,
        "session_date": session_date.isoformat(),
        "bar_start": (bar_end - timedelta(minutes=5)).isoformat(),
        "bar_end": bar_end.isoformat(),
        "open": str(base),
        "high": str(base + Decimal("0.20")),
        "low": str(base - Decimal("0.20")),
        "close": str(base + Decimal("0.05")),
        "volume": 10_000 + index * 1_000,
        "transaction_count": 1_000 + index * 100,
        "interval": "5m",
        "adjustment_basis": "RAW",
        "is_final": True,
        "source_kind": "DEVELOPMENT_FIXTURE",
        "observed_at": bar_end.isoformat(),
        "authorization_reference": authorization_reference,
    }
    identity["payload_sha256"] = canonical_sha256(identity)
    identity["bar_revision_id"] = str(uuid5(
        NAMESPACE_URL,
        "development-fixture-e:"
        f"{authorization_reference}:{ticker}:{bar_end.isoformat()}:"
        f"{identity['payload_sha256']}",
    ))
    return identity


def build_development_fixture_e(
    *,
    session_date: date,
    equity_cutoff: datetime,
    authorization_reference: str,
    qualification_semantic_sha256: str,
    qualification_file_sha256: str,
) -> dict[str, object]:
    cutoff = _utc(equity_cutoff, "equity_cutoff")
    reference = authorization_reference.strip()
    if (
        cutoff.date() != session_date
        or cutoff.second
        or cutoff.microsecond
        or cutoff.minute % 5
        or not reference
        or any(
            len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
            for value in (
                qualification_semantic_sha256,
                qualification_file_sha256,
            )
        )
    ):
        raise ValueError("development fixture E scope is invalid")
    bars = tuple(
        _bar(
            ticker,
            session_date=session_date,
            bar_end=cutoff - timedelta(minutes=5 * (2 - index)),
            index=index,
            authorization_reference=reference,
        )
        for ticker in ("QQQ", "SPY")
        for index in range(3)
    )
    fixture_identity = {
        "schema": DEVELOPMENT_FIXTURE_E_SCHEMA,
        "policy_version": DEVELOPMENT_FIXTURE_E_POLICY,
        "validation_mode": (
            AdvancedOptionsValidationMode.DEVELOPMENT_DIAGNOSTIC.value
        ),
        "synthetic": True,
        "production_eligible": False,
        "serving_authorized": False,
        "publication_authorized": False,
        "session_date": session_date.isoformat(),
        "equity_cutoff_utc": cutoff.isoformat(),
        "authorization_reference": reference,
        "symbols": list(EQUITY_VALIDATION_UNIVERSE.equity_tickers),
        "bars": list(bars),
        "qualification_semantic_sha256": qualification_semantic_sha256,
        "qualification_file_sha256": qualification_file_sha256,
    }
    fixture_sha256 = canonical_sha256(fixture_identity)
    cohort_id = uuid5(
        NAMESPACE_URL,
        f"development-fixture-e-cohort:{fixture_sha256}",
    )
    window_id = uuid5(
        NAMESPACE_URL,
        f"development-fixture-e-window:{fixture_sha256}",
    )
    receipt_id = uuid5(
        NAMESPACE_URL,
        f"development-fixture-e-receipt:{fixture_sha256}",
    )
    input_sha256 = canonical_sha256(tuple(
        row["payload_sha256"] for row in bars
    ))
    result = {
        **fixture_identity,
        "fixture_sha256": fixture_sha256,
        "source_cohort_id": str(cohort_id),
        "equity_window_id": str(window_id),
        "equity_receipt": {
            "receipt_id": str(receipt_id),
            "receipt_kind": "INPUT_READY",
            "cutoff": cutoff.isoformat(),
            "reference_generation": 1,
            "universe_sha256": canonical_sha256(
                EQUITY_VALIDATION_UNIVERSE.equity_tickers
            ),
            "input_sha256": input_sha256,
            "build_id": DEVELOPMENT_FIXTURE_E_POLICY,
            "validation_mode": (
                AdvancedOptionsValidationMode.DEVELOPMENT_DIAGNOSTIC.value
            ),
            "production_eligible": False,
        },
    }
    result["semantic_sha256"] = canonical_sha256(result)
    return result


def validate_development_fixture_e(
    value: Mapping[str, object],
    *,
    environment: Mapping[str, str],
) -> Mapping[str, object]:
    if (
        advanced_options_validation_mode(environment)
        is not AdvancedOptionsValidationMode.DEVELOPMENT_DIAGNOSTIC
        or value.get("schema") != DEVELOPMENT_FIXTURE_E_SCHEMA
        or value.get("policy_version") != DEVELOPMENT_FIXTURE_E_POLICY
        or value.get("validation_mode")
        != AdvancedOptionsValidationMode.DEVELOPMENT_DIAGNOSTIC.value
        or value.get("synthetic") is not True
        or value.get("production_eligible") is not False
        or value.get("serving_authorized") is not False
        or value.get("publication_authorized") is not False
        or value.get("symbols")
        != list(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
    ):
        raise ValueError("development fixture E identity changed")
    unsigned = dict(value)
    semantic_sha256 = unsigned.pop("semantic_sha256", None)
    bars = value.get("bars")
    receipt = value.get("equity_receipt")
    cutoff = datetime.fromisoformat(str(value.get("equity_cutoff_utc")))
    if (
        semantic_sha256 != canonical_sha256(unsigned)
        or not isinstance(bars, list)
        or len(bars) != 6
        or {
            (str(row.get("ticker")), str(row.get("bar_end")))
            for row in bars
            if isinstance(row, Mapping)
        }
        != {
            (
                ticker,
                (cutoff - timedelta(minutes=minutes)).isoformat(),
            )
            for ticker in ("QQQ", "SPY")
            for minutes in (10, 5, 0)
        }
        or not isinstance(receipt, Mapping)
        or receipt.get("receipt_kind") != "INPUT_READY"
        or receipt.get("cutoff") != cutoff.isoformat()
        or receipt.get("production_eligible") is not False
    ):
        raise ValueError("development fixture E contents changed")
    return value
