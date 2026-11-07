"""Typed underlying-price evidence from the bounded P8 option snapshots."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Mapping
from uuid import UUID

from .contracts import canonical_sha256


SNAPSHOT_BINDING_CONTRACT = (
    "advanced-options-underlying-snapshot-binding-v1"
)
SNAPSHOT_TICK_CONTRACT = "advanced-options-underlying-snapshot-tick-v1"
PRICE_SOURCE = "MASSIVE_OPTION_CHAIN_UNDERLYING_ASSET_V1"
REQUIRED_TIMEFRAME = "REAL-TIME"
MAXIMUM_PRICE_AGE_SECONDS = 300
MAXIMUM_PROVIDER_CLOCK_SKEW_SECONDS = 5
_UNDERLYING_TICKER = re.compile(r"[A-Z]{1,5}")
_OPTION_ON_UNDERLYING = re.compile(r"O:([A-Z]{1,5})\d{6}[CP]\d{8}")
PRICE_POLICY_SHA256 = canonical_sha256({
    "contract": "advanced-options-underlying-snapshot-policy-v1",
    "price_source": PRICE_SOURCE,
    "required_timeframe": REQUIRED_TIMEFRAME,
    "maximum_price_age_seconds": MAXIMUM_PRICE_AGE_SECONDS,
    "maximum_provider_clock_skew_seconds": (
        MAXIMUM_PROVIDER_CLOCK_SKEW_SECONDS
    ),
})


def _utc(value: datetime, name: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _decimal(value: object, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError(f"{name} must be numeric") from error
    if not result.is_finite() or result <= 0:
        raise ValueError(f"{name} must be finite and positive")
    return result


def _nanoseconds(value: object, name: str) -> datetime:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive nanosecond timestamp")
    return datetime.fromtimestamp(
        value / 1_000_000_000,
        tz=timezone.utc,
    )


def _sha256(value: str, name: str) -> str:
    if (
        len(value) != 64
        or any(
            character not in "0123456789abcdef"
            for character in value
        )
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


@dataclass(frozen=True, slots=True)
class UnderlyingSnapshotPrice:
    ticker: str
    session_date: date
    price: Decimal
    market_time: datetime
    timeframe: str
    selected_contract_ticker: str
    selected_contract_source_sha256: str
    underlying_payload_sha256: str

    def __post_init__(self) -> None:
        ticker = self.ticker.strip().upper()
        timeframe = self.timeframe.strip().upper()
        selected_contract = self.selected_contract_ticker.strip().upper()
        market_time = _utc(self.market_time, "underlying market_time")
        # G96 (Nov 6): any underlying in the session's capture, not only SPY and QQQ. The contract must still be an
        # option on exactly this underlying (the next character after the symbol is the expiry's first digit).
        if (
            not _UNDERLYING_TICKER.fullmatch(ticker)
            or timeframe != REQUIRED_TIMEFRAME
            or not _OPTION_ON_UNDERLYING.fullmatch(selected_contract)
            or _OPTION_ON_UNDERLYING.fullmatch(selected_contract).group(1) != ticker
            or not isinstance(self.session_date, date)
            or isinstance(self.session_date, datetime)
            or market_time.date() != self.session_date
        ):
            raise ValueError("P8 underlying snapshot identity changed")
        object.__setattr__(self, "ticker", ticker)
        object.__setattr__(self, "timeframe", timeframe)
        object.__setattr__(
            self,
            "selected_contract_ticker",
            selected_contract,
        )
        object.__setattr__(
            self,
            "price",
            _decimal(self.price, "underlying price"),
        )
        object.__setattr__(self, "market_time", market_time)
        object.__setattr__(
            self,
            "selected_contract_source_sha256",
            _sha256(
                self.selected_contract_source_sha256,
                "selected contract source",
            ),
        )
        object.__setattr__(
            self,
            "underlying_payload_sha256",
            _sha256(
                self.underlying_payload_sha256,
                "underlying payload",
            ),
        )

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class UnderlyingSnapshotBinding:
    selection_sha256: str
    source_snapshot_sha256: str
    reviewed_at: datetime
    valid_until: datetime
    prices: tuple[UnderlyingSnapshotPrice, ...]
    maximum_price_age_seconds: int = MAXIMUM_PRICE_AGE_SECONDS
    binding_sha256: str = ""

    def __post_init__(self) -> None:
        reviewed = _utc(self.reviewed_at, "underlying reviewed_at")
        valid = _utc(self.valid_until, "underlying valid_until")
        prices = tuple(sorted(self.prices, key=lambda row: row.ticker))
        if (
            type(self.maximum_price_age_seconds) is not int
            or self.maximum_price_age_seconds <= 0
            or not prices
            or len({row.ticker for row in prices}) != len(prices)
            or reviewed >= valid
        ):
            raise ValueError("P8 underlying snapshot binding is invalid")
        object.__setattr__(
            self,
            "selection_sha256",
            _sha256(self.selection_sha256, "selection_sha256"),
        )
        object.__setattr__(
            self,
            "source_snapshot_sha256",
            _sha256(
                self.source_snapshot_sha256,
                "source_snapshot_sha256",
            ),
        )
        object.__setattr__(self, "reviewed_at", reviewed)
        object.__setattr__(self, "valid_until", valid)
        object.__setattr__(self, "prices", prices)
        expected = canonical_sha256(
            underlying_snapshot_binding_payload(
                self,
                include_sha256=False,
            )
        )
        if self.binding_sha256 and self.binding_sha256 != expected:
            raise ValueError(
                "P8 underlying snapshot binding identity changed"
            )
        object.__setattr__(self, "binding_sha256", expected)
        self.require_fresh(reviewed)

    def price_for(self, ticker: str) -> UnderlyingSnapshotPrice:
        normalized = ticker.strip().upper()
        matches = [
            row for row in self.prices
            if row.ticker == normalized
        ]
        if len(matches) != 1:
            raise ValueError("P8 underlying snapshot price is ambiguous")
        return matches[0]

    def require_fresh(self, checked_at: datetime) -> None:
        checked = _utc(checked_at, "underlying decision clock")
        if not self.reviewed_at <= checked < self.valid_until:
            raise ValueError(
                "P8 underlying snapshot binding is stale or future"
            )
        for price in self.prices:
            age = (checked - price.market_time).total_seconds()
            if not 0 <= age <= self.maximum_price_age_seconds:
                raise ValueError(
                    "P8 underlying snapshot price is stale or future"
                )


@dataclass(frozen=True, slots=True)
class UnderlyingSnapshotTick:
    instrument_id: int
    evidence: UnderlyingSnapshotPrice
    selected_at: datetime
    option_market_time: datetime
    maximum_price_age_seconds: int
    contract: str = SNAPSHOT_TICK_CONTRACT

    def __post_init__(self) -> None:
        if type(self.instrument_id) is not int or self.instrument_id <= 0:
            raise ValueError("P8 underlying snapshot instrument is invalid")
        if self.contract != SNAPSHOT_TICK_CONTRACT:
            raise ValueError("P8 underlying snapshot tick contract changed")
        selected = _utc(self.selected_at, "underlying selected_at")
        option_time = _utc(
            self.option_market_time,
            "option market time",
        )
        object.__setattr__(self, "selected_at", selected)
        object.__setattr__(self, "option_market_time", option_time)
        self.require_usable(selected, option_time)

    @property
    def ticker(self) -> str:
        return self.evidence.ticker

    @property
    def session_date(self) -> date:
        return self.evidence.session_date

    @property
    def availability(self):
        from .advanced_options_rolling_state import RollingAvailability

        return RollingAvailability.READY

    @property
    def quote(self):
        return None

    @property
    def trade(self):
        return None

    @property
    def reason_codes(self) -> tuple[str, ...]:
        return ()

    @property
    def freshness_ms(self) -> int:
        return int(
            (
                self.selected_at - self.evidence.market_time
            ).total_seconds()
            * 1000
        )

    @property
    def source_identity_sha256(self) -> str:
        return self.evidence.sha256

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)

    def require_usable(
        self,
        checked_at: datetime,
        option_market_time: datetime,
    ) -> None:
        checked = _utc(checked_at, "underlying decision time")
        option_time = _utc(option_market_time, "option market time")
        price_age = (checked - self.evidence.market_time).total_seconds()
        option_skew = abs(
            (option_time - self.evidence.market_time).total_seconds()
        )
        if (
            self.evidence.market_time > checked
            or option_time > checked
            or not 0 <= price_age <= self.maximum_price_age_seconds
            or option_skew > self.maximum_price_age_seconds
        ):
            raise ValueError(
                "option event and underlying snapshot are not aligned"
            )


def binding_from_selection_capture(
    capture,
) -> UnderlyingSnapshotBinding:
    if (
        capture.selection.policy.maximum_snapshot_age_seconds
        != MAXIMUM_PRICE_AGE_SECONDS
    ):
        raise ValueError(
            "P8 underlying snapshot freshness policy changed"
        )
    evidence = capture.evidence
    selected_at = _utc(
        datetime.fromisoformat(str(evidence["selected_at_utc"])),
        "selection selected_at",
    )
    selection_valid_until = _utc(
        datetime.fromisoformat(str(evidence["valid_until_utc"])),
        "selection valid_until",
    )
    prices_by_ticker = {}
    for selected in sorted(
        capture.selection.selected, key=lambda row: row.contract_ticker
    ):
        raw = capture.selected_snapshots[selected.contract_ticker]
        underlying = raw.get("underlying_asset")
        if not isinstance(underlying, Mapping):
            raise ValueError("P8 underlying snapshot is absent")
        market_time = _nanoseconds(
            underlying.get("last_updated"),
            "underlying_asset.last_updated",
        )
        if (
            market_time - selected_at
        ).total_seconds() > MAXIMUM_PROVIDER_CLOCK_SKEW_SECONDS:
            raise ValueError("P8 underlying provider clock skew exceeded")
        price = UnderlyingSnapshotPrice(
            ticker=str(underlying.get("ticker", "")),
            session_date=selected_at.date(),
            price=_decimal(
                underlying.get("price", underlying.get("value")),
                "underlying_asset.price",
            ),
            market_time=market_time,
            timeframe=str(underlying.get("timeframe", "")),
            selected_contract_ticker=selected.contract_ticker,
            selected_contract_source_sha256=selected.source_sha256,
            underlying_payload_sha256=canonical_sha256(underlying),
        )
        previous = prices_by_ticker.get(price.ticker)
        if previous is not None:
            if (
                previous.market_time == price.market_time
                and previous.price != price.price
            ):
                raise ValueError("underlying prices diverge at the same market clock")
            if previous.market_time >= price.market_time:
                continue
        prices_by_ticker[price.ticker] = price
    prices = tuple(prices_by_ticker.values())
    valid_until = min(
        selection_valid_until,
        *(
            price.market_time
            + timedelta(seconds=MAXIMUM_PRICE_AGE_SECONDS)
            for price in prices
        ),
    )
    reviewed_at = max(
        selected_at,
        *(price.market_time for price in prices),
    )
    return UnderlyingSnapshotBinding(
        selection_sha256=str(evidence["selection_sha256"]),
        source_snapshot_sha256=str(
            evidence["source_snapshot_sha256"]
        ),
        reviewed_at=reviewed_at,
        valid_until=valid_until,
        prices=tuple(prices),
    )


def underlying_snapshot_price_payload(
    value: UnderlyingSnapshotPrice,
) -> dict[str, object]:
    return {
        "ticker": value.ticker,
        "session_date": value.session_date.isoformat(),
        "price": str(value.price),
        "market_time": value.market_time.isoformat(),
        "timeframe": value.timeframe,
        "selected_contract_ticker": value.selected_contract_ticker,
        "selected_contract_source_sha256": (
            value.selected_contract_source_sha256
        ),
        "underlying_payload_sha256": value.underlying_payload_sha256,
    }


def underlying_snapshot_price_from_payload(
    value: object,
) -> UnderlyingSnapshotPrice:
    expected = {
        "ticker",
        "session_date",
        "price",
        "market_time",
        "timeframe",
        "selected_contract_ticker",
        "selected_contract_source_sha256",
        "underlying_payload_sha256",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError("P8 underlying snapshot price fields changed")
    return UnderlyingSnapshotPrice(
        ticker=str(value["ticker"]),
        session_date=date.fromisoformat(str(value["session_date"])),
        price=Decimal(str(value["price"])),
        market_time=datetime.fromisoformat(str(value["market_time"])),
        timeframe=str(value["timeframe"]),
        selected_contract_ticker=str(
            value["selected_contract_ticker"]
        ),
        selected_contract_source_sha256=str(
            value["selected_contract_source_sha256"]
        ),
        underlying_payload_sha256=str(
            value["underlying_payload_sha256"]
        ),
    )


def underlying_snapshot_binding_payload(
    binding: UnderlyingSnapshotBinding,
    *,
    include_sha256: bool = True,
) -> dict[str, object]:
    if not isinstance(binding, UnderlyingSnapshotBinding):
        raise TypeError("P8 underlying snapshot binding is invalid")
    value = {
        "contract": SNAPSHOT_BINDING_CONTRACT,
        "price_source": PRICE_SOURCE,
        "selection_sha256": binding.selection_sha256,
        "source_snapshot_sha256": binding.source_snapshot_sha256,
        "reviewed_at": binding.reviewed_at.isoformat(),
        "valid_until": binding.valid_until.isoformat(),
        "maximum_price_age_seconds": (
            binding.maximum_price_age_seconds
        ),
        "prices": [
            underlying_snapshot_price_payload(row)
            for row in binding.prices
        ],
    }
    if include_sha256:
        value["binding_sha256"] = binding.binding_sha256
    return value


def underlying_snapshot_binding_from_payload(
    value: object,
) -> UnderlyingSnapshotBinding:
    if (
        not isinstance(value, dict)
        or value.get("contract") != SNAPSHOT_BINDING_CONTRACT
        or value.get("price_source") != PRICE_SOURCE
        or not isinstance(value.get("prices"), list)
    ):
        raise ValueError("P8 underlying snapshot binding fields changed")
    expected = {
        "contract",
        "price_source",
        "selection_sha256",
        "source_snapshot_sha256",
        "reviewed_at",
        "valid_until",
        "maximum_price_age_seconds",
        "prices",
        "binding_sha256",
    }
    if set(value) != expected:
        raise ValueError("P8 underlying snapshot binding fields changed")
    prices = [
        underlying_snapshot_price_from_payload(row)
        for row in value["prices"]
    ]
    return UnderlyingSnapshotBinding(
        selection_sha256=str(value["selection_sha256"]),
        source_snapshot_sha256=str(value["source_snapshot_sha256"]),
        reviewed_at=datetime.fromisoformat(str(value["reviewed_at"])),
        valid_until=datetime.fromisoformat(str(value["valid_until"])),
        prices=tuple(prices),
        maximum_price_age_seconds=int(
            value["maximum_price_age_seconds"]
        ),
        binding_sha256=str(value["binding_sha256"]),
    )
