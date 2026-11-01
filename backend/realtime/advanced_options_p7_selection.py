"""Activity-first contract selection for bounded P7 transport diagnostics."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
import re

from .contracts import canonical_sha256


UTC = timezone.utc
_OPTION_PATTERN = re.compile(
    r"^O:(?P<underlyer>[A-Z][A-Z0-9.-]{0,14})"
    r"(?P<expiration>\d{6})(?P<option_type>[CP])"
    r"(?P<strike>\d{8})$"
)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


class P7TransportSelectionStatus(str, Enum):
    READY = "READY"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True, slots=True)
class P7TransportSelectionPolicy:
    version: str = "advanced-options-p7-transport-activity-v1"
    underlyings: tuple[str, ...] = ("QQQ", "SPY")
    minimum_dte: int = 0
    maximum_dte: int = 2
    maximum_moneyness_fraction: Decimal = Decimal("0.05")
    minimum_day_volume: int = 100
    minimum_open_interest: int = 100
    maximum_relative_spread: Decimal = Decimal("0.25")
    maximum_snapshot_age_seconds: int = 300
    review_cadence: str = "BEFORE_EACH_RUN"

    def __post_init__(self) -> None:
        version = self.version.strip()
        underlyings = tuple(sorted({
            value.strip().upper()
            for value in self.underlyings
            if value.strip()
        }))
        if (
            not version
            or not underlyings
            or len(underlyings) != len(self.underlyings)
            or type(self.minimum_dte) is not int
            or type(self.maximum_dte) is not int
            or not 0 <= self.minimum_dte <= self.maximum_dte <= 60
            or not Decimal("0")
            < self.maximum_moneyness_fraction
            <= Decimal("0.10")
            or type(self.minimum_day_volume) is not int
            or self.minimum_day_volume <= 0
            or type(self.minimum_open_interest) is not int
            or self.minimum_open_interest <= 0
            or not Decimal("0")
            < self.maximum_relative_spread
            <= Decimal("1")
            or type(self.maximum_snapshot_age_seconds) is not int
            or self.maximum_snapshot_age_seconds <= 0
            or self.review_cadence != "BEFORE_EACH_RUN"
        ):
            raise ValueError("P7 transport selection policy is invalid")
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "underlyings", underlyings)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class P7TransportCandidate:
    contract_ticker: str
    underlyer: str
    expiration_date: date
    option_type: str
    strike: Decimal
    spot: Decimal
    bid: Decimal
    ask: Decimal
    day_volume: int
    open_interest: int | None
    calendar_dte: int
    market_data_time: datetime
    source_sha256: str

    def __post_init__(self) -> None:
        ticker = self.contract_ticker.strip().upper()
        underlyer = self.underlyer.strip().upper()
        option_type = self.option_type.strip().upper()
        market_data_time = _utc(
            self.market_data_time,
            "market_data_time",
        )
        match = _OPTION_PATTERN.fullmatch(ticker)
        if (
            match is None
            or match.group("underlyer") != underlyer
            or datetime.strptime(
                match.group("expiration"),
                "%y%m%d",
            ).date()
            != self.expiration_date
            or match.group("option_type") != option_type
            or Decimal(match.group("strike")) / Decimal("1000")
            != self.strike
            or not underlyer
            or option_type not in {"C", "P"}
            or not isinstance(self.expiration_date, date)
            or isinstance(self.expiration_date, datetime)
            or self.strike <= 0
            or self.spot <= 0
            or self.bid < 0
            or self.ask <= 0
            or self.ask < self.bid
            or type(self.day_volume) is not int
            or self.day_volume < 0
            or (
                self.open_interest is not None
                and (
                    type(self.open_interest) is not int
                    or self.open_interest < 0
                )
            )
            or type(self.calendar_dte) is not int
            or self.calendar_dte < 0
            or len(self.source_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.source_sha256
            )
        ):
            raise ValueError("P7 transport candidate is invalid")
        object.__setattr__(self, "contract_ticker", ticker)
        object.__setattr__(self, "underlyer", underlyer)
        object.__setattr__(self, "option_type", option_type)
        object.__setattr__(self, "market_data_time", market_data_time)

    @property
    def moneyness_fraction(self) -> Decimal:
        return abs(self.strike - self.spot) / self.spot

    @property
    def relative_spread(self) -> Decimal:
        midpoint = (self.bid + self.ask) / Decimal("2")
        return (self.ask - self.bid) / midpoint


@dataclass(frozen=True, slots=True)
class P7TransportSelection:
    status: P7TransportSelectionStatus
    policy: P7TransportSelectionPolicy
    reviewed_at: datetime
    valid_until: datetime | None
    selected: tuple[P7TransportCandidate, ...]
    source_sha256: str
    selection_sha256: str
    reason_codes: tuple[str, ...]

    @property
    def contracts(self) -> tuple[str, ...]:
        return tuple(row.contract_ticker for row in self.selected)


def p7_transport_candidate_payload(
    candidate: P7TransportCandidate,
) -> dict[str, object]:
    return {
        "contract_ticker": candidate.contract_ticker,
        "underlyer": candidate.underlyer,
        "expiration_date": candidate.expiration_date.isoformat(),
        "option_type": candidate.option_type,
        "strike": str(candidate.strike),
        "spot": str(candidate.spot),
        "bid": str(candidate.bid),
        "ask": str(candidate.ask),
        "day_volume": candidate.day_volume,
        "open_interest": candidate.open_interest,
        "calendar_dte": candidate.calendar_dte,
        "market_data_time": candidate.market_data_time.isoformat(),
        "source_sha256": candidate.source_sha256,
    }


def _policy_payload(
    policy: P7TransportSelectionPolicy,
) -> dict[str, object]:
    return {
        "version": policy.version,
        "underlyings": list(policy.underlyings),
        "minimum_dte": policy.minimum_dte,
        "maximum_dte": policy.maximum_dte,
        "maximum_moneyness_fraction": str(
            policy.maximum_moneyness_fraction
        ),
        "minimum_day_volume": policy.minimum_day_volume,
        "minimum_open_interest": policy.minimum_open_interest,
        "maximum_relative_spread": str(
            policy.maximum_relative_spread
        ),
        "maximum_snapshot_age_seconds": (
            policy.maximum_snapshot_age_seconds
        ),
        "review_cadence": policy.review_cadence,
    }


def _policy_from_payload(
    value: object,
) -> P7TransportSelectionPolicy:
    if not isinstance(value, dict) or set(value) != {
        "version",
        "underlyings",
        "minimum_dte",
        "maximum_dte",
        "maximum_moneyness_fraction",
        "minimum_day_volume",
        "minimum_open_interest",
        "maximum_relative_spread",
        "maximum_snapshot_age_seconds",
        "review_cadence",
    }:
        raise ValueError("P7 transport policy fields are not exact")
    policy = P7TransportSelectionPolicy(
        version=str(value["version"]),
        underlyings=tuple(
            str(item) for item in value["underlyings"]
        ),
        minimum_dte=int(value["minimum_dte"]),
        maximum_dte=int(value["maximum_dte"]),
        maximum_moneyness_fraction=Decimal(
            str(value["maximum_moneyness_fraction"])
        ),
        minimum_day_volume=int(value["minimum_day_volume"]),
        minimum_open_interest=int(value["minimum_open_interest"]),
        maximum_relative_spread=Decimal(
            str(value["maximum_relative_spread"])
        ),
        maximum_snapshot_age_seconds=int(
            value["maximum_snapshot_age_seconds"]
        ),
        review_cadence=str(value["review_cadence"]),
    )
    if _policy_payload(policy) != value:
        raise ValueError("P7 transport policy payload changed")
    return policy


def p7_transport_selection_payload(
    selection: P7TransportSelection,
) -> dict[str, object]:
    return {
        "contract_version": "advanced-options-p7-transport-selection-v1",
        "status": selection.status.value,
        "policy": _policy_payload(selection.policy),
        "policy_sha256": selection.policy.sha256,
        "reviewed_at": selection.reviewed_at.isoformat(),
        "valid_until": (
            None
            if selection.valid_until is None
            else selection.valid_until.isoformat()
        ),
        "selected": [
            p7_transport_candidate_payload(row)
            for row in selection.selected
        ],
        "source_sha256": selection.source_sha256,
        "selection_sha256": selection.selection_sha256,
        "reason_codes": list(selection.reason_codes),
    }


def p7_transport_candidate_from_payload(
    value: object,
) -> P7TransportCandidate:
    if not isinstance(value, dict) or set(value) != {
        "contract_ticker",
        "underlyer",
        "expiration_date",
        "option_type",
        "strike",
        "spot",
        "bid",
        "ask",
        "day_volume",
        "open_interest",
        "calendar_dte",
        "market_data_time",
        "source_sha256",
    }:
        raise ValueError("P7 transport candidate fields are not exact")
    return P7TransportCandidate(
        contract_ticker=str(value["contract_ticker"]),
        underlyer=str(value["underlyer"]),
        expiration_date=date.fromisoformat(
            str(value["expiration_date"])
        ),
        option_type=str(value["option_type"]),
        strike=Decimal(str(value["strike"])),
        spot=Decimal(str(value["spot"])),
        bid=Decimal(str(value["bid"])),
        ask=Decimal(str(value["ask"])),
        day_volume=int(value["day_volume"]),
        open_interest=(
            None
            if value.get("open_interest") is None
            else int(value["open_interest"])
        ),
        calendar_dte=int(value["calendar_dte"]),
        market_data_time=datetime.fromisoformat(
            str(value["market_data_time"]).replace("Z", "+00:00")
        ),
        source_sha256=str(value["source_sha256"]),
    )


def validate_p7_transport_selection_payload(
    value: object,
    *,
    required_at: datetime | None = None,
) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != {
        "contract_version",
        "status",
        "policy",
        "policy_sha256",
        "reviewed_at",
        "valid_until",
        "selected",
        "source_sha256",
        "selection_sha256",
        "reason_codes",
    }:
        raise ValueError("P7 transport selection fields are not exact")
    policy = _policy_from_payload(value.get("policy"))
    if (
        value["contract_version"]
        != "advanced-options-p7-transport-selection-v1"
        or value["status"] != P7TransportSelectionStatus.READY.value
        or value["policy"] != _policy_payload(policy)
        or value["policy_sha256"] != policy.sha256
        or value["reason_codes"] != []
        or not isinstance(value["selected"], list)
        or len(value["selected"]) != len(policy.underlyings)
    ):
        raise ValueError("P7 transport selection is not ready")
    selected = tuple(
        p7_transport_candidate_from_payload(row)
        for row in value["selected"]
    )
    if tuple(row.underlyer for row in selected) != policy.underlyings:
        raise ValueError("P7 transport selection underlyings changed")
    reviewed = _utc(
        datetime.fromisoformat(
            str(value["reviewed_at"]).replace("Z", "+00:00")
        ),
        "reviewed_at",
    )
    valid_until = _utc(
        datetime.fromisoformat(
            str(value["valid_until"]).replace("Z", "+00:00")
        ),
        "valid_until",
    )
    expected_valid_until = min(
        row.market_data_time
        + timedelta(seconds=policy.maximum_snapshot_age_seconds)
        for row in selected
    )
    if valid_until != expected_valid_until:
        raise ValueError("P7 transport selection validity changed")
    if required_at is not None:
        required = _utc(required_at, "required_at")
        if reviewed > required or valid_until < required:
            raise ValueError("P7 transport selection is stale")
    source_sha256 = str(value["source_sha256"])
    if len(source_sha256) != 64 or any(
        character not in "0123456789abcdef"
        for character in source_sha256
    ):
        raise ValueError("P7 transport selection source digest is invalid")
    selection_sha256 = canonical_sha256({
        "status": P7TransportSelectionStatus.READY,
        "policy_sha256": policy.sha256,
        "reviewed_at": reviewed,
        "valid_until": valid_until,
        "source_sha256": source_sha256,
        "selected": selected,
        "reason_codes": (),
    })
    if value["selection_sha256"] != selection_sha256:
        raise ValueError("P7 transport selection digest changed")
    return p7_transport_selection_payload(
        P7TransportSelection(
            P7TransportSelectionStatus.READY,
            policy,
            reviewed,
            valid_until,
            selected,
            source_sha256,
            selection_sha256,
            (),
        )
    )


def _rank(candidate: P7TransportCandidate) -> tuple:
    return (
        -candidate.day_volume,
        -(
            -1
            if candidate.open_interest is None
            else candidate.open_interest
        ),
        candidate.moneyness_fraction,
        candidate.relative_spread,
        candidate.calendar_dte,
        candidate.expiration_date,
        candidate.option_type,
        candidate.contract_ticker,
    )


def select_p7_transport_contracts(
    candidates: tuple[P7TransportCandidate, ...],
    *,
    reviewed_at: datetime,
    policy: P7TransportSelectionPolicy | None = None,
) -> P7TransportSelection:
    selected_policy = policy or P7TransportSelectionPolicy()
    reviewed = _utc(reviewed_at, "reviewed_at")
    ordered = tuple(sorted(
        candidates,
        key=lambda row: (
            row.underlyer,
            row.contract_ticker,
            row.market_data_time,
        ),
    ))
    if len({row.contract_ticker for row in ordered}) != len(ordered):
        raise ValueError("P7 transport candidates contain duplicate contracts")
    if any(row.market_data_time > reviewed for row in ordered):
        raise ValueError("P7 transport candidate is from the future")

    source_sha256 = canonical_sha256(tuple(
        (
            row.contract_ticker,
            row.market_data_time,
            row.source_sha256,
        )
        for row in ordered
    ))
    selected = []
    reasons = []
    for underlyer in selected_policy.underlyings:
        eligible = tuple(
            row
            for row in ordered
            if row.underlyer == underlyer
            and selected_policy.minimum_dte
            <= row.calendar_dte
            <= selected_policy.maximum_dte
            and reviewed - row.market_data_time
            <= timedelta(
                seconds=selected_policy.maximum_snapshot_age_seconds
            )
            and row.moneyness_fraction
            <= selected_policy.maximum_moneyness_fraction
            and row.day_volume >= selected_policy.minimum_day_volume
            and row.open_interest is not None
            and row.open_interest
            >= selected_policy.minimum_open_interest
            and row.relative_spread
            <= selected_policy.maximum_relative_spread
        )
        if not eligible:
            reasons.append(f"{underlyer}_ACTIVE_CONTRACT_UNAVAILABLE")
            continue
        selected.append(min(eligible, key=_rank))

    selected_rows = tuple(sorted(
        selected,
        key=lambda row: row.underlyer,
    ))
    status = (
        P7TransportSelectionStatus.READY
        if len(selected_rows) == len(selected_policy.underlyings)
        else P7TransportSelectionStatus.BLOCKED
    )
    valid_until = (
        min(
            row.market_data_time
            + timedelta(
                seconds=selected_policy.maximum_snapshot_age_seconds
            )
            for row in selected_rows
        )
        if status is P7TransportSelectionStatus.READY
        else None
    )
    selection_sha256 = canonical_sha256({
        "status": status,
        "policy_sha256": selected_policy.sha256,
        "reviewed_at": reviewed,
        "valid_until": valid_until,
        "source_sha256": source_sha256,
        "selected": selected_rows,
        "reason_codes": tuple(reasons),
    })
    return P7TransportSelection(
        status,
        selected_policy,
        reviewed,
        valid_until,
        selected_rows,
        source_sha256,
        selection_sha256,
        tuple(reasons),
    )
