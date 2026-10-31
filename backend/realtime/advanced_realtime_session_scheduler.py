"""Immutable daily contract and coordinator for Advanced Equity and Options."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from options.calendar import OptionExchangeCalendar

from .acquisition_ledger import ProviderRequestLane
from .advanced_options_v3_authority import SessionAcquisitionAuthorization
from .contracts import EQUITY_VALIDATION_UNIVERSE, canonical_sha256
from .provider_budget import ProviderBudgetScope
from .session_windows import advanced_session_windows


UTC = timezone.utc
ADVANCED_SESSION_SCHEDULER_VERSION = (
    "advanced-realtime-session-scheduler-v2"
)
ADVANCED_SESSION_PROVIDER_CONTRACT_VERSION = (
    "advanced-options-exact-snapshot-budget-v1"
)
ADVANCED_SESSION_DECISION_POLICY_VERSION = (
    "advanced-equity-options-e-o-v1"
)
ADVANCED_SESSION_WINDOW_MINUTES = 5
ADVANCED_SESSION_MAXIMUM_E_O_SECONDS = 300
ADVANCED_SESSION_FINALITY_DELAY_SECONDS = 5
ADVANCED_SESSION_COMPLETION_BUDGET_SECONDS = 300
ADVANCED_SESSION_SERVING_LAG_SECONDS = 30
ADVANCED_SESSION_OPTIONS_ENDPOINT = "/v3/snapshot"
ADVANCED_SESSION_OPTIONS_REQUESTS_PER_CYCLE = 1
ADVANCED_SESSION_OPTIONS_LANE = ProviderRequestLane.OPTIONS_ALIGNMENT
ADVANCED_SESSION_OPTIONS_UNDERLYINGS = ("QQQ", "SPY")
ADVANCED_SESSION_EQUITY_SYMBOLS = (
    EQUITY_VALIDATION_UNIVERSE.equity_tickers
)


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _sha256(value: str, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


def _text(value: str, name: str, *, maximum: int = 160) -> str:
    if not isinstance(value, str) or not 1 <= len(value.strip()) <= maximum:
        raise ValueError(f"{name} is invalid")
    return value.strip()


@dataclass(frozen=True, slots=True)
class AdvancedSessionDecisionWindow:
    index: int
    equity_cutoff: datetime
    equity_available_after: datetime
    equity_deadline: datetime
    latest_options_cutoff: datetime

    def __post_init__(self) -> None:
        equity = _utc(self.equity_cutoff, "equity_cutoff")
        available = _utc(
            self.equity_available_after,
            "equity_available_after",
        )
        deadline = _utc(self.equity_deadline, "equity_deadline")
        latest = _utc(
            self.latest_options_cutoff,
            "latest_options_cutoff",
        )
        if (
            type(self.index) is not int
            or self.index < 1
            or not equity < available <= deadline <= latest
            or (latest - equity).total_seconds()
            != ADVANCED_SESSION_MAXIMUM_E_O_SECONDS
        ):
            raise ValueError("Advanced session decision window is invalid")
        object.__setattr__(self, "equity_cutoff", equity)
        object.__setattr__(self, "equity_available_after", available)
        object.__setattr__(self, "equity_deadline", deadline)
        object.__setattr__(self, "latest_options_cutoff", latest)

    @property
    def key(self) -> str:
        return (
            f"advanced-5m-{self.equity_cutoff:%Y%m%dT%H%M%SZ}"
        )


def build_advanced_session_windows(
    session_date: date,
    *,
    calendar: OptionExchangeCalendar | None = None,
) -> tuple[AdvancedSessionDecisionWindow, ...]:
    exchange = calendar or OptionExchangeCalendar("XNYS")
    windows = advanced_session_windows(
        session_date,
        finality_delay_seconds=(
            ADVANCED_SESSION_FINALITY_DELAY_SECONDS
        ),
        completion_budget_seconds=(
            ADVANCED_SESSION_COMPLETION_BUDGET_SECONDS
        ),
        serving_lag_seconds=ADVANCED_SESSION_SERVING_LAG_SECONDS,
        calendar=exchange,
    )
    return tuple(
        AdvancedSessionDecisionWindow(
            index=index,
            equity_cutoff=window.cutoff,
            equity_available_after=window.available_after,
            equity_deadline=min(
                window.deadline,
                window.cutoff
                + timedelta(
                    seconds=ADVANCED_SESSION_MAXIMUM_E_O_SECONDS
                ),
            ),
            latest_options_cutoff=(
                window.cutoff
                + timedelta(
                    seconds=ADVANCED_SESSION_MAXIMUM_E_O_SECONDS
                )
            ),
        )
        for index, window in enumerate(windows, start=1)
    )


@dataclass(frozen=True, slots=True)
class AdvancedSessionProviderContract:
    budget: ProviderBudgetScope
    response_byte_ceiling: int
    contract_tickers: tuple[str, ...]
    endpoint_path: str = ADVANCED_SESSION_OPTIONS_ENDPOINT
    policy_version: str = ADVANCED_SESSION_PROVIDER_CONTRACT_VERSION

    def __post_init__(self) -> None:
        tickers = tuple(sorted(
            _text(value, "contract ticker", maximum=48).upper()
            for value in self.contract_tickers
        ))
        if (
            not isinstance(self.budget, ProviderBudgetScope)
            or self.budget.lanes
            != frozenset({ADVANCED_SESSION_OPTIONS_LANE})
            or type(self.response_byte_ceiling) is not int
            or self.response_byte_ceiling <= 0
            or self.budget.decoded_byte_ceiling
            != self.budget.request_ceiling
            * self.response_byte_ceiling
            or len(tickers) != 4
            or len(set(tickers)) != 4
            or {
                (ticker[2:5], ticker[11:12])
                for ticker in tickers
                if ticker.startswith("O:") and len(ticker) >= 12
            }
            != {
                ("QQQ", "C"),
                ("QQQ", "P"),
                ("SPY", "C"),
                ("SPY", "P"),
            }
            or any(not ticker.startswith("O:") for ticker in tickers)
            or self.endpoint_path != ADVANCED_SESSION_OPTIONS_ENDPOINT
            or self.policy_version
            != ADVANCED_SESSION_PROVIDER_CONTRACT_VERSION
        ):
            raise ValueError(
                "Advanced session exact-snapshot provider contract is invalid"
            )
        object.__setattr__(self, "contract_tickers", tickers)

    @property
    def sha256(self) -> str:
        return canonical_sha256({
            "policy_version": self.policy_version,
            "budget_sha256": self.budget.sha256,
            "response_byte_ceiling": self.response_byte_ceiling,
            "contract_tickers": self.contract_tickers,
            "endpoint_path": self.endpoint_path,
            "query": {
                "ticker.any_of": ",".join(self.contract_tickers),
                "type": "options",
                "limit": len(self.contract_tickers),
                "sort": "ticker",
                "order": "asc",
            },
            "pagination_allowed": False,
            "requests_per_cycle": (
                ADVANCED_SESSION_OPTIONS_REQUESTS_PER_CYCLE
            ),
        })


@dataclass(frozen=True, slots=True)
class AdvancedRealtimeSessionAuthorization:
    authorization_id: UUID
    authorization_sha256: str
    authorization_reference: str
    session_date: date
    prepared_at: datetime
    acquisition_start: datetime
    market_capture_stop: datetime
    decision_stop: datetime
    equity_authorization_id: UUID
    equity_cohort_id: UUID
    options_session_authorization_id: UUID
    options_session_authorization_sha256: str
    options_cohort_id: UUID
    package_sha256: str
    bundle_sha256: str
    provider_contract: AdvancedSessionProviderContract
    windows: tuple[AdvancedSessionDecisionWindow, ...]

    def __post_init__(self) -> None:
        prepared = _utc(self.prepared_at, "prepared_at")
        start = _utc(self.acquisition_start, "acquisition_start")
        capture_stop = _utc(
            self.market_capture_stop,
            "market_capture_stop",
        )
        decision_stop = _utc(self.decision_stop, "decision_stop")
        exchange = OptionExchangeCalendar("XNYS")
        session_open = exchange.session_open(self.session_date)
        session_close = exchange.session_close(self.session_date)
        expected_windows = build_advanced_session_windows(
            self.session_date,
            calendar=exchange,
        )
        if (
            not all(
                isinstance(value, UUID) and value.int
                for value in (
                    self.authorization_id,
                    self.equity_authorization_id,
                    self.equity_cohort_id,
                    self.options_session_authorization_id,
                    self.options_cohort_id,
                )
            )
            or not prepared < start < session_open
            or capture_stop != session_close
            or decision_stop
            != session_close
            + timedelta(
                seconds=ADVANCED_SESSION_MAXIMUM_E_O_SECONDS
            )
            or self.windows != expected_windows
            or not self.windows
            or self.provider_contract.budget.session_date
            != self.session_date
            or self.provider_contract.budget.account_alias
            != "MASSIVE_ADVANCED"
            or self.provider_contract.budget.authorization_reference
            != self.authorization_reference
            or self.provider_contract.budget.request_ceiling
            != len(self.windows)
            or self.provider_contract.budget.authorized_until
            != decision_stop
            or self.windows[-1].equity_cutoff != capture_stop
            or self.windows[-1].latest_options_cutoff
            != decision_stop
        ):
            raise ValueError(
                "Advanced realtime session scheduler scope is invalid"
            )
        object.__setattr__(
            self,
            "authorization_reference",
            _text(
                self.authorization_reference,
                "authorization_reference",
            ),
        )
        object.__setattr__(self, "prepared_at", prepared)
        object.__setattr__(self, "acquisition_start", start)
        object.__setattr__(
            self,
            "market_capture_stop",
            capture_stop,
        )
        object.__setattr__(self, "decision_stop", decision_stop)
        for name in (
            "options_session_authorization_sha256",
            "package_sha256",
            "bundle_sha256",
        ):
            object.__setattr__(
                self,
                name,
                _sha256(getattr(self, name), name),
            )
        expected = canonical_sha256(
            advanced_realtime_session_authorization_payload(
                self,
                include_identity=False,
            )
        )
        expected_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-session-scheduler:{expected}",
        )
        if (
            self.authorization_sha256 != expected
            or self.authorization_id != expected_id
        ):
            raise ValueError(
                "Advanced realtime session scheduler identity changed"
            )


def _window_payload(
    window: AdvancedSessionDecisionWindow,
) -> dict[str, object]:
    return {
        "index": window.index,
        "key": window.key,
        "equity_cutoff": window.equity_cutoff.isoformat(),
        "equity_available_after": (
            window.equity_available_after.isoformat()
        ),
        "equity_deadline": window.equity_deadline.isoformat(),
        "latest_options_cutoff": (
            window.latest_options_cutoff.isoformat()
        ),
    }


def advanced_realtime_session_authorization_payload(
    authority: AdvancedRealtimeSessionAuthorization,
    *,
    include_identity: bool = True,
) -> dict[str, object]:
    provider = authority.provider_contract
    value: dict[str, object] = {
        "schema": ADVANCED_SESSION_SCHEDULER_VERSION,
        "authorization_reference": authority.authorization_reference,
        "session_date": authority.session_date.isoformat(),
        "exchange": "XNYS",
        "prepared_at": authority.prepared_at.isoformat(),
        "acquisition_start": authority.acquisition_start.isoformat(),
        "market_capture_stop": authority.market_capture_stop.isoformat(),
        "decision_stop": authority.decision_stop.isoformat(),
        "run_scope": {
            "kind": "FULL_EXCHANGE_SESSION",
            "window_count": len(authority.windows),
            "first_window_index": authority.windows[0].index,
            "last_window_index": authority.windows[-1].index,
        },
        "equity": {
            "target": len(ADVANCED_SESSION_EQUITY_SYMBOLS),
            "symbols": list(ADVANCED_SESSION_EQUITY_SYMBOLS),
            "authorization_id": str(
                authority.equity_authorization_id
            ),
            "cohort_id": str(authority.equity_cohort_id),
            "feed": "STOCKS",
            "channels": ["AM"],
        },
        "options": {
            "underlyings": list(
                ADVANCED_SESSION_OPTIONS_UNDERLYINGS
            ),
            "contract_tickers": list(provider.contract_tickers),
            "session_authorization_id": str(
                authority.options_session_authorization_id
            ),
            "session_authorization_sha256": (
                authority.options_session_authorization_sha256
            ),
            "cohort_id": str(authority.options_cohort_id),
            "package_sha256": authority.package_sha256,
            "bundle_sha256": authority.bundle_sha256,
            "feed": "OPTIONS",
            "channels": ["T", "Q"],
            "nine_underlying_expansion_authorized": False,
        },
        "provider_contract": {
            "version": provider.policy_version,
            "sha256": provider.sha256,
            "budget": {
                "scope_sha256": provider.budget.sha256,
                "policy_version": provider.budget.policy_version,
                "account_alias": provider.budget.account_alias,
                "session_date": (
                    provider.budget.session_date.isoformat()
                ),
                "authorization_reference": (
                    provider.budget.authorization_reference
                ),
                "provider_authorization_reference": (
                    provider.budget.provider_authorization_reference
                ),
                "expected_cost": provider.budget.expected_cost,
                "request_ceiling": (
                    provider.budget.request_ceiling
                ),
                "decoded_byte_ceiling": (
                    provider.budget.decoded_byte_ceiling
                ),
                "lanes": sorted(
                    lane.value for lane in provider.budget.lanes
                ),
                "authorized_until": (
                    provider.budget.authorized_until.isoformat()
                ),
            },
            "response_byte_ceiling": provider.response_byte_ceiling,
            "endpoint_path": provider.endpoint_path,
            "query": {
                "ticker.any_of": ",".join(
                    provider.contract_tickers
                ),
                "type": "options",
                "limit": 4,
                "sort": "ticker",
                "order": "asc",
            },
            "pagination_allowed": False,
            "requests_per_cycle": 1,
        },
        "decision_policy": {
            "version": ADVANCED_SESSION_DECISION_POLICY_VERSION,
            "interval_minutes": ADVANCED_SESSION_WINDOW_MINUTES,
            "maximum_e_o_seconds": (
                ADVANCED_SESSION_MAXIMUM_E_O_SECONDS
            ),
            "windows": [
                _window_payload(window)
                for window in authority.windows
            ],
        },
        "authority": {
            "start_existing_equity_residents": True,
            "start_existing_options_residents": True,
            "provider_request": True,
            "new_provider_socket_scope": False,
            "window_cycle": True,
            "canonical_publication": True,
            "stored_read_pin": True,
            "dispatch": False,
            "delivery": False,
            "broker": False,
            "trading": False,
        },
    }
    if include_identity:
        value.update({
            "authorization_id": str(authority.authorization_id),
            "authorization_sha256": authority.authorization_sha256,
        })
    return value


def advanced_realtime_session_authorization_from_payload(
    value: object,
) -> AdvancedRealtimeSessionAuthorization:
    if not isinstance(value, Mapping):
        raise TypeError("scheduler authorization payload is invalid")
    if value.get("schema") != ADVANCED_SESSION_SCHEDULER_VERSION:
        raise ValueError("scheduler authorization schema changed")
    equity = value.get("equity")
    options = value.get("options")
    provider = value.get("provider_contract")
    decision = value.get("decision_policy")
    budget = (
        provider.get("budget")
        if isinstance(provider, Mapping)
        else None
    )
    window_values = (
        decision.get("windows")
        if isinstance(decision, Mapping)
        else None
    )
    if (
        not isinstance(equity, Mapping)
        or not isinstance(options, Mapping)
        or not isinstance(provider, Mapping)
        or not isinstance(budget, Mapping)
        or not isinstance(window_values, list)
        or provider.get("version")
        != ADVANCED_SESSION_PROVIDER_CONTRACT_VERSION
        or budget.get("lanes")
        != [ADVANCED_SESSION_OPTIONS_LANE.value]
    ):
        raise ValueError(
            "scheduler authorization dependency scope changed"
        )
    provider_scope = ProviderBudgetScope(
        account_alias=str(budget["account_alias"]),
        session_date=date.fromisoformat(
            str(budget["session_date"])
        ),
        authorization_reference=str(
            budget["authorization_reference"]
        ),
        provider_authorization_reference=str(
            budget["provider_authorization_reference"]
        ),
        expected_cost=str(budget["expected_cost"]),
        request_ceiling=int(budget["request_ceiling"]),
        decoded_byte_ceiling=int(
            budget["decoded_byte_ceiling"]
        ),
        lanes=frozenset({
            ProviderRequestLane(str(budget["lanes"][0])),
        }),
        authorized_until=datetime.fromisoformat(
            str(budget["authorized_until"])
        ),
        policy_version=str(budget["policy_version"]),
    )
    provider_contract = AdvancedSessionProviderContract(
        budget=provider_scope,
        response_byte_ceiling=int(
            provider["response_byte_ceiling"]
        ),
        contract_tickers=tuple(
            str(ticker)
            for ticker in options["contract_tickers"]
        ),
        endpoint_path=str(provider["endpoint_path"]),
        policy_version=str(provider["version"]),
    )
    if (
        provider.get("sha256") != provider_contract.sha256
        or budget.get("scope_sha256") != provider_scope.sha256
    ):
        raise ValueError("scheduler provider budget identity changed")
    windows = tuple(
        AdvancedSessionDecisionWindow(
            index=int(row["index"]),
            equity_cutoff=datetime.fromisoformat(
                str(row["equity_cutoff"])
            ),
            equity_available_after=datetime.fromisoformat(
                str(row["equity_available_after"])
            ),
            equity_deadline=datetime.fromisoformat(
                str(row["equity_deadline"])
            ),
            latest_options_cutoff=datetime.fromisoformat(
                str(row["latest_options_cutoff"])
            ),
        )
        for row in window_values
        if isinstance(row, Mapping)
    )
    authority = AdvancedRealtimeSessionAuthorization(
        authorization_id=UUID(str(value["authorization_id"])),
        authorization_sha256=str(value["authorization_sha256"]),
        authorization_reference=str(
            value["authorization_reference"]
        ),
        session_date=date.fromisoformat(str(value["session_date"])),
        prepared_at=datetime.fromisoformat(str(value["prepared_at"])),
        acquisition_start=datetime.fromisoformat(
            str(value["acquisition_start"])
        ),
        market_capture_stop=datetime.fromisoformat(
            str(value["market_capture_stop"])
        ),
        decision_stop=datetime.fromisoformat(
            str(value["decision_stop"])
        ),
        equity_authorization_id=UUID(
            str(equity["authorization_id"])
        ),
        equity_cohort_id=UUID(str(equity["cohort_id"])),
        options_session_authorization_id=UUID(
            str(options["session_authorization_id"])
        ),
        options_session_authorization_sha256=str(
            options["session_authorization_sha256"]
        ),
        options_cohort_id=UUID(str(options["cohort_id"])),
        package_sha256=str(options["package_sha256"]),
        bundle_sha256=str(options["bundle_sha256"]),
        provider_contract=provider_contract,
        windows=windows,
    )
    if advanced_realtime_session_authorization_payload(
        authority
    ) != value:
        raise ValueError("scheduler authorization payload changed")
    return authority


def build_advanced_realtime_session_authorization(
    *,
    authorization_reference: str,
    session_date: date,
    prepared_at: datetime,
    acquisition_start: datetime,
    equity_authorization_id: UUID,
    equity_cohort_id: UUID,
    options_session_authority: SessionAcquisitionAuthorization,
    package_sha256: str,
    bundle_sha256: str,
    provider_contract: AdvancedSessionProviderContract,
) -> AdvancedRealtimeSessionAuthorization:
    exchange = OptionExchangeCalendar("XNYS")
    capture_stop = exchange.session_close(session_date)
    decision_stop = capture_stop + timedelta(
        seconds=ADVANCED_SESSION_MAXIMUM_E_O_SECONDS
    )
    if (
        not isinstance(
            options_session_authority,
            SessionAcquisitionAuthorization,
        )
        or options_session_authority.session_date != session_date
        or options_session_authority.acquisition_stop < decision_stop
    ):
        raise ValueError(
            "Advanced session Options authority does not cover the decision drain"
        )
    values = {
        "authorization_reference": _text(
            authorization_reference,
            "authorization_reference",
        ),
        "session_date": session_date,
        "prepared_at": prepared_at,
        "acquisition_start": acquisition_start,
        "market_capture_stop": capture_stop,
        "decision_stop": decision_stop,
        "equity_authorization_id": equity_authorization_id,
        "equity_cohort_id": equity_cohort_id,
        "options_session_authorization_id": (
            options_session_authority.authorization_id
        ),
        "options_session_authorization_sha256": (
            options_session_authority.authorization_sha256
        ),
        "options_cohort_id": options_session_authority.cohort_id,
        "package_sha256": package_sha256,
        "bundle_sha256": bundle_sha256,
        "provider_contract": provider_contract,
        "windows": build_advanced_session_windows(
            session_date,
            calendar=exchange,
        ),
    }
    unsigned = AdvancedRealtimeSessionAuthorization.__new__(
        AdvancedRealtimeSessionAuthorization
    )
    object.__setattr__(unsigned, "authorization_id", UUID(int=1))
    object.__setattr__(
        unsigned,
        "authorization_sha256",
        "0" * 64,
    )
    for name, value in values.items():
        object.__setattr__(unsigned, name, value)
    digest = canonical_sha256(
        advanced_realtime_session_authorization_payload(
            unsigned,
            include_identity=False,
        )
    )
    return AdvancedRealtimeSessionAuthorization(
        authorization_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-session-scheduler:{digest}",
        ),
        authorization_sha256=digest,
        **values,
    )


@dataclass(frozen=True, slots=True)
class SchedulerEquityReceipt:
    receipt_id: UUID
    cutoff: datetime
    created_at: datetime
    input_sha256: str

    def __post_init__(self) -> None:
        cutoff = _utc(self.cutoff, "receipt cutoff")
        created = _utc(self.created_at, "receipt created_at")
        if (
            not isinstance(self.receipt_id, UUID)
            or not self.receipt_id.int
            or created < cutoff
            or _sha256(
                self.input_sha256,
                "receipt input_sha256",
            )
            != self.input_sha256
        ):
            raise ValueError("scheduler Equity receipt is invalid")
        object.__setattr__(self, "cutoff", cutoff)
        object.__setattr__(self, "created_at", created)


@dataclass(frozen=True, slots=True)
class SchedulerOptionsCapture:
    evidence_sha256: str
    reviewed_at: datetime
    contract_tickers: tuple[str, ...]
    request_count: int
    pagination_requests: int
    options_cutoff: datetime | None = None

    def __post_init__(self) -> None:
        reviewed = _utc(self.reviewed_at, "capture reviewed_at")
        options_cutoff = (
            reviewed
            if self.options_cutoff is None
            else _utc(self.options_cutoff, "capture options_cutoff")
        )
        if (
            _sha256(self.evidence_sha256, "evidence_sha256")
            != self.evidence_sha256
            or self.request_count
            != ADVANCED_SESSION_OPTIONS_REQUESTS_PER_CYCLE
            or self.pagination_requests != 0
            or len(self.contract_tickers) != 4
            or len(set(self.contract_tickers)) != 4
            or not reviewed <= options_cutoff
            or (options_cutoff - reviewed).total_seconds() > 300
        ):
            raise ValueError("scheduler Options capture is invalid")
        object.__setattr__(self, "reviewed_at", reviewed)
        object.__setattr__(self, "options_cutoff", options_cutoff)
        object.__setattr__(
            self,
            "contract_tickers",
            tuple(sorted(self.contract_tickers)),
        )


@dataclass(frozen=True, slots=True)
class SchedulerPublishedCycle:
    target_window_id: UUID
    release_id: UUID
    release_cutoff: datetime
    read_pin_release_id: UUID
    stored_reader_count: int
    stock_alert_batch_id: UUID
    stock_alert_batch_sha256: str
    stock_alert_evaluation_count: int
    stock_alert_unavailable_count: int
    stock_alert_reason_codes_exact: bool

    def __post_init__(self) -> None:
        cutoff = _utc(self.release_cutoff, "release_cutoff")
        if (
            not all(
                isinstance(value, UUID) and value.int
                for value in (
                    self.target_window_id,
                    self.release_id,
                    self.read_pin_release_id,
                    self.stock_alert_batch_id,
                )
            )
            or self.release_id != self.read_pin_release_id
            or self.stored_reader_count != 4
            or _sha256(
                self.stock_alert_batch_sha256,
                "stock_alert_batch_sha256",
            )
            != self.stock_alert_batch_sha256
            or self.stock_alert_evaluation_count != 9
            or self.stock_alert_unavailable_count != 9
            or self.stock_alert_reason_codes_exact is not True
        ):
            raise ValueError("scheduler publication/read-pin result is invalid")
        object.__setattr__(self, "release_cutoff", cutoff)


class AdvancedSessionSchedulerPort(Protocol):
    def existing_cycle(
        self,
        window: AdvancedSessionDecisionWindow,
    ) -> SchedulerPublishedCycle | None: ...

    def load_equity_receipt(
        self,
        window: AdvancedSessionDecisionWindow,
    ) -> SchedulerEquityReceipt | None: ...

    def capture_options(
        self,
        window: AdvancedSessionDecisionWindow,
        receipt: SchedulerEquityReceipt,
    ) -> SchedulerOptionsCapture: ...

    def run_cycle(
        self,
        window: AdvancedSessionDecisionWindow,
        receipt: SchedulerEquityReceipt,
        capture: SchedulerOptionsCapture,
    ) -> SchedulerPublishedCycle: ...


class AdvancedRealtimeSessionScheduler:
    def __init__(
        self,
        authority: AdvancedRealtimeSessionAuthorization,
        port: AdvancedSessionSchedulerPort,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not isinstance(
            authority,
            AdvancedRealtimeSessionAuthorization,
        ):
            raise TypeError("scheduler authority is invalid")
        if not callable(clock):
            raise TypeError("scheduler clock is invalid")
        self._authority = authority
        self._port = port
        self._clock = clock

    def run_window(
        self,
        window: AdvancedSessionDecisionWindow,
    ) -> Mapping[str, object]:
        if window not in self._authority.windows:
            raise ValueError("window is outside scheduler authority")
        existing = self._port.existing_cycle(window)
        if existing is not None:
            return self._report(window, existing, replayed=True)
        now = _utc(self._clock(), "scheduler clock")
        if now < window.equity_available_after:
            raise RuntimeError("Equity window is not finalized")
        if now > window.latest_options_cutoff:
            raise RuntimeError("Equity-to-Options decision deadline expired")
        receipt = self._port.load_equity_receipt(window)
        if receipt is None:
            raise RuntimeError("finalized Equity INPUT_READY is unavailable")
        if (
            receipt.cutoff != window.equity_cutoff
            or receipt.created_at > window.latest_options_cutoff
        ):
            raise RuntimeError("Equity INPUT_READY is outside E/O scope")
        capture = self._port.capture_options(window, receipt)
        if (
            capture.contract_tickers
            != self._authority.provider_contract.contract_tickers
            or not receipt.created_at
            <= capture.reviewed_at
            <= window.latest_options_cutoff
        ):
            raise RuntimeError("Options capture is outside exact E/O scope")
        published = self._port.run_cycle(
            window,
            receipt,
            capture,
        )
        if (
            published.release_cutoff < window.equity_cutoff
            or published.release_cutoff
            > window.latest_options_cutoff
        ):
            raise RuntimeError("published cycle is outside exact E/O scope")
        report = dict(self._report(
            window,
            published,
            replayed=False,
        ))
        report["options_activity_evidence_sha256"] = (
            capture.evidence_sha256
        )
        report["equity_receipt_id"] = str(receipt.receipt_id)
        report["equity_receipt_cutoff"] = (
            receipt.cutoff.isoformat()
        )
        report["equity_receipt_created_at"] = (
            receipt.created_at.isoformat()
        )
        report["equity_input_sha256"] = receipt.input_sha256
        report["options_capture_reviewed_at"] = (
            capture.reviewed_at.isoformat()
        )
        report["options_contract_tickers"] = list(
            capture.contract_tickers
        )
        report["options_request_count"] = capture.request_count
        report["options_pagination_requests"] = (
            capture.pagination_requests
        )
        return report

    def _report(
        self,
        window: AdvancedSessionDecisionWindow,
        published: SchedulerPublishedCycle,
        *,
        replayed: bool,
    ) -> Mapping[str, object]:
        return {
            "contract": ADVANCED_SESSION_SCHEDULER_VERSION,
            "authorization_id": str(self._authority.authorization_id),
            "window": _window_payload(window),
            "target_window_id": str(published.target_window_id),
            "release_id": str(published.release_id),
            "release_cutoff": published.release_cutoff.isoformat(),
            "read_pin_release_id": str(
                published.read_pin_release_id
            ),
            "stored_reader_count": published.stored_reader_count,
            "stock_alert_batch_id": str(
                published.stock_alert_batch_id
            ),
            "stock_alert_batch_sha256": (
                published.stock_alert_batch_sha256
            ),
            "stock_alert_evaluation_count": (
                published.stock_alert_evaluation_count
            ),
            "stock_alert_unavailable_count": (
                published.stock_alert_unavailable_count
            ),
            "stock_alert_reason_codes_exact": (
                published.stock_alert_reason_codes_exact
            ),
            "e_o_seconds": (
                published.release_cutoff - window.equity_cutoff
            ).total_seconds(),
            "status": "COMPLETE_PUBLISHED_PINNED",
            "replayed": replayed,
            "forbidden_effects": {
                "dispatch": 0,
                "delivery": 0,
                "broker": 0,
                "trading": 0,
            },
        }
