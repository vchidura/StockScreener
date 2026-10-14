from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Mapping
from urllib.parse import quote, urlsplit

from options.data.polygon_http import PolygonHttpResponse, PolygonHttpTransport
from options.domain import DataCapability
from provider_endpoints import REST_BASE_URL, REST_HOSTS

from .contracts import canonical_sha256
from .preflight import DetectorFamily


DEVELOPER_CANARY_TICKERS = (
    "SPY",
    "QQQ",
    "IWM",
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "TSLA",
)


class DependencyDisposition(str, Enum):
    READY = "READY"
    DEGRADED = "DEGRADED"
    NO_MATCH = "NO_MATCH"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class DeveloperCanaryPolicy:
    tickers: tuple[str, ...] = DEVELOPER_CANARY_TICKERS
    snapshot_contracts_per_type: int = 8
    trade_contracts_per_type: int = 2
    minimum_daily_bars: int = 100
    minimum_intraday_bars: int = 20
    minimum_smile_contracts_per_type: int = 7
    minimum_dte: int = 7
    maximum_dte: int = 45
    o2_enabled: bool = False
    maximum_requests: int = 88
    maximum_response_bytes: int = 2 * 1024 * 1024
    maximum_total_response_bytes: int = 32 * 1024 * 1024
    maximum_wall_seconds: int = 240

    def __post_init__(self) -> None:
        normalized = tuple(ticker.strip().upper() for ticker in self.tickers)
        if not normalized or len(normalized) != len(set(normalized)):
            raise ValueError("Developer canary tickers must be unique and nonempty")
        if any(ticker not in DEVELOPER_CANARY_TICKERS for ticker in normalized):
            raise ValueError("Developer canary includes an unreviewed ticker")
        if not 1 <= self.trade_contracts_per_type <= self.snapshot_contracts_per_type:
            raise ValueError("trade contract depth must fit the snapshot depth")
        if self.minimum_smile_contracts_per_type > self.snapshot_contracts_per_type:
            raise ValueError("smile minimum must fit the snapshot depth")
        if self.minimum_dte < 0 or self.maximum_dte < self.minimum_dte:
            raise ValueError("Developer canary DTE window is invalid")
        if self.maximum_requests < len(normalized) * 11:
            raise ValueError("Developer canary request cap cannot cover the plan")
        if min(
            self.maximum_response_bytes,
            self.maximum_total_response_bytes,
            self.maximum_wall_seconds,
        ) <= 0:
            raise ValueError("Developer canary bounds must be positive")
        object.__setattr__(self, "tickers", normalized)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class DeveloperTickerResult:
    ticker: str
    expiration: date
    daily_bar_count: int
    intraday_bar_count: int
    minute_bar_count: int
    call_snapshot_count: int
    put_snapshot_count: int
    call_mark_count: int
    put_mark_count: int
    call_oi_count: int
    put_oi_count: int
    aggregate_fallback_request_count: int
    delayed_trade_contract_count: int
    delayed_trade_event_count: int
    quote_access_denied: bool
    o2_group_dispositions: tuple[tuple[str, str], ...]
    dependency_dispositions: tuple[tuple[str, str], ...]
    reason_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DeveloperCanaryReport:
    status: str
    checked_at: datetime
    policy_sha256: str
    configuration_sha256: str
    capabilities: tuple[str, ...]
    developer_disabled: tuple[str, ...]
    advanced_unavailable: tuple[str, ...]
    request_count: int
    response_bytes: int
    elapsed_seconds: float
    ticker_results: tuple[DeveloperTickerResult, ...]
    provider_requests: bool = True
    database_writes: bool = False
    raw_payloads_retained: bool = False


@dataclass(frozen=True, slots=True)
class DeveloperCanaryAssessment:
    status: str
    policy_sha256: str
    source_report_sha256: str
    tickers: tuple[str, ...]
    developer_disabled: tuple[str, ...]
    advanced_unavailable: tuple[str, ...]
    o2_unavailable_groups: tuple[tuple[str, str], ...]
    blocking_reasons: tuple[str, ...]


class DeveloperValidationError(RuntimeError):
    pass


def assess_sanitized_developer_report(
    payload: Mapping[str, object],
    *,
    source_report_sha256: str,
    policy: DeveloperCanaryPolicy | None = None,
) -> DeveloperCanaryAssessment:
    policy = policy or DeveloperCanaryPolicy()
    if len(source_report_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in source_report_sha256
    ):
        raise ValueError("source_report_sha256 must be a SHA-256 digest")
    if payload.get("database_writes") is not False or payload.get(
        "raw_payloads_retained"
    ) is not False:
        raise ValueError("Developer report violates the read-only evidence contract")
    rows = payload.get("ticker_results")
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError("Developer report ticker_results are invalid")
    by_ticker = {str(row.get("ticker") or ""): row for row in rows}
    if set(by_ticker) != set(policy.tickers):
        raise ValueError("Developer report does not match the reviewed ticker scope")
    required = {
        DetectorFamily.STOCK_INTRADAY.value,
        DetectorFamily.STOCK_SWING.value,
        DetectorFamily.O1.value,
        DetectorFamily.O3.value,
        DetectorFamily.S1.value,
        DetectorFamily.S2.value,
    }
    blockers = []
    o2_unavailable = []
    for ticker in policy.tickers:
        row = by_ticker[ticker]
        if row.get("quote_access_denied") is not True:
            blockers.append(f"{ticker}:DEVELOPER_QUOTE_ACCESS_UNEXPECTED")
        dispositions_value = row.get("dependency_dispositions")
        if not isinstance(dispositions_value, list):
            blockers.append(f"{ticker}:DEPENDENCY_DISPOSITIONS_INVALID")
            continue
        dispositions = {
            str(item[0]): str(item[1])
            for item in dispositions_value
            if isinstance(item, list) and len(item) == 2
        }
        for detector in sorted(required):
            if dispositions.get(detector) not in {
                DependencyDisposition.READY.value,
                DependencyDisposition.NO_MATCH.value,
            }:
                blockers.append(f"{ticker}:{detector}_NOT_READY")
        groups = row.get("o2_group_dispositions")
        if isinstance(groups, list):
            o2_unavailable.extend(
                (ticker, str(item[0]))
                for item in groups
                if isinstance(item, list)
                and len(item) == 2
                and str(item[1]) == DependencyDisposition.UNAVAILABLE.value
            )
    expected_advanced = {
        DetectorFamily.AGGRESSIVE_PREMIUM_BURST.value,
        DetectorFamily.PERSISTENT_REALTIME_FLOW.value,
        DetectorFamily.SWEEP_LIKE_CLUSTER.value,
    }
    observed_advanced = set(payload.get("advanced_unavailable") or ())
    if observed_advanced != expected_advanced:
        blockers.append("ADVANCED_UNAVAILABLE_SET_MISMATCH")
    return DeveloperCanaryAssessment(
        status="PORTABLE_READY" if not blockers else "PORTABLE_BLOCKED",
        policy_sha256=policy.sha256,
        source_report_sha256=source_report_sha256,
        tickers=policy.tickers,
        developer_disabled=(DetectorFamily.O2.value,),
        advanced_unavailable=tuple(sorted(expected_advanced)),
        o2_unavailable_groups=tuple(sorted(o2_unavailable)),
        blocking_reasons=tuple(sorted(blockers)),
    )


class DeveloperCanaryValidator:
    def __init__(
        self,
        transport: PolygonHttpTransport,
        *,
        configuration_sha256: str,
        policy: DeveloperCanaryPolicy | None = None,
        base_url: str = REST_BASE_URL,
        monotonic=time.monotonic,
    ) -> None:
        if len(configuration_sha256) != 64 or any(
            character not in "0123456789abcdef"
            for character in configuration_sha256
        ):
            raise ValueError("configuration_sha256 must be a SHA-256 digest")
        base = urlsplit(base_url.rstrip("/"))
        if base.scheme != "https" or base.hostname not in REST_HOSTS:
            raise ValueError("Developer validation requires the reviewed Polygon HTTPS host")
        self.transport = transport
        self.configuration_sha256 = configuration_sha256
        self.policy = policy or DeveloperCanaryPolicy()
        self.base_url = base_url.rstrip("/")
        self.monotonic = monotonic
        self.request_count = 0
        self.response_bytes = 0
        self.started = 0.0

    def _request(
        self,
        path: str,
        params: Mapping[str, str],
        *,
        allowed_statuses: frozenset[int] = frozenset({200}),
    ) -> tuple[int, dict[str, object] | None]:
        if self.request_count >= self.policy.maximum_requests:
            raise DeveloperValidationError("DEVELOPER_REQUEST_CAP_EXCEEDED")
        if self.monotonic() - self.started > self.policy.maximum_wall_seconds:
            raise DeveloperValidationError("DEVELOPER_WALL_TIME_EXCEEDED")
        response = self.transport.get(f"{self.base_url}{path}", params)
        self.request_count += 1
        body_size = len(response.body)
        self.response_bytes += body_size
        if body_size > self.policy.maximum_response_bytes:
            raise DeveloperValidationError("DEVELOPER_RESPONSE_BYTES_EXCEEDED")
        if self.response_bytes > self.policy.maximum_total_response_bytes:
            raise DeveloperValidationError("DEVELOPER_TOTAL_BYTES_EXCEEDED")
        if response.status_code not in allowed_statuses:
            if response.status_code == 429:
                raise DeveloperValidationError("DEVELOPER_RATE_LIMITED")
            raise DeveloperValidationError(
                f"DEVELOPER_HTTP_STATUS_{response.status_code}"
            )
        if response.status_code != 200:
            return response.status_code, None
        try:
            payload = json.loads(response.body)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise DeveloperValidationError("DEVELOPER_INVALID_JSON") from error
        if not isinstance(payload, dict):
            raise DeveloperValidationError("DEVELOPER_INVALID_RESPONSE_SHAPE")
        status = str(payload.get("status") or "").upper()
        if status not in {"OK", "DELAYED"}:
            raise DeveloperValidationError("DEVELOPER_PROVIDER_STATUS_INVALID")
        return response.status_code, payload

    @staticmethod
    def _results(payload: dict[str, object], label: str) -> list[dict[str, object]]:
        results = payload.get("results")
        if not isinstance(results, list) or not all(
            isinstance(row, dict) for row in results
        ):
            raise DeveloperValidationError(f"{label}_RESULTS_INVALID")
        return results

    def _bars(
        self,
        ticker: str,
        multiplier: int,
        timespan: str,
        start: date,
        end: date,
    ) -> list[dict[str, object]]:
        path = (
            f"/v2/aggs/ticker/{quote(ticker, safe='')}/range/"
            f"{multiplier}/{timespan}/{start.isoformat()}/{end.isoformat()}"
        )
        _, payload = self._request(
            path,
            {"adjusted": "true", "sort": "asc", "limit": "5000"},
        )
        rows = self._results(payload, "BAR")
        valid = []
        for row in rows:
            try:
                close = Decimal(str(row["c"]))
            except (KeyError, InvalidOperation, ValueError):
                continue
            if close > 0 and isinstance(row.get("t"), int):
                valid.append(row)
        return valid

    def _expiration(self, ticker: str, as_of: date) -> date:
        path = "/v3/reference/options/contracts"
        _, payload = self._request(
            path,
            {
                "underlying_ticker": ticker,
                "expiration_date.gte": (
                    as_of + timedelta(days=self.policy.minimum_dte)
                ).isoformat(),
                "expiration_date.lte": (
                    as_of + timedelta(days=self.policy.maximum_dte)
                ).isoformat(),
                "expired": "false",
                "limit": "1",
                "sort": "expiration_date",
                "order": "asc",
            },
        )
        rows = self._results(payload, "REFERENCE")
        if not rows:
            raise DeveloperValidationError("REFERENCE_EXPIRATION_UNAVAILABLE")
        try:
            return date.fromisoformat(str(rows[0]["expiration_date"]))
        except (KeyError, ValueError) as error:
            raise DeveloperValidationError("REFERENCE_EXPIRATION_INVALID") from error

    def _snapshots(
        self,
        ticker: str,
        expiration: date,
        contract_type: str,
    ) -> list[dict[str, object]]:
        path = f"/v3/snapshot/options/{quote(ticker, safe='')}"
        _, payload = self._request(
            path,
            {
                "contract_type": contract_type,
                "expiration_date": expiration.isoformat(),
                "limit": str(self.policy.snapshot_contracts_per_type),
                "sort": "strike_price",
                "order": "asc",
            },
        )
        rows = self._results(payload, "SNAPSHOT")
        if len(rows) > self.policy.snapshot_contracts_per_type:
            raise DeveloperValidationError("SNAPSHOT_CONTRACT_CAP_EXCEEDED")
        return rows

    @staticmethod
    def _snapshot_identity(row: dict[str, object]) -> tuple[str, Decimal, int, bool]:
        details = row.get("details")
        if not isinstance(details, dict):
            raise DeveloperValidationError("SNAPSHOT_DETAILS_INVALID")
        try:
            ticker = str(details["ticker"])
            strike = Decimal(str(details["strike_price"]))
            shares = int(details.get("shares_per_contract") or 100)
        except (KeyError, InvalidOperation, TypeError, ValueError) as error:
            raise DeveloperValidationError("SNAPSHOT_IDENTITY_INVALID") from error
        if not ticker.startswith("O:") or strike <= 0 or shares != 100:
            raise DeveloperValidationError("SNAPSHOT_STANDARD_CONTRACT_REQUIRED")
        has_oi = isinstance(row.get("open_interest"), int) and row["open_interest"] >= 0
        return ticker, strike, shares, has_oi

    @staticmethod
    def _snapshot_mark(row: dict[str, object]) -> Decimal | None:
        for parent, key in (("last_trade", "price"), ("day", "close")):
            section = row.get(parent)
            if isinstance(section, dict) and section.get(key) is not None:
                try:
                    value = Decimal(str(section[key]))
                except (InvalidOperation, ValueError):
                    continue
                if value > 0:
                    return value
        return None

    def _trade_count(self, contract_ticker: str) -> int:
        path = f"/v3/trades/{quote(contract_ticker, safe=':')}"
        _, payload = self._request(
            path,
            {"limit": "10", "sort": "timestamp", "order": "desc"},
        )
        return len(self._results(payload, "TRADE"))

    def _daily_option_mark(
        self,
        contract_ticker: str,
        session: date,
    ) -> Decimal | None:
        path = (
            f"/v2/aggs/ticker/{quote(contract_ticker, safe=':')}/range/1/day/"
            f"{(session - timedelta(days=7)).isoformat()}/{session.isoformat()}"
        )
        _, payload = self._request(
            path,
            {"adjusted": "false", "sort": "desc", "limit": "1"},
        )
        if payload.get("results") is None:
            return None
        rows = self._results(payload, "OPTION_AGGREGATE")
        if not rows:
            return None
        try:
            mark = Decimal(str(rows[0]["c"]))
        except (KeyError, InvalidOperation, ValueError):
            return None
        return mark if mark > 0 else None

    def _mark_count_with_fallback(
        self,
        rows: list[dict[str, object]],
        identities: list[tuple[str, Decimal, int, bool]],
        session: date,
        spot: Decimal,
    ) -> tuple[int, int]:
        marks = [self._snapshot_mark(row) for row in rows]
        request_count = 0
        missing_indexes = sorted(
            (index for index, mark in enumerate(marks) if mark is None),
            key=lambda index: (abs(identities[index][1] - spot), identities[index][0]),
        )
        for index in missing_indexes:
            if sum(value is not None for value in marks) >= self.policy.minimum_smile_contracts_per_type:
                break
            if request_count >= self.policy.minimum_smile_contracts_per_type:
                break
            marks[index] = self._daily_option_mark(identities[index][0], session)
            request_count += 1
        return sum(value is not None for value in marks), request_count

    def _ticker(self, ticker: str, checked_at: datetime) -> DeveloperTickerResult:
        session = checked_at.date()
        daily = self._bars(
            ticker,
            1,
            "day",
            session - timedelta(days=400),
            session,
        )
        intraday = self._bars(
            ticker,
            30,
            "minute",
            session - timedelta(days=30),
            session,
        )
        minute = self._bars(
            ticker,
            1,
            "minute",
            session - timedelta(days=4),
            session,
        )
        expiration = self._expiration(ticker, session)
        calls = self._snapshots(ticker, expiration, "call")
        puts = self._snapshots(ticker, expiration, "put")
        call_identities = [self._snapshot_identity(row) for row in calls]
        put_identities = [self._snapshot_identity(row) for row in puts]
        spot = Decimal(str(minute[-1]["c"])) if minute else Decimal("0")
        selected = []
        for rows, identities in ((calls, call_identities), (puts, put_identities)):
            ranked = sorted(
                zip(rows, identities),
                key=lambda pair: (abs(pair[1][1] - spot), pair[1][0]),
            )
            selected.extend(ranked[: self.policy.trade_contracts_per_type])
        trade_events = sum(self._trade_count(identity[0]) for _, identity in selected)
        quote_denied = False
        if selected:
            contract_ticker = selected[0][1][0]
            status, _ = self._request(
                f"/v3/quotes/{quote(contract_ticker, safe=':')}",
                {"limit": "1"},
                allowed_statuses=frozenset({200, 403}),
            )
            quote_denied = status == 403
        reasons = []
        stock_ready = (
            len(daily) >= self.policy.minimum_daily_bars
            and len(intraday) >= self.policy.minimum_intraday_bars
            and bool(minute)
        )
        if not stock_ready:
            reasons.append("STOCK_BAR_WARMUP_INCOMPLETE")
        if self.policy.o2_enabled:
            call_marks, call_fallback_requests = self._mark_count_with_fallback(
                calls,
                call_identities,
                session,
                spot,
            )
            put_marks, put_fallback_requests = self._mark_count_with_fallback(
                puts,
                put_identities,
                session,
                spot,
            )
        else:
            call_marks = sum(self._snapshot_mark(row) is not None for row in calls)
            put_marks = sum(self._snapshot_mark(row) is not None for row in puts)
            call_fallback_requests = put_fallback_requests = 0
        call_smile_ready = call_marks >= self.policy.minimum_smile_contracts_per_type
        put_smile_ready = put_marks >= self.policy.minimum_smile_contracts_per_type
        qualified_smile_groups = int(call_smile_ready) + int(put_smile_ready)
        if not self.policy.o2_enabled:
            reasons.append("O2_DEVELOPER_POLICY_DISABLED")
        elif qualified_smile_groups == 0:
            reasons.append("O2_SMILE_COVERAGE_UNAVAILABLE")
        elif qualified_smile_groups == 1:
            reasons.append("O2_PARTIAL_SMILE_COVERAGE")
        call_oi = sum(identity[3] for identity in call_identities)
        put_oi = sum(identity[3] for identity in put_identities)
        options_ready = (
            len(selected) == self.policy.trade_contracts_per_type * 2
            and call_oi >= 2
            and put_oi >= 2
        )
        if not options_ready:
            reasons.append("OPTION_ACTIVITY_COVERAGE_INCOMPLETE")
        if not quote_denied:
            reasons.append("DEVELOPER_QUOTE_ACCESS_UNEXPECTED")
        dispositions = {
            DetectorFamily.STOCK_INTRADAY.value: (
                DependencyDisposition.READY if stock_ready else DependencyDisposition.UNAVAILABLE
            ),
            DetectorFamily.STOCK_SWING.value: (
                DependencyDisposition.READY if stock_ready else DependencyDisposition.UNAVAILABLE
            ),
            DetectorFamily.O1.value: (
                DependencyDisposition.READY if options_ready and stock_ready else DependencyDisposition.UNAVAILABLE
            ),
            DetectorFamily.O2.value: (
                DependencyDisposition.UNAVAILABLE
                if not self.policy.o2_enabled
                else DependencyDisposition.READY
                if qualified_smile_groups == 2
                else DependencyDisposition.DEGRADED
                if qualified_smile_groups == 1
                else DependencyDisposition.UNAVAILABLE
            ),
            DetectorFamily.O3.value: (
                DependencyDisposition.READY if options_ready else DependencyDisposition.UNAVAILABLE
            ),
            DetectorFamily.S1.value: (
                DependencyDisposition.READY if options_ready and stock_ready else DependencyDisposition.UNAVAILABLE
            ),
            DetectorFamily.S2.value: (
                DependencyDisposition.READY if options_ready and stock_ready else DependencyDisposition.UNAVAILABLE
            ),
        }
        return DeveloperTickerResult(
            ticker=ticker,
            expiration=expiration,
            daily_bar_count=len(daily),
            intraday_bar_count=len(intraday),
            minute_bar_count=len(minute),
            call_snapshot_count=len(calls),
            put_snapshot_count=len(puts),
            call_mark_count=call_marks,
            put_mark_count=put_marks,
            call_oi_count=call_oi,
            put_oi_count=put_oi,
            aggregate_fallback_request_count=(
                call_fallback_requests + put_fallback_requests
            ),
            delayed_trade_contract_count=len(selected),
            delayed_trade_event_count=trade_events,
            quote_access_denied=quote_denied,
            o2_group_dispositions=(
                (
                    "CALL",
                    (
                        DependencyDisposition.READY.value
                        if call_smile_ready
                        else DependencyDisposition.UNAVAILABLE.value
                    ),
                ),
                (
                    "PUT",
                    (
                        DependencyDisposition.READY.value
                        if put_smile_ready
                        else DependencyDisposition.UNAVAILABLE.value
                    ),
                ),
            ),
            dependency_dispositions=tuple(
                (name, disposition.value) for name, disposition in dispositions.items()
            ),
            reason_codes=tuple(reasons),
        )

    def validate(self, *, checked_at: datetime) -> DeveloperCanaryReport:
        if checked_at.tzinfo is None or checked_at.utcoffset() is None:
            raise ValueError("checked_at must be timezone-aware")
        checked_at = checked_at.astimezone(timezone.utc)
        self.started = self.monotonic()
        results = tuple(self._ticker(ticker, checked_at) for ticker in self.policy.tickers)
        elapsed = self.monotonic() - self.started
        unavailable = tuple(
            detector.value
            for detector in (
                DetectorFamily.AGGRESSIVE_PREMIUM_BURST,
                DetectorFamily.PERSISTENT_REALTIME_FLOW,
                DetectorFamily.SWEEP_LIKE_CLUSTER,
            )
        )
        portable_blocked = any(
            not result.quote_access_denied
            or any(
                disposition == DependencyDisposition.UNAVAILABLE.value
                for detector, disposition in result.dependency_dispositions
                if detector != DetectorFamily.O2.value or self.policy.o2_enabled
            )
            for result in results
        )
        portable_degraded = any(
            any(
                disposition == DependencyDisposition.DEGRADED.value
                for detector, disposition in result.dependency_dispositions
                if detector != DetectorFamily.O2.value or self.policy.o2_enabled
            )
            for result in results
        )
        status = (
            "PORTABLE_BLOCKED"
            if portable_blocked
            else "PORTABLE_DEGRADED"
            if portable_degraded
            else "PORTABLE_READY"
        )
        return DeveloperCanaryReport(
            status=status,
            checked_at=checked_at,
            policy_sha256=self.policy.sha256,
            configuration_sha256=self.configuration_sha256,
            capabilities=tuple(
                sorted(
                    capability.value
                    for capability in (
                        DataCapability.CHAIN_SNAPSHOT,
                        DataCapability.OPTION_TRADES,
                        DataCapability.UNDERLYING_PRICE,
                    )
                )
            ),
            developer_disabled=(DetectorFamily.O2.value,),
            advanced_unavailable=unavailable,
            request_count=self.request_count,
            response_bytes=self.response_bytes,
            elapsed_seconds=elapsed,
            ticker_results=results,
        )