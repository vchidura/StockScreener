from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
import threading
import time
from typing import Callable, Mapping
from urllib.parse import parse_qs, quote, urlparse
from uuid import NAMESPACE_URL, UUID, uuid5
from zoneinfo import ZoneInfo

import exchange_calendars
import pandas as pd

from equity.derivation import derive_canonical_bars
from equity.domain import BarAvailabilityMode, SecurityReferenceRevision
from equity.polygon import normalize_grouped_daily_bars, normalize_native_bars
from options.data.polygon_http import PolygonHttpTransport
from provider_endpoints import REST_BASE_URL, REST_HOSTS

from .contracts import EQUITY_VALIDATION_UNIVERSE, UniverseManifest, canonical_sha256
from .equity_bar_adapter import adapt_developer_equity_bar
from .equity_validation_history import (
    EQUITY_VALIDATION_HISTORY_POLICY,
    EquityValidationHistoryPackage,
    EquityValidationHistoryPolicy,
    SplitAction,
    SplitCoverage,
    build_equity_validation_history_package,
)
from .equity_portal_history import (
    REALTIME_EQUITY_PORTAL_HISTORY_POLICY,
    RealtimeEquityPortalHistoryPackage,
    RealtimeEquityPortalHistoryPlan,
    RealtimeEquityPortalHistoryPolicy,
    build_realtime_equity_portal_history_package,
    build_realtime_equity_portal_history_plan,
)


def _aggregate_path_identity(path: str, interval_minutes: int = 30, timespan: str = "minute"):
    parts = path.rstrip("/").split("/")
    if (
        len(parts) != 10
        or parts[1:4] != ["v2", "aggs", "ticker"]
        or parts[5:8] != ["range", str(interval_minutes), timespan]
    ):
        raise ValueError("equity validation aggregate path shape is invalid")

    def endpoint_date(value: str) -> date:
        try:
            return date.fromisoformat(value)
        except ValueError:
            if not value.isdigit():
                raise ValueError(
                    "equity validation aggregate endpoint is invalid"
                ) from None
            timestamp = int(value)
            if timestamp > 10_000_000_000:
                timestamp /= 1000
            return datetime.fromtimestamp(timestamp, tz=timezone.utc).date()

    return (
        parts[4].upper(),
        endpoint_date(parts[8]),
        endpoint_date(parts[9]),
    )


def bind_equity_validation_adjustment_identity(revision):
    if revision.interval != "1d" or not revision.adjusted:
        raise ValueError("adjustment identity requires an adjusted daily bar")
    identity_sha256 = canonical_sha256({
        "version": "equity-validation-adjustment-identity-v1",
        "source_bar_revision_id": revision.bar_revision_id,
        "adjusted": True,
        "payload_sha256": revision.payload_sha256,
    })
    return replace(
        revision,
        bar_revision_id=uuid5(
            NAMESPACE_URL,
            f"equity-validation-adjusted-bar:{identity_sha256}",
        ),
        quality_codes=tuple(dict.fromkeys(
            (*revision.quality_codes, "ADJUSTMENT_BASIS_IDENTITY_V1")
        )),
    )


@dataclass(frozen=True, slots=True)
class EquityValidationIdentity:
    ticker: str
    security: SecurityReferenceRevision
    instrument_id: int

    def __post_init__(self) -> None:
        ticker = self.ticker.strip().upper()
        if self.security.ticker != ticker or self.instrument_id <= 0:
            raise ValueError("equity validation identity is inconsistent")
        object.__setattr__(self, "ticker", ticker)


@dataclass(frozen=True, slots=True)
class EquityValidationSourceLimits:
    maximum_requests: int = 310
    maximum_response_bytes: int = 512 * 1024 * 1024
    maximum_response_rows: int = 50_000
    maximum_response_bytes_each: int = 10 * 1024 * 1024
    maximum_pages_per_request: int = 4
    maximum_retries_per_request: int = 3
    default_retry_after_seconds: float = 2.0
    maximum_retry_after_seconds: float = 30.0
    maximum_workers: int = 4

    def __post_init__(self) -> None:
        if not 1 <= self.maximum_workers <= 8:
            raise ValueError("equity validation workers must be in [1, 8]")


@dataclass(frozen=True, slots=True)
class EquityValidationAcquisitionPlan:
    session_date: date
    adjusted_sessions: tuple[date, ...]
    raw_start: date
    raw_end: date
    raw_sessions: tuple[date, ...]
    expected_raw_daily_bars_per_ticker: int
    expected_raw_hourly_bars_per_ticker: int
    expected_raw_thirty_minute_bars_per_ticker: int
    expected_provider_requests: int
    policy_sha256: str

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class EquityValidationSourceResult:
    plan: EquityValidationAcquisitionPlan
    package: EquityValidationHistoryPackage
    provider_requests: int
    response_bytes: int


@dataclass(frozen=True, slots=True)
class EquityValidationDailySourceResult:
    plan: EquityValidationAcquisitionPlan
    facts: tuple
    provider_requests: int
    response_bytes: int


@dataclass(frozen=True, slots=True)
class RealtimeEquityPortalSourceResult:
    plan: RealtimeEquityPortalHistoryPlan
    packages: tuple[RealtimeEquityPortalHistoryPackage, ...]
    facts: tuple
    provider_requests: int
    response_bytes: int


@dataclass(frozen=True, slots=True)
class RealtimeEquityPortalWeeklySourceResult:
    plan: RealtimeEquityPortalHistoryPlan
    facts: tuple
    provider_requests: int
    response_bytes: int


@dataclass(frozen=True, slots=True)
class RealtimeEquitySessionRecoveryResult:
    session_date: date
    facts: tuple
    provider_requests: int
    response_bytes: int


def build_equity_validation_acquisition_plan(
    session_date: date,
    *,
    policy: EquityValidationHistoryPolicy = EQUITY_VALIDATION_HISTORY_POLICY,
    daily: bool = False,
) -> EquityValidationAcquisitionPlan:
    calendar = exchange_calendars.get_calendar("XNYS")
    requested = pd.Timestamp(session_date)
    if not calendar.is_session(requested):
        raise ValueError("equity validation session_date must be an XNYS session")
    completed = calendar.sessions[calendar.sessions <= requested]
    adjusted = (
        completed[-1:]
        if daily
        else completed[-policy.adjusted_daily_sessions:]
    )
    if len(adjusted) != (1 if daily else policy.adjusted_daily_sessions):
        raise ValueError("equity validation calendar history is incomplete")
    raw_sessions = completed[-policy.raw_intraday_sessions:]
    if len(raw_sessions) != policy.raw_intraday_sessions:
        raise ValueError("equity validation raw session history is incomplete")
    expected_thirty = 0
    expected_hourly = 0
    for session in raw_sessions:
        minutes = int(
            (calendar.session_close(session) - calendar.session_open(session)).total_seconds()
            // 60
        )
        if minutes <= 0 or minutes % 30:
            raise ValueError("XNYS session duration is not compatible with 30m bars")
        expected_thirty += minutes // 30
        expected_hourly += (minutes + 59) // 60
    raw_start = raw_sessions[0].date()
    return EquityValidationAcquisitionPlan(
        session_date=session_date,
        adjusted_sessions=tuple(value.date() for value in adjusted),
        raw_start=raw_start,
        raw_end=session_date,
        raw_sessions=tuple(value.date() for value in raw_sessions),
        expected_raw_daily_bars_per_ticker=policy.raw_intraday_sessions,
        expected_raw_hourly_bars_per_ticker=expected_hourly,
        expected_raw_thirty_minute_bars_per_ticker=expected_thirty,
        expected_provider_requests=(
            policy.adjusted_daily_sessions
            + len(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
            if not daily
            else 1 + len(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
        ),
        policy_sha256=policy.sha256,
    )


class PolygonEquityValidationHistorySource:
    def __init__(
        self,
        transport: PolygonHttpTransport,
        identities: tuple[EquityValidationIdentity, ...],
        *,
        cohort_id: UUID,
        base_url: str = REST_BASE_URL,
        limits: EquityValidationSourceLimits = EquityValidationSourceLimits(),
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        sleep: Callable[[float], None] = time.sleep,
        universe: UniverseManifest = EQUITY_VALIDATION_UNIVERSE,
    ) -> None:
        if {row.ticker for row in identities} != set(
            universe.equity_tickers
        ) or len(identities) != len(universe.equity_tickers):
            raise ValueError("equity validation source requires exact identities")
        parsed = urlparse(base_url)
        if parsed.scheme != "https" or parsed.hostname not in REST_HOSTS:
            raise ValueError("equity validation source requires an approved HTTPS host")
        self.transport = transport
        self.universe = universe
        self.identities = tuple(sorted(identities, key=lambda row: row.ticker))
        self.cohort_id = cohort_id
        self.base_url = base_url.rstrip("/")
        self.limits = limits
        self.clock = clock
        self.sleep = sleep
        self.provider_requests = 0
        self.response_bytes = 0
        self._reserved_response_bytes = 0
        self._metrics_lock = threading.Lock()

    def _get(
        self,
        path: str,
        params: Mapping[str, str],
        *,
        interval_minutes: int = 30,
        timespan: str = "minute",
    ) -> tuple[dict, ...]:
        url = f"{self.base_url}{path}"
        request_params = dict(params)
        expected_filters = dict(params)
        try:
            expected_path_identity = _aggregate_path_identity(path, interval_minutes, timespan)
        except ValueError:
            expected_path_identity = None

        def path_identity(candidate: str):
            if expected_path_identity is None:
                return None
            try:
                return _aggregate_path_identity(candidate, interval_minutes, timespan)
            except ValueError:
                return None

        def path_matches(candidate: str) -> bool:
            if expected_path_identity is None:
                return candidate.rstrip("/") == path.rstrip("/")
            candidate_identity = path_identity(candidate)
            return (
                candidate_identity is not None
                and candidate_identity[0] == expected_path_identity[0]
                and expected_path_identity[1] <= candidate_identity[1]
                <= expected_path_identity[2]
                and candidate_identity[2] == expected_path_identity[2]
            )
        seen = set()
        collected = []
        page_count = 0
        previous_start = (
            expected_path_identity[1]
            if expected_path_identity is not None
            else None
        )
        while url:
            parsed = urlparse(url)
            if (
                parsed.scheme != "https"
                or parsed.hostname not in REST_HOSTS
                or not path_matches(parsed.path)
            ):
                raise ValueError("equity validation pagination identity drifted")
            request_identity = (parsed.path, tuple(sorted(request_params.items())))
            if request_identity in seen:
                raise ValueError("equity validation pagination repeated a request")
            seen.add(request_identity)
            retries = 0
            while True:
                with self._metrics_lock:
                    if self.provider_requests >= self.limits.maximum_requests:
                        raise ValueError(
                            "equity validation source exceeded its request bound: "
                            f"{self.provider_requests}>={self.limits.maximum_requests}"
                        )
                    if (
                        self.response_bytes + self._reserved_response_bytes
                        + self.limits.maximum_response_bytes_each
                        > self.limits.maximum_response_bytes
                    ):
                        raise ValueError("equity validation response-byte budget cannot admit another request")
                    self.provider_requests += 1
                    self._reserved_response_bytes += self.limits.maximum_response_bytes_each
                try:
                    response = self.transport.get(url, request_params)
                except Exception:
                    with self._metrics_lock:
                        self._reserved_response_bytes -= self.limits.maximum_response_bytes_each
                    raise
                with self._metrics_lock:
                    self._reserved_response_bytes -= self.limits.maximum_response_bytes_each
                    self.response_bytes += len(response.body)
                    if self.response_bytes > self.limits.maximum_response_bytes:
                        raise ValueError(
                            "equity validation source exceeded its total response-byte bound"
                        )
                    if len(response.body) > self.limits.maximum_response_bytes_each:
                        raise ValueError(
                            "equity validation source exceeded its per-response byte bound"
                        )
                if response.status_code == 200:
                    break
                if (
                    response.status_code not in {429, 502, 503, 504}
                    or retries >= self.limits.maximum_retries_per_request
                ):
                    raise RuntimeError(
                        f"equity validation provider returned HTTP {response.status_code}"
                    )
                retry_header = response.header("Retry-After")
                try:
                    requested_delay = float(retry_header) if retry_header is not None else 0.0
                except ValueError:
                    requested_delay = 0.0
                fallback_delay = self.limits.default_retry_after_seconds * (2 ** retries)
                self.sleep(min(
                    max(requested_delay, fallback_delay),
                    self.limits.maximum_retry_after_seconds,
                ))
                retries += 1
            payload = json.loads(response.body)
            if not isinstance(payload, dict):
                raise ValueError("equity validation provider payload is invalid")
            rows = payload.get("results")
            if rows is None:
                rows = []
            if (
                not isinstance(rows, list)
                or any(not isinstance(row, dict) for row in rows)
            ):
                raise ValueError("equity validation provider rows exceed bounds or shape")
            collected.extend(rows)
            page_count += 1
            if (
                page_count > self.limits.maximum_pages_per_request
                or len(collected) > self.limits.maximum_response_rows
            ):
                raise ValueError("equity validation pagination exceeded its bounds")
            next_url = payload.get("next_url")
            if next_url:
                next_page = urlparse(str(next_url))
                if (
                    next_page.scheme != "https"
                    or next_page.hostname not in REST_HOSTS
                    or not path_matches(next_page.path)
                    or next_page.username is not None
                    or next_page.password is not None
                ):
                    raise ValueError(
                        "equity validation pagination identity drifted: "
                        f"scheme={next_page.scheme or 'MISSING'} "
                        f"host={next_page.hostname or 'MISSING'} "
                        f"path={next_page.path or 'MISSING'} "
                        f"path_matches={path_matches(next_page.path)} "
                        f"credentials_present={bool(next_page.username or next_page.password)}"
                    )
                next_identity = path_identity(next_page.path)
                if (
                    expected_path_identity is not None
                    and (
                        next_identity is None
                        or next_identity[1] <= previous_start
                    )
                ):
                    raise ValueError(
                        "equity validation pagination range did not advance"
                    )
                query = parse_qs(next_page.query, keep_blank_values=True)
                allowed_keys = set(expected_filters) | {"cursor", "apiKey"}
                if set(query) - allowed_keys or any(
                    len(values) != 1 for values in query.values()
                ):
                    raise ValueError("equity validation pagination parameters drifted")
                cursor_values = query.get("cursor")
                if not cursor_values or not cursor_values[0]:
                    raise ValueError("equity validation pagination cursor is missing")
                for key, expected in expected_filters.items():
                    if key in query and query[key] != [expected]:
                        raise ValueError("equity validation pagination filters drifted")
                url = f"{self.base_url}{next_page.path}"
                if next_identity is not None:
                    previous_start = next_identity[1]
                request_params = {
                    key: values[0]
                    for key, values in query.items()
                    if key != "apiKey"
                }
            else:
                url = ""
                request_params = {}
        return tuple(collected)

    def _collect_adjusted_session(
        self,
        adjusted_session: date,
        *,
        observed_at: datetime,
        security_ids: Mapping[str, UUID],
        identity_by_ticker: Mapping[str, EquityValidationIdentity],
    ) -> tuple:
        rows = self._get(
            f"/v2/aggs/grouped/locale/us/market/stocks/{adjusted_session.isoformat()}",
            {"adjusted": "true"},
        )
        segment_id = uuid5(
            NAMESPACE_URL,
            f"equity-validation-adjusted:{adjusted_session}:{canonical_sha256(rows)}",
        )
        normalized = normalize_grouped_daily_bars(
            rows,
            session_date=adjusted_session,
            security_ids=security_ids,
            observed_at=observed_at,
            ingestion_segment_id=segment_id,
            availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
            adjusted=True,
        )
        if {row.ticker for row in normalized} != set(security_ids):
            raise ValueError(
                f"equity validation adjusted coverage is incomplete for {adjusted_session}"
            )
        return tuple(
            adapt_developer_equity_bar(
                bind_equity_validation_adjustment_identity(revision),
                cohort_id=self.cohort_id,
                instrument_id=identity_by_ticker[revision.ticker].instrument_id,
            )
            for revision in normalized
        )

    def _collect_adjusted_daily_identity(
        self,
        identity: EquityValidationIdentity,
        *,
        plan: RealtimeEquityPortalHistoryPlan,
        observed_at: datetime,
    ) -> tuple:
        rows = self._get(
            f"/v2/aggs/ticker/{quote(identity.ticker, safe='')}/range/1/day/"
            f"{plan.adjusted_sessions[0].isoformat()}/{plan.adjusted_sessions[-1].isoformat()}",
            {"adjusted": "true", "sort": "asc", "limit": "50000"},
        )
        segment_id = uuid5(
            NAMESPACE_URL,
            f"realtime-portal-adjusted-daily:{identity.ticker}:"
            f"{plan.adjusted_sessions[0]}:{plan.adjusted_sessions[-1]}:{canonical_sha256(rows)}",
        )
        rows = tuple(sorted(rows, key=lambda row: int(row["t"])))
        if len(rows) != len(plan.adjusted_sessions):
            raise ValueError(
                f"realtime portal adjusted daily history is incomplete for {identity.ticker}"
            )
        calendar = exchange_calendars.get_calendar("XNYS")
        revisions = []
        for session, row in zip(plan.adjusted_sessions, rows):
            expected_open = calendar.session_open(str(session)).to_pydatetime().astimezone(timezone.utc)
            if int(row["t"]) != int(expected_open.timestamp() * 1000):
                raise ValueError(
                    f"realtime portal adjusted daily session clock drift for {identity.ticker}"
                )
            revisions.extend(normalize_grouped_daily_bars(
                ({**row, "T": identity.ticker},), session_date=session,
                security_ids={identity.ticker: identity.security.security_id},
                observed_at=observed_at, ingestion_segment_id=segment_id,
                availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
                adjusted=True,
            ))
        revisions = tuple(revisions)
        if tuple(revision.session_date for revision in revisions) != plan.adjusted_sessions:
            raise ValueError(
                f"realtime portal adjusted daily history is incomplete for {identity.ticker}"
            )
        return tuple(
            adapt_developer_equity_bar(
                bind_equity_validation_adjustment_identity(revision),
                cohort_id=self.cohort_id,
                instrument_id=identity.instrument_id,
            )
            for revision in revisions
        )

    def _collect_raw_identity(
        self,
        identity: EquityValidationIdentity,
        *,
        plan: EquityValidationAcquisitionPlan,
        observed_at: datetime,
    ) -> tuple:
        rows = self._get(
            f"/v2/aggs/ticker/{quote(identity.ticker, safe='')}/range/30/minute/"
            f"{plan.raw_start.isoformat()}/{plan.raw_end.isoformat()}",
            {"adjusted": "false", "sort": "asc", "limit": "50000"},
        )
        segment_id = uuid5(
            NAMESPACE_URL,
            f"equity-validation-raw:{identity.ticker}:{plan.raw_start}:{plan.raw_end}:"
            f"{canonical_sha256(rows)}",
        )
        thirty = normalize_native_bars(
            identity.security.security_id,
            identity.ticker,
            "30m",
            rows,
            observed_at=observed_at,
            adjusted=False,
            availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
            ingestion_segment_id=segment_id,
            replay_available_at=observed_at,
        )
        hourly = derive_canonical_bars(
            identity.security,
            thirty,
            target_interval="1h",
            observed_at=observed_at,
            ingestion_segment_id=segment_id,
        )
        daily = derive_canonical_bars(
            identity.security,
            thirty,
            target_interval="1d",
            observed_at=observed_at,
            ingestion_segment_id=segment_id,
        )
        if (
            len(daily) != plan.expected_raw_daily_bars_per_ticker
            or len(hourly) != plan.expected_raw_hourly_bars_per_ticker
            or len(thirty) != plan.expected_raw_thirty_minute_bars_per_ticker
        ):
            raise ValueError(
                f"equity validation raw derivation is incomplete for {identity.ticker}"
            )
        return tuple(
            adapt_developer_equity_bar(
                revision,
                cohort_id=self.cohort_id,
                instrument_id=identity.instrument_id,
            )
            for revision in (*daily, *hourly, *thirty)
        )

    def _collect_native_five_minute_identity(
        self,
        identity: EquityValidationIdentity,
        *,
        plan: RealtimeEquityPortalHistoryPlan,
        observed_at: datetime,
    ) -> tuple:
        rows = self._get(
            f"/v2/aggs/ticker/{quote(identity.ticker, safe='')}/range/5/minute/"
            f"{plan.raw_start.isoformat()}/{plan.raw_end.isoformat()}",
            {"adjusted": "false", "sort": "asc", "limit": "50000"},
            interval_minutes=5,
        )
        segment_id = uuid5(
            NAMESPACE_URL,
            f"realtime-portal-native-5m:{identity.ticker}:{plan.raw_start}:"
            f"{plan.raw_end}:{canonical_sha256(rows)}",
        )
        revisions = normalize_native_bars(
            identity.security.security_id,
            identity.ticker,
            "5m",
            rows,
            observed_at=observed_at,
            adjusted=False,
            availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
            ingestion_segment_id=segment_id,
            replay_available_at=observed_at,
        )
        if len(revisions) != plan.expected_count("5m", "RAW_UNADJUSTED"):
            raise ValueError(
                f"realtime portal native 5m history is incomplete for {identity.ticker}"
            )
        return tuple(
            adapt_developer_equity_bar(
                revision,
                cohort_id=self.cohort_id,
                instrument_id=identity.instrument_id,
            )
            for revision in revisions
        )

    def _collect_native_one_minute_identity(
        self,
        identity: EquityValidationIdentity,
        *,
        plan: RealtimeEquityPortalHistoryPlan,
        observed_at: datetime,
    ) -> tuple:
        rows = self._get(
            f"/v2/aggs/ticker/{quote(identity.ticker, safe='')}/range/1/minute/"
            f"{plan.one_minute_start.isoformat()}/{plan.one_minute_end.isoformat()}",
            {"adjusted": "false", "sort": "asc", "limit": "50000"},
            interval_minutes=1,
        )
        segment_id = uuid5(
            NAMESPACE_URL,
            f"realtime-portal-native-1m:{identity.ticker}:{plan.one_minute_start}:"
            f"{plan.one_minute_end}:{canonical_sha256(rows)}",
        )
        revisions = normalize_native_bars(
            identity.security.security_id, identity.ticker, "1m", rows,
            observed_at=observed_at, adjusted=False,
            availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
            ingestion_segment_id=segment_id, replay_available_at=observed_at,
        )
        if len(revisions) != plan.expected_count("1m", "RAW_UNADJUSTED"):
            raise ValueError(
                f"realtime portal native 1m history is incomplete for {identity.ticker}"
            )
        return tuple(
            adapt_developer_equity_bar(
                revision, cohort_id=self.cohort_id, instrument_id=identity.instrument_id,
            )
            for revision in revisions
        )

    def _collect_native_monthly_identity(
        self,
        identity: EquityValidationIdentity,
        *,
        plan: RealtimeEquityPortalHistoryPlan,
        observed_at: datetime,
    ) -> tuple:
        start = plan.monthly_start.replace(day=1)
        rows = self._get(
            f"/v2/aggs/ticker/{quote(identity.ticker, safe='')}/range/1/month/"
            f"{start.isoformat()}/{plan.monthly_end.isoformat()}",
            {"adjusted": "true", "sort": "asc", "limit": "50000"},
            interval_minutes=1,
            timespan="month",
        )
        segment_id = uuid5(
            NAMESPACE_URL,
            f"realtime-portal-native-1mo:{identity.ticker}:{start}:"
            f"{plan.monthly_end}:{canonical_sha256(rows)}",
        )
        revisions = normalize_native_bars(
            identity.security.security_id, identity.ticker, "1mo", rows,
            observed_at=observed_at, adjusted=True,
            availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
            ingestion_segment_id=segment_id, replay_available_at=observed_at,
        )
        if (
            len(revisions) != plan.expected_count("1mo", "ADJUSTED")
            or tuple(revision.session_date for revision in revisions) != plan.monthly_sessions
        ):
            raise ValueError(
                f"realtime portal native adjusted monthly history is incomplete for {identity.ticker}"
            )
        return tuple(
            adapt_developer_equity_bar(
                revision, cohort_id=self.cohort_id, instrument_id=identity.instrument_id,
            )
            for revision in revisions
        )

    def _collect_native_weekly_identity(
        self,
        identity: EquityValidationIdentity,
        *,
        plan: RealtimeEquityPortalHistoryPlan,
        observed_at: datetime,
    ) -> tuple:
        first_iso = plan.weekly_start.isocalendar()
        request_start = date.fromisocalendar(first_iso.year, first_iso.week, 1)
        rows = self._get(
            f"/v2/aggs/ticker/{quote(identity.ticker, safe='')}/range/1/week/"
            f"{request_start.isoformat()}/{plan.weekly_end.isoformat()}",
            {"adjusted": "true", "sort": "asc", "limit": "50000"},
            interval_minutes=1,
            timespan="week",
        )
        segment_id = uuid5(
            NAMESPACE_URL,
            f"realtime-portal-native-1wk:{identity.ticker}:{request_start}:"
            f"{plan.weekly_end}:{canonical_sha256(rows)}",
        )
        revisions = normalize_native_bars(
            identity.security.security_id, identity.ticker, "1wk", rows,
            observed_at=observed_at, adjusted=True,
            availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
            ingestion_segment_id=segment_id, replay_available_at=observed_at,
        )
        if (
            len(revisions) != plan.expected_count("1wk", "ADJUSTED")
            or tuple(revision.session_date for revision in revisions) != plan.weekly_sessions
        ):
            raise ValueError(
                f"realtime portal native adjusted weekly history is incomplete for {identity.ticker}"
            )
        return tuple(
            adapt_developer_equity_bar(
                revision, cohort_id=self.cohort_id, instrument_id=identity.instrument_id,
            )
            for revision in revisions
        )

    def _parallel_results(self, items, operation, key):
        results = {}
        with ThreadPoolExecutor(
            max_workers=min(self.limits.maximum_workers, len(items)),
        ) as executor:
            futures = {
                executor.submit(operation, item): key(item)
                for item in items
            }
            for future in as_completed(futures):
                results[futures[future]] = future.result()
        return results

    def collect(
        self,
        session_date: date,
        *,
        split_coverage: Mapping[str, SplitCoverage],
        split_actions: tuple[SplitAction, ...],
        policy: EquityValidationHistoryPolicy = EQUITY_VALIDATION_HISTORY_POLICY,
    ) -> EquityValidationSourceResult:
        if self.universe != EQUITY_VALIDATION_UNIVERSE:
            raise ValueError("accepted equity validation collector requires its fixed universe")
        plan = build_equity_validation_acquisition_plan(session_date, policy=policy)
        if plan.expected_provider_requests > self.limits.maximum_requests:
            raise ValueError("equity validation plan exceeds the request bound")
        observed_at = self.clock().astimezone(timezone.utc)
        facts = []
        security_ids = {
            row.ticker: row.security.security_id for row in self.identities
        }
        identity_by_ticker = {row.ticker: row for row in self.identities}
        adjusted_results = self._parallel_results(
            plan.adjusted_sessions,
            lambda session: self._collect_adjusted_session(
                session,
                observed_at=observed_at,
                security_ids=security_ids,
                identity_by_ticker=identity_by_ticker,
            ),
            lambda session: session,
        )
        for adjusted_session in plan.adjusted_sessions:
            facts.extend(adjusted_results[adjusted_session])
        raw_results = self._parallel_results(
            self.identities,
            lambda identity: self._collect_raw_identity(
                identity,
                plan=plan,
                observed_at=observed_at,
            ),
            lambda identity: identity.ticker,
        )
        for identity in self.identities:
            facts.extend(raw_results[identity.ticker])
        if self.provider_requests < plan.expected_provider_requests:
            raise ValueError("equity validation request count changed")
        package = build_equity_validation_history_package(
            facts,
            split_coverage=split_coverage,
            split_actions=split_actions,
            policy=policy,
        )
        return EquityValidationSourceResult(
            plan=plan,
            package=package,
            provider_requests=self.provider_requests,
            response_bytes=self.response_bytes,
        )

    def collect_portal_history(
        self,
        session_date: date,
        *,
        split_coverage: Mapping[str, SplitCoverage],
        split_actions: tuple[SplitAction, ...],
        policy: RealtimeEquityPortalHistoryPolicy = REALTIME_EQUITY_PORTAL_HISTORY_POLICY,
    ) -> RealtimeEquityPortalSourceResult:
        plan = build_realtime_equity_portal_history_plan(
            session_date,
            policy=policy,
            equity_ticker_count=len(self.universe.equity_tickers),
        )
        if plan.expected_provider_requests > self.limits.maximum_requests:
            raise ValueError("realtime portal history plan exceeds the request bound")
        if set(split_coverage) != set(self.universe.equity_tickers):
            raise ValueError("realtime portal split coverage is incomplete")
        observed_at = self.clock().astimezone(timezone.utc)
        security_ids = {
            row.ticker: row.security.security_id for row in self.identities
        }
        identity_by_ticker = {row.ticker: row for row in self.identities}
        adjusted_results = (
            self._parallel_results(
                plan.adjusted_sessions,
                lambda session: self._collect_adjusted_session(
                    session,
                    observed_at=observed_at,
                    security_ids=security_ids,
                    identity_by_ticker=identity_by_ticker,
                ),
                lambda session: session,
            )
            if policy.version == "realtime-equity-portal-history-v2"
            else self._parallel_results(
                self.identities,
                lambda identity: self._collect_adjusted_daily_identity(
                    identity, plan=plan, observed_at=observed_at,
                ),
                lambda identity: identity.ticker,
            )
        )
        native_results = self._parallel_results(
            self.identities,
            lambda identity: self._collect_native_five_minute_identity(
                identity,
                plan=plan,
                observed_at=observed_at,
            ),
            lambda identity: identity.ticker,
        )
        one_minute_results = self._parallel_results(
            self.identities,
            lambda identity: self._collect_native_one_minute_identity(
                identity, plan=plan, observed_at=observed_at,
            ),
            lambda identity: identity.ticker,
        )
        monthly_results = self._parallel_results(
            self.identities,
            lambda identity: self._collect_native_monthly_identity(
                identity, plan=plan, observed_at=observed_at,
            ),
            lambda identity: identity.ticker,
        )
        weekly_results = (
            self._parallel_results(
                self.identities,
                lambda identity: self._collect_native_weekly_identity(
                    identity, plan=plan, observed_at=observed_at,
                ),
                lambda identity: identity.ticker,
            )
            if policy.version == "realtime-equity-portal-history-v3" else {}
        )
        packages = []
        facts = []
        for identity in self.identities:
            adjusted_facts = (
                tuple(
                    fact
                    for session in plan.adjusted_sessions
                    for fact in adjusted_results[session]
                    if fact.revision.ticker == identity.ticker
                )
                if policy.version == "realtime-equity-portal-history-v2"
                else adjusted_results[identity.ticker]
            )
            source_facts = (
                adjusted_facts
                + weekly_results.get(identity.ticker, ())
                + monthly_results[identity.ticker]
                + one_minute_results[identity.ticker]
                + native_results[identity.ticker]
            )
            package = build_realtime_equity_portal_history_package(
                identity.security,
                source_facts,
                plan=plan,
                observed_at=observed_at,
                ingestion_segment_id=uuid5(
                    NAMESPACE_URL,
                    f"realtime-portal-derived:{self.cohort_id}:{identity.ticker}:"
                    f"{plan.sha256}",
                ),
                split_coverage=split_coverage[identity.ticker],
                split_actions=split_actions,
                policy=policy,
            )
            packages.append(package)
            facts.extend(package.facts)
        if self.provider_requests < plan.expected_provider_requests:
            raise ValueError("realtime portal history request count changed")
        if len(facts) != plan.expected_total_facts:
            raise ValueError("realtime portal history fact count changed")
        return RealtimeEquityPortalSourceResult(
            plan=plan,
            packages=tuple(packages),
            facts=tuple(facts),
            provider_requests=self.provider_requests,
            response_bytes=self.response_bytes,
        )

    def collect_portal_weekly_history(
        self,
        session_date: date,
        *,
        policy: RealtimeEquityPortalHistoryPolicy = REALTIME_EQUITY_PORTAL_HISTORY_POLICY,
    ) -> RealtimeEquityPortalWeeklySourceResult:
        plan = build_realtime_equity_portal_history_plan(
            session_date, policy=policy,
            equity_ticker_count=len(self.universe.equity_tickers),
        )
        if policy.version != "realtime-equity-portal-history-v3":
            raise ValueError("weekly-only portal history requires v3 policy")
        expected_requests = len(self.identities)
        if expected_requests > self.limits.maximum_requests:
            raise ValueError("weekly-only portal history exceeds the request bound")
        observed_at = self.clock().astimezone(timezone.utc)
        weekly_results = self._parallel_results(
            self.identities,
            lambda identity: self._collect_native_weekly_identity(
                identity, plan=plan, observed_at=observed_at,
            ),
            lambda identity: identity.ticker,
        )
        facts = tuple(
            fact for identity in self.identities for fact in weekly_results[identity.ticker]
        )
        expected_facts = plan.expected_count("1wk", "ADJUSTED") * len(self.identities)
        if self.provider_requests != expected_requests or len(facts) != expected_facts:
            raise ValueError("weekly-only portal history count changed")
        return RealtimeEquityPortalWeeklySourceResult(
            plan=plan, facts=facts, provider_requests=self.provider_requests,
            response_bytes=self.response_bytes,
        )

    def collect_session_recovery(self, session_date: date) -> RealtimeEquitySessionRecoveryResult:
        if self.universe != EQUITY_VALIDATION_UNIVERSE:
            raise ValueError("session recovery requires its fixed nine-equity universe")
        if (
            not 27 <= self.limits.maximum_requests <= 36
            or self.limits.maximum_response_bytes > 32 * 1024 * 1024
            or self.limits.maximum_response_bytes_each > 2 * 1024 * 1024
        ):
            raise ValueError("session recovery limits exceed the approved envelope")
        calendar = exchange_calendars.get_calendar("XNYS")
        if not calendar.is_session(str(session_date)):
            raise ValueError("session recovery requires an XNYS session")
        opened = calendar.session_open(str(session_date)).to_pydatetime()
        closed = calendar.session_close(str(session_date)).to_pydatetime()
        if self.clock().utcoffset() is None or self.clock() < closed + timedelta(minutes=15):
            raise ValueError("session recovery cannot precede the completed Developer release")
        minutes = int((closed - opened).total_seconds() // 60)

        def collect_identity(identity):
            facts = []
            for interval, width in (("1m", 1), ("5m", 5), ("1d", 1)):
                adjusted = interval == "1d"
                start = str(session_date) if adjusted else str(int(opened.timestamp() * 1000))
                end = str(session_date) if adjusted else str(int(closed.timestamp() * 1000) - 1)
                rows = self._get(
                    f"/v2/aggs/ticker/{quote(identity.ticker, safe='')}/range/"
                    f"{width}/{'day' if adjusted else 'minute'}/{start}/{end}",
                    {"adjusted": "true" if adjusted else "false", "sort": "asc", "limit": "50000"},
                    interval_minutes=width, timespan="day" if adjusted else "minute",
                )
                received_at = self.clock().astimezone(timezone.utc)
                segment = uuid5(NAMESPACE_URL, canonical_sha256((
                    "realtime-equity-session-recovery-v1", self.cohort_id,
                    identity.ticker, session_date, interval, rows,
                )))
                if adjusted:
                    if (
                        len(rows) != 1 or type(rows[0].get("t")) is not int
                        or datetime.fromtimestamp(rows[0]["t"] / 1000, timezone.utc)
                        .astimezone(ZoneInfo("America/New_York")).date() != session_date
                    ):
                        raise ValueError(f"session recovery daily coverage is incomplete for {identity.ticker}")
                    revisions = tuple(bind_equity_validation_adjustment_identity(row) for row in normalize_grouped_daily_bars(
                        ({**rows[0], "T": identity.ticker},), session_date=session_date,
                        security_ids={identity.ticker: identity.security.security_id},
                        observed_at=received_at, ingestion_segment_id=segment,
                        availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED, adjusted=True,
                    ))
                else:
                    revisions = normalize_native_bars(
                        identity.security.security_id, identity.ticker, interval, rows,
                        observed_at=received_at, ingestion_segment_id=segment, adjusted=False,
                        availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
                        replay_available_at=received_at,
                    )
                    expected = tuple(opened + timedelta(minutes=width * index)
                                     for index in range(1, minutes // width + 1))
                    if tuple(row.bar_end for row in revisions) != expected:
                        raise ValueError(f"session recovery {interval} coverage is incomplete for {identity.ticker}")
                if any(not row.is_final or row.session_date != session_date for row in revisions):
                    raise ValueError("session recovery source contains non-final or out-of-session bars")
                facts.extend(adapt_developer_equity_bar(
                    row, cohort_id=self.cohort_id, instrument_id=identity.instrument_id,
                ) for row in revisions)
            return tuple(facts)

        results = self._parallel_results(self.identities, collect_identity, lambda identity: identity.ticker)
        return RealtimeEquitySessionRecoveryResult(
            session_date, tuple(fact for identity in self.identities for fact in results[identity.ticker]),
            self.provider_requests, self.response_bytes,
        )

    def collect_forward_minute_inputs(self, cutoff: datetime) -> tuple:
        if cutoff.utcoffset() is None or self.clock().utcoffset() is None:
            raise ValueError("forward minute collection requires aware clocks")
        observed = self.clock().astimezone(timezone.utc)
        cutoff = cutoff.astimezone(timezone.utc)
        if observed < cutoff + timedelta(minutes=15):
            raise ValueError("forward minute collection cannot precede Developer release")
        calendar = exchange_calendars.get_calendar("XNYS")
        session = pd.Timestamp(cutoff.astimezone(ZoneInfo("America/New_York")).date())
        opened = calendar.session_open(session).to_pydatetime()
        closed = calendar.session_close(session).to_pydatetime()
        if not opened < cutoff <= closed or cutoff.second or cutoff.microsecond:
            raise ValueError("forward minute cutoff must be a completed exchange minute")
        start = max(opened, cutoff - timedelta(hours=1))
        expected_count = int((cutoff - start).total_seconds() // 60)
        facts = []
        for identity in self.identities:
            rows = self._get(
                f"/v2/aggs/ticker/{quote(identity.ticker, safe='')}/range/1/minute/"
                f"{int(start.timestamp() * 1000)}/{int(cutoff.timestamp() * 1000) - 1}",
                {"adjusted": "false", "sort": "asc", "limit": "50000"}, interval_minutes=1,
            )
            received_at = self.clock().astimezone(timezone.utc)
            segment_id = uuid5(NAMESPACE_URL, f"developer-forward-minute:{self.cohort_id}:"
                f"{identity.ticker}:{cutoff.isoformat()}:{canonical_sha256(rows)}")
            revisions = normalize_native_bars(identity.security.security_id, identity.ticker, "1m", rows,
                observed_at=received_at, adjusted=False, availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
                ingestion_segment_id=segment_id)
            if (len(revisions) != expected_count
                    or tuple(row.bar_end for row in revisions) != tuple(start + timedelta(minutes=index)
                        for index in range(1, expected_count + 1))
                    or any(not row.is_final or row.quality_codes for row in revisions)):
                raise ValueError(f"forward exact minute coverage is incomplete for {identity.ticker}")
            facts.extend(adapt_developer_equity_bar(row, cohort_id=self.cohort_id,
                instrument_id=identity.instrument_id) for row in revisions)
        return tuple(facts)

    def collect_forward_context_inputs(self, cutoff: datetime) -> tuple:
        if cutoff.utcoffset() is None or self.clock() < cutoff + timedelta(minutes=15):
            raise ValueError("forward context collection cannot precede Developer release")
        calendar = exchange_calendars.get_calendar("XNYS")
        session = pd.Timestamp(cutoff.astimezone(ZoneInfo("America/New_York")).date())
        opened, closed = calendar.session_open(session).to_pydatetime(), calendar.session_close(session).to_pydatetime()
        if not opened < cutoff <= closed:
            raise ValueError("forward context cutoff is outside its exchange session")
        previous = calendar.sessions[calendar.sessions < session][-21:]
        if len(previous) != 21:
            raise ValueError("forward liquidity history calendar is incomplete")
        facts = []
        for identity in self.identities:
            rows = self._get(
                f"/v2/aggs/ticker/{quote(identity.ticker, safe='')}/range/30/minute/"
                f"{previous[0].date().isoformat()}/{int(cutoff.timestamp() * 1000) - 1}",
                {"adjusted": "false", "sort": "asc", "limit": "50000"}, interval_minutes=30)
            observed_at = self.clock().astimezone(timezone.utc)
            segment_id = uuid5(NAMESPACE_URL, f"developer-forward-context:{self.cohort_id}:"
                f"{identity.ticker}:{cutoff.isoformat()}:{canonical_sha256(rows)}")
            if any(type(row.get("t")) is not int for row in rows):
                raise ValueError("forward context source has invalid aggregate timestamps")
            completed_rows = tuple(row for row in rows if row["t"] + 30 * 60 * 1000 <= int(cutoff.timestamp() * 1000))
            bars = normalize_native_bars(identity.security.security_id, identity.ticker, "30m", completed_rows,
                observed_at=observed_at, adjusted=False, availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
                ingestion_segment_id=segment_id)
            if (len(bars) < 202 or any(row.bar_end > cutoff or not row.is_final or row.quality_codes for row in bars)):
                raise ValueError(f"forward trend coverage is incomplete for {identity.ticker}")
            daily = derive_canonical_bars(identity.security, bars, target_interval="1d",
                observed_at=observed_at, ingestion_segment_id=segment_id)
            if len({row.session_date for row in daily if row.session_date in {value.date() for value in previous}}) != 21:
                raise ValueError(f"forward daily liquidity coverage is incomplete for {identity.ticker}")
            facts.extend(adapt_developer_equity_bar(row, cohort_id=self.cohort_id,
                instrument_id=identity.instrument_id) for row in (*bars, *daily))
        return tuple(facts)

    def collect_daily(
        self,
        session_date: date,
        *,
        policy: EquityValidationHistoryPolicy = EQUITY_VALIDATION_HISTORY_POLICY,
    ) -> EquityValidationDailySourceResult:
        if self.universe != EQUITY_VALIDATION_UNIVERSE:
            raise ValueError("accepted equity validation daily collector requires its fixed universe")
        plan = build_equity_validation_acquisition_plan(
            session_date,
            policy=policy,
            daily=True,
        )
        if plan.expected_provider_requests > self.limits.maximum_requests:
            raise ValueError("equity validation daily plan exceeds the request bound")
        observed_at = self.clock().astimezone(timezone.utc)
        facts = []
        security_ids = {
            row.ticker: row.security.security_id for row in self.identities
        }
        identity_by_ticker = {row.ticker: row for row in self.identities}
        facts.extend(self._collect_adjusted_session(
            session_date,
            observed_at=observed_at,
            security_ids=security_ids,
            identity_by_ticker=identity_by_ticker,
        ))
        raw_results = self._parallel_results(
            self.identities,
            lambda identity: self._collect_raw_identity(
                identity,
                plan=plan,
                observed_at=observed_at,
            ),
            lambda identity: identity.ticker,
        )
        for identity in self.identities:
            facts.extend(raw_results[identity.ticker])
        if self.provider_requests < plan.expected_provider_requests:
            raise ValueError("equity validation daily request count changed")
        return EquityValidationDailySourceResult(
            plan=plan,
            facts=tuple(facts),
            provider_requests=self.provider_requests,
            response_bytes=self.response_bytes,
        )