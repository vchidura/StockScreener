"""Disabled-by-default incremental 1m/5m V2 acquisition planning and execution."""
from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote
from uuid import NAMESPACE_URL, UUID, uuid5

import exchange_calendars
import requests

from equity.domain import BarAvailabilityMode
from equity.polygon import normalize_native_bars
from options.data.polygon_http import (
    PolygonHttpResponse,
    PolygonHttpTransport,
    PolygonResponseTooLarge,
)

from .acquisition_handoff import (
    DeveloperAcquisitionFactKind,
    DeveloperEquityBarAcquisitionFact,
)
from .acquisition_ledger import (
    PriceBasis,
    ProviderRequestCompletion,
    ProviderRequestLane,
    ProviderRequestOutcome,
    ProviderRequestReservation,
    ProviderRequestState,
    RecordedProviderRequest,
    RequestCompleteness,
    finish_provider_request_cursor,
    load_raw_payload_cursor,
    load_recorded_provider_request_cursor,
    persist_raw_payload_cursor,
)
from .acquisition_producer import (
    CompletedDeveloperSourceBatch,
    DeveloperSourceResult,
)
from .contracts import canonical_sha256
from .database_access_v2 import APP_ROLE, V2ConnectionProvider
from .equity_recent_repair import NativeRepairBlocked, _clock_utc
from .equity_validation_source import EquityValidationIdentity
from .provider_budget import (
    ProviderBudgetScope,
    reserve_coordinated_provider_request_cursor,
)


INCREMENTAL_POLICY_VERSION = "v2-incremental-equity-v1"
INCREMENTAL_ENDPOINT_CLASS = "EQUITY_INCREMENTAL"
INCREMENTAL_ACCOUNT_ALIAS = "MASSIVE_DEVELOPER"
INTERVAL_WIDTHS = {"1m": 1, "5m": 5}
OVERLAP_BARS = {"1m": 2, "5m": 1}


class IncrementalAcquisitionBlocked(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class IncrementalRequestPlan:
    cohort_id: UUID
    instrument_id: int
    security_id: UUID
    ticker: str
    session_date: date
    interval: str
    requested_from: datetime
    requested_to: datetime
    overlap_bars: int
    prior_complete_bar_end: datetime | None
    prior_examined_bar_end: datetime | None
    prior_terminal_state: str | None

    def __post_init__(self) -> None:
        width = INTERVAL_WIDTHS.get(self.interval)
        if (
            type(self.instrument_id) is not int
            or self.instrument_id <= 0
            or not self.ticker.strip()
            or width is None
            or type(self.overlap_bars) is not int
            or self.overlap_bars < 0
        ):
            raise ValueError("incremental request identity is invalid")
        start = _clock(self.requested_from, "requested_from")
        end = _clock(self.requested_to, "requested_to")
        if start >= end or int((end - start).total_seconds()) % (width * 60):
            raise ValueError("incremental request range is not interval aligned")
        object.__setattr__(self, "ticker", self.ticker.strip().upper())
        object.__setattr__(self, "requested_from", start)
        object.__setattr__(self, "requested_to", end)

    @property
    def expected_bars(self) -> int:
        return int(
            (self.requested_to - self.requested_from).total_seconds()
            // (INTERVAL_WIDTHS[self.interval] * 60)
        )


def _clock(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def latest_qualified_cutoff(
    session_date: date,
    interval: str,
    *,
    known_at: datetime,
    provider_delay: timedelta,
    calendar=None,
) -> datetime | None:
    width = INTERVAL_WIDTHS.get(interval)
    if width is None or provider_delay < timedelta(0):
        raise ValueError("incremental cutoff policy is invalid")
    known = _clock(known_at, "known_at")
    calendar = calendar or exchange_calendars.get_calendar("XNYS")
    key = session_date.isoformat()
    if not calendar.is_session(key):
        return None
    opened = calendar.session_open(key).to_pydatetime().astimezone(timezone.utc)
    closed = calendar.session_close(key).to_pydatetime().astimezone(timezone.utc)
    qualified = min(known - provider_delay, closed)
    if qualified < opened + timedelta(minutes=width):
        return None
    elapsed = int((qualified - opened).total_seconds() // 60)
    completed_minutes = elapsed - (elapsed % width)
    return min(opened + timedelta(minutes=completed_minutes), closed)


def plan_incremental_requests_cursor(
    cursor,
    *,
    cohort_id: UUID,
    identities: Sequence[EquityValidationIdentity],
    session_date: date,
    interval: str,
    cutoff: datetime,
    calendar=None,
) -> tuple[IncrementalRequestPlan, ...]:
    width = INTERVAL_WIDTHS.get(interval)
    if width is None:
        raise ValueError("incremental interval is unsupported")
    boundary = _clock(cutoff, "cutoff")
    calendar = calendar or exchange_calendars.get_calendar("XNYS")
    key = session_date.isoformat()
    if not calendar.is_session(key):
        raise ValueError("incremental acquisition requires an XNYS session")
    opened = calendar.session_open(key).to_pydatetime().astimezone(timezone.utc)
    closed = calendar.session_close(key).to_pydatetime().astimezone(timezone.utc)
    if (
        not opened < boundary <= closed
        or int((boundary - opened).total_seconds()) % (width * 60)
    ):
        raise ValueError("incremental cutoff is outside its aligned XNYS session")
    members = tuple(identities)
    if not members:
        raise ValueError("incremental acquisition requires exact members")
    instrument_ids = [row.instrument_id for row in members]
    cursor.execute(
        "SELECT instrument_id,last_complete_bar_end,last_examined_bar_end,"
        "terminal_state FROM rt.acquisition_watermarks "
        "WHERE cohort_id=%s AND interval=%s AND source_kind='NATIVE_REST' "
        "AND instrument_id=ANY(%s::bigint[])",
        (str(cohort_id), interval, instrument_ids),
    )
    retained = {
        int(row[0]): (row[1], row[2], row[3])
        for row in cursor.fetchall()
    }
    overlap = OVERLAP_BARS[interval]
    step = timedelta(minutes=width)
    plans = []
    for identity in sorted(members, key=lambda row: row.instrument_id):
        complete, examined, state = retained.get(
            identity.instrument_id,
            (None, None, None),
        )
        if state == "COMPLETE" and complete is not None and complete >= boundary:
            continue
        request_start = (
            opened
            if complete is None
            else max(opened, complete - overlap * step)
        )
        plans.append(IncrementalRequestPlan(
            cohort_id,
            identity.instrument_id,
            identity.security.security_id,
            identity.ticker,
            session_date,
            interval,
            request_start,
            boundary,
            overlap,
            complete,
            examined,
            state,
        ))
    return tuple(plans)


class IncrementalEquitySource:
    fact_kind = DeveloperAcquisitionFactKind.EQUITY_BAR

    def __init__(
        self,
        provider: V2ConnectionProvider,
        transport: PolygonHttpTransport,
        budget_scope: ProviderBudgetScope,
        identities: Sequence[EquityValidationIdentity],
        plans: Sequence[IncrementalRequestPlan],
        *,
        response_byte_limit: int,
        clock: Callable[[], datetime],
        reservation_repository=reserve_coordinated_provider_request_cursor,
        endpoint_class: str = INCREMENTAL_ENDPOINT_CLASS,
        eligibility_policy_version: str = INCREMENTAL_POLICY_VERSION,
    ) -> None:
        if (
            type(response_byte_limit) is not int
            or response_byte_limit < 1_024
        ):
            raise ValueError("incremental response limit is invalid")
        self.provider = provider
        self.transport = transport
        self.budget_scope = budget_scope
        self.identities = {
            row.instrument_id: row for row in identities
        }
        self.plans = tuple(plans)
        self.response_byte_limit = response_byte_limit
        self.clock = clock
        self.reservation_repository = reservation_repository
        self.endpoint_class = endpoint_class.strip()
        self.eligibility_policy_version = (
            eligibility_policy_version.strip()
        )
        if (
            not self.plans
            or set(plan.instrument_id for plan in self.plans)
            - set(self.identities)
            or len(self.budget_scope.lanes) != 1
            or not self.endpoint_class
            or not self.eligibility_policy_version
        ):
            raise ValueError("incremental source plan/member scope is invalid")
        self.request_lane = next(iter(self.budget_scope.lanes))

    def _request_identity(self, plan):
        width = INTERVAL_WIDTHS[plan.interval]
        url = (
            "https://api.massive.com/v2/aggs/ticker/"
            f"{quote(plan.ticker, safe='')}/range/{width}/minute/"
            f"{int(plan.requested_from.timestamp() * 1000)}/"
            f"{int(plan.requested_to.timestamp() * 1000)}"
        )
        params = {"adjusted": "false", "sort": "asc", "limit": "50000"}
        request_sha256 = canonical_sha256({
            "policy": INCREMENTAL_POLICY_VERSION,
            "eligibility_policy_version": (
                self.eligibility_policy_version
            ),
            "endpoint_class": self.endpoint_class,
            "budget_scope_sha256": self.budget_scope.sha256,
            "cohort_id": plan.cohort_id,
            "instrument_id": plan.instrument_id,
            "interval": plan.interval,
            "url": url,
            "params": params,
        })
        request_id = uuid5(
            NAMESPACE_URL,
            f"v2-incremental-provider-request:{request_sha256}",
        )
        return url, params, request_sha256, request_id

    def _finish_failure(
        self,
        request_id,
        *,
        started_at,
        state,
        outcome,
        failure_code,
        response_bytes=None,
        completed_at=None,
    ):
        with self.provider.transaction(APP_ROLE) as unit:
            finish_provider_request_cursor(
                unit.cursor,
                request_id,
                ProviderRequestCompletion(
                    state,
                    started_at=started_at,
                    completed_at=completed_at,
                    outcome=outcome,
                    response_bytes=response_bytes,
                    failure_code=failure_code,
                ),
            )

    def _fetch(self, plan):
        identity = self.identities[plan.instrument_id]
        url, params, request_sha256, request_id = self._request_identity(plan)
        reserved_at = _clock_utc(self.clock)
        base_reservation = ProviderRequestReservation(
            request_id,
            self.budget_scope.account_alias,
            self.request_lane,
            self.endpoint_class,
            request_sha256,
            reserved_at,
            cohort_id=plan.cohort_id,
        )
        with self.provider.transaction(APP_ROLE) as unit:
            admission = self.reservation_repository(
                unit.cursor,
                self.budget_scope,
                base_reservation,
                reserved_response_bytes=self.response_byte_limit + 1,
            )
        if not admission.newly_reserved:
            if admission.state != ProviderRequestState.COMPLETE.value:
                raise IncrementalAcquisitionBlocked(
                    f"incremental request is retained as {admission.state}"
                )
            with self.provider.transaction(
                APP_ROLE,
                read_only=True,
            ) as unit:
                recorded = load_recorded_provider_request_cursor(
                    unit.cursor,
                    request_id,
                )
                if (
                    recorded is None
                    or recorded.completion.payload_id is None
                    or recorded.completion.state
                    is not ProviderRequestState.COMPLETE
                ):
                    raise IncrementalAcquisitionBlocked(
                        "sealed incremental response is unavailable"
                    )
                body = load_raw_payload_cursor(
                    unit.cursor,
                    recorded.completion.payload_id,
                )
            external = 0
        else:
            started_at = _clock_utc(self.clock)
            if started_at > self.budget_scope.authorized_until:
                self._finish_failure(
                    request_id,
                    started_at=started_at,
                    state=ProviderRequestState.FAILED,
                    outcome=ProviderRequestOutcome.INVALID,
                    failure_code="INCREMENTAL_AUTHORIZATION_EXPIRED",
                    response_bytes=0,
                    completed_at=started_at,
                )
                raise IncrementalAcquisitionBlocked(
                    "incremental authorization expired"
                )
            try:
                response = self.transport.get(url, params)
            except NativeRepairBlocked as error:
                completed_at = _clock_utc(self.clock)
                self._finish_failure(
                    request_id,
                    started_at=started_at,
                    state=ProviderRequestState.FAILED,
                    outcome=ProviderRequestOutcome.INVALID,
                    failure_code=str(error),
                    response_bytes=0,
                    completed_at=completed_at,
                )
                raise IncrementalAcquisitionBlocked(str(error)) from error
            except PolygonResponseTooLarge as error:
                completed_at = _clock_utc(self.clock)
                self._finish_failure(
                    request_id,
                    started_at=started_at,
                    state=ProviderRequestState.FAILED,
                    outcome=ProviderRequestOutcome.INVALID,
                    failure_code="INCREMENTAL_RESPONSE_LIMIT",
                    response_bytes=error.observed_bytes,
                    completed_at=completed_at,
                )
                raise IncrementalAcquisitionBlocked(
                    "incremental response limit exceeded"
                ) from error
            except (requests.RequestException, OSError, TimeoutError) as error:
                self._finish_failure(
                    request_id,
                    started_at=started_at,
                    state=ProviderRequestState.AMBIGUOUS,
                    outcome=None,
                    failure_code="INCREMENTAL_TRANSPORT_AMBIGUOUS",
                )
                raise IncrementalAcquisitionBlocked(
                    "incremental transport outcome is ambiguous"
                ) from error
            completed_at = _clock_utc(self.clock)
            body = response.body
            external = 1
            recorded = None
            if response.status_code != 200:
                self._finish_failure(
                    request_id,
                    started_at=started_at,
                    state=ProviderRequestState.FAILED,
                    outcome=(
                        ProviderRequestOutcome.HTTP_429
                        if response.status_code == 429
                        else ProviderRequestOutcome.HTTP_ERROR
                    ),
                    failure_code=f"HTTP_{response.status_code}",
                    response_bytes=len(body),
                    completed_at=completed_at,
                )
                raise IncrementalAcquisitionBlocked(
                    f"incremental provider HTTP {response.status_code}"
                )
        rows = None
        try:
            payload = json.loads(body)
            if (
                not isinstance(payload, dict)
                or payload.get("next_url")
                or not isinstance(payload.get("results"), list)
            ):
                raise ValueError("incremental response is incomplete")
            rows = payload["results"]
            revisions = normalize_native_bars(
                identity.security.security_id,
                identity.ticker,
                plan.interval,
                rows,
                observed_at=(
                    recorded.completion.completed_at
                    if recorded is not None else completed_at
                ),
                adjusted=False,
                availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
                replay_available_at=None,
                ingestion_segment_id=request_id,
            )
            complete = tuple(
                revision for revision in revisions
                if revision.bar_start >= plan.requested_from
                and revision.bar_end <= plan.requested_to
            )
            if (
                len(complete) != plan.expected_bars
                or complete[0].bar_start != plan.requested_from
                or complete[-1].bar_end != plan.requested_to
            ):
                raise ValueError("incremental response lacks exact bar coverage")
        except (ValueError, KeyError, TypeError, ArithmeticError) as error:
            if external:
                self._finish_failure(
                    request_id,
                    started_at=started_at,
                    state=ProviderRequestState.FAILED,
                    outcome=ProviderRequestOutcome.INVALID,
                    failure_code="INCREMENTAL_RESPONSE_INVALID",
                    response_bytes=len(body),
                    completed_at=completed_at,
                )
            raise IncrementalAcquisitionBlocked(
                "incremental response is invalid"
            ) from error
        if external:
            completion = ProviderRequestCompletion(
                ProviderRequestState.COMPLETE,
                started_at=started_at,
                completed_at=completed_at,
                outcome=ProviderRequestOutcome.OK,
                http_status=response.status_code,
                response_bytes=len(body),
                row_count=len(rows),
                pages_fetched=1,
                requested_from=plan.requested_from,
                requested_to=plan.requested_to,
                price_basis=PriceBasis.RAW_FINALIZED_MINUTE_CLOSE_V1,
                completeness=RequestCompleteness.COMPLETE,
            )
            with self.provider.transaction(APP_ROLE) as unit:
                payload_id = persist_raw_payload_cursor(
                    unit.cursor,
                    body,
                    arrived_at=completed_at,
                    request_id=request_id,
                )
                completion = replace(completion, payload_id=payload_id)
                finish_provider_request_cursor(
                    unit.cursor,
                    request_id,
                    completion,
                )
            reservation = self.budget_scope.bind(
                base_reservation,
                reserved_response_bytes=self.response_byte_limit + 1,
            )
            recorded = RecordedProviderRequest(reservation, completion)
        assert recorded is not None
        segment = uuid5(
            NAMESPACE_URL,
            "v2-incremental-equity:"
            f"{plan.cohort_id}:{plan.instrument_id}:{plan.interval}:"
            f"{plan.requested_from.isoformat()}:{plan.requested_to.isoformat()}:"
            f"{canonical_sha256(rows)}",
        )
        facts = tuple(
            DeveloperEquityBarAcquisitionFact(
                plan.instrument_id,
                replace(revision, ingestion_segment_id=segment),
            )
            for revision in complete
        )
        batch = CompletedDeveloperSourceBatch(
            DeveloperAcquisitionFactKind.EQUITY_BAR,
            segment,
            recorded.completion.started_at or recorded.reservation.reserved_at,
            recorded.completion.completed_at
            or recorded.reservation.reserved_at,
            facts,
            request_id=recorded.reservation.request_id,
        )
        return batch, recorded, external

    def collect(self, request) -> DeveloperSourceResult:
        if (
            request.cohort_id != self.plans[0].cohort_id
            or request.requested_at
            != max(plan.requested_to for plan in self.plans)
        ):
            raise ValueError("incremental acquisition request changed scope")
        batches = []
        evidence = []
        external = 0
        for plan in self.plans:
            batch, recorded, used = self._fetch(plan)
            batches.append(batch)
            evidence.append(recorded)
            external += used
        return DeveloperSourceResult(
            provider="MASSIVE_INCREMENTAL",
            batches=tuple(batches),
            provider_requests=external,
            response_bytes=sum(
                item.completion.response_bytes or 0 for item in evidence
            ),
            requests=tuple(evidence),
        )
