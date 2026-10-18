"""Explicit bounded native-session repair; no scheduler, release or audit authority."""
from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from urllib.parse import quote
from uuid import NAMESPACE_URL, UUID, uuid5

import exchange_calendars
import pandas as pd
import requests

from equity.domain import BarAvailabilityMode
from equity.calendar import latest_expected_market_time
from equity.polygon import normalize_native_bars
from options.data.polygon_http import PolygonHttpResponse, PolygonHttpTransport, PolygonResponseTooLarge

from .acquisition_ledger import (
    PriceBasis, ProviderRequestCompletion, ProviderRequestLane, ProviderRequestOutcome,
    ProviderRequestReservation, ProviderRequestState, RecordedProviderRequest,
    RequestCompleteness, finish_provider_request_cursor, load_raw_payload_cursor,
    persist_raw_payload_cursor, reserve_provider_request_cursor,
)
from .contracts import canonical_json, canonical_sha256
from .database_access import RealtimeDatabaseRole
from .database_access_v2 import APP_ROLE, V2ConnectionProvider
from .equity_bar_adapter import adapt_developer_equity_bar
from .equity_bulk_backfill import build_equity_bulk_backfill_plan, execute_equity_bulk_backfill
from .equity_interval_derivation import REALTIME_DERIVATION_SOURCES, derive_realtime_interval_facts
from .equity_interval_finalizer import _bar_revision
from .equity_validation_source import EquityValidationIdentity
from .stage_database_v2 import V2WorkerDatabase

SUPPORTED_NATIVE_INTERVALS = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
}
NATIVE_INTERVALS = {
    interval: SUPPORTED_NATIVE_INTERVALS[interval]
    for interval in ("5m", "15m", "30m")
}
DERIVED_INTERVALS = ("15m", "30m", "1h", "1d", "1wk", "1mo")
POLICY = "v2-native-recent-session-repair-v1"
NATIVE_ENDPOINT_POLICY = {
    "method": "GET",
    "base_url": "https://api.massive.com/v2/aggs/ticker",
    "path": "/{ticker}/range/{width}/minute/{opened_ms}/{closed_ms_minus_one}",
    "params": {"adjusted": "false", "sort": "asc", "limit": "50000"},
}
DATABASE_DECIMAL_QUANTUM = Decimal("0.00000001")


class NativeRepairBlocked(RuntimeError):
    def __init__(self, reason_code: str, *, external_requests: int = 0):
        super().__init__(reason_code)
        self.external_requests = external_requests


@dataclass(frozen=True)
class NativeRequestSpec:
    url: str
    params: Mapping[str, str]
    request_sha256: str


def _clock_utc(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("native repair clock must be timezone-aware")
    return value.astimezone(timezone.utc)


def _database_decimal(value):
    return (
        value.quantize(DATABASE_DECIMAL_QUANTUM, rounding=ROUND_HALF_UP)
        if value is not None else None
    )


class AuthorizedNativeRepairTransport:
    """Check admission after pacing, immediately before the bounded HTTP read."""

    def __init__(
        self, delegate: PolygonHttpTransport, authorized_until: datetime,
        clock: Callable[[], datetime],
    ):
        self.delegate, self.authorized_until, self.clock = delegate, authorized_until, clock

    def get(self, url: str, params: Mapping[str, str]) -> PolygonHttpResponse:
        if _clock_utc(self.clock) > self.authorized_until:
            raise NativeRepairBlocked("NATIVE_REPAIR_AUTHORIZATION_EXPIRED")
        return self.delegate.get(url, params)


def validate_native_repair_budget(
    member_count,
    session_count,
    requests_limit,
    bytes_limit,
    response_limit,
    *,
    native_intervals=tuple(NATIVE_INTERVALS),
):
    if any(type(value) is not int for value in (
        member_count, session_count, requests_limit, bytes_limit, response_limit,
    )):
        raise ValueError("native repair budget fields must be integers")
    intervals = tuple(dict.fromkeys(native_intervals))
    required = member_count * session_count * len(intervals)
    if (
        not 1 <= member_count <= 400 or not 1 <= session_count <= 10
        or not intervals
        or any(
            interval not in SUPPORTED_NATIVE_INTERVALS
            for interval in intervals
        )
        or requests_limit != required or required > 2000
        or not 1024 <= response_limit <= 2 * 1024 * 1024
        or not required * (response_limit + 1) <= bytes_limit <= 512 * 1024 * 1024
    ):
        raise ValueError("native repair budget does not match its exact request scope")
    return required


@dataclass(frozen=True)
class NativeRepairPlan:
    cohort_id: UUID
    identities: tuple[EquityValidationIdentity, ...]
    sessions: tuple[date, ...]
    authorized_until: datetime
    request_ceiling: int
    byte_ceiling: int
    response_byte_limit: int
    authorization_reference: str
    provider_authorization_reference: str
    expected_cost: str
    native_intervals: tuple[str, ...] = tuple(NATIVE_INTERVALS)
    identity_continuity: tuple[tuple[object, ...], ...] = ()

    def __post_init__(self):
        validate_native_repair_budget(
            len(self.identities), len(self.sessions), self.request_ceiling,
            self.byte_ceiling, self.response_byte_limit,
            native_intervals=self.native_intervals,
        )
        intervals = tuple(dict.fromkeys(self.native_intervals))
        if (
            not self.identities or not 1 <= len(self.sessions) <= 10
            or not intervals
            or any(
                interval not in SUPPORTED_NATIVE_INTERVALS
                for interval in intervals
            )
            or (
                self.identity_continuity
                and len(self.identity_continuity) != len(self.identities)
            )
            or len({row.ticker for row in self.identities}) != len(self.identities)
            or len({row.instrument_id for row in self.identities}) != len(self.identities)
            or len(set(self.sessions)) != len(self.sessions)
            or tuple(sorted(self.sessions)) != self.sessions
            or self.authorized_until.tzinfo is None
            or self.authorized_until.utcoffset() is None
            or not all(value.strip() for value in (
                self.authorization_reference, self.provider_authorization_reference,
                self.expected_cost,
            ))
        ):
            raise ValueError("native repair scope, authorization or budget is invalid")
        object.__setattr__(self, "authorized_until", self.authorized_until.astimezone(timezone.utc))
        object.__setattr__(self, "native_intervals", intervals)
        for name in ("authorization_reference", "provider_authorization_reference", "expected_cost"):
            object.__setattr__(self, name, getattr(self, name).strip())

    def identity(self):
        return {
            "policy": POLICY, "cohort_id": self.cohort_id,
            "members": tuple(
                (row.instrument_id, row.ticker, row.security.security_id, row.security.payload_sha256)
                for row in self.identities
            ),
            "identity_continuity": self.identity_continuity,
            "sessions": self.sessions,
            "native_intervals": self.native_intervals,
            "native_widths": {
                interval: SUPPORTED_NATIVE_INTERVALS[interval]
                for interval in self.native_intervals
            },
            "derived_intervals": DERIVED_INTERVALS,
            "derivation_sources": REALTIME_DERIVATION_SOURCES,
            "endpoint_policy": NATIVE_ENDPOINT_POLICY,
            "authorized_until": self.authorized_until,
            "request_ceiling": self.request_ceiling, "byte_ceiling": self.byte_ceiling,
            "response_byte_limit": self.response_byte_limit,
            "reserved_bytes_each": self.response_byte_limit + 1,
            "authorization_reference": self.authorization_reference,
            "provider_authorization_reference": self.provider_authorization_reference,
            "expected_cost": self.expected_cost,
        }

    @property
    def sha256(self):
        return canonical_sha256(self.identity())


def recent_completed_sessions(through_session: date, count: int, *, known_by: datetime):
    if known_by.tzinfo is None or known_by.utcoffset() is None or not 1 <= count <= 10:
        raise ValueError("recent repair requires an aware clock and 1-10 sessions")
    calendar = exchange_calendars.get_calendar("XNYS")
    end = calendar.date_to_session(pd.Timestamp(through_session), direction="previous")
    if known_by < calendar.session_close(end).to_pydatetime() + timedelta(minutes=15):
        raise ValueError("recent repair session is not complete under the provider delay")
    return tuple(
        value.date() for value in calendar.sessions_window(end, -count)
    )


def _native_request_spec(plan, identity, session, interval) -> NativeRequestSpec:
    calendar = exchange_calendars.get_calendar("XNYS")
    opened = calendar.session_open(str(session)).to_pydatetime()
    closed = calendar.session_close(str(session)).to_pydatetime()
    if interval not in plan.native_intervals:
        raise ValueError("native repair interval is outside the plan")
    width = SUPPORTED_NATIVE_INTERVALS[interval]
    url = (
        f"{NATIVE_ENDPOINT_POLICY['base_url']}/{quote(identity.ticker, safe='')}"
        f"/range/{width}/minute/{int(opened.timestamp() * 1000)}/"
        f"{int(closed.timestamp() * 1000) - 1}"
    )
    params = dict(NATIVE_ENDPOINT_POLICY["params"])
    return NativeRequestSpec(
        url,
        params,
        canonical_sha256({
            "policy": POLICY,
            "plan_sha256": plan.sha256,
            "method": "GET",
            "url": url,
            "params": params,
        }),
    )


def _remember_plan(provider, plan, observed_at):
    identity = plan.identity()
    plan_id = uuid5(
        NAMESPACE_URL, f"{POLICY}:{plan.cohort_id}:{plan.authorization_reference}"
    )
    with provider.transaction(APP_ROLE) as unit:
        unit.cursor.execute(
            "SELECT DISTINCT member.instrument_id,instrument.security_id,"
            "profile.provider_tier,manifest.stock_count FROM rt.cohorts cohort "
            "JOIN rt.subscription_manifests manifest USING (manifest_id) "
            "JOIN rt.capability_profiles profile USING (profile_id) "
            "JOIN rt.subscription_members member USING (manifest_id) "
            "JOIN rt.instruments instrument USING (instrument_id) "
            "WHERE cohort.cohort_id=%s AND instrument.asset_class='EQUITY' AND member.channel='AM'",
            (str(plan.cohort_id),),
        )
        members = tuple(unit.cursor.fetchall())
        if (
            {(row[0], str(row[1])) for row in members}
            != {(row.instrument_id, str(row.security.security_id)) for row in plan.identities}
            or any(row[2] != "MASSIVE_DEVELOPER" or row[3] != len(plan.identities) for row in members)
        ):
            raise ValueError("native repair does not match the exact cohort manifest")
        unit.cursor.execute(
            "SELECT config_sha256,scope_reference FROM rt.service_plans WHERE plan_id=%s",
            (str(plan_id),),
        )
        stored = unit.cursor.fetchone()
        expected = (plan.sha256, canonical_json(identity))
        if stored is not None:
            if tuple(stored) != expected:
                raise ValueError("durable native repair authorization conflicts with its plan")
            return
        if not observed_at <= plan.authorized_until <= observed_at + timedelta(hours=1):
            raise ValueError("native repair authorization must expire within one hour")
        unit.cursor.execute(
            "INSERT INTO rt.service_plans (plan_id,cohort_id,session_date,"
            "stage_catalog_version,enabled_stages,config_sha256,deadline,scope_reference) "
            "VALUES (%s,%s,%s,'stages_v2',ARRAY['EQUITY_INGEST'],%s,%s,%s) "
            "ON CONFLICT (plan_id) DO NOTHING",
            (
                str(plan_id), str(plan.cohort_id), plan.sessions[-1], plan.sha256,
                plan.authorized_until, canonical_json(identity),
            ),
        )
        unit.cursor.execute(
            "SELECT config_sha256,scope_reference FROM rt.service_plans WHERE plan_id=%s",
            (str(plan_id),),
        )
        if tuple(unit.cursor.fetchone() or ()) != (plan.sha256, canonical_json(identity)):
            raise ValueError("durable native repair authorization conflicts with its plan")


def _request(provider, transport, plan, identity, session, interval, clock):
    calendar = exchange_calendars.get_calendar("XNYS")
    opened = calendar.session_open(str(session)).to_pydatetime()
    closed = calendar.session_close(str(session)).to_pydatetime()
    if interval not in plan.native_intervals:
        raise ValueError("native repair interval is outside the plan")
    width = SUPPORTED_NATIVE_INTERVALS[interval]
    request = _native_request_spec(plan, identity, session, interval)
    digest = request.request_sha256
    request_id = uuid5(NAMESPACE_URL, f"{POLICY}:{digest}")
    now = _clock_utc(clock)
    with provider.transaction(APP_ROLE) as unit:
        unit.cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"native-repair:{plan.sha256}",),
        )
        unit.cursor.execute(
            "SELECT state,reserved_at,started_at,completed_at,payload_id,"
            "request_sha256,account_alias,cohort_id,outcome,completeness,http_status "
            "FROM rt.provider_request_ledger "
            "WHERE request_id=%s",
            (str(request_id),),
        )
        stored = unit.cursor.fetchone()
        if stored is not None:
            if (
                stored[5] != digest or stored[6] != "MASSIVE_DEVELOPER"
                or str(stored[7]) != str(plan.cohort_id)
            ):
                raise ValueError("native repair request identity conflict")
            if (
                stored[0] != "COMPLETE" or stored[4] is None
                or tuple(stored[8:]) != ("OK", "COMPLETE", 200)
            ):
                raise NativeRepairBlocked("NATIVE_REQUEST_UNRESOLVED_OR_FAILED")
            body = load_raw_payload_cursor(unit.cursor, UUID(str(stored[4])))
            started, completed = stored[2], stored[3]
            reserved_at = stored[1]
        else:
            if now > plan.authorized_until:
                raise NativeRepairBlocked("NATIVE_REPAIR_AUTHORIZATION_EXPIRED")
            reservation = ProviderRequestReservation(
                request_id, "MASSIVE_DEVELOPER", ProviderRequestLane.EQUITY_HISTORY,
                "NATIVE_SESSION_REPAIR", digest, now, cohort_id=plan.cohort_id,
            )
            reserve_provider_request_cursor(unit.cursor, reservation)
            body = None
            reserved_at = started = now
    external = body is None
    if external:
        try:
            response = transport.get(request.url, request.params)
        except NativeRepairBlocked as exc:
            if str(exc) != "NATIVE_REPAIR_AUTHORIZATION_EXPIRED":
                raise
            with provider.transaction(APP_ROLE) as unit:
                finish_provider_request_cursor(
                    unit.cursor, request_id,
                    ProviderRequestCompletion(
                        ProviderRequestState.FAILED, started_at=started,
                        completed_at=_clock_utc(clock), outcome=ProviderRequestOutcome.INVALID,
                        response_bytes=0, failure_code="NATIVE_REPAIR_AUTHORIZATION_EXPIRED",
                    ),
                )
            raise
        except PolygonResponseTooLarge as exc:
            with provider.transaction(APP_ROLE) as unit:
                finish_provider_request_cursor(
                    unit.cursor, request_id,
                    ProviderRequestCompletion(
                        ProviderRequestState.FAILED, started_at=started,
                        completed_at=_clock_utc(clock), outcome=ProviderRequestOutcome.INVALID,
                        response_bytes=exc.observed_bytes,
                        failure_code="NATIVE_REPAIR_RESPONSE_LIMIT",
                    ),
                )
            raise NativeRepairBlocked("NATIVE_REPAIR_RESPONSE_LIMIT", external_requests=1) from exc
        except (requests.RequestException, OSError, TimeoutError) as exc:
            with provider.transaction(APP_ROLE) as unit:
                finish_provider_request_cursor(
                    unit.cursor, request_id,
                    ProviderRequestCompletion(
                        ProviderRequestState.AMBIGUOUS, started_at=started,
                        failure_code="NATIVE_REPAIR_TRANSPORT_AMBIGUOUS",
                    ),
                )
            raise NativeRepairBlocked("NATIVE_REPAIR_TRANSPORT_AMBIGUOUS", external_requests=1) from exc
        completed = _clock_utc(clock)
        body = response.body
        valid_response = response.status_code == 200 and 0 < len(body) <= plan.response_byte_limit
    else:
        valid_response = True
    rows = None
    try:
        payload = json.loads(body)
        if not valid_response or not isinstance(payload, dict) or payload.get("next_url"):
            raise ValueError("native repair response is not a complete bounded aggregate page")
        rows = payload["results"]
        if not isinstance(rows, list):
            raise ValueError("native repair aggregates must be a list")
        revisions = normalize_native_bars(
            identity.security.security_id, identity.ticker, interval, rows,
            observed_at=completed, adjusted=False,
            availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
            replay_available_at=completed, ingestion_segment_id=request_id,
        )
        expected = tuple(
            opened + timedelta(minutes=width * index)
            for index in range(1, int((closed - opened).total_seconds() // (width * 60)) + 1)
        )
        if tuple(row.bar_end for row in revisions) != expected:
            raise ValueError("native repair full-session membership is incomplete")
    except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
        if external:
            with provider.transaction(APP_ROLE) as unit:
                finish_provider_request_cursor(
                    unit.cursor, request_id,
                    ProviderRequestCompletion(
                        ProviderRequestState.FAILED, started_at=started, completed_at=completed,
                        outcome=ProviderRequestOutcome.INVALID, http_status=response.status_code,
                        response_bytes=len(body), row_count=len(rows) if isinstance(rows, list) else None,
                        pages_fetched=1, requested_from=opened, requested_to=closed,
                        completeness=RequestCompleteness.INCOMPLETE,
                        failure_code="NATIVE_REPAIR_RESPONSE_INVALID",
                    ),
                )
        raise NativeRepairBlocked(
            "NATIVE_REPAIR_RESPONSE_INVALID", external_requests=int(external)
        ) from exc
    completion = ProviderRequestCompletion(
        ProviderRequestState.COMPLETE, started_at=started, completed_at=completed,
        outcome=ProviderRequestOutcome.OK, http_status=200, response_bytes=len(body),
        row_count=len(rows), pages_fetched=1, requested_from=opened, requested_to=closed,
        price_basis=PriceBasis.RAW_FINALIZED_MINUTE_CLOSE_V1,
        completeness=RequestCompleteness.COMPLETE,
    )
    if external:
        with provider.transaction(APP_ROLE) as unit:
            payload_id = persist_raw_payload_cursor(
                unit.cursor, body, arrived_at=completed, request_id=request_id
            )
            completion = replace(completion, payload_id=payload_id)
            finish_provider_request_cursor(unit.cursor, request_id, completion)
    else:
        completion = replace(completion, payload_id=UUID(str(stored[4])))
    reservation = ProviderRequestReservation(
        request_id, "MASSIVE_DEVELOPER", ProviderRequestLane.EQUITY_HISTORY,
        "NATIVE_SESSION_REPAIR", digest, reserved_at, cohort_id=plan.cohort_id,
    )
    return revisions, RecordedProviderRequest(reservation, completion), int(external)


def _existing_facts(provider, plan, facts, *, known_by: datetime):
    with provider.transaction(APP_ROLE, read_only=True, dict_cursor=True) as unit:
        unit.cursor.execute(
            "SELECT revision.* FROM rt.bar_catalog catalog JOIN rt.bar_revisions revision "
            "ON revision.bar_revision_id=catalog.bar_revision_id "
            "AND revision.observed_at=catalog.observed_at "
            "WHERE catalog.bar_revision_id=ANY(%s::uuid[])",
            ([str(fact.revision.bar_revision_id) for fact in facts],),
        )
        stored = {UUID(str(row["bar_revision_id"])): row for row in unit.cursor.fetchall()}
    retained = []
    for fact in facts:
        row = stored.get(fact.revision.bar_revision_id)
        if row is not None:
            if (
                str(row["cohort_id"]) != str(plan.cohort_id)
                or row["instrument_id"] != fact.instrument_id
                or row["payload_sha256"] != fact.revision.payload_sha256
            ):
                raise ValueError("native repair revision conflicts with retained identity")
            original = _bar_revision(row)
            identity_fields = (
                "security_id", "ticker", "interval", "bar_start", "bar_end",
                "session_date", "session_scope", "adjusted", "source_kind",
                "availability_mode", "is_final", "source_bar_revision_ids",
                "transaction_count",
            )
            decimal_fields = (
                "open_price", "high_price", "low_price", "close_price",
                "volume", "vwap",
            )
            mismatches = [
                name for name in identity_fields
                if getattr(original, name) != getattr(fact.revision, name)
            ]
            mismatches.extend(
                name for name in decimal_fields
                if getattr(original, name) != _database_decimal(
                    getattr(fact.revision, name)
                )
            )
            if mismatches:
                raise ValueError(
                    "native repair payload disagrees with retained bar values: "
                    f"revision_id={fact.revision.bar_revision_id} "
                    f"ticker={fact.revision.ticker} interval={fact.revision.interval} "
                    f"bar_start={fact.revision.bar_start.isoformat()} "
                    f"fields={','.join(mismatches)}"
                )
            if any(
                value is not None and value > known_by for value in (
                    original.system_observed_at, original.replay_available_at,
                    original.provider_published_at, row["created_at"],
                )
            ):
                raise ValueError("retained bar is not visible at the repair inspection clock")
            # Preserve JSON decimal spelling; PostgreSQL numeric adds scale on reads.
            fact = replace(fact, revision=replace(
                fact.revision,
                system_observed_at=original.system_observed_at,
                replay_available_at=original.replay_available_at,
                provider_published_at=original.provider_published_at,
                ingestion_segment_id=original.ingestion_segment_id,
                source_bar_revision_ids=original.source_bar_revision_ids,
                supersedes_bar_revision_id=original.supersedes_bar_revision_id,
                quality_codes=original.quality_codes,
            ))
        retained.append(fact)
    return tuple(retained)


def _materialize(provider, plan, facts, evidence=None):
    batch_plan = build_equity_bulk_backfill_plan(
        facts, cohort_id=plan.cohort_id,
        tickers=tuple(sorted({fact.revision.ticker for fact in facts})),
    )
    return execute_equity_bulk_backfill(
        V2WorkerDatabase(provider, RealtimeDatabaseRole.INGRESS),
        V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE),
        batch_plan, recorded_at=max(fact.revision.system_observed_at for fact in facts),
        request_evidence_by_segment=evidence,
        retained_lineage="V2",
    )


def execute_recent_native_repair(provider: V2ConnectionProvider, transport, plan, *, clock):
    provider.verify()
    now = _clock_utc(clock)
    for session in plan.sessions:
        if recent_completed_sessions(session, 1, known_by=now) != (session,):
            raise ValueError("native repair plan contains a non-session date")
    _remember_plan(provider, plan, now)
    outcomes = []
    observations: list[datetime] = []
    external = replayed = 0
    external_response_bytes = sealed_response_bytes = 0
    for session in plan.sessions:
        for interval in plan.native_intervals:
            facts, evidence_by_revision, unavailable = [], {}, []
            for identity in plan.identities:
                try:
                    revisions, recorded, used = _request(
                        provider, transport, plan, identity, session, interval, clock
                    )
                except NativeRepairBlocked as exc:
                    external += exc.external_requests
                    unavailable.append({"ticker": identity.ticker, "reason_code": str(exc)})
                    continue
                external += used
                replayed += 1 - used
                response_bytes = int(
                    recorded.completion.response_bytes or 0
                )
                sealed_response_bytes += response_bytes
                if used:
                    external_response_bytes += response_bytes
                observed_at = recorded.completion.completed_at
                if observed_at is None:
                    raise ValueError("sealed native repair response lacks its observation clock")
                observations.append(observed_at)
                for revision in revisions:
                    facts.append(adapt_developer_equity_bar(
                        revision, cohort_id=plan.cohort_id, instrument_id=identity.instrument_id
                    ))
                    evidence_by_revision[revision.bar_revision_id] = recorded
            if facts:
                retained = _existing_facts(provider, plan, facts, known_by=_clock_utc(clock))
                evidence = {
                    (fact.revision.ingestion_segment_id, fact.revision.ticker, fact.revision.interval):
                    evidence_by_revision[fact.revision.bar_revision_id]
                    for fact in retained
                }
                _materialize(provider, plan, retained, evidence)
            outcomes.append({
                "session": str(session), "interval": interval,
                "status": "BLOCKED" if unavailable else "COMPLETE",
                "unavailable": unavailable,
            })
    calendar = exchange_calendars.get_calendar("XNYS")
    history_start = calendar.sessions_window(pd.Timestamp(plan.sessions[-1]), -64)[0].date()
    repair_boundary = calendar.session_close(str(plan.sessions[-1])).to_pydatetime()
    for identity in plan.identities:
        known_by = _clock_utc(clock)
        with provider.transaction(APP_ROLE, read_only=True, dict_cursor=True) as unit:
            unit.cursor.execute(
                "SELECT revision.* FROM rt.bar_current cur JOIN rt.bar_revisions revision "
                "ON revision.bar_revision_id=cur.bar_revision_id AND revision.observed_at=cur.observed_at "
                "WHERE cur.cohort_id=%s AND cur.instrument_id=%s AND cur.session_scope='RTH' "
                "AND revision.is_final AND revision.observed_at<=%s AND revision.created_at<=%s "
                "AND (revision.replay_available_at IS NULL OR revision.replay_available_at<=%s) "
                "AND (revision.provider_published_at IS NULL OR revision.provider_published_at<=%s) "
                "AND ((cur.interval='5m' AND cur.adjustment_basis='RAW_UNADJUSTED' "
                "AND revision.session_date BETWEEN %s AND %s) OR "
                "(cur.interval='1d' AND cur.adjustment_basis='ADJUSTED' "
                "AND revision.session_date BETWEEN %s AND %s)) "
                "ORDER BY revision.interval,revision.bar_start",
                (
                    str(plan.cohort_id), identity.instrument_id,
                    known_by, known_by, known_by, known_by,
                    plan.sessions[0], plan.sessions[-1], history_start, plan.sessions[-1],
                ),
            )
            sources = tuple(adapt_developer_equity_bar(
                _bar_revision(row), cohort_id=plan.cohort_id, instrument_id=identity.instrument_id
            ) for row in unit.cursor.fetchall())
        for interval in DERIVED_INTERVALS:
            derived = derive_realtime_interval_facts(
                identity.security, sources, target_interval=interval,
                observed_at=_clock_utc(clock), ingestion_segment_id=uuid5(
                    NAMESPACE_URL, f"{plan.sha256}:{identity.instrument_id}:{interval}"
                ),
            )
            target = latest_expected_market_time(
                repair_boundary + timedelta(microseconds=1), interval
            )
            if not derived or not any(fact.revision.bar_end == target for fact in derived):
                outcomes.append({
                    "ticker": identity.ticker, "interval": interval, "status": "BLOCKED",
                    "reason_code": "COMPLETE_RETAINED_DERIVATION_INPUTS_MISSING",
                })
                continue
            _materialize(
                provider, plan, _existing_facts(
                    provider, plan, derived, known_by=_clock_utc(clock)
                ),
            )
            outcomes.append({"ticker": identity.ticker, "interval": interval, "status": "COMPLETE"})
    return {
        "plan_sha256": plan.sha256, "outcomes": outcomes, "provider_requests": external,
        "provider_response_replays": replayed,
        "source_observed_from": min(observations).isoformat() if observations else None,
        "source_observed_through": max(observations).isoformat() if observations else None,
        "inspected_at": _clock_utc(clock).isoformat(),
        "provider_response_bytes": external_response_bytes,
        "sealed_response_bytes": sealed_response_bytes,
        "status": "COMPLETE" if all(row["status"] == "COMPLETE" for row in outcomes) else "BLOCKED",
        "release_advanced": False, "portal_readiness": "NOT_EVALUATED",
    }
