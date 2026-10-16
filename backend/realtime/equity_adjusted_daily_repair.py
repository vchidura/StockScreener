"""Bounded V2 adjusted-daily session repair from one grouped-market response."""
from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

import exchange_calendars
import requests

from equity.domain import BarAvailabilityMode
from equity.polygon import normalize_grouped_daily_bars
from options.data.polygon_http import (
    PolygonHttpTransport,
    PolygonResponseTooLarge,
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
    persist_raw_payload_cursor,
    reserve_provider_request_cursor,
)
from .contracts import canonical_json, canonical_sha256
from .database_access_v2 import APP_ROLE, V2ConnectionProvider
from .equity_bar_adapter import adapt_developer_equity_bar
from .equity_recent_repair import (
    NativeRepairBlocked,
    _clock_utc,
    _existing_facts,
    _materialize,
    recent_completed_sessions,
)
from .equity_validation_source import (
    EquityValidationIdentity,
    bind_equity_validation_adjustment_identity,
)


POLICY = "v2-adjusted-daily-session-repair-v1"
ENDPOINT_CLASS = "ADJUSTED_DAILY_SESSION_REPAIR"
ENDPOINT_POLICY = {
    "method": "GET",
    "base_url": "https://api.massive.com",
    "path": "/v2/aggs/grouped/locale/us/market/stocks/{session_date}",
    "params": {"adjusted": "true"},
}


@dataclass(frozen=True)
class AdjustedDailyRepairPlan:
    cohort_id: UUID
    identities: tuple[EquityValidationIdentity, ...]
    session_date: date
    authorized_until: datetime
    request_ceiling: int
    byte_ceiling: int
    response_byte_limit: int
    authorization_reference: str
    provider_authorization_reference: str
    expected_cost: str

    def __post_init__(self):
        if (
            not 1 <= len(self.identities) <= 400
            or len({row.ticker for row in self.identities}) != len(self.identities)
            or len({row.instrument_id for row in self.identities}) != len(self.identities)
            or self.authorized_until.tzinfo is None
            or self.authorized_until.utcoffset() is None
            or self.request_ceiling != 1
            or not 1024 <= self.response_byte_limit <= 16 * 1024 * 1024
            or self.byte_ceiling != self.response_byte_limit + 1
            or not all(value.strip() for value in (
                self.authorization_reference,
                self.provider_authorization_reference,
                self.expected_cost,
            ))
        ):
            raise ValueError("adjusted-daily repair scope, authorization or budget is invalid")
        object.__setattr__(
            self,
            "authorized_until",
            self.authorized_until.astimezone(timezone.utc),
        )
        for name in (
            "authorization_reference",
            "provider_authorization_reference",
            "expected_cost",
        ):
            object.__setattr__(self, name, getattr(self, name).strip())

    def identity(self):
        return {
            "policy": POLICY,
            "cohort_id": self.cohort_id,
            "members": tuple(
                (
                    row.instrument_id,
                    row.ticker,
                    row.security.security_id,
                    row.security.payload_sha256,
                )
                for row in self.identities
            ),
            "session_date": self.session_date,
            "endpoint_policy": ENDPOINT_POLICY,
            "authorized_until": self.authorized_until,
            "request_ceiling": self.request_ceiling,
            "byte_ceiling": self.byte_ceiling,
            "response_byte_limit": self.response_byte_limit,
            "authorization_reference": self.authorization_reference,
            "provider_authorization_reference": self.provider_authorization_reference,
            "expected_cost": self.expected_cost,
        }

    @property
    def sha256(self):
        return canonical_sha256(self.identity())


def _request_spec(plan: AdjustedDailyRepairPlan):
    url = (
        f"{ENDPOINT_POLICY['base_url']}"
        f"/v2/aggs/grouped/locale/us/market/stocks/{plan.session_date.isoformat()}"
    )
    params = dict(ENDPOINT_POLICY["params"])
    request_sha256 = canonical_sha256({
        "policy": POLICY,
        "plan_sha256": plan.sha256,
        "method": "GET",
        "url": url,
        "params": params,
    })
    return url, params, request_sha256


def _remember_plan(
    provider: V2ConnectionProvider,
    plan: AdjustedDailyRepairPlan,
    observed_at: datetime,
):
    identity = plan.identity()
    plan_id = uuid5(
        NAMESPACE_URL,
        f"{POLICY}:{plan.cohort_id}:{plan.authorization_reference}",
    )
    with provider.transaction(APP_ROLE) as unit:
        unit.cursor.execute(
            "SELECT DISTINCT member.instrument_id,instrument.security_id,"
            "profile.provider_tier,manifest.stock_count FROM rt.cohorts cohort "
            "JOIN rt.subscription_manifests manifest USING (manifest_id) "
            "JOIN rt.capability_profiles profile USING (profile_id) "
            "JOIN rt.subscription_members member USING (manifest_id) "
            "JOIN rt.instruments instrument USING (instrument_id) "
            "WHERE cohort.cohort_id=%s AND instrument.asset_class='EQUITY' "
            "AND member.channel='AM'",
            (str(plan.cohort_id),),
        )
        members = tuple(unit.cursor.fetchall())
        if (
            {(row[0], str(row[1])) for row in members}
            != {
                (row.instrument_id, str(row.security.security_id))
                for row in plan.identities
            }
            or any(
                row[2] != "MASSIVE_DEVELOPER"
                or row[3] != len(plan.identities)
                for row in members
            )
        ):
            raise ValueError("adjusted-daily repair does not match the exact cohort manifest")
        unit.cursor.execute(
            "SELECT config_sha256,scope_reference FROM rt.service_plans WHERE plan_id=%s",
            (str(plan_id),),
        )
        stored = unit.cursor.fetchone()
        expected = (plan.sha256, canonical_json(identity))
        if stored is not None:
            if tuple(stored) != expected:
                raise ValueError("durable adjusted-daily authorization conflicts with its plan")
            return
        if not observed_at <= plan.authorized_until <= observed_at + timedelta(hours=1):
            raise ValueError("adjusted-daily authorization must expire within one hour")
        unit.cursor.execute(
            "INSERT INTO rt.service_plans (plan_id,cohort_id,session_date,"
            "stage_catalog_version,enabled_stages,config_sha256,deadline,scope_reference) "
            "VALUES (%s,%s,%s,'stages_v2',ARRAY['EQUITY_INGEST'],%s,%s,%s) "
            "ON CONFLICT (plan_id) DO NOTHING",
            (
                str(plan_id),
                str(plan.cohort_id),
                plan.session_date,
                plan.sha256,
                plan.authorized_until,
                canonical_json(identity),
            ),
        )
        unit.cursor.execute(
            "SELECT config_sha256,scope_reference FROM rt.service_plans WHERE plan_id=%s",
            (str(plan_id),),
        )
        if tuple(unit.cursor.fetchone() or ()) != expected:
            raise ValueError("durable adjusted-daily authorization conflicts with its plan")


def _request(
    provider: V2ConnectionProvider,
    transport: PolygonHttpTransport,
    plan: AdjustedDailyRepairPlan,
    clock: Callable[[], datetime],
):
    calendar = exchange_calendars.get_calendar("XNYS")
    opened = calendar.session_open(plan.session_date.isoformat()).to_pydatetime()
    closed = calendar.session_close(plan.session_date.isoformat()).to_pydatetime()
    url, params, digest = _request_spec(plan)
    request_id = uuid5(NAMESPACE_URL, f"{POLICY}:{digest}")
    now = _clock_utc(clock)
    with provider.transaction(APP_ROLE) as unit:
        unit.cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"adjusted-daily-repair:{plan.sha256}",),
        )
        unit.cursor.execute(
            "SELECT state,reserved_at,started_at,completed_at,payload_id,"
            "request_sha256,account_alias,cohort_id,outcome,completeness,http_status "
            "FROM rt.provider_request_ledger WHERE request_id=%s",
            (str(request_id),),
        )
        stored = unit.cursor.fetchone()
        if stored is not None:
            if (
                stored[5] != digest
                or stored[6] != "MASSIVE_DEVELOPER"
                or str(stored[7]) != str(plan.cohort_id)
            ):
                raise ValueError("adjusted-daily request identity conflict")
            if (
                stored[0] != "COMPLETE"
                or stored[4] is None
                or tuple(stored[8:]) != ("OK", "COMPLETE", 200)
            ):
                raise NativeRepairBlocked("ADJUSTED_DAILY_REQUEST_UNRESOLVED_OR_FAILED")
            body = load_raw_payload_cursor(unit.cursor, UUID(str(stored[4])))
            reserved_at, started, completed = stored[1], stored[2], stored[3]
        else:
            if now > plan.authorized_until:
                raise NativeRepairBlocked("ADJUSTED_DAILY_AUTHORIZATION_EXPIRED")
            reservation = ProviderRequestReservation(
                request_id,
                "MASSIVE_DEVELOPER",
                ProviderRequestLane.EQUITY_HISTORY,
                ENDPOINT_CLASS,
                digest,
                now,
                cohort_id=plan.cohort_id,
            )
            reserve_provider_request_cursor(unit.cursor, reservation)
            body = None
            reserved_at = started = now
    external = body is None
    if external:
        try:
            response = transport.get(url, params)
        except NativeRepairBlocked as exc:
            with provider.transaction(APP_ROLE) as unit:
                finish_provider_request_cursor(
                    unit.cursor,
                    request_id,
                    ProviderRequestCompletion(
                        ProviderRequestState.FAILED,
                        started_at=started,
                        completed_at=_clock_utc(clock),
                        outcome=ProviderRequestOutcome.INVALID,
                        response_bytes=0,
                        failure_code=str(exc),
                    ),
                )
            raise
        except PolygonResponseTooLarge as exc:
            with provider.transaction(APP_ROLE) as unit:
                finish_provider_request_cursor(
                    unit.cursor,
                    request_id,
                    ProviderRequestCompletion(
                        ProviderRequestState.FAILED,
                        started_at=started,
                        completed_at=_clock_utc(clock),
                        outcome=ProviderRequestOutcome.INVALID,
                        response_bytes=exc.observed_bytes,
                        failure_code="ADJUSTED_DAILY_RESPONSE_LIMIT",
                    ),
                )
            raise NativeRepairBlocked(
                "ADJUSTED_DAILY_RESPONSE_LIMIT",
                external_requests=1,
            ) from exc
        except (requests.RequestException, OSError, TimeoutError) as exc:
            with provider.transaction(APP_ROLE) as unit:
                finish_provider_request_cursor(
                    unit.cursor,
                    request_id,
                    ProviderRequestCompletion(
                        ProviderRequestState.AMBIGUOUS,
                        started_at=started,
                        failure_code="ADJUSTED_DAILY_TRANSPORT_AMBIGUOUS",
                    ),
                )
            raise NativeRepairBlocked(
                "ADJUSTED_DAILY_TRANSPORT_AMBIGUOUS",
                external_requests=1,
            ) from exc
        completed = _clock_utc(clock)
        body = response.body
        valid_response = (
            response.status_code == 200
            and 0 < len(body) <= plan.response_byte_limit
        )
    else:
        valid_response = True
    rows = None
    try:
        payload = json.loads(body)
        if not valid_response or not isinstance(payload, dict) or payload.get("next_url"):
            raise ValueError("adjusted-daily response is not one complete bounded page")
        rows = payload["results"]
        if not isinstance(rows, list):
            raise ValueError("adjusted-daily results must be a list")
        identity_by_ticker = {row.ticker: row for row in plan.identities}
        selected = tuple(
            row for row in rows
            if str(row.get("T") or "").strip().upper() in identity_by_ticker
        )
        if {
            str(row.get("T") or "").strip().upper() for row in selected
        } != set(identity_by_ticker):
            raise ValueError("adjusted-daily response lacks exact cohort membership")
        revisions = normalize_grouped_daily_bars(
            selected,
            session_date=plan.session_date,
            security_ids={
                ticker: identity.security.security_id
                for ticker, identity in identity_by_ticker.items()
            },
            observed_at=completed,
            ingestion_segment_id=request_id,
            availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
            adjusted=True,
        )
        revisions = tuple(
            bind_equity_validation_adjustment_identity(revision)
            for revision in revisions
        )
        if {row.ticker for row in revisions} != set(identity_by_ticker):
            raise ValueError("adjusted-daily normalization changed cohort membership")
    except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
        if external:
            with provider.transaction(APP_ROLE) as unit:
                finish_provider_request_cursor(
                    unit.cursor,
                    request_id,
                    ProviderRequestCompletion(
                        ProviderRequestState.FAILED,
                        started_at=started,
                        completed_at=completed,
                        outcome=ProviderRequestOutcome.INVALID,
                        http_status=response.status_code,
                        response_bytes=len(body),
                        row_count=len(rows) if isinstance(rows, list) else None,
                        pages_fetched=1,
                        requested_from=opened,
                        requested_to=closed,
                        completeness=RequestCompleteness.INCOMPLETE,
                        failure_code="ADJUSTED_DAILY_RESPONSE_INVALID",
                    ),
                )
        raise NativeRepairBlocked(
            "ADJUSTED_DAILY_RESPONSE_INVALID",
            external_requests=int(external),
        ) from exc
    completion = ProviderRequestCompletion(
        ProviderRequestState.COMPLETE,
        started_at=started,
        completed_at=completed,
        outcome=ProviderRequestOutcome.OK,
        http_status=200,
        response_bytes=len(body),
        row_count=len(rows),
        pages_fetched=1,
        requested_from=opened,
        requested_to=closed,
        price_basis=PriceBasis.ADJUSTED_BAR_START_V1,
        completeness=RequestCompleteness.COMPLETE,
    )
    if external:
        with provider.transaction(APP_ROLE) as unit:
            payload_id = persist_raw_payload_cursor(
                unit.cursor,
                body,
                arrived_at=completed,
                request_id=request_id,
            )
            completion = replace(completion, payload_id=payload_id)
            finish_provider_request_cursor(unit.cursor, request_id, completion)
    else:
        completion = replace(completion, payload_id=UUID(str(stored[4])))
    reservation = ProviderRequestReservation(
        request_id,
        "MASSIVE_DEVELOPER",
        ProviderRequestLane.EQUITY_HISTORY,
        ENDPOINT_CLASS,
        digest,
        reserved_at,
        cohort_id=plan.cohort_id,
    )
    return revisions, RecordedProviderRequest(reservation, completion), int(external)


def execute_adjusted_daily_repair(
    provider: V2ConnectionProvider,
    transport: PolygonHttpTransport,
    plan: AdjustedDailyRepairPlan,
    *,
    clock: Callable[[], datetime],
):
    provider.verify()
    now = _clock_utc(clock)
    if recent_completed_sessions(plan.session_date, 1, known_by=now) != (
        plan.session_date,
    ):
        raise ValueError("adjusted-daily repair date is not a completed XNYS session")
    _remember_plan(provider, plan, now)
    revisions, recorded, external = _request(
        provider,
        transport,
        plan,
        clock,
    )
    identity_by_ticker = {row.ticker: row for row in plan.identities}
    facts = tuple(
        adapt_developer_equity_bar(
            revision,
            cohort_id=plan.cohort_id,
            instrument_id=identity_by_ticker[revision.ticker].instrument_id,
        )
        for revision in revisions
    )
    retained = _existing_facts(
        provider,
        plan,
        facts,
        known_by=_clock_utc(clock),
    )
    evidence = {
        (
            fact.revision.ingestion_segment_id,
            fact.revision.ticker,
            fact.revision.interval,
        ): recorded
        for fact in retained
    }
    result = _materialize(provider, plan, retained, evidence)
    observed_at = recorded.completion.completed_at
    return {
        "plan_sha256": plan.sha256,
        "session_date": plan.session_date.isoformat(),
        "tickers": sorted(identity_by_ticker),
        "provider_requests": external,
        "provider_response_replays": 1 - external,
        "source_observed_at": (
            observed_at.isoformat() if observed_at is not None else None
        ),
        "facts": len(retained),
        "bars_materialized": result.bars_materialized,
        "status": "COMPLETE",
        "release_advanced": False,
        "portal_readiness": "NOT_EVALUATED",
    }
