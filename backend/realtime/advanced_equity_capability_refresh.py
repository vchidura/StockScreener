from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Protocol
from uuid import UUID

from psycopg2.extras import Json

from options.calendar import OptionExchangeCalendar

from .advanced_equity_control_plane import (
    AdvancedEquityControlPlanePlan,
    build_advanced_equity_control_plane_plan,
    persist_advanced_equity_control_plane_cursor,
)
from .advanced_gateway import (
    AdvancedGateway,
    GatewayConfiguration,
)
from .advanced_realtime_desired_state import DesiredMemberKind
from .advanced_realtime_desired_state_repository import (
    load_current_desired_state_revision_cursor,
)
from .buffer import BufferLimits, RawMessageBuffer
from .contracts import (
    AssetClass,
    MarketChannel,
    SubscriptionManifest,
    SubscriptionMember,
    UniverseManifest,
    canonical_sha256,
)


UTC = timezone.utc
CAPABILITY_REFRESH_RESOURCE = "advanced-equity:capability-refresh"
MASSIVE_STOCKS_CONNECTION_RESOURCE = (
    "provider-connection:massive:STOCKS"
)
CAPABILITY_REFRESH_SCOPE_TYPE = "RESIDENT"
CAPABILITY_REFRESH_SCOPE_ID = "ADVANCED_EQUITY_CAPABILITY_REFRESH"
CAPABILITY_REFRESH_CAPABILITY = "CAPABILITY_REFRESH"


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def capability_probe_not_before(
    session_date: date,
    *,
    evidence_validity_seconds: int,
    calendar: OptionExchangeCalendar | None = None,
) -> datetime:
    if evidence_validity_seconds < 300:
        raise ValueError(
            "capability evidence validity must be at least 300s"
        )
    exchange = calendar or OptionExchangeCalendar("XNYS")
    return exchange.session_close(session_date) - timedelta(
        seconds=evidence_validity_seconds
    )


@dataclass(frozen=True, slots=True)
class AdvancedCapabilityRefreshRequest:
    revision_id: UUID
    revision_sha256: str
    session_date: date
    security_ids: tuple[tuple[str, UUID], ...]
    source_profile_id: UUID
    current_profile_id: UUID
    current_expires_at: datetime
    required_through: datetime
    probe_not_before: datetime
    configuration_sha256: str
    schema_sha256: str
    security_policy_sha256: str
    requires_refresh: bool

    @property
    def tickers(self) -> tuple[str, ...]:
        return tuple(ticker for ticker, _security_id in self.security_ids)

    @property
    def identity_sha256(self) -> str:
        return canonical_sha256({
            "revision_sha256": self.revision_sha256,
            "session_date": self.session_date,
            "security_ids": self.security_ids,
            "configuration_sha256": self.configuration_sha256,
            "required_through": self.required_through,
        })


@dataclass(frozen=True, slots=True)
class AdvancedCapabilityEvidence:
    tickers: tuple[str, ...]
    feed: str
    channel: str
    configuration_sha256: str
    checked_at: datetime
    expires_at: datetime
    entitlement_evidence_sha256: str
    authorization_reference: str
    accepted_membership: tuple[str, ...]
    rejected_membership: tuple[str, ...]
    provider_requests: int
    received_control_messages: int
    received_bytes: int
    subscribe_request_bytes: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "checked_at",
            _utc(self.checked_at, "capability checked_at"),
        )
        object.__setattr__(
            self,
            "expires_at",
            _utc(self.expires_at, "capability expires_at"),
        )
        if self.expires_at <= self.checked_at:
            raise ValueError("capability evidence expiry is invalid")


class AdvancedCapabilityRefreshSource(Protocol):
    def refresh(
        self,
        request: AdvancedCapabilityRefreshRequest,
    ) -> AdvancedCapabilityEvidence: ...


@dataclass(frozen=True, slots=True)
class AdvancedCapabilityRefreshOutcome:
    status: str
    checked_at: datetime
    last_success_at: datetime | None
    next_retry_at: datetime | None
    profile_id: UUID
    request_identity_sha256: str
    plan: AdvancedEquityControlPlanePlan | None = None
    evidence: AdvancedCapabilityEvidence | None = None
    reason_code: str | None = None


def _capability_probe_manifest(
    request: AdvancedCapabilityRefreshRequest,
) -> SubscriptionManifest:
    universe = UniverseManifest(
        version=f"advanced-equity-{len(request.tickers)}-am-v1",
        equity_tickers=request.tickers,
        option_underlyings=(),
    )
    members = tuple(
        SubscriptionMember(
            AssetClass.EQUITY,
            ticker,
            ticker,
            MarketChannel.MINUTE_AGGREGATE,
        )
        for ticker in request.tickers
    )
    return SubscriptionManifest(
        version="advanced-equity-capability-probe-v1",
        session_date=request.session_date,
        universe=universe,
        capability_profile_sha256=canonical_sha256({
            "configuration_sha256": request.configuration_sha256,
            "revision_sha256": request.revision_sha256,
        }),
        allocation_policy_sha256=canonical_sha256({
            "version": "advanced-equity-capability-probe-allocation-v1",
            "members": members,
        }),
        members=members,
        maximum_option_quote_contracts=0,
    )


class AdvancedGatewayCapabilityRefreshSource:
    def __init__(
        self,
        *,
        api_key: str | None,
        clock,
        evidence_validity_seconds: int = 86_400,
        probe_timeout_seconds: float = 10.0,
        probe_byte_ceiling: int = 64 * 1024,
        gateway_factory=None,
    ) -> None:
        if evidence_validity_seconds < 300:
            raise ValueError(
                "capability evidence validity must be at least 300s"
            )
        if gateway_factory is None and not str(api_key or "").strip():
            raise ValueError("capability gateway requires an API key")
        self._api_key = api_key
        self._clock = clock
        self._validity_seconds = evidence_validity_seconds
        if not 0 < probe_timeout_seconds <= 60:
            raise ValueError("capability probe timeout is invalid")
        if not 1024 <= probe_byte_ceiling <= 1024 * 1024:
            raise ValueError("capability probe byte ceiling is invalid")
        self._probe_timeout_seconds = probe_timeout_seconds
        self._probe_byte_ceiling = probe_byte_ceiling
        self._gateway_factory = gateway_factory

    def _gateway(self, request):
        manifest = _capability_probe_manifest(request)
        if self._gateway_factory is not None:
            return self._gateway_factory(manifest)
        return AdvancedGateway(
            GatewayConfiguration.stocks(),
            manifest,
            RawMessageBuffer(
                BufferLimits(
                    maximum_events=1,
                    maximum_bytes=1024,
                    maximum_age_ns=1_000_000_000,
                ),
                monotonic_ns=time.monotonic_ns,
            ),
            api_key=self._api_key,
            monotonic_ns=time.monotonic_ns,
        )

    def refresh(
        self,
        request: AdvancedCapabilityRefreshRequest,
    ) -> AdvancedCapabilityEvidence:
        checked = _utc(self._clock(), "capability source clock")
        result = asyncio.run(
            self._gateway(request).run_subscription_probe(
                timeout_seconds=self._probe_timeout_seconds,
                maximum_control_bytes=self._probe_byte_ceiling,
            )
        )
        accepted = tuple(sorted(
            subscription.split(".", 1)[1]
            for subscription in result.accepted_subscriptions
        ))
        rejected = tuple(sorted(
            subscription.split(".", 1)[1]
            for subscription in result.rejected_subscriptions
        ))
        evidence_sha256 = canonical_sha256({
            "version": "advanced-equity-capability-refresh-v1",
            "tickers": request.tickers,
            "feed": "STOCKS",
            "channel": "AM",
            "configuration_sha256": request.configuration_sha256,
            "accepted_membership": accepted,
            "rejected_membership": rejected,
            "received_control_messages": (
                result.received_control_messages
            ),
            "received_bytes": result.received_bytes,
            "subscribe_request_bytes": result.subscribe_request_bytes,
            "checked_at": checked,
        })
        return AdvancedCapabilityEvidence(
            tickers=request.tickers,
            feed="STOCKS",
            channel="AM",
            configuration_sha256=request.configuration_sha256,
            checked_at=checked,
            expires_at=checked + timedelta(
                seconds=self._validity_seconds
            ),
            entitlement_evidence_sha256=evidence_sha256,
            authorization_reference=(
                f"advanced-capability-refresh:{evidence_sha256}"
            ),
            accepted_membership=accepted,
            rejected_membership=rejected,
            provider_requests=1,
            received_control_messages=(
                result.received_control_messages
            ),
            received_bytes=result.received_bytes,
            subscribe_request_bytes=result.subscribe_request_bytes,
        )


def load_capability_refresh_request_cursor(
    cursor,
    *,
    checked_at: datetime,
    refresh_before_seconds: int,
    evidence_validity_seconds: int = 86_400,
    calendar: OptionExchangeCalendar | None = None,
) -> AdvancedCapabilityRefreshRequest | None:
    checked = _utc(checked_at, "capability refresh checked_at")
    if refresh_before_seconds < 0 or evidence_validity_seconds < 300:
        raise ValueError("refresh_before_seconds must be nonnegative")
    revision = load_current_desired_state_revision_cursor(cursor)
    if revision is None:
        return None
    security_ids = dict(revision.security_ids)
    tickers = tuple(sorted(
        member.ticker
        for member in revision.members
        if member.enabled
        and member.kind
        in {
            DesiredMemberKind.EQUITY,
            DesiredMemberKind.SECTOR_ETF,
        }
    ))
    if not tickers or set(tickers) != set(security_ids):
        selected_security_ids = tuple(
            (ticker, security_ids[ticker]) for ticker in tickers
        )
    else:
        selected_security_ids = tuple(sorted(security_ids.items()))
    universe = UniverseManifest(
        version=f"advanced-equity-{len(tickers)}-am-v1",
        equity_tickers=tickers,
        option_underlyings=(),
    )
    cursor.execute(
        "SELECT manifest.profile_id::text,cohort.configuration_sha256,"
        "profile.schema_sha256,profile.security_policy_sha256 "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
        "JOIN rt.capability_profiles AS profile USING(profile_id) "
        "WHERE cohort.cohort_id=%s",
        (str(revision.source_cohort_id),),
    )
    source = cursor.fetchone()
    if source is None:
        raise ValueError(
            "capability refresh source control plane is unavailable"
        )
    exchange = calendar or OptionExchangeCalendar("XNYS")
    required_through = exchange.session_close(
        revision.effective_session_date
    )
    probe_not_before = capability_probe_not_before(
        revision.effective_session_date,
        evidence_validity_seconds=evidence_validity_seconds,
        calendar=exchange,
    )
    cursor.execute(
        "SELECT profile_id::text,expires_at "
        "FROM rt.capability_profiles "
        "WHERE provider_tier='MASSIVE_ADVANCED' "
        "AND config_sha256=%s AND universe_sha256=%s "
        "AND status='PASS' "
        "ORDER BY expires_at DESC,checked_at DESC,profile_id DESC "
        "LIMIT 1",
        (str(source[1]), universe.sha256),
    )
    current = cursor.fetchone()
    current_profile_id = (
        UUID(str(source[0]))
        if current is None
        else UUID(str(current[0]))
    )
    current_expires_at = (
        checked
        if current is None
        else _utc(current[1], "current capability expires_at")
    )
    return AdvancedCapabilityRefreshRequest(
        revision_id=revision.revision_id,
        revision_sha256=revision.revision_sha256,
        session_date=revision.effective_session_date,
        security_ids=selected_security_ids,
        source_profile_id=UUID(str(source[0])),
        current_profile_id=current_profile_id,
        current_expires_at=current_expires_at,
        required_through=required_through,
        probe_not_before=probe_not_before,
        configuration_sha256=str(source[1]),
        schema_sha256=str(source[2]),
        security_policy_sha256=str(source[3]),
        requires_refresh=(
            current is None
            or current_expires_at < required_through
            or current_expires_at
            <= checked + timedelta(seconds=refresh_before_seconds)
        ),
    )


def build_capability_refresh_plan(
    request: AdvancedCapabilityRefreshRequest,
    evidence: AdvancedCapabilityEvidence,
) -> AdvancedEquityControlPlanePlan:
    if (
        evidence.tickers != request.tickers
        or evidence.feed != "STOCKS"
        or evidence.channel != "AM"
        or evidence.configuration_sha256
        != request.configuration_sha256
        or evidence.accepted_membership != request.tickers
        or evidence.rejected_membership
        or evidence.provider_requests != 1
        or evidence.expires_at < request.required_through
    ):
        raise ValueError(
            "capability refresh evidence does not cover desired scope"
        )
    return build_advanced_equity_control_plane_plan(
        session_date=request.session_date,
        security_ids=dict(request.security_ids),
        entitlement_evidence_sha256=(
            evidence.entitlement_evidence_sha256
        ),
        authorization_reference=evidence.authorization_reference,
        checked_at=evidence.checked_at,
        expires_at=evidence.expires_at,
        schema_sha256=request.schema_sha256,
        security_policy_sha256=request.security_policy_sha256,
        configuration_sha256=request.configuration_sha256,
    )


class AdvancedCapabilityRefreshResident:
    def __init__(
        self,
        source: AdvancedCapabilityRefreshSource,
        *,
        retry_initial_seconds: int = 15,
        retry_maximum_seconds: int = 300,
    ) -> None:
        if (
            retry_initial_seconds <= 0
            or retry_maximum_seconds < retry_initial_seconds
        ):
            raise ValueError("capability refresh retry policy is invalid")
        self._source = source
        self._retry_initial = retry_initial_seconds
        self._retry_maximum = retry_maximum_seconds
        self._failure_count = 0
        self._last_identity = None
        self._last_success_at = None
        self._next_retry_at = None

    def step(
        self,
        request: AdvancedCapabilityRefreshRequest,
        *,
        checked_at: datetime,
    ) -> AdvancedCapabilityRefreshOutcome:
        checked = _utc(checked_at, "capability resident checked_at")
        identity = request.identity_sha256
        changed = identity != self._last_identity
        if changed:
            self._failure_count = 0
            self._next_retry_at = None
            self._last_identity = identity
        if not request.requires_refresh:
            return AdvancedCapabilityRefreshOutcome(
                "READY",
                checked,
                self._last_success_at,
                None,
                request.current_profile_id,
                identity,
            )
        if checked < request.probe_not_before:
            return AdvancedCapabilityRefreshOutcome(
                "WAITING",
                checked,
                self._last_success_at,
                request.probe_not_before,
                request.current_profile_id,
                identity,
                reason_code="CAPABILITY_PROBE_NOT_BEFORE",
            )
        if (
            self._next_retry_at is not None
            and checked < self._next_retry_at
        ):
            return AdvancedCapabilityRefreshOutcome(
                "DEGRADED",
                checked,
                self._last_success_at,
                self._next_retry_at,
                request.current_profile_id,
                identity,
                reason_code="CAPABILITY_PROVIDER_RETRY_WAIT",
            )
        evidence = None
        try:
            evidence = self._source.refresh(request)
            plan = build_capability_refresh_plan(request, evidence)
        except Exception as error:
            self._failure_count += 1
            delay = min(
                self._retry_maximum,
                self._retry_initial
                * (2 ** (self._failure_count - 1)),
            )
            self._next_retry_at = checked + timedelta(seconds=delay)
            return AdvancedCapabilityRefreshOutcome(
                "DEGRADED",
                checked,
                self._last_success_at,
                self._next_retry_at,
                request.current_profile_id,
                identity,
                evidence=evidence,
                reason_code=(
                    "CAPABILITY_SUBSCRIPTION_REJECTED:"
                    + ",".join(evidence.rejected_membership)
                    if (
                        evidence is not None
                        and evidence.rejected_membership
                    )
                    else "CAPABILITY_PROVIDER_UNAVAILABLE:"
                    f"{type(error).__name__}:{error}"
                ),
            )
        self._failure_count = 0
        self._last_success_at = evidence.checked_at
        self._next_retry_at = None
        return AdvancedCapabilityRefreshOutcome(
            "READY",
            checked,
            self._last_success_at,
            None,
            plan.profile_id,
            identity,
            plan=plan,
            evidence=evidence,
        )

    def provider_probe_due(
        self,
        request: AdvancedCapabilityRefreshRequest,
        *,
        checked_at: datetime,
    ) -> bool:
        checked = _utc(checked_at, "capability probe due checked_at")
        return (
            request.requires_refresh
            and checked >= request.probe_not_before
            and (
                self._next_retry_at is None
                or checked >= self._next_retry_at
                or request.identity_sha256 != self._last_identity
            )
        )

    def connection_unavailable(
        self,
        request: AdvancedCapabilityRefreshRequest,
        *,
        checked_at: datetime,
    ) -> AdvancedCapabilityRefreshOutcome:
        checked = _utc(
            checked_at,
            "capability connection unavailable checked_at",
        )
        identity = request.identity_sha256
        if identity != self._last_identity:
            self._failure_count = 0
            self._last_identity = identity
        self._failure_count += 1
        delay = min(
            self._retry_maximum,
            self._retry_initial * (2 ** (self._failure_count - 1)),
        )
        self._next_retry_at = checked + timedelta(seconds=delay)
        return AdvancedCapabilityRefreshOutcome(
            "DEGRADED",
            checked,
            self._last_success_at,
            self._next_retry_at,
            request.current_profile_id,
            identity,
            reason_code="CAPABILITY_STOCKS_CONNECTION_OWNED",
        )


def persist_capability_refresh_outcome_cursor(
    cursor,
    outcome: AdvancedCapabilityRefreshOutcome,
    *,
    owner_epoch: int,
    lease_until: datetime,
) -> None:
    if outcome.plan is not None:
        persist_advanced_equity_control_plane_cursor(
            cursor,
            outcome.plan,
        )
    if outcome.evidence is not None:
        evidence = outcome.evidence
        cursor.execute(
            "INSERT INTO rt.capability_checks ("
            "profile_id,check_name,scope_key,verdict,required,"
            "reason_codes,observed_at,expires_at,evidence) VALUES ("
            "%s,'exact_subscription_refresh',%s,%s,true,%s,%s,%s,%s) "
            "ON CONFLICT DO NOTHING",
            (
                str(outcome.profile_id),
                "stocks:AM:" + outcome.request_identity_sha256,
                (
                    "PASS"
                    if outcome.status == "READY"
                    else "FAIL"
                ),
                (
                    []
                    if outcome.status == "READY"
                    else [
                        outcome.reason_code
                        or "CAPABILITY_SUBSCRIPTION_REJECTED"
                    ]
                ),
                evidence.checked_at,
                evidence.expires_at,
                Json({
                    "accepted_membership": (
                        evidence.accepted_membership
                    ),
                    "rejected_membership": (
                        evidence.rejected_membership
                    ),
                    "provider_requests": evidence.provider_requests,
                    "received_control_messages": (
                        evidence.received_control_messages
                    ),
                    "received_bytes": evidence.received_bytes,
                    "subscribe_request_bytes": (
                        evidence.subscribe_request_bytes
                    ),
                    "feed": evidence.feed,
                    "channel": evidence.channel,
                }),
            ),
        )
    lease = _utc(lease_until, "capability resident lease_until")
    stored_status = (
        "READY" if outcome.status == "READY" else "DEGRADED"
    )
    reasons = (
        []
        if stored_status == "READY"
        else [outcome.reason_code or "CAPABILITY_REFRESH_DEGRADED"]
    )
    cursor.execute(
        "SELECT status,epoch FROM rt.readiness_current "
        "WHERE scope_type=%s AND scope_id=%s AND capability=%s "
        "FOR UPDATE",
        (
            CAPABILITY_REFRESH_SCOPE_TYPE,
            CAPABILITY_REFRESH_SCOPE_ID,
            CAPABILITY_REFRESH_CAPABILITY,
        ),
    )
    current = cursor.fetchone()
    epoch = 1 if current is None else int(current[1]) + 1
    watermark = {
        "heartbeat_at": outcome.checked_at.isoformat(),
        "last_success_at": (
            None
            if outcome.last_success_at is None
            else outcome.last_success_at.isoformat()
        ),
        "next_retry_at": (
            None
            if outcome.next_retry_at is None
            else outcome.next_retry_at.isoformat()
        ),
        "request_identity_sha256": outcome.request_identity_sha256,
        "state": outcome.status,
    }
    if current is None or str(current[0]) != stored_status:
        cursor.execute(
            "INSERT INTO rt.readiness_transitions ("
            "scope_type,scope_id,capability,epoch,owner_epoch,profile_id,"
            "previous_status,status,reasons,transitioned_at,lease_until,"
            "source_watermark) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,"
            "%s,%s,%s)",
            (
                CAPABILITY_REFRESH_SCOPE_TYPE,
                CAPABILITY_REFRESH_SCOPE_ID,
                CAPABILITY_REFRESH_CAPABILITY,
                epoch,
                owner_epoch,
                str(outcome.profile_id),
                None if current is None else str(current[0]),
                stored_status,
                reasons,
                outcome.checked_at,
                lease,
                Json(watermark),
            ),
        )
    cursor.execute(
        "INSERT INTO rt.readiness_current ("
        "scope_type,scope_id,capability,epoch,owner_epoch,profile_id,"
        "status,reasons,checked_at,lease_until,source_watermark) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (scope_type,scope_id,capability) DO UPDATE SET "
        "epoch=EXCLUDED.epoch,owner_epoch=EXCLUDED.owner_epoch,"
        "profile_id=EXCLUDED.profile_id,status=EXCLUDED.status,"
        "reasons=EXCLUDED.reasons,checked_at=EXCLUDED.checked_at,"
        "lease_until=EXCLUDED.lease_until,"
        "source_watermark=EXCLUDED.source_watermark,"
        "updated_at=clock_timestamp()",
        (
            CAPABILITY_REFRESH_SCOPE_TYPE,
            CAPABILITY_REFRESH_SCOPE_ID,
            CAPABILITY_REFRESH_CAPABILITY,
            epoch,
            owner_epoch,
            str(outcome.profile_id),
            stored_status,
            reasons,
            outcome.checked_at,
            lease,
            Json(watermark),
        ),
    )
