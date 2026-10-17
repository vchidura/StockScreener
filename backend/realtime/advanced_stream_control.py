from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import (
    MarketFeed,
    canonical_sha256,
)
from .advanced_ingress_repository import (
    admit_advanced_ingress_session_cursor,
    record_advanced_ingress_usage_cursor,
)
from .preflight import ProviderTier


ADVANCED_STREAM_PROVIDER = "massive"
ADVANCED_STREAM_TERMINAL_STATES = frozenset(
    {"STOPPED", "FAILED", "BLOCKED"}
)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _text(value: str, name: str, maximum: int) -> str:
    normalized = value.strip()
    if not 1 <= len(normalized) <= maximum:
        raise ValueError(f"{name} must contain 1-{maximum} characters")
    return normalized


@dataclass(frozen=True, slots=True)
class AdvancedStreamScope:
    ingress_plan_id: UUID
    cohort_id: UUID
    manifest_id: UUID
    journal_generation: UUID
    owner_id: UUID
    feed: MarketFeed
    normalizer_version: str
    authorization_reference: str
    connection_attempt: int
    connection_ceiling: int
    decoded_byte_ceiling: int
    stale_after_seconds: int
    started_at: datetime

    def __post_init__(self) -> None:
        if self.feed is not MarketFeed.STOCKS:
            raise ValueError(
                "the initial Advanced equity runtime supports STOCKS only"
            )
        object.__setattr__(
            self,
            "normalizer_version",
            _text(self.normalizer_version, "normalizer_version", 96),
        )
        object.__setattr__(
            self,
            "authorization_reference",
            _text(
                self.authorization_reference,
                "authorization_reference",
                160,
            ),
        )
        if (
            type(self.connection_attempt) is not int
            or type(self.connection_ceiling) is not int
            or not 1 <= self.connection_attempt <= self.connection_ceiling
            or type(self.decoded_byte_ceiling) is not int
            or self.decoded_byte_ceiling <= 0
            or type(self.stale_after_seconds) is not int
            or self.stale_after_seconds <= 0
        ):
            raise ValueError("Advanced stream connection budget is invalid")
        object.__setattr__(
            self,
            "started_at",
            _utc(self.started_at, "started_at"),
        )

    @property
    def budget_scope_sha256(self) -> str:
        return canonical_sha256(
            {
                "contract": "advanced-stream-budget-v2",
                "ingress_plan_id": self.ingress_plan_id,
                "cohort_id": self.cohort_id,
                "manifest_id": self.manifest_id,
                "feed": self.feed.value,
                "authorization_reference": self.authorization_reference,
                "connection_ceiling": self.connection_ceiling,
                "decoded_byte_ceiling": self.decoded_byte_ceiling,
            }
        )


@dataclass(frozen=True, slots=True)
class RegisteredAdvancedStreamSession:
    session_id: UUID
    ingress_plan_id: UUID
    journal_generation: UUID
    owner_id: UUID
    owner_epoch: int
    feed: MarketFeed
    started_at: datetime


@dataclass(frozen=True, slots=True)
class AdvancedStreamUsage:
    received_frame_count: int
    received_byte_count: int
    admitted_market_message_count: int
    admitted_market_byte_count: int
    last_raw_offset: int
    last_event_offset: int
    event_time_watermark: datetime | None

    def __post_init__(self) -> None:
        values = (
            self.received_frame_count,
            self.received_byte_count,
            self.admitted_market_message_count,
            self.admitted_market_byte_count,
            self.last_raw_offset,
            self.last_event_offset,
        )
        if any(type(value) is not int or value < 0 for value in values):
            raise ValueError("Advanced stream usage cannot be negative")
        if (
            self.admitted_market_message_count
            > self.received_frame_count
            or self.admitted_market_byte_count > self.received_byte_count
        ):
            raise ValueError("Advanced market usage exceeds all-frame usage")
        if self.event_time_watermark is not None:
            object.__setattr__(
                self,
                "event_time_watermark",
                _utc(
                    self.event_time_watermark,
                    "event_time_watermark",
                ),
            )


def register_advanced_stream_session_cursor(
    cursor,
    scope: AdvancedStreamScope,
) -> RegisteredAdvancedStreamSession:
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (f"v2-advanced-stream:{scope.feed.value}",),
    )
    cursor.execute(
        "SELECT profile.provider_tier,cohort.manifest_id,"
        "manifest.stock_count,"
        "COUNT(member.instrument_id) FILTER (WHERE member.channel='AM') "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
        "JOIN rt.capability_profiles AS profile USING(profile_id) "
        "LEFT JOIN rt.subscription_members AS member USING(manifest_id) "
        "WHERE cohort.cohort_id=%s AND profile.status='PASS' "
        "AND profile.expires_at>%s "
        "GROUP BY profile.provider_tier,cohort.manifest_id,"
        "manifest.stock_count",
        (str(scope.cohort_id), scope.started_at),
    )
    identity = cursor.fetchone()
    if (
        identity is None
        or identity[0] != ProviderTier.MASSIVE_ADVANCED.value
        or str(identity[1]) != str(scope.manifest_id)
        or int(identity[2]) != int(identity[3])
    ):
        raise ValueError(
            "Advanced stream scope is not an exact AM cohort manifest"
        )
    cursor.execute(
        "SELECT session_id,owner_id,owner_epoch,journal_generation,"
        "heartbeat_at FROM rt.stream_sessions "
        "WHERE provider=%s AND feed=%s AND ended_at IS NULL "
        "AND status NOT IN ('STOPPED','FAILED','BLOCKED') "
        "ORDER BY owner_epoch DESC LIMIT 1 FOR UPDATE",
        (ADVANCED_STREAM_PROVIDER, scope.feed.value),
    )
    active_owner = cursor.fetchone()
    if active_owner is not None:
        heartbeat_at = active_owner[4]
        stale_before = scope.started_at - timedelta(
            seconds=scope.stale_after_seconds
        )
        if heartbeat_at is None or heartbeat_at > stale_before:
            raise ValueError(
                "an Advanced stream owner is already active for this feed"
            )
        cursor.execute(
            "UPDATE rt.stream_sessions SET status='FAILED',"
            "failure_code='STALE_OWNER_RECLAIMED',ended_at=%s "
            "WHERE session_id=%s AND owner_id=%s AND owner_epoch=%s "
            "AND journal_generation=%s AND ended_at IS NULL "
            "AND heartbeat_at=%s RETURNING session_id",
            (
                scope.started_at,
                str(active_owner[0]),
                str(active_owner[1]),
                int(active_owner[2]),
                str(active_owner[3]),
                heartbeat_at,
            ),
        )
        if cursor.fetchone() is None:
            raise ValueError(
                "stale Advanced stream owner changed during reclamation"
            )
    cursor.execute(
        "SELECT COALESCE(MAX(owner_epoch),0)+1 "
        "FROM rt.stream_sessions WHERE provider=%s AND feed=%s",
        (ADVANCED_STREAM_PROVIDER, scope.feed.value),
    )
    owner_epoch = int(cursor.fetchone()[0])
    identity_sha256 = canonical_sha256(
        {
            "scope": scope.budget_scope_sha256,
            "journal_generation": scope.journal_generation,
            "owner_id": scope.owner_id,
            "owner_epoch": owner_epoch,
            "started_at": scope.started_at,
        }
    )
    session_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-stream-session:{identity_sha256}",
    )
    cursor.execute(
        "INSERT INTO rt.stream_sessions ("
        "session_id,provider,feed,manifest_id,owner_id,owner_epoch,"
        "started_at,status,journal_generation,normalizer_version,"
        "authorization_reference,budget_scope_sha256,"
        "connection_attempt,connection_ceiling,decoded_byte_ceiling,"
        "heartbeat_at,advanced_ingress_plan_id) VALUES ("
        "%s,%s,%s,%s,%s,%s,%s,'CONNECTING',%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            str(session_id),
            ADVANCED_STREAM_PROVIDER,
            scope.feed.value,
            str(scope.manifest_id),
            str(scope.owner_id),
            owner_epoch,
            scope.started_at,
            str(scope.journal_generation),
            scope.normalizer_version,
            scope.authorization_reference,
            scope.budget_scope_sha256,
            scope.connection_attempt,
            scope.connection_ceiling,
            scope.decoded_byte_ceiling,
            scope.started_at,
            str(scope.ingress_plan_id),
        ),
    )
    admit_advanced_ingress_session_cursor(
        cursor,
        plan_id=scope.ingress_plan_id,
        cohort_id=scope.cohort_id,
        manifest_id=scope.manifest_id,
        authorization_reference=scope.authorization_reference,
        connection_attempt=scope.connection_attempt,
        connection_ceiling=scope.connection_ceiling,
        decoded_byte_ceiling=scope.decoded_byte_ceiling,
        stream_session_id=session_id,
        owner_epoch=owner_epoch,
        checked_at=scope.started_at,
    )
    cursor.execute(
        "UPDATE rt.stream_gaps SET reconnect_at=COALESCE(reconnect_at,%s) "
        "WHERE plan_id=%s AND recovery_status='UNRESOLVED' "
        "AND reconnect_at IS NULL",
        (scope.started_at, str(scope.ingress_plan_id)),
    )
    return RegisteredAdvancedStreamSession(
        session_id,
        scope.ingress_plan_id,
        scope.journal_generation,
        scope.owner_id,
        owner_epoch,
        scope.feed,
        scope.started_at,
    )


def update_advanced_stream_session_cursor(
    cursor,
    session: RegisteredAdvancedStreamSession,
    usage: AdvancedStreamUsage,
    *,
    status: str,
    checked_at: datetime,
    failure_code: str | None = None,
    terminal: bool = False,
) -> None:
    checked = _utc(checked_at, "checked_at")
    normalized_status = _text(status, "status", 24).upper()
    if terminal != (
        normalized_status in ADVANCED_STREAM_TERMINAL_STATES
    ):
        raise ValueError("Advanced stream terminal status is inconsistent")
    failure = (
        _text(failure_code, "failure_code", 64)
        if failure_code is not None
        else None
    )
    cursor.execute(
        "SELECT advanced_ingress_plan_id,received_frame_count,"
        "received_byte_count,admitted_market_message_count,"
        "admitted_market_byte_count,last_raw_offset,last_event_offset "
        "FROM rt.stream_sessions "
        "WHERE session_id=%s AND owner_id=%s AND owner_epoch=%s "
        "AND journal_generation=%s AND ended_at IS NULL FOR UPDATE",
        (
            str(session.session_id),
            str(session.owner_id),
            session.owner_epoch,
            str(session.journal_generation),
        ),
    )
    previous = cursor.fetchone()
    if previous is None:
        raise ValueError("Advanced stream session fence rejected the update")
    previous_counts = tuple(int(value) for value in previous[1:])
    current_counts = (
        usage.received_frame_count,
        usage.received_byte_count,
        usage.admitted_market_message_count,
        usage.admitted_market_byte_count,
        usage.last_raw_offset,
        usage.last_event_offset,
    )
    if any(
        current < prior
        for current, prior in zip(current_counts, previous_counts)
    ):
        raise ValueError("Advanced stream usage cannot reset")
    cursor.execute(
        "UPDATE rt.stream_sessions SET status=%s,"
        "authenticated_at=CASE WHEN %s IN ('SUBSCRIBING','WARMING','LIVE') "
        "THEN COALESCE(authenticated_at,%s) ELSE authenticated_at END,"
        "subscribed_at=CASE WHEN %s IN ('SUBSCRIBING','WARMING','LIVE') "
        "THEN COALESCE(subscribed_at,%s) ELSE subscribed_at END,"
        "ended_at=CASE WHEN %s THEN %s ELSE ended_at END,"
        "failure_code=%s,received_frame_count=%s,received_byte_count=%s,"
        "admitted_market_message_count=%s,admitted_market_byte_count=%s,"
        "last_raw_offset=%s,last_event_offset=%s,"
        "event_time_watermark=%s,heartbeat_at=%s "
        "WHERE session_id=%s AND owner_id=%s AND owner_epoch=%s "
        "AND journal_generation=%s AND ended_at IS NULL RETURNING session_id",
        (
            normalized_status,
            normalized_status,
            checked,
            normalized_status,
            checked,
            terminal,
            checked,
            failure,
            usage.received_frame_count,
            usage.received_byte_count,
            usage.admitted_market_message_count,
            usage.admitted_market_byte_count,
            usage.last_raw_offset,
            usage.last_event_offset,
            usage.event_time_watermark,
            checked,
            str(session.session_id),
            str(session.owner_id),
            session.owner_epoch,
            str(session.journal_generation),
        ),
    )
    if cursor.fetchone() is None:
        raise ValueError("Advanced stream session fence rejected the update")
    if previous[0] is not None:
        record_advanced_ingress_usage_cursor(
            cursor,
            plan_id=UUID(str(previous[0])),
            stream_session_id=session.session_id,
            owner_epoch=session.owner_epoch,
            decoded_bytes=(
                usage.received_byte_count - previous_counts[1]
            ),
            checked_at=checked,
        )


def record_advanced_stream_gap_cursor(
    cursor,
    session: RegisteredAdvancedStreamSession,
    *,
    gap_start: datetime,
    gap_end: datetime | None,
    last_durable_offset: int,
    reason: str,
    last_durable_watermark: datetime | None = None,
    instrument_id: int | None = None,
    shard_id: int = 0,
) -> UUID:
    started = _utc(gap_start, "gap_start")
    ended = _utc(gap_end, "gap_end") if gap_end is not None else None
    reason_code = _text(reason, "reason", 64).upper()
    watermark = (
        _utc(last_durable_watermark, "last_durable_watermark")
        if last_durable_watermark is not None
        else None
    )
    if (
        ended is not None
        and ended < started
        or type(last_durable_offset) is not int
        or last_durable_offset < 0
        or type(shard_id) is not int
        or shard_id < 0
        or (
            instrument_id is not None
            and (type(instrument_id) is not int or instrument_id <= 0)
        )
    ):
        raise ValueError("Advanced stream gap scope is invalid")
    gap_id = uuid5(
        NAMESPACE_URL,
        "stockscreener:advanced-stream-gap:"
        f"{session.session_id}:{session.journal_generation}:{shard_id}:"
        f"{last_durable_offset}:{started.isoformat()}:{reason_code}",
    )
    cursor.execute(
        "INSERT INTO rt.stream_gaps ("
        "gap_id,session_id,instrument_id,channel,gap_start,gap_end,"
        "journal_generation,shard_id,last_durable_offset,reason,"
        "recovery_status,plan_id,last_durable_watermark) VALUES ("
        "%s,%s,%s,'AM',%s,%s,%s,%s,%s,%s,'UNRESOLVED',%s,%s) "
        "ON CONFLICT (gap_id) DO NOTHING",
        (
            str(gap_id),
            str(session.session_id),
            instrument_id,
            started,
            ended,
            str(session.journal_generation),
            shard_id,
            last_durable_offset,
            reason_code,
            str(session.ingress_plan_id),
            watermark,
        ),
    )
    return gap_id
