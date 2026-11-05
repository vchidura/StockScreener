from __future__ import annotations

import asyncio
import json
import logging
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Callable
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from .buffer import BufferFull, RawMessageBuffer
from provider_endpoints import OPTIONS_REALTIME_WS_URL, STOCKS_REALTIME_WS_URL
from .contracts import (
    AssetClass,
    MarketChannel,
    MarketFeed,
    RawMessageEnvelope,
    SubscriptionManifest,
)


class GatewayFeedState(str, Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    AUTHENTICATING = "AUTHENTICATING"
    SUBSCRIBING = "SUBSCRIBING"
    WARMING = "WARMING"
    LIVE = "LIVE"
    BLOCKED = "BLOCKED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


class GatewayControlError(RuntimeError):
    def __init__(
        self,
        reason_code: str,
        statuses: tuple[str, ...] = (),
        provider_messages: tuple[str, ...] = (),
    ) -> None:
        normalized = tuple(sorted({
            status.strip().lower() for status in statuses if status.strip()
        }))
        messages = tuple(sorted({
            message.strip()[:256]
            for message in provider_messages
            if message.strip()
        }))
        self.reason_code = reason_code
        self.statuses = normalized
        self.provider_messages = messages
        detail = (
            reason_code
            if not normalized
            else f"{reason_code}:{','.join(normalized)}"
        )
        super().__init__(detail)


class GatewaySubscriptionProbeFailure(RuntimeError):
    def __init__(
        self,
        reason_code: str,
        *,
        received_control_messages: int,
        received_bytes: int,
        accepted_subscriptions: tuple[str, ...] = (),
        rejected_subscriptions: tuple[str, ...] = (),
        unconfirmed_subscriptions: tuple[str, ...] = (),
    ) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code
        self.received_control_messages = received_control_messages
        self.received_bytes = received_bytes
        self.accepted_subscriptions = accepted_subscriptions
        self.rejected_subscriptions = rejected_subscriptions
        self.unconfirmed_subscriptions = unconfirmed_subscriptions


class _GatewayStopRequested(Exception):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True, slots=True)
class GatewayOverload(RuntimeError):
    reason_code: str
    stream_session_id: UUID
    last_received_message_number: int
    last_admitted_message_number: int

    def __str__(self) -> str:
        return self.reason_code


_LOGGER = logging.getLogger(__name__)
# The Options feed is closed only after this long with no data frame and no pong.
OPTIONS_SILENCE_CLOSE_SECONDS = 60
_SILENCE_POLL_SECONDS = 1.0


@dataclass(frozen=True, slots=True)
class GatewayConfiguration:
    feed: MarketFeed
    endpoint: str
    open_timeout_seconds: int = 10
    ping_interval_seconds: int = 20
    ping_timeout_seconds: int = 10
    close_timeout_seconds: int = 5
    maximum_message_bytes: int = 2 * 1024 * 1024
    library_max_queue: int = 8
    maximum_control_messages: int = 8
    compression: str | None = None
    allow_insecure_loopback: bool = False
    silence_close_seconds: int = OPTIONS_SILENCE_CLOSE_SECONDS

    def __post_init__(self) -> None:
        parsed = urlsplit(self.endpoint)
        secure = parsed.scheme == "wss" and bool(parsed.hostname)
        loopback = (
            self.allow_insecure_loopback
            and parsed.scheme == "ws"
            and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
        )
        if not secure and not loopback:
            raise ValueError("Advanced gateway endpoint must be WSS or approved loopback WS")
        if min(
            self.open_timeout_seconds,
            self.ping_interval_seconds,
            self.ping_timeout_seconds,
            self.close_timeout_seconds,
            self.maximum_message_bytes,
            self.library_max_queue,
            self.maximum_control_messages,
            self.silence_close_seconds,
        ) <= 0:
            raise ValueError("Advanced gateway bounds must be positive")
        # A quiet feed shows only pongs, one per ping, so a shorter limit would
        # close a healthy connection.
        if (
            self.feed is MarketFeed.OPTIONS
            and self.silence_close_seconds <= self.ping_interval_seconds
        ):
            raise ValueError(
                "Advanced gateway silence limit must outlast the ping interval"
            )

    @classmethod
    def stocks(cls) -> GatewayConfiguration:
        return cls(MarketFeed.STOCKS, STOCKS_REALTIME_WS_URL)

    @classmethod
    def options(cls) -> GatewayConfiguration:
        return cls(MarketFeed.OPTIONS, OPTIONS_REALTIME_WS_URL)


@dataclass(frozen=True, slots=True)
class GatewaySessionResult:
    stream_session_id: UUID
    feed: MarketFeed
    subscriptions: tuple[str, ...]
    received_messages: int
    received_bytes: int
    accepted_messages: int
    accepted_bytes: int
    final_state: GatewayFeedState
    transitions: tuple[GatewayFeedState, ...]
    connection_attempts: int = 1
    provider_requests: bool = False
    library_max_queue: int = 0
    kernel_receive_buffer_bytes: int | None = None
    stop_reason: str | None = None


@dataclass(frozen=True, slots=True)
class GatewaySubscriptionProbeResult:
    stream_session_id: UUID
    feed: MarketFeed
    requested_subscriptions: tuple[str, ...]
    accepted_subscriptions: tuple[str, ...]
    rejected_subscriptions: tuple[str, ...]
    received_control_messages: int
    received_bytes: int
    subscribe_request_bytes: int
    transitions: tuple[GatewayFeedState, ...]


_CHANNEL_ORDER = {
    MarketChannel.TRADE: 0,
    MarketChannel.QUOTE: 1,
    MarketChannel.MINUTE_AGGREGATE: 2,
}


class _WatchedConnection:
    """The configured connection, and for Options its silence watcher. A class, as
    contextlib would write to GatewayOverload's traceback, and that is frozen."""

    def __init__(self, gateway: AdvancedGateway) -> None:
        self._gateway = gateway
        self._context = gateway._connection_context()
        self._connection = None
        self._watcher: asyncio.Task | None = None

    async def __aenter__(self):
        gateway = self._gateway
        self._connection = await self._context.__aenter__()
        if gateway.configuration.feed is MarketFeed.OPTIONS:
            gateway._awaiting_frame_since = None
            gateway._last_frame_at = None
            gateway._last_pong_at = None
            self._watcher = asyncio.create_task(
                gateway._close_when_silent(self._connection)
            )
        return self._connection

    async def __aexit__(self, *exc_info):
        if self._watcher is not None:
            self._watcher.cancel()
            await asyncio.gather(self._watcher, return_exceptions=True)
        try:
            return await self._context.__aexit__(*exc_info)
        finally:
            close_code = getattr(self._connection, "close_code", None)
            if self._watcher is not None and close_code != 1000:
                self._gateway._log_liveness("connection_closed", close_code)


class AdvancedGateway:
    def __init__(
        self,
        configuration: GatewayConfiguration,
        manifest: SubscriptionManifest,
        buffer: RawMessageBuffer,
        *,
        api_key: str,
        connection_factory: Callable | None = None,
        utc_clock: Callable[[], datetime] | None = None,
        monotonic_ns: Callable[[], int],
        session_id_factory: Callable[[], UUID] = uuid4,
        state_observer: Callable[[GatewayFeedState], None] | None = None,
        usage_observer: Callable[[int, int], None] | None = None,
        market_observer: Callable[[int, int], None] | None = None,
    ) -> None:
        if not api_key.strip():
            raise ValueError("Advanced gateway API key is required")
        self.configuration = configuration
        self.manifest = manifest
        self.buffer = buffer
        self._api_key = api_key
        self._connection_factory = connection_factory
        self._utc_clock = utc_clock or (lambda: datetime.now(timezone.utc))
        self._monotonic_ns = monotonic_ns
        self._session_id_factory = session_id_factory
        self._state_observer = state_observer
        if usage_observer is not None and not callable(usage_observer):
            raise TypeError("Advanced gateway usage observer is invalid")
        self._usage_observer = usage_observer
        if market_observer is not None and not callable(market_observer):
            raise TypeError("Advanced gateway market observer is invalid")
        self._market_observer = market_observer
        self._state = GatewayFeedState.DISCONNECTED
        self._transitions = [self._state]
        self._message_number = 0
        self._last_admitted_message_number = -1
        self._received_messages = 0
        self._received_bytes = 0
        self._maximum_received_bytes: int | None = None
        self._active_stream_session_id: UUID | None = None
        self._kernel_receive_buffer_bytes: int | None = None
        self._subscriptions = self._build_subscriptions()
        self._stop_requested: Callable[[], bool] | None = None
        self._stop_at: datetime | None = None
        self._receive_poll_seconds = 1.0
        self._awaiting_frame_since: float | None = None
        self._last_frame_at: float | None = None
        self._last_pong_at: float | None = None

    @property
    def state(self) -> GatewayFeedState:
        return self._state

    @property
    def subscriptions(self) -> tuple[str, ...]:
        return self._subscriptions

    @property
    def received_messages(self) -> int:
        return self._received_messages

    @property
    def received_bytes(self) -> int:
        return self._received_bytes

    @property
    def transitions(self) -> tuple[GatewayFeedState, ...]:
        return tuple(self._transitions)

    @property
    def kernel_receive_buffer_bytes(self) -> int | None:
        return self._kernel_receive_buffer_bytes

    @property
    def active_stream_session_id(self) -> UUID | None:
        return self._active_stream_session_id

    @property
    def next_message_number(self) -> int:
        return self._message_number

    def _set_state(self, state: GatewayFeedState) -> None:
        if state is self._state:
            return
        self._state = state
        self._transitions.append(state)
        if self._state_observer is not None:
            self._state_observer(state)

    def _build_subscriptions(self) -> tuple[str, ...]:
        expected_asset = (
            AssetClass.EQUITY
            if self.configuration.feed is MarketFeed.STOCKS
            else AssetClass.OPTION
        )
        members = tuple(
            member
            for member in self.manifest.members
            if member.asset_class is expected_asset
        )
        subscriptions = tuple(
            f"{member.channel.value}.{member.provider_symbol}"
            for member in sorted(
                members,
                key=lambda item: (
                    _CHANNEL_ORDER[item.channel],
                    item.provider_symbol,
                ),
            )
        )
        if not subscriptions:
            raise ValueError("Advanced gateway manifest has no members for its feed")
        return subscriptions

    def _connection_context(self):
        if self._connection_factory is not None:
            return self._connection_factory(self.configuration)
        from websockets.asyncio.client import connect

        return connect(
            self.configuration.endpoint,
            compression=self.configuration.compression,
            open_timeout=self.configuration.open_timeout_seconds,
            ping_interval=self.configuration.ping_interval_seconds,
            # A pong queued behind a backlog of data must not end an Options
            # connection that is still delivering frames: see _close_when_silent.
            ping_timeout=(
                None
                if self.configuration.feed is MarketFeed.OPTIONS
                else self.configuration.ping_timeout_seconds
            ),
            close_timeout=self.configuration.close_timeout_seconds,
            max_size=self.configuration.maximum_message_bytes,
            max_queue=self.configuration.library_max_queue,
        )

    def _watched_connection(self) -> _WatchedConnection:
        return _WatchedConnection(self)

    def _log_liveness(self, event: str, close_code: int | None = None) -> None:
        now = time.monotonic()

        def age(at: float | None) -> str:
            return "never" if at is None else f"{now - at:.1f}"

        _LOGGER.warning(
            "advanced_gateway_%s feed=%s close_code=%s "
            "last_frame_age_seconds=%s last_pong_age_seconds=%s",
            event,
            self.configuration.feed.value,
            close_code,
            age(self._last_frame_at),
            age(self._last_pong_at),
        )

    async def _close_when_silent(self, connection) -> None:
        """The library's pong timeout ends a connection while frames keep arriving,
        because it reads a pong only after the frames queued ahead of it. So the
        library keeps pinging, and this closes only when nothing at all has arrived.
        A pong has arrived when the library's measured latency changes."""
        limit = self.configuration.silence_close_seconds
        opened_at = time.monotonic()
        latency = getattr(connection, "latency", None)
        pong_late_logged = False
        while True:
            await asyncio.sleep(_SILENCE_POLL_SECONDS)
            now = time.monotonic()
            current = getattr(connection, "latency", None)
            if current != latency:
                latency = current
                self._last_pong_at = now
            newest_pong = self._last_pong_at or opened_at
            pong_late = now - newest_pong >= limit
            if pong_late and not pong_late_logged:
                self._log_liveness("pong_late")
            pong_late_logged = pong_late
            waiting_since = self._awaiting_frame_since
            # With the consumer busy, frames can be queued unread, so nothing here
            # says the feed has gone quiet.
            if (
                waiting_since is None
                or now - max(waiting_since, newest_pong) < limit
            ):
                continue
            self._log_liveness("silence_close", 1011)
            await connection.close(
                code=1011,
                reason="no frame or pong within the silence limit",
            )
            return

    async def _receive_bytes(self, connection) -> bytes:
        self._awaiting_frame_since = time.monotonic()
        while True:
            if (
                self._stop_requested is not None
                and self._stop_requested()
            ):
                raise _GatewayStopRequested("REQUESTED")
            timeout = self._receive_poll_seconds
            if self._stop_at is not None:
                remaining = (
                    self._stop_at - self._utc_clock()
                ).total_seconds()
                if remaining <= 0:
                    raise _GatewayStopRequested("AUTHORITY_DEADLINE")
                timeout = min(timeout, remaining)
            try:
                message = await asyncio.wait_for(
                    connection.recv(decode=False),
                    timeout=timeout,
                )
                break
            except asyncio.TimeoutError:
                continue
        self._awaiting_frame_since = None
        self._last_frame_at = time.monotonic()
        if type(message) is not bytes:
            raise GatewayControlError("ADVANCED_NON_BYTES_MESSAGE")
        self._received_messages += 1
        self._received_bytes += len(message)
        if self._usage_observer is not None:
            self._usage_observer(
                self._received_messages,
                self._received_bytes,
            )
        if (
            self._maximum_received_bytes is not None
            and self._received_bytes > self._maximum_received_bytes
        ):
            self._set_state(GatewayFeedState.BLOCKED)
            await connection.close(
                code=1009,
                reason="session exceeds configured byte ceiling",
            )
            raise GatewayOverload(
                "ADVANCED_SESSION_BYTE_CEILING_EXCEEDED",
                self._active_stream_session_id or UUID(int=0),
                self._message_number,
                self._last_admitted_message_number,
            )
        return message

    @staticmethod
    def _control_rows(payload: bytes) -> tuple[dict[str, object], ...] | None:
        try:
            decoded = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
        rows = decoded if isinstance(decoded, list) else [decoded]
        if not rows or not all(isinstance(row, dict) for row in rows):
            return None
        if not all(str(row.get("ev") or "").lower() == "status" for row in rows):
            return None
        return tuple(rows)

    async def _await_control(
        self,
        connection,
        *,
        accepted_statuses: frozenset[str],
        failure_prefix: str,
        allow_market_message: bool,
    ) -> bytes | None:
        for _ in range(self.configuration.maximum_control_messages):
            payload = await self._receive_bytes(connection)
            rows = self._control_rows(payload)
            if rows is None:
                if allow_market_message:
                    return payload
                raise GatewayControlError(f"{failure_prefix}_MARKET_BEFORE_READY")
            statuses = {str(row.get("status") or "").lower() for row in rows}
            if statuses & {
                "auth_failed",
                "failed",
                "error",
                "not_authorized",
                "max_connections",
            }:
                self._set_state(GatewayFeedState.BLOCKED)
                provider_messages = tuple(
                    str(row.get("message") or "")
                    for row in rows
                    if str(row.get("status") or "").lower()
                    == "max_connections"
                )
                raise GatewayControlError(
                    f"{failure_prefix}_REJECTED",
                    tuple(statuses),
                    provider_messages,
                )
            if statuses & accepted_statuses:
                return None
        self._set_state(GatewayFeedState.BLOCKED)
        raise GatewayControlError(f"{failure_prefix}_CONTROL_LIMIT_EXCEEDED")

    def _accept_delayed_subscription_control(
        self,
        rows: tuple[dict[str, object], ...],
    ) -> int:
        statuses = {
            str(row.get("status") or "").strip().lower()
            for row in rows
        }
        if statuses != {"success"}:
            self._set_state(GatewayFeedState.BLOCKED)
            raise GatewayControlError(
                "ADVANCED_STREAM_CONTROL_REJECTED",
                tuple(statuses),
                tuple(str(row.get("message") or "") for row in rows),
            )
        for row in rows:
            message = str(row.get("message") or "").strip()
            prefix = "subscribed to:"
            if message.lower().startswith(prefix):
                subscription = message[len(prefix):].strip()
                if subscription not in self._subscriptions:
                    self._set_state(GatewayFeedState.BLOCKED)
                    raise GatewayControlError(
                        "ADVANCED_SUBSCRIPTION_ACK_SCOPE_MISMATCH",
                        tuple(statuses),
                        (message,),
                    )
            elif message.lower() != "success":
                self._set_state(GatewayFeedState.BLOCKED)
                raise GatewayControlError(
                    "ADVANCED_SUBSCRIPTION_ACK_INVALID",
                    tuple(statuses),
                    (message,),
                )
        return len(rows)

    def _accumulate_subscription_membership(
        self,
        rows: tuple[dict[str, object], ...],
        accepted: set[str],
        rejected: set[str],
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        statuses = {
            str(row.get("status") or "").strip().lower()
            for row in rows
        }
        for row in rows:
            status = str(row.get("status") or "").strip().lower()
            message = str(row.get("message") or "").strip()
            if status == "success" and message.lower() == "success":
                continue
            prefixes = (
                ("subscribed to:", accepted),
                ("rejected:", rejected),
                ("not subscribed to:", rejected),
                ("subscription rejected:", rejected),
            )
            matched = False
            for prefix, destination in prefixes:
                if not message.lower().startswith(prefix):
                    continue
                destination.update(
                    value.strip()
                    for value in message[len(prefix):].split(",")
                    if value.strip()
                )
                matched = True
                break
            if not matched:
                raise GatewayControlError(
                    "ADVANCED_SUBSCRIPTION_ACK_INVALID",
                    tuple(statuses),
                    (message,),
                )
        requested = set(self._subscriptions)
        extras = (accepted | rejected) - requested
        if extras:
            raise GatewayControlError(
                "ADVANCED_SUBSCRIPTION_ACK_SCOPE_MISMATCH",
                tuple(statuses),
                tuple(sorted(extras)),
            )
        overlap = accepted & rejected
        if overlap:
            raise GatewayControlError(
                "ADVANCED_SUBSCRIPTION_ACK_CONFLICT",
                tuple(statuses),
                tuple(sorted(overlap)),
            )
        return tuple(sorted(accepted)), tuple(
            sorted(rejected)
        )

    async def run_subscription_probe(
        self,
        *,
        timeout_seconds: float = 10.0,
        maximum_control_bytes: int = 64 * 1024,
    ) -> GatewaySubscriptionProbeResult:
        if not 0 < timeout_seconds <= 60:
            raise ValueError(
                "subscription probe timeout must be in (0, 60]"
            )
        if not 1024 <= maximum_control_bytes <= 1024 * 1024:
            raise ValueError(
                "subscription probe control-byte ceiling is invalid"
            )
        try:
            return await asyncio.wait_for(
                self._run_subscription_probe(
                    maximum_control_bytes=maximum_control_bytes,
                ),
                timeout=timeout_seconds,
            )
        except asyncio.TimeoutError as error:
            self._set_state(GatewayFeedState.BLOCKED)
            accepted = tuple(sorted(self._probe_accepted))
            rejected = tuple(sorted(self._probe_rejected))
            raise GatewaySubscriptionProbeFailure(
                "ADVANCED_SUBSCRIPTION_PROBE_TIMEOUT",
                received_control_messages=self._received_messages,
                received_bytes=self._received_bytes,
                accepted_subscriptions=accepted,
                rejected_subscriptions=rejected,
                unconfirmed_subscriptions=tuple(sorted(
                    set(self._subscriptions)
                    - set(accepted)
                    - set(rejected)
                )),
            ) from error
        except GatewayOverload as error:
            if (
                error.reason_code
                != "ADVANCED_SESSION_BYTE_CEILING_EXCEEDED"
            ):
                raise
            raise GatewaySubscriptionProbeFailure(
                "ADVANCED_SUBSCRIPTION_PROBE_BYTE_CEILING_EXCEEDED",
                received_control_messages=self._received_messages,
                received_bytes=self._received_bytes,
                accepted_subscriptions=tuple(
                    sorted(self._probe_accepted)
                ),
                rejected_subscriptions=tuple(
                    sorted(self._probe_rejected)
                ),
                unconfirmed_subscriptions=tuple(sorted(
                    set(self._subscriptions)
                    - self._probe_accepted
                    - self._probe_rejected
                )),
            ) from error

    async def _run_subscription_probe(
        self,
        *,
        maximum_control_bytes: int,
    ) -> GatewaySubscriptionProbeResult:
        stream_session_id = self._session_id_factory()
        self._active_stream_session_id = stream_session_id
        self._received_messages = 0
        self._received_bytes = 0
        self._maximum_received_bytes = maximum_control_bytes
        self._stop_requested = None
        self._stop_at = None
        self._receive_poll_seconds = 1.0
        self._probe_accepted = set()
        self._probe_rejected = set()
        subscribe_payload = json.dumps(
            {
                "action": "subscribe",
                "params": ",".join(self._subscriptions),
            },
            separators=(",", ":"),
        )
        self._set_state(GatewayFeedState.CONNECTING)
        connection = None
        try:
            async with self._watched_connection() as connection:
                self._set_state(GatewayFeedState.AUTHENTICATING)
                await connection.send(
                    json.dumps(
                        {"action": "auth", "params": self._api_key},
                        separators=(",", ":"),
                    )
                )
                await self._await_control(
                    connection,
                    accepted_statuses=frozenset({"auth_success"}),
                    failure_prefix="ADVANCED_AUTH",
                    allow_market_message=False,
                )
                self._set_state(GatewayFeedState.SUBSCRIBING)
                await connection.send(subscribe_payload)
                requested = set(self._subscriptions)
                while (
                    self._probe_accepted | self._probe_rejected
                ) != requested:
                    payload = await self._receive_bytes(connection)
                    rows = self._control_rows(payload)
                    if rows is None:
                        self._set_state(GatewayFeedState.BLOCKED)
                        raise GatewayControlError(
                            "ADVANCED_SUBSCRIPTION_MARKET_BEFORE_READY"
                        )
                    accepted, rejected = (
                        self._accumulate_subscription_membership(
                            rows,
                            self._probe_accepted,
                            self._probe_rejected,
                        )
                    )
                if rejected:
                    self._set_state(GatewayFeedState.BLOCKED)
                else:
                    self._set_state(GatewayFeedState.WARMING)
                await connection.close(
                    code=1000,
                    reason="subscription probe complete",
                )
            if not rejected:
                self._set_state(GatewayFeedState.STOPPED)
            return GatewaySubscriptionProbeResult(
                stream_session_id=stream_session_id,
                feed=self.configuration.feed,
                requested_subscriptions=self._subscriptions,
                accepted_subscriptions=accepted,
                rejected_subscriptions=rejected,
                received_control_messages=self._received_messages,
                received_bytes=self._received_bytes,
                subscribe_request_bytes=len(
                    subscribe_payload.encode("utf-8")
                ),
                transitions=tuple(self._transitions),
            )
        except asyncio.CancelledError:
            self._set_state(GatewayFeedState.STOPPING)
            if connection is not None:
                await connection.close(
                    code=1001,
                    reason="subscription probe timeout",
                )
            raise

    async def _admit(
        self,
        payload: bytes,
        stream_session_id: UUID,
        connection,
    ) -> int:
        if len(payload) > self.configuration.maximum_message_bytes:
            self._set_state(GatewayFeedState.BLOCKED)
            await connection.close(code=1009, reason="message exceeds configured bound")
            raise GatewayOverload(
                "ADVANCED_MESSAGE_SIZE_EXCEEDED",
                stream_session_id,
                self._message_number,
                self._last_admitted_message_number,
            )
        envelope = RawMessageEnvelope(
            stream_session_id=stream_session_id,
            message_number=self._message_number,
            feed=self.configuration.feed,
            received_at=self._utc_clock(),
            received_monotonic_ns=self._monotonic_ns(),
            payload=payload,
        )
        try:
            self.buffer.put_nowait(envelope)
        except BufferFull as error:
            self._set_state(GatewayFeedState.BLOCKED)
            await connection.close(code=1013, reason="bounded handoff unavailable")
            raise GatewayOverload(
                f"ADVANCED_HANDOFF_{error.reason}",
                stream_session_id,
                self._message_number,
                self._last_admitted_message_number,
            ) from error
        self._last_admitted_message_number = self._message_number
        self._message_number += 1
        if self._market_observer is not None:
            self._market_observer(
                self._received_messages,
                self._received_bytes,
            )
        return len(payload)

    async def run_session(
        self,
        *,
        maximum_market_messages: int | None = None,
        maximum_received_bytes: int | None = None,
        initial_message_number: int = 0,
        stop_requested: Callable[[], bool] | None = None,
        stop_at: datetime | None = None,
        receive_poll_seconds: float = 1.0,
    ) -> GatewaySessionResult:
        if maximum_market_messages is not None and maximum_market_messages < 0:
            raise ValueError("maximum_market_messages must be nonnegative")
        if maximum_received_bytes is not None and maximum_received_bytes <= 0:
            raise ValueError("maximum_received_bytes must be positive")
        if type(initial_message_number) is not int or initial_message_number < 0:
            raise ValueError("initial_message_number must be nonnegative")
        if stop_requested is not None and not callable(stop_requested):
            raise TypeError("stop_requested must be callable")
        if receive_poll_seconds <= 0:
            raise ValueError("receive_poll_seconds must be positive")
        if stop_at is not None:
            if stop_at.tzinfo is None or stop_at.utcoffset() is None:
                raise ValueError("stop_at must be timezone-aware")
            stop_at = stop_at.astimezone(timezone.utc)
        stream_session_id = self._session_id_factory()
        self._active_stream_session_id = stream_session_id
        self._message_number = initial_message_number
        self._last_admitted_message_number = initial_message_number - 1
        self._received_messages = 0
        self._received_bytes = 0
        self._maximum_received_bytes = maximum_received_bytes
        self._kernel_receive_buffer_bytes = None
        self._stop_requested = stop_requested
        self._stop_at = stop_at
        self._receive_poll_seconds = receive_poll_seconds
        accepted_messages = 0
        accepted_bytes = 0
        kernel_receive_buffer_bytes = None
        stop_reason = None
        self._set_state(GatewayFeedState.CONNECTING)
        try:
            async with self._watched_connection() as connection:
                transport = getattr(connection, "transport", None)
                network_socket = (
                    None
                    if transport is None
                    else transport.get_extra_info("socket")
                )
                if network_socket is not None:
                    kernel_receive_buffer_bytes = network_socket.getsockopt(
                        socket.SOL_SOCKET,
                        socket.SO_RCVBUF,
                    )
                    self._kernel_receive_buffer_bytes = (
                        kernel_receive_buffer_bytes
                    )
                try:
                    self._set_state(GatewayFeedState.AUTHENTICATING)
                    await connection.send(
                        json.dumps(
                            {"action": "auth", "params": self._api_key},
                            separators=(",", ":"),
                        )
                    )
                    await self._await_control(
                        connection,
                        accepted_statuses=frozenset({"auth_success"}),
                        failure_prefix="ADVANCED_AUTH",
                        allow_market_message=False,
                    )
                    self._set_state(GatewayFeedState.SUBSCRIBING)
                    await connection.send(
                        json.dumps(
                            {
                                "action": "subscribe",
                                "params": ",".join(self._subscriptions),
                            },
                            separators=(",", ":"),
                        )
                    )
                    pending = await self._await_control(
                        connection,
                        accepted_statuses=frozenset({"success"}),
                        failure_prefix="ADVANCED_SUBSCRIPTION",
                        allow_market_message=True,
                    )
                    self._set_state(GatewayFeedState.WARMING)
                    delayed_subscription_rows = 0
                    while (
                        maximum_market_messages is None
                        or accepted_messages < maximum_market_messages
                    ):
                        payload = pending
                        pending = None
                        if payload is None:
                            payload = await self._receive_bytes(connection)
                        control_rows = self._control_rows(payload)
                        if control_rows is not None:
                            delayed_subscription_rows += (
                                self._accept_delayed_subscription_control(
                                    control_rows
                                )
                            )
                            if delayed_subscription_rows > len(
                                self._subscriptions
                            ):
                                self._set_state(GatewayFeedState.BLOCKED)
                                raise GatewayControlError(
                                    "ADVANCED_SUBSCRIPTION_ACK_LIMIT_EXCEEDED"
                                )
                            continue
                        accepted_bytes += await self._admit(
                            payload,
                            stream_session_id,
                            connection,
                        )
                        accepted_messages += 1
                        if accepted_messages == 1:
                            self._set_state(GatewayFeedState.LIVE)
                except _GatewayStopRequested as stopped:
                    stop_reason = stopped.reason
                finally:
                    if self._state not in {
                        GatewayFeedState.BLOCKED,
                        GatewayFeedState.STOPPING,
                        GatewayFeedState.STOPPED,
                    }:
                        self._set_state(GatewayFeedState.STOPPING)
                        await connection.close(
                            code=1000,
                            reason=(
                                "bounded session complete"
                                if stop_reason is None
                                else f"session stop: {stop_reason.lower()}"
                            )
                        )
            self._set_state(GatewayFeedState.STOPPED)
        except asyncio.CancelledError:
            self._set_state(GatewayFeedState.STOPPING)
            raise
        except (GatewayControlError, GatewayOverload):
            raise
        finally:
            if self._state is GatewayFeedState.STOPPING:
                self._set_state(GatewayFeedState.STOPPED)
        return GatewaySessionResult(
            stream_session_id=stream_session_id,
            feed=self.configuration.feed,
            subscriptions=self._subscriptions,
            received_messages=self._received_messages,
            received_bytes=self._received_bytes,
            accepted_messages=accepted_messages,
            accepted_bytes=accepted_bytes,
            final_state=self._state,
            transitions=tuple(self._transitions),
            library_max_queue=self.configuration.library_max_queue,
            kernel_receive_buffer_bytes=kernel_receive_buffer_bytes,
            stop_reason=stop_reason,
        )
