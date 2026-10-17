from __future__ import annotations

import asyncio
import json
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
    pass


@dataclass(frozen=True, slots=True)
class GatewayOverload(RuntimeError):
    reason_code: str
    stream_session_id: UUID
    last_received_message_number: int
    last_admitted_message_number: int

    def __str__(self) -> str:
        return self.reason_code


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
        ) <= 0:
            raise ValueError("Advanced gateway bounds must be positive")

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


_CHANNEL_ORDER = {
    MarketChannel.TRADE: 0,
    MarketChannel.QUOTE: 1,
    MarketChannel.MINUTE_AGGREGATE: 2,
}


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
        self._state = GatewayFeedState.DISCONNECTED
        self._transitions = [self._state]
        self._message_number = 0
        self._last_admitted_message_number = -1
        self._received_messages = 0
        self._received_bytes = 0
        self._subscriptions = self._build_subscriptions()

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
            ping_timeout=self.configuration.ping_timeout_seconds,
            close_timeout=self.configuration.close_timeout_seconds,
            max_size=self.configuration.maximum_message_bytes,
            max_queue=self.configuration.library_max_queue,
        )

    async def _receive_bytes(self, connection) -> bytes:
        message = await connection.recv(decode=False)
        if type(message) is not bytes:
            raise GatewayControlError("ADVANCED_NON_BYTES_MESSAGE")
        self._received_messages += 1
        self._received_bytes += len(message)
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
            if statuses & accepted_statuses:
                return None
            if statuses & {
                "auth_failed",
                "failed",
                "error",
                "not_authorized",
                "max_connections",
            }:
                self._set_state(GatewayFeedState.BLOCKED)
                raise GatewayControlError(f"{failure_prefix}_REJECTED")
        self._set_state(GatewayFeedState.BLOCKED)
        raise GatewayControlError(f"{failure_prefix}_CONTROL_LIMIT_EXCEEDED")

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
        return len(payload)

    async def run_session(
        self,
        *,
        maximum_market_messages: int | None = None,
    ) -> GatewaySessionResult:
        if maximum_market_messages is not None and maximum_market_messages <= 0:
            raise ValueError("maximum_market_messages must be positive")
        stream_session_id = self._session_id_factory()
        self._message_number = 0
        self._last_admitted_message_number = -1
        self._received_messages = 0
        self._received_bytes = 0
        accepted_messages = 0
        accepted_bytes = 0
        self._set_state(GatewayFeedState.CONNECTING)
        try:
            async with self._connection_context() as connection:
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
                while (
                    maximum_market_messages is None
                    or accepted_messages < maximum_market_messages
                ):
                    payload = pending
                    pending = None
                    if payload is None:
                        payload = await self._receive_bytes(connection)
                    accepted_bytes += await self._admit(
                        payload,
                        stream_session_id,
                        connection,
                    )
                    accepted_messages += 1
                    if accepted_messages == 1:
                        self._set_state(GatewayFeedState.LIVE)
                self._set_state(GatewayFeedState.STOPPING)
                await connection.close(code=1000, reason="bounded session complete")
            self._set_state(GatewayFeedState.STOPPED)
        except (GatewayControlError, GatewayOverload):
            raise
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
        )
