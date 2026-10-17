import asyncio
import json
from collections import deque
from datetime import date, datetime, timezone
from uuid import UUID

import pytest
from websockets.asyncio.server import serve

from realtime.advanced_gateway import (
    AdvancedGateway,
    GatewayConfiguration,
    GatewayControlError,
    GatewayFeedState,
    GatewayOverload,
)
from realtime.buffer import BufferLimits, RawMessageBuffer
from realtime.bridge import drain_raw_buffer_once
from realtime.contracts import (
    CANARY_UNIVERSE,
    AssetClass,
    MarketChannel,
    MarketFeed,
    RawMessageEnvelope,
    SubscriptionManifest,
    SubscriptionMember,
)
from realtime.journal import RawJournalReader, RawJournalWriter
from realtime.normalizer import InstrumentAdmission, normalize_raw_message


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 14, 30, tzinfo=UTC)
SESSION = UUID("22222222-2222-2222-2222-222222222222")


class FakeConnection:
    def __init__(self, incoming):
        self.incoming = deque(incoming)
        self.sent = []
        self.recv_decode = []
        self.closed = None
        self.active_receivers = 0
        self.maximum_active_receivers = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        return False

    async def send(self, payload):
        self.sent.append(json.loads(payload))

    async def recv(self, *, decode=None):
        self.active_receivers += 1
        self.maximum_active_receivers = max(
            self.maximum_active_receivers,
            self.active_receivers,
        )
        try:
            self.recv_decode.append(decode)
            return self.incoming.popleft()
        finally:
            self.active_receivers -= 1

    async def close(self, *, code, reason):
        self.closed = (code, reason)


def _manifest():
    members = []
    catalog_version = 1
    for ticker in CANARY_UNIVERSE.equity_tickers:
        for channel in (
            MarketChannel.TRADE,
            MarketChannel.QUOTE,
            MarketChannel.MINUTE_AGGREGATE,
        ):
            members.append(
                SubscriptionMember(AssetClass.EQUITY, ticker, ticker, channel)
            )
        symbol = f"O:{ticker}261218C00000001"
        for channel in (MarketChannel.TRADE, MarketChannel.QUOTE):
            members.append(
                SubscriptionMember(
                    AssetClass.OPTION,
                    symbol,
                    ticker,
                    channel,
                    catalog_version_id=catalog_version,
                )
            )
        catalog_version += 1
    return SubscriptionManifest(
        version="advanced-fake-v1",
        session_date=date(2026, 10, 5),
        universe=CANARY_UNIVERSE,
        capability_profile_sha256="a" * 64,
        allocation_policy_sha256="b" * 64,
        members=tuple(members),
    )


def _buffer(maximum_events=4, maximum_bytes=4096):
    return RawMessageBuffer(
        BufferLimits(maximum_events, maximum_bytes, 1_000_000),
        monotonic_ns=lambda: 100,
    )


def _status(status):
    return json.dumps(
        [{"ev": "status", "status": status, "message": status}],
        separators=(",", ":"),
    ).encode("ascii")


def _gateway(feed, connection, buffer=None):
    configuration = (
        GatewayConfiguration.stocks()
        if feed is MarketFeed.STOCKS
        else GatewayConfiguration.options()
    )
    return AdvancedGateway(
        configuration,
        _manifest(),
        buffer or _buffer(),
        api_key="never-print-this",
        connection_factory=lambda _: connection,
        utc_clock=lambda: NOW,
        monotonic_ns=lambda: 100,
        session_id_factory=lambda: SESSION,
    )


def test_stock_gateway_authenticates_subscribes_and_hands_off_raw_bytes():
    market = b'[{"ev":"Q","sym":"SPY","bp":100,"ap":101,"t":1}]'
    connection = FakeConnection(
        (_status("connected"), _status("auth_success"), _status("success"), market)
    )
    buffer = _buffer()
    gateway = _gateway(MarketFeed.STOCKS, connection, buffer)

    result = asyncio.run(gateway.run_session(maximum_market_messages=1))
    claim = buffer.claim_nowait(maximum_events=1, maximum_bytes=4096)

    assert result.final_state is GatewayFeedState.STOPPED
    assert result.received_messages == 4
    assert result.received_bytes > result.accepted_bytes
    assert result.accepted_messages == 1
    assert claim.messages[0].payload == market
    assert claim.messages[0].stream_session_id == SESSION
    assert connection.sent[0] == {
        "action": "auth",
        "params": "never-print-this",
    }
    assert connection.sent[1]["action"] == "subscribe"
    assert "Q.SPY" in connection.sent[1]["params"]
    assert "AM.SPY" in connection.sent[1]["params"]
    assert all(value is False for value in connection.recv_decode)
    assert connection.maximum_active_receivers == 1
    assert "never-print-this" not in repr(result)


def test_option_gateway_uses_explicit_contract_trade_and_quote_membership():
    connection = FakeConnection(
        (
            _status("auth_success"),
            _status("success"),
            b'[{"ev":"T","sym":"O:SPY261218C00000001","p":1,"s":1,"t":1}]',
        )
    )
    gateway = _gateway(MarketFeed.OPTIONS, connection)

    asyncio.run(gateway.run_session(maximum_market_messages=1))

    subscriptions = connection.sent[1]["params"].split(",")
    assert len(subscriptions) == 16
    assert all(item.startswith(("T.O:", "Q.O:")) for item in subscriptions)
    assert not any("*" in item for item in subscriptions)


def test_gateway_auth_failure_is_blocked_and_never_admitted():
    connection = FakeConnection((_status("connected"), _status("auth_failed")))
    buffer = _buffer()
    gateway = _gateway(MarketFeed.STOCKS, connection, buffer)

    with pytest.raises(GatewayControlError, match="ADVANCED_AUTH_REJECTED"):
        asyncio.run(gateway.run_session(maximum_market_messages=1))

    assert gateway.state is GatewayFeedState.BLOCKED
    assert buffer.snapshot().admitted_events == 0


def test_gateway_handoff_overload_disconnects_without_evicting():
    buffer = _buffer(maximum_events=1, maximum_bytes=128)
    buffer.put_nowait(
        RawMessageEnvelope(
            SESSION,
            99,
            MarketFeed.STOCKS,
            NOW,
            100,
            b"already-admitted",
        )
    )
    connection = FakeConnection(
        (
            _status("auth_success"),
            _status("success"),
            b'[{"ev":"Q","sym":"SPY","bp":100,"ap":101,"t":1}]',
        )
    )
    gateway = _gateway(MarketFeed.STOCKS, connection, buffer)

    with pytest.raises(GatewayOverload, match="MAXIMUM_EVENTS_EXCEEDED") as error:
        asyncio.run(gateway.run_session(maximum_market_messages=1))

    assert error.value.last_admitted_message_number == -1
    assert gateway.state is GatewayFeedState.BLOCKED
    assert connection.closed[0] == 1013
    claim = buffer.claim_nowait(maximum_events=1, maximum_bytes=128)
    assert claim.messages[0].message_number == 99


def test_gateway_configuration_matches_reviewed_websocket_bounds():
    configuration = GatewayConfiguration.stocks()

    assert configuration.compression is None
    assert configuration.open_timeout_seconds == 10
    assert configuration.ping_interval_seconds == 20
    assert configuration.ping_timeout_seconds == 10
    assert configuration.close_timeout_seconds == 5
    assert configuration.maximum_message_bytes == 2 * 1024 * 1024
    assert configuration.library_max_queue == 8


def test_gateway_uses_real_websockets_client_against_loopback_fake_server():
    async def scenario():
        controls = []

        async def handler(websocket):
            controls.append(json.loads(await websocket.recv()))
            await websocket.send(_status("auth_success").decode("ascii"))
            controls.append(json.loads(await websocket.recv()))
            await websocket.send(_status("success").decode("ascii"))
            await websocket.send('[{"ev":"Q","sym":"SPY","bp":100,"ap":101,"t":1}]')

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            configuration = GatewayConfiguration(
                MarketFeed.STOCKS,
                f"ws://127.0.0.1:{port}",
                allow_insecure_loopback=True,
            )
            buffer = _buffer()
            gateway = AdvancedGateway(
                configuration,
                _manifest(),
                buffer,
                api_key="loopback-only",
                utc_clock=lambda: NOW,
                monotonic_ns=lambda: 100,
                session_id_factory=lambda: SESSION,
            )
            result = await gateway.run_session(maximum_market_messages=1)
        return result, controls, buffer

    result, controls, buffer = asyncio.run(scenario())

    assert result.accepted_messages == 1
    assert [row["action"] for row in controls] == ["auth", "subscribe"]
    claim = buffer.claim_nowait(maximum_events=1, maximum_bytes=4096)
    assert claim.messages[0].payload.startswith(b'[{"ev":"Q"')


def test_option_gateway_uses_real_loopback_server():
    async def scenario():
        async def handler(websocket):
            await websocket.recv()
            await websocket.send(_status("auth_success").decode("ascii"))
            subscription = json.loads(await websocket.recv())
            await websocket.send(_status("success").decode("ascii"))
            await websocket.send(
                '[{"ev":"Q","sym":"O:SPY261218C00000001",'
                '"bp":1,"ap":1.1,"bs":2,"as":3,"t":1}]'
            )
            assert "Q.O:SPY261218C00000001" in subscription["params"]

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            gateway = AdvancedGateway(
                GatewayConfiguration(
                    MarketFeed.OPTIONS,
                    f"ws://127.0.0.1:{port}",
                    allow_insecure_loopback=True,
                ),
                _manifest(),
                _buffer(),
                api_key="loopback-only",
                utc_clock=lambda: NOW,
                monotonic_ns=lambda: 100,
            )
            return await gateway.run_session(maximum_market_messages=1)

    assert asyncio.run(scenario()).accepted_messages == 1


def test_gateway_subscription_rejection_is_blocked():
    connection = FakeConnection(
        (_status("auth_success"), _status("error"))
    )
    gateway = _gateway(MarketFeed.STOCKS, connection)

    with pytest.raises(
        GatewayControlError,
        match="ADVANCED_SUBSCRIPTION_REJECTED",
    ):
        asyncio.run(gateway.run_session(maximum_market_messages=1))

    assert gateway.state is GatewayFeedState.BLOCKED


def test_gateway_oversized_message_closes_with_protocol_size_code():
    connection = FakeConnection(
        (_status("auth_success"), _status("success"), b"x" * 11)
    )
    configuration = GatewayConfiguration(
        MarketFeed.STOCKS,
        "wss://socket.polygon.io/stocks",
        maximum_message_bytes=10,
    )
    gateway = AdvancedGateway(
        configuration,
        _manifest(),
        _buffer(),
        api_key="never-print-this",
        connection_factory=lambda _: connection,
        utc_clock=lambda: NOW,
        monotonic_ns=lambda: 100,
    )

    with pytest.raises(GatewayOverload, match="MESSAGE_SIZE_EXCEEDED"):
        asyncio.run(gateway.run_session(maximum_market_messages=1))

    assert connection.closed[0] == 1009


def test_gateway_raw_array_replays_through_journal_and_normalizer(tmp_path):
    start = int(NOW.timestamp() * 1000) - 60_000
    end = start + 60_000
    payload = (
        '[{"ev":"Q","sym":"SPY","bp":100,"ap":101,"bs":2,"as":3,'
        f'"t":{end},"q":1}},'
        '{"ev":"T","sym":"SPY","p":100.5,"s":10,"x":4,'
        f'"c":[0],"t":{end},"q":2}},'
        '{"ev":"AM","sym":"SPY","o":100,"h":101,"l":99,'
        f'"c":100.5,"v":1000,"vw":100.25,"s":{start},"e":{end}}}]'
    ).encode("ascii")
    connection = FakeConnection(
        (_status("auth_success"), _status("success"), payload)
    )
    buffer = _buffer()
    gateway = _gateway(MarketFeed.STOCKS, connection, buffer)

    asyncio.run(gateway.run_session(maximum_market_messages=1))
    path = tmp_path / "advanced.sqlite"
    with RawJournalWriter(path, generation=SESSION) as writer:
        commit = drain_raw_buffer_once(
            buffer,
            writer,
            maximum_events=1,
            maximum_bytes=4096,
        )
    raw = RawJournalReader(path).read_after(0)[0]
    normalized = normalize_raw_message(
        raw,
        {
            "SPY": InstrumentAdmission(
                1,
                "SPY",
                "SPY",
                AssetClass.EQUITY,
                0,
            )
        },
    )

    assert commit.inserted_count == 1
    assert normalized.quarantines == ()
    assert [event.event_kind for event in normalized.events] == [
        MarketChannel.QUOTE,
        MarketChannel.TRADE,
        MarketChannel.MINUTE_AGGREGATE,
    ]