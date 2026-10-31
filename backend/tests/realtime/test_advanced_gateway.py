import asyncio
import json
import threading
from collections import deque
from datetime import date, datetime, timedelta, timezone
from uuid import UUID

import pytest
from websockets.asyncio.server import serve

from realtime.advanced_gateway import (
    AdvancedGateway,
    GatewayConfiguration,
    GatewayControlError,
    GatewayFeedState,
    GatewayOverload,
    GatewaySubscriptionProbeFailure,
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
from scripts.capture_advanced_realtime_v3_websockets import (
    _manifest as provider_test_manifest,
)


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


class QuietConnection(FakeConnection):
    async def recv(self, *, decode=None):
        if self.incoming:
            return await super().recv(decode=decode)
        await asyncio.Future()


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


def test_host_provider_test_uses_separate_equity_and_options_manifests():
    contracts = (
        "O:QQQ261030C00696000",
        "O:QQQ261030P00696000",
        "O:SPY261030C00761000",
        "O:SPY261030P00761000",
    )
    session = date(2026, 10, 30)

    equity = provider_test_manifest(session, contracts, options=False)
    options = provider_test_manifest(session, contracts, options=True)

    assert equity.universe.option_underlyings == ()
    assert equity.maximum_option_quote_contracts == 0
    assert options.universe.option_underlyings == ("QQQ", "SPY")
    assert options.maximum_option_quote_contracts == 4


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


def test_stock_gateway_skips_delayed_per_subscription_acknowledgements():
    market = b'[{"ev":"Q","sym":"SPY","bp":100,"ap":101,"t":1}]'
    delayed = json.dumps(
        [
            {
                "ev": "status",
                "status": "success",
                "message": "subscribed to: AM.SPY",
            },
            {
                "ev": "status",
                "status": "success",
                "message": "subscribed to: AM.TSLA",
            },
        ],
        separators=(",", ":"),
    ).encode("ascii")
    connection = FakeConnection(
        (_status("auth_success"), _status("success"), delayed, market)
    )
    buffer = _buffer()
    gateway = _gateway(MarketFeed.STOCKS, connection, buffer)

    result = asyncio.run(gateway.run_session(maximum_market_messages=1))
    claim = buffer.claim_nowait(maximum_events=1, maximum_bytes=4096)

    assert result.received_messages == 4
    assert result.accepted_messages == 1
    assert claim.messages[0].payload == market


def test_stock_gateway_fails_closed_on_delayed_control_rejection():
    connection = FakeConnection(
        (
            _status("auth_success"),
            _status("success"),
            _status("error"),
        )
    )
    buffer = _buffer()
    gateway = _gateway(MarketFeed.STOCKS, connection, buffer)

    with pytest.raises(
        GatewayControlError,
        match="ADVANCED_STREAM_CONTROL_REJECTED:error",
    ):
        asyncio.run(gateway.run_session(maximum_market_messages=1))

    assert gateway.state is GatewayFeedState.BLOCKED
    assert buffer.snapshot().admitted_events == 0


def test_stock_gateway_can_verify_subscription_without_market_data():
    connection = FakeConnection(
        (_status("auth_success"), _status("success"))
    )
    gateway = _gateway(MarketFeed.STOCKS, connection)

    result = asyncio.run(
        gateway.run_session(maximum_market_messages=0)
    )

    assert result.final_state is GatewayFeedState.STOPPED
    assert result.accepted_messages == 0
    assert result.subscriptions
    assert connection.sent[1]["action"] == "subscribe"


def test_subscription_probe_reports_exact_accepted_membership():
    manifest = _manifest()
    subscriptions = tuple(sorted(
        f"{member.channel.value}.{member.provider_symbol}"
        for member in manifest.members
        if member.asset_class is AssetClass.EQUITY
    ))
    acknowledged = json.dumps([
        {
            "ev": "status",
            "status": "success",
            "message": f"subscribed to: {subscription}",
        }
        for subscription in subscriptions
    ]).encode("ascii")
    connection = FakeConnection((
        _status("auth_success"),
        acknowledged,
    ))
    gateway = _gateway(MarketFeed.STOCKS, connection)

    result = asyncio.run(gateway.run_subscription_probe())

    assert result.accepted_subscriptions == subscriptions
    assert result.rejected_subscriptions == ()
    assert result.received_control_messages == 2
    assert connection.sent[1]["action"] == "subscribe"


def test_subscription_probe_accumulates_split_acknowledgements():
    subscriptions = tuple(sorted(
        f"{member.channel.value}.{member.provider_symbol}"
        for member in _manifest().members
        if member.asset_class is AssetClass.EQUITY
    ))
    midpoint = len(subscriptions) // 2

    def frame(values):
        return json.dumps([
            {
                "ev": "status",
                "status": "success",
                "message": f"subscribed to: {subscription}",
            }
            for subscription in values
        ]).encode("ascii")

    connection = FakeConnection((
        _status("auth_success"),
        frame(subscriptions[:midpoint]),
        frame(subscriptions[midpoint:]),
    ))
    gateway = _gateway(MarketFeed.STOCKS, connection)

    result = asyncio.run(gateway.run_subscription_probe())

    assert result.accepted_subscriptions == subscriptions
    assert result.rejected_subscriptions == ()


def test_generic_success_waits_for_explicit_member_acknowledgements():
    subscriptions = tuple(sorted(
        f"{member.channel.value}.{member.provider_symbol}"
        for member in _manifest().members
        if member.asset_class is AssetClass.EQUITY
    ))
    explicit = json.dumps([
        {
            "ev": "status",
            "status": "success",
            "message": f"subscribed to: {subscription}",
        }
        for subscription in subscriptions
    ]).encode("ascii")
    connection = FakeConnection((
        _status("auth_success"),
        _status("success"),
        explicit,
    ))
    gateway = _gateway(MarketFeed.STOCKS, connection)

    result = asyncio.run(gateway.run_subscription_probe())

    assert result.accepted_subscriptions == subscriptions
    assert result.received_control_messages == 3


def test_duplicate_ack_is_idempotent_and_out_of_scope_ack_fails():
    subscriptions = tuple(sorted(
        f"{member.channel.value}.{member.provider_symbol}"
        for member in _manifest().members
        if member.asset_class is AssetClass.EQUITY
    ))
    duplicate = json.dumps([
        *[
            {
                "ev": "status",
                "status": "success",
                "message": f"subscribed to: {subscription}",
            }
            for subscription in subscriptions
        ],
        {
            "ev": "status",
            "status": "success",
            "message": f"subscribed to: {subscriptions[0]}",
        },
    ]).encode("ascii")
    result = asyncio.run(
        _gateway(
            MarketFeed.STOCKS,
            FakeConnection((_status("auth_success"), duplicate)),
        ).run_subscription_probe()
    )
    assert result.accepted_subscriptions == subscriptions

    out_of_scope = json.dumps([{
        "ev": "status",
        "status": "success",
        "message": "subscribed to: AM.NOT_REQUESTED",
    }]).encode("ascii")
    with pytest.raises(
        GatewayControlError,
        match="ADVANCED_SUBSCRIPTION_ACK_SCOPE_MISMATCH",
    ):
        asyncio.run(
            _gateway(
                MarketFeed.STOCKS,
                FakeConnection((
                    _status("auth_success"),
                    out_of_scope,
                )),
            ).run_subscription_probe()
        )


def test_subscription_probe_reports_unconfirmed_missing_member():
    manifest = _manifest()
    subscriptions = tuple(sorted(
        f"{member.channel.value}.{member.provider_symbol}"
        for member in manifest.members
        if member.asset_class is AssetClass.EQUITY
    ))
    acknowledged = json.dumps([
        {
            "ev": "status",
            "status": "success",
            "message": f"subscribed to: {subscription}",
        }
        for subscription in subscriptions[:-1]
    ]).encode("ascii")
    connection = QuietConnection((
        _status("auth_success"),
        acknowledged,
    ))
    gateway = _gateway(MarketFeed.STOCKS, connection)

    with pytest.raises(GatewaySubscriptionProbeFailure) as captured:
        asyncio.run(gateway.run_subscription_probe(
            timeout_seconds=0.05,
            maximum_control_bytes=64 * 1024,
        ))

    assert captured.value.accepted_subscriptions == subscriptions[:-1]
    assert captured.value.rejected_subscriptions == ()
    assert captured.value.unconfirmed_subscriptions == subscriptions[-1:]
    assert gateway.state is GatewayFeedState.BLOCKED


def test_subscription_probe_reports_explicit_rejection():
    manifest = _manifest()
    subscriptions = tuple(sorted(
        f"{member.channel.value}.{member.provider_symbol}"
        for member in manifest.members
        if member.asset_class is AssetClass.EQUITY
    ))
    acknowledged = json.dumps([
        *[
            {
                "ev": "status",
                "status": "success",
                "message": f"subscribed to: {subscription}",
            }
            for subscription in subscriptions[:-1]
        ],
        {
            "ev": "status",
            "status": "error",
            "message": f"rejected: {subscriptions[-1]}",
        },
    ]).encode("ascii")
    connection = FakeConnection((
        _status("auth_success"),
        acknowledged,
    ))
    gateway = _gateway(MarketFeed.STOCKS, connection)

    result = asyncio.run(gateway.run_subscription_probe())

    assert result.accepted_subscriptions == subscriptions[:-1]
    assert result.rejected_subscriptions == subscriptions[-1:]


def test_subscription_probe_times_out_and_closes_during_silent_auth():
    connection = QuietConnection(())
    gateway = _gateway(MarketFeed.STOCKS, connection)

    with pytest.raises(
        GatewaySubscriptionProbeFailure,
        match="ADVANCED_SUBSCRIPTION_PROBE_TIMEOUT",
    ) as captured:
        asyncio.run(gateway.run_subscription_probe(
            timeout_seconds=0.05,
            maximum_control_bytes=4096,
        ))

    assert captured.value.received_control_messages == 0
    assert captured.value.received_bytes == 0
    assert connection.closed == (
        1001,
        "subscription probe timeout",
    )
    assert gateway.state is GatewayFeedState.BLOCKED


def test_subscription_probe_times_out_during_silent_subscription_ack():
    connection = QuietConnection((
        _status("auth_success"),
        _status("success"),
    ))
    gateway = _gateway(MarketFeed.STOCKS, connection)

    with pytest.raises(
        GatewaySubscriptionProbeFailure,
        match="ADVANCED_SUBSCRIPTION_PROBE_TIMEOUT",
    ) as captured:
        asyncio.run(gateway.run_subscription_probe(
            timeout_seconds=0.05,
            maximum_control_bytes=4096,
        ))

    assert captured.value.received_control_messages == 2
    assert captured.value.received_bytes > 0
    assert captured.value.accepted_subscriptions == ()
    assert captured.value.rejected_subscriptions == ()
    assert captured.value.unconfirmed_subscriptions
    assert connection.closed == (
        1001,
        "subscription probe timeout",
    )


def test_subscription_probe_closes_on_cumulative_control_byte_ceiling():
    oversized = json.dumps([{
        "ev": "status",
        "status": "success",
        "message": "x" * 2048,
    }]).encode("ascii")
    connection = FakeConnection((
        _status("auth_success"),
        oversized,
    ))
    gateway = _gateway(MarketFeed.STOCKS, connection)

    with pytest.raises(
        GatewaySubscriptionProbeFailure,
        match="ADVANCED_SUBSCRIPTION_PROBE_BYTE_CEILING_EXCEEDED",
    ) as captured:
        asyncio.run(gateway.run_subscription_probe(
            timeout_seconds=1,
            maximum_control_bytes=1024,
        ))

    assert captured.value.received_control_messages == 2
    assert captured.value.received_bytes > 1024
    assert connection.closed == (
        1009,
        "session exceeds configured byte ceiling",
    )


def test_quiet_subscribed_gateway_stops_without_waiting_for_market_data():
    stop = threading.Event()
    connection = QuietConnection(
        (_status("auth_success"), _status("success"))
    )
    gateway = _gateway(MarketFeed.OPTIONS, connection)

    async def scenario():
        asyncio.get_running_loop().call_later(0.05, stop.set)
        return await gateway.run_session(
            stop_requested=stop.is_set,
            stop_at=NOW + timedelta(hours=1),
            receive_poll_seconds=0.01,
        )

    result = asyncio.run(scenario())

    assert result.stop_reason == "REQUESTED"
    assert result.accepted_messages == 0
    assert result.final_state is GatewayFeedState.STOPPED
    assert GatewayFeedState.WARMING in result.transitions
    assert connection.closed == (1000, "session stop: requested")


def test_gateway_separates_received_usage_from_market_activity():
    market = b'[{"ev":"Q","sym":"SPY","bp":100,"ap":101,"t":1}]'
    connection = FakeConnection(
        (_status("auth_success"), _status("success"), market)
    )
    usage = []
    activity = []
    gateway = AdvancedGateway(
        GatewayConfiguration.stocks(),
        _manifest(),
        _buffer(),
        api_key="never-print-this",
        connection_factory=lambda _: connection,
        utc_clock=lambda: NOW,
        monotonic_ns=lambda: 100,
        session_id_factory=lambda: SESSION,
        usage_observer=lambda messages, size: usage.append((messages, size)),
        market_observer=lambda messages, size: activity.append((messages, size)),
    )

    asyncio.run(gateway.run_session(maximum_market_messages=1))

    assert len(usage) == 3
    assert activity == [usage[-1]]


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


@pytest.mark.parametrize("status", ("auth_failed", "max_connections"))
def test_gateway_auth_failure_is_blocked_and_never_admitted(status):
    connection = FakeConnection((_status("connected"), _status(status)))
    buffer = _buffer()
    gateway = _gateway(MarketFeed.STOCKS, connection, buffer)

    with pytest.raises(
        GatewayControlError,
        match=rf"ADVANCED_AUTH_REJECTED:{status}",
    ) as raised:
        asyncio.run(gateway.run_session(maximum_market_messages=1))

    assert raised.value.statuses == (status,)
    assert raised.value.provider_messages == (
        (status,) if status == "max_connections" else ()
    )
    assert gateway.state is GatewayFeedState.BLOCKED
    assert buffer.snapshot().admitted_events == 0


def test_gateway_received_byte_ceiling_blocks_before_market_admission():
    auth = _status("auth_success")
    subscribed = _status("success")
    market = (
        b'[{"ev":"T","sym":"O:SPY261218C00000001",'
        b'"p":1,"s":1,"t":1}]'
    )
    connection = FakeConnection((auth, subscribed, market))
    buffer = _buffer()
    gateway = _gateway(MarketFeed.OPTIONS, connection, buffer)

    with pytest.raises(
        GatewayOverload,
        match="ADVANCED_SESSION_BYTE_CEILING_EXCEEDED",
    ):
        asyncio.run(
            gateway.run_session(
                maximum_market_messages=1,
                maximum_received_bytes=len(auth) + len(subscribed),
            )
        )

    assert gateway.state is GatewayFeedState.BLOCKED
    assert gateway.received_bytes == len(auth) + len(subscribed) + len(market)
    assert buffer.snapshot().admitted_events == 0
    assert connection.closed == (
        1009,
        "session exceeds configured byte ceiling",
    )


def test_gateway_can_continue_message_numbers_across_reconnect_attempts():
    market = (
        b'[{"ev":"T","sym":"O:SPY261218C00000001",'
        b'"p":1,"s":1,"t":1}]'
    )
    connection = FakeConnection(
        (_status("auth_success"), _status("success"), market)
    )
    buffer = _buffer()
    gateway = _gateway(MarketFeed.OPTIONS, connection, buffer)

    asyncio.run(
        gateway.run_session(
            maximum_market_messages=1,
            initial_message_number=7,
        )
    )
    claim = buffer.claim_nowait(maximum_events=1, maximum_bytes=4096)

    assert claim.messages[0].message_number == 7
    assert gateway.next_message_number == 8


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
