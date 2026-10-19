import asyncio
import socket
import sqlite3
from random import Random
from unittest.mock import MagicMock
from uuid import UUID

import psycopg2
import pytest
import requests
from websockets.exceptions import ConnectionClosedError

from realtime.advanced_equity_ingress import (
    AdvancedEquityJournalPump,
    AdvancedJournalDecodeError,
)
from realtime.advanced_gateway import AdvancedGateway, GatewayConfiguration, GatewayOverload
from realtime.advanced_ingress_controller import (
    AdvancedReconnectState,
    IngressFailureDisposition,
    classify_advanced_ingress_failure,
    full_jitter_backoff_seconds,
)
from realtime.advanced_journal_replay import JournalReplayQuarantine, inspect_registered_journal
from realtime.bridge import drain_raw_buffer_once
from realtime.contracts import MarketFeed
from realtime.journal import RawJournalReader, RawJournalWriter
from tests.realtime.test_advanced_equity_ingress import (
    GENERATION,
    _admissions,
    _message,
)
from tests.realtime.test_advanced_gateway import (
    NOW,
    FakeConnection,
    _buffer,
    _gateway,
    _manifest,
    _status,
)
from tests.realtime.test_advanced_journal_registry import _registration


class FaultConnection(FakeConnection):
    def __init__(self, incoming, *, connect_error=None):
        super().__init__(incoming)
        self.connect_error = connect_error

    async def __aenter__(self):
        if self.connect_error is not None:
            raise self.connect_error
        return self

    async def recv(self, *, decode=None):
        if isinstance(self.incoming[0], Exception):
            raise self.incoming.popleft()
        return await super().recv(decode=decode)


@pytest.mark.parametrize(
    ("error", "at_connect"),
    (
        pytest.param(socket.gaierror(-3, "simulated DNS loss"), True, id="dns-loss"),
        pytest.param(ConnectionRefusedError("simulated TCP refusal"), True, id="tcp-refusal"),
        pytest.param(ConnectionClosedError(None, None), False, id="socket-close"),
        pytest.param(TimeoutError("simulated ping timeout"), False, id="delayed-pong"),
    ),
)
def test_transient_transport_fault_preserves_input_and_fresh_session(error, at_connect):
    payload = b'[{"ev":"AM","sym":"SPY"}]'
    ready = (_status("auth_success"), _status("success"))
    failed = FaultConnection(
        () if at_connect else (*ready, payload, error),
        connect_error=error if at_connect else None,
    )
    recovered = FakeConnection((*ready, payload))
    connections = iter((failed, recovered))
    sessions = iter((UUID(int=1), UUID(int=2)))
    buffer = _buffer()
    gateway = AdvancedGateway(
        GatewayConfiguration.stocks(),
        _manifest(),
        buffer,
        api_key="synthetic-offline-key",
        connection_factory=lambda _config: next(connections),
        utc_clock=lambda: NOW,
        monotonic_ns=lambda: 100,
        session_id_factory=lambda: next(sessions),
    )

    with pytest.raises(type(error)) as captured:
        asyncio.run(gateway.run_session(maximum_market_messages=2))
    failure = classify_advanced_ingress_failure(
        type(captured.value).__name__,
        detail=str(captured.value),
    )
    assert failure.disposition is IngressFailureDisposition.TRANSIENT
    state = AdvancedReconnectState().after_failure(
        failure,
        stable_live_reset_seconds=60,
    )
    backoff = full_jitter_backoff_seconds(
        state,
        initial_seconds=0.5,
        multiplier=2,
        maximum_seconds=30,
        jitter_ceiling_seconds=30,
        random_source=Random(1),
    )
    assert 0 <= backoff <= 0.5

    result = asyncio.run(gateway.run_session(maximum_market_messages=1))
    assert result.stream_session_id == UUID(int=2)
    claim = buffer.claim_nowait(maximum_events=4, maximum_bytes=4096)
    expected_sessions = [UUID(int=2)] if at_connect else [UUID(int=1), UUID(int=2)]
    assert [row.stream_session_id for row in claim.messages] == expected_sessions
    assert all(row.payload == payload for row in claim.messages)


def test_malformed_market_bytes_remain_durable_without_normalized_progress(tmp_path):
    payload = b"not valid market JSON"
    buffer = _buffer()
    buffer.put_nowait(_message(payload))
    path = tmp_path / "malformed.sqlite"
    pump = AdvancedEquityJournalPump(
        buffer,
        path,
        GENERATION,
        _admissions(),
        maximum_batch_messages=4,
        maximum_batch_bytes=4096,
    )

    pump.start()
    pump.request_stop()
    with pytest.raises(AdvancedJournalDecodeError, match="QUARANTINED"):
        pump.join(5)

    reader = RawJournalReader(path)
    assert [row.payload for row in reader.read_after(0)] == [payload]
    assert reader.read_normalized_after(0) == ()
    assert pump.progress.last_event_offset == 0


def test_full_journal_releases_claim_and_replays_without_loss(tmp_path):
    buffer = _buffer()
    message = _message(b'[{"ev":"AM","sym":"SPY"}]')
    buffer.put_nowait(message)
    failed_writer = MagicMock(spec=RawJournalWriter)
    failed_writer.append_batch.side_effect = sqlite3.OperationalError("database or disk is full")

    with pytest.raises(sqlite3.OperationalError, match="disk is full"):
        drain_raw_buffer_once(buffer, failed_writer, maximum_events=4, maximum_bytes=4096)

    assert buffer.snapshot().queued_events == 1
    assert buffer.snapshot().claimed_events == 0
    path = tmp_path / "recovered.sqlite"
    with RawJournalWriter(path, generation=GENERATION) as writer:
        commit = drain_raw_buffer_once(buffer, writer, maximum_events=4, maximum_bytes=4096)
    assert commit.inserted_count == 1
    assert buffer.snapshot().admitted_events == 0
    assert [row.payload for row in RawJournalReader(path).read_after(0)] == [message.payload]


def test_stalled_bridge_claim_applies_backpressure_without_eviction(tmp_path):
    buffer = _buffer(maximum_events=1)
    retained = _message(b"retained-before-stall")
    buffer.put_nowait(retained)
    claim = buffer.claim_nowait(maximum_events=1, maximum_bytes=4096)
    connection = FakeConnection((
        _status("auth_success"),
        _status("success"),
        b'[{"ev":"AM","sym":"SPY"}]',
    ))
    gateway = _gateway(MarketFeed.STOCKS, connection, buffer)

    with pytest.raises(GatewayOverload, match="MAXIMUM_EVENTS_EXCEEDED"):
        asyncio.run(gateway.run_session(maximum_market_messages=1))

    assert buffer.snapshot().claimed_events == 1
    assert claim.messages == (retained,)
    buffer.release(claim.claim_id)
    path = tmp_path / "unstalled.sqlite"
    with RawJournalWriter(path, generation=GENERATION) as writer:
        drain_raw_buffer_once(buffer, writer, maximum_events=1, maximum_bytes=4096)
    assert [row.payload for row in RawJournalReader(path).read_after(0)] == [retained.payload]


def test_corrupt_registered_journal_is_quarantined_without_replacement(tmp_path):
    path = tmp_path / "corrupt.sqlite"
    original = b"deliberately invalid SQLite journal"
    path.write_bytes(original)
    registration = _registration(tmp_path, path)

    with pytest.raises(JournalReplayQuarantine, match="CORRUPT_JOURNAL") as captured:
        inspect_registered_journal(tmp_path, registration)

    assert captured.value.reason == "CORRUPT_JOURNAL"
    assert path.read_bytes() == original


def test_rehearsal_guard_blocks_network_and_database(request):
    if not request.config.getoption("--realtime-no-network", default=False):
        pytest.skip("network-denial proof is required only by the offline rehearsal")
    with pytest.raises(RuntimeError, match="I/O prohibited"):
        socket.getaddrinfo("offline.invalid", 443)
    with socket.socket() as connection:
        with pytest.raises(RuntimeError, match="I/O prohibited"):
            connection.connect(("127.0.0.1", 1))
        with pytest.raises(RuntimeError, match="I/O prohibited"):
            connection.connect_ex(("127.0.0.1", 1))
    left, right = socket.socketpair()
    with left, right:
        left.sendall(b"private wakeup")
        assert right.recv(32) == b"private wakeup"
    with pytest.raises(RuntimeError, match="I/O prohibited"):
        requests.get("https://offline.invalid")
    with pytest.raises(RuntimeError, match="I/O prohibited"):
        psycopg2.connect("dbname=offline_rehearsal")
