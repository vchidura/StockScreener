from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from io import BytesIO
import json
from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest
from requests import ConnectionError, Timeout

from options.data.polygon_http import SingleAttemptPolygonHttpTransport
from realtime import advanced_options_continuous_scheduler as scheduler
from realtime.acquisition_ledger import ProviderRequestOutcome, ProviderRequestState
from realtime.contracts import canonical_sha256
from realtime.advanced_options_p8_selection_source import combine_exact_contract_captures


NOW = datetime(2026, 11, 2, 14, 35, 20, tzinfo=timezone.utc)


class Clock:
    def __init__(self):
        self.value = NOW

    def __call__(self):
        value = self.value
        self.value += timedelta(milliseconds=5)
        return value


def _snapshots(underlying_count, observed):
    rows = {}
    timestamp = int(observed.timestamp() * 1_000_000_000)
    for index in range(underlying_count):
        underlying = f"T{chr(65 + index // 26)}{chr(65 + index % 26)}"
        for strike in range(498, 502):
            for side, kind in (("C", "call"), ("P", "put")):
                ticker = f"O:{underlying}261106{side}{strike * 1000:08d}"
                rows[ticker] = {
                    "details": {
                        "ticker": ticker, "contract_type": kind,
                        "expiration_date": "2026-11-06", "strike_price": strike,
                        "exercise_style": "american", "shares_per_contract": 100,
                    },
                    "day": {"volume": 1000}, "open_interest": 200,
                    "last_quote": {"bid": 1, "ask": 1.1, "last_updated": timestamp},
                    "underlying_asset": {
                        "ticker": underlying, "price": 500,
                        "last_updated": timestamp, "timeframe": "REAL-TIME",
                    },
                }
    return rows


class Ledger:
    def __init__(self, monkeypatch):
        self.requests = {}
        self.payloads = {}
        monkeypatch.setattr(scheduler, "reserve_coordinated_provider_request_cursor", self.reserve)
        monkeypatch.setattr(scheduler, "load_recorded_provider_request_cursor", self.load)
        monkeypatch.setattr(scheduler, "persist_raw_payload_cursor", self.persist)
        monkeypatch.setattr(
            scheduler, "load_raw_payload_cursor",
            lambda _cursor, payload_id: self.payloads[payload_id],
        )
        monkeypatch.setattr(scheduler, "finish_provider_request_cursor", self.finish)

    @contextmanager
    def transaction(self, *_args, **_kwargs):
        yield SimpleNamespace(cursor=object())

    def reserve(self, _cursor, budget, reservation, **kwargs):
        assert kwargs["account_wide"] is True
        assert kwargs["reserved_response_bytes"] == scheduler.ACCOUNTED_RESPONSE_BYTE_CEILING
        fresh = reservation.request_id not in self.requests
        if fresh:
            assert len(self.requests) < budget.request_ceiling
            self.requests[reservation.request_id] = SimpleNamespace(
                reservation=reservation, completion=None,
            )
        return SimpleNamespace(newly_reserved=fresh)

    def load(self, _cursor, request_id):
        entry = self.requests[request_id]
        return entry if entry.completion is not None else None

    def persist(self, _cursor, body, **_kwargs):
        payload_id = uuid5(NAMESPACE_URL, canonical_sha256(body.hex()))
        self.payloads[payload_id] = body
        return payload_id

    def finish(self, _cursor, request_id, completion):
        self.requests[request_id].completion = completion


class Raw:
    def __init__(self, body, *, partial=False, after_read=None):
        self.buffer = BytesIO(body)
        self.partial = partial
        self.after_read = after_read
        self.reads = 0

    def read(self, size, *, decode_content):
        assert decode_content is True
        self.reads += 1
        if self.partial and self.reads > 1:
            raise Timeout("partial body timed out")
        body = self.buffer.read(min(size, 3) if self.partial else size)
        if self.after_read is not None:
            self.after_read()
        return body


class Harness:
    def __init__(self, monkeypatch, *, underlyings=1, status=200, mode=None):
        self.clock = Clock()
        self.ledger = Ledger(monkeypatch)
        self.calls = []
        self.responses = []
        self.status = status
        self.mode = mode
        self.rows = _snapshots(underlyings, NOW)
        self.acquisition = SimpleNamespace(
            contract_tickers=tuple(sorted(self.rows)),
            authority=SimpleNamespace(
                authorization_id=uuid4(), cohort_id=uuid4(), session_date=NOW.date(),
                acquisition_stop=NOW + timedelta(hours=5),
            ),
        )
        self.state = SimpleNamespace(load_acquisition_binding=lambda: self.acquisition)
        self.receipt = (str(uuid4()), NOW - timedelta(seconds=20))
        self.deadline = self.receipt[1] + timedelta(seconds=300)
        self.budget = scheduler._scheduler_budget(self.acquisition, "test-options-scheduler")

    def transport(self, api_key, **kwargs):
        harness = self

        class Session:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                pass

            def mount(self, _prefix, adapter):
                assert adapter.max_retries.total == 0

            def get(self, url, **options):
                harness.calls.append((url, options))
                assert options["allow_redirects"] is False
                assert options["stream"] is True
                assert 0 < options["timeout"] <= 30
                if harness.mode == "connection-timeout":
                    raise Timeout("connection timed out")
                tickers = options["params"]["ticker.any_of"].split(",")
                assert len(tickers) <= scheduler.CONTRACTS_PER_REQUEST
                results = [harness.rows[ticker] for ticker in tickers]
                if len(harness.calls) == 2:
                    if harness.mode == "missing":
                        results.pop()
                    elif harness.mode == "duplicate":
                        results[-1] = results[0]
                    elif harness.mode == "unexpected":
                        results[-1] = _snapshots(51, NOW)["O:TBY261106C00498000"]
                payload = {"status": "OK", "results": results, "request_id": str(len(harness.calls))}
                if harness.mode == "pagination" and len(harness.calls) == 2:
                    payload["next_url"] = "https://api.massive.com/v3/snapshot?cursor=unexpected"
                body = (
                    b"{" if harness.mode == "invalid-json"
                    else b"x" * (scheduler.RESPONSE_BYTE_CEILING + 10) if harness.mode == "oversize"
                    else b'{"error":"unavailable"}' if harness.status != 200
                    else json.dumps(payload).encode()
                )
                response = SimpleNamespace(
                    status_code=harness.status, headers={}, closed=False,
                )
                response.raw = Raw(
                    body, partial=harness.mode == "partial",
                    after_read=(
                        lambda: setattr(harness.clock, "value", harness.deadline)
                    ) if harness.mode == "deadline" else None,
                )
                response.close = lambda: setattr(response, "closed", True)
                harness.responses.append(response)
                return response

        return SingleAttemptPolygonHttpTransport(
            api_key, session_factory=Session, **kwargs,
        )

    def capture(self):
        return scheduler._capture(
            self.ledger, state=self.state, receipt=self.receipt, budget=self.budget,
            api_key="provider-free", clock=self.clock,
            transport_factory=self.transport, deadline=self.deadline,
        )


@pytest.mark.parametrize("underlyings,requests", [(1, 234), (50, 468), (125, 936)])
def test_session_budget_scales_with_bounded_frozen_batches(monkeypatch, underlyings, requests):
    harness = Harness(monkeypatch, underlyings=underlyings)
    assert harness.budget.request_ceiling == requests
    assert harness.budget.decoded_byte_ceiling == requests * 2_000_001


@pytest.mark.parametrize("underlyings", [1, 50, 125])
def test_completed_capture_replays_exact_evidence_with_advancing_clock(monkeypatch, underlyings):
    harness = Harness(monkeypatch, underlyings=underlyings)
    original = harness.capture()
    entries = tuple(harness.ledger.requests.values())
    assert all(
        entry.completion.requested_to < entry.completion.completed_at
        for entry in entries
    )
    harness.clock.value += timedelta(seconds=2)
    replay = harness.capture()
    assert replay == original
    assert len(harness.calls) == (underlyings * 8 + 249) // 250
    assert len(original.selection.contracts) == underlyings * 8
    assert set(original.selected_snapshots) == set(harness.acquisition.contract_tickers)
    assert original.request_count == len(harness.calls)
    assert original.decoded_bytes == sum(
        entry.completion.response_bytes for entry in entries
    )
    assert all(response.closed for response in harness.responses)


def test_partial_400_contract_capture_reuses_the_completed_batch(monkeypatch):
    harness = Harness(monkeypatch, underlyings=50)
    reserve = harness.ledger.reserve

    def interrupted_reservation(*args, **kwargs):
        if (
            len(harness.ledger.requests) == 1
            and args[2].request_id not in harness.ledger.requests
        ):
            raise RuntimeError("interrupted between batches")
        return reserve(*args, **kwargs)

    monkeypatch.setattr(
        scheduler, "reserve_coordinated_provider_request_cursor", interrupted_reservation,
    )
    with pytest.raises(RuntimeError, match="between batches"):
        harness.capture()
    assert len(harness.calls) == 1
    first = next(iter(harness.ledger.requests.values())).completion
    monkeypatch.setattr(scheduler, "reserve_coordinated_provider_request_cursor", reserve)
    harness.clock.value += timedelta(seconds=2)
    capture = harness.capture()
    assert len(harness.calls) == 2
    assert len(capture.selection.contracts) == 400
    assert capture.evidence["batches"][0]["selected_at_utc"] == first.requested_to.isoformat()


@pytest.mark.parametrize("status", [302, 429, 500, 503])
def test_http_retries_are_exactly_three_accounted_attempts(monkeypatch, status):
    harness = Harness(monkeypatch, status=status)
    for _ in range(3):
        with pytest.raises(RuntimeError, match=f"HTTP {status}"):
            harness.capture()
    with pytest.raises(RuntimeError, match="retry budget exhausted"):
        harness.capture()
    assert len(harness.calls) == len(harness.ledger.requests) == 3
    for entry in harness.ledger.requests.values():
        completion = entry.completion
        assert completion.state is ProviderRequestState.FAILED
        assert completion.http_status == status
        assert completion.response_bytes == len(b'{"error":"unavailable"}')
        assert harness.ledger.payloads[completion.payload_id] == b'{"error":"unavailable"}'


@pytest.mark.parametrize(
    "mode,expected_bytes,outcome",
    [
        ("invalid-json", 1, ProviderRequestOutcome.INVALID),
        ("oversize", 2_000_001, ProviderRequestOutcome.INVALID),
        ("partial", 3, ProviderRequestOutcome.TIMEOUT),
        ("connection-timeout", 0, ProviderRequestOutcome.TIMEOUT),
    ],
)
def test_failed_capture_accounts_for_consumed_body(monkeypatch, mode, expected_bytes, outcome):
    harness = Harness(monkeypatch, mode=mode)
    with pytest.raises((RuntimeError, ValueError, Timeout)):
        harness.capture()
    assert len(harness.calls) == len(harness.ledger.requests) == 1
    completion = next(iter(harness.ledger.requests.values())).completion
    assert completion.state is ProviderRequestState.FAILED
    assert completion.response_bytes == expected_bytes
    assert completion.outcome is outcome
    assert completion.http_status == (None if mode == "connection-timeout" else 200)
    if expected_bytes:
        assert len(harness.ledger.payloads[completion.payload_id]) == expected_bytes


@pytest.mark.parametrize("mode", ["missing", "duplicate", "unexpected", "pagination"])
def test_bad_second_batch_cannot_produce_a_partial_capture(monkeypatch, mode):
    harness = Harness(monkeypatch, underlyings=50, mode=mode)
    with pytest.raises((ValueError, RuntimeError), match="scope|membership|selection"):
        harness.capture()
    assert len(harness.calls) == 2
    assert [entry.completion.state for entry in harness.ledger.requests.values()] == [
        ProviderRequestState.COMPLETE, ProviderRequestState.FAILED,
    ]


def test_deadline_prevents_requests_and_retries_after_expiry(monkeypatch):
    harness = Harness(monkeypatch, underlyings=50, mode="deadline")
    with pytest.raises(Timeout, match="deadline"):
        harness.capture()
    assert len(harness.calls) == len(harness.ledger.requests) == 1
    completion = next(iter(harness.ledger.requests.values())).completion
    assert completion.state is ProviderRequestState.FAILED
    assert completion.response_bytes > 0
    with pytest.raises(Timeout, match="deadline"):
        harness.capture()
    assert len(harness.calls) == len(harness.ledger.requests) == 1


def test_expired_deadline_does_not_even_reserve_a_request(monkeypatch):
    harness = Harness(monkeypatch)
    harness.clock.value = harness.deadline
    with pytest.raises(Timeout, match="deadline"):
        harness.capture()
    assert not harness.calls and not harness.ledger.requests


def test_batch_combination_rejects_duplicate_or_incomplete_membership(monkeypatch):
    harness = Harness(monkeypatch)
    capture = harness.capture()
    with pytest.raises(ValueError, match="membership"):
        combine_exact_contract_captures(
            (capture, capture), contract_tickers=harness.acquisition.contract_tickers,
        )
    with pytest.raises(ValueError, match="membership"):
        combine_exact_contract_captures(
            (capture,), contract_tickers=harness.acquisition.contract_tickers[:-1],
        )


def test_missing_original_replay_clock_is_not_replaced_by_completion_time(monkeypatch):
    harness = Harness(monkeypatch)
    harness.capture()
    entry = next(iter(harness.ledger.requests.values()))
    entry.completion = replace(entry.completion, requested_to=None)
    with pytest.raises(ValueError, match="original selection clock"):
        harness.capture()
    assert len(harness.calls) == 1


def test_ambiguous_prior_reservation_never_reissues_http(monkeypatch):
    harness = Harness(monkeypatch)
    request_id = uuid5(
        NAMESPACE_URL,
        f"continuous-options:{harness.acquisition.authority.authorization_id}:"
        f"{harness.receipt[0]}:0",
    )
    harness.ledger.requests[request_id] = SimpleNamespace(completion=None)
    with pytest.raises(RuntimeError, match="ambiguous"):
        harness.capture()
    assert not harness.calls and len(harness.ledger.requests) == 1


def test_frozen_selection_over_physical_ceiling_is_rejected_before_io(monkeypatch):
    harness = Harness(monkeypatch)
    harness.acquisition.contract_tickers = tuple(sorted(
        _snapshots(126, NOW)
    ))
    with pytest.raises(ValueError, match="frozen membership"):
        harness.capture()
    assert not harness.calls and not harness.ledger.requests


def test_transport_rechecks_deadline_after_session_creation():
    clock = Clock()
    deadline = NOW + timedelta(seconds=1)

    class LateSession:
        def __enter__(self):
            clock.value = deadline
            return self

        def __exit__(self, *_args):
            pass

        def mount(self, *_args):
            pass

        def get(self, *_args, **_kwargs):
            pytest.fail("HTTP started after the interval deadline")

    transport = SingleAttemptPolygonHttpTransport(
        "provider-free", deadline=deadline, maximum_response_bytes=100,
        clock=clock, session_factory=LateSession,
    )
    with pytest.raises(Timeout, match="deadline"):
        transport.get("https://api.massive.com/v3/snapshot", {})
    assert transport.response_bytes == 0


def test_connection_error_still_consumes_one_ledger_attempt(monkeypatch):
    harness = Harness(monkeypatch)

    class FailingTransport:
        http_status = None
        response_body = b""
        response_bytes = 0

        def get(self, *_args):
            raise ConnectionError("provider-free connection refused")

    harness.transport = lambda *_args, **_kwargs: FailingTransport()
    with pytest.raises(ConnectionError):
        harness.capture()
    completion = next(iter(harness.ledger.requests.values())).completion
    assert completion.state is ProviderRequestState.FAILED
    assert completion.outcome is ProviderRequestOutcome.HTTP_ERROR
    assert completion.response_bytes == 0
