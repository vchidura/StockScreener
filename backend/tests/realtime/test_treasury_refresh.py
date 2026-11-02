from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from options.model_inputs import TREASURY_TENORS
from realtime import treasury_refresh as treasury
from realtime.advanced_equity_lifecycle import AdvancedEquityLifecyclePlanner
from scripts import backfill_realtime_v2_treasury_curve as cli
from scripts import run_advanced_options_v3_lifecycle as resident


UTC = timezone.utc
SESSION = date(2026, 11, 2)
REQUIRED = date(2026, 10, 30)
NOW = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
RECEIVED = NOW + timedelta(seconds=2)


class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return RECEIVED


def _xml(*dates, updated="2026-10-30T22:00:00Z", fields=None):
    tenors = TREASURY_TENORS if fields is None else fields
    entries = "".join(
        "<entry><content><m:properties>"
        f"<d:NEW_DATE>{day.isoformat()}T00:00:00Z</d:NEW_DATE>"
        + "".join(f"<d:{field}>4.20</d:{field}>" for field in tenors)
        + "</m:properties></content></entry>"
        for day in dates
    )
    return (
        '<feed xmlns="http://www.w3.org/2005/Atom" '
        'xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" '
        'xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">'
        f"<updated>{updated}</updated>{entries}</feed>"
    )


class FixtureHttp:
    def __init__(self, *, xml=None, received_at=RECEIVED, fail=False, **kwargs):
        self.xml = xml or _xml(REQUIRED)
        self.received_at = received_at
        self.fail = fail
        self.requests = 0
        self.response_bytes = 0
        self.http_status = None
        self.urls = []

    def get(self, url, params, headers):
        self.requests += 1
        self.urls.append(url)
        if self.fail:
            raise TimeoutError("do not expose provider details")
        self.http_status = 200
        self.response_bytes = len(self.xml.encode())
        return treasury.ReferenceHttpResponse(200, self.xml.encode())


def _facts(http=None, required=REQUIRED):
    return treasury.collect_treasury_facts(
        required_rate_date=required, http=http or FixtureHttp(),
        checked_at=lambda: RECEIVED,
    )


def _rows():
    return [dict(fact.values) for fact in _facts()]


def test_exact_required_date_and_real_receipt_survive_newer_year_rows():
    http = FixtureHttp(xml=_xml(REQUIRED, SESSION, updated="2026-11-02T11:00:00Z"))
    facts = _facts(http)
    assert http.requests == 1
    assert len(facts) == len(TREASURY_TENORS)
    assert {dict(fact.values)["rate_date"] for fact in facts} == {REQUIRED}
    assert {dict(fact.values)["received_at"] for fact in facts} == {RECEIVED}
    assert RECEIVED > NOW


def test_required_previous_session_year_is_requested_across_new_year():
    required = date(2026, 12, 31)
    assert treasury.required_treasury_rate_date(date(2027, 1, 4)) == required
    receipt = datetime(2027, 1, 4, 12, tzinfo=UTC)
    http = FixtureHttp(xml=_xml(required, updated="2026-12-31T22:00:00Z"),
                       received_at=receipt)
    treasury.collect_treasury_facts(
        required_rate_date=required, http=http, checked_at=lambda: receipt,
    )
    assert "field_tdr_date_value=2026" in http.urls[0]


@pytest.mark.parametrize("xml", [
    _xml(date(2026, 10, 29)),
    _xml(SESSION),
    _xml(REQUIRED, fields=("BC_1MONTH",)),
    _xml(REQUIRED, REQUIRED),
    _xml(REQUIRED, updated="2026-11-03T22:00:00Z"),
    _xml(REQUIRED, updated="2026-10-29T22:00:00Z"),
])
def test_collection_rejects_missing_exact_date_incomplete_or_bad_provenance(xml):
    with pytest.raises(ValueError):
        _facts(FixtureHttp(xml=xml))


@pytest.mark.parametrize("field,value", [
    ("source", "synthetic"),
    ("source_key", "2026-10-30:1d"),
    ("rate_date", SESSION),
    ("source_observed_at", NOW + timedelta(days=1)),
    ("received_at", NOW + timedelta(days=1)),
    ("source_observed_at", None),
    ("payload_sha256", "z" * 64),
    ("payload_sha256", "a" * 64),
    ("observation_id", uuid4()),
    ("annual_rate", float("nan")),
])
def test_verification_rejects_invalid_evidence(field, value):
    rows = _rows()
    rows[0][field] = value
    with pytest.raises((ValueError, AttributeError)):
        treasury.validate_treasury_rows(
            rows, required_rate_date=REQUIRED, checked_at=RECEIVED,
        )


def test_verification_rejects_mixed_receipts_even_with_valid_ids():
    rows = _rows()
    other_http = FixtureHttp(received_at=NOW + timedelta(seconds=1))
    rows[0] = dict(_facts(other_http)[0].values)
    with pytest.raises(ValueError, match="different response generations"):
        treasury.validate_treasury_rows(
            rows, required_rate_date=REQUIRED, checked_at=RECEIVED,
        )


class FakeCursor:
    def __init__(self, store):
        self.store = store
        self.result = []

    def execute(self, query, parameters=None):
        self.store.queries.append((query, parameters))
        if "DISTINCT ON (tenor_days)" in query:
            self.result = self.store.rows
        elif "SELECT attempt,reserved_at,state" in query:
            self.result = sorted(
                ((r.attempt, r.reserved_at, self.store.states[r.request_id])
                 for r in self.store.reservations.values()),
                reverse=True,
            )[:7]
        else:
            self.result = []

    def fetchall(self):
        return self.result

    def fetchone(self):
        return self.result[0] if self.result else None


class FakeProvider:
    def __init__(self):
        self.rows = []
        self.reservations = {}
        self.states = {}
        self.queries = []
        self.transactions = []
        self.fences = []
        self.cursor = FakeCursor(self)

    @contextmanager
    def transaction(self, *args, **kwargs):
        self.transactions.append(kwargs)
        if kwargs.get("fence") is not None:
            self.fences.append(kwargs["fence"])
        yield SimpleNamespace(cursor=self.cursor)


def test_read_only_verifier_reports_clock_keys_hashes_complete_decimal_rows():
    store = FakeProvider()
    store.rows = _rows()
    for row in store.rows:
        row["annual_rate"] = Decimal("0.042")
    report = treasury.verify_treasury_curve_cursor(
        store.cursor, session_date=SESSION, checked_at=RECEIVED,
    )
    assert report["status"] == "VERIFIED"
    assert report["required_rate_date"] == "2026-10-30"
    assert len(report["observations"]) == len(TREASURY_TENORS)
    json.dumps(report)
    assert all(query.startswith("SELECT") for query, _ in store.queries)
    assert store.queries[0][1] == (treasury.TREASURY_CURVE_SOURCE, REQUIRED)


def test_verify_cli_needs_no_execution_authority_and_never_fetches(monkeypatch, capsys):
    provider = FakeProvider()
    provider.rows = _rows()
    pool = SimpleNamespace(closeall=lambda: None)
    monkeypatch.setattr(cli, "_provider", lambda: (provider, pool))
    monkeypatch.setattr(cli, "datetime", FixedDatetime)
    monkeypatch.setattr(cli, "BoundedTreasuryHttp", lambda **_: pytest.fail("provider call"))
    result = cli.main([
        "--database", "stocks_realtime_v2",
        "--confirm-database-name", "stocks_realtime_v2",
        "--session-date", SESSION.isoformat(),
        "--required-rate-date", REQUIRED.isoformat(),
        "--verify",
    ])
    assert result == 0
    report = json.loads(capsys.readouterr().out)
    assert report["database_writes"] == report["provider_requests"] == report["migrations"] == 0
    assert provider.transactions == [{"read_only": True, "isolation_level": "REPEATABLE READ"}]


def test_missing_verify_cli_returns_nonzero(monkeypatch, capsys):
    provider = FakeProvider()
    monkeypatch.setattr(cli, "_provider", lambda: (provider, SimpleNamespace(closeall=lambda: None)))
    assert cli.main([
        "--database", "stocks_realtime_v2", "--confirm-database-name", "stocks_realtime_v2",
        "--session-date", SESSION.isoformat(), "--verify",
    ]) == 1
    assert json.loads(capsys.readouterr().out)["status"] == "MISSING_OR_INVALID"


def _execution_args():
    return [
        "--database", "stocks_realtime_v2", "--confirm-database-name", "stocks_realtime_v2",
        "--session-date", SESSION.isoformat(), "--required-rate-date", REQUIRED.isoformat(),
        "--authorized-until", (RECEIVED + timedelta(minutes=30)).isoformat(),
        "--maximum-provider-requests", "1", "--response-byte-limit", "2000000",
        "--authorization-reference", "bounded-offline-test",
        "--report-path", str(cli.Path(__file__).with_name("treasury-unused-test-report.json")),
    ]


def test_execute_cli_uses_one_request_persists_real_receipt_and_verifies(monkeypatch, capsys):
    provider = FakeProvider()
    reports = []
    http = FixtureHttp()
    monkeypatch.setattr(cli, "datetime", FixedDatetime)
    monkeypatch.setattr(cli, "_provider", lambda: (provider, SimpleNamespace(closeall=lambda: None)))
    monkeypatch.setattr(cli, "BoundedTreasuryHttp", lambda **_: http)
    monkeypatch.setattr(cli, "write_report", lambda path, report: reports.append(report))

    def persist(cursor, facts):
        provider.rows = [dict(fact.values) for fact in facts]

    monkeypatch.setattr(cli, "persist_reference_facts_cursor", persist)
    assert cli.main([*_execution_args(), "--execute"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "COMPLETE"
    assert report["verification"]["status"] == "VERIFIED"
    assert report["provider_requests"] == 1
    assert report["database_writes"] == len(TREASURY_TENORS)
    assert {row["received_at"] for row in provider.rows} == {RECEIVED}
    assert isinstance(provider.fences[0], treasury.TreasuryAuthorizationFence)
    assert len(reports) == 1


def test_plan_cli_has_no_database_or_provider_effects(monkeypatch, capsys):
    monkeypatch.setattr(cli, "datetime", FixedDatetime)
    monkeypatch.setattr(cli, "_provider", lambda: pytest.fail("database access"))
    monkeypatch.setattr(cli, "BoundedTreasuryHttp", lambda **_: pytest.fail("provider call"))
    monkeypatch.setattr(cli, "write_report", lambda *args: None)
    assert cli.main(_execution_args()) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "PLANNED"


@pytest.mark.parametrize("flag,value", [
    ("--maximum-provider-requests", "2"),
    ("--response-byte-limit", "2000001"),
    ("--authorized-until", (RECEIVED + timedelta(hours=2)).isoformat()),
    ("--authorized-until", RECEIVED.replace(tzinfo=None).isoformat()),
])
def test_execute_cli_cannot_expand_budgets_or_authorization(monkeypatch, flag, value):
    monkeypatch.setattr(cli, "datetime", FixedDatetime)
    monkeypatch.setattr(cli, "_provider", lambda: pytest.fail("database access"))
    args = _execution_args()
    args[args.index(flag) + 1] = value
    with pytest.raises((SystemExit, ValueError)):
        cli.main([*args, "--execute"])


@pytest.mark.parametrize("extra", [
    ["--required-rate-date", "2026-10-29", "--verify"],
    ["--execute", "--verify"],
    ["--execute"],
])
def test_cli_rejects_wrong_exact_date_or_execution_scope(extra):
    with pytest.raises(SystemExit):
        cli.parse_args([
            "--database", "stocks_realtime_v2", "--confirm-database-name", "stocks_realtime_v2",
            "--session-date", SESSION.isoformat(), *extra,
        ])


class FakeSession:
    def __init__(self, chunks, *, status=200):
        self.chunks = chunks
        self.status_code = status
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def get(self, url, **kwargs):
        self.calls.append(kwargs)
        return self

    def iter_content(self, **kwargs):
        yield from self.chunks


def test_http_has_single_attempt_no_redirects_and_real_post_response_receipt():
    clock_values = iter((NOW, NOW, RECEIVED))
    session = FakeSession([b"body"])
    http = treasury.BoundedTreasuryHttp(
        authorized_until=NOW + timedelta(minutes=1), clock=lambda: next(clock_values),
        session_factory=lambda: session,
    )
    assert http.get("https://home.treasury.gov", {}, {}).body == b"body"
    assert http.received_at == RECEIVED
    assert session.calls[0]["allow_redirects"] is False
    assert session.calls[0]["stream"] is True
    assert http.requests == 1
    with pytest.raises(RuntimeError):
        http.get("https://home.treasury.gov", {}, {})


def test_http_rejects_oversize_response_and_cannot_repeat():
    session = FakeSession([b"x" * (treasury.TREASURY_RESPONSE_BYTE_LIMIT + 1)])
    http = treasury.BoundedTreasuryHttp(
        authorized_until=NOW + timedelta(minutes=1), clock=lambda: NOW,
        session_factory=lambda: session,
    )
    with pytest.raises(ValueError, match="byte/time budget"):
        http.get("https://home.treasury.gov", {}, {})
    with pytest.raises(RuntimeError):
        http.get("https://home.treasury.gov", {}, {})
    assert len(session.calls) == 1


def test_http_expired_authorization_never_opens_session():
    http = treasury.BoundedTreasuryHttp(
        authorized_until=NOW - timedelta(seconds=1), clock=lambda: NOW,
        session_factory=lambda: pytest.fail("opened session"),
    )
    with pytest.raises(RuntimeError):
        http.get("https://home.treasury.gov", {}, {})
    assert http.requests == 0


@pytest.fixture
def owner_harness(monkeypatch):
    provider = FakeProvider()
    clock = [NOW]
    transports = []
    fail = [False]
    claims = [True]

    def http_factory(**kwargs):
        http = FixtureHttp(received_at=clock[0], fail=fail[0])
        transports.append(http)
        return http

    def reserve(cursor, reservation):
        provider.reservations[reservation.request_id] = reservation
        provider.states[reservation.request_id] = "RESERVED"

    def finish(cursor, request_id, completion):
        provider.states[request_id] = completion.state.value

    def persist(cursor, facts):
        provider.rows = [dict(fact.values) for fact in facts]

    monkeypatch.setattr(treasury, "reserve_provider_request_cursor", reserve)
    monkeypatch.setattr(treasury, "finish_provider_request_cursor", finish)
    monkeypatch.setattr(treasury, "persist_reference_facts_cursor", persist)
    monkeypatch.setattr(treasury, "claim_lifecycle_lease_cursor",
                        lambda *_, **kw: SimpleNamespace(
                            resource_key=kw["resource_key"], owner_id=kw["owner_id"], owner_epoch=1,
                        ) if claims[0] else None)
    monkeypatch.setattr(treasury, "release_lifecycle_lease_cursor", lambda *_: True)

    def owner():
        return treasury.TreasuryRefreshOwner(
            provider, owner_id=uuid4(), clock=lambda: clock[0], http_factory=http_factory,
        )

    return SimpleNamespace(
        provider=provider, clock=clock, fail=fail, claims=claims,
        transports=transports, owner=owner,
    )


def test_owner_catches_up_before_activation_and_does_not_repeat_each_loop(owner_harness):
    harness = owner_harness
    owner = harness.owner()
    result = owner.tick(SESSION)
    assert result["status"] == "REFRESHED"
    assert result["provider_requests_this_cycle"] == 1
    assert len(harness.provider.fences) == 1
    for _ in range(10):
        harness.clock[0] += timedelta(seconds=2)
        assert owner.tick(SESSION)["provider_requests_this_cycle"] == 0
    assert len(harness.transports) == 1
    assert harness.owner().tick(SESSION)["status"] == "VERIFIED"
    assert len(harness.transports) == 1
    reservation = next(iter(harness.provider.reservations.values()))
    assert reservation.reserved_response_bytes == 2_000_000
    assert reservation.account_request_ceiling == 6
    assert reservation.account_decoded_byte_ceiling == 12_000_000


def test_owner_retry_wait_and_attempt_ceiling_survive_restarts(owner_harness):
    harness = owner_harness
    harness.fail[0] = True
    for attempt in range(1, 7):
        result = harness.owner().tick(SESSION)
        assert result["status"] == "FAILED"
        assert result["attempts"] == attempt
        assert result["provider_requests_this_cycle"] == 1
        assert harness.owner().tick(SESSION)["status"] == (
            "RETRY_WAIT" if attempt < 6 else "BUDGET_EXHAUSTED"
        )
        harness.clock[0] = treasury.treasury_retry_at(harness.clock[0], attempt)
    assert harness.owner().tick(SESSION)["status"] == "BUDGET_EXHAUSTED"
    assert len(harness.transports) == 6
    assert set(harness.provider.states.values()) == {"FAILED"}


def test_owner_with_reserved_crash_attempt_waits_then_consumes_next_slot(owner_harness):
    harness = owner_harness
    harness.fail[0] = True
    assert harness.owner().tick(SESSION)["status"] == "FAILED"
    request_id = next(iter(harness.provider.states))
    harness.provider.states[request_id] = "RESERVED"
    assert harness.owner().tick(SESSION)["status"] == "RETRY_WAIT"
    harness.clock[0] += timedelta(minutes=15)
    harness.fail[0] = False
    assert harness.owner().tick(SESSION)["status"] == "REFRESHED"
    assert len(harness.provider.reservations) == 2


def test_owner_without_lease_does_not_fetch(owner_harness):
    owner_harness.claims[0] = False
    assert owner_harness.owner().tick(SESSION)["status"] == "OTHER_OWNER"
    assert not owner_harness.transports
    assert not owner_harness.provider.reservations


@pytest.mark.parametrize("at,expected", [
    (datetime(2026, 10, 30, 19, 59, tzinfo=UTC), date(2026, 10, 29)),
    (datetime(2026, 10, 30, 20, 0, tzinfo=UTC), date(2026, 10, 30)),
    (datetime(2026, 11, 2, 14, 31, tzinfo=UTC), date(2026, 10, 30)),
    (datetime(2026, 11, 27, 18, 0, tzinfo=UTC), date(2026, 11, 27)),
])
def test_end_of_xnys_day_schedule_handles_dst_half_day_and_market_restart(at, expected):
    plan = AdvancedEquityLifecyclePlanner(clock=lambda: at).plan(
        drain_seconds=60, finality_delay_seconds=1, completion_budget_seconds=60,
    )
    assert treasury.required_treasury_rate_date(plan.preparation_session_date) == expected


def test_treasury_write_fence_rejects_lost_lease_or_deadline():
    fence = treasury.TreasuryRefreshFence(
        SimpleNamespace(resource_key="treasury-curve:2026-10-30", owner_id=uuid4(), owner_epoch=3),
        NOW,
    )
    cursor = FakeProvider().cursor
    with pytest.raises(treasury.WriteFenceRejected):
        fence.verify(cursor)
    assert "clock_timestamp()<=%s" in cursor.store.queries[0][0]


def test_existing_resident_owns_refresh_before_preparation_and_reports_errors(monkeypatch, capsys):
    calls = []
    provider = FakeProvider()
    lifecycle = SimpleNamespace(
        phase=SimpleNamespace(value="BEFORE_OPEN"),
        processing_session_date=SESSION, preparation_session_date=SESSION,
    )

    class Stop:
        def __init__(self):
            self.done = False

        def is_set(self):
            return self.done

        def wait(self, seconds):
            self.done = True

    class Refresh:
        def __init__(self, *args, **kwargs):
            pass

        def tick(self, session_date):
            calls.append(("treasury", session_date))
            return {"status": "FAILED", "provider_requests_this_cycle": 1}

    def reconcile(*args, **kwargs):
        calls.append(("reconcile", kwargs["target_session_date"]))

    def prepare(*args, **kwargs):
        calls.append(("prepare", SESSION))
        return {"provider_requests": 2}

    monkeypatch.setenv("POLYGON_API_KEY", "offline-fixture")
    monkeypatch.setattr(resident, "dotenv_values", lambda *_: {})
    monkeypatch.setattr(resident, "_provider",
                        lambda: (provider, SimpleNamespace(closeall=lambda: None)))
    monkeypatch.setattr(resident, "PostgresAutomaticOptionsPreparationExecutor",
                        lambda *_, **kw: object())
    monkeypatch.setattr(resident, "AdvancedEquityLifecyclePlanner",
                        lambda: SimpleNamespace(plan=lambda **_: lifecycle))
    monkeypatch.setattr(resident, "TreasuryRefreshOwner", Refresh)
    monkeypatch.setattr(resident, "reconcile_advanced_options_lifecycle_cursor", reconcile)
    monkeypatch.setattr(resident, "run_advanced_options_preparation_cycle", prepare)
    monkeypatch.setattr(resident.threading, "Event", Stop)
    monkeypatch.setattr(resident.signal, "signal", lambda *_: None)
    assert resident.main([
        "--database", "stocks_realtime_v2", "--confirm-database-name", "stocks_realtime_v2",
        "--continuous", "--execute",
    ]) == 0
    assert calls == [
        ("treasury", SESSION), ("reconcile", SESSION),
        ("prepare", SESSION), ("reconcile", SESSION),
    ]
    report = json.loads(capsys.readouterr().out)
    assert report["treasury_refresh"]["status"] == "FAILED"
    assert report["provider_requests_this_cycle"] == 3
