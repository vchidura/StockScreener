from contextlib import contextmanager
from dataclasses import replace
from datetime import date, datetime, timezone
from types import SimpleNamespace
from uuid import UUID

from fastapi.testclient import TestClient
from psycopg2 import OperationalError

from realtime import advanced_options_p8_production as production
from realtime.portal_api import (
    RealtimePortalRepositories,
    create_realtime_portal_app,
)
from realtime.portal_compatibility_repository import _active_cohort
from realtime.portal_identity import (
    AWAITING_FIRST_RELEASE,
    CURRENT,
    PRIOR_SESSION,
    PortalIdentity,
    PortalIdentityRepository,
    PortalPublicationIdentity,
    load_portal_identity_cursor,
)
from realtime.ticker_repository import TickerPublicationUnavailable


UTC = timezone.utc
NOW = datetime(2026, 11, 3, 15, 0, tzinfo=UTC)
TODAY_COHORT = UUID("11111111-1111-5111-8111-111111111111")
PRIOR_COHORT = UUID("22222222-2222-5222-8222-222222222222")
OPTIONS_COHORT = UUID("33333333-3333-5333-8333-333333333333")
RELEASE = UUID("44444444-4444-5444-8444-444444444444")
PRIOR_OPTIONS_COHORT = UUID("66666666-6666-5666-8666-666666666666")
TODAY = date(2026, 11, 3)
PRIOR = date(2026, 11, 2)


class _Cursor:
    def __init__(self, equity=(), pin=None):
        pins = [] if pin is None else (
            list(pin) if isinstance(pin, (list, tuple)) else [pin]
        )
        self.results = iter((list(equity), pins))
        self.current = None
        self.statements = []

    def execute(self, statement, _parameters=None):
        self.statements.append(statement)
        self.current = next(self.results)

    def fetchall(self):
        return self.current

    def fetchone(self):
        return self.current


def _equity_row(cohort, session, *, release=None, cutoff=None):
    return {
        "cohort_id": str(cohort),
        "session_date": session,
        "release_id": None if release is None else str(release),
        "cutoff": cutoff,
        "release_session_date": None if release is None else session,
    }


def test_current_session_release_is_served():
    identity = load_portal_identity_cursor(
        _Cursor((
            _equity_row(TODAY_COHORT, TODAY, release=RELEASE, cutoff=NOW),
            _equity_row(PRIOR_COHORT, PRIOR, release=RELEASE, cutoff=NOW),
        )),
        available_by=NOW,
    )

    assert identity.equity.cohort_id == TODAY_COHORT
    assert identity.equity.status == CURRENT
    assert identity.session_cohorts == {
        TODAY.isoformat(): TODAY_COHORT,
        PRIOR.isoformat(): PRIOR_COHORT,
    }


def test_new_session_without_a_release_keeps_serving_the_prior_session():
    identity = load_portal_identity_cursor(
        _Cursor((
            _equity_row(TODAY_COHORT, TODAY),
            _equity_row(PRIOR_COHORT, PRIOR, release=RELEASE, cutoff=NOW),
        )),
        available_by=NOW,
    )

    assert identity.equity.cohort_id == PRIOR_COHORT
    assert identity.equity.status == PRIOR_SESSION
    assert identity.equity.session_date == PRIOR
    assert identity.equity.latest_session_date == TODAY


def test_first_session_without_any_release_is_an_explicit_state():
    identity = load_portal_identity_cursor(
        _Cursor((_equity_row(TODAY_COHORT, TODAY),)),
        available_by=NOW,
    )

    assert identity.equity.cohort_id == TODAY_COHORT
    assert identity.equity.status == AWAITING_FIRST_RELEASE
    assert identity.equity.release_id is None


def test_no_snapshot_and_no_pin_resolves_nothing():
    identity = load_portal_identity_cursor(_Cursor(), available_by=NOW)

    assert identity.equity is None
    assert identity.options is None
    assert identity.session_cohorts == {}


def test_only_an_enabled_pin_identifies_options():
    cursor = _Cursor(pin={
        "cohort_id": str(OPTIONS_COHORT),
        "release_id": str(RELEASE),
        "cutoff": NOW,
        "session_date": TODAY,
    })

    identity = load_portal_identity_cursor(cursor, available_by=NOW)

    assert identity.options.cohort_id == OPTIONS_COHORT
    assert identity.options_session_cohorts == {
        TODAY.isoformat(): OPTIONS_COHORT,
    }
    assert "pin.enabled" in cursor.statements[1]


def _identity(*, options=True):
    equity = PortalPublicationIdentity(
        TODAY_COHORT,
        TODAY,
        CURRENT,
        TODAY,
        RELEASE,
        NOW,
    )
    return PortalIdentity(
        equity,
        (
            PortalPublicationIdentity(
                OPTIONS_COHORT,
                TODAY,
                CURRENT,
                TODAY,
                RELEASE,
                NOW,
            )
            if options
            else None
        ),
        {TODAY.isoformat(): TODAY_COHORT, PRIOR.isoformat(): PRIOR_COHORT},
        {
            TODAY.isoformat(): OPTIONS_COHORT,
            PRIOR.isoformat(): PRIOR_OPTIONS_COHORT,
        },
    )


class _Tickers:
    def __init__(self):
        self.cohorts = []

    def list_tickers(self, *, available_by, cohort_id):
        self.cohorts.append(cohort_id)
        return ("SPY",)

    def workspace_publication(self, *, cohort_id, session_date=None, **_):
        self.cohorts.append(cohort_id)
        return {"cohort_id": str(cohort_id), "session": session_date}

    def trading_sessions(self, *, interval, limit, available_by, cohort_id):
        self.cohorts.append(cohort_id)
        if cohort_id == TODAY_COHORT:
            return {"interval": interval, "sessions": [TODAY.isoformat()]}
        if cohort_id == PRIOR_COHORT:
            return {"interval": interval, "sessions": [PRIOR.isoformat()]}
        raise TickerPublicationUnavailable("no stored sessions")

    def workspace_closing_sessions(self, *, available_by, cohort_id):
        self.cohorts.append(cohort_id)
        if cohort_id == TODAY_COHORT:
            return {
                "cohort_id": str(cohort_id),
                "interval": "1d",
                "sessions": [TODAY.isoformat()],
                "snapshots": {TODAY.isoformat(): "today"},
            }
        if cohort_id == PRIOR_COHORT:
            return {
                "cohort_id": str(cohort_id),
                "interval": "1d",
                "sessions": [PRIOR.isoformat()],
                "snapshots": {PRIOR.isoformat(): "prior"},
            }
        raise TickerPublicationUnavailable("no stored publication")


class _Identity:
    def __init__(self, identity):
        self.identity = identity
        self.calls = 0

    def current(self, *, available_by):
        self.calls += 1
        if isinstance(self.identity, Exception):
            raise self.identity
        return self.identity


def _client(identity, *, options_reads_enabled=False):
    tickers = _Tickers()
    resolver = _Identity(identity)
    app = create_realtime_portal_app(
        RealtimePortalRepositories(
            ticker_screener=None,
            stock_alerts=None,
            option_alerts=None,
            ticker_workspace=tickers,
            option_flow=SimpleNamespace(
                flow=lambda **values: {
                    "cohort_id": str(values["cohort_id"]),
                },
            ),
            portal_identity=resolver,
        ),
        clock=lambda: NOW,
        advanced_options_v2_enabled=options_reads_enabled,
    )
    return TestClient(app), tickers, resolver


def test_reader_naming_no_cohort_gets_the_current_publication():
    client, tickers, _resolver = _client(_identity())

    response = client.get("/api/tickers")

    assert response.status_code == 200
    assert tickers.cohorts == [TODAY_COHORT]
    assert response.headers["X-Realtime-Cohort-Id"] == str(TODAY_COHORT)


def test_explicit_cohort_is_honored_exactly_and_never_resolved():
    client, tickers, resolver = _client(_identity())

    response = client.get("/api/tickers", params={"cohort_id": str(PRIOR_COHORT)})

    assert response.status_code == 200
    assert tickers.cohorts == [PRIOR_COHORT]
    assert resolver.calls == 0
    assert "X-Realtime-Cohort-Id" not in response.headers


def test_requested_session_is_served_from_the_cohort_holding_it():
    client, tickers, _resolver = _client(_identity())

    response = client.get(
        "/api/tickers/workspace-publication",
        params={"session_date": PRIOR.isoformat(), "read_mode": "session_close"},
    )

    assert response.status_code == 200
    assert tickers.cohorts == [PRIOR_COHORT]


def test_closing_sessions_span_every_session_cohort():
    client, tickers, _resolver = _client(_identity())

    result = client.get("/api/tickers/sessions").json()

    assert result["cohort_id"] == str(TODAY_COHORT)
    assert result["sessions"] == [PRIOR.isoformat(), TODAY.isoformat()]
    assert result["snapshots"] == {
        PRIOR.isoformat(): "prior",
        TODAY.isoformat(): "today",
    }
    assert tickers.cohorts == [TODAY_COHORT, PRIOR_COHORT]


def test_explicit_closing_sessions_list_only_that_cohort():
    client, _tickers, _resolver = _client(_identity())

    result = client.get(
        "/api/tickers/sessions",
        params={"cohort_id": str(PRIOR_COHORT)},
    ).json()

    assert result["sessions"] == [PRIOR.isoformat()]


def test_the_portal_naming_the_current_cohort_still_gets_every_session():
    # The portal always names the cohort it was given, so its pickers saw one date.
    client, tickers, _resolver = _client(_identity())

    result = client.get(
        "/api/tickers/sessions",
        params={"cohort_id": str(TODAY_COHORT)},
    ).json()

    assert result["cohort_id"] == str(TODAY_COHORT)
    assert result["sessions"] == [PRIOR.isoformat(), TODAY.isoformat()]
    assert tickers.cohorts == [TODAY_COHORT, PRIOR_COHORT]


def test_a_current_cohort_with_nothing_yet_still_answers_for_itself():
    identity = _identity()
    empty = UUID("66666666-6666-6666-8666-666666666666")
    identity = PortalIdentity(
        replace(identity.equity, cohort_id=empty),
        identity.options,
        {PRIOR.isoformat(): PRIOR_COHORT},
    )
    client, _tickers, _resolver = _client(identity)

    result = client.get(
        "/api/tickers/sessions",
        params={"cohort_id": str(empty)},
    ).json()

    assert result["cohort_id"] == str(empty)
    assert result["sessions"] == [PRIOR.isoformat()]


def test_trading_sessions_span_every_session_cohort():
    client, tickers, _resolver = _client(_identity())

    result = client.get(
        "/api/trading-sessions",
        params={"interval": "1d", "cohort_id": str(TODAY_COHORT)},
    ).json()

    assert result["sessions"] == [PRIOR.isoformat(), TODAY.isoformat()]
    assert tickers.cohorts == [TODAY_COHORT, PRIOR_COHORT]


def test_explicit_older_cohort_trading_sessions_list_only_that_cohort():
    client, _tickers, _resolver = _client(_identity())

    result = client.get(
        "/api/trading-sessions",
        params={"interval": "1d", "cohort_id": str(PRIOR_COHORT)},
    ).json()

    assert result["sessions"] == [PRIOR.isoformat()]


def test_a_failed_identity_read_lists_only_the_named_cohort():
    client, _tickers, _resolver = _client(OperationalError("down"))

    result = client.get(
        "/api/trading-sessions",
        params={"interval": "1d", "cohort_id": str(TODAY_COHORT)},
    ).json()

    assert result["sessions"] == [TODAY.isoformat()]


def test_one_unavailable_session_cohort_does_not_hide_the_others():
    identity = _identity()
    missing = UUID("55555555-5555-5555-8555-555555555555")
    identity = PortalIdentity(
        identity.equity,
        identity.options,
        {**identity.session_cohorts, "2026-10-30": missing},
    )
    client, _tickers, _resolver = _client(identity)

    result = client.get("/api/tickers/sessions").json()

    assert result["sessions"] == [PRIOR.isoformat(), TODAY.isoformat()]


def test_options_never_receive_the_equity_cohort():
    client, _tickers, _resolver = _client(
        _identity(),
        options_reads_enabled=True,
    )

    response = client.get("/api/options/flow")

    assert response.headers["X-Realtime-Cohort-Id"] == str(OPTIONS_COHORT)


def test_historical_options_session_uses_its_own_pinned_cohort():
    client, _tickers, _resolver = _client(
        _identity(),
        options_reads_enabled=True,
    )

    response = client.get(
        "/api/options/flow",
        params={"session_date": PRIOR.isoformat()},
    )

    assert response.status_code == 200
    assert response.json()["cohort_id"] == str(PRIOR_OPTIONS_COHORT)
    assert response.headers["X-Realtime-Cohort-Id"] == str(
        PRIOR_OPTIONS_COHORT
    )


def test_options_stay_unresolved_while_their_reads_are_off():
    client, _tickers, _resolver = _client(_identity())

    response = client.get("/api/options/flow")

    assert "X-Realtime-Cohort-Id" not in response.headers
    current = client.get("/api/realtime/portal/current").json()
    assert current["options"] is None
    assert current["options_reads_enabled"] is False
    assert current["equity"]["cohort_id"] == str(TODAY_COHORT)


def test_options_cohort_is_offered_once_reads_are_on():
    client, _tickers, _resolver = _client(
        _identity(),
        options_reads_enabled=True,
    )

    current = client.get("/api/realtime/portal/current").json()

    assert current["options"]["cohort_id"] == str(OPTIONS_COHORT)
    assert current["options_reads_enabled"] is True


def test_identity_free_routes_do_not_read_identity():
    client, _tickers, resolver = _client(_identity())

    assert client.get("/api/realtime/health").status_code == 200
    assert resolver.calls == 0


def test_identity_failure_leaves_the_route_to_report_it():
    client, tickers, _resolver = _client(OperationalError("down"))

    response = client.get("/api/tickers")

    assert tickers.cohorts == [None]
    assert "X-Realtime-Cohort-Id" not in response.headers


def test_identity_repository_reads_through_its_cursor_factory():
    @contextmanager
    def cursor_factory():
        yield _Cursor((_equity_row(TODAY_COHORT, TODAY),))

    identity = PortalIdentityRepository(cursor_factory).current(
        available_by=NOW,
    )

    assert identity.equity.cohort_id == TODAY_COHORT


def test_options_only_cohort_is_active_through_its_release_pointer():
    class Cursor:
        rows = iter((
            None,
            {
                "cohort_id": str(OPTIONS_COHORT),
                "provider_tier": "MASSIVE_ADVANCED",
                "effective_at": NOW,
            },
        ))

        def execute(self, _statement, _parameters=None):
            return None

        def fetchone(self):
            return next(self.rows)

    active = _active_cohort(Cursor(), NOW, OPTIONS_COHORT)

    assert active["provider_tier"] == "MASSIVE_ADVANCED"


def _pin_publisher(monkeypatch, *, swapped):
    advanced = []

    class Port:
        def __init__(self, _provider, *, package_sha256):
            self.package_sha256 = package_sha256

        def advance(self, **values):
            advanced.append({**values, "package_sha256": self.package_sha256})

    monkeypatch.setattr(
        production,
        "PostgresAdvancedOptionsStoredReadPort",
        Port,
    )
    monkeypatch.setattr(
        production,
        "load_advanced_options_read_pin_cursor",
        lambda _cursor, *, cohort_id: {"cohort_id": str(cohort_id)},
    )

    @contextmanager
    def transaction(*_args, **kwargs):
        assert kwargs["read_only"] is True
        yield SimpleNamespace(cursor=None)

    result = SimpleNamespace(
        release=SimpleNamespace(release_id=RELEASE, pointer_swapped=swapped),
    )
    publish = production.publisher_advancing_read_pin(
        lambda _batch, *, checked_at: result,
        provider=SimpleNamespace(transaction=transaction),
        cohort_id=OPTIONS_COHORT,
        package_sha256="c" * 64,
    )
    return publish("batch", checked_at=NOW), result, advanced


def test_current_options_release_advances_the_read_pin(monkeypatch):
    returned, result, advanced = _pin_publisher(monkeypatch, swapped=True)

    assert returned is result
    assert advanced == [{
        "cohort_id": OPTIONS_COHORT,
        "release_id": RELEASE,
        "previous": {"cohort_id": str(OPTIONS_COHORT)},
        "package_sha256": "c" * 64,
    }]


def test_late_options_release_never_moves_the_read_pin(monkeypatch):
    _returned, _result, advanced = _pin_publisher(monkeypatch, swapped=False)

    assert advanced == []
