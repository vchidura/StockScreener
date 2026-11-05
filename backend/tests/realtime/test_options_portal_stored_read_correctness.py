from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from options.discovery import EligibleChainQuery
from realtime.advanced_options_projection import (
    AdvancedOptionsFlowPagePublication,
    AdvancedOptionsPageUnavailable,
    OPTIONS_PAGE_CONTRACTS,
    OPTIONS_PAGE_SNAPSHOT_TYPES,
    OPTIONS_SNAPSHOT_TYPES,
    load_advanced_options_page_release_cursor,
)
from realtime.contracts import canonical_sha256
from realtime.portal_compatibility_repository import (
    ReleaseView,
    RealtimeOptionFlowRepository,
    RealtimeOptionScreenerRepository,
    RealtimeOptionsAlertsPageRepository,
)
from realtime.preflight import ProviderTier


UTC = timezone.utc
NOW = datetime(2026, 11, 3, 21, 0, tzinfo=UTC)
SESSION = date(2026, 11, 3)
COHORT = UUID("11111111-1111-5111-8111-111111111111")
OTHER_COHORT = UUID("22222222-2222-5222-8222-222222222222")
RELEASE = UUID("33333333-3333-5333-8333-333333333333")
SCOPE_SHA256 = "a" * 64


class _ScopeCursor:
    def __init__(self):
        self.statements = []
        self._row = None

    def execute(self, statement, _parameters=()):
        self.statements.append(statement)
        if "manifest.option_underlying_count" in statement:
            self._row = {"option_underlying_count": 2}
        else:
            self._row = None

    def fetchone(self):
        return self._row


def _factory(cursor):
    @contextmanager
    def factory():
        yield cursor

    return factory


def _release(cohort_id=COHORT, session_date=SESSION):
    return ReleaseView(
        release_id=RELEASE,
        cohort_id=cohort_id,
        provider_tier=ProviderTier.MASSIVE_ADVANCED.value,
        release_class="CURRENT",
        session_date=session_date,
        cutoff=NOW - timedelta(minutes=10),
        entries={},
    )


def _repository(kind, cursor, *, reads_enabled=True):
    arguments = {
        "transaction_configured": True,
        "require_read_pin": True,
        "reads_enabled": reads_enabled,
    }
    if kind == "FLOW":
        return RealtimeOptionFlowRepository(_factory(cursor), **arguments)
    if kind == "SCREENER":
        return RealtimeOptionScreenerRepository(_factory(cursor), **arguments)
    return RealtimeOptionsAlertsPageRepository(_factory(cursor), **arguments)


def _read(repository, kind, *, session_date=None):
    if kind == "FLOW":
        return repository.flow(
            available_by=NOW,
            cohort_id=COHORT,
            session_date=session_date,
        )
    if kind == "SCREENER":
        return repository.eligible_chain(
            EligibleChainQuery(session_date=session_date),
            available_by=NOW,
            cohort_id=COHORT,
        )
    return repository.read(
        "datasets",
        available_by=NOW,
        cohort_id=COHORT,
    )


@pytest.mark.parametrize("kind", ("FLOW", "SCREENER", "ALERTS"))
def test_explicit_cohort_cannot_bypass_disabled_options_reads(
    monkeypatch,
    kind,
):
    cursor = _ScopeCursor()
    pin = pytest.fail
    monkeypatch.setattr(
        "realtime.portal_compatibility_repository."
        "load_enabled_advanced_options_release_cursor",
        pin,
    )

    result = _read(
        _repository(kind, cursor, reads_enabled=False),
        kind,
    )

    assert result["available"] is False
    assert result["reason"] == "ADVANCED_OPTIONS_V2_DISABLED"
    assert cursor.statements == []


@pytest.mark.parametrize("kind", ("FLOW", "SCREENER", "ALERTS"))
def test_explicit_cohort_requires_an_enabled_exact_read_pin(
    monkeypatch,
    kind,
):
    cursor = _ScopeCursor()
    monkeypatch.setattr(
        "realtime.portal_compatibility_repository."
        "load_enabled_advanced_options_release_cursor",
        lambda *_args, **_kwargs: None,
    )

    result = _read(_repository(kind, cursor), kind)

    assert result["available"] is False
    assert result["reason"] == "ADVANCED_OPTIONS_READ_PIN_UNAVAILABLE"
    assert cursor.statements == []


@pytest.mark.parametrize("kind", ("FLOW", "SCREENER", "ALERTS"))
def test_wrong_cohort_read_pin_fails_closed(monkeypatch, kind):
    cursor = _ScopeCursor()
    monkeypatch.setattr(
        "realtime.portal_compatibility_repository."
        "load_enabled_advanced_options_release_cursor",
        lambda *_args, **_kwargs: (RELEASE, NOW - timedelta(minutes=10)),
    )
    monkeypatch.setattr(
        "realtime.portal_compatibility_repository.load_release",
        lambda *_args, **_kwargs: _release(OTHER_COHORT),
    )

    with pytest.raises(ValueError, match="cohort identity mismatch"):
        _read(_repository(kind, cursor), kind)


def test_all_compatibility_pages_use_the_same_resolved_pinned_release(
    monkeypatch,
):
    loaded = []
    cursor = _ScopeCursor()
    release = _release()
    monkeypatch.setattr(
        "realtime.portal_compatibility_repository."
        "load_enabled_advanced_options_release_cursor",
        lambda *_args, **_kwargs: (release.release_id, release.cutoff),
    )
    monkeypatch.setattr(
        "realtime.portal_compatibility_repository.load_release",
        lambda *_args, **_kwargs: release,
    )

    class Publication:
        provider_tier = ProviderTier.MASSIVE_ADVANCED

        def read(self, **_kwargs):
            return {"available": True, "data": {"underlyers": []}}

        def query(self, _request, **_kwargs):
            return {"available": True, "data": {"rows": []}}

        def datasets(self, **_kwargs):
            return {"available": True, "data": {"datasets": []}}

    def page_loader(_cursor, **arguments):
        loaded.append(arguments)
        return Publication(), {
            "release_id": str(release.release_id),
            "cutoff": release.cutoff,
            "scope_sha256": SCOPE_SHA256,
            "retained_session_date": SESSION.isoformat(),
            "valid_until": NOW.isoformat(),
            "checked_at": NOW.isoformat(),
            "stale": False,
            "staleness_seconds": 0,
            "serving_latest_available": False,
        }

    monkeypatch.setattr(
        "realtime.advanced_options_projection."
        "load_advanced_options_page_release_cursor",
        page_loader,
    )

    results = [
        _read(_repository(kind, cursor), kind)
        for kind in ("FLOW", "SCREENER", "ALERTS")
    ]

    assert [row["artifact_kind"] for row in loaded] == [
        "FLOW",
        "SCREENER",
        "ALERTS",
    ]
    assert {
        (row["release_id"], row["release_cutoff"])
        for row in loaded
    } == {(release.release_id, release.cutoff)}
    assert all(result["release_id"] == str(RELEASE) for result in results)
    assert not any(
        "portal_release_current" in statement
        for statement in cursor.statements
    )


def test_flow_rejects_a_pinned_release_from_another_session(monkeypatch):
    cursor = _ScopeCursor()
    release = _release(session_date=SESSION - timedelta(days=1))
    monkeypatch.setattr(
        "realtime.portal_compatibility_repository."
        "load_enabled_advanced_options_release_cursor",
        lambda *_args, **_kwargs: (release.release_id, release.cutoff),
    )
    monkeypatch.setattr(
        "realtime.portal_compatibility_repository.load_release",
        lambda *_args, **_kwargs: release,
    )

    result = _read(
        _repository("FLOW", cursor),
        "FLOW",
        session_date=SESSION,
    )

    assert result["available"] is False
    assert result["reason"] == "OPTIONS_SESSION_RELEASE_UNAVAILABLE"
    assert cursor.statements == []


def _flow_envelope(selected):
    return {
        "available": True,
        "reason": None,
        "data_tier": "REAL-TIME ADVANCED RESEARCH DATA",
        "generated_at": NOW.isoformat(),
        "as_of": NOW.isoformat(),
        "observed_at": NOW.isoformat(),
        "policy_sha256": "b" * 64,
        "model_version": "advanced-options-v2",
        "data": {
            "underlyers": [{"underlying": selected}],
            "selected": selected,
        },
    }


def test_cross_sectional_activity_does_not_require_spy():
    payload = {
        "cohort_id": str(COHORT),
        "provider_tier": ProviderTier.MASSIVE_ADVANCED.value,
        "scope_sha256": SCOPE_SHA256,
        "responses": {"AAPL": _flow_envelope("AAPL")},
    }
    publication = AdvancedOptionsFlowPagePublication(payload)
    identity = {
        "cohort_id": COHORT,
        "provider_tier": ProviderTier.MASSIVE_ADVANCED,
        "scope_sha256": SCOPE_SHA256,
    }

    activity = publication.read(underlyer=None, **identity)

    assert activity["data"]["underlyers"] == [{"underlying": "AAPL"}]
    with pytest.raises(
        AdvancedOptionsPageUnavailable,
        match="outside the Advanced release",
    ):
        publication.read(underlyer="SPY", **identity)


class _PageCursor:
    def __init__(self, rows):
        self.rows = rows
        self.statement = ""
        self.parameters = ()

    def execute(self, statement, parameters):
        self.statement = statement
        self.parameters = parameters

    def fetchall(self):
        return self.rows


def _retained_page_rows():
    published = NOW - timedelta(minutes=20)
    valid_until = NOW - timedelta(minutes=5)
    payloads = {}
    for kind in OPTIONS_PAGE_SNAPSHOT_TYPES:
        payload = {
            "contract": OPTIONS_PAGE_CONTRACTS[kind],
            "artifact_kind": kind,
            "cohort_id": str(COHORT),
            "provider_tier": ProviderTier.MASSIVE_ADVANCED.value,
            "scope_sha256": SCOPE_SHA256,
            "cycle_sha256": "c" * 64,
            "dataset_id": str(uuid4()),
            "session_date": SESSION.isoformat(),
            "published_at": published.isoformat(),
            "valid_until": valid_until.isoformat(),
        }
        if kind == "FLOW":
            payload["responses"] = {"AAPL": _flow_envelope("AAPL")}
        payloads[kind] = payload
    dataset_id = str(uuid4())
    for payload in payloads.values():
        payload["dataset_id"] = dataset_id
    snapshot_ids = {
        snapshot_type: uuid4()
        for snapshot_type in OPTIONS_SNAPSHOT_TYPES
    }
    manifest = {
        "version": "realtime-portal-release-v1",
        "release_class": "CURRENT",
        "snapshots": [
            {
                "snapshot_type": snapshot_type,
                "snapshot_key": "CURRENT",
                "snapshot_id": str(snapshot_ids[snapshot_type]),
            }
            for snapshot_type in OPTIONS_SNAPSHOT_TYPES
        ],
    }
    manifest_sha256 = canonical_sha256(manifest)
    rows = []
    for kind, snapshot_type in OPTIONS_PAGE_SNAPSHOT_TYPES.items():
        payload = payloads[kind]
        rows.append({
            "release_id": RELEASE,
            "cutoff": published,
            "session_date": SESSION,
            "provider_tier": ProviderTier.MASSIVE_ADVANCED.value,
            "manifest": manifest,
            "manifest_sha256": manifest_sha256,
            "snapshot_type": snapshot_type,
            "snapshot_id": snapshot_ids[snapshot_type],
            "generated_at": published,
            "payload_sha256": canonical_sha256(payload),
            "payload": payload,
        })
    return rows, published


def test_expired_checksum_valid_page_is_served_with_stale_metadata():
    rows, release_cutoff = _retained_page_rows()
    cursor = _PageCursor(rows)

    publication, retained = load_advanced_options_page_release_cursor(
        cursor,
        cohort_id=COHORT,
        available_by=NOW,
        artifact_kind="FLOW",
        release_id=RELEASE,
        release_cutoff=release_cutoff,
    )
    result = publication.read(
        underlyer=None,
        cohort_id=COHORT,
        provider_tier=ProviderTier.MASSIVE_ADVANCED,
        scope_sha256=SCOPE_SHA256,
    )

    assert result["available"] is True
    assert retained["stale"] is True
    assert retained["serving_latest_available"] is True
    assert retained["staleness_seconds"] == 300
    assert "portal_release_current" not in cursor.statement
    assert cursor.parameters[1:3] == (str(RELEASE), release_cutoff)


def test_stale_page_checksum_corruption_remains_fail_closed():
    rows, release_cutoff = _retained_page_rows()
    corrupt = deepcopy(rows)
    corrupt[0]["payload"]["scope_sha256"] = "f" * 64

    with pytest.raises(
        AdvancedOptionsPageUnavailable,
        match="identity drift",
    ):
        load_advanced_options_page_release_cursor(
            _PageCursor(corrupt),
            cohort_id=COHORT,
            available_by=NOW,
            artifact_kind="FLOW",
            release_id=RELEASE,
            release_cutoff=release_cutoff,
        )
