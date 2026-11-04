from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from realtime.contracts import canonical_sha256
from realtime.equity_bar_adapter import MAXIMUM_HISTORY_BINDING_DEPTH
from realtime.equity_history import (
    EquityHistoryIdentityError,
    EquityHistoryScope,
    validate_equity_history_scope_chain_cursor,
)
from realtime.preflight import ProviderTier
from realtime.reference_continuity import (
    reference_continuity_manifest_fields,
    source_manifest_continuity_fields,
    source_manifest_identity,
    source_manifest_identity_cursor,
)
from realtime.screening_projection import RealtimeScreeningInput


CUTOFF = datetime(2026, 10, 16, 20, 0, tzinfo=timezone.utc)
KNOWN_BY = datetime(2026, 10, 18, 1, 11, tzinfo=timezone.utc)


class _Cursor:
    def __init__(self, members, prior, latest):
        self.members = members
        self.rows = [prior, latest, {"count": 0}]
        self.statements = []

    def execute(self, statement, parameters=()):
        self.statements.append((statement, parameters))

    def fetchall(self):
        return self.members

    def fetchone(self):
        return self.rows.pop(0)


class _HistoryCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.statements = []

    def execute(self, statement, parameters=()):
        self.statements.append((statement, parameters))

    def fetchone(self):
        return self.rows.pop(0)


def _fixture():
    security_id = uuid4()
    prior_id = uuid4()
    latest_id = uuid4()
    prior = {
        "revision_id": prior_id,
        "security_revision_id": prior_id,
        "security_id": security_id,
        "ticker": "SPY",
        "payload_sha256": "a" * 64,
        "observed_at": CUTOFF,
        "effective_from": datetime(2026, 10, 1, tzinfo=timezone.utc),
        "effective_to": datetime(2026, 10, 5, tzinfo=timezone.utc),
        "active": False,
    }
    latest = {
        "revision_id": latest_id,
        "security_revision_id": latest_id,
        "security_id": security_id,
        "ticker": "SPY",
        "payload_sha256": "b" * 64,
        "observed_at": KNOWN_BY,
        "effective_from": KNOWN_BY,
        "effective_to": None,
        "active": True,
    }
    evidence = ((
        1,
        "SPY",
        "LATE_OBSERVED_CONTINUITY",
        str(prior_id),
        prior["payload_sha256"],
        prior["observed_at"],
        prior["effective_from"],
        prior["effective_to"],
        str(latest_id),
        latest["payload_sha256"],
        latest["observed_at"],
        latest["effective_from"],
        latest["effective_to"],
    ),)
    return security_id, prior, latest, evidence


def test_reference_continuity_manifest_carries_digest_and_visibility_clock():
    inputs = SimpleNamespace(
        identity_continuity_sha256="c" * 64,
        recorded_by=KNOWN_BY,
    )

    assert reference_continuity_manifest_fields(inputs) == {
        "identity_continuity_sha256": "c" * 64,
        "known_by": KNOWN_BY.isoformat(),
    }


def _screening_history_input(depth):
    scopes = tuple(
        EquityHistoryScope(
            UUID(int=index + 1),
            ProviderTier.MASSIVE_ADVANCED,
            CUTOFF - timedelta(days=index),
            canonical_sha256(("screening-history", index)),
        )
        for index in range(depth)
    )
    first = scopes[0] if scopes else None
    return RealtimeScreeningInput(
        cohort_id=UUID(int=1000),
        provider_tier=ProviderTier.MASSIVE_ADVANCED,
        source_manifest_sha256="a" * 64,
        source_generation=1,
        session=CUTOFF.date(),
        cutoff=CUTOFF,
        members=(),
        bars=(),
        actions=(),
        coverage=(),
        history_source_cohort_id=None if first is None else first.cohort_id,
        history_source_provider_tier=None if first is None else first.provider_tier,
        history_before=None if first is None else first.history_before,
        history_binding_sha256=None if first is None else first.binding_sha256,
        known_by=KNOWN_BY,
        history_scopes=scopes,
    )


@pytest.mark.parametrize("depth", (0, 1, 4, 5, MAXIMUM_HISTORY_BINDING_DEPTH))
def test_screening_history_accepts_the_shared_depth_bound(depth):
    inputs = _screening_history_input(depth)

    assert len(inputs.history_scopes) == depth
    assert replace(inputs).history_scopes == inputs.history_scopes
    assert inputs.recorded_by == KNOWN_BY


def test_screening_history_rejects_depth_over_the_shared_bound():
    with pytest.raises(ValueError, match="history scope chain diverged"):
        _screening_history_input(MAXIMUM_HISTORY_BINDING_DEPTH + 1)


def test_screening_history_still_requires_the_exact_first_binding():
    inputs = _screening_history_input(5)

    with pytest.raises(ValueError, match="history scope chain diverged"):
        replace(inputs, history_binding_sha256="f" * 64)


def _with_chained_history(inputs):
    scopes = (
        EquityHistoryScope(
            UUID(int=90),
            ProviderTier.MASSIVE_DEVELOPER,
            inputs.cutoff - timedelta(days=60),
            "b" * 64,
        ),
        EquityHistoryScope(
            UUID(int=91),
            ProviderTier.MASSIVE_DEVELOPER,
            inputs.cutoff - timedelta(days=365),
            "c" * 64,
        ),
    )
    chain = [scope.payload() for scope in scopes]
    return replace(
        inputs,
        source_manifest_sha256=source_manifest_identity(
            cohort_manifest_sha256="a" * 64,
            history_binding_sha256=canonical_sha256(chain),
        ),
        history_source_cohort_id=scopes[0].cohort_id,
        history_source_provider_tier=scopes[0].provider_tier,
        history_before=scopes[0].history_before,
        history_binding_sha256=scopes[0].binding_sha256,
        history_scopes=scopes,
    ), chain


def test_source_manifest_continuity_fields_carries_bounded_history_chain():
    inputs = SimpleNamespace(
        identity_continuity_sha256="d" * 64,
        recorded_by=KNOWN_BY,
        history_scopes=(
            EquityHistoryScope(
                UUID(int=90),
                ProviderTier.MASSIVE_DEVELOPER,
                CUTOFF,
                "b" * 64,
            ),
            EquityHistoryScope(
                UUID(int=91),
                ProviderTier.MASSIVE_DEVELOPER,
                CUTOFF - timedelta(days=30),
                "c" * 64,
            ),
        ),
    )

    assert source_manifest_continuity_fields(inputs) == {
        "identity_continuity_sha256": "d" * 64,
        "known_by": KNOWN_BY.isoformat(),
        "history_scope_chain": [
            scope.payload() for scope in inputs.history_scopes
        ],
    }


def test_reader_validates_history_chain_without_capability_profile_access():
    scopes = (
        EquityHistoryScope(
            UUID(int=90),
            ProviderTier.MASSIVE_DEVELOPER,
            CUTOFF - timedelta(days=60),
            "b" * 64,
        ),
        EquityHistoryScope(
            UUID(int=91),
            ProviderTier.MASSIVE_DEVELOPER,
            CUTOFF - timedelta(days=365),
            "c" * 64,
        ),
    )
    cursor = _HistoryCursor([
        {
            "source_cohort_id": scopes[0].cohort_id,
            "source_provider_tier": scopes[0].provider_tier.value,
            "history_before": scopes[0].history_before,
            "source_manifest_sha256": "1" * 64,
            "binding_sha256": scopes[0].binding_sha256,
            "binding_created_at": CUTOFF,
            "actual_source_manifest_sha256": "1" * 64,
        },
        {
            "source_cohort_id": scopes[1].cohort_id,
            "source_provider_tier": scopes[1].provider_tier.value,
            "history_before": scopes[1].history_before,
            "source_manifest_sha256": "2" * 64,
            "binding_sha256": scopes[1].binding_sha256,
            "binding_created_at": CUTOFF,
            "actual_source_manifest_sha256": "2" * 64,
        },
        {
            "source_cohort_id": None,
            "binding_created_at": None,
        },
    ])

    assert validate_equity_history_scope_chain_cursor(
        cursor,
        cohort_id=UUID(int=89),
        checked_at=CUTOFF,
        expected_chain=[scope.payload() for scope in scopes],
    ) == scopes
    assert all(
        "capability_profiles" not in statement
        for statement, _parameters in cursor.statements
    )


def test_reader_rejects_history_chain_source_manifest_drift():
    scope = EquityHistoryScope(
        UUID(int=90),
        ProviderTier.MASSIVE_DEVELOPER,
        CUTOFF - timedelta(days=60),
        "b" * 64,
    )
    cursor = _HistoryCursor([{
        "source_cohort_id": scope.cohort_id,
        "source_provider_tier": scope.provider_tier.value,
        "history_before": scope.history_before,
        "source_manifest_sha256": "1" * 64,
        "binding_sha256": scope.binding_sha256,
        "binding_created_at": CUTOFF,
        "actual_source_manifest_sha256": "2" * 64,
    }])

    with pytest.raises(
        EquityHistoryIdentityError,
        match="source identity diverged",
    ):
        validate_equity_history_scope_chain_cursor(
            cursor,
            cohort_id=UUID(int=89),
            checked_at=CUTOFF,
            expected_chain=[
                scope.payload(),
                EquityHistoryScope(
                    UUID(int=91),
                    ProviderTier.MASSIVE_DEVELOPER,
                    CUTOFF - timedelta(days=365),
                    "c" * 64,
                ).payload(),
            ],
        )


def test_aggregate_portal_manifests_carry_chained_history_identity():
    from realtime.dashboard_projection import produce_realtime_dashboard
    from realtime.stock_alert_projection import produce_realtime_stock_alerts
    from realtime.ticker_projection import (
        produce_realtime_pattern_watch_publication,
        produce_realtime_ticker_workspace_summary,
    )
    from tests.realtime.test_dashboard_vertical_slice import dashboard_fixture
    from tests.realtime.test_stock_alert_vertical_slice import timely_fixture
    from tests.realtime.test_ticker_vertical_slice import (
        ticker_fixture,
        ticker_projection,
    )

    ticker_inputs, chain = _with_chained_history(ticker_fixture())
    ticker = ticker_projection()
    snapshots = (
        produce_realtime_ticker_workspace_summary(
            ticker_inputs,
            ticker.snapshots,
            ticker.summary_charts,
        ),
        produce_realtime_pattern_watch_publication(
            ticker_inputs,
            ticker.snapshots,
        ),
        produce_realtime_dashboard(
            _with_chained_history(dashboard_fixture())[0]
        ),
        produce_realtime_stock_alerts(
            _with_chained_history(timely_fixture())[0]
        ),
    )

    assert all(
        snapshot.source_manifest["history_scope_chain"] == chain
        for snapshot in snapshots
    )


def test_source_manifest_identity_recomputes_late_continuity_evidence():
    security_id, prior, latest, evidence = _fixture()
    manifest_sha256 = "d" * 64
    manifest = {
        "identity_continuity_sha256": canonical_sha256(evidence),
        "known_by": KNOWN_BY.isoformat(),
    }
    cursor = _Cursor(
        [{"instrument_id": 1, "security_id": security_id}],
        prior,
        latest,
    )

    identity, rows = source_manifest_identity_cursor(
        cursor,
        manifest,
        manifest_id=uuid4(),
        cohort_manifest_sha256=manifest_sha256,
        history_binding_sha256=None,
        cutoff=CUTOFF,
    )

    assert identity == canonical_sha256({
        "manifest_sha256": manifest_sha256,
        "history_binding_sha256": None,
        "identity_continuity": evidence,
    })
    assert rows[0]["revision_id"] == latest["revision_id"]


def test_source_manifest_identity_checks_the_chain_at_cutoff_without_known_by(
    monkeypatch,
):
    # Ticker readers pass the key with no value when a release recorded none.
    seen = {}
    binding_sha256 = "e" * 64

    def validate(_cursor, *, cohort_id, checked_at, expected_chain):
        seen["checked_at"] = checked_at
        return (SimpleNamespace(binding_sha256=binding_sha256),)

    monkeypatch.setattr(
        "realtime.equity_history.validate_equity_history_scope_chain_cursor",
        validate,
    )

    source_manifest_identity_cursor(
        None,
        {
            "cohort_id": str(uuid4()),
            "known_by": None,
            "identity_continuity_sha256": None,
            "history_scope_chain": [{"binding": binding_sha256}],
        },
        manifest_id=uuid4(),
        cohort_manifest_sha256="d" * 64,
        history_binding_sha256=binding_sha256,
        cutoff=CUTOFF,
    )

    assert seen["checked_at"] == CUTOFF


def test_source_manifest_identity_rejects_a_changed_continuity_digest():
    security_id, prior, latest, _ = _fixture()
    cursor = _Cursor(
        [{"instrument_id": 1, "security_id": security_id}],
        prior,
        latest,
    )

    with pytest.raises(ValueError, match="digest changed"):
        source_manifest_identity_cursor(
            cursor,
            {
                "identity_continuity_sha256": "0" * 64,
                "known_by": KNOWN_BY.isoformat(),
            },
            manifest_id=uuid4(),
            cohort_manifest_sha256="d" * 64,
            history_binding_sha256=None,
            cutoff=CUTOFF,
        )
