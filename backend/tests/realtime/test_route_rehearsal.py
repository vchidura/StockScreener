from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from realtime.preflight import ProviderTier
from realtime.route_rehearsal import (
    PORTAL_SURFACES,
    PortalRouteState,
    PortalRouteTarget,
    build_route_state,
    rehearse_forward_reverse_routes,
    select_portal_repository_bundle,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 15, tzinfo=UTC)


def test_forward_and_reverse_rehearsal_covers_whole_portal_without_persisting():
    retained = PortalRouteTarget("retained-v1", "RETAINED")
    realtime = PortalRouteTarget(
        "realtime-developer-v1",
        "REALTIME",
        cohort_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
    )
    initial = build_route_state(retained, generation=4, effective_at=NOW)
    observed = []

    def probe(state):
        observed.append(state.target.source)
        return {surface: True for surface in PORTAL_SURFACES}

    report = rehearse_forward_reverse_routes(
        initial,
        realtime,
        effective_at=NOW + timedelta(minutes=1),
        probe=probe,
    )

    assert observed == ["REALTIME", "RETAINED"]
    assert report.forward.target == realtime
    assert report.reverse.target == retained
    assert report.reverse.generation == initial.generation + 2
    assert report.passed
    assert not report.persisted
    assert not report.activation_authorized
    assert select_portal_repository_bundle(
        report.forward, retained="old", realtime="new"
    ) == "new"
    assert select_portal_repository_bundle(
        report.reverse, retained="old", realtime="new"
    ) == "old"


def test_failed_forward_probe_still_rehearses_reverse_and_fails_report():
    retained = PortalRouteTarget("retained-v1", "RETAINED")
    realtime = PortalRouteTarget(
        "realtime-developer-v1",
        "REALTIME",
        cohort_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
    )
    initial = build_route_state(retained, generation=1, effective_at=NOW)
    observed = []

    def probe(state):
        observed.append(state.target.source)
        return {
            surface: not (
                state.target.source == "REALTIME" and surface == "OPTIONS_ALERTS"
            )
            for surface in PORTAL_SURFACES
        }

    report = rehearse_forward_reverse_routes(
        initial,
        realtime,
        effective_at=NOW,
        probe=probe,
    )

    assert observed == ["REALTIME", "RETAINED"]
    assert not report.passed
    assert report.reverse.target == retained


def test_mixed_surface_route_state_is_rejected():
    retained = PortalRouteTarget("retained-v1", "RETAINED")
    realtime = PortalRouteTarget(
        "realtime-developer-v1",
        "REALTIME",
        cohort_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
    )
    targets = tuple(
        (surface, realtime if surface == "OPTIONS_ALERTS" else retained)
        for surface in PORTAL_SURFACES
    )

    with pytest.raises(ValueError, match="Mixed|mixed"):
        PortalRouteState(generation=1, effective_at=NOW, targets=targets)