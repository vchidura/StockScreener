import inspect
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime import advanced_equity_capability_refresh as refresh_module
from realtime.advanced_equity_capability_refresh import (
    AdvancedCapabilityEvidence,
    AdvancedCapabilityRefreshRequest,
    AdvancedCapabilityRefreshResident,
    AdvancedGatewayCapabilityRefreshSource,
    capability_probe_not_before,
)
from realtime.advanced_gateway import (
    GatewaySubscriptionProbeFailure,
    GatewaySubscriptionProbeResult,
)
from realtime.contracts import MarketFeed
from scripts import run_realtime_v2_advanced_capability_refresh as command


UTC = timezone.utc
NOW = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)
PROFILE = UUID(int=1)


def _request(
    *,
    requires_refresh=True,
    profile_id=PROFILE,
    probe_not_before=NOW,
    security_ids=(("QQQ", UUID(int=3)), ("SPY", UUID(int=4))),
):
    return AdvancedCapabilityRefreshRequest(
        revision_id=UUID(int=2),
        revision_sha256="a" * 64,
        session_date=date(2026, 11, 9),
        security_ids=security_ids,
        source_profile_id=PROFILE,
        current_profile_id=profile_id,
        current_expires_at=NOW - timedelta(minutes=1),
        required_through=NOW + timedelta(hours=9),
        probe_not_before=probe_not_before,
        configuration_sha256="b" * 64,
        schema_sha256="c" * 64,
        security_policy_sha256="d" * 64,
        requires_refresh=requires_refresh,
    )


class _Source:
    def __init__(self, error=None):
        self.error = error
        self.calls = 0

    def refresh(self, request):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return AdvancedCapabilityEvidence(
            tickers=request.tickers,
            feed="STOCKS",
            channel="AM",
            configuration_sha256=request.configuration_sha256,
            checked_at=NOW,
            expires_at=NOW + timedelta(days=1),
            entitlement_evidence_sha256="e" * 64,
            authorization_reference="fake-provider-success",
            accepted_membership=request.tickers,
            rejected_membership=(),
            provider_requests=1,
            received_control_messages=2,
            received_bytes=256,
            subscribe_request_bytes=128,
        )


class _Gateway:
    def __init__(self, result=None, error=None):
        self._result = result
        self._error = error
        self.calls = 0
        self.arguments = []

    async def run_subscription_probe(self, **kwargs):
        self.calls += 1
        self.arguments.append(kwargs)
        if self._error is not None:
            raise self._error
        return self._result


def _probe_result(subscriptions, *, rejected=()):
    requested = tuple(subscriptions)
    rejected = tuple(rejected)
    accepted = tuple(
        value for value in requested if value not in rejected
    )
    return GatewaySubscriptionProbeResult(
        stream_session_id=UUID(int=10),
        feed=MarketFeed.STOCKS,
        requested_subscriptions=requested,
        accepted_subscriptions=accepted,
        rejected_subscriptions=rejected,
        received_control_messages=2,
        received_bytes=512,
        subscribe_request_bytes=256,
        transitions=(),
    )


def test_refresh_cli_is_bounded_and_requires_execution():
    args = command.parse_args([
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--execute",
    ])

    assert args.poll_seconds == 15
    assert args.refresh_before_seconds == 1800
    assert args.retry_maximum_seconds == 300
    assert args.probe_timeout_seconds == 10
    assert args.probe_timeout_seconds < args.lease_seconds
    assert args.probe_byte_ceiling == 64 * 1024
    assert args.status_cadence_seconds == 60

    with pytest.raises(SystemExit):
        command.parse_args([
            "--database",
            "stocks_realtime_v2",
            "--confirm-database-name",
            "stocks_realtime_v2",
            "--probe-timeout-seconds",
            "45",
            "--lease-seconds",
            "45",
            "--execute",
        ])


def test_exact_target_membership_is_accepted_in_one_subscription_session():
    gateway = _Gateway(_probe_result(("AM.QQQ", "AM.SPY")))
    manifests = []
    source = AdvancedGatewayCapabilityRefreshSource(
        api_key=None,
        clock=lambda: NOW,
        gateway_factory=lambda manifest: (
            manifests.append(manifest) or gateway
        ),
    )

    evidence = source.refresh(_request())

    assert evidence.accepted_membership == ("QQQ", "SPY")
    assert evidence.rejected_membership == ()
    assert evidence.provider_requests == 1
    assert gateway.calls == 1
    assert gateway.arguments == [{
        "timeout_seconds": 10.0,
        "maximum_control_bytes": 64 * 1024,
    }]
    assert tuple(
        member.provider_symbol
        for member in manifests[0].members
    ) == ("QQQ", "SPY")


def test_one_missing_member_is_degraded_and_never_passes():
    gateway = _Gateway(
        _probe_result(
            ("AM.QQQ", "AM.SPY"),
            rejected=("AM.SPY",),
        )
    )
    source = AdvancedGatewayCapabilityRefreshSource(
        api_key=None,
        clock=lambda: NOW,
        gateway_factory=lambda _manifest: gateway,
    )
    resident = AdvancedCapabilityRefreshResident(source)

    outcome = resident.step(_request(), checked_at=NOW)

    assert outcome.status == "DEGRADED"
    assert outcome.plan is None
    assert outcome.evidence.accepted_membership == ("QQQ",)
    assert outcome.evidence.rejected_membership == ("SPY",)


def test_historical_rest_evidence_is_not_used():
    source = inspect.getsource(refresh_module)

    assert "/v2/aggs" not in source
    assert "PolygonHttpTransport" not in source
    assert "run_subscription_probe" in source


def test_weekend_early_check_waits_without_provider_call():
    session_date = date(2026, 7, 6)
    probe_not_before = capability_probe_not_before(
        session_date,
        evidence_validity_seconds=3600,
    )
    source = _Source()
    resident = AdvancedCapabilityRefreshResident(source)
    request = replace(
        _request(probe_not_before=probe_not_before),
        session_date=session_date,
        required_through=datetime(
            2026, 7, 6, 20, 0, tzinfo=UTC
        ),
    )

    checked = datetime(2026, 7, 3, 16, 0, tzinfo=UTC)
    outcomes = tuple(
        resident.step(request, checked_at=checked)
        for _message_count in range(100)
    )
    outcome = outcomes[-1]

    assert outcome.status == "WAITING"
    assert outcome.next_retry_at == probe_not_before
    assert outcome.reason_code == "CAPABILITY_PROBE_NOT_BEFORE"
    assert source.calls == 0
    assert {row.next_retry_at for row in outcomes} == {
        probe_not_before
    }
    assert probe_not_before == datetime(
        2026, 7, 6, 19, 0, tzinfo=UTC
    )


def test_eligible_failure_is_visible_and_uses_bounded_backoff():
    source = _Source(RuntimeError("provider temporarily unavailable"))
    resident = AdvancedCapabilityRefreshResident(
        source,
        retry_initial_seconds=15,
        retry_maximum_seconds=60,
    )
    unrelated_role = MagicMock()

    failed = resident.step(_request(), checked_at=NOW)
    waiting = resident.step(
        _request(),
        checked_at=NOW + timedelta(seconds=1),
    )
    unrelated_role()

    assert failed.status == "DEGRADED"
    assert failed.next_retry_at == NOW + timedelta(seconds=15)
    assert "CAPABILITY_PROVIDER_UNAVAILABLE" in failed.reason_code
    assert waiting.reason_code == "CAPABILITY_PROVIDER_RETRY_WAIT"
    assert source.calls == 1
    unrelated_role.assert_called_once_with()


def test_typed_probe_timeout_returns_to_resident_backoff():
    gateway = _Gateway(error=GatewaySubscriptionProbeFailure(
        "ADVANCED_SUBSCRIPTION_PROBE_TIMEOUT",
        received_control_messages=1,
        received_bytes=64,
    ))
    source = AdvancedGatewayCapabilityRefreshSource(
        api_key=None,
        clock=lambda: NOW,
        gateway_factory=lambda _manifest: gateway,
    )
    resident = AdvancedCapabilityRefreshResident(
        source,
        retry_initial_seconds=15,
        retry_maximum_seconds=60,
    )

    outcome = resident.step(_request(), checked_at=NOW)

    assert outcome.status == "DEGRADED"
    assert outcome.next_retry_at == NOW + timedelta(seconds=15)
    assert "ADVANCED_SUBSCRIPTION_PROBE_TIMEOUT" in outcome.reason_code
    assert gateway.calls == 1


def test_success_at_probe_boundary_covers_target_close():
    required_through = NOW + timedelta(hours=9)
    validity = 3600
    checked = required_through - timedelta(seconds=validity)
    gateway = _Gateway(_probe_result(("AM.QQQ", "AM.SPY")))
    source = AdvancedGatewayCapabilityRefreshSource(
        api_key=None,
        clock=lambda: checked,
        evidence_validity_seconds=validity,
        gateway_factory=lambda _manifest: gateway,
    )
    request = replace(
        _request(probe_not_before=checked),
        required_through=required_through,
    )

    outcome = AdvancedCapabilityRefreshResident(source).step(
        request,
        checked_at=checked,
    )

    assert outcome.status == "READY"
    assert outcome.evidence.expires_at == required_through
    assert outcome.plan.expires_at == required_through


def test_membership_change_invalidates_backoff_and_probes_new_scope_once():
    source = _Source(RuntimeError("temporary"))
    resident = AdvancedCapabilityRefreshResident(source)
    failed = resident.step(_request(), checked_at=NOW)
    source.error = None
    changed = replace(
        _request(),
        revision_sha256="f" * 64,
        security_ids=(("IWM", UUID(int=5)),),
    )

    refreshed = resident.step(
        changed,
        checked_at=NOW + timedelta(seconds=1),
    )

    assert failed.status == "DEGRADED"
    assert refreshed.status == "READY"
    assert refreshed.plan.universe.equity_tickers == ("IWM",)
    assert source.calls == 2


def test_four_hundred_members_use_one_connection_not_rest_bursts():
    security_ids = tuple(
        (f"T{index:03d}", UUID(int=index + 100))
        for index in range(400)
    )
    subscriptions = tuple(
        f"AM.{ticker}" for ticker, _security_id in security_ids
    )
    gateway = _Gateway(_probe_result(subscriptions))
    source = AdvancedGatewayCapabilityRefreshSource(
        api_key=None,
        clock=lambda: NOW,
        gateway_factory=lambda _manifest: gateway,
    )

    evidence = source.refresh(
        _request(security_ids=security_ids)
    )

    assert len(evidence.accepted_membership) == 400
    assert evidence.rejected_membership == ()
    assert evidence.provider_requests == 1
    assert gateway.calls == 1
