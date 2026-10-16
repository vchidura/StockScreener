from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.equity_recent_repair import (
    AuthorizedNativeRepairTransport, NativeRepairBlocked, NativeRepairPlan, recent_completed_sessions,
    execute_recent_native_repair,
)
from realtime.contracts import canonical_sha256
from realtime.equity_adjusted_daily_repair import (
    AdjustedDailyRepairPlan,
    ENDPOINT_POLICY as ADJUSTED_ENDPOINT_POLICY,
    _request_spec as adjusted_request_spec,
)

NOW = datetime(2026, 10, 20, 12, tzinfo=timezone.utc)


def _plan():
    identity = SimpleNamespace(
        ticker="AAPL", instrument_id=1,
        security=SimpleNamespace(security_id=UUID(int=1), payload_sha256="a" * 64),
    )
    return NativeRepairPlan(
        UUID(int=2), (identity,), (date(2026, 10, 19),), NOW + timedelta(minutes=30),
        3, 6 * 1024 * 1024 + 3, 2 * 1024 * 1024, "operator", "entitlement", "approved cost",
    )


def test_recent_completed_sessions_skip_weekends_and_exchange_holidays():
    assert recent_completed_sessions(date(2026, 10, 19), 3, known_by=NOW) == (
        date(2026, 10, 15), date(2026, 10, 16), date(2026, 10, 19),
    )
    assert recent_completed_sessions(
        date(2026, 9, 8), 2, known_by=datetime(2026, 9, 9, tzinfo=timezone.utc)
    ) == (date(2026, 9, 4), date(2026, 9, 8))


def test_early_close_repair_waits_for_provider_eligibility():
    early = datetime(2026, 11, 27, 18, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="not complete"):
        recent_completed_sessions(
            early.date(), 1, known_by=early + timedelta(minutes=14, seconds=59)
        )
    assert recent_completed_sessions(
        early.date(), 1, known_by=early + timedelta(minutes=15)
    ) == (early.date(),)


def test_repair_identity_scope_uses_the_repaired_session_close():
    from scripts.repair_realtime_v2_recent_sessions import _repair_identity_cutoff

    assert _repair_identity_cutoff(date(2026, 10, 15)) == datetime(
        2026, 10, 15, 20, 0, tzinfo=timezone.utc
    )
    assert _repair_identity_cutoff(date(2026, 11, 27)) == datetime(
        2026, 11, 27, 18, 0, tzinfo=timezone.utc
    )


@pytest.mark.parametrize("changes", (
    {"request_ceiling": 2}, {"byte_ceiling": 1024}, {"response_byte_limit": 1},
    {"authorization_reference": ""}, {"provider_authorization_reference": ""},
    {"expected_cost": ""}, {"authorized_until": datetime(2026, 10, 20)},
    {"sessions": ()},
))
def test_repair_scope_and_budget_fail_closed(changes):
    with pytest.raises(ValueError):
        replace(_plan(), **changes)


def test_repair_identity_pins_provider_scope_and_authorization():
    plan = _plan()
    assert plan.sha256 == _plan().sha256
    assert plan.sha256 != replace(plan, authorization_reference="different approval").sha256


def test_native_request_identity_binds_exact_endpoint_and_parameters():
    import realtime.equity_recent_repair as repair

    plan = _plan()
    request = repair._native_request_spec(
        plan, plan.identities[0], plan.sessions[0], "5m"
    )

    assert request.url.startswith("https://api.massive.com/v2/aggs/ticker/AAPL/range/5/minute/")
    assert request.params == {"adjusted": "false", "sort": "asc", "limit": "50000"}
    assert request.request_sha256 == canonical_sha256({
        "policy": repair.POLICY,
        "plan_sha256": plan.sha256,
        "method": "GET",
        "url": request.url,
        "params": request.params,
    })


def test_adjusted_daily_plan_binds_one_exact_grouped_request():
    native = _plan()
    plan = AdjustedDailyRepairPlan(
        native.cohort_id,
        native.identities,
        native.sessions[0],
        native.authorized_until,
        1,
        8 * 1024 * 1024 + 1,
        8 * 1024 * 1024,
        "operator",
        "entitlement",
        "approved cost",
    )
    url, params, request_sha256 = adjusted_request_spec(plan)

    assert url.endswith("/v2/aggs/grouped/locale/us/market/stocks/2026-10-19")
    assert params == {"adjusted": "true"}
    assert plan.identity()["endpoint_policy"] == ADJUSTED_ENDPOINT_POLICY
    assert request_sha256 == canonical_sha256({
        "policy": "v2-adjusted-daily-session-repair-v1",
        "plan_sha256": plan.sha256,
        "method": "GET",
        "url": url,
        "params": params,
    })


def test_bounded_http_read_closes_response_and_never_retains_oversized_body():
    from options.data.polygon_http import PolygonResponseTooLarge, RequestsPolygonHttpTransport

    response = MagicMock()
    response.raw.read.return_value = b"12345"
    session = MagicMock()
    session.get.return_value = response
    transport = RequestsPolygonHttpTransport(
        "fixture", session_factory=lambda: session, maximum_response_bytes=4
    )
    with pytest.raises(PolygonResponseTooLarge) as error:
        transport.get("https://api.massive.com/fixture", {})
    assert error.value.observed_bytes == 5
    response.raw.read.assert_called_once_with(5, decode_content=True)
    response.close.assert_called_once()


def test_network_admission_rechecks_deadline_after_pacing():
    delegate = MagicMock()
    transport = AuthorizedNativeRepairTransport(
        delegate, NOW, lambda: NOW + timedelta(seconds=1)
    )
    with pytest.raises(NativeRepairBlocked, match="AUTHORIZATION_EXPIRED"):
        transport.get("https://api.massive.com/fixture", {})
    delegate.get.assert_not_called()


def test_existing_http_callers_keep_their_non_streaming_behavior():
    from options.data.polygon_http import RequestsPolygonHttpTransport

    response, session = MagicMock(), MagicMock()
    response.status_code, response.content, response.headers = 200, b"{}", {}
    session.get.return_value = response
    transport = RequestsPolygonHttpTransport("fixture", session_factory=lambda: session)
    assert transport.get("https://api.massive.com/fixture", {}).body == b"{}"
    assert "stream" not in session.get.call_args.kwargs
    response.close.assert_not_called()


def test_native_repair_cli_requires_explicit_scope_without_allowing_live_bypass(tmp_path):
    from scripts.repair_realtime_v2_recent_sessions import parse_args

    args = [
        "--database", "stocks_realtime_v2", "--confirm-database-name", "stocks_realtime_v2",
        "--cohort-id", str(UUID(int=2)), "--through-session", "2026-10-19",
        "--repair-sessions", "1", "--authorized-until", "2026-10-20T12:30:00+00:00",
        "--maximum-provider-requests", "3", "--maximum-response-bytes", str(6 * 1024 * 1024 + 3),
        "--authorization-reference", "operator", "--provider-authorization-reference", "provider",
        "--expected-cost", "approved", "--report-path", str(tmp_path / "report.json"),
    ]
    assert not parse_args(args).execute
    for option, value in (
        ("--repair-sessions", "0"), ("--minimum-spacing-seconds", "0"),
        ("--authorized-until", "2026-10-20T12:30:00"),
    ):
        changed = list(args)
        if option in changed:
            changed[changed.index(option) + 1] = value
        else:
            changed.extend((option, value))
        with pytest.raises(SystemExit):
            parse_args(changed)


def test_repair_verifies_target_before_any_mutation_or_provider_call():
    provider, transport = MagicMock(), MagicMock()
    provider.verify.side_effect = RuntimeError("wrong target")
    with pytest.raises(RuntimeError, match="wrong target"):
        execute_recent_native_repair(provider, transport, _plan(), clock=lambda: NOW)
    provider.transaction.assert_not_called()
    transport.get.assert_not_called()
