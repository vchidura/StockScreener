from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from realtime.advanced_equity_control_plane import (
    build_advanced_equity_control_plane_plan,
    persist_advanced_equity_control_plane_cursor,
    validate_active_target9_equity_source_cursor,
)
from realtime.contracts import EQUITY_VALIDATION_UNIVERSE
from realtime.preflight import DetectorFamily, ProviderTier


NOW = datetime(2026, 10, 16, 16, 0, tzinfo=timezone.utc)
SECURITY = UUID("11111111-1111-1111-1111-111111111111")


def _plan():
    return build_advanced_equity_control_plane_plan(
        session_date=date(2026, 10, 16),
        security_ids={"SPY": SECURITY},
        entitlement_evidence_sha256="a" * 64,
        authorization_reference="advanced-equity-v2-20261016",
        checked_at=NOW,
        expires_at=NOW + timedelta(hours=8),
        schema_sha256="b" * 64,
        security_policy_sha256="c" * 64,
    )


def _target9_plan():
    return build_advanced_equity_control_plane_plan(
        session_date=date(2026, 10, 16),
        security_ids={
            ticker: uuid5(NAMESPACE_URL, f"test-security:{ticker}")
            for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers
        },
        entitlement_evidence_sha256="a" * 64,
        authorization_reference="advanced-equity-target9-20261016",
        checked_at=NOW,
        expires_at=NOW + timedelta(hours=8),
        schema_sha256="b" * 64,
        security_policy_sha256="c" * 64,
        configuration_sha256="d" * 64,
    )


def test_advanced_control_plane_is_exact_spy_am_and_options_disabled():
    plan = _plan()

    assert plan.universe.equity_tickers == ("SPY",)
    assert plan.universe.option_underlyings == ()
    assert tuple(
        member.channel.value for member in plan.manifest.members
    ) == ("AM",)
    assert plan.binding.provider_tier is ProviderTier.MASSIVE_ADVANCED
    assert plan.binding.options_tier is None
    assert DetectorFamily.STOCK_INTRADAY not in (
        plan.binding.disabled_detectors
    )
    assert DetectorFamily.O1 in plan.binding.disabled_detectors


def test_advanced_control_plane_persists_without_portal_activation():
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        (7,),
        (
            str(_plan().profile_id),
            "MASSIVE_ADVANCED",
            "MASSIVE_ADVANCED",
            None,
            "ADVANCED_READY",
            _plan().manifest.sha256,
            _plan().valuation_sha256,
            sorted(
                value.value for value in _plan().binding.capabilities
            ),
            sorted(
                value.value
                for value in _plan().binding.disabled_detectors
            ),
            _plan().binding.effective_at,
            _plan().binding.sha256,
        ),
    )

    persist_advanced_equity_control_plane_cursor(cursor, _plan())

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert any(
        value.startswith("INSERT INTO rt.capability_profiles")
        for value in statements
    )
    assert any(
        value.startswith("INSERT INTO rt.subscription_manifests")
        for value in statements
    )
    assert any(
        value.startswith("INSERT INTO rt.provider_tier_cohorts")
        for value in statements
    )
    assert all("portal_cohort_current" not in value for value in statements)


def test_options_source_requires_active_target9_runtime_profile():
    plan = _target9_plan()
    cursor = MagicMock()
    cursor.fetchone.return_value = {
        "universe_target": 9,
        "provider_tier": "MASSIVE_ADVANCED",
        "feed": "STOCKS",
        "channels": ["AM"],
        "status": "ACTIVE",
        "configuration_sha256": plan.configuration_sha256,
    }

    validate_active_target9_equity_source_cursor(cursor, plan)

    cursor.fetchone.return_value = {
        "universe_target": 9,
        "provider_tier": "MASSIVE_ADVANCED",
        "feed": "STOCKS",
        "channels": ["AM"],
        "status": "CANDIDATE",
        "configuration_sha256": plan.configuration_sha256,
    }
    with pytest.raises(ValueError, match="runtime profile is not active"):
        validate_active_target9_equity_source_cursor(cursor, plan)

    with pytest.raises(ValueError, match="exact target-9"):
        validate_active_target9_equity_source_cursor(cursor, _plan())
