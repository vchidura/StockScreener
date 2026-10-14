from datetime import date, datetime, timedelta, timezone

import pytest

from realtime.contracts import (
    CANARY_UNIVERSE,
    AssetClass,
    CapabilityCheck,
    CapabilityVerdict,
    MarketChannel,
    ReadinessStatus,
    SubscriptionManifest,
    SubscriptionMember,
)
from realtime.preflight import (
    DetectorFamily,
    EXPECTED_SCHEMA_VERSIONS,
    PassiveHostEvidence,
    PassivePreflightConfiguration,
    PassivePreflightEvidence,
    PassiveSchemaEvidence,
    ProviderTier,
    TierState,
    ProductEntitlementProfile,
    detector_availability,
    product_scoped_detector_availability,
    run_passive_preflight,
    transition_tier,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 3, 18, 0, tzinfo=UTC)
SCHEMA_HASH = "a" * 64
SECURITY_HASH = "b" * 64


def _subscription():
    members = []
    catalog_version = 1
    for ticker in CANARY_UNIVERSE.equity_tickers:
        for channel in (
            MarketChannel.TRADE,
            MarketChannel.QUOTE,
            MarketChannel.MINUTE_AGGREGATE,
        ):
            members.append(
                SubscriptionMember(AssetClass.EQUITY, ticker, ticker, channel)
            )
        symbol = f"O:{ticker}261218C00000001"
        for channel in (MarketChannel.TRADE, MarketChannel.QUOTE):
            members.append(
                SubscriptionMember(
                    AssetClass.OPTION,
                    symbol,
                    ticker,
                    channel,
                    catalog_version_id=catalog_version,
                )
            )
        catalog_version += 1
    return SubscriptionManifest(
        version="canary-v1",
        session_date=date(2026, 10, 5),
        universe=CANARY_UNIVERSE,
        capability_profile_sha256="c" * 64,
        allocation_policy_sha256="d" * 64,
        members=tuple(members),
    )


def _configuration(tier=ProviderTier.MASSIVE_ADVANCED, **changes):
    values = {
        "provider_tier": tier,
        "account_alias": "massive-primary",
        "universe": CANARY_UNIVERSE,
        "expected_schema_sha256": SCHEMA_HASH,
        "expected_security_policy_sha256": SECURITY_HASH,
        "advanced_subscription": (
            _subscription() if tier is ProviderTier.MASSIVE_ADVANCED else None
        ),
    }
    values.update(changes)
    return PassivePreflightConfiguration(**values)


def _evidence(**changes):
    values = {
        "schema": PassiveSchemaEvidence(
            database_name="stocks_realtime_v1",
            applied_versions=EXPECTED_SCHEMA_VERSIONS,
            schema_sha256=SCHEMA_HASH,
            security_policy_sha256=SECURITY_HASH,
            role_audit_passed=True,
        ),
        "host": PassiveHostEvidence(
            local_filesystem=True,
            journal_mode="wal",
            journal_synchronous=2,
            planned_journal_bytes=50,
            usable_disk_bytes=100,
            clock_verified=True,
        ),
    }
    values.update(changes)
    return PassivePreflightEvidence(**values)


def test_passive_preflight_cannot_report_ready_without_live_evidence():
    profile = run_passive_preflight(
        _configuration(),
        _evidence(),
        checked_at=NOW,
    )

    assert profile.verdict is CapabilityVerdict.UNAVAILABLE
    assert profile.readiness.status is ReadinessStatus.BLOCKED
    assert profile.provider_requests is False
    assert profile.action_authorized is False
    assert {check.check_name for check in profile.checks} >= {
        "entitlement",
        "live_recency",
        "active_probe",
    }


def test_passive_preflight_schema_versions_follow_latest_migration_catalog():
    assert EXPECTED_SCHEMA_VERSIONS[-1] == (
        "012_realtime_option_manifest_selection"
    )
    profile = run_passive_preflight(
        _configuration(ProviderTier.MASSIVE_DEVELOPER),
        _evidence(),
        checked_at=NOW,
    )

    schema_check = next(
        check for check in profile.checks if check.check_name == "schema_versions"
    )
    assert schema_check.verdict is CapabilityVerdict.PASS


def test_retained_entitlement_pass_still_cannot_fabricate_live_recency():
    entitlement = CapabilityCheck(
        check_name="entitlement",
        scope_key="massive-advanced",
        verdict=CapabilityVerdict.PASS,
        observed_at=NOW,
        expires_at=NOW + timedelta(hours=1),
        evidence_reference="retained:entitlement",
    )

    profile = run_passive_preflight(
        _configuration(),
        _evidence(entitlement=entitlement),
        checked_at=NOW,
    )

    assert profile.verdict is CapabilityVerdict.UNAVAILABLE
    assert profile.readiness.status is ReadinessStatus.BLOCKED
    assert next(
        check for check in profile.checks if check.check_name == "live_recency"
    ).verdict is CapabilityVerdict.UNAVAILABLE


@pytest.mark.parametrize(
    ("evidence", "reason"),
    (
        (
            _evidence(
                schema=PassiveSchemaEvidence(
                    "stocks_db",
                    ("000_realtime_baseline", "001_realtime_security_policy"),
                    
                    SCHEMA_HASH,
                    SECURITY_HASH,
                    True,
                )
            ),
            "WRONG_DATABASE_TARGET",
        ),
        (
            _evidence(
                host=PassiveHostEvidence(True, "delete", 2, 50, 100, True)
            ),
            "JOURNAL_DURABILITY_NOT_VERIFIED",
        ),
        (
            _evidence(
                host=PassiveHostEvidence(True, "wal", 2, 80, 100, True)
            ),
            "DISK_HEADROOM_BELOW_30_PERCENT",
        ),
    ),
)
def test_passive_preflight_fails_closed_for_local_contract_violations(evidence, reason):
    profile = run_passive_preflight(
        _configuration(),
        evidence,
        checked_at=NOW,
    )

    assert profile.verdict is CapabilityVerdict.FAIL
    assert any(reason in check.reason_codes for check in profile.checks)


def test_passive_preflight_rejects_enabled_publication_or_broker():
    profile = run_passive_preflight(
        _configuration(publication_enabled=True),
        _evidence(),
        checked_at=NOW,
    )

    action = next(check for check in profile.checks if check.check_name == "action_mode")
    assert action.verdict is CapabilityVerdict.FAIL
    assert profile.action_authorized is False


def test_developer_mode_keeps_portable_detectors_and_blocks_realtime_detectors():
    configuration = _configuration(ProviderTier.MASSIVE_DEVELOPER)
    profile = run_passive_preflight(
        configuration,
        _evidence(),
        checked_at=NOW,
    )

    assert profile.provider_tier is ProviderTier.MASSIVE_DEVELOPER
    assert detector_availability(
        DetectorFamily.O1,
        ProviderTier.MASSIVE_DEVELOPER,
    ).verdict is CapabilityVerdict.PASS
    blocked = detector_availability(
        DetectorFamily.AGGRESSIVE_PREMIUM_BURST,
        ProviderTier.MASSIVE_DEVELOPER,
    )
    assert blocked.verdict is CapabilityVerdict.UNAVAILABLE
    assert blocked.reason_codes == ("ADVANCED_REALTIME_QUOTES_REQUIRED",)
    o2 = detector_availability(
        DetectorFamily.O2,
        ProviderTier.MASSIVE_DEVELOPER,
    )
    assert o2.verdict is CapabilityVerdict.UNAVAILABLE
    assert o2.reason_codes == ("DEVELOPER_O2_DISABLED_BY_POLICY",)


def test_options_advanced_without_stocks_advanced_does_not_create_advanced_platform():
    mixed = ProductEntitlementProfile(
        stocks_tier=ProviderTier.MASSIVE_DEVELOPER,
        options_tier=ProviderTier.MASSIVE_ADVANCED,
    )

    o1 = product_scoped_detector_availability(DetectorFamily.O1, mixed)
    assert o1.verdict is CapabilityVerdict.PASS
    assert o1.realtime_qualified is False
    assert "PORTABLE_DELAYED_STOCK_CONTEXT" in o1.reason_codes

    o2 = product_scoped_detector_availability(DetectorFamily.O2, mixed)
    assert o2.verdict is CapabilityVerdict.UNAVAILABLE
    assert o2.reason_codes == ("EQUITY_ADVANCED_SPOT_REQUIRED",)

    s1 = product_scoped_detector_availability(DetectorFamily.S1, mixed)
    assert s1.verdict is CapabilityVerdict.PASS
    assert s1.realtime_qualified is False

    burst = product_scoped_detector_availability(
        DetectorFamily.AGGRESSIVE_PREMIUM_BURST,
        mixed,
    )
    assert burst.verdict is CapabilityVerdict.PASS
    assert burst.realtime_qualified is True
    assert burst.reason_codes == ("OPTIONS_ADVANCED_ONLY_SHADOW",)

    persistent = product_scoped_detector_availability(
        DetectorFamily.PERSISTENT_REALTIME_FLOW,
        mixed,
    )
    assert persistent.verdict is CapabilityVerdict.UNAVAILABLE


def test_full_advanced_products_qualify_cross_asset_detectors():
    advanced = ProductEntitlementProfile(
        stocks_tier=ProviderTier.MASSIVE_ADVANCED,
        options_tier=ProviderTier.MASSIVE_ADVANCED,
    )

    for detector in (
        DetectorFamily.O2,
        DetectorFamily.O3,
        DetectorFamily.S1,
        DetectorFamily.S2,
        DetectorFamily.PERSISTENT_REALTIME_FLOW,
    ):
        availability = product_scoped_detector_availability(detector, advanced)
        assert availability.verdict is CapabilityVerdict.PASS
        assert availability.realtime_qualified is True


def test_provider_tier_transitions_require_approval_and_warmup():
    blocked = transition_tier(
        TierState.ADVANCED_READY,
        TierState.ADVANCED_BLOCKED,
        transitioned_at=NOW,
        reason_code="QUOTE_FEED_LOST",
    )
    pending = transition_tier(
        blocked.target,
        TierState.FALLBACK_PENDING_APPROVAL,
        transitioned_at=NOW,
        reason_code="FALLBACK_AVAILABLE",
    )
    with pytest.raises(ValueError, match="operator approval"):
        transition_tier(
            pending.target,
            TierState.DEVELOPER_WARMING,
            transitioned_at=NOW,
            reason_code="OPERATOR_SWITCH",
        )
    warming = transition_tier(
        pending.target,
        TierState.DEVELOPER_WARMING,
        transitioned_at=NOW,
        reason_code="OPERATOR_SWITCH",
        operator_approved=True,
        operator_approval_reference="approval:fallback-20261003",
    )
    with pytest.raises(ValueError, match="warmed dependencies"):
        transition_tier(
            warming.target,
            TierState.DEVELOPER_READY,
            transitioned_at=NOW,
            reason_code="WARMUP_COMPLETE",
        )
    ready = transition_tier(
        warming.target,
        TierState.DEVELOPER_READY,
        transitioned_at=NOW,
        reason_code="WARMUP_COMPLETE",
        dependencies_warm=True,
    )

    assert ready.target is TierState.DEVELOPER_READY
    with pytest.raises(ValueError, match="not allowed"):
        transition_tier(
            TierState.DEVELOPER_READY,
            TierState.ADVANCED_READY,
            transitioned_at=NOW,
            reason_code="SHORTCUT",
            operator_approved=True,
            operator_approval_reference="approval:return-20261003",
            dependencies_warm=True,
        )