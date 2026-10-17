from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from options.data.base import validate_developer_capabilities
from options.domain import DataCapability
from realtime.contracts import (
    CANARY_UNIVERSE,
    DEVELOPER_FORWARD_UNIVERSE,
    EQUITY_VALIDATION_UNIVERSE,
    AssetClass,
    CapabilityCheck,
    CapabilityVerdict,
    MarketChannel,
    MarketFeed,
    RawMessageEnvelope,
    ReadinessStatus,
    SubscriptionManifest,
    SubscriptionMember,
    UniverseManifest,
    assess_readiness,
    canonical_json,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 3, 16, 0, tzinfo=UTC)
HASH_A = "a" * 64
HASH_B = "b" * 64


def test_canonical_json_preserves_exact_finite_decimal_text():
    assert canonical_json({"value": Decimal("1.2300")}) == '{"value":"1.2300"}'
    with pytest.raises(ValueError, match="finite"):
        canonical_json({"value": Decimal("NaN")})


def test_canary_universe_is_canonical_and_deterministic():
    assert CANARY_UNIVERSE.equity_tickers == (
        "AAPL", "AMZN", "IWM", "MSFT", "NVDA", "QQQ", "SPY", "TSLA"
    )
    assert CANARY_UNIVERSE.option_underlyings == CANARY_UNIVERSE.equity_tickers
    repeated = UniverseManifest(
        version=CANARY_UNIVERSE.version,
        equity_tickers=tuple(reversed(CANARY_UNIVERSE.equity_tickers)),
        option_underlyings=tuple(reversed(CANARY_UNIVERSE.option_underlyings)),
    )
    assert repeated == CANARY_UNIVERSE
    assert repeated.sha256 == CANARY_UNIVERSE.sha256


def test_universe_rejects_option_underlying_outside_equities():
    with pytest.raises(ValueError, match="must belong"):
        UniverseManifest("test-v1", ("SPY",), ("QQQ",))


def test_equity_validation_universe_is_explicit_and_does_not_expand_options():
    assert EQUITY_VALIDATION_UNIVERSE.equity_tickers == (
        "AAPL", "AMZN", "IWM", "MSFT", "NFLX", "NVDA", "QQQ", "SPY", "TSLA"
    )
    assert EQUITY_VALIDATION_UNIVERSE.option_underlyings == ()
    assert CANARY_UNIVERSE.option_underlyings == CANARY_UNIVERSE.equity_tickers


def test_developer_forward_universe_is_separate_exact_nine_name_identity():
    assert DEVELOPER_FORWARD_UNIVERSE.equity_tickers == EQUITY_VALIDATION_UNIVERSE.equity_tickers
    assert DEVELOPER_FORWARD_UNIVERSE.option_underlyings == DEVELOPER_FORWARD_UNIVERSE.equity_tickers
    assert len(DEVELOPER_FORWARD_UNIVERSE.option_underlyings) == 9
    assert "NFLX" in DEVELOPER_FORWARD_UNIVERSE.option_underlyings
    assert DEVELOPER_FORWARD_UNIVERSE.sha256 != CANARY_UNIVERSE.sha256
    assert DEVELOPER_FORWARD_UNIVERSE.sha256 != EQUITY_VALIDATION_UNIVERSE.sha256
    assert len(CANARY_UNIVERSE.option_underlyings) == 8
    assert EQUITY_VALIDATION_UNIVERSE.option_underlyings == ()


def _stock_members():
    return tuple(
        SubscriptionMember(AssetClass.EQUITY, ticker, ticker, channel)
        for ticker in CANARY_UNIVERSE.equity_tickers
        for channel in (
            MarketChannel.TRADE,
            MarketChannel.QUOTE,
            MarketChannel.MINUTE_AGGREGATE,
        )
    )


def _option_members(contract_count=1):
    members = []
    catalog_version_id = 1
    for underlying in CANARY_UNIVERSE.option_underlyings:
        for contract_index in range(contract_count):
            symbol = f"O:{underlying}261218C{contract_index + 1:08d}"
            for channel in (MarketChannel.TRADE, MarketChannel.QUOTE):
                members.append(
                    SubscriptionMember(
                        AssetClass.OPTION,
                        symbol,
                        underlying,
                        channel,
                        catalog_version_id=catalog_version_id,
                    )
                )
            catalog_version_id += 1
    return tuple(members)


def _manifest(option_contracts_per_underlying=1, maximum_quotes=1000):
    return SubscriptionManifest(
        version="canary-session-v1",
        session_date=date(2026, 10, 5),
        universe=CANARY_UNIVERSE,
        capability_profile_sha256=HASH_A,
        allocation_policy_sha256=HASH_B,
        members=_stock_members() + _option_members(option_contracts_per_underlying),
        maximum_option_quote_contracts=maximum_quotes,
    )


def test_subscription_manifest_requires_explicit_complete_membership():
    manifest = _manifest()

    assert manifest.sha256 == _manifest().sha256
    assert len(manifest.members) == 40
    assert all("*" not in member.provider_symbol for member in manifest.members)


def test_subscription_manifest_enforces_option_quote_budget():
    with pytest.raises(ValueError, match="connection budget"):
        _manifest(option_contracts_per_underlying=2, maximum_quotes=15)


def test_subscription_manifest_supports_exact_equity_only_membership():
    members = tuple(
        SubscriptionMember(AssetClass.EQUITY, ticker, ticker, channel)
        for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers
        for channel in (
            MarketChannel.TRADE,
            MarketChannel.QUOTE,
            MarketChannel.MINUTE_AGGREGATE,
        )
    )

    manifest = SubscriptionManifest(
        version="equity-validation-session-v1",
        session_date=date(2026, 10, 2),
        universe=EQUITY_VALIDATION_UNIVERSE,
        capability_profile_sha256=HASH_A,
        allocation_policy_sha256=HASH_B,
        members=members,
        maximum_option_quote_contracts=0,
    )

    assert len(manifest.members) == 27
    assert not any(member.asset_class is AssetClass.OPTION for member in manifest.members)


def test_subscription_manifest_supports_exact_am_only_equity_membership():
    universe = UniverseManifest(
        version="advanced-spy-am-v1",
        equity_tickers=("SPY",),
        option_underlyings=(),
    )
    member = SubscriptionMember(
        AssetClass.EQUITY,
        "SPY",
        "SPY",
        MarketChannel.MINUTE_AGGREGATE,
    )

    manifest = SubscriptionManifest(
        version="advanced-spy-am-v1",
        session_date=date(2026, 10, 16),
        universe=universe,
        capability_profile_sha256=HASH_A,
        allocation_policy_sha256=HASH_B,
        members=(member,),
        maximum_option_quote_contracts=0,
    )

    assert manifest.members == (member,)


def test_subscription_manifest_requires_one_channel_for_every_equity():
    universe = UniverseManifest(
        version="advanced-two-name-v1",
        equity_tickers=("SPY", "QQQ"),
        option_underlyings=(),
    )

    with pytest.raises(ValueError, match="at least one explicit channel"):
        SubscriptionManifest(
            version="advanced-two-name-v1",
            session_date=date(2026, 10, 16),
            universe=universe,
            capability_profile_sha256=HASH_A,
            allocation_policy_sha256=HASH_B,
            members=(
                SubscriptionMember(
                    AssetClass.EQUITY,
                    "SPY",
                    "SPY",
                    MarketChannel.MINUTE_AGGREGATE,
                ),
            ),
            maximum_option_quote_contracts=0,
        )


def test_option_universe_cannot_use_zero_quote_budget():
    with pytest.raises(ValueError, match="universe kind"):
        _manifest(maximum_quotes=0)


def test_subscription_manifest_rejects_trade_only_option_contract():
    members = list(_stock_members() + _option_members())
    members.pop()

    with pytest.raises(ValueError, match="trade and quote contract sets must match"):
        SubscriptionManifest(
            version="canary-session-v1",
            session_date=date(2026, 10, 5),
            universe=CANARY_UNIVERSE,
            capability_profile_sha256=HASH_A,
            allocation_policy_sha256=HASH_B,
            members=tuple(members),
        )


def test_raw_message_envelope_retains_immutable_bytes_and_clocks():
    envelope = RawMessageEnvelope(
        stream_session_id=uuid4(),
        message_number=0,
        feed=MarketFeed.STOCKS,
        received_at=NOW,
        received_monotonic_ns=123,
        payload=b'[{"ev":"Q","sym":"SPY"}]',
    )

    assert envelope.byte_length == len(envelope.payload)
    assert envelope.received_at is NOW
    with pytest.raises(ValueError, match="immutable bytes"):
        RawMessageEnvelope(
            stream_session_id=uuid4(),
            message_number=1,
            feed=MarketFeed.OPTIONS,
            received_at=NOW,
            received_monotonic_ns=124,
            payload=bytearray(b"{}"),
        )


def _check(
    name,
    verdict=CapabilityVerdict.PASS,
    *,
    required=True,
    expires_at=None,
    reasons=(),
):
    return CapabilityCheck(
        check_name=name,
        scope_key="canary",
        verdict=verdict,
        observed_at=NOW,
        expires_at=expires_at or NOW + timedelta(minutes=5),
        required=required,
        reason_codes=reasons,
        evidence_reference=f"fixture:{name}",
    )


def test_readiness_is_fail_closed_and_scoped():
    ready = assess_readiness((_check("schema"), _check("manifest")), NOW)
    blocked = assess_readiness(
        (
            _check("schema"),
            _check(
                "entitlement",
                CapabilityVerdict.UNAVAILABLE,
                reasons=("NO_ACTIVE_PROBE",),
            ),
        ),
        NOW,
    )
    degraded = assess_readiness(
        (
            _check("schema"),
            _check(
                "archive",
                CapabilityVerdict.UNAVAILABLE,
                required=False,
                reasons=("ARCHIVE_NOT_CONFIGURED",),
            ),
        ),
        NOW,
    )

    assert ready.status is ReadinessStatus.READY
    assert blocked.status is ReadinessStatus.BLOCKED
    assert blocked.reason_codes == (
        "entitlement:canary:NO_ACTIVE_PROBE",
    )
    assert degraded.status is ReadinessStatus.DEGRADED


def test_expired_pass_cannot_report_ready():
    expired = _check("schema", expires_at=NOW + timedelta(seconds=1))

    assessment = assess_readiness((expired,), NOW + timedelta(seconds=1))

    assert assessment.status is ReadinessStatus.BLOCKED
    assert assessment.reason_codes == ("schema:canary:CHECK_EXPIRED",)


def test_existing_developer_capability_contract_is_unchanged():
    validate_developer_capabilities(
        {
            DataCapability.CHAIN_SNAPSHOT,
            DataCapability.OPTION_TRADES,
            DataCapability.UNDERLYING_PRICE,
        }
    )
    with pytest.raises(RuntimeError, match="unexpectedly includes"):
        validate_developer_capabilities(
            {
                DataCapability.CHAIN_SNAPSHOT,
                DataCapability.OPTION_TRADES,
                DataCapability.OPTION_QUOTES,
                DataCapability.UNDERLYING_PRICE,
            }
        )