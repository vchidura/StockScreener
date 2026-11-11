"""R18 (Nov 10, live): a V3 Options cohort needs its provider-tier binding; rt.option_packages references it."""
from datetime import datetime, timezone
from uuid import UUID

from realtime.advanced_options_automatic_preparation import (
    _C8_VALUATION_POLICY_SHA256, c8_options_tier_binding,
)
from realtime.preflight import ProviderTier, TierState


def test_c8_options_cohort_binds_the_advanced_tier_for_stocks_and_options():
    effective = datetime(2026, 11, 10, 14, 29, 59, 999999, tzinfo=timezone.utc)
    binding = c8_options_tier_binding(
        cohort_id=UUID("ccaed5c3-1b40-5bc7-8595-d46d59bdb864"),
        profile_id=UUID("1b29f901-7df5-5ec1-b4a7-c2f1343c5007"),
        manifest_sha256="a" * 64,
        effective_at=effective,
    )
    assert binding.provider_tier is ProviderTier.MASSIVE_ADVANCED
    assert binding.stocks_tier is ProviderTier.MASSIVE_ADVANCED and binding.options_tier is ProviderTier.MASSIVE_ADVANCED
    assert binding.ready_state is TierState.ADVANCED_READY
    assert binding.valuation_policy_sha256 == _C8_VALUATION_POLICY_SHA256
    assert binding.effective_at == effective and len(binding.sha256) == 64
