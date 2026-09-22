"""Retained O2 checkpoint measurements, independent of detector membership."""
from collections import Counter
from datetime import datetime
import math

from options.analytics.smile import SmileInput, fit_smile_groups, qualifying_distortions
from options.surface_detection import SURFACE_POLICY, surface_snapshot_eligible


FOLLOWUP_POLICY = {
    "version": "o2_fixed_checkpoint_followup_v1",
    "offset_minutes": [15, 30, 60],
    "clock_basis": "ORIGINAL_PUBLICATION_PLUS_ELAPSED_MINUTES",
    "source_selection": "LATEST_COMPLETED_MATRIX_AVAILABLE_AT_CHECKPOINT_NO_FALLBACK",
    "cohort_basis": "EXACT_TARGET_BATCH_SPOT_TIME_VALUATION_MODEL",
    "comparison_basis": "ORIGINAL_PEER_CONTRACTS_REQUIRED_FOR_FIXED_PEER_FIT",
    "missing_measurement": None,
    "execution_permission": False,
}


def coherent_surface_peers(target, snapshots, valuation_policy):
    fields = ("batch_id", "underlyer", "contract_type", "expiration_date", "expiration_cutoff",
              "spot", "spot_market_data_time", "valuation_policy_sha256", "model_version")
    return tuple(sorted((row for row in snapshots
        if all(getattr(row, field) == getattr(target, field) for field in fields)
        and surface_snapshot_eligible(row, valuation_policy=valuation_policy)),
        key=lambda row: (row.strike, row.contract_id)))


def _surface_fit(peers, contract_id):
    if (len(peers) > 256 or len({row.contract_id for row in peers}) != len(peers)
            or len({row.strike for row in peers}) != len(peers)):
        return None
    fits = fit_smile_groups(tuple(SmileInput(row.contract_id, row.contract_type, row.expiration_date,
        row.strike, row.spot, row.local_iv) for row in peers), minimum_strikes=SURFACE_POLICY.minimum_strikes)
    if len(fits) != 1:
        return None
    fit = fits[0]
    residual = next((row for row in fit.residuals if row.contract_id == contract_id), None)
    if residual is None or not all(math.isfinite(value) for value in (
        residual.local_iv, residual.fitted_iv, residual.residual, residual.robust_z, fit.mad,
    )):
        return None
    detected = qualifying_distortions(fit, minimum_absolute_robust_z=SURFACE_POLICY.minimum_absolute_robust_z)
    return dict(local_iv_percent=residual.local_iv * 100, fitted_iv_percent=residual.fitted_iv * 100,
        residual_iv_points=residual.residual * 100, robust_z=residual.robust_z,
        residual_mad=fit.mad, input_strikes=fit.distinct_strike_count,
        meets_original_distortion_rule=any(row.contract_id == contract_id for row in detected),
        coefficients=fit.coefficients,
        peer_contract_ids=tuple(row.contract_id for row in peers),
        peer_snapshots=tuple((str(row.snapshot_id), row.normalized_payload_sha256) for row in peers))


def measure_surface_checkpoint(*, baseline_peers, contract_id, snapshots, checkpoint_at: datetime, valuation_policy):
    if (checkpoint_at.utcoffset() is None or not 1 <= len(baseline_peers) <= 256
            or len(snapshots) > 5000 or len({row.contract_id for row in baseline_peers}) != len(baseline_peers)):
        raise ValueError("surface checkpoint requires bounded unique original peers and an aware clock")
    original = next((row for row in baseline_peers if row.contract_id == contract_id), None)
    if original is None:
        raise ValueError("surface checkpoint target is absent from original peers")
    baseline_fit = _surface_fit(baseline_peers, contract_id)
    if baseline_fit is None:
        raise ValueError("original surface fit cannot be reproduced")
    result = dict(checkpoint_at=checkpoint_at, contract_id=contract_id, status="UNAVAILABLE",
        reason=None, cohort_fit=None, fixed_peer_fit=None, baseline_fit=baseline_fit,
        absolute_residual_change_iv_points=None, original_peer_count=len(baseline_peers),
        available_original_peer_count=None, same_residual_sign=None)

    def unavailable(reason):
        return dict(result, reason=reason)

    targets = [row for row in snapshots if row.contract_id == contract_id]
    if not targets:
        return unavailable("CONTRACT_ABSENT_FROM_LATEST_MATRIX")
    if len(targets) != 1:
        return unavailable("AMBIGUOUS_CONTRACT_SNAPSHOTS")
    target = targets[0]
    result.update(snapshot_id=target.snapshot_id, snapshot_sha256=target.normalized_payload_sha256,
        market_time=target.market_data_time, observed_at=target.first_observed_at,
        mark_age_seconds=(checkpoint_at - target.market_data_time).total_seconds(),
        model_mark=target.model_mark, spot=target.spot, batch_id=target.batch_id,
        target_quality_flags=tuple(flag.value for flag in target.quality_flags),
        target_iv_converged=target.iv_converged, target_iv_failure_reason=target.iv_failure_reason,
        target_revised_observed_at=target.revised_observed_at, target_mark_source=target.mark_source.value,
        target_option_spot_skew_seconds=(target.market_data_time - target.spot_market_data_time).total_seconds())
    identity = ("underlyer", "provider", "contract_ticker", "contract_type", "expiration_date",
                "expiration_cutoff", "strike", "shares_per_contract", "exercise_style")
    if any(getattr(target, field) != getattr(original, field) for field in identity):
        return unavailable("CONTRACT_IDENTITY_CHANGED")
    if target.valuation_policy_sha256 != original.valuation_policy_sha256 or target.model_version != original.model_version:
        return unavailable("VALUATION_BASIS_CHANGED")
    if not surface_snapshot_eligible(target, valuation_policy=valuation_policy):
        return unavailable("TARGET_SOURCE_INELIGIBLE")
    if target.market_data_time <= original.market_data_time:
        return unavailable("NO_NEW_OPTION_MARK")
    if checkpoint_at >= target.expiration_cutoff:
        return unavailable("CONTRACT_EXPIRED_AT_CHECKPOINT")
    peers = coherent_surface_peers(target, snapshots, valuation_policy)
    if any(row.market_data_time > checkpoint_at or row.first_observed_at > checkpoint_at for row in peers):
        return unavailable("SOURCE_NOT_AVAILABLE_AT_CHECKPOINT")
    maximum_age = min(valuation_policy.maximum_source_age_seconds, SURFACE_POLICY.maximum_source_age_seconds)
    if any((checkpoint_at - row.market_data_time).total_seconds() >= maximum_age for row in peers):
        return unavailable("COHORT_EXPIRED_AT_CHECKPOINT")
    originals = {row.contract_id: row for row in baseline_peers}
    fixed = tuple(row for row in peers if row.contract_id in originals)
    result.update(eligible_cohort_strikes=len({row.strike for row in peers}),
        available_original_peer_count=len(fixed),
        excluded_point_quality=dict(Counter(flag.value for row in snapshots for flag in row.quality_flags)))
    cohort_fit = _surface_fit(peers, contract_id)
    if cohort_fit is None:
        return unavailable("COHERENT_SURFACE_FIT_UNAVAILABLE")
    result.update(status="MEASURED_COHORT_ONLY", cohort_fit=cohort_fit)
    if len(fixed) != len(baseline_peers):
        return dict(result, reason="ORIGINAL_PEER_COHORT_INCOMPLETE")
    if any(any(getattr(row, field) != getattr(originals[row.contract_id], field) for field in identity) for row in fixed):
        return dict(result, reason="ORIGINAL_PEER_IDENTITY_CHANGED")
    fixed_fit = _surface_fit(fixed, contract_id)
    if fixed_fit is None:
        return dict(result, reason="FIXED_PEER_FIT_UNAVAILABLE")
    return dict(result, status="MEASURED_FIXED_PEERS", reason=None, fixed_peer_fit=fixed_fit,
        absolute_residual_change_iv_points=abs(fixed_fit["residual_iv_points"]) - abs(baseline_fit["residual_iv_points"]),
        same_residual_sign=fixed_fit["residual_iv_points"] * baseline_fit["residual_iv_points"] > 0)