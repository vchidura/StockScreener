from __future__ import annotations

from uuid import UUID

from psycopg2.extras import Json

from .preflight import PreflightProfile, TierTransition


def persist_preflight_profile(
    connection,
    profile: PreflightProfile,
    *,
    readiness_epoch: int = 1,
    owner_epoch: int = 1,
) -> None:
    if readiness_epoch <= 0 or owner_epoch <= 0:
        raise ValueError("readiness and owner epochs must be positive")
    scope_type = "PROVIDER_TIER"
    scope_id = profile.provider_tier.value
    capability = "MARKET_DATA"
    reasons = list(profile.readiness.reason_codes)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO rt.capability_profiles ("
                "profile_id, profile_sha256, config_sha256, schema_sha256, "
                "account_alias, capabilities_sha256, universe_sha256, "
                "security_policy_sha256, provider_tier, preflight_mode, "
                "provider_requests, action_authorized, checked_at, expires_at, status"
                ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'PASSIVE', "
                "false, false, %s, %s, %s) ON CONFLICT (profile_id) DO NOTHING",
                (
                    str(profile.profile_id),
                    profile.profile_sha256,
                    profile.configuration_sha256,
                    profile.schema_sha256,
                    profile.account_alias,
                    profile.capabilities_sha256,
                    profile.universe_sha256,
                    profile.security_policy_sha256,
                    profile.provider_tier.value,
                    profile.checked_at,
                    profile.expires_at,
                    profile.verdict.value,
                ),
            )
            cursor.execute(
                "SELECT profile_sha256 FROM rt.capability_profiles "
                "WHERE profile_id = %s",
                (str(profile.profile_id),),
            )
            row = cursor.fetchone()
            if row is None or row[0] != profile.profile_sha256:
                raise ValueError("persisted preflight profile identity conflict")
            for check in profile.checks:
                evidence = {
                    "evidence_reference": check.evidence_reference,
                    "passive": True,
                    "profile_sha256": profile.profile_sha256,
                    "provider_requests": False,
                    "action_authorized": False,
                }
                cursor.execute(
                    "INSERT INTO rt.capability_checks ("
                    "profile_id, check_name, scope_key, verdict, required, "
                    "reason_codes, observed_at, expires_at, evidence"
                    ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (profile_id, check_name, scope_key) DO NOTHING",
                    (
                        str(profile.profile_id),
                        check.check_name,
                        check.scope_key,
                        check.verdict.value,
                        check.required,
                        list(check.reason_codes),
                        check.observed_at,
                        check.expires_at,
                        Json(evidence),
                    ),
                )
            cursor.execute(
                "SELECT status, reasons, epoch, profile_id "
                "FROM rt.readiness_current "
                "WHERE scope_type = %s AND scope_id = %s AND capability = %s "
                "FOR UPDATE",
                (scope_type, scope_id, capability),
            )
            current = cursor.fetchone()
            if current is not None and current[2] > readiness_epoch:
                raise ValueError("stale readiness epoch cannot overwrite current state")
            changed = current is None or (
                current[0],
                tuple(current[1]),
                current[3],
            ) != (
                profile.readiness.status.value,
                tuple(reasons),
                str(profile.profile_id),
            )
            if changed:
                cursor.execute(
                    "INSERT INTO rt.readiness_transitions ("
                    "scope_type, scope_id, capability, epoch, owner_epoch, profile_id, "
                    "previous_status, status, reasons, transitioned_at, lease_until, "
                    "source_watermark) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, "
                    "%s, %s, %s, %s)",
                    (
                        scope_type,
                        scope_id,
                        capability,
                        readiness_epoch,
                        owner_epoch,
                        str(profile.profile_id),
                        current[0] if current is not None else None,
                        profile.readiness.status.value,
                        reasons,
                        profile.checked_at,
                        profile.expires_at,
                        Json({"profile_sha256": profile.profile_sha256}),
                    ),
                )
            cursor.execute(
                "INSERT INTO rt.readiness_current ("
                "scope_type, scope_id, capability, epoch, owner_epoch, profile_id, "
                "status, reasons, checked_at, lease_until, source_watermark"
                ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (scope_type, scope_id, capability) DO UPDATE SET "
                "epoch = EXCLUDED.epoch, owner_epoch = EXCLUDED.owner_epoch, "
                "profile_id = EXCLUDED.profile_id, status = EXCLUDED.status, "
                "reasons = EXCLUDED.reasons, checked_at = EXCLUDED.checked_at, "
                "lease_until = EXCLUDED.lease_until, "
                "source_watermark = EXCLUDED.source_watermark, "
                "updated_at = clock_timestamp() "
                "WHERE rt.readiness_current.epoch <= EXCLUDED.epoch",
                (
                    scope_type,
                    scope_id,
                    capability,
                    readiness_epoch,
                    owner_epoch,
                    str(profile.profile_id),
                    profile.readiness.status.value,
                    reasons,
                    profile.checked_at,
                    profile.expires_at,
                    Json({"profile_sha256": profile.profile_sha256}),
                ),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def persist_tier_transition(
    connection,
    transition: TierTransition,
    *,
    from_profile_id: UUID | None = None,
    to_profile_id: UUID | None = None,
) -> None:
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO rt.provider_tier_transitions ("
                "transition_id, previous_state, target_state, transitioned_at, "
                "operator_approved, operator_approval_reference, dependencies_warm, "
                "reason_code, from_profile_id, to_profile_id, transition_sha256"
                ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (transition_id) DO NOTHING",
                (
                    str(transition.transition_id),
                    transition.previous.value,
                    transition.target.value,
                    transition.transitioned_at,
                    transition.operator_approved,
                    transition.operator_approval_reference,
                    transition.dependencies_warm,
                    transition.reason_code,
                    str(from_profile_id) if from_profile_id is not None else None,
                    str(to_profile_id) if to_profile_id is not None else None,
                    transition.transition_sha256,
                ),
            )
            cursor.execute(
                "SELECT transition_sha256 FROM rt.provider_tier_transitions "
                "WHERE transition_id = %s",
                (str(transition.transition_id),),
            )
            row = cursor.fetchone()
            if row is None or row[0] != transition.transition_sha256:
                raise ValueError("persisted tier transition identity conflict")
        connection.commit()
    except Exception:
        connection.rollback()
        raise