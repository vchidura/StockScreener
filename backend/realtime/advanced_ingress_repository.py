"""Durable repository for Advanced standing authorization and daily ingress."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import NAMESPACE_URL, UUID, uuid5

from .advanced_ingress_plan import (
    ADVANCED_INGRESS_TERMINAL_OUTCOMES,
    AdvancedIngressBudgets,
    AdvancedIngressPlan,
    AdvancedIngressPlanRequest,
    AdvancedIngressUsage,
    AdvancedStreamAuthorization,
    build_advanced_ingress_plan,
)
from .advanced_ingress_runtime_profile import (
    AdvancedIngressRuntimeCatalog,
    AdvancedIngressRuntimeProfile,
)
from .contracts import canonical_json, canonical_sha256


class AdvancedIngressAdmissionError(RuntimeError):
    pass


class AdvancedCohortResolutionError(ValueError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _sha256(value: str, name: str) -> str:
    normalized = value.strip().lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef"
        for character in normalized
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return normalized


@dataclass(frozen=True, slots=True)
class AdvancedRuntimeProfileRegistration:
    runtime_profile_sha256: str
    runtime_profile_version: str
    universe_target: int
    configuration_sha256: str
    runtime_policy_sha256: str
    status: str


def register_advanced_runtime_profile_cursor(
    cursor,
    profile: AdvancedIngressRuntimeProfile,
    *,
    runtime_policy_sha256: str,
    ladder_position: int,
    previous_universe_target: int | None,
) -> AdvancedRuntimeProfileRegistration:
    policy_sha256 = _sha256(
        runtime_policy_sha256,
        "runtime_policy_sha256",
    )
    configuration_sha256 = profile.configuration_sha256(policy_sha256)
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (f"v2-advanced-runtime-profile:{profile.universe_target}",),
    )
    cursor.execute(
        "INSERT INTO rt.advanced_ingress_runtime_profiles ("
        "runtime_profile_sha256,runtime_profile_version,universe_target,"
        "ladder_position,previous_universe_target,"
        "provider_tier,feed,channels,profile_document,"
        "configuration_sha256,runtime_policy_sha256,status"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,'CANDIDATE') "
        "ON CONFLICT (runtime_profile_sha256) DO NOTHING",
        (
            profile.sha256,
            profile.version,
            profile.universe_target,
            ladder_position,
            previous_universe_target,
            profile.provider_tier,
            profile.feed,
            list(profile.channels),
            canonical_json(profile.identity),
            configuration_sha256,
            policy_sha256,
        ),
    )
    cursor.execute(
        "SELECT runtime_profile_sha256,runtime_profile_version,"
        "universe_target,ladder_position,previous_universe_target,"
        "configuration_sha256,runtime_policy_sha256,status,profile_document "
        "FROM rt.advanced_ingress_runtime_profiles "
        "WHERE runtime_profile_sha256=%s FOR UPDATE",
        (profile.sha256,),
    )
    row = cursor.fetchone()
    if (
        row is None
        or str(row[0]) != profile.sha256
        or str(row[1]) != profile.version
        or int(row[2]) != profile.universe_target
        or int(row[3]) != ladder_position
        or row[4] != previous_universe_target
        or str(row[5]) != configuration_sha256
        or str(row[6]) != policy_sha256
        or canonical_sha256(row[8]) != profile.sha256
    ):
        raise ValueError(
            "stored Advanced runtime profile conflicts with source catalog"
        )
    return AdvancedRuntimeProfileRegistration(
        str(row[0]),
        str(row[1]),
        int(row[2]),
        str(row[5]),
        str(row[6]),
        str(row[7]),
    )


def activate_advanced_runtime_profile_cursor(
    cursor,
    runtime_profile_sha256: str,
    *,
    stability_evidence_sha256: str,
    capacity_evidence_sha256: str,
    approval_reference: str,
    activated_at: datetime,
) -> AdvancedRuntimeProfileRegistration:
    profile_sha256 = _sha256(
        runtime_profile_sha256,
        "runtime_profile_sha256",
    )
    stability_sha256 = _sha256(
        stability_evidence_sha256,
        "stability_evidence_sha256",
    )
    capacity_sha256 = _sha256(
        capacity_evidence_sha256,
        "capacity_evidence_sha256",
    )
    approval = approval_reference.strip()
    if not approval:
        raise ValueError("activation approval reference cannot be blank")
    cursor.execute(
        "SELECT runtime_profile_version,universe_target,"
        "configuration_sha256,runtime_policy_sha256,status,"
        "stability_evidence_sha256,capacity_evidence_sha256,"
        "activation_approval_reference "
        "FROM rt.advanced_ingress_runtime_profiles "
        "WHERE runtime_profile_sha256=%s FOR UPDATE",
        (profile_sha256,),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("Advanced runtime profile is unavailable")
    universe_target = int(row[1])
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (f"v2-advanced-runtime-profile:{universe_target}",),
    )
    if row[4] == "ACTIVE":
        if tuple(str(value) for value in row[5:8]) != (
            stability_sha256,
            capacity_sha256,
            approval,
        ):
            raise ValueError(
                "Advanced runtime profile was activated by other evidence"
            )
        return AdvancedRuntimeProfileRegistration(
            profile_sha256,
            str(row[0]),
            universe_target,
            str(row[2]),
            str(row[3]),
            "ACTIVE",
        )
    if row[4] != "CANDIDATE":
        raise ValueError("only a CANDIDATE runtime profile can be activated")
    cursor.execute(
        "UPDATE rt.advanced_ingress_runtime_profiles "
        "SET status='SUPERSEDED',superseded_at=%s "
        "WHERE universe_target=%s AND status='ACTIVE'",
        (activated_at, universe_target),
    )
    cursor.execute(
        "UPDATE rt.advanced_ingress_runtime_profiles SET status='ACTIVE',"
        "stability_evidence_sha256=%s,capacity_evidence_sha256=%s,"
        "activation_approval_reference=%s,activated_at=%s "
        "WHERE runtime_profile_sha256=%s AND status='CANDIDATE' "
        "RETURNING runtime_profile_sha256",
        (
            stability_sha256,
            capacity_sha256,
            approval,
            activated_at,
            profile_sha256,
        ),
    )
    if cursor.fetchone() is None:
        raise ValueError("Advanced runtime profile activation lost its lock")
    return AdvancedRuntimeProfileRegistration(
        profile_sha256,
        str(row[0]),
        universe_target,
        str(row[2]),
        str(row[3]),
        "ACTIVE",
    )


def bind_advanced_ingress_cohort_cursor(
    cursor,
    *,
    session_date,
    universe_target: int,
    runtime_profile_sha256: str,
    cohort_id: UUID,
    authorized_end_at: datetime,
    approval_reference: str,
) -> UUID:
    approval = approval_reference.strip()
    if not approval:
        raise ValueError("cohort binding approval reference cannot be blank")
    cursor.execute(
        "SELECT runtime_profile_version,configuration_sha256,"
        "runtime_policy_sha256,status "
        "FROM rt.advanced_ingress_runtime_profiles "
        "WHERE runtime_profile_sha256=%s FOR UPDATE",
        (runtime_profile_sha256,),
    )
    profile = cursor.fetchone()
    if profile is None or profile[3] != "ACTIVE":
        raise ValueError("cohort binding requires an ACTIVE runtime profile")
    cursor.execute(
        "SELECT cohort.manifest_id,manifest.session_date,"
        "manifest.stock_count,COUNT(member.instrument_id),"
        "COUNT(member.instrument_id) FILTER (WHERE member.channel='AM'),"
        "capability.provider_tier,capability.status,"
        "capability.expires_at,capability.config_sha256,"
        "cohort.configuration_sha256 "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
        "JOIN rt.capability_profiles AS capability USING(profile_id) "
        "LEFT JOIN rt.subscription_members AS member USING(manifest_id) "
        "WHERE cohort.cohort_id=%s "
        "GROUP BY cohort.manifest_id,manifest.session_date,"
        "manifest.stock_count,capability.provider_tier,"
        "capability.status,capability.expires_at,"
        "capability.config_sha256,cohort.configuration_sha256",
        (str(cohort_id),),
    )
    identity = cursor.fetchone()
    if (
        identity is None
        or identity[1] != session_date
        or int(identity[2]) != universe_target
        or int(identity[3]) != universe_target
        or int(identity[4]) != universe_target
        or identity[5] != "MASSIVE_ADVANCED"
        or identity[6] != "PASS"
        or identity[7] < authorized_end_at
        or str(identity[8]) != str(profile[1])
        or str(identity[9]) != str(profile[1])
    ):
        raise ValueError("cohort binding scope is not exact and eligible")
    manifest_id = UUID(str(identity[0]))
    binding_identity = {
        "contract": "advanced-ingress-cohort-binding-v1",
        "session_date": session_date,
        "universe_target": universe_target,
        "runtime_profile_sha256": runtime_profile_sha256,
        "configuration_sha256": str(profile[1]),
        "runtime_policy_sha256": str(profile[2]),
        "cohort_id": cohort_id,
        "manifest_id": manifest_id,
        "approval_reference": approval,
    }
    binding_id = uuid5(
        NAMESPACE_URL,
        "stockscreener:advanced-ingress-cohort-binding:"
        f"{canonical_sha256(binding_identity)}",
    )
    cursor.execute(
        "SELECT binding_id,cohort_id,manifest_id "
        "FROM rt.advanced_ingress_cohort_bindings "
        "WHERE session_date=%s AND universe_target=%s "
        "AND runtime_profile_sha256=%s "
        "AND runtime_policy_sha256=%s AND status='ACTIVE' FOR UPDATE",
        (
            session_date,
            universe_target,
            runtime_profile_sha256,
            str(profile[2]),
        ),
    )
    existing = cursor.fetchone()
    if existing is not None:
        if (
            str(existing[0]) != str(binding_id)
            or str(existing[1]) != str(cohort_id)
            or str(existing[2]) != str(manifest_id)
        ):
            raise ValueError(
                "another cohort is already bound to this exact plan scope"
            )
        return binding_id
    cursor.execute(
        "INSERT INTO rt.advanced_ingress_cohort_bindings ("
        "binding_id,session_date,universe_target,runtime_profile_sha256,"
        "configuration_sha256,runtime_policy_sha256,cohort_id,manifest_id,"
        "approval_reference,status) VALUES ("
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,'ACTIVE')",
        (
            str(binding_id),
            session_date,
            universe_target,
            runtime_profile_sha256,
            str(profile[1]),
            str(profile[2]),
            str(cohort_id),
            str(manifest_id),
            approval,
        ),
    )
    return binding_id


def _authorization_from_row(row) -> AdvancedStreamAuthorization:
    budgets = AdvancedIngressBudgets(
        int(row[7]),
        int(row[8]),
        int(row[9]),
        int(row[10]),
        int(row[11]),
    )
    return AdvancedStreamAuthorization(
        authorization_id=UUID(str(row[0])),
        authorization_reference=str(row[1]),
        scope_sha256=str(row[2]),
        parent_authorization_id=(
            UUID(str(row[3])) if row[3] is not None else None
        ),
        provider=str(row[4]),
        feed=str(row[5]),
        channel=str(row[6]),
        allowed_universe_targets=tuple(int(value) for value in row[12]),
        valid_from=row[13],
        valid_through=row[14],
        maximum_budgets=budgets,
        runtime_policy_sha256=str(row[15]),
        approval_source=str(row[16]),
        approval_reference=str(row[17]),
        status=str(row[18]),
    )


_AUTHORIZATION_SELECT = (
    "SELECT authorization_id,authorization_reference,scope_sha256,"
    "parent_authorization_id,provider,feed,channel,"
    "maximum_daily_stream_sessions,"
    "maximum_daily_connection_attempts,maximum_daily_decoded_bytes,"
    "maximum_daily_rest_repair_requests,"
    "maximum_daily_rest_repair_decoded_bytes,"
    "allowed_universe_targets,valid_from,valid_through,"
    "runtime_policy_sha256,approval_source,approval_reference,status "
    "FROM rt.advanced_stream_authorizations "
)


def load_advanced_stream_authorization_cursor(
    cursor,
    authorization_id: UUID,
    *,
    for_update: bool = False,
) -> AdvancedStreamAuthorization:
    cursor.execute(
        _AUTHORIZATION_SELECT
        + "WHERE authorization_id=%s"
        + (" FOR UPDATE" if for_update else ""),
        (str(authorization_id),),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("standing authorization is unavailable")
    return _authorization_from_row(row)


def create_advanced_stream_authorization_cursor(
    cursor,
    authorization: AdvancedStreamAuthorization,
    *,
    promotion_ladder: tuple[int, ...] | None = None,
    calendar=None,
) -> AdvancedStreamAuthorization:
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (f"v2-advanced-authorization:{authorization.scope_sha256}",),
    )
    parent = None
    if authorization.parent_authorization_id is not None:
        parent = load_advanced_stream_authorization_cursor(
            cursor,
            authorization.parent_authorization_id,
            for_update=True,
        )
    if promotion_ladder is not None:
        target = authorization.allowed_universe_targets[0]
        if target not in promotion_ladder:
            raise ValueError("authorization target is outside ladder policy")
        position = promotion_ladder.index(target)
        if position == 0:
            if parent is not None:
                raise ValueError(
                    "initial ladder authorization cannot have a parent"
                )
        else:
            if parent is None:
                raise ValueError(
                    "promotion requires a parent authorization"
                )
            if (
                parent.status != "READY"
                or parent.allowed_universe_targets[0]
                != promotion_ladder[position - 1]
                or parent.runtime_policy_sha256
                != authorization.runtime_policy_sha256
            ):
                raise ValueError(
                    "promotion must follow one configured ladder rung"
                )
            if calendar is None:
                from options.calendar import OptionExchangeCalendar

                calendar = OptionExchangeCalendar("XNYS")
            if authorization.valid_from != calendar.next_session_date(
                parent.valid_through
            ):
                raise ValueError(
                    "promotion must become effective next market session"
                )
    cursor.execute(
        "INSERT INTO rt.advanced_stream_authorizations ("
        "authorization_id,authorization_reference,scope_sha256,"
        "parent_authorization_id,provider,feed,channel,"
        "allowed_universe_targets,valid_from,valid_through,"
        "maximum_daily_stream_sessions,"
        "maximum_daily_connection_attempts,maximum_daily_decoded_bytes,"
        "maximum_daily_rest_repair_requests,"
        "maximum_daily_rest_repair_decoded_bytes,"
        "runtime_policy_sha256,approval_source,approval_reference,status"
        ") VALUES ("
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'READY'"
        ") ON CONFLICT (authorization_id) DO NOTHING",
        (
            str(authorization.authorization_id),
            authorization.authorization_reference,
            authorization.scope_sha256,
            (
                str(authorization.parent_authorization_id)
                if authorization.parent_authorization_id is not None
                else None
            ),
            authorization.provider,
            authorization.feed,
            authorization.channel,
            list(authorization.allowed_universe_targets),
            authorization.valid_from,
            authorization.valid_through,
            authorization.maximum_budgets.stream_sessions,
            authorization.maximum_budgets.connection_attempts,
            authorization.maximum_budgets.decoded_bytes,
            authorization.maximum_budgets.rest_repair_requests,
            authorization.maximum_budgets.rest_repair_decoded_bytes,
            authorization.runtime_policy_sha256,
            authorization.approval_source,
            authorization.approval_reference,
        ),
    )
    stored = load_advanced_stream_authorization_cursor(
        cursor,
        authorization.authorization_id,
        for_update=True,
    )
    if stored.scope_identity != authorization.scope_identity:
        raise ValueError("stored standing authorization conflicts with scope")
    return stored


def revoke_advanced_stream_authorization_cursor(
    cursor,
    authorization_id: UUID,
    *,
    revoked_at: datetime,
    revocation_reference: str,
) -> AdvancedStreamAuthorization:
    normalized_reference = revocation_reference.strip()
    if not normalized_reference:
        raise ValueError("revocation_reference cannot be blank")
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (f"v2-advanced-authorization:{authorization_id}",),
    )
    current = load_advanced_stream_authorization_cursor(
        cursor,
        authorization_id,
        for_update=True,
    )
    if current.status == "REVOKED":
        cursor.execute(
            "SELECT revocation_reference "
            "FROM rt.advanced_stream_authorizations "
            "WHERE authorization_id=%s",
            (str(authorization_id),),
        )
        if cursor.fetchone()[0] != normalized_reference:
            raise ValueError(
                "standing authorization was revoked under another reference"
            )
        return current
    if current.status != "READY":
        raise ValueError("only a READY standing authorization can be revoked")
    cursor.execute(
        "UPDATE rt.advanced_stream_authorizations "
        "SET status='REVOKED',revoked_at=%s,revocation_reference=%s "
        "WHERE authorization_id=%s AND status='READY' "
        "RETURNING authorization_id",
        (revoked_at, normalized_reference, str(authorization_id)),
    )
    if cursor.fetchone() is None:
        raise ValueError("standing authorization revoke lost its lock")
    return current.with_status("REVOKED")


def resolve_advanced_ingress_plan_cursor(
    cursor,
    request: AdvancedIngressPlanRequest,
    *,
    runtime_catalog: AdvancedIngressRuntimeCatalog,
    calendar=None,
) -> AdvancedIngressPlan:
    authorization = load_advanced_stream_authorization_cursor(
        cursor,
        request.authorization_id,
    )
    source_profile = runtime_catalog.profile_for_target(
        request.universe_target
    )
    runtime_policy_sha256 = runtime_catalog.scale_policy.sha256
    configuration_sha256 = source_profile.configuration_sha256(
        runtime_policy_sha256
    )
    if authorization.runtime_policy_sha256 != runtime_policy_sha256:
        raise AdvancedCohortResolutionError("RUNTIME_POLICY_MISMATCH")
    cursor.execute(
        "SELECT runtime_profile_version,configuration_sha256,"
        "runtime_policy_sha256,status,profile_document "
        "FROM rt.advanced_ingress_runtime_profiles "
        "WHERE runtime_profile_sha256=%s",
        (source_profile.sha256,),
    )
    stored_profile = cursor.fetchone()
    if (
        stored_profile is None
        or str(stored_profile[0]) != source_profile.version
        or str(stored_profile[1]) != configuration_sha256
        or str(stored_profile[2]) != runtime_policy_sha256
        or stored_profile[3] != "ACTIVE"
        or canonical_sha256(stored_profile[4]) != source_profile.sha256
    ):
        raise AdvancedCohortResolutionError("RUNTIME_PROFILE_NOT_ACTIVE")
    if calendar is None:
        from options.calendar import OptionExchangeCalendar

        calendar = OptionExchangeCalendar("XNYS")
    authorized_end_at = calendar.session_close(request.session_date)
    cursor.execute(
        "SELECT cohort.cohort_id,cohort.manifest_id "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
        "JOIN rt.capability_profiles AS capability USING(profile_id) "
        "LEFT JOIN rt.subscription_members AS member USING(manifest_id) "
        "WHERE manifest.session_date=%s AND manifest.stock_count=%s "
        "AND capability.provider_tier='MASSIVE_ADVANCED' "
        "AND capability.status='PASS' AND capability.expires_at>=%s "
        "AND capability.config_sha256=%s "
        "AND cohort.configuration_sha256=%s "
        "GROUP BY cohort.cohort_id,cohort.manifest_id,manifest.member_count "
        "HAVING COUNT(member.instrument_id)=%s "
        "AND COUNT(member.instrument_id) FILTER "
        "(WHERE member.channel='AM')=%s "
        "ORDER BY cohort.cohort_id",
        (
            request.session_date,
            request.universe_target,
            authorized_end_at,
            configuration_sha256,
            configuration_sha256,
            request.universe_target,
            request.universe_target,
        ),
    )
    candidates = tuple(
        (UUID(str(row[0])), UUID(str(row[1])))
        for row in cursor.fetchall()
    )
    cursor.execute(
        "SELECT cohort_id,manifest_id "
        "FROM rt.advanced_ingress_cohort_bindings "
        "WHERE session_date=%s AND universe_target=%s "
        "AND runtime_profile_sha256=%s "
        "AND configuration_sha256=%s "
        "AND runtime_policy_sha256=%s AND status='ACTIVE' "
        "ORDER BY binding_id",
        (
            request.session_date,
            request.universe_target,
            source_profile.sha256,
            configuration_sha256,
            runtime_policy_sha256,
        ),
    )
    bindings = tuple(
        (UUID(str(row[0])), UUID(str(row[1])))
        for row in cursor.fetchall()
    )
    if len(bindings) > 1:
        raise AdvancedCohortResolutionError("COHORT_AMBIGUOUS")
    if bindings:
        candidates = tuple(
            candidate
            for candidate in candidates
            if candidate == bindings[0]
        )
    if not candidates:
        raise AdvancedCohortResolutionError("COHORT_NOT_PROVISIONED")
    if len(candidates) > 1:
        raise AdvancedCohortResolutionError("COHORT_AMBIGUOUS")
    cohort_id, manifest_id = candidates[0]
    plan = build_advanced_ingress_plan(
        authorization=authorization,
        cohort_id=cohort_id,
        manifest_id=manifest_id,
        session_date=request.session_date,
        universe_target=request.universe_target,
        runtime_profile_version=source_profile.version,
        runtime_profile_sha256=source_profile.sha256,
        configuration_sha256=configuration_sha256,
        budgets=request.budgets,
        calendar=calendar,
    )
    return plan


def prepare_advanced_ingress_plan_cursor(
    cursor,
    plan: AdvancedIngressPlan,
    *,
    checked_at: datetime,
) -> AdvancedIngressPlan:
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (
            "v2-advanced-ingress-plan:"
            f"{plan.authorization_id}:{plan.session_date.isoformat()}",
        ),
    )
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (f"v2-advanced-authorization:{plan.authorization_id}",),
    )
    authorization = load_advanced_stream_authorization_cursor(
        cursor,
        plan.authorization_id,
    )
    if authorization.status != "READY":
        raise AdvancedIngressAdmissionError(
            "standing authorization is not READY"
        )
    if checked_at > plan.authorized_end_at:
        raise AdvancedIngressAdmissionError(
            "daily ingress plan preparation is expired"
        )
    if (
        plan.authorization_scope_sha256 != authorization.scope_sha256
        or plan.authorization_reference
        != authorization.authorization_reference
        or plan.universe_target
        not in authorization.allowed_universe_targets
        or plan.runtime_policy_sha256
        != authorization.runtime_policy_sha256
        or not plan.budgets.is_within(authorization.maximum_budgets)
    ):
        raise AdvancedIngressAdmissionError(
            "daily ingress plan exceeds standing authorization"
        )
    cursor.execute(
        "SELECT plan_id,plan_sha256 FROM rt.advanced_ingress_plans "
        "WHERE authorization_id=%s AND session_date=%s FOR UPDATE",
        (str(plan.authorization_id), plan.session_date),
    )
    existing = cursor.fetchone()
    if existing is not None:
        if (
            str(existing[0]) != str(plan.plan_id)
            or str(existing[1]) != plan.plan_sha256
        ):
            raise ValueError(
                "standing authorization already has another daily plan"
            )
        return plan
    cursor.execute(
        "INSERT INTO rt.advanced_ingress_plans ("
        "plan_id,plan_sha256,authorization_id,"
        "authorization_scope_sha256,cohort_id,manifest_id,session_date,"
        "universe_target,mode,runtime_profile_sha256,"
        "runtime_profile_version,configuration_sha256,"
        "runtime_policy_sha256,authorized_start_at,authorized_end_at,"
        "maximum_stream_sessions,maximum_connection_attempts,"
        "maximum_decoded_bytes,maximum_rest_repair_requests,"
        "maximum_rest_repair_decoded_bytes,status"
        ") VALUES ("
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
        "%s,%s,%s,%s,%s,'READY')",
        (
            str(plan.plan_id),
            plan.plan_sha256,
            str(plan.authorization_id),
            plan.authorization_scope_sha256,
            str(plan.cohort_id),
            str(plan.manifest_id),
            plan.session_date,
            plan.universe_target,
            plan.mode,
            plan.runtime_profile_sha256,
            plan.runtime_profile_version,
            plan.configuration_sha256,
            plan.runtime_policy_sha256,
            plan.authorized_start_at,
            plan.authorized_end_at,
            plan.budgets.stream_sessions,
            plan.budgets.connection_attempts,
            plan.budgets.decoded_bytes,
            plan.budgets.rest_repair_requests,
            plan.budgets.rest_repair_decoded_bytes,
        ),
    )
    cursor.execute(
        "INSERT INTO rt.advanced_ingress_plan_usage (plan_id) VALUES (%s)",
        (str(plan.plan_id),),
    )
    return plan


@dataclass(frozen=True, slots=True)
class AdvancedIngressRuntimePlanScope:
    plan_id: UUID
    authorization_reference: str
    cohort_id: UUID
    manifest_id: UUID
    session_date: object
    universe_target: int
    runtime_profile_version: str
    runtime_profile_sha256: str
    configuration_sha256: str
    runtime_policy_sha256: str
    budgets: AdvancedIngressBudgets
    authorized_start_at: datetime
    authorized_end_at: datetime
    next_connection_attempt: int
    latest_owner_epoch: int


def load_advanced_ingress_runtime_plan_cursor(
    cursor,
    plan_id: UUID,
) -> AdvancedIngressRuntimePlanScope:
    cursor.execute(
        "SELECT plan.authorization_id,auth.authorization_reference,"
        "plan.cohort_id,plan.manifest_id,plan.session_date,"
        "plan.universe_target,plan.runtime_profile_version,"
        "plan.runtime_profile_sha256,plan.configuration_sha256,"
        "plan.runtime_policy_sha256,plan.maximum_stream_sessions,"
        "plan.maximum_connection_attempts,plan.maximum_decoded_bytes,"
        "plan.maximum_rest_repair_requests,"
        "plan.maximum_rest_repair_decoded_bytes,"
        "plan.authorized_start_at,plan.authorized_end_at,plan.status,"
        "auth.status,profile.status,profile.runtime_profile_version,"
        "profile.configuration_sha256,profile.runtime_policy_sha256,"
        "manifest.session_date,manifest.stock_count,"
        "COUNT(member.instrument_id),"
        "COUNT(member.instrument_id) FILTER (WHERE member.channel='AM'),"
        "capability.provider_tier,capability.status,"
        "capability.expires_at,capability.config_sha256,"
        "cohort.configuration_sha256 "
        "FROM rt.advanced_ingress_plans AS plan "
        "JOIN rt.advanced_stream_authorizations AS auth "
        "USING(authorization_id) "
        "JOIN rt.advanced_ingress_runtime_profiles AS profile "
        "USING(runtime_profile_sha256) "
        "JOIN rt.cohorts AS cohort ON cohort.cohort_id=plan.cohort_id "
        "JOIN rt.subscription_manifests AS manifest "
        "ON manifest.manifest_id=plan.manifest_id "
        "JOIN rt.capability_profiles AS capability "
        "ON capability.profile_id=manifest.profile_id "
        "LEFT JOIN rt.subscription_members AS member "
        "ON member.manifest_id=manifest.manifest_id "
        "WHERE plan.plan_id=%s "
        "GROUP BY plan.plan_id,auth.authorization_id,"
        "auth.authorization_reference,auth.status,profile.status,"
        "profile.runtime_profile_version,profile.configuration_sha256,"
        "profile.runtime_policy_sha256,manifest.session_date,"
        "manifest.stock_count,capability.provider_tier,"
        "capability.status,capability.expires_at,"
        "capability.config_sha256,cohort.configuration_sha256",
        (str(plan_id),),
    )
    row = cursor.fetchone()
    if row is None:
        raise AdvancedIngressAdmissionError(
            "daily ingress plan is unavailable"
        )
    if (
        row[17] not in {"READY", "ACTIVE"}
        or row[18] != "READY"
        or row[19] not in {"ACTIVE", "SUPERSEDED"}
        or row[4] != row[23]
        or int(row[5]) != int(row[24])
        or int(row[25]) != int(row[5])
        or int(row[26]) != int(row[5])
        or row[27] != "MASSIVE_ADVANCED"
        or row[28] != "PASS"
        or row[29] < row[16]
        or str(row[20]) != str(row[6])
        or str(row[21]) != str(row[8])
        or str(row[22]) != str(row[9])
        or str(row[30]) != str(row[8])
        or str(row[31]) != str(row[8])
    ):
        raise AdvancedIngressAdmissionError(
            "daily ingress plan exact scope is unavailable"
        )
    cursor.execute(
        "SELECT connection_attempt_count,latest_owner_epoch "
        "FROM rt.advanced_ingress_plan_usage WHERE plan_id=%s",
        (str(plan_id),),
    )
    usage = cursor.fetchone()
    if usage is None:
        raise AdvancedIngressAdmissionError(
            "daily ingress usage ledger is unavailable"
        )
    return AdvancedIngressRuntimePlanScope(
        plan_id=plan_id,
        authorization_reference=str(row[1]),
        cohort_id=UUID(str(row[2])),
        manifest_id=UUID(str(row[3])),
        session_date=row[4],
        universe_target=int(row[5]),
        runtime_profile_version=str(row[6]),
        runtime_profile_sha256=str(row[7]),
        configuration_sha256=str(row[8]),
        runtime_policy_sha256=str(row[9]),
        budgets=AdvancedIngressBudgets(
            int(row[10]),
            int(row[11]),
            int(row[12]),
            int(row[13]),
            int(row[14]),
        ),
        authorized_start_at=row[15],
        authorized_end_at=row[16],
        next_connection_attempt=int(usage[0]) + 1,
        latest_owner_epoch=int(usage[1]),
    )


def _usage_from_row(plan_id: UUID, row) -> AdvancedIngressUsage:
    return AdvancedIngressUsage(
        plan_id=plan_id,
        stream_session_count=int(row[0]),
        connection_attempt_count=int(row[1]),
        decoded_byte_count=int(row[2]),
        rest_repair_request_count=int(row[3]),
        rest_repair_decoded_byte_count=int(row[4]),
        latest_owner_epoch=int(row[5]),
        latest_stream_session_id=(
            UUID(str(row[6])) if row[6] is not None else None
        ),
    )


def admit_advanced_ingress_session_cursor(
    cursor,
    *,
    plan_id: UUID,
    cohort_id: UUID,
    manifest_id: UUID,
    authorization_reference: str,
    connection_attempt: int,
    connection_ceiling: int,
    decoded_byte_ceiling: int,
    stream_session_id: UUID,
    owner_epoch: int,
    checked_at: datetime,
) -> AdvancedIngressUsage:
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (f"v2-advanced-ingress-usage:{plan_id}",),
    )
    cursor.execute(
        "SELECT authorization_id FROM rt.advanced_ingress_plans "
        "WHERE plan_id=%s",
        (str(plan_id),),
    )
    authorization_identity = cursor.fetchone()
    if authorization_identity is None:
        raise AdvancedIngressAdmissionError(
            "daily ingress plan is unavailable"
        )
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (
            "v2-advanced-authorization:"
            f"{authorization_identity[0]}",
        ),
    )
    cursor.execute(
        "SELECT plan.cohort_id,plan.manifest_id,plan.status,"
        "plan.authorized_start_at,plan.authorized_end_at,"
        "plan.maximum_stream_sessions,plan.maximum_connection_attempts,"
        "plan.maximum_decoded_bytes,auth.authorization_reference,"
        "auth.status,auth.valid_from,auth.valid_through,"
        "usage.stream_session_count,usage.connection_attempt_count,"
        "usage.decoded_byte_count,usage.rest_repair_request_count,"
        "usage.rest_repair_decoded_byte_count,usage.latest_owner_epoch,"
        "usage.latest_stream_session_id "
        "FROM rt.advanced_ingress_plans AS plan "
        "JOIN rt.advanced_stream_authorizations AS auth "
        "USING(authorization_id) "
        "JOIN rt.advanced_ingress_plan_usage AS usage USING(plan_id) "
        "WHERE plan.plan_id=%s FOR UPDATE OF plan,usage",
        (str(plan_id),),
    )
    row = cursor.fetchone()
    if row is None:
        raise AdvancedIngressAdmissionError(
            "daily ingress plan is unavailable"
        )
    if (
        str(row[0]) != str(cohort_id)
        or str(row[1]) != str(manifest_id)
        or row[2] not in {"READY", "ACTIVE"}
        or row[8] != authorization_reference
        or row[9] != "READY"
        or not row[10] <= checked_at.date() <= row[11]
        or not row[3] <= checked_at <= row[4]
    ):
        raise AdvancedIngressAdmissionError(
            "daily ingress plan is revoked, expired, or mismatched"
        )
    if (
        type(connection_attempt) is not int
        or connection_attempt != int(row[13]) + 1
        or connection_ceiling != int(row[6])
        or decoded_byte_ceiling != int(row[7])
        or type(owner_epoch) is not int
        or owner_epoch <= int(row[17])
    ):
        raise AdvancedIngressAdmissionError(
            "child stream identity or cumulative attempt is invalid"
        )
    if int(row[12]) + 1 > int(row[5]) or connection_attempt > int(row[6]):
        raise AdvancedIngressAdmissionError(
            "daily stream session or connection-attempt budget is exhausted"
        )
    cursor.execute(
        "UPDATE rt.advanced_ingress_plan_usage SET "
        "stream_session_count=stream_session_count+1,"
        "connection_attempt_count=connection_attempt_count+1,"
        "latest_owner_epoch=%s,latest_stream_session_id=%s,"
        "updated_at=%s WHERE plan_id=%s RETURNING "
        "stream_session_count,connection_attempt_count,decoded_byte_count,"
        "rest_repair_request_count,rest_repair_decoded_byte_count,"
        "latest_owner_epoch,latest_stream_session_id",
        (
            owner_epoch,
            str(stream_session_id),
            checked_at,
            str(plan_id),
        ),
    )
    usage = _usage_from_row(plan_id, cursor.fetchone())
    cursor.execute(
        "UPDATE rt.advanced_ingress_plans SET status='ACTIVE',"
        "activated_at=COALESCE(activated_at,%s) "
        "WHERE plan_id=%s AND status='READY'",
        (checked_at, str(plan_id)),
    )
    return usage


def record_advanced_ingress_usage_cursor(
    cursor,
    *,
    plan_id: UUID,
    stream_session_id: UUID,
    owner_epoch: int,
    decoded_bytes: int = 0,
    rest_repair_requests: int = 0,
    rest_repair_decoded_bytes: int = 0,
    checked_at: datetime,
) -> AdvancedIngressUsage:
    increments = (
        decoded_bytes,
        rest_repair_requests,
        rest_repair_decoded_bytes,
    )
    if any(type(value) is not int or value < 0 for value in increments):
        raise ValueError("cumulative usage increments cannot be negative")
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (f"v2-advanced-ingress-usage:{plan_id}",),
    )
    cursor.execute(
        "SELECT usage.stream_session_count,"
        "usage.connection_attempt_count,usage.decoded_byte_count,"
        "usage.rest_repair_request_count,"
        "usage.rest_repair_decoded_byte_count,usage.latest_owner_epoch,"
        "usage.latest_stream_session_id,plan.maximum_decoded_bytes,"
        "plan.maximum_rest_repair_requests,"
        "plan.maximum_rest_repair_decoded_bytes "
        "FROM rt.advanced_ingress_plan_usage AS usage "
        "JOIN rt.advanced_ingress_plans AS plan USING(plan_id) "
        "WHERE usage.plan_id=%s FOR UPDATE OF usage,plan",
        (str(plan_id),),
    )
    row = cursor.fetchone()
    if row is None:
        raise AdvancedIngressAdmissionError(
            "daily ingress usage ledger is unavailable"
        )
    if (
        int(row[5]) != owner_epoch
        or str(row[6]) != str(stream_session_id)
    ):
        raise AdvancedIngressAdmissionError(
            "old stream owner cannot update cumulative plan counters"
        )
    if (
        int(row[2]) + decoded_bytes > int(row[7])
        or int(row[3]) + rest_repair_requests > int(row[8])
        or int(row[4]) + rest_repair_decoded_bytes > int(row[9])
    ):
        raise AdvancedIngressAdmissionError(
            "cumulative daily ingress budget is exhausted"
        )
    cursor.execute(
        "UPDATE rt.advanced_ingress_plan_usage SET "
        "decoded_byte_count=decoded_byte_count+%s,"
        "rest_repair_request_count=rest_repair_request_count+%s,"
        "rest_repair_decoded_byte_count="
        "rest_repair_decoded_byte_count+%s,updated_at=%s "
        "WHERE plan_id=%s RETURNING "
        "stream_session_count,connection_attempt_count,decoded_byte_count,"
        "rest_repair_request_count,rest_repair_decoded_byte_count,"
        "latest_owner_epoch,latest_stream_session_id",
        (
            decoded_bytes,
            rest_repair_requests,
            rest_repair_decoded_bytes,
            checked_at,
            str(plan_id),
        ),
    )
    return _usage_from_row(plan_id, cursor.fetchone())


@dataclass(frozen=True, slots=True)
class AdvancedIngressStatus:
    plan_id: UUID
    session_date: object
    universe_target: int
    plan_status: str
    authorization_status: str
    stream_sessions: int
    connection_attempts: int
    decoded_bytes: int
    rest_repair_requests: int
    rest_repair_decoded_bytes: int


@dataclass(frozen=True, slots=True)
class AdvancedIngressControllerStatus:
    plan_id: UUID
    plan_status: str
    terminal_outcome: str | None
    authorization_status: str
    authorized_end_at: datetime
    maximum_connection_attempts: int
    connection_attempt_count: int


def advanced_ingress_controller_status_cursor(
    cursor,
    plan_id: UUID,
) -> AdvancedIngressControllerStatus:
    cursor.execute(
        "SELECT plan.status,plan.terminal_outcome,auth.status,"
        "plan.authorized_end_at,plan.maximum_connection_attempts,"
        "usage.connection_attempt_count "
        "FROM rt.advanced_ingress_plans AS plan "
        "JOIN rt.advanced_stream_authorizations AS auth "
        "USING(authorization_id) "
        "JOIN rt.advanced_ingress_plan_usage AS usage USING(plan_id) "
        "WHERE plan.plan_id=%s",
        (str(plan_id),),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("daily ingress plan is unavailable")
    return AdvancedIngressControllerStatus(
        plan_id,
        str(row[0]),
        str(row[1]) if row[1] is not None else None,
        str(row[2]),
        row[3],
        int(row[4]),
        int(row[5]),
    )


def finish_advanced_ingress_plan_cursor(
    cursor,
    plan_id: UUID,
    *,
    terminal_outcome: str,
    completed_at: datetime,
) -> AdvancedIngressControllerStatus:
    if terminal_outcome not in ADVANCED_INGRESS_TERMINAL_OUTCOMES:
        raise ValueError("unknown Advanced ingress terminal outcome")
    terminal_status = (
        "COMPLETE"
        if terminal_outcome == "SESSION_COMPLETE"
        else "FAILED"
        if terminal_outcome == "FAILED_INFRASTRUCTURE"
        else "BLOCKED"
    )
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (f"v2-advanced-ingress-usage:{plan_id}",),
    )
    current = advanced_ingress_controller_status_cursor(cursor, plan_id)
    if current.terminal_outcome is not None:
        if (
            current.terminal_outcome != terminal_outcome
            or current.plan_status != terminal_status
        ):
            raise ValueError(
                "daily ingress plan already has another terminal outcome"
            )
        return current
    cursor.execute(
        "UPDATE rt.advanced_ingress_plans SET status=%s,"
        "terminal_outcome=%s,completed_at=%s "
        "WHERE plan_id=%s AND status IN ('READY','ACTIVE') "
        "AND terminal_outcome IS NULL RETURNING plan_id",
        (
            terminal_status,
            terminal_outcome,
            completed_at,
            str(plan_id),
        ),
    )
    if cursor.fetchone() is None:
        raise ValueError("daily ingress terminal transition was rejected")
    return advanced_ingress_controller_status_cursor(cursor, plan_id)


def advanced_ingress_status_cursor(
    cursor,
    plan_id: UUID,
    *,
    checked_at: datetime,
) -> AdvancedIngressStatus:
    cursor.execute(
        "SELECT plan.session_date,plan.universe_target,plan.status,"
        "auth.status,auth.valid_from,auth.valid_through,"
        "plan.authorized_end_at,usage.stream_session_count,"
        "usage.connection_attempt_count,usage.decoded_byte_count,"
        "usage.rest_repair_request_count,"
        "usage.rest_repair_decoded_byte_count "
        "FROM rt.advanced_ingress_plans AS plan "
        "JOIN rt.advanced_stream_authorization_status AS auth "
        "USING(authorization_id) "
        "JOIN rt.advanced_ingress_plan_usage AS usage USING(plan_id) "
        "WHERE plan.plan_id=%s",
        (str(plan_id),),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("daily ingress plan is unavailable")
    authorization_status = str(row[3])
    if (
        authorization_status == "READY"
        and (
            not row[4] <= checked_at.date() <= row[5]
            or checked_at > row[6]
        )
    ):
        authorization_status = "EXPIRED"
    return AdvancedIngressStatus(
        plan_id,
        row[0],
        int(row[1]),
        str(row[2]),
        authorization_status,
        int(row[7]),
        int(row[8]),
        int(row[9]),
        int(row[10]),
        int(row[11]),
    )
