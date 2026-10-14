from __future__ import annotations

from dataclasses import dataclass

from .bootstrap_manifest import (
    BOOTSTRAP_STAGE_ORDER,
    BootstrapTargetKind,
    RealtimeBootstrapManifest,
    validate_bootstrap_target,
)
from .contracts import canonical_sha256
from .security import build_role_names


@dataclass(frozen=True, slots=True)
class RealtimeIntegrationResetPlan:
    database: str
    role_prefix: str
    manifest_sha256: str
    confirmation_token: str
    actions: tuple[str, ...]
    preserved_roles: tuple[str, ...]
    rebuild_stages: tuple[str, ...]
    provider_requests: int
    database_connections: int
    database_writes: int

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


def build_realtime_integration_reset_plan(
    manifest: RealtimeBootstrapManifest,
    *,
    database: str,
    confirmation: str,
    role_prefix: str,
) -> RealtimeIntegrationResetPlan:
    if manifest.target_kind is not BootstrapTargetKind.INTEGRATION_DISPOSABLE:
        raise ValueError("reset plan accepts only disposable integration manifests")
    target = validate_bootstrap_target(
        database,
        confirmation,
        manifest.target_kind,
    )
    roles = build_role_names(role_prefix)
    confirmation_token = f"RESET:{target}:{manifest.sha256[:12]}"
    return RealtimeIntegrationResetPlan(
        database=target,
        role_prefix=role_prefix,
        manifest_sha256=manifest.sha256,
        confirmation_token=confirmation_token,
        actions=(
            "LOCK_PRINCIPALS",
            "VERIFY_EXACT_DATABASE_IDENTITY",
            "TERMINATE_TARGET_DATABASE_SESSIONS",
            "DROP_TARGET_DATABASE",
            "CREATE_EMPTY_TARGET_DATABASE",
            "RUN_FULL_BOOTSTRAP",
        ),
        preserved_roles=tuple(sorted(roles.all_names)),
        rebuild_stages=tuple(stage.value for stage in BOOTSTRAP_STAGE_ORDER),
        provider_requests=0,
        database_connections=0,
        database_writes=0,
    )
