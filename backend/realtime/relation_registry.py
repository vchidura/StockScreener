"""V2 relation registry: one source of truth for retention, partitioning and grants.

Every table in the sql_v2 baseline must appear here, and every entry must exist in the
database; tests compare the registry with the SQL files and a disposable catalog.
Grants are derived from this module, never hand-copied into several lists.

Three database roles. `admin` owns every object and is the only role that runs DDL, migrations,
hardening and partition detach/drop. `app` is the one runtime role (orchestrator, stages, workers).
`api` is the read-only role of the HTTP process. `app` holds INSERT and UPDATE on every writable
table; an append-only table is kept append-only by its immutability trigger (a test requires one),
not by withholding the privilege.
"""
from __future__ import annotations

from dataclasses import dataclass

from .contracts import canonical_sha256

SECURITY_POLICY_VERSION = "realtime_security_v3"
# Ledger states in rt.security_phase_events. DEFERRED is the structural baseline: no roles
# or grants exist and the runtime connects as the database owner.
SECURITY_PHASES = ("DEFERRED", "APPLIED", "AUDITED")

ADMIN_ROLE = "admin"
APP_ROLE = "app"
API_ROLE = "api"
RUNTIME_ROLES = (APP_ROLE, API_ROLE)
ROLES = (ADMIN_ROLE, *RUNTIME_ROLES)

RETENTION_CLASSES = (
    "BOUNDED", "REFERENCE", "HOT_TTL", "SESSION_WINDOW", "KEPT_3M", "HISTORY_PLAN",
    "ARCHIVE_FIRST", "DEFERRED", "OWNER_ONLY",
)
# NONE: the runtime only reads it. APPEND: rows are inserted and never changed (an UPDATE-blocking
# trigger enforces it). MUTABLE: rows are updated in place (current pointers, leases, jobs).
WRITE_MODES = ("NONE", "APPEND", "MUTABLE")


@dataclass(frozen=True, slots=True)
class Relation:
    domain: str
    retention: str
    partitioned: bool = False
    writes: str = "NONE"
    delete: bool = False
    api: bool = False

    def __post_init__(self) -> None:
        if self.domain not in {"shared", "equity", "options", "control"}:
            raise ValueError(f"unknown domain {self.domain}")
        if self.retention not in RETENTION_CLASSES:
            raise ValueError(f"unknown retention class {self.retention}")
        if self.writes not in WRITE_MODES:
            raise ValueError(f"unknown write mode {self.writes}")
        if self.delete and self.writes != "MUTABLE":
            raise ValueError("only a mutable relation grants DELETE to app")
        if self.retention == "OWNER_ONLY" and (self.writes != "NONE" or self.delete or self.api):
            raise ValueError("an owner-only relation grants nothing to app or api")


def _r(domain, retention, *, append=False, mutable=False, delete=False, api=False, partitioned=False):
    """`delete` is app DELETE (retention-pruned runtime tables); catalog prunes and partition
    detach/drop need no flag because admin owns every relation."""
    if append and mutable:
        raise ValueError("a relation is either append-only or mutable")
    writes = "MUTABLE" if mutable else "APPEND" if append else "NONE"
    return Relation(domain, retention, partitioned, writes, delete, api)


_REF = dict(retention="REFERENCE")

RELATIONS: dict[str, Relation] = {
    # Foundation
    "rt.database_identity": _r("shared", "BOUNDED", api=True),
    "rt.schema_migrations": _r("shared", "OWNER_ONLY"),
    "rt.database_security_policies": _r("shared", "BOUNDED", api=True),
    "rt.security_phase_events": _r("shared", "BOUNDED", api=True),
    "rt.partition_policies": _r("shared", "BOUNDED"),
    # Reference data
    "ref.securities": _r("shared", append=True, api=True, **_REF),
    "ref.security_revisions": _r("shared", append=True, api=True, **_REF),
    "ref.option_contracts": _r("shared", append=True, api=True, **_REF),
    "ref.option_contract_versions": _r("shared", append=True, api=True, **_REF),
    "ref.corporate_action_revisions": _r("shared", append=True, api=True, **_REF),
    "ref.corporate_action_coverage": _r("shared", append=True, api=True, **_REF),
    "ref.oi_baselines": _r("shared", append=True, **_REF),
    "ref.market_event_revisions": _r("shared", append=True, **_REF),
    "ref.market_event_coverage": _r("shared", append=True, **_REF),
    "ref.risk_free_rate_observations": _r("shared", append=True, **_REF),
    "ref.fundamental_reports": _r("shared", append=True, **_REF),
    "ref.fundamental_report_coverage": _r("shared", append=True, **_REF),
    # Control plane
    "rt.instruments": _r("shared", "BOUNDED", append=True, api=True),
    "rt.capability_profiles": _r("control", "BOUNDED", append=True),
    "rt.capability_checks": _r("control", "BOUNDED", append=True),
    "rt.subscription_manifests": _r("control", "BOUNDED", append=True, api=True),
    "rt.subscription_members": _r("control", "BOUNDED", append=True, api=True),
    "rt.slot_set_versions": _r("options", "BOUNDED", append=True),
    "rt.slot_set_members": _r("options", "BOUNDED", append=True),
    "rt.slot_set_current": _r("options", "BOUNDED", mutable=True),
    "rt.cohorts": _r("control", "BOUNDED", append=True, api=True),
    "rt.cohort_history_bindings": _r(
        "control", "BOUNDED", append=True, api=True
    ),
    "rt.worker_leases": _r("control", "BOUNDED", mutable=True, delete=True),
    "rt.readiness_current": _r("control", "BOUNDED", mutable=True, api=True),
    "rt.readiness_transitions": _r("control", "BOUNDED", append=True, api=True),
    "rt.advanced_stream_authorizations": _r("control", "BOUNDED"),
    "rt.advanced_stream_authorization_status": _r(
        "control", "BOUNDED", api=True
    ),
    "rt.advanced_ingress_runtime_profiles": _r(
        "control", "BOUNDED", api=True
    ),
    "rt.advanced_ingress_plans": _r(
        "control", "BOUNDED", mutable=True, api=True
    ),
    "rt.advanced_ingress_plan_usage": _r(
        "control", "BOUNDED", mutable=True, api=True
    ),
    "rt.advanced_journal_registry": _r(
        "control", "BOUNDED", mutable=True, api=True
    ),
    "rt.stream_sessions": _r("shared", "BOUNDED", mutable=True),
    "rt.stream_gaps": _r("shared", "BOUNDED", mutable=True),
    "rt.provider_tier_transitions": _r("control", "BOUNDED", append=True, api=True),
    "rt.provider_tier_cohorts": _r("control", "BOUNDED", append=True, api=True),
    "rt.provider_tier_activations": _r("control", "BOUNDED", append=True, api=True),
    "rt.portal_cohort_current": _r("control", "BOUNDED", mutable=True, api=True),
    "rt.seed_manifests": _r("shared", "BOUNDED", append=True, api=True),
    "rt.seed_manifest_members": _r("shared", "BOUNDED", append=True, api=True),
    "rt.seed_current": _r("shared", "BOUNDED", mutable=True, api=True),
    # Orchestration (independent stages)
    "rt.stage_catalog": _r("control", "BOUNDED"),
    "rt.service_plans": _r("control", "BOUNDED", append=True),
    "rt.window_runs": _r("control", "BOUNDED", mutable=True),
    "rt.stage_runs": _r("control", "BOUNDED", mutable=True),
    "rt.stage_readiness_current": _r("control", "BOUNDED", mutable=True, api=True),
    "rt.cycle_receipts": _r("control", "BOUNDED", append=True),
    "rt.release_current": _r("control", "BOUNDED", mutable=True),
    # Provider ledger, watermarks and acquisition
    "rt.provider_request_ledger": _r("shared", "KEPT_3M", mutable=True, delete=True),
    "rt.acquisition_watermarks": _r("equity", "BOUNDED", mutable=True),
    "rt.raw_payloads": _r("shared", "HOT_TTL", append=True, partitioned=True),
    "rt.raw_payload_chunks": _r("shared", "HOT_TTL", append=True, partitioned=True),
    "rt.acquisition_batches": _r("shared", "BOUNDED", append=True),
    "rt.acquisition_facts": _r("shared", "KEPT_3M", append=True, partitioned=True),
    # Bars and jobs
    "rt.bar_catalog": _r("equity", "HISTORY_PLAN", append=True, api=True),
    "rt.bar_revisions": _r("equity", "HISTORY_PLAN", append=True, partitioned=True, api=True),
    "rt.bar_current": _r("equity", "HISTORY_PLAN", mutable=True, api=True),
    "rt.session_close_audits": _r("equity", "KEPT_3M", append=True, api=True),
    "rt.session_close_anomalies": _r("equity", "KEPT_3M", append=True, api=True),
    "rt.session_close_current": _r("equity", "BOUNDED", mutable=True, api=True),
    "rt.quote_current": _r("shared", "BOUNDED", mutable=True, api=True),
    "rt.trade_current": _r("shared", "BOUNDED", mutable=True, api=True),
    "rt.consumer_checkpoints": _r("shared", "BOUNDED", mutable=True),
    "rt.archive_segments": _r("shared", "BOUNDED", append=True),
    "rt.materialization_jobs": _r("shared", "HOT_TTL", mutable=True, delete=True),
    "rt.job_barriers": _r("shared", "HOT_TTL", mutable=True, delete=True),
    "rt.job_barrier_members": _r("shared", "HOT_TTL", mutable=True, delete=True),
    # Evidence
    "rt.evidence_catalog": _r("equity", "KEPT_3M", append=True, api=True),
    "rt.evidence_source_window_catalog": _r("equity", "KEPT_3M", append=True, api=True),
    "rt.equity_evidence": _r("equity", "KEPT_3M", append=True, partitioned=True, api=True),
    "rt.equity_evidence_inputs": _r("equity", "KEPT_3M", append=True, partitioned=True, api=True),
    "rt.equity_evidence_dependencies": _r("equity", "KEPT_3M", append=True, partitioned=True, api=True),
    "rt.equity_evidence_source_windows": _r("equity", "KEPT_3M", append=True, partitioned=True, api=True),
    "rt.feature_definitions": _r("shared", "BOUNDED", append=True, api=True),
    "rt.feature_snapshots": _r("equity", "KEPT_3M", append=True, partitioned=True, api=True),
    "rt.feature_current": _r("equity", "BOUNDED", mutable=True, api=True),
    "rt.equity_evidence_current": _r("equity", "KEPT_3M", mutable=True, api=True),
    # Detectors, screening and alerts
    "rt.detector_versions": _r("shared", "BOUNDED", append=True, api=True),
    "rt.detector_events": _r("shared", "DEFERRED", append=True, api=True),
    "rt.detector_event_inputs": _r("shared", "DEFERRED", append=True, api=True),
    "rt.detector_state": _r("shared", "BOUNDED", mutable=True, api=True),
    "rt.screening_generations": _r("equity", "DEFERRED", append=True, api=True),
    "rt.screening_rows": _r("equity", "DEFERRED", append=True, api=True),
    "rt.screening_current": _r("equity", "BOUNDED", mutable=True, api=True),
    "rt.detector_datasets": _r("shared", "ARCHIVE_FIRST", append=True, api=True),
    "rt.detector_evaluations": _r("shared", "ARCHIVE_FIRST", append=True, api=True),
    "rt.alert_management_assessments": _r("shared", "ARCHIVE_FIRST", append=True, api=True),
    "rt.alert_management_current": _r("shared", "BOUNDED", mutable=True, api=True),
    "rt.alert_current": _r("shared", "ARCHIVE_FIRST", mutable=True, api=True),
    "rt.alert_outbox": _r("shared", "BOUNDED", mutable=True),
    # Options acquisition, selection and packages
    "rt.option_discovery_batches": _r("options", "DEFERRED", append=True),
    "rt.option_discovery_pages": _r("options", "HOT_TTL", append=True, partitioned=True),
    "rt.option_discovery_coverage": _r("options", "DEFERRED", append=True),
    "rt.option_manifest_selection_audits": _r("options", "DEFERRED", append=True),
    "rt.option_manifest_contract_dispositions": _r("options", "DEFERRED", append=True),
    "rt.option_manifest_coverage": _r("options", "DEFERRED", append=True),
    "rt.option_pre_session_runs": _r("options", "DEFERRED", append=True),
    "rt.option_pre_session_work": _r("options", "DEFERRED", mutable=True),
    "rt.option_pre_session_account_usage": _r("options", "DEFERRED", mutable=True),
    "rt.option_pre_session_packages": _r("options", "DEFERRED", append=True),
    "rt.option_pre_session_finalizations": _r("options", "DEFERRED", append=True),
    "rt.option_packages": _r("options", "DEFERRED", append=True, api=True),
    "rt.option_package_legs": _r("options", "DEFERRED", append=True, api=True),
    "rt.option_mark_evidence": _r("options", "DEFERRED", append=True, api=True),
    "rt.option_package_mark_revisions": _r("options", "DEFERRED", append=True, api=True),
    "rt.option_package_mark_inputs": _r("options", "DEFERRED", append=True, api=True),
    "rt.option_package_mark_current": _r("options", "BOUNDED", mutable=True, api=True),
    "rt.option_strategy_admissions": _r("options", "BOUNDED", append=True, api=True),
    "rt.normalization_diagnostics": _r("options", "KEPT_3M", append=True, partitioned=True),
    # Options flow (opening flow and block detection)
    "ref.trade_condition_codes": _r("shared", append=True, **_REF),
    "ref.exchange_codes": _r("shared", append=True, **_REF),
    "ref.option_condition_semantics": _r("shared", append=True, **_REF),
    "rt.option_flow_events": _r("options", "ARCHIVE_FIRST", append=True, partitioned=True, api=True),
    "rt.option_flow_clusters": _r("options", "ARCHIVE_FIRST", append=True, partitioned=True, api=True),
    "rt.option_volume_checkpoints": _r("options", "HOT_TTL", mutable=True, delete=True),
    "rt.local_mock_requests": _r("options", "OWNER_ONLY"),
    "rt.local_mock_request_inputs": _r("options", "OWNER_ONLY"),
    "rt.local_mock_dispositions": _r("options", "OWNER_ONLY"),
    "rt.local_mock_fills": _r("options", "OWNER_ONLY"),
    # Portal
    "rt.snapshot_catalog": _r("equity", "HOT_TTL", append=True, api=True),
    "rt.portal_snapshot_hot": _r("equity", "HOT_TTL", append=True, partitioned=True, api=True),
    "rt.portal_snapshot_kept": _r("equity", "SESSION_WINDOW", append=True, partitioned=True, api=True),
    "rt.portal_releases": _r("equity", "KEPT_3M", append=True, partitioned=True, api=True),
    "rt.portal_release_current": _r("equity", "BOUNDED", mutable=True, api=True),
    # Archive
    "rt.session_archives": _r("shared", "BOUNDED", append=True),
}

VIEWS = frozenset({"rt.advanced_stream_authorization_status"})
PARTITIONED_RELATIONS = tuple(sorted(name for name, spec in RELATIONS.items() if spec.partitioned))

# Retention windows (docs/REALTIME_SCHEMA_V2_DESIGN.md). Mirrors rt.partition_policies.
PARTITION_POLICY_SEEDS = {
    "rt.raw_payloads": ("DAILY", "HOT_TTL", "CALENDAR_DAYS", 14),
    "rt.raw_payload_chunks": ("DAILY", "HOT_TTL", "CALENDAR_DAYS", 14),
    "rt.option_discovery_pages": ("DAILY", "HOT_TTL", "CALENDAR_DAYS", 14),
    "rt.acquisition_facts": ("MONTHLY", "KEPT_3M", "MONTHS", 3),
    "rt.bar_revisions": ("MONTHLY", "HISTORY_PLAN", "MONTHS", 15),
    "rt.equity_evidence": ("MONTHLY", "KEPT_3M", "MONTHS", 3),
    "rt.equity_evidence_inputs": ("MONTHLY", "KEPT_3M", "MONTHS", 3),
    "rt.equity_evidence_dependencies": ("MONTHLY", "KEPT_3M", "MONTHS", 3),
    "rt.equity_evidence_source_windows": ("MONTHLY", "KEPT_3M", "MONTHS", 3),
    "rt.feature_snapshots": ("MONTHLY", "KEPT_3M", "MONTHS", 3),
    "rt.normalization_diagnostics": ("MONTHLY", "KEPT_3M", "MONTHS", 3),
    "rt.portal_snapshot_hot": ("DAILY", "HOT_TTL", "CALENDAR_DAYS", 3),
    "rt.portal_snapshot_kept": ("WEEKLY", "SESSION_WINDOW", "SESSIONS", 26),
    "rt.portal_releases": ("MONTHLY", "KEPT_3M", "MONTHS", 3),
    "rt.option_flow_events": ("WEEKLY", "ARCHIVE_FIRST", "SESSIONS", 26),
    "rt.option_flow_clusters": ("WEEKLY", "ARCHIVE_FIRST", "SESSIONS", 26),
}


def security_policy() -> dict[str, object]:
    return {
        "version": SECURITY_POLICY_VERSION,
        "owner_role": ADMIN_ROLE,
        "runtime_roles": list(RUNTIME_ROLES),
        "public_access": "NONE",
        "object_grants": "RELATION_REGISTRY_V2",
    }


def security_policy_sha256() -> str:
    return canonical_sha256(security_policy())


def select_roles(spec: Relation) -> tuple[str, ...]:
    """Runtime roles that hold SELECT on a relation (admin owns it and needs no grant)."""
    if spec.retention == "OWNER_ONLY":
        return ()
    return (APP_ROLE, API_ROLE) if spec.api else (APP_ROLE,)


def grants() -> dict[str, dict[str, tuple[str, ...]]]:
    """Return {relation: {privilege: roles}} derived from the registry."""
    result: dict[str, dict[str, tuple[str, ...]]] = {}
    for name, spec in RELATIONS.items():
        writer = (APP_ROLE,) if spec.writes != "NONE" else ()
        entry = {
            "SELECT": select_roles(spec),
            "INSERT": writer,
            "UPDATE": writer,
            "DELETE": (APP_ROLE,) if spec.delete else (),
        }
        result[name] = {key: value for key, value in entry.items() if value}
    return result
