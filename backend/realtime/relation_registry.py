"""V2 relation registry: one source of truth for retention, partitioning and grants.

Every table in the sql_v2 baseline must appear here, and every entry must exist in the
database; tests compare the registry with the SQL files and a disposable catalog.
Grants are derived from this module, never hand-copied into several lists.
"""
from __future__ import annotations

from dataclasses import dataclass

from .contracts import canonical_sha256

SECURITY_POLICY_VERSION = "realtime_security_v2"
# Ledger states in rt.security_phase_events. DEFERRED is the structural baseline: no roles
# or grants exist and the runtime connects as the database owner.
SECURITY_PHASES = ("DEFERRED", "APPLIED", "AUDITED")

CAPABILITY_ROLES = (
    "control",
    "ingress",
    "compute",
    "references",
    "publication",
    "reader",
    "maintenance",
    "options_ingress",
    "options_compute",
    "partition_admin",
)
PRINCIPAL_PROFILES = {
    "migrator": ("owner",),
    "runtime_data": ("control", "ingress", "compute", "references", "publication", "maintenance"),
    "runtime_options": ("control", "options_ingress", "options_compute"),
    "api": ("reader",),
    "archivist": ("partition_admin", "owner"),
}

# Roles that may read non-ingress tables by default. Ingress roles read only tables
# flagged ingress_read; the reader reads only tables flagged api.
DEFAULT_READ_ROLES = (
    "control", "compute", "references", "publication", "maintenance", "options_compute",
)
INGRESS_ROLES = ("ingress", "options_ingress")

RETENTION_CLASSES = (
    "BOUNDED", "REFERENCE", "HOT_TTL", "SESSION_WINDOW", "KEPT_3M", "HISTORY_PLAN",
    "ARCHIVE_FIRST", "DEFERRED", "OWNER_ONLY",
)


@dataclass(frozen=True, slots=True)
class Relation:
    domain: str
    retention: str
    partitioned: bool = False
    insert: tuple[str, ...] = ()
    update: tuple[str, ...] = ()
    delete: tuple[str, ...] = ()
    ingress_read: bool = False
    api: bool = False
    prune_read: bool = False

    def __post_init__(self) -> None:
        if self.domain not in {"shared", "equity", "options", "control"}:
            raise ValueError(f"unknown domain {self.domain}")
        if self.retention not in RETENTION_CLASSES:
            raise ValueError(f"unknown retention class {self.retention}")
        for role in (*self.insert, *self.update, *self.delete):
            if role not in CAPABILITY_ROLES:
                raise ValueError(f"unknown capability role {role}")


def _r(domain, retention, *, insert=(), update=(), delete=(), ingress_read=False, api=False,
       partitioned=False, prune_read=False):
    return Relation(domain, retention, partitioned, tuple(insert), tuple(update), tuple(delete),
                    ingress_read, api, prune_read)


_REF = dict(retention="REFERENCE")
_SHARED_WRITE = ("control", "compute", "options_compute")
_MIXED_COMPUTE = ("compute", "options_compute")
_LEASE_ROLES = ("control", "ingress", "compute", "maintenance", "options_ingress", "options_compute")

RELATIONS: dict[str, Relation] = {
    # Foundation
    "rt.database_identity": _r("shared", "BOUNDED", ingress_read=True, api=True),
    "rt.schema_migrations": _r("shared", "OWNER_ONLY"),
    "rt.database_security_policies": _r("shared", "BOUNDED", api=True),
    "rt.security_phase_events": _r("shared", "BOUNDED", api=True),
    "rt.partition_policies": _r("shared", "BOUNDED", prune_read=True),
    # Reference data
    "ref.securities": _r("shared", insert=("references",), ingress_read=True, api=True, **_REF),
    "ref.security_revisions": _r("shared", insert=("references",), ingress_read=True, api=True, **_REF),
    "ref.option_contracts": _r("shared", insert=("references",), ingress_read=True, api=True, **_REF),
    "ref.option_contract_versions": _r("shared", insert=("references",), ingress_read=True, api=True, **_REF),
    "ref.corporate_action_revisions": _r("shared", insert=("references",), ingress_read=True, api=True, **_REF),
    "ref.corporate_action_coverage": _r("shared", insert=("references",), ingress_read=True, api=True, **_REF),
    "ref.oi_baselines": _r("shared", insert=("references",), ingress_read=True, **_REF),
    "ref.market_event_revisions": _r("shared", insert=("references",), ingress_read=True, **_REF),
    "ref.market_event_coverage": _r("shared", insert=("references",), ingress_read=True, **_REF),
    "ref.risk_free_rate_observations": _r("shared", insert=("references",), ingress_read=True, **_REF),
    "ref.fundamental_reports": _r("shared", insert=("references",), ingress_read=True, **_REF),
    "ref.fundamental_report_coverage": _r("shared", insert=("references",), ingress_read=True, **_REF),
    # Control plane
    "rt.instruments": _r("shared", "BOUNDED", insert=("references",), ingress_read=True, api=True),
    "rt.capability_profiles": _r("control", "BOUNDED", insert=("control",), ingress_read=True),
    "rt.capability_checks": _r("control", "BOUNDED", insert=("control",), ingress_read=True),
    "rt.subscription_manifests": _r("control", "BOUNDED", insert=("control",), ingress_read=True, api=True),
    "rt.subscription_members": _r("control", "BOUNDED", insert=("control",), ingress_read=True, api=True),
    "rt.slot_set_versions": _r("options", "BOUNDED", insert=("control",), ingress_read=True),
    "rt.slot_set_members": _r("options", "BOUNDED", insert=("control",), ingress_read=True),
    "rt.slot_set_current": _r("options", "BOUNDED", insert=("control",), update=("control",), ingress_read=True),
    "rt.cohorts": _r("control", "BOUNDED", insert=("control",), ingress_read=True, api=True),
    "rt.worker_leases": _r("control", "BOUNDED", insert=_LEASE_ROLES, update=_LEASE_ROLES,
                           delete=_LEASE_ROLES, ingress_read=True),
    "rt.readiness_current": _r("control", "BOUNDED", insert=("control", "compute", "options_compute"),
                               update=("control", "compute", "options_compute"), ingress_read=True, api=True),
    "rt.readiness_transitions": _r("control", "BOUNDED", insert=("control",), api=True),
    "rt.stream_sessions": _r("shared", "BOUNDED", insert=INGRESS_ROLES, update=INGRESS_ROLES, ingress_read=True),
    "rt.stream_gaps": _r("shared", "BOUNDED", insert=(*INGRESS_ROLES, "maintenance"),
                         update=(*INGRESS_ROLES, "maintenance"), ingress_read=True),
    "rt.provider_tier_transitions": _r("control", "BOUNDED", insert=("control",), ingress_read=True, api=True),
    "rt.provider_tier_cohorts": _r("control", "BOUNDED", insert=("control",), ingress_read=True, api=True),
    "rt.provider_tier_activations": _r("control", "BOUNDED", insert=("control",), ingress_read=True, api=True),
    "rt.portal_cohort_current": _r("control", "BOUNDED", insert=("control",), update=("control",), api=True),
    "rt.seed_manifests": _r("shared", "BOUNDED", insert=("references",), ingress_read=True, api=True),
    "rt.seed_manifest_members": _r("shared", "BOUNDED", insert=("references",), ingress_read=True, api=True),
    "rt.seed_current": _r("shared", "BOUNDED", insert=("references",), update=("references",),
                          ingress_read=True, api=True),
    # Orchestration (independent stages)
    "rt.stage_catalog": _r("control", "BOUNDED", ingress_read=True),
    "rt.service_plans": _r("control", "BOUNDED", insert=("control",), ingress_read=True),
    "rt.window_runs": _r("control", "BOUNDED", insert=("control",), update=("control",), ingress_read=True),
    "rt.stage_runs": _r("control", "BOUNDED", insert=("control",), update=("control",), ingress_read=True),
    "rt.stage_readiness_current": _r("control", "BOUNDED", insert=("control",), update=("control",),
                                     ingress_read=True, api=True),
    "rt.cycle_receipts": _r("control", "BOUNDED", insert=_SHARED_WRITE, ingress_read=True),
    "rt.release_current": _r("control", "BOUNDED", insert=_SHARED_WRITE, update=_SHARED_WRITE),
    # Provider ledger, watermarks and acquisition
    "rt.provider_request_ledger": _r("shared", "KEPT_3M",
                                     insert=("control", "references", *INGRESS_ROLES),
                                     update=("control", "references", *INGRESS_ROLES),
                                     delete=("maintenance",), ingress_read=True),
    "rt.acquisition_watermarks": _r("equity", "BOUNDED", insert=("ingress",), update=("ingress",),
                                    ingress_read=True),
    "rt.raw_payloads": _r("shared", "HOT_TTL", insert=(*INGRESS_ROLES, "references"), partitioned=True,
                          ingress_read=True),
    "rt.raw_payload_chunks": _r("shared", "HOT_TTL", insert=(*INGRESS_ROLES, "references"),
                                partitioned=True, ingress_read=True),
    "rt.acquisition_batches": _r("shared", "BOUNDED", insert=INGRESS_ROLES, ingress_read=True),
    "rt.acquisition_facts": _r("shared", "KEPT_3M", insert=INGRESS_ROLES, partitioned=True, ingress_read=True),
    # Bars and jobs
    "rt.bar_catalog": _r("equity", "HISTORY_PLAN", insert=("compute",), delete=("partition_admin",),
                         api=True, prune_read=True),
    "rt.bar_revisions": _r("equity", "HISTORY_PLAN", insert=("compute",), partitioned=True, api=True),
    "rt.bar_current": _r("equity", "HISTORY_PLAN", insert=("compute",), update=("compute",), api=True),
    "rt.quote_current": _r("shared", "BOUNDED", insert=_MIXED_COMPUTE, update=_MIXED_COMPUTE, api=True),
    "rt.trade_current": _r("shared", "BOUNDED", insert=_MIXED_COMPUTE, update=_MIXED_COMPUTE, api=True),
    "rt.consumer_checkpoints": _r("shared", "BOUNDED", insert=_MIXED_COMPUTE, update=_MIXED_COMPUTE),
    "rt.archive_segments": _r("shared", "BOUNDED", insert=("maintenance",)),
    "rt.materialization_jobs": _r("shared", "HOT_TTL", insert=(*_MIXED_COMPUTE, "maintenance"),
                                  update=(*_MIXED_COMPUTE, "maintenance"), delete=("maintenance",)),
    "rt.job_barriers": _r("shared", "HOT_TTL", insert=_MIXED_COMPUTE, update=_MIXED_COMPUTE,
                          delete=("maintenance",)),
    "rt.job_barrier_members": _r("shared", "HOT_TTL", insert=_MIXED_COMPUTE, update=_MIXED_COMPUTE,
                                 delete=("maintenance",)),
    # Evidence
    "rt.evidence_catalog": _r("equity", "KEPT_3M", insert=("compute",), delete=("partition_admin",),
                              api=True, prune_read=True),
    "rt.evidence_source_window_catalog": _r("equity", "KEPT_3M", insert=("compute",),
                                            delete=("partition_admin",), api=True, prune_read=True),
    "rt.equity_evidence": _r("equity", "KEPT_3M", insert=("compute",), partitioned=True, api=True),
    "rt.equity_evidence_inputs": _r("equity", "KEPT_3M", insert=("compute",), partitioned=True, api=True),
    "rt.equity_evidence_dependencies": _r("equity", "KEPT_3M", insert=("compute",), partitioned=True, api=True),
    "rt.equity_evidence_source_windows": _r("equity", "KEPT_3M", insert=("compute",), partitioned=True, api=True),
    "rt.feature_definitions": _r("shared", "BOUNDED", insert=_MIXED_COMPUTE, api=True),
    "rt.feature_snapshots": _r("equity", "KEPT_3M", insert=("compute",), partitioned=True, api=True),
    "rt.feature_current": _r("equity", "BOUNDED", insert=("compute",), update=("compute",), api=True),
    "rt.equity_evidence_current": _r("equity", "KEPT_3M", insert=("compute",), update=("compute",), api=True),
    # Detectors, screening and alerts (mixed equity and options writers)
    "rt.detector_versions": _r("shared", "BOUNDED", insert=_MIXED_COMPUTE, api=True),
    "rt.detector_events": _r("shared", "DEFERRED", insert=_MIXED_COMPUTE, api=True),
    "rt.detector_event_inputs": _r("shared", "DEFERRED", insert=_MIXED_COMPUTE, api=True),
    "rt.detector_state": _r("shared", "BOUNDED", insert=_MIXED_COMPUTE, update=_MIXED_COMPUTE, api=True),
    "rt.screening_generations": _r("equity", "DEFERRED", insert=("compute",), api=True),
    "rt.screening_rows": _r("equity", "DEFERRED", insert=("compute",), api=True),
    "rt.screening_current": _r("equity", "BOUNDED", insert=("compute",), update=("compute",), api=True),
    "rt.detector_datasets": _r("shared", "ARCHIVE_FIRST", insert=_MIXED_COMPUTE, api=True),
    "rt.detector_evaluations": _r("shared", "ARCHIVE_FIRST", insert=_MIXED_COMPUTE, api=True),
    "rt.alert_management_assessments": _r("shared", "ARCHIVE_FIRST", insert=_MIXED_COMPUTE, api=True),
    "rt.alert_management_current": _r("shared", "BOUNDED", insert=_MIXED_COMPUTE, update=_MIXED_COMPUTE, api=True),
    "rt.alert_current": _r("shared", "ARCHIVE_FIRST", insert=_MIXED_COMPUTE, update=_MIXED_COMPUTE, api=True),
    "rt.alert_outbox": _r("shared", "BOUNDED", insert=_MIXED_COMPUTE, update=("publication",)),
    # Options acquisition, selection and packages
    "rt.option_discovery_batches": _r("options", "DEFERRED", insert=("references",)),
    "rt.option_discovery_pages": _r("options", "HOT_TTL", insert=("references",), partitioned=True),
    "rt.option_discovery_coverage": _r("options", "DEFERRED", insert=("references",)),
    "rt.option_manifest_selection_audits": _r("options", "DEFERRED", insert=("control",)),
    "rt.option_manifest_contract_dispositions": _r("options", "DEFERRED", insert=("control",)),
    "rt.option_manifest_coverage": _r("options", "DEFERRED", insert=("control",)),
    "rt.option_pre_session_runs": _r("options", "DEFERRED", insert=("references",)),
    "rt.option_pre_session_work": _r("options", "DEFERRED", insert=("references",), update=("references",)),
    "rt.option_pre_session_account_usage": _r("options", "DEFERRED", insert=("references",),
                                              update=("references",)),
    "rt.option_pre_session_packages": _r("options", "DEFERRED", insert=("references",)),
    "rt.option_pre_session_finalizations": _r("options", "DEFERRED", insert=("control",)),
    "rt.option_packages": _r("options", "DEFERRED", insert=("options_compute",), api=True),
    "rt.option_package_legs": _r("options", "DEFERRED", insert=("options_compute",), api=True),
    "rt.option_mark_evidence": _r("options", "DEFERRED", insert=("options_compute",), api=True),
    "rt.option_package_mark_revisions": _r("options", "DEFERRED", insert=("options_compute",), api=True),
    "rt.option_package_mark_inputs": _r("options", "DEFERRED", insert=("options_compute",), api=True),
    "rt.option_package_mark_current": _r("options", "BOUNDED", insert=("options_compute",),
                                         update=("options_compute",), api=True),
    "rt.option_strategy_admissions": _r("options", "BOUNDED", insert=("options_compute",), api=True),
    "rt.normalization_diagnostics": _r("options", "KEPT_3M", insert=("options_compute",), partitioned=True),
    # Options flow (opening flow and block detection): engine writes as options_compute.
    "ref.trade_condition_codes": _r("shared", insert=("references",), ingress_read=True, **_REF),
    "ref.exchange_codes": _r("shared", insert=("references",), ingress_read=True, **_REF),
    "ref.option_condition_semantics": _r("shared", insert=("references",), ingress_read=True, **_REF),
    "rt.option_flow_events": _r("options", "ARCHIVE_FIRST", insert=("options_compute",), partitioned=True,
                                api=True),
    "rt.option_flow_clusters": _r("options", "ARCHIVE_FIRST", insert=("options_compute",), partitioned=True,
                                  api=True),
    "rt.option_volume_checkpoints": _r("options", "HOT_TTL", insert=("options_compute",),
                                       update=("options_compute",), delete=("maintenance",)),
    "rt.local_mock_requests": _r("options", "OWNER_ONLY"),
    "rt.local_mock_request_inputs": _r("options", "OWNER_ONLY"),
    "rt.local_mock_dispositions": _r("options", "OWNER_ONLY"),
    "rt.local_mock_fills": _r("options", "OWNER_ONLY"),
    # Portal
    "rt.snapshot_catalog": _r("equity", "HOT_TTL", insert=("publication",), delete=("partition_admin",),
                              api=True, prune_read=True),
    "rt.portal_snapshot_hot": _r("equity", "HOT_TTL", insert=("publication",), partitioned=True, api=True),
    "rt.portal_snapshot_kept": _r("equity", "SESSION_WINDOW", insert=("publication",), partitioned=True, api=True),
    "rt.portal_releases": _r("equity", "KEPT_3M", insert=("publication",), partitioned=True, api=True),
    "rt.portal_release_current": _r("equity", "BOUNDED", insert=("publication",), update=("publication",), api=True),
    # Archive
    "rt.session_archives": _r("shared", "BOUNDED", insert=("maintenance",), prune_read=True),
}

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
        "owner_role": "owner",
        "capability_roles": list(CAPABILITY_ROLES),
        "principal_profiles": {key: list(value) for key, value in PRINCIPAL_PROFILES.items()},
        "public_access": "NONE",
        "object_grants": "RELATION_REGISTRY_V1",
    }


def security_policy_sha256() -> str:
    return canonical_sha256(security_policy())


def select_roles(spec: Relation) -> tuple[str, ...]:
    """Capability roles that hold SELECT on a relation."""
    if spec.retention == "OWNER_ONLY":
        return ()
    roles = list(DEFAULT_READ_ROLES)
    if spec.ingress_read:
        roles.extend(INGRESS_ROLES)
    if spec.api:
        roles.append("reader")
    if spec.prune_read:
        roles.append("partition_admin")
    return tuple(dict.fromkeys(roles))


def grants() -> dict[str, dict[str, tuple[str, ...]]]:
    """Return {relation: {privilege: roles}} derived from the registry."""
    result: dict[str, dict[str, tuple[str, ...]]] = {}
    for name, spec in RELATIONS.items():
        entry = {
            "SELECT": select_roles(spec),
            "INSERT": spec.insert,
            "UPDATE": spec.update,
            "DELETE": spec.delete,
        }
        result[name] = {key: value for key, value in entry.items() if value}
    return result
