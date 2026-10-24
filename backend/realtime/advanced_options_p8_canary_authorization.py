"""Inert authorization package and validators for one Advanced Options P8 canary."""
from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from options.config import load_valuation_policy

from .advanced_options_control_plane import (
    CANONICAL_FLOW_OWNER,
    CANONICAL_INGRESS_OWNER,
    CANONICAL_MARKET_STATE_OWNER,
)
from .advanced_options_flow_policy import load_advanced_options_flow_policy
from .advanced_options_input import (
    ADVANCED_OPTIONS_INPUT_READY_PLAN_VERSION,
    ADVANCED_OPTIONS_INPUT_VERSION,
)
from .advanced_options_mark_producer import (
    ADVANCED_NBBO_MARK_PLAN_VERSION,
    ADVANCED_NBBO_MAXIMUM_AGE_SECONDS,
)
from .advanced_options_native_loader import NATIVE_INPUT_PLAN_VERSION
from .advanced_options_package_evidence import FLOW_PACKAGE_EVIDENCE_VERSION
from .advanced_options_p8_selection_source import (
    P8_SELECTION_MAXIMUM_PROVIDER_CLOCK_SKEW_SECONDS,
    P8_SELECTION_POLICY,
)
from .advanced_options_projection import (
    OPTIONS_PAGE_CONTRACTS,
    OPTIONS_PAGE_SNAPSHOT_TYPES,
    OPTIONS_PROJECTION_TYPES,
)
from .advanced_options_provider_contract import (
    MASSIVE_OPTIONS_PROVIDER_CONTRACT,
    load_default_options_exchange_reference,
)
from .advanced_options_reference_producer import REFERENCE_PRODUCER_VERSION
from .advanced_options_rolling_producer import ROLLING_PRODUCER_VERSION
from .advanced_options_underlying_snapshot import (
    MAXIMUM_PRICE_AGE_SECONDS,
    PRICE_POLICY_SHA256,
    PRICE_SOURCE,
    REQUIRED_TIMEFRAME,
    UnderlyingSnapshotBinding,
    underlying_snapshot_binding_from_payload,
    underlying_snapshot_binding_payload,
)
from .advanced_options_service import (
    AdvancedOptionsResidentPolicy,
    OPTIONS_NON_SERVING_STAGE_ORDER,
)
from .contracts import EQUITY_VALIDATION_UNIVERSE
from .option_flow.conditions import load_default_live_semantics
from .provider_socket_lease import MASSIVE_OPTIONS_SOCKET_RESOURCE
from .stage_graph import STAGES_V4, STAGE_CATALOG_VERSION_V4


AUTHORIZATION_SCHEMA = "advanced-options-p8-live-canary-authorization-v5"
LIVE_OBSERVATION_SCHEMA = "advanced-options-p8-live-canary-observation-v3"
PAGE_RELEASE_OBSERVATION_SCHEMA = (
    "advanced-options-p8-page-release-observation-v2"
)
SELECTION_EVIDENCE_SCHEMA = "advanced-options-p8-activity-selection-v1"
MISSED_WINDOW_POLICY = "EXPIRE_WITHOUT_ROLLOVER_OR_REUSE"
QUALIFICATION_SEMANTIC_SHA256 = (
    "3aa845435af4523d1206b69214895413c495d8cebaae56a531e4bef0c6777bdb"
)
QUALIFICATION_FILE_SHA256 = (
    "56ed324ee9df11ea9019a7a2f955164e5447219f13b3e520545efdf181d0cbd5"
)
PREPARED_AUTHORITIES = (
    "provider_snapshot_selection",
    "websocket_ingress",
    "production_cohort_slot_enrollment",
    "resident_service_start",
    "stages_v4_execution",
    "canary_release_pointer",
)
RETAINED_DEVELOPER_HISTORY_COHORT_ID = (
    "d1baee6f-4ab2-543c-8a86-ce10f8952e69"
)
EQUITY_INPUT_MAXIMUM_AGE_SECONDS = 300
SOURCE_BINDING_PATHS = (
    "backend/realtime/advanced_options_p8_canary_authorization.py",
    "backend/realtime/advanced_options_p8_canary_executor.py",
    "backend/realtime/advanced_options_p8_docker.py",
    "backend/realtime/advanced_options_p8_production.py",
    "backend/realtime/advanced_options_p8_roles.py",
    "backend/realtime/advanced_options_p8_runtime.py",
    "backend/realtime/advanced_options_p8_window.py",
    "backend/realtime/advanced_options_p8_selection_source.py",
    "backend/realtime/advanced_options_underlying_snapshot.py",
    "backend/realtime/advanced_options_read_pin.py",
    "backend/scripts/prepare_advanced_options_p8_canary_authorization.py",
    "backend/scripts/audit_advanced_options_p8_canary_authorization.py",
    "backend/scripts/run_advanced_options_p8_canary.py",
    "backend/scripts/inspect_advanced_options_v3_package.py",
    "backend/scripts/run_realtime_v2_container.py",
    "backend/scripts/run_realtime_v2_options_enrollment.py",
    "docker-compose.realtime-v2.yml",
    "backend/realtime/advanced_options_p7_selection.py",
    "backend/realtime/advanced_options_provider_contract.py",
    "backend/realtime/advanced_equity_control_plane.py",
    "backend/realtime/advanced_options_control_plane.py",
    "backend/realtime/advanced_options_equity_input.py",
    "backend/realtime/advanced_options_service.py",
    "backend/realtime/provider_socket_lease.py",
    "backend/realtime/normalizer.py",
    "backend/realtime/stage_graph.py",
    "backend/realtime/advanced_options_input.py",
    "backend/realtime/advanced_options_reference_producer.py",
    "backend/realtime/advanced_options_rolling_producer.py",
    "backend/realtime/advanced_options_native_loader.py",
    "backend/realtime/advanced_options_mark_producer.py",
    "backend/realtime/advanced_options_loaders.py",
    "backend/realtime/advanced_options_runtime.py",
    "backend/realtime/advanced_options_v3_authority.py",
    "backend/realtime/advanced_options_v3_deployment.py",
    "backend/realtime/advanced_options_resident_barrier.py",
    "backend/realtime/advanced_options_cycle.py",
    "backend/realtime/advanced_options_package_evidence.py",
    "backend/realtime/advanced_options_projection.py",
    "backend/realtime/advanced_options_close.py",
    "backend/realtime/advanced_options_api.py",
    "backend/realtime/portal_publisher.py",
    "backend/realtime/portal_api.py",
    "backend/realtime/portal_compatibility_repository.py",
    "backend/realtime/relation_registry.py",
    "backend/realtime/equity_live024_compatibility.py",
    "backend/realtime/sql_v2/032_advanced_options_read_pins.sql",
    "backend/realtime/sql_v2/033_advanced_options_equity_input_bindings.sql",
    "backend/scripts/prepare_advanced_options_p8_equity_input_source.py",
    "backend/scripts/audit_advanced_options_input_readiness.py",
    "backend/realtime/screening_projection.py",
    "backend/realtime/screening_repository.py",
    "backend/realtime/stock_alert_projection.py",
    "backend/realtime/stock_alert_repository.py",
    "backend/realtime/stage_runners_alerts.py",
    "backend/scripts/run_realtime_v2_portal_api.py",
    "backend/realtime/policies/advanced_options_flow_v1.json",
    "backend/options/policies/valuation_advanced_nbbo_v1.json",
    "frontend/src/services/api.ts",
    "frontend/src/services/screening.ts",
    "frontend/src/services/stockDiscovery.ts",
    "frontend/src/pages/StockScreeningPage.tsx",
    "frontend/src/pages/StockAlertsPage.tsx",
    "frontend/src/pages/OptionsAlertsPage.tsx",
    "frontend/src/pages/OptionsActivityPage.tsx",
    "frontend/src/pages/OptionsFlowPage.tsx",
    "frontend/src/pages/OptionsScreeningWorkspace.tsx",
    "frontend/src/pages/OptionsScreenLibrary.tsx",
    "frontend/tests/stockAlertsRealtime.test.mjs",
    "frontend/tests/realtimePortal.test.mjs",
    "frontend/tests/dailyScannersRealtime.test.mjs",
    "frontend/tests/optionsAlertsRealtime.test.mjs",
    "frontend/tests/optionsFlowRealtime.test.mjs",
    "frontend/tests/optionsScreenerRealtime.test.mjs",
)
PROHIBITED_PROVIDER_ENV = (
    "MASSIVE_API_KEY",
    "POLYGON_API_KEY",
)
_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_OPTION_PATTERN = re.compile(
    r"^O:(?P<underlyer>[A-Z][A-Z0-9.-]{0,14})\d{6}[CP]\d{8}$"
)


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
    ).hexdigest()


def _sha256(value: object, name: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


def _commit(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("source_commit must be a string")
    digest = value.strip().lower()
    if _COMMIT_PATTERN.fullmatch(digest) is None:
        raise ValueError("source_commit must be a 40-character commit digest")
    return digest


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _timestamp(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a timestamp string")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{name} is not an ISO timestamp") from error
    return _utc(result, name)


def _load_object(path: Path, name: str) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{name} must contain a JSON object")
    return value


def _source_identity(
    repository_root: Path,
    source_files: Sequence[str],
) -> list[dict[str, object]]:
    root = repository_root.resolve()
    rows: list[dict[str, object]] = []
    normalized = tuple(source_files)
    if not normalized or len(normalized) != len(set(normalized)):
        raise ValueError("source binding paths must be unique and nonempty")
    for relative in normalized:
        path = (root / relative).resolve()
        try:
            path.relative_to(root)
        except ValueError as error:
            raise ValueError("source binding escaped the repository root") from error
        payload = path.read_bytes()
        rows.append({
            "path": relative.replace("\\", "/"),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "bytes": len(payload),
        })
    return rows


def source_binding_identity(
    repository_root: Path,
    source_files: Sequence[str],
) -> list[dict[str, object]]:
    return _source_identity(repository_root, source_files)


def _stage_payload() -> dict[str, object]:
    stages = [
        {
            "stage_id": stage.stage_id,
            "domain": stage.domain,
            "prerequisites": list(stage.prerequisites),
            "execution_lane": stage.execution_lane,
            "schedule_kind": stage.schedule_kind,
            "gates_strategy": stage.gates_strategy,
            "deadline_offset_seconds": stage.deadline_offset_seconds,
        }
        for stage in STAGES_V4.values()
    ]
    options = [
        row for row in stages
        if row["stage_id"] in OPTIONS_NON_SERVING_STAGE_ORDER
    ]
    if [row["stage_id"] for row in options] != list(
        OPTIONS_NON_SERVING_STAGE_ORDER
    ):
        raise ValueError("stages_v4 Options membership changed")
    return {
        "catalog_version": STAGE_CATALOG_VERSION_V4,
        "catalog": stages,
        "catalog_sha256": canonical_sha256(stages),
        "enabled_stage_ids": list(OPTIONS_NON_SERVING_STAGE_ORDER),
        "enabled_stage_definitions": options,
    }


def _contract_payload(
    repository_root: Path,
    *,
    cohort_id: UUID,
) -> dict[str, object]:
    backend = repository_root.resolve() / "backend"
    selection = P8_SELECTION_POLICY
    flow = load_advanced_options_flow_policy()
    semantics = load_default_live_semantics()
    exchange = load_default_options_exchange_reference()
    valuation = load_valuation_policy(
        backend / "options/policies/valuation_advanced_nbbo_v1.json"
    )
    resident = AdvancedOptionsResidentPolicy(
        "advanced-options-resident-v1"
    )
    resident_policy = {
        "version": resident.version,
        "heartbeat_interval_seconds": resident.heartbeat_interval_seconds,
        "stale_after_seconds": resident.stale_after_seconds,
        "restart_initial_backoff_seconds": (
            resident.restart_initial_backoff_seconds
        ),
        "restart_maximum_backoff_seconds": (
            resident.restart_maximum_backoff_seconds
        ),
        "restart_failure_limit": resident.restart_failure_limit,
        "restart_failure_window_seconds": (
            resident.restart_failure_window_seconds
        ),
        "shutdown_deadline_seconds": resident.shutdown_deadline_seconds,
        "stage_subset": sorted(resident.stage_subset),
    }
    resident_policy_sha256 = canonical_sha256(resident_policy)
    cohort_template = {
        "cohort_id": str(cohort_id),
        "kind": "MARKET_OPTIONS",
        "mode": "CANARY",
        "provider_tier": "MASSIVE_ADVANCED",
        "underlyings": ["QQQ", "SPY"],
        "contracts_per_underlying": 1,
        "contract_count": 2,
        "channels": ["T", "Q"],
        "slot_kind": "PRE_SESSION",
        "slot_version": 1,
        "slot_purpose": "PRIMARY",
        "slot_allocation": 100,
        "wildcards": False,
        "contract_substitution": False,
        "scope_expansion": False,
        "required_detectors": ["O1", "O2", "O3"],
        "o2_policy": "UNAVAILABLE/O2_MODEL_NOT_ACCEPTED",
    }
    policy_hashes = {
        "provider_contract_sha256": MASSIVE_OPTIONS_PROVIDER_CONTRACT.sha256,
        "condition_semantics_sha256": semantics.semantics_sha256,
        "condition_reference_sha256": semantics.pin.payload_sha256,
        "exchange_reference_sha256": exchange.sha256,
        "activity_selection_policy_sha256": selection.sha256,
        "underlying_price_policy_sha256": PRICE_POLICY_SHA256,
        "flow_policy_sha256": flow.sha256,
        "rolling_policy_sha256": canonical_sha256({
            "producer_version": ROLLING_PRODUCER_VERSION,
            "required_owners": [
                CANONICAL_MARKET_STATE_OWNER,
                CANONICAL_FLOW_OWNER,
            ],
        }),
        "native_policy_sha256": canonical_sha256({
            "plan_version": NATIVE_INPUT_PLAN_VERSION,
            "detectors": ["O1", "O2", "O3", "S1", "S2"],
            "o2_policy": "UNAVAILABLE/O2_MODEL_NOT_ACCEPTED",
        }),
        "mark_policy_sha256": canonical_sha256({
            "plan_version": ADVANCED_NBBO_MARK_PLAN_VERSION,
            "maximum_source_age_seconds": (
                ADVANCED_NBBO_MAXIMUM_AGE_SECONDS
            ),
            "valuation_policy_sha256": valuation.policy_sha256,
        }),
        "valuation_policy_sha256": valuation.policy_sha256,
        "package_policy_sha256": canonical_sha256({
            "evidence_version": FLOW_PACKAGE_EVIDENCE_VERSION,
            "immutable": True,
            "atomic_with_cycle": True,
        }),
        "cohort_template_sha256": canonical_sha256(cohort_template),
        "resident_policy_sha256": resident_policy_sha256,
    }
    profile = {
        "contract": "advanced-options-p8-canary-profile-v1",
        "provider_tier": "MASSIVE_ADVANCED",
        "feed": "OPTIONS",
        "channels": ["T", "Q"],
        "cohort_template_sha256": policy_hashes[
            "cohort_template_sha256"
        ],
        "policy_hashes": policy_hashes,
    }
    return {
        "policy_hashes": policy_hashes,
        "cohort_template": cohort_template,
        "profile": {
            **profile,
            "profile_sha256": canonical_sha256(profile),
        },
        "resident_policy": {
            **resident_policy,
            "sha256": resident_policy_sha256,
        },
    }


def _authority_payload(
    *,
    stored_reads_prepared: bool = False,
) -> dict[str, object]:
    result = {
        name: {
            "prepared": True,
            "owner_granted": False,
            "executed": False,
        }
        for name in PREPARED_AUTHORITIES
    }
    result.update({
        "stored_reads": {
            "prepared": stored_reads_prepared,
            "owner_granted": False,
            "enabled": False,
        },
        "dispatch_delivery": {
            "prepared": False,
            "owner_granted": False,
            "dispatch": False,
            "delivery": False,
        },
        "broker_trading": {
            "prepared": False,
            "owner_granted": False,
            "broker": False,
            "trading": False,
        },
    })
    return result


def _equity_input_source(
    value: Mapping[str, object],
    *,
    target_options_window_id: UUID,
    target_cohort_id: UUID,
    window_start: datetime,
    authorization_reference: str,
) -> dict[str, object]:
    required = {
        "source_cohort_id",
        "source_cohort_sha256",
        "source_manifest_id",
        "source_manifest_sha256",
        "source_profile_id",
        "source_profile_sha256",
        "equity_window_id",
        "equity_receipt_id",
        "equity_cutoff_utc",
        "reference_generation",
        "universe_sha256",
        "input_sha256",
        "build_id",
        "symbols",
    }
    if not isinstance(value, Mapping) or set(value) != required:
        raise ValueError("P8 Equity input source fields changed")
    source_cohort_id = UUID(str(value["source_cohort_id"]))
    source_manifest_id = UUID(str(value["source_manifest_id"]))
    source_profile_id = UUID(str(value["source_profile_id"]))
    equity_window_id = UUID(str(value["equity_window_id"]))
    equity_receipt_id = UUID(str(value["equity_receipt_id"]))
    equity_cutoff = _utc(
        datetime.fromisoformat(str(value["equity_cutoff_utc"])),
        "equity_cutoff_utc",
    )
    reference_generation = value["reference_generation"]
    symbols = tuple(sorted(str(item) for item in value["symbols"]))
    if (
        source_cohort_id.int == 0
        or source_cohort_id == target_cohort_id
        or source_manifest_id.int == 0
        or source_profile_id.int == 0
        or equity_window_id.int == 0
        or equity_receipt_id.int == 0
        or type(reference_generation) is not int
        or reference_generation <= 0
        or equity_cutoff.date() != window_start.date()
        or equity_cutoff > window_start
        or (
            window_start - equity_cutoff
        ).total_seconds() > EQUITY_INPUT_MAXIMUM_AGE_SECONDS
        or symbols != EQUITY_VALIDATION_UNIVERSE.equity_tickers
        or not str(value["build_id"]).strip()
    ):
        raise ValueError("P8 Equity input source scope changed")
    result = {
        "contract_version": "advanced-options-equity-input-binding-v2",
        "target_options_window_id": str(target_options_window_id),
        "target_cohort_id": str(target_cohort_id),
        "source_cohort_id": str(source_cohort_id),
        "source_cohort_sha256": _sha256(
            str(value["source_cohort_sha256"]),
            "source_cohort_sha256",
        ),
        "source_manifest_id": str(source_manifest_id),
        "source_manifest_sha256": _sha256(
            str(value["source_manifest_sha256"]),
            "source_manifest_sha256",
        ),
        "source_profile_id": str(source_profile_id),
        "source_profile_sha256": _sha256(
            str(value["source_profile_sha256"]),
            "source_profile_sha256",
        ),
        "equity_window_id": str(equity_window_id),
        "equity_receipt_id": str(equity_receipt_id),
        "equity_cutoff": equity_cutoff.isoformat(),
        "reference_generation": reference_generation,
        "universe_sha256": _sha256(
            str(value["universe_sha256"]),
            "universe_sha256",
        ),
        "input_sha256": _sha256(
            str(value["input_sha256"]),
            "input_sha256",
        ),
        "build_id": str(value["build_id"]).strip(),
        "maximum_age_seconds": EQUITY_INPUT_MAXIMUM_AGE_SECONDS,
        "authorization_reference": authorization_reference,
        "symbols": list(symbols),
    }
    binding = {
        key: result[key]
        for key in result
        if key != "symbols"
    }
    result["binding_sha256"] = canonical_sha256(binding)
    return result


def build_p8_canary_authorization(
    qualification_report_path: Path,
    *,
    repository_root: Path,
    source_commit: str,
    authorization_reference: str,
    package_nonce: UUID | str,
    prepared_at: datetime,
    window_start: datetime,
    window_end: datetime,
    equity_input_source: Mapping[str, object],
    expected_qualification_semantic_sha256: str = (
        QUALIFICATION_SEMANTIC_SHA256
    ),
    expected_qualification_file_sha256: str = QUALIFICATION_FILE_SHA256,
    source_files: Sequence[str] = SOURCE_BINDING_PATHS,
    supersedes_package_sha256: str | None = None,
    stored_reads_prepared: bool = False,
) -> dict[str, object]:
    report_path = qualification_report_path.resolve()
    report = _load_object(report_path, "P8 qualification report")
    report_file_sha256 = hashlib.sha256(report_path.read_bytes()).hexdigest()
    semantic_report = dict(report)
    embedded_report_sha256 = semantic_report.pop("report_sha256", None)
    report_semantic_sha256 = canonical_sha256(semantic_report)
    expected_semantic = _sha256(
        expected_qualification_semantic_sha256,
        "expected_qualification_semantic_sha256",
    )
    expected_file = _sha256(
        expected_qualification_file_sha256,
        "expected_qualification_file_sha256",
    )
    if (
        report.get("verdict") != "PASS"
        or embedded_report_sha256 != expected_semantic
        or report_semantic_sha256 != expected_semantic
        or report_file_sha256 != expected_file
    ):
        raise ValueError("P8 qualification report identity or verdict changed")
    reference = authorization_reference.strip()
    if not reference or len(reference) > 160:
        raise ValueError("authorization_reference must be 1 to 160 characters")
    try:
        nonce = package_nonce if isinstance(
            package_nonce, UUID
        ) else UUID(str(package_nonce))
    except ValueError as error:
        raise ValueError("package_nonce must be a UUID") from error
    if nonce.int == 0:
        raise ValueError("package_nonce cannot be nil")
    prepared = _utc(prepared_at, "prepared_at")
    start = _utc(window_start, "window_start")
    stop = _utc(window_end, "window_end")
    if start <= prepared or stop - start != timedelta(minutes=10):
        raise ValueError(
            "P8 canary window must be prospective and exactly ten minutes"
        )
    if start.date() != stop.date():
        raise ValueError("P8 canary window must stay inside one UTC session")
    if type(stored_reads_prepared) is not bool:
        raise TypeError("stored_reads_prepared must be boolean")
    commit = _commit(source_commit)
    cohort_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:{reference}:{nonce}:MARKET_OPTIONS",
    )
    plan_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:{reference}:{nonce}:stages_v4",
    )
    target_options_window_id = uuid5(
        NAMESPACE_URL,
        f"p8-window:{nonce}",
    )
    contract = _contract_payload(
        repository_root,
        cohort_id=cohort_id,
    )
    equity_input_binding = _equity_input_source(
        equity_input_source,
        target_options_window_id=target_options_window_id,
        target_cohort_id=cohort_id,
        window_start=start,
        authorization_reference=reference,
    )
    superseded = (
        None
        if supersedes_package_sha256 is None
        else _sha256(
            supersedes_package_sha256,
            "supersedes_package_sha256",
        )
    )
    stage_payload = _stage_payload()
    owners = [
        {
            "kind": "INGRESS",
            "owner_name": CANONICAL_INGRESS_OWNER,
            "lease_resource_key": MASSIVE_OPTIONS_SOCKET_RESOURCE,
            "host_lock_required": True,
        },
        {
            "kind": "MARKET_STATE",
            "owner_name": CANONICAL_MARKET_STATE_OWNER,
            "lease_resource_key": (
                f"advanced-options:{cohort_id}:{start.date()}:market-state"
            ),
            "host_lock_required": False,
        },
        {
            "kind": "FLOW",
            "owner_name": CANONICAL_FLOW_OWNER,
            "lease_resource_key": (
                f"advanced-options:{cohort_id}:{start.date()}:flow"
            ),
            "host_lock_required": False,
        },
        {
            "kind": "WINDOW",
            "owner_name": "advanced-options-window-v1",
            "lease_resource_key": (
                f"advanced-options:{cohort_id}:{start.date()}:window"
            ),
            "host_lock_required": False,
        },
    ]
    evidence_stem = (
        "advanced-options-p8-live-canary-"
        f"{start.strftime('%Y%m%dT%H%M%SZ')}-{str(nonce)[:8]}"
    )
    package: dict[str, object] = {
        "schema": AUTHORIZATION_SCHEMA,
        "state": "PREPARED_UNARMED_UNCONSUMED",
        "authorization_reference": reference,
        "package_nonce": str(nonce),
        "prepared_at_utc": prepared.isoformat(),
        "source": {
            "commit": commit,
            "files": _source_identity(repository_root, source_files),
        },
        "qualification": {
            "report_semantic_sha256": report_semantic_sha256,
            "report_file_sha256": report_file_sha256,
            "verdict": "PASS",
        },
        "window": {
            "session_date": start.date().isoformat(),
            "start_utc": start.isoformat(),
            "stop_utc": stop.isoformat(),
            "missed_window_policy": MISSED_WINDOW_POLICY,
            "rollover_authorized": False,
            "reuse_authorized": False,
        },
        "equity_input_binding": equity_input_binding,
        "supersession": (
            None
            if superseded is None
            else {
                "package_sha256": superseded,
                "reason": "SOURCE_DRIFT",
                "reuse_authorized": False,
            }
        ),
        "provider_selection": {
            "evidence_schema": SELECTION_EVIDENCE_SCHEMA,
            "admission_role": "POST_E_ACTIVITY_RANKED_LATE_CANARY",
            "policy": {
                "version": P8_SELECTION_POLICY.version,
                "sha256": P8_SELECTION_POLICY.sha256,
                "review_cadence": "BEFORE_EACH_RUN",
                "maximum_snapshot_age_seconds": 300,
                "maximum_provider_clock_skew_seconds": (
                    P8_SELECTION_MAXIMUM_PROVIDER_CLOCK_SKEW_SECONDS
                ),
            },
            "underlyings": ["QQQ", "SPY"],
            "contracts_per_underlying": 1,
            "rest_budget": {
                "requests": 2,
                "decoded_bytes": 100_000_000,
                "pages": 2,
                "pagination_requests": 0,
                "lanes": ["OPTIONS_CHAIN"],
            },
            "fresh_evidence_required": True,
            "selection_result_bound_by_live_preflight": True,
        },
        "v3_authority_classification": {
            "path": "POST_E_ACTIVITY_RANKED_LATE_CANARY",
            "prospective_opening_admission": {
                "status": "UNAVAILABLE",
                "authority_id": None,
                "reason_code": (
                    "POST_E_SELECTION_CANNOT_BIND_PREOPEN_CONTRACTS"
                ),
            },
            "session_acquisition_authority": {
                "status": "UNAVAILABLE",
                "authority_id": None,
                "reason_code": (
                    "EXACT_PREOPEN_SESSION_AUTHORITY_NOT_PREPARED"
                ),
            },
            "window_cycle_authority": {
                "status": "UNAVAILABLE",
                "authority_id": None,
                "reason_code": (
                    "LEGACY_SINGLE_PACKAGE_CANARY_AUTHORITY"
                ),
            },
            "opening_coverage_claimed": False,
            "accumulation_900_second_coverage_claimed": False,
        },
        "websocket_ingress": {
            "provider": "massive",
            "account_alias": "MASSIVE_ADVANCED",
            "product": "US_OPTIONS_ADVANCED_REALTIME",
            "feed": "OPTIONS",
            "channels": ["T", "Q"],
            "socket_owner": CANONICAL_INGRESS_OWNER,
            "lease_scope": "HOST_LOCAL",
            "lease_resource_key": MASSIVE_OPTIONS_SOCKET_RESOURCE,
            "stream_sessions": 1,
            "connection_attempts": 6,
            "reconnects": 5,
            "websocket_frames": 25_000,
            "decoded_messages": 100_000,
            "decoded_bytes": 32 * 1024 * 1024,
            "maximum_frame_bytes": 512 * 1024,
            "library_max_queue": 8,
            "open_timeout_seconds": 10,
            "ping_interval_seconds": 20,
            "ping_timeout_seconds": 10,
            "close_timeout_seconds": 5,
            "reconnect_backoff": {
                "initial_seconds": 5,
                "minimum_seconds": 5,
                "multiplier": 2,
                "maximum_seconds": 60,
                "jitter_ceiling_seconds": 60,
            },
            "wildcard_subscriptions": False,
        },
        "cohort": {
            **contract["cohort_template"],
            "template_sha256": contract[
                "policy_hashes"
            ]["cohort_template_sha256"],
            "profile": contract["profile"],
            "realized_manifest_bound_by_live_preflight": True,
            "equity_entries_preserved": True,
        },
        "history_binding": {
            "target_cohort_id": str(cohort_id),
            "source_cohort_id": RETAINED_DEVELOPER_HISTORY_COHORT_ID,
            "source_provider_tier": "MASSIVE_DEVELOPER",
            "history_before_utc": start.isoformat(),
            "immutable_history_only": True,
            "detector_ready_required": False,
            "source_manifest_bound_by_live_preflight": True,
            "required_underlyings": ["QQQ", "SPY"],
            "required_adjusted_daily_sessions": 31,
            "required_raw_daily_sessions": 15,
            "required_sessions": [
                (start.date() - timedelta(days=2)).isoformat(),
                (start.date() - timedelta(days=1)).isoformat(),
            ],
        },
        "underlying_price_binding": {
            "required": True,
            "bound_by_live_preflight": True,
            "source": PRICE_SOURCE,
            "provider_request_count": 2,
            "additional_provider_requests": 0,
            "underlyings": ["QQQ", "SPY"],
            "required_timeframe": REQUIRED_TIMEFRAME,
            "maximum_price_age_seconds": MAXIMUM_PRICE_AGE_SECONDS,
            "policy_sha256": PRICE_POLICY_SHA256,
            "selection_sha256_bound": True,
            "source_snapshot_sha256_bound": True,
            "no_equity_service_required": True,
            "no_equity_tick_or_bar_fallback": True,
        },
        "owners": {
            "host_socket_lock_count": 1,
            "resident_owner_count": 4,
            "processes": owners,
            "resident_policy": contract["resident_policy"],
            "running_peer_owner_allowed": False,
        },
        "orchestration": {
            "plan_id": str(plan_id),
            **stage_payload,
            "build_identities": {
                "reference": REFERENCE_PRODUCER_VERSION,
                "input_contract": ADVANCED_OPTIONS_INPUT_VERSION,
                "input_plan": ADVANCED_OPTIONS_INPUT_READY_PLAN_VERSION,
                "rolling": ROLLING_PRODUCER_VERSION,
                "native": NATIVE_INPUT_PLAN_VERSION,
                "mark": ADVANCED_NBBO_MARK_PLAN_VERSION,
                "package": FLOW_PACKAGE_EVIDENCE_VERSION,
                "cycle": "advanced-options-p8-live-canary-v1",
                "projection": "advanced-options-p8-live-canary-v1",
                "publish": "portal-publisher-p8-canary-v1",
            },
        },
        "policy_hashes": contract["policy_hashes"],
        "publication": {
            "release_scope": "COHORT_LOCAL",
            "pointer_scope": "COHORT_LOCAL",
            "publisher_owner": "portal-publisher",
            "projection_types": list(OPTIONS_PROJECTION_TYPES),
            "page_artifacts": [
                {
                    "artifact_kind": kind,
                    "snapshot_type": OPTIONS_PAGE_SNAPSHOT_TYPES[kind],
                    "contract": OPTIONS_PAGE_CONTRACTS[kind],
                }
                for kind in OPTIONS_PAGE_SNAPSHOT_TYPES
            ],
            "manifest_entry_count": 8,
            "preserve_equity_entries": True,
            "dispatch_authorized": False,
            "page_contracts": [
                {
                    "page": "STOCK_SCREENER",
                    "artifact_kind": "SCREENING_GENERATION",
                    "route": "/api/stocks/screening/*",
                    "presentation": "StockScreeningPage",
                },
                {
                    "page": "STOCK_ALERTS",
                    "artifact_kind": "STOCK_ALERTS_V1/PLATFORM",
                    "route": "/api/stocks/alert-*",
                    "presentation": "StockAlertsPage",
                },
                {
                    "page": "OPTIONS_ACTIVITY",
                    "artifact_kind": "FLOW",
                    "route": "/api/options/flow",
                    "presentation": "OptionsActivityPage",
                },
                {
                    "page": "OPTIONS_FLOW",
                    "artifact_kind": "FLOW",
                    "route": "/api/options/flow",
                    "presentation": "OptionsFlowPage",
                },
                {
                    "page": "OPTIONS_SCREENER",
                    "artifact_kind": "SCREENER",
                    "route": (
                        "/api/options/discovery-catalog+"
                        "/api/options/eligible-chain/query"
                    ),
                    "presentation": "OptionsScreeningWorkspace",
                },
                {
                    "page": "OPTIONS_ALERTS",
                    "artifact_kind": "ALERTS",
                    "route": "/api/options/alerts/*",
                    "presentation": "OptionsAlertsPage",
                },
            ],
            "page_contract_policy": {
                "exact_existing_page_designs": True,
                "explicit_cohort_required": True,
                "same_portal_release_scope_required": True,
                "request_time_provider_or_detector_fallback": False,
                "portal_projection_snapshots_are_diagnostics_only": True,
            },
        },
        "stored_reads": {
            "authority_prepared": stored_reads_prepared,
            "backend_flag": "REALTIME_V2_ADVANCED_OPTIONS_READS=ENABLED",
            "frontend_flag": "VITE_REALTIME_OPTIONS_V2=true",
            "required_cohort_id": str(cohort_id),
            "release_pinned": True,
            "provider_fetch_performed": False,
            "detector_work_performed": False,
            "publication_performed": False,
        },
        "alert_shape": {
            "event_count": 1,
            "state_count": 1,
            "current_alert_count": 1,
            "outbox_count": 1,
            "outbox_state": "PENDING",
            "outbox_claimed": 0,
            "outbox_dispatched": 0,
            "outbox_delivered": 0,
            "outbox_abandoned": 0,
            "destination": "PORTAL",
            "dispatch_authorized": False,
            "delivery_authorized": False,
            "broker_authorized": False,
            "trade_authorized": False,
        },
        "acceptance": {
            "resource": {
                "maximum_cpu_percent_per_owner": 80,
                "maximum_rss_bytes_per_owner": 1_073_741_824,
                "minimum_disk_free_percent": 20,
                "maximum_database_connections": 12,
            },
            "lag": {
                "maximum_checkpoint_offset_lag": 0,
                "maximum_checkpoint_age_seconds": 5,
                "maximum_end_to_end_p99_ms": 500,
                "maximum_end_to_end_ms": 5_000,
            },
            "queue": {
                "buffer_capacity_events": 2_048,
                "abort_buffer_events": 1_536,
                "maximum_buffer_bytes": 32 * 1024 * 1024,
                "abort_buffer_bytes": 24 * 1024 * 1024,
                "abort_oldest_age_seconds": 5,
                "final_backlog": 0,
            },
            "loss": {
                "received_equals_durable_frames": True,
                "durable_equals_accounted_frames": True,
                "unresolved_gaps": 0,
                "quarantined_messages": 0,
                "dropped_messages": 0,
                "buffer_full_count": 0,
            },
            "restart": {
                "maximum_canary_restarts_total": 1,
                "maximum_owner_restart_exhaustions": 0,
                "restore_mode": "RESTORED",
            },
            "replay": {
                "journal_replay_equal": True,
                "projection_replay_equal": True,
                "release_replay_claimed": 0,
                "stale_release_fenced": True,
            },
        },
        "abort_gates": [
            "STALE_OR_MISSING_ACTIVITY_SELECTION",
            "SOURCE_OR_CONFIG_IDENTITY_DRIFT",
            "UNIVERSE_CONTRACT_STAGE_OR_BUDGET_EXPANSION",
            "EQUITY_WINDOW_OVERLAP_OR_UNKNOWN",
            "RUNNING_OR_AMBIGUOUS_PEER_OWNER",
            "RETENTION_DEPENDENCY",
            "CHECKPOINT_LAG_QUEUE_LOSS_OR_RESOURCE_LIMIT",
            "RESTART_OR_REPLAY_FAILURE",
            "RELEASE_OR_POINTER_DIVERGENCE",
            "PAGE_FORWARD_RELEASE_MISSING_OR_SCOPE_DRIFT",
            "ALERT_OUTBOX_CLAIM_DISPATCH_OR_DELIVERY",
            "BROKER_OR_TRADE_AUTHORITY",
            "REUSED_EXPIRED_OR_AMBIGUOUS_PACKAGE",
        ],
        "rollback": {
            "kill_switch_order": [
                "DISABLE_EXACT_COHORT_STORED_READ_FLAGS_IF_SEPARATELY_ENABLED",
                "REFUSE_NEW_STAGES_V4_WINDOWS",
                "RESTORE_PRIOR_COHORT_LOCAL_OPTIONS_POINTER",
                "STOP_EXACT_FOUR_RESIDENT_OWNERS",
                "CLOSE_EXACT_OPTIONS_SOCKET",
                "RELEASE_EXACT_HOST_LOCK",
            ],
            "preserve_equity_release_entries": True,
            "production_data_delete_authorized": False,
            "repair_backfill_replay_authorized": False,
            "retention_lifecycle_authorized": False,
        },
        "cleanup": {
            "evidence_directory": (
                f"backend/.realtime-canary/{evidence_stem}"
            ),
            "lock_path": (
                f"backend/.realtime-canary/{evidence_stem}.lock"
            ),
            "consumption_receipt_path": (
                f"backend/.realtime-canary/{evidence_stem}.receipt.json"
            ),
            "remove_owned_temporary_files": True,
            "remove_owned_host_lock": True,
            "stop_owned_resident_processes": True,
            "preserve_production_database_rows": True,
            "preserve_release_evidence": True,
            "preserve_original_timestamps": True,
            "equity_cleanup_authorized": False,
            "retention_cleanup_authorized": False,
            "v3_authority_classification_carried": True,
        },
        "authorities": _authority_payload(
            stored_reads_prepared=stored_reads_prepared,
        ),
        "retention_dependency": {
            "required": False,
            "package_references": [],
            "lifecycle_actions_authorized": False,
            "reconciliation_authorized": False,
        },
        "owner_inputs_required": [
            "Approve the exact package hash and only the six prepared authorities.",
            "Provide fresh two-request QQQ/SPY activity-selection evidence within five minutes of the window.",
            "Confirm the exact two selected contracts and derived cohort/profile/manifest/slot hashes.",
            "Provide read-only proof of no overlapping Equity window and no running or ambiguous peer owner.",
            "Accept the host-local socket fence limitation and all resource, lag, queue, loss, restart and replay thresholds.",
            "Provide the current source commit, source hashes, stages_v4 identity and production configuration hashes.",
            "Provide an empty, unambiguous prior-run/consumption observation for this nonce and package.",
            "Identify the prior cohort-local Options pointer for rollback while preserving Equity entries.",
            "Decide separately whether exact-cohort backend/frontend stored reads may be enabled.",
            "Keep dispatch/delivery and broker/trading authority denied.",
        ],
        "automatic_execution_available": False,
        "guarded_one_use_execution_available": True,
    }
    package["package_sha256"] = canonical_sha256(package)
    return package


def validate_p8_canary_authorization(
    package: object,
    *,
    qualification_report_path: Path,
    repository_root: Path,
    current_source_commit: str,
    expected_package_sha256: str,
    expected_qualification_semantic_sha256: str = (
        QUALIFICATION_SEMANTIC_SHA256
    ),
    expected_qualification_file_sha256: str = QUALIFICATION_FILE_SHA256,
    source_files: Sequence[str] = SOURCE_BINDING_PATHS,
) -> dict[str, object]:
    if not isinstance(package, dict):
        raise TypeError("P8 canary authorization must be a JSON object")
    expected_package = _sha256(
        expected_package_sha256,
        "expected_package_sha256",
    )
    unsigned = dict(package)
    actual_package = unsigned.pop("package_sha256", None)
    if (
        actual_package != expected_package
        or canonical_sha256(unsigned) != expected_package
    ):
        raise ValueError("P8 canary authorization package hash changed")
    window = package.get("window")
    if not isinstance(window, dict):
        raise ValueError("P8 canary authorization window is unavailable")
    equity_input = package.get("equity_input_binding")
    if not isinstance(equity_input, Mapping):
        raise ValueError(
            "P8 canary Equity input binding is unavailable"
        )
    expected = build_p8_canary_authorization(
        qualification_report_path,
        repository_root=repository_root,
        source_commit=current_source_commit,
        authorization_reference=str(
            package.get("authorization_reference", "")
        ),
        package_nonce=str(package.get("package_nonce", "")),
        prepared_at=_timestamp(
            package.get("prepared_at_utc"),
            "prepared_at_utc",
        ),
        window_start=_timestamp(
            window.get("start_utc"),
            "window.start_utc",
        ),
        window_end=_timestamp(
            window.get("stop_utc"),
            "window.stop_utc",
        ),
        equity_input_source={
            "source_cohort_id": equity_input["source_cohort_id"],
            "source_cohort_sha256": equity_input[
                "source_cohort_sha256"
            ],
            "source_manifest_id": equity_input["source_manifest_id"],
            "source_manifest_sha256": equity_input[
                "source_manifest_sha256"
            ],
            "source_profile_id": equity_input["source_profile_id"],
            "source_profile_sha256": equity_input[
                "source_profile_sha256"
            ],
            "equity_window_id": equity_input["equity_window_id"],
            "equity_receipt_id": equity_input["equity_receipt_id"],
            "equity_cutoff_utc": equity_input["equity_cutoff"],
            "reference_generation": equity_input[
                "reference_generation"
            ],
            "universe_sha256": equity_input["universe_sha256"],
            "input_sha256": equity_input["input_sha256"],
            "build_id": equity_input["build_id"],
            "symbols": equity_input["symbols"],
        },
        expected_qualification_semantic_sha256=(
            expected_qualification_semantic_sha256
        ),
        expected_qualification_file_sha256=(
            expected_qualification_file_sha256
        ),
        source_files=source_files,
        supersedes_package_sha256=(
            package["supersession"]["package_sha256"]
            if isinstance(package.get("supersession"), Mapping)
            else None
        ),
        stored_reads_prepared=bool(
            package["stored_reads"]["authority_prepared"]
        ),
    )
    if package != expected:
        raise ValueError(
            "P8 source, config, universe, stage, contract or authority drifted"
        )
    return package


def p8_canary_plan(package: Mapping[str, object]) -> dict[str, object]:
    return {
        "mode": "PLAN",
        "status": "PASS_INERT_EXACT_P8_CANARY_PLAN",
        "package_sha256": package["package_sha256"],
        "package_nonce": package["package_nonce"],
        "window": package["window"],
        "v3_authority_classification": package[
            "v3_authority_classification"
        ],
        "prepared_authorities": list(PREPARED_AUTHORITIES),
        "stored_reads_prepared": bool(
            package["stored_reads"]["authority_prepared"]
        ),
        "dispatch_delivery_authorized": False,
        "broker_trading_authorized": False,
        "automatic_execution_available": False,
        "guarded_one_use_execution_available": True,
        "provider_requests": 0,
        "external_socket_connections": 0,
        "production_database_writes": 0,
        "service_lifecycle_actions": 0,
        "stages_v4_runs": 0,
        "releases_created": 0,
        "pointers_changed": 0,
        "stored_read_flags_changed": 0,
        "dispatches": 0,
        "deliveries": 0,
        "broker_actions": 0,
        "trades": 0,
    }


def p8_canary_status(
    package: Mapping[str, object],
    *,
    now: datetime,
    prior_run_records: Sequence[Mapping[str, object]] | None,
) -> dict[str, object]:
    current = _utc(now, "now")
    prepared = _timestamp(package["prepared_at_utc"], "prepared_at_utc")
    start = _timestamp(package["window"]["start_utc"], "window.start_utc")
    stop = _timestamp(package["window"]["stop_utc"], "window.stop_utc")
    if current < prepared:
        raise ValueError("P8 canary package predates the status clock")
    if prior_run_records is None:
        state = "AMBIGUOUS_PRIOR_RUN"
        detail = "prior-run evidence was not supplied"
    elif not isinstance(prior_run_records, Sequence) or isinstance(
        prior_run_records, (str, bytes)
    ):
        raise TypeError("prior_run_records must be a sequence")
    else:
        matching = [
            row for row in prior_run_records
            if isinstance(row, Mapping)
            and (
                row.get("package_nonce") == package["package_nonce"]
                or row.get("package_sha256") == package["package_sha256"]
                or row.get("authorization_reference")
                == package["authorization_reference"]
            )
        ]
        if matching:
            if len(matching) == 1 and matching[0].get(
                "state"
            ) == "CONSUMED_CLEAN":
                state = "CONSUMED"
                detail = "one completed consumption record exists"
            else:
                state = "AMBIGUOUS_PRIOR_RUN"
                detail = "prior package execution evidence is not uniquely clean"
        elif current >= stop:
            state = "EXPIRED_UNCONSUMED"
            detail = "the missed window expired without rollover or reuse"
        elif current < start:
            state = "PREPARED_UNARMED_UNCONSUMED"
            detail = "the exact canary window remains prospective"
        else:
            state = "ACTIVE_UNCONSUMED"
            detail = "the exact canary window is active and unconsumed"
    return {
        "mode": "STATUS",
        "state": state,
        "detail": detail,
        "package_sha256": package["package_sha256"],
        "package_nonce": package["package_nonce"],
        "window": package["window"],
        "v3_authority_classification": package[
            "v3_authority_classification"
        ],
        "automatic_execution_available": False,
        "guarded_one_use_execution_available": True,
        "provider_requests": 0,
        "external_socket_connections": 0,
        "production_writes": 0,
        "service_lifecycle_actions": 0,
        "dispatches": 0,
        "deliveries": 0,
        "broker_actions": 0,
        "trades": 0,
    }


def provider_free_p8_canary_preflight(
    package: Mapping[str, object],
    *,
    repository_root: Path,
    now: datetime,
    prior_run_records: Sequence[Mapping[str, object]] | None,
    environ: Mapping[str, str] | None = None,
    allow_active: bool = False,
    allow_execution_artifacts: bool = False,
    allow_provider_credentials: bool = False,
) -> dict[str, object]:
    current = _utc(now, "now")
    status = p8_canary_status(
        package,
        now=current,
        prior_run_records=prior_run_records,
    )
    accepted_states = {"PREPARED_UNARMED_UNCONSUMED"}
    if allow_active:
        accepted_states.add("ACTIVE_UNCONSUMED")
    if status["state"] not in accepted_states:
        raise ValueError(
            f"P8 canary package is not prospective: {status['state']}"
        )
    environment = os.environ if environ is None else environ
    present = sorted(
        name for name in PROHIBITED_PROVIDER_ENV
        if environment.get(name)
    )
    if present and not allow_provider_credentials:
        raise RuntimeError(
            "provider-free P8 preflight received provider credentials: "
            + ",".join(present)
        )
    for key in (
        "evidence_directory",
        "lock_path",
        "consumption_receipt_path",
    ):
        path = (
            repository_root.resolve()
            / str(package["cleanup"][key])
        ).resolve()
        try:
            path.relative_to(repository_root.resolve())
        except ValueError as error:
            raise ValueError("P8 cleanup target escaped the repository") from error
        if path.exists() and not (
            allow_execution_artifacts
            and key in {"lock_path", "consumption_receipt_path"}
        ):
            raise FileExistsError(
                f"P8 {key} already exists and makes prior state ambiguous"
            )
    return {
        "mode": "PREFLIGHT",
        "status": "PASS_PROVIDER_FREE_P8_CANARY_PREFLIGHT",
        "package_status": status,
        "package_sha256": package["package_sha256"],
        "window_status": "PROSPECTIVE",
        "live_observation_validated": False,
        "live_readiness": "NOT_EVALUATED",
        "automatic_execution_available": False,
        "provider_requests": 0,
        "external_socket_connections": 0,
        "database_connections": 0,
        "production_writes": 0,
        "service_lifecycle_actions": 0,
        "stages_v4_runs": 0,
        "releases_created": 0,
        "pointers_changed": 0,
        "stored_read_flags_changed": 0,
        "dispatches": 0,
        "deliveries": 0,
        "broker_actions": 0,
        "trades": 0,
    }


def _selection_binding(
    package: Mapping[str, object],
    selection: object,
    *,
    checked_at: datetime,
) -> dict[str, object]:
    if not isinstance(selection, dict):
        raise TypeError("selection evidence must be a JSON object")
    required = {
        "schema",
        "source_snapshot_at_utc",
        "selected_at_utc",
        "valid_until_utc",
        "policy_sha256",
        "source_snapshot_sha256",
        "request_count",
        "decoded_bytes",
        "pages",
        "pagination_requests",
        "selected",
        "selection_sha256",
    }
    if set(selection) != required:
        raise ValueError("selection evidence fields changed")
    unsigned = dict(selection)
    evidence_sha256 = unsigned.pop("selection_sha256")
    if (
        selection["schema"] != SELECTION_EVIDENCE_SCHEMA
        or evidence_sha256 != canonical_sha256(unsigned)
    ):
        raise ValueError("selection evidence identity changed")
    provider_scope = package["provider_selection"]
    if selection["policy_sha256"] != provider_scope["policy"]["sha256"]:
        raise ValueError("activity-selection policy drifted")
    budget = provider_scope["rest_budget"]
    if (
        selection["request_count"] != budget["requests"]
        or type(selection["decoded_bytes"]) is not int
        or not 0 < selection["decoded_bytes"] <= budget["decoded_bytes"]
        or selection["pages"] != budget["pages"]
        or selection["pagination_requests"] != 0
    ):
        raise ValueError("provider selection budget expanded or diverged")
    start = _timestamp(package["window"]["start_utc"], "window.start_utc")
    stop = _timestamp(package["window"]["stop_utc"], "window.stop_utc")
    snapshot_at = _timestamp(
        selection["source_snapshot_at_utc"],
        "selection.source_snapshot_at_utc",
    )
    selected_at = _timestamp(
        selection["selected_at_utc"],
        "selection.selected_at_utc",
    )
    valid_until = _timestamp(
        selection["valid_until_utc"],
        "selection.valid_until_utc",
    )
    if (
        snapshot_at > selected_at
        or selected_at > checked_at
        or max(start, checked_at) - snapshot_at > timedelta(seconds=300)
        or valid_until < max(start, checked_at)
    ):
        raise ValueError("activity-selection evidence is stale")
    rows = selection["selected"]
    if not isinstance(rows, list) or len(rows) != 2:
        raise ValueError("P8 canary requires exactly two selected contracts")
    contracts: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != {
            "underlyer",
            "contract_ticker",
        }:
            raise ValueError("selected contract shape changed")
        underlyer = str(row["underlyer"]).strip().upper()
        ticker = str(row["contract_ticker"]).strip().upper()
        match = _OPTION_PATTERN.fullmatch(ticker)
        if (
            underlyer not in {"QQQ", "SPY"}
            or match is None
            or match.group("underlyer") != underlyer
            or underlyer in contracts
        ):
            raise ValueError("selected contract universe expanded or diverged")
        contracts[underlyer] = ticker
    if set(contracts) != {"QQQ", "SPY"}:
        raise ValueError("P8 canary selection underlyings changed")
    manifest = {
        "cohort_id": package["cohort"]["cohort_id"],
        "kind": "MARKET_OPTIONS",
        "session_date": package["window"]["session_date"],
        "profile_sha256": package["cohort"]["profile"]["profile_sha256"],
        "template_sha256": package["cohort"]["template_sha256"],
        "selection_sha256": evidence_sha256,
        "underlyings": ["QQQ", "SPY"],
        "contracts": [contracts["QQQ"], contracts["SPY"]],
        "member_count": 4,
        "equity_entries_preserved": True,
    }
    slot_members = [
        {
            "contract_ticker": contracts[underlyer],
            "channel": channel,
            "purpose": "PRIMARY",
            "allocation": 100,
        }
        for underlyer in ("QQQ", "SPY")
        for channel in ("T", "Q")
    ]
    slot = {
        "cohort_id": package["cohort"]["cohort_id"],
        "kind": "PRE_SESSION",
        "version": 1,
        "members": slot_members,
    }
    return {
        "selection_sha256": evidence_sha256,
        "contracts": manifest["contracts"],
        "profile_sha256": manifest["profile_sha256"],
        "manifest_sha256": canonical_sha256(manifest),
        "slot_sha256": canonical_sha256(slot),
        "manifest": manifest,
        "slot": slot,
    }


def validate_p8_live_observation(
    package: Mapping[str, object],
    observation: object,
    *,
    repository_root: Path,
    now: datetime,
    prior_run_records: Sequence[Mapping[str, object]] | None,
    environ: Mapping[str, str] | None = None,
    execution_context: bool = False,
    stored_reads_authorized: bool = False,
) -> dict[str, object]:
    provider_free = provider_free_p8_canary_preflight(
        package,
        repository_root=repository_root,
        now=now,
        prior_run_records=prior_run_records,
        environ=environ,
        allow_active=True,
        allow_execution_artifacts=execution_context,
        allow_provider_credentials=execution_context,
    )
    if not isinstance(observation, dict):
        raise TypeError("P8 live preflight observation must be a JSON object")
    required = {
        "schema",
        "checked_at_utc",
        "current_source_commit",
        "source_files",
        "policy_hashes",
        "orchestration",
        "selection",
        "advanced_enrollment",
        "history_source",
        "equity_input_source",
        "equity_input_readiness",
        "underlying_price_binding",
        "equity_windows",
        "running_peer_owners",
        "retention_dependencies",
        "existing_options_releases",
        "existing_options_pointers",
        "stored_read_flags",
        "owner_authorization",
    }
    if set(observation) != required:
        raise ValueError("P8 live observation fields changed")
    checked_at = _timestamp(
        observation["checked_at_utc"],
        "observation.checked_at_utc",
    )
    if (
        observation["schema"] != LIVE_OBSERVATION_SCHEMA
        or checked_at != _utc(now, "now")
        or observation["current_source_commit"]
        != package["source"]["commit"]
        or observation["source_files"] != package["source"]["files"]
        or observation["policy_hashes"] != package["policy_hashes"]
        or observation["orchestration"] != package["orchestration"]
    ):
        raise ValueError("P8 source or configuration identity drifted")
    if observation["running_peer_owners"]:
        raise ValueError("running or ambiguous peer owner blocks P8")
    if observation["retention_dependencies"]:
        raise ValueError("P8 canary cannot depend on retention state or packages")
    if (
        observation["existing_options_releases"]
        or observation["existing_options_pointers"]
    ):
        raise ValueError("existing canary release or pointer is ambiguous")
    if observation["stored_read_flags"] != {
        "backend": False,
        "frontend": False,
    }:
        raise ValueError("stored-read flags must remain disabled at preflight")
    history = observation["history_source"]
    expected_history_keys = {
        "cohort_id",
        "provider_tier",
        "manifest_sha256",
        "immutable",
        "detector_ready",
        "repaired_sessions",
        "coverage_requirements",
    }
    history_scope = package["history_binding"]
    if (
        not isinstance(history, dict)
        or set(history) != expected_history_keys
        or history["cohort_id"] != history_scope["source_cohort_id"]
        or history["provider_tier"]
        != history_scope["source_provider_tier"]
        or _SHA256_PATTERN.fullmatch(
            str(history["manifest_sha256"])
        ) is None
        or history["immutable"] is not True
        or history["detector_ready"] is not False
        or history["repaired_sessions"]
        != history_scope["required_sessions"]
        or history["coverage_requirements"] != {
            "underlyings": history_scope["required_underlyings"],
            "adjusted_daily_sessions": history_scope[
                "required_adjusted_daily_sessions"
            ],
            "raw_daily_sessions": history_scope[
                "required_raw_daily_sessions"
            ],
        }
    ):
        raise ValueError(
            "retained Developer history binding changed or became executable"
        )
    equity_input = observation["equity_input_source"]
    expected_equity_input = {
        key: value
        for key, value in package["equity_input_binding"].items()
        if key != "symbols"
    }
    if (
        not isinstance(equity_input, dict)
        or equity_input != expected_equity_input
        or _timestamp(
            equity_input["equity_cutoff"],
            "equity_input_source.equity_cutoff",
        ) > checked_at
        or (
            checked_at
            - _timestamp(
                equity_input["equity_cutoff"],
                "equity_input_source.equity_cutoff",
            )
        ).total_seconds() > equity_input["maximum_age_seconds"]
    ):
        raise ValueError(
            "completed Equity input binding is stale or divergent"
        )
    equity_readiness = observation["equity_input_readiness"]
    if (
        not isinstance(equity_readiness, dict)
        or equity_readiness.get("status") != "READY"
        or equity_readiness.get("history_source_cohort_id")
        != package["history_binding"]["source_cohort_id"]
        or equity_readiness.get("equity_source_cohort_id")
        != equity_input["source_cohort_id"]
        or equity_readiness.get("equity_cutoff_utc")
        != equity_input["equity_cutoff"]
        or set(equity_readiness.get("lane_context", {}))
        != {"O1", "O3"}
        or equity_readiness["lane_context"]["O1"].get("status")
        != "AVAILABLE"
        or equity_readiness["lane_context"]["O3"].get("status")
        not in {"AVAILABLE", "BLOCKED"}
    ):
        raise ValueError(
            "P8 Equity dependency readiness is incomplete"
        )
    binding = _selection_binding(
        package,
        observation["selection"],
        checked_at=checked_at,
    )
    underlying_prices = underlying_snapshot_binding_from_payload(
        observation["underlying_price_binding"]
    )
    underlying_scope = package["underlying_price_binding"]
    selected_contracts = {
        str(row["underlyer"]): str(row["contract_ticker"])
        for row in observation["selection"]["selected"]
    }
    if (
        underlying_scope["source"] != PRICE_SOURCE
        or underlying_scope["required_timeframe"]
        != REQUIRED_TIMEFRAME
        or underlying_scope["maximum_price_age_seconds"]
        != underlying_prices.maximum_price_age_seconds
        or underlying_scope["policy_sha256"] != PRICE_POLICY_SHA256
        or underlying_prices.selection_sha256
        != observation["selection"]["selection_sha256"]
        or underlying_prices.source_snapshot_sha256
        != observation["selection"]["source_snapshot_sha256"]
        or tuple(row.ticker for row in underlying_prices.prices)
        != tuple(package["cohort"]["underlyings"])
        or any(
            row.selected_contract_ticker
            != selected_contracts.get(row.ticker)
            for row in underlying_prices.prices
        )
        or any(
            row.session_date
            != _timestamp(
                package["window"]["start_utc"],
                "window.start_utc",
            ).date()
            for row in underlying_prices.prices
        )
    ):
        raise ValueError(
            "P8 underlying snapshot binding is divergent"
        )
    underlying_prices.require_fresh(checked_at)
    start = _timestamp(package["window"]["start_utc"], "window.start_utc")
    stop = _timestamp(package["window"]["stop_utc"], "window.stop_utc")
    equity_windows = observation["equity_windows"]
    if not isinstance(equity_windows, list):
        raise TypeError("equity_windows must be a list")
    for row in equity_windows:
        if not isinstance(row, Mapping):
            raise TypeError("Equity window observation must be an object")
        equity_start = _timestamp(
            row.get("start_utc"),
            "equity_window.start_utc",
        )
        equity_stop = _timestamp(
            row.get("stop_utc"),
            "equity_window.stop_utc",
        )
        if equity_start < stop and start < equity_stop:
            raise ValueError(
                "P8 canary overlaps an Equity qualification window"
            )
    owner = observation["owner_authorization"]
    expected_owner_keys = {
        "authorization_reference",
        "package_sha256",
        "granted_at_utc",
        "granted_authorities",
        "stored_reads",
        "dispatch_delivery",
        "broker_trading",
    }
    if not isinstance(owner, dict) or set(owner) != expected_owner_keys:
        raise ValueError("owner authorization shape changed")
    granted_at = _timestamp(
        owner["granted_at_utc"],
        "owner_authorization.granted_at_utc",
    )
    if (
        owner["authorization_reference"]
        != package["authorization_reference"]
        or owner["package_sha256"] != package["package_sha256"]
        or owner["granted_authorities"] != list(PREPARED_AUTHORITIES)
        or owner["stored_reads"] is not stored_reads_authorized
        or (
            stored_reads_authorized
            and not package["stored_reads"]["authority_prepared"]
        )
        or owner["dispatch_delivery"] is not False
        or owner["broker_trading"] is not False
        or granted_at < _timestamp(
            package["prepared_at_utc"],
            "prepared_at_utc",
        )
        or granted_at > checked_at
    ):
        raise ValueError("owner authority is absent, expanded or stale")
    enrollment = observation["advanced_enrollment"]
    expected_enrollment_keys = {
        "enrollment_id",
        "enrollment_sha256",
        "control_plane_plan_id",
        "control_plane_plan_sha256",
        "cohort_id",
        "cohort_sha256",
        "profile_id",
        "profile_sha256",
        "manifest_id",
        "manifest_sha256",
        "slot_set_sha256",
        "selected_contracts",
    }
    if not isinstance(enrollment, dict) or set(
        enrollment
    ) != expected_enrollment_keys:
        raise ValueError("Advanced enrollment observation fields changed")
    uuid_fields = (
        "enrollment_id",
        "control_plane_plan_id",
        "cohort_id",
        "profile_id",
        "manifest_id",
    )
    sha_fields = (
        "enrollment_sha256",
        "control_plane_plan_sha256",
        "cohort_sha256",
        "profile_sha256",
        "manifest_sha256",
        "slot_set_sha256",
    )
    if (
        any(UUID(str(enrollment[name])).int == 0 for name in uuid_fields)
        or any(
            _SHA256_PATTERN.fullmatch(str(enrollment[name])) is None
            for name in sha_fields
        )
        or enrollment["control_plane_plan_id"]
        != package["orchestration"]["plan_id"]
        or enrollment["cohort_id"] != package["cohort"]["cohort_id"]
        or enrollment["selected_contracts"] != binding["contracts"]
    ):
        raise ValueError("Advanced enrollment identity or selection changed")
    history_binding = {
        "contract_version": "advanced-options-history-binding-v1",
        "target_cohort_id": package["cohort"]["cohort_id"],
        "source_cohort_id": history["cohort_id"],
        "source_provider_tier": history["provider_tier"],
        "history_before": history_scope["history_before_utc"],
        "source_manifest_sha256": history["manifest_sha256"],
        "authorization_reference": package["authorization_reference"],
    }
    history_binding["binding_sha256"] = canonical_sha256(
        history_binding
    )
    return {
        **provider_free,
        "status": "PASS_READ_ONLY_LIVE_P8_CANARY_PREFLIGHT",
        "live_observation_validated": True,
        "live_readiness": "READY_FOR_MANUAL_CANARY_STEPS",
        "realized_binding": binding,
        "advanced_enrollment": enrollment,
        "realized_history_binding": history_binding,
        "realized_equity_input_binding": equity_input,
        "realized_underlying_price_binding": (
            underlying_snapshot_binding_payload(
                underlying_prices
            )
        ),
        "owner_authorities": list(PREPARED_AUTHORITIES),
        "stored_reads_authorized": stored_reads_authorized,
        "dispatch_delivery_authorized": False,
        "broker_trading_authorized": False,
    }


def validate_p8_page_release_observation(
    package: Mapping[str, object],
    observation: object,
) -> dict[str, object]:
    if not isinstance(observation, dict):
        raise TypeError("P8 page release observation must be a JSON object")
    required = {
        "schema",
        "checked_at_utc",
        "cohort_id",
        "scope_sha256",
        "release_id",
        "published_at_utc",
        "valid_until_utc",
        "artifacts",
        "portal_projection_diagnostics",
        "request_time_effects",
        "stock_publications",
    }
    if set(observation) != required:
        raise ValueError("P8 page release observation fields changed")
    checked = _timestamp(
        observation["checked_at_utc"],
        "page_release.checked_at_utc",
    )
    published = _timestamp(
        observation["published_at_utc"],
        "page_release.published_at_utc",
    )
    valid_until = _timestamp(
        observation["valid_until_utc"],
        "page_release.valid_until_utc",
    )
    start = _timestamp(package["window"]["start_utc"], "window.start_utc")
    stop = _timestamp(package["window"]["stop_utc"], "window.stop_utc")
    if (
        observation["schema"] != PAGE_RELEASE_OBSERVATION_SCHEMA
        or observation["cohort_id"] != package["cohort"]["cohort_id"]
        or _SHA256_PATTERN.fullmatch(str(observation["scope_sha256"]))
        is None
        or UUID(str(observation["release_id"])).int == 0
        or published < start
        or published > checked
        or checked > stop
        or valid_until < stop
    ):
        raise ValueError("P8 page release identity or clocks changed")
    page_presentations = {
        "FLOW": ["OPTIONS_ACTIVITY", "OPTIONS_FLOW"],
        "SCREENER": ["OPTIONS_SCREENER"],
        "ALERTS": ["OPTIONS_ALERTS"],
    }
    artifact_keys = {
        "artifact_kind",
        "snapshot_type",
        "contract",
        "snapshot_id",
        "cohort_id",
        "release_id",
        "scope_sha256",
        "pages",
    }
    if (
        not isinstance(observation["artifacts"], list)
        or len(observation["artifacts"])
        != len(package["publication"]["page_artifacts"])
        or any(
            not isinstance(artifact, dict)
            or set(artifact) != artifact_keys
            for artifact in observation["artifacts"]
        )
    ):
        raise ValueError(
            "P8 page V2-native release membership or presentation expanded"
        )
    expected_artifacts = [
        {
            **artifact,
            "snapshot_id": observation_artifact["snapshot_id"],
            "cohort_id": observation["cohort_id"],
            "release_id": observation["release_id"],
            "scope_sha256": observation["scope_sha256"],
            "pages": page_presentations[artifact["artifact_kind"]],
        }
        for artifact, observation_artifact in zip(
            package["publication"]["page_artifacts"],
            observation["artifacts"],
            strict=True,
        )
    ]
    if observation["artifacts"] != expected_artifacts or any(
        UUID(str(artifact["snapshot_id"])).int == 0
        for artifact in observation["artifacts"]
    ):
        raise ValueError(
            "P8 page V2-native release membership or presentation expanded"
        )
    stock = observation["stock_publications"]
    if not isinstance(stock, dict) or set(stock) != {
        "screening",
        "alerts",
    }:
        raise ValueError("P8 stock page publication observation changed")
    screening = stock["screening"]
    alerts = stock["alerts"]
    screening_keys = {
        "page",
        "contract",
        "generation_id",
        "cohort_id",
        "provider_tier",
        "status",
        "generated_at_utc",
    }
    alerts_keys = {
        "page",
        "contract",
        "release_id",
        "snapshot_id",
        "screening_generation_id",
        "cohort_id",
        "provider_tier",
        "generated_at_utc",
    }
    if (
        not isinstance(screening, dict)
        or set(screening) != screening_keys
        or not isinstance(alerts, dict)
        or set(alerts) != alerts_keys
    ):
        raise ValueError("P8 stock page publication fields changed")
    screening_generated = _timestamp(
        screening["generated_at_utc"],
        "stock_screening.generated_at_utc",
    )
    alerts_generated = _timestamp(
        alerts["generated_at_utc"],
        "stock_alerts.generated_at_utc",
    )
    generation_id = UUID(str(screening["generation_id"]))
    if (
        screening["page"] != "STOCK_SCREENER"
        or screening["contract"] != "SCREENING_GENERATION"
        or generation_id.int == 0
        or screening["cohort_id"] != package["cohort"]["cohort_id"]
        or screening["provider_tier"] != "MASSIVE_ADVANCED"
        or screening["status"] != "COMPLETE"
        or screening_generated < start
        or screening_generated > checked
        or alerts["page"] != "STOCK_ALERTS"
        or alerts["contract"] != "STOCK_ALERTS_V1/PLATFORM"
        or alerts["release_id"] != observation["release_id"]
        or UUID(str(alerts["snapshot_id"])).int == 0
        or alerts["screening_generation_id"]
        != screening["generation_id"]
        or alerts["cohort_id"] != package["cohort"]["cohort_id"]
        or alerts["provider_tier"] != screening["provider_tier"]
        or alerts_generated < screening_generated
        or alerts_generated > checked
    ):
        raise ValueError("P8 stock screener or alert publication identity changed")
    if observation["portal_projection_diagnostics"] != list(
        package["publication"]["projection_types"]
    ):
        raise ValueError("P8 diagnostic portal projection membership changed")
    expected_effects = {
        "provider_fetches": 0,
        "detector_runs": 0,
        "publication_retries": 0,
        "dispatches": 0,
        "deliveries": 0,
        "broker_actions": 0,
        "trades": 0,
    }
    if observation["request_time_effects"] != expected_effects:
        raise ValueError("P8 page read performed a forbidden request-time effect")
    return {
        "status": "PASS_P8_COMBINED_PAGE_RELEASE_GATE",
        "package_sha256": package["package_sha256"],
        "cohort_id": observation["cohort_id"],
        "release_id": observation["release_id"],
        "scope_sha256": observation["scope_sha256"],
        "artifact_kinds": ["FLOW", "SCREENER", "ALERTS"],
        "pages": [
            "STOCK_SCREENER",
            "STOCK_ALERTS",
            "OPTIONS_ACTIVITY",
            "OPTIONS_FLOW",
            "OPTIONS_SCREENER",
            "OPTIONS_ALERTS",
        ],
        "portal_projections_are_diagnostics_only": True,
        "stock_screening_generation_id": screening["generation_id"],
        "stock_alert_release_id": alerts["release_id"],
        "stock_alert_snapshot_id": alerts["snapshot_id"],
        "request_time_effects": expected_effects,
    }
