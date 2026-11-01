"""Disabled-by-default deployment identity for Advanced Options V3."""
from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from .advanced_options_p8_canary_authorization import (
    SOURCE_BINDING_PATHS,
    canonical_sha256,
    source_binding_identity,
)
from .advanced_options_service import (
    AdvancedOptionsProcessKind,
    OPTIONS_V3_STAGE_ORDER,
)
from .contracts import EQUITY_VALIDATION_UNIVERSE
from .stage_graph import STAGE_CATALOG_VERSION_V5


ADVANCED_OPTIONS_V3_PACKAGE_SCHEMA = (
    "advanced-options-v3-live-package-v1"
)
ADVANCED_OPTIONS_V3_PROFILE_VERSION = (
    "advanced-options-v3-target9-qqq-spy-v1"
)
ADVANCED_OPTIONS_V3_COMPOSE_PROFILE = "realtime-v3-options"
ADVANCED_OPTIONS_V3_DISABLED_STATE = "DISABLED_NOT_PREPARED"
ADVANCED_OPTIONS_V3_QUALIFICATION_STATE = (
    "OWNED_DISPOSABLE_QUALIFICATION"
)
ADVANCED_OPTIONS_V3_LIVE_STATE = "PREPARED_ARMED_UNCONSUMED"
ADVANCED_OPTIONS_V3_AUTHORITY_REFERENCES_SCHEMA = (
    "advanced-options-v3-authority-references-v1"
)
ADVANCED_OPTIONS_V3_SOURCE_BINDING_PATHS = tuple(dict.fromkeys((
    *SOURCE_BINDING_PATHS,
    "backend/realtime/advanced_gateway.py",
    "backend/realtime/advanced_options_v3_deployment.py",
    "backend/realtime/advanced_options_acquisition_runtime.py",
    "backend/realtime/advanced_options_selection_c6.py",
    "backend/realtime/advanced_options_activity_momentum_c6.py",
    "backend/realtime/advanced_options_session_state.py",
    "backend/realtime/policies/advanced_options_selection_c6_v1.json",
    "backend/realtime/policies/advanced_options_activity_momentum_c6_v1.json",
    "backend/realtime/sql_v2/042_advanced_options_selection_shards.sql",
    "backend/realtime/advanced_options_v3_history.py",
    "backend/realtime/advanced_options_v3_qualification.py",
    "backend/realtime/advanced_options_v3_authority.py",
    "backend/realtime/advanced_options_resident_barrier.py",
    "backend/realtime/advanced_realtime_session_scheduler.py",
    "backend/realtime/advanced_realtime_session_operations.py",
    "backend/realtime/advanced_realtime_daily_supervisor.py",
    "backend/realtime/advanced_realtime_v3_local_preflight.py",
    "backend/realtime/advanced_options_v3_broad_stream.py",
    "backend/realtime/equity_bulk_backfill.py",
    "backend/realtime/equity_recent_repair.py",
    "backend/options/model_inputs.py",
    "backend/realtime/canonical_stock_alert_product.py",
    "backend/realtime/canonical_stock_alert_repository.py",
    "backend/realtime/advanced_ingress_controller.py",
    "backend/realtime/advanced_journal_replay.py",
    "backend/realtime/journal.py",
    "backend/realtime/orchestration_repository.py",
    "backend/realtime/sql_v2/038_realtime_stock_alert_input_bindings.sql",
    "backend/equity/model_detection/__init__.py",
    "backend/equity/model_detection/codec.py",
    "backend/equity/model_detection/contracts.py",
    "backend/equity/model_detection/engine.py",
    "backend/equity/model_detection/feature_definitions.py",
    "backend/equity/model_detection/feature_engine.py",
    "backend/equity/model_detection/policies.py",
    "backend/equity/model_detection/v2_adapter.py",
    "backend/scripts/inspect_advanced_options_v3_package.py",
    "backend/scripts/inspect_advanced_options_v3_history.py",
    "backend/scripts/prepare_advanced_options_v3_pre_session.py",
    "backend/scripts/prepare_advanced_options_v3_operations.py",
    "backend/scripts/apply_advanced_options_v3_pre_session_capture.py",
    "backend/scripts/qualify_advanced_realtime_session_scheduler.py",
    "backend/scripts/render_advanced_realtime_v3_freeze_inputs.py",
    "backend/scripts/render_advanced_realtime_v3_compose.py",
    "backend/scripts/run_advanced_options_v3_acquisition.py",
    "backend/scripts/prepare_advanced_options_v3_pre_session_authority.py",
    "backend/scripts/prepare_realtime_v2_docker_secrets.py",
    "backend/scripts/prepare_realtime_v2_advanced_ingress.py",
    "backend/scripts/qualify_advanced_options_v3_docker.py",
    "backend/scripts/run_realtime_v2_canonical_stock_alerts.py",
    "backend/scripts/audit_advanced_options_v3_operations.py",
    "backend/scripts/assemble_advanced_realtime_v3_freeze.py",
    "backend/scripts/audit_realtime_v2_target9_scanner_warmup.py",
    "backend/scripts/bind_realtime_v2_advanced_history.py",
    "backend/scripts/replay_realtime_v2_retained_native_plan.py",
    "backend/scripts/replay_advanced_options_v3_capture_local.py",
    "backend/scripts/audit_advanced_realtime_v3_post_close_history_source.py",
    "backend/scripts/capture_advanced_options_v3_exact_cycles.py",
    "backend/scripts/capture_advanced_realtime_v3_websockets.py",
    "backend/scripts/validate_advanced_realtime_v3_host_pipeline.py",
    "backend/scripts/validate_advanced_realtime_v3_local_holistic.py",
    "backend/scripts/run_advanced_realtime_v3_host_market_segment.py",
    "backend/scripts/capture_advanced_options_v3_broader_chains.py",
    "backend/scripts/export_advanced_realtime_v3_post_close_history.py",
    "backend/scripts/prepare_advanced_options_v3_broad_stream.py",
    "backend/scripts/prepare_advanced_options_v3_post_close_reference.py",
    "backend/scripts/prepare_advanced_realtime_v3_post_close_replay.py",
    "backend/scripts/qualify_advanced_options_v3_broad_stream_capacity.py",
    "backend/scripts/register_advanced_realtime_v3_session_task.ps1",
    "backend/scripts/run_advanced_realtime_one_shot.py",
    "backend/scripts/run_advanced_options_v3_market_loop.py",
    "backend/scripts/run_advanced_options_v3_production.py",
    "backend/scripts/run_advanced_realtime_v3_daily_supervisor.py",
    "backend/scripts/run_advanced_realtime_session_scheduler.py",
    "backend/scripts/run_realtime_v2_advanced_controller.py",
    "backend/scripts/run_realtime_v2_advanced_equity_window.py",
    "backend/scripts/validate_advanced_realtime_v3_holistic.py",
    "backend/scripts/validate_advanced_realtime_v3_local_preflight.py",
    "backend/scripts/validate_advanced_realtime_v3_post_close_replay.py",
    "docker-compose.realtime-v2.yml",
)))
_SHA256_HEX = frozenset("0123456789abcdef")
_ZERO_EFFECTS = {
    "provider_rest": 0,
    "provider_websocket": 0,
    "production_database_writes": 0,
    "production_service_actions": 0,
    "production_migrations": 0,
    "releases": 0,
    "pointer_changes": 0,
    "stored_read_changes": 0,
    "dispatch": 0,
    "delivery": 0,
    "broker": 0,
    "trading": 0,
}


def _sha256(value: object, name: str) -> str:
    normalized = str(value)
    if len(normalized) != 64 or any(
        character not in _SHA256_HEX for character in normalized
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return normalized


def _commit(value: object) -> str:
    normalized = str(value)
    if len(normalized) != 40 or any(
        character not in _SHA256_HEX for character in normalized
    ):
        raise ValueError("source commit must be a full Git commit")
    return normalized


def validate_immutable_image_id(value: object) -> str:
    image = str(value).strip()
    name, separator, digest = image.rpartition("@sha256:")
    local_digest = (
        image.removeprefix("sha256:")
        if image.startswith("sha256:")
        else ""
    )
    remote = (
        bool(name)
        and separator == "@sha256:"
        and len(digest) == 64
        and not any(character not in _SHA256_HEX for character in digest)
    )
    local = (
        len(local_digest) == 64
        and not any(
            character not in _SHA256_HEX
            for character in local_digest
        )
    )
    if (not remote and not local) or any(
        character.isspace() for character in image
    ):
        raise ValueError("V3 backend image identity must be immutable")
    return image


def advanced_options_v3_profile_payload() -> dict[str, object]:
    return {
        "version": ADVANCED_OPTIONS_V3_PROFILE_VERSION,
        "enabled_by_default": False,
        "compose_profile": ADVANCED_OPTIONS_V3_COMPOSE_PROFILE,
        "equity": {
            "target": 9,
            "provider_tier": "MASSIVE_ADVANCED",
            "feed": "STOCKS",
            "channels": ["AM"],
            "symbols": list(
                EQUITY_VALIDATION_UNIVERSE.equity_tickers
            ),
        },
        "options": {
            "provider_tier": "MASSIVE_ADVANCED",
            "feed": "OPTIONS",
            "channels": ["T", "Q"],
            "underlyings": ["QQQ", "SPY"],
            "nine_underlying_expansion_authorized": False,
        },
        "orchestration": {
            "catalog_version": STAGE_CATALOG_VERSION_V5,
            "stage_order": list(OPTIONS_V3_STAGE_ORDER),
            "durable_resident_barrier": True,
            "filesystem_correctness_authority": False,
        },
        "roles": [
            kind.value for kind in AdvancedOptionsProcessKind
        ],
        "publication": {
            "canonical": True,
            "preserve_equity_entries": True,
            "cohort_local_pointer": True,
            "dispatch_authorized": False,
            "delivery_authorized": False,
            "broker_authorized": False,
            "trading_authorized": False,
        },
        "authorities": [
            "SESSION_ACQUISITION",
            "WINDOW_CYCLE",
            "CANONICAL_PUBLICATION",
            "STORED_READ",
        ],
    }


ADVANCED_OPTIONS_V3_PROFILE_SHA256 = canonical_sha256(
    advanced_options_v3_profile_payload()
)


def build_disabled_advanced_options_v3_package(
    *,
    repository_root: Path,
    source_commit: str,
    backend_image: str,
    qualification_semantic_sha256: str,
    qualification_file_sha256: str,
    source_files: Sequence[str] = (
        ADVANCED_OPTIONS_V3_SOURCE_BINDING_PATHS
    ),
) -> dict[str, object]:
    package = {
        "schema": ADVANCED_OPTIONS_V3_PACKAGE_SCHEMA,
        "state": ADVANCED_OPTIONS_V3_DISABLED_STATE,
        "profile": advanced_options_v3_profile_payload(),
        "profile_sha256": ADVANCED_OPTIONS_V3_PROFILE_SHA256,
        "source": {
            "commit": _commit(source_commit),
            "files": source_binding_identity(
                repository_root,
                source_files,
            ),
        },
        "qualification": {
            "verdict": "PASS",
            "semantic_sha256": _sha256(
                qualification_semantic_sha256,
                "qualification semantic SHA",
            ),
            "file_sha256": _sha256(
                qualification_file_sha256,
                "qualification file SHA",
            ),
        },
        "docker": {
            "backend_image": validate_immutable_image_id(
                backend_image
            ),
            "compose_profile": ADVANCED_OPTIONS_V3_COMPOSE_PROFILE,
            "service_count": 4,
            "build_authorized": False,
            "execution_requires_no_build": True,
        },
        "activation": {
            "session_acquisition": "NOT_PREPARED",
            "window_cycle": "NOT_PREPARED",
            "canonical_publication": "NOT_PREPARED",
            "stored_read": "NOT_AUTHORIZED",
            "window_role_authorized": False,
            "live_package_created": False,
        },
        "effects": dict(_ZERO_EFFECTS),
    }
    package["package_sha256"] = canonical_sha256(package)
    return package


def validate_disabled_advanced_options_v3_package(
    package: object,
    *,
    repository_root: Path,
    source_files: Sequence[str] = (
        ADVANCED_OPTIONS_V3_SOURCE_BINDING_PATHS
    ),
) -> dict[str, object]:
    if not isinstance(package, dict):
        raise TypeError("V3 package must be a JSON object")
    digest = package.get("package_sha256")
    unsigned = dict(package)
    unsigned.pop("package_sha256", None)
    if (
        package.get("schema") != ADVANCED_OPTIONS_V3_PACKAGE_SCHEMA
        or package.get("state") != ADVANCED_OPTIONS_V3_DISABLED_STATE
        or digest != canonical_sha256(unsigned)
        or package.get("profile")
        != advanced_options_v3_profile_payload()
        or package.get("profile_sha256")
        != ADVANCED_OPTIONS_V3_PROFILE_SHA256
        or package.get("effects") != _ZERO_EFFECTS
        or package.get("activation") != {
            "session_acquisition": "NOT_PREPARED",
            "window_cycle": "NOT_PREPARED",
            "canonical_publication": "NOT_PREPARED",
            "stored_read": "NOT_AUTHORIZED",
            "window_role_authorized": False,
            "live_package_created": False,
        }
        or package.get("source", {}).get("files")
        != source_binding_identity(repository_root, source_files)
    ):
        raise ValueError("disabled V3 package identity changed")
    _commit(package["source"]["commit"])
    _sha256(
        package["qualification"]["semantic_sha256"],
        "qualification semantic SHA",
    )
    _sha256(
        package["qualification"]["file_sha256"],
        "qualification file SHA",
    )
    validate_immutable_image_id(
        package["docker"]["backend_image"]
    )
    return package


def advanced_options_v3_package_plan(
    package: Mapping[str, object],
) -> dict[str, object]:
    return {
        "mode": "PLAN",
        "status": "PASS_PROVIDER_FREE_V3_DISABLED_PLAN",
        "package_sha256": package["package_sha256"],
        "profile_sha256": package["profile_sha256"],
        "state": package["state"],
        "authority_phases": {
            "session_acquisition": "SEPARATE",
            "window_cycle": "SEPARATE",
            "canonical_publication": "SEPARATE",
            "stored_read": "SEPARATE_NOT_AUTHORIZED",
        },
        "docker": package["docker"],
        "effects": dict(_ZERO_EFFECTS),
    }


def provider_free_advanced_options_v3_preflight(
    package: Mapping[str, object],
    *,
    repository_root: Path,
) -> dict[str, object]:
    validate_disabled_advanced_options_v3_package(
        dict(package),
        repository_root=repository_root,
    )
    return {
        "mode": "READ_ONLY_PREFLIGHT",
        "status": "PASS_PROVIDER_FREE_V3_DISABLED_PREFLIGHT",
        "package_sha256": package["package_sha256"],
        "profile_sha256": package["profile_sha256"],
        "source_file_count": len(package["source"]["files"]),
        "live_blockers": [
            "SESSION_ACQUISITION_AUTHORITY_NOT_PREPARED",
            "WINDOW_CYCLE_AUTHORITY_NOT_PREPARED",
            "CANONICAL_PUBLICATION_AUTHORITY_NOT_PREPARED",
            "STORED_READ_AUTHORITY_NOT_AUTHORIZED",
            "IMMUTABLE_IMAGE_NOT_DEPLOYED",
            "LIVE_PACKAGE_NOT_CREATED",
        ],
        "effects": dict(_ZERO_EFFECTS),
    }


def advanced_options_v3_package_status(
    package: Mapping[str, object],
) -> dict[str, object]:
    return {
        "mode": "STATUS",
        "status": "DISABLED",
        "detail": (
            "source-qualified V3 profile exists; no live package or "
            "activation authority exists"
        ),
        "package_sha256": package["package_sha256"],
        "profile_sha256": package["profile_sha256"],
        "activation": package["activation"],
        "effects": dict(_ZERO_EFFECTS),
    }


def build_advanced_options_v3_authority_references(
    session_authority_payload: object,
    *,
    target_window_id: UUID,
) -> dict[str, object]:
    from .advanced_options_v3_authority import (
        session_acquisition_authorization_from_payload,
    )

    session = session_acquisition_authorization_from_payload(
        session_authority_payload
    )
    if not isinstance(target_window_id, UUID) or target_window_id.int == 0:
        raise ValueError("V3 target window identity is invalid")
    return {
        "schema": ADVANCED_OPTIONS_V3_AUTHORITY_REFERENCES_SCHEMA,
        "session_acquisition": {
            "authorization_id": str(session.authorization_id),
            "authorization_sha256": session.authorization_sha256,
        },
        "window_cycle": {
            "load_mode": "DURABLE_SESSION_WINDOWS",
            "session_authorization_id": str(session.authorization_id),
            "window_id_version": "session-cohort-key-cutoff-v1",
        },
        "canonical_publication": {
            "load_mode": "DERIVE_FROM_DURABLE_WINDOW_CYCLE",
            "authorization_id": str(uuid5(
                NAMESPACE_URL,
                (
                    "advanced-options-v3-session-publisher:"
                    f"{session.authorization_id}"
                ),
            )),
            "session_authorization_id": str(session.authorization_id),
        },
        "stored_read": {
            "authorized": False,
            "activation_mode": "SEPARATE_AFTER_CANONICAL_PUBLICATION",
        },
    }


def validate_advanced_options_v3_authority_references(
    package: Mapping[str, object],
    bundle: Mapping[str, object],
) -> None:
    authorities = bundle.get("v3_authorities")
    references = bundle.get("v3_authority_references")
    production = bundle.get("production_binding")
    session_payload = (
        authorities.get("session_acquisition")
        if isinstance(authorities, Mapping)
        else None
    )
    if (
        not isinstance(production, Mapping)
        or not isinstance(references, Mapping)
        or set(authorities or {}) != {"session_acquisition"}
    ):
        raise ValueError(
            "V3 immutable session bundle authority scope changed"
        )
    expected = build_advanced_options_v3_authority_references(
        session_payload,
        target_window_id=UUID(str(bundle["window_id"])),
    )
    if (
        references != expected
        or bundle.get("v3_authorities")
        != package.get("v3_authorities")
    ):
        raise ValueError("V3 durable authority references changed")


def validate_advanced_options_v3_runtime_identity(
    package: Mapping[str, object],
    bundle: Mapping[str, object],
) -> None:
    deployment = package.get("v3_deployment")
    orchestration = bundle.get("orchestration")
    production = bundle.get("production_binding")
    control = (
        production.get("control")
        if isinstance(production, Mapping)
        else None
    )
    equity = package.get("equity_input_binding")
    cohort = package.get("cohort")
    owners = package.get("owners")
    processes = (
        owners.get("processes")
        if isinstance(owners, Mapping)
        else None
    )
    waiting = bundle.get("session_binding") is not None
    if (
        not isinstance(deployment, Mapping)
        or deployment.get("profile")
        != advanced_options_v3_profile_payload()
        or deployment.get("profile_sha256")
        != ADVANCED_OPTIONS_V3_PROFILE_SHA256
        or deployment.get("state") not in {
            ADVANCED_OPTIONS_V3_QUALIFICATION_STATE,
            ADVANCED_OPTIONS_V3_LIVE_STATE,
        }
        or deployment.get("window_role_authorized") is not True
        or deployment.get("qualification_verdict") != "PASS"
        or not isinstance(orchestration, Mapping)
        or orchestration.get("catalog_version")
        != STAGE_CATALOG_VERSION_V5
        or orchestration.get("stage_order")
        != list(OPTIONS_V3_STAGE_ORDER)
        or not isinstance(equity, Mapping)
        or equity.get("symbols")
        != list(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
        or not isinstance(cohort, Mapping)
        or cohort.get("underlyings") != ["QQQ", "SPY"]
        or not isinstance(processes, list)
        or [row.get("kind") for row in processes]
        != [kind.value for kind in AdvancedOptionsProcessKind]
        or bundle.get("package_sha256")
        != package.get("package_sha256")
    ):
        raise ValueError(
            "V5 WINDOW requires the exact qualified V3 identity"
        )
    if waiting:
        from .advanced_options_p8_runtime import (
            p8_preopen_session_binding_from_payload,
        )

        session_binding = p8_preopen_session_binding_from_payload(
            bundle["session_binding"]
        )
        if (
            production is not None
            or bundle.get("window_id") is not None
            or session_binding.package_sha256
            != package.get("package_sha256")
            or str(session_binding.cohort_id)
            != cohort.get("cohort_id")
            or str(session_binding.plan_id)
            != orchestration.get("plan_id")
            or list(session_binding.contract_tickers)
            != sorted(package.get("v3_admitted_contracts") or ())
        ):
            raise ValueError(
                "V5 waiting session identity contains cycle authority"
            )
    elif (
        not isinstance(control, Mapping)
        or control.get("option_underlyings") != ["QQQ", "SPY"]
    ):
        raise ValueError(
            "V5 WINDOW requires the exact qualified V3 identity"
        )
    if (
        package.get("v3_authorities") is not None
        or bundle.get("v3_authority_references") is not None
    ):
        if waiting:
            if bundle.get("v3_authority_references") is not None:
                raise ValueError(
                    "V5 waiting session has prewritten cycle authority"
                )
        else:
            validate_advanced_options_v3_authority_references(
                package,
                bundle,
            )


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
