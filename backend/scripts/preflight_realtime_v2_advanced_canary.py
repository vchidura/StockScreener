#!/usr/bin/env python3
"""Inspect the approved canary image and live V2 schema using read-only transactions."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
UTC = timezone.utc


def image_metadata():
    signal.signal(signal.SIGALRM, signal.SIG_DFL)
    signal.setitimer(signal.ITIMER_REAL, 25)
    sys.path.insert(0, "/app")
    from realtime.advanced_ingress_runtime_profile import load_advanced_ingress_runtime_catalog
    from realtime.migrations import V2_MIGRATIONS_DIR, load_realtime_migrations
    from realtime.normalizer import NORMALIZER_VERSION

    if any(os.getenv(name) for name in (
        "POLYGON_API_KEY", "FINNHUB_API_KEY", "REALTIME_V2_APP_DSN", "REALTIME_V2_ADMIN_DSN",
    )):
        raise RuntimeError("image preflight received an unowned credential")
    catalog = load_advanced_ingress_runtime_catalog(
        Path("/app/realtime/policies/advanced_equity_ingress_v1.json"),
    )
    files = {
        "registry": "/app/realtime/relation_registry.py",
        "normalizer": "/app/realtime/normalizer.py",
        "bridge": "/app/realtime/advanced_equity_journal_bridge.py",
    }
    compatibility_metadata = None
    if Path("/app/realtime/equity_live024_compatibility.py").is_file():
        from realtime.equity_live024_compatibility import (
            CONTRACT_ID,
            LIVE024_REGISTRY_SHA256,
            LIVE024_SCHEMA_SHA256,
            RUNTIME_PATHS,
            live024_migration_rows,
            source_bundle_identity,
        )

        compatibility_metadata = {
            "contract_id": CONTRACT_ID,
            "schema_sha256": LIVE024_SCHEMA_SHA256,
            "registry_sha256": LIVE024_REGISTRY_SHA256,
            "migrations": live024_migration_rows(),
            "runtime": source_bundle_identity(Path("/app"), RUNTIME_PATHS),
        }
    return {
        "migrations": [
            {"version": migration.version, "checksum_sha256": migration.checksum_sha256}
            for migration in load_realtime_migrations(V2_MIGRATIONS_DIR)
        ],
        "profile_sha256": catalog.profile_for_target(1).sha256,
        "policy_sha256": catalog.scale_policy.sha256,
        "normalizer_version": NORMALIZER_VERSION,
        "timeout_available": shutil.which("timeout") is not None,
        "raw_code_sha256": {
            key: hashlib.sha256(Path(path).read_bytes()).hexdigest() for key, path in files.items()
        },
        "registry_normalized_sha256": hashlib.sha256(
            Path(files["registry"]).read_text(encoding="utf-8").encode("utf-8")
        ).hexdigest(),
        "provider_requests": 0,
        "equity_live024": compatibility_metadata,
    }


def approved_manifest(path, approval_path):
    body = path.read_bytes()
    manifest = json.loads(body)
    approval = json.loads(approval_path.read_text(encoding="utf-8"))
    if (
        hashlib.sha256(body).hexdigest().lower() != approval["manifest_sha256"].lower()
        or approval["manifest_id"] != manifest["manifest_id"]
        or approval["owner_decision"] != "approve_bounded_live_manifest"
        or approval["conditional_execution_authorized"] is not True
        or manifest["database"] != "stocks_realtime_v2"
    ):
        raise ValueError("canary preflight lacks exact conditional manifest approval")
    return manifest


def migration_differences(expected, observed):
    wanted = {row["version"]: row["checksum_sha256"].strip() for row in expected}
    actual = {row["version"]: row["checksum_sha256"].strip() for row in observed}
    if len(wanted) != len(expected) or len(actual) != len(observed):
        raise ValueError("schema evidence contains duplicate migration identities")
    return {
        "missing": sorted(set(wanted) - set(actual)),
        "unexpected": sorted(set(actual) - set(wanted)),
        "checksum_mismatch": sorted(
            version for version in set(wanted) & set(actual) if wanted[version] != actual[version]
        ),
    }


def compatibility_binding(manifest):
    from realtime.equity_live024_compatibility import (
        CONTRACT_ID,
        load_qualification_report,
    )

    binding = manifest["verified_bindings"].get("equity_live024")
    if binding is None:
        return None
    if binding.get("contract_id") != CONTRACT_ID:
        raise ValueError("manifest names an unknown equity compatibility contract")
    report, digest = load_qualification_report(
        Path(binding["qualification_report"]).resolve(),
        binding["qualification_report_sha256"],
    )
    return binding, report, digest


def evaluate_history_bridge(
    references,
    history,
    bridge,
):
    history_by_interval = {
        row["interval"]: row
        for row in history
    }
    history_reasons = []
    if len(references) != 1:
        history_reasons.append("SPY_REFERENCE_IDENTITY_UNAVAILABLE")
    for interval in ("1m", "5m"):
        row = history_by_interval.get(interval)
        if row is None or int(row["bar_count"]) <= 0:
            history_reasons.append(f"SPY_{interval.upper()}_HISTORY_UNAVAILABLE")
    history_status = (
        "PASS" if not history_reasons else "HISTORY_BOOTSTRAP_REQUIRED"
    )
    if history_reasons:
        return (
            {
                "phase": "PRE_PROVISIONING_COMPATIBILITY_BOOTSTRAP_ADMISSION",
                "status": history_status,
                "reason_codes": history_reasons,
                "references": references,
                "intervals": history,
                "bootstrap_required": True,
                "permitted_action": (
                    "SEPARATELY_AUTHORIZED_HISTORY_BOOTSTRAP"
                ),
                "execution_eligible": False,
            },
            {
                "phase": "POST_BOOTSTRAP_HISTORY_BRIDGE_PREARM",
                "status": "NOT_RUN",
                "reason_codes": ["HISTORY_BOOTSTRAP_REQUIRED"],
                "execution_eligible": False,
            },
        )
    bridge_reasons = []
    if int(bridge["quarantined_journals"]):
        bridge_reasons.append("QUARANTINED_JOURNAL_PRESENT")
    if int(bridge["inflight_journals"]):
        bridge_reasons.append("INFLIGHT_JOURNAL_PRESENT")
    if int(bridge["bridge_offset_lag"]):
        bridge_reasons.append("BRIDGE_OFFSET_LAG_PRESENT")
    if int(bridge["unresolved_gaps"]):
        bridge_reasons.append("UNRESOLVED_STREAM_GAP_PRESENT")
    return (
        {
            "phase": "PRE_PROVISIONING_COMPATIBILITY_BOOTSTRAP_ADMISSION",
            "status": history_status,
            "reason_codes": [],
            "references": references,
            "intervals": history,
            "bootstrap_required": False,
            "permitted_action": "POST_BOOTSTRAP_HISTORY_BRIDGE_PREARM",
            "execution_eligible": False,
        },
        {
            "phase": "POST_BOOTSTRAP_HISTORY_BRIDGE_PREARM",
            "status": "PASS" if not bridge_reasons else "BLOCKED",
            "reason_codes": bridge_reasons,
            "execution_eligible": False,
            **bridge,
        },
    )


def history_bridge_checks(cursor, manifest):
    from scripts.realtime_v2_audit_support import query_rows

    started = datetime.fromisoformat(manifest["scope"]["start_utc"])
    references = query_rows(
        cursor,
        "SELECT security_id,ticker,calendar_id,effective_from,effective_to,"
        "observed_at FROM ref.security_revisions "
        "WHERE ticker='SPY' AND active AND effective_from<=%s "
        "AND (effective_to IS NULL OR effective_to>%s) AND observed_at<=%s "
        "ORDER BY observed_at DESC,revision_id DESC",
        (started, started, started),
    )
    security_ids = [row["security_id"] for row in references[:1]]
    history = []
    if security_ids:
        history = query_rows(
            cursor,
            "SELECT catalog.interval,count(*) AS bar_count,"
            "max(catalog.bar_end) AS latest_bar_end "
            "FROM rt.bar_catalog AS catalog "
            "JOIN rt.instruments AS instrument USING(instrument_id) "
            "WHERE instrument.security_id=%s "
            "AND catalog.interval IN ('1m','5m') AND catalog.bar_end<=%s "
            "GROUP BY catalog.interval ORDER BY catalog.interval",
            (security_ids[0], started),
        )
    journals = query_rows(
        cursor,
        "SELECT count(*) FILTER (WHERE status='QUARANTINED') "
        "AS quarantined_journals,"
        "count(*) FILTER (WHERE status IN ('REGISTERED','WRITING','DRAINING')) "
        "AS inflight_journals,"
        "COALESCE(max(normalized_committed_offset-bridge_committed_offset),0) "
        "AS bridge_offset_lag FROM rt.advanced_journal_registry",
    )[0]
    gaps = query_rows(
        cursor,
        "SELECT count(*) AS unresolved_gaps FROM rt.stream_gaps "
        "WHERE recovery_status NOT IN ('REPAIRED','WAIVED')",
    )[0]
    return evaluate_history_bridge(
        references,
        history,
        {**journals, **gaps},
    )


def inspect_image(manifest, output):
    from scripts.qualify_realtime_v2_advanced_docker import docker, inspect_container, remove_owned_container

    bindings = manifest["verified_bindings"]
    image = bindings["image"]
    if docker("image", "inspect", image, "--format", "{{.Id}}") != bindings["image_id"]:
        raise RuntimeError("approved candidate image identity changed")
    scope = manifest["container_operations_requiring_approval"]["preflight_container"]
    owned = {"container": scope["name"], "run_id": manifest["manifest_id"]}
    if inspect_container(owned["container"]) is not None:
        raise RuntimeError("preflight container already exists; recover its state before retrying")
    try:
        container_id = docker(
            "create", "--name", owned["container"],
            "--label", f"qualification.run_id={owned['run_id']}",
            "--network", "none", "--read-only", "--cpus", str(scope["cpus"]),
            "--memory", str(scope["memory_mib"] * 1024**2),
            "--memory-swap", str(scope["memory_mib"] * 1024**2),
            "--pids-limit", "128", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges",
            "--env", "PYTHONDONTWRITEBYTECODE=1",
            "--mount", f"type=bind,source={Path(__file__).resolve()},target=/operator-preflight.py,readonly",
            image, "python", "-B", "/operator-preflight.py", "--image-metadata",
        )
        if len(container_id) != 64:
            raise RuntimeError("unknown preflight container identity")
        completed = subprocess.run(
            ["docker", "start", "--attach", container_id],
            capture_output=True, text=True, timeout=scope["maximum_seconds"], check=False,
        )
        output.with_suffix(".image.log").write_text(completed.stdout + completed.stderr, encoding="utf-8")
        info = inspect_container(owned["container"])
        if (
            info is None or completed.returncode != 0
            or info["State"]["Running"] or info["State"]["ExitCode"] != 0
            or info["HostConfig"]["Memory"] != scope["memory_mib"] * 1024**2
            or info["HostConfig"]["NanoCpus"] != scope["cpus"] * 1_000_000_000
        ):
            raise RuntimeError("isolated candidate preflight failed; inspect its retained log")
        return json.loads(completed.stdout)
    finally:
        remove_owned_container(owned)


def preflight(manifest, output):
    from realtime.equity_live024_compatibility import (
        OPERATOR_PATHS,
        acl_inventory_cursor,
        live024_migration_rows,
        live024_acl_contract_inventory,
        live024_schema_contract_inventory,
        schema_inventory_cursor,
        source_bundle_identity,
    )
    from realtime.security_v2 import (
        audit_database_security_v2,
        audit_equity_live024_database_security_v2,
        build_role_names_v2,
    )
    from scripts.realtime_script_support import write_report
    from scripts.realtime_v2_audit_support import admin_settings, connect, query_rows, read_only_transaction

    blockers = []
    compatibility = compatibility_binding(manifest)
    checked_at = datetime.now(UTC)
    if checked_at >= datetime.fromisoformat(manifest["scope"]["arm_deadline"]):
        blockers.append("AUTHORIZED_ARM_DEADLINE_MISSED")
    image = inspect_image(manifest, output)
    binding = manifest["verified_bindings"]
    if image["profile_sha256"] != binding["profile_sha256"] or image["policy_sha256"] != binding["policy_sha256"]:
        blockers.append("QUALIFIED_IMAGE_PROFILE_OR_POLICY_MISMATCH")
    if not image["timeout_available"]:
        blockers.append("BOUNDED_CONTAINER_TIMER_UNAVAILABLE")
    qualification_report = None
    qualification_report_sha256 = None
    if compatibility is None:
        source_registry = BACKEND_DIR / "realtime" / "relation_registry.py"
        if hashlib.sha256(source_registry.read_text(encoding="utf-8").encode("utf-8")).hexdigest() != image["registry_normalized_sha256"]:
            blockers.append("HOST_AND_QUALIFIED_IMAGE_REGISTRY_DIFFER")
    else:
        compatibility_manifest, qualification_report, qualification_report_sha256 = compatibility
        expected_image = image.get("equity_live024") or {}
        host_operator = source_bundle_identity(BACKEND_DIR, OPERATOR_PATHS)
        if (
            expected_image.get("contract_id") != compatibility_manifest["contract_id"]
            or expected_image.get("schema_sha256") != qualification_report["schema_sha256"]
            or expected_image.get("registry_sha256") != qualification_report["registry_sha256"]
            or expected_image.get("runtime", {}).get("sha256")
            != qualification_report["runtime_bundle_sha256"]
            or image["profile_sha256"]
            != qualification_report["runtime_profile_sha256"]
            or image["policy_sha256"]
            != qualification_report["runtime_policy_sha256"]
            or host_operator["sha256"]
            != qualification_report["operator_bundle_sha256"]
            or qualification_report["image_id"] != binding["image_id"]
        ):
            blockers.append("EQUITY_LIVE024_ARTIFACT_IDENTITY_MISMATCH")
    settings = admin_settings(BACKEND_DIR / ".env")
    with closing(connect(
        manifest["database"], settings, application_name="preflight_approved_advanced_canary_readonly",
    )) as connection:
        with read_only_transaction(connection, manifest["database"], statement_timeout="5s") as (cursor, control):
            live_migrations = query_rows(
                cursor, "SELECT version,checksum_sha256 FROM rt.schema_migrations ORDER BY version",
            )
            expected_migrations = (
                image["migrations"]
                if compatibility is None
                else live024_migration_rows()
            )
            differences = migration_differences(expected_migrations, live_migrations)
            if any(differences.values()):
                blockers.append(
                    "LIVE_AND_QUALIFIED_IMAGE_SCHEMA_DIFFER"
                    if compatibility is None
                    else "LIVE_SCHEMA_IS_NOT_EXACT_EQUITY_LIVE024"
                )
            phase = query_rows(
                cursor, "SELECT phase FROM rt.security_phase_events ORDER BY event_id DESC LIMIT 1",
            )
            if not phase or phase[0]["phase"] != "AUDITED":
                blockers.append("LIVE_SECURITY_PHASE_NOT_AUDITED")
            security_policy_rows = query_rows(
                cursor,
                "SELECT policy_sha256 FROM rt.database_security_policies "
                "WHERE policy_version='realtime_security_v3'",
            )
            if (
                compatibility is not None
                and (
                    len(security_policy_rows) != 1
                    or security_policy_rows[0]["policy_sha256"].strip()
                    != qualification_report["security_policy_sha256"]
                )
            ):
                blockers.append("LIVE_SECURITY_POLICY_IDENTITY_MISMATCH")
            profiles = query_rows(
                cursor,
                "SELECT runtime_profile_sha256,universe_target,feed,channels,status "
                "FROM rt.advanced_ingress_runtime_profiles "
                "WHERE universe_target=1 AND feed='STOCKS' "
                "AND (runtime_profile_sha256=%s OR status='ACTIVE') "
                "ORDER BY runtime_profile_sha256",
                (binding["profile_sha256"],),
            )
            if any(row["status"] == "ACTIVE" for row in profiles):
                blockers.append("EXISTING_ACTIVE_PROFILE_REQUIRES_OWNER_REVIEW")
            stock_owners = query_rows(
                cursor,
                "SELECT session_id,status,heartbeat_at FROM rt.stream_sessions "
                "WHERE feed='STOCKS' AND ended_at IS NULL ORDER BY started_at",
            )
            if stock_owners:
                blockers.append("EXISTING_STOCK_STREAM_OWNER_REQUIRES_REVIEW")
            security_audit = []
            schema_inventory = None
            acl_inventory = None
            history_check = {
                "status": "NOT_RUN",
                "reason_codes": ["SCHEMA_SECURITY_GATE_BLOCKED"],
            }
            bridge_check = {
                "status": "NOT_RUN",
                "reason_codes": ["SCHEMA_SECURITY_GATE_BLOCKED"],
            }
            if not blockers:
                roles = build_role_names_v2(manifest["database"])
                security_audit = (
                    audit_database_security_v2(connection, roles)
                    if compatibility is None
                    else audit_equity_live024_database_security_v2(
                        connection,
                        roles,
                    )
                )
                if any(not state.startswith("PASS") for _, state in security_audit):
                    blockers.append("LIVE_ROLE_AUDIT_FAILED")
                elif compatibility is not None:
                    schema_inventory = live024_schema_contract_inventory(
                        schema_inventory_cursor(cursor)
                    )
                    acl_inventory = live024_acl_contract_inventory(
                        acl_inventory_cursor(
                        cursor,
                        role_names={
                            suffix: roles.name(suffix)
                            for suffix in ("admin", "app", "api")
                        },
                        )
                    )
                    if (
                        schema_inventory["sha256"]
                        != qualification_report["schema_inventory_sha256"]
                        or acl_inventory["sha256"]
                        != qualification_report["acl_inventory_sha256"]
                    ):
                        blockers.append("LIVE024_SCHEMA_OR_ACL_INVENTORY_MISMATCH")
                    else:
                        history_check, bridge_check = history_bridge_checks(
                            cursor,
                            manifest,
                        )
                        if history_check["status"] not in {
                            "PASS",
                            "HISTORY_BOOTSTRAP_REQUIRED",
                        }:
                            blockers.append("LIVE_HISTORY_REFERENCE_GATE_FAILED")
                        if bridge_check["status"] == "BLOCKED":
                            blockers.append("LIVE_BRIDGE_GATE_FAILED")
    bootstrap_required = (
        history_check["status"] == "HISTORY_BOOTSTRAP_REQUIRED"
    )
    report = {
        "status": (
            "BLOCKED"
            if blockers
            else (
                "HISTORY_BOOTSTRAP_REQUIRED"
                if bootstrap_required
                else "SCHEMA_SECURITY_HISTORY_BRIDGE_COMPATIBLE_METADATA_GATES_REQUIRED"
            )
        ),
        "manifest_id": manifest["manifest_id"], "database": manifest["database"],
        "checked_at": checked_at, "arm_deadline": manifest["scope"]["arm_deadline"],
        "control": control,
        "blockers": blockers, "schema_differences": differences,
        "image": image, "live_migrations": live_migrations,
        "live_security_phase": phase, "live_target_1_profiles": profiles,
        "live_security_policy": security_policy_rows,
        "existing_stock_stream_owners": stock_owners,
        "security_audit": security_audit,
        "contract_id": (
            None
            if compatibility is None
            else compatibility_manifest["contract_id"]
        ),
        "schema_sha256": (
            None
            if compatibility is None
            else qualification_report["schema_sha256"]
        ),
        "schema_inventory_sha256": (
            None if schema_inventory is None else schema_inventory["sha256"]
        ),
        "acl_inventory_sha256": (
            None if acl_inventory is None else acl_inventory["sha256"]
        ),
        "qualification_report_sha256": qualification_report_sha256,
        "security_policy_sha256": (
            None
            if qualification_report is None
            else qualification_report["security_policy_sha256"]
        ),
        "runtime_profile_sha256": (
            None
            if qualification_report is None
            else qualification_report["runtime_profile_sha256"]
        ),
        "runtime_policy_sha256": (
            None
            if qualification_report is None
            else qualification_report["runtime_policy_sha256"]
        ),
        "history_check": history_check,
        "bridge_check": bridge_check,
        "pre_provisioning_admission": history_check,
        "post_bootstrap_prearm_verification": bridge_check,
        "bootstrap_eligible": bootstrap_required and not blockers,
        "permitted_actions": (
            ["SEPARATELY_AUTHORIZED_HISTORY_BOOTSTRAP"]
            if bootstrap_required and not blockers
            else []
        ),
        "provider_requests": 0, "live_database_writes": 0,
        "live_profile_activation_performed": False,
        "preflight_container_removed": True,
        "execution_eligible": False,
    }
    write_report(output, json.loads(json.dumps(report, default=str)))
    print(json.dumps({
        "status": report["status"], "blockers": blockers, "schema_differences": differences,
        "provider_requests": 0, "live_database_writes": 0, "report": str(output),
    }))
    return 2 if blockers else 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image-metadata", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--approval", type=Path)
    parser.add_argument("--report-path", type=Path)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if args.image_metadata:
        print(json.dumps(image_metadata(), sort_keys=True))
        return 0
    if args.manifest is None or args.approval is None or args.report_path is None:
        parser.error("--manifest, --approval and --report-path are required")
    if args.report_path.exists() or args.report_path.with_suffix(".image.log").exists():
        parser.error("--report-path and its image log must be new")
    sys.path.insert(0, str(BACKEND_DIR))
    manifest = approved_manifest(args.manifest, args.approval)
    if not args.execute:
        print(json.dumps({
            "status": "PLAN", "manifest_id": manifest["manifest_id"],
            "actions": ["isolated no-secret image metadata", "read-only admin schema/security checks"],
            "provider_requests": 0, "live_database_writes": 0,
        }))
        return 0
    return preflight(manifest, args.report_path.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
