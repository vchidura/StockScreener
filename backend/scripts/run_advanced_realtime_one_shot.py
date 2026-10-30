#!/usr/bin/env python3
"""Run one authorized Equity-to-Options production session from a sealed plan."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import EXPECTED_V2_DATABASE_NAME  # noqa: E402


UTC = timezone.utc
EQUITY_SERVICES = (
    "realtime-v2-advanced-controller",
    "realtime-v2-advanced-ingress",
    "realtime-v2-advanced-bridge",
    "realtime-v2-advanced-scheduler",
    "realtime-v2-advanced-finalizer",
    "realtime-v2-stock-alert-results",
)
OPTIONS_SERVICES = (
    "realtime-v2-options-p8-ingress",
    "realtime-v2-options-p8-market-state",
    "realtime-v2-options-p8-flow",
    "realtime-v2-options-p8-window",
)
ZERO_FORBIDDEN_EFFECTS = {
    "migration": 0,
    "retention": 0,
    "replay": 0,
    "dispatch": 0,
    "delivery": 0,
    "broker": 0,
    "trading": 0,
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--plan-file-sha256", required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    args.plan = args.plan.resolve()
    args.output_directory = args.output_directory.resolve()
    if (
        not args.plan.is_file()
        or len(args.plan_file_sha256) != 64
        or any(
            value not in "0123456789abcdef"
            for value in args.plan_file_sha256
        )
        or not args.execute
    ):
        parser.error("sealed one-shot execution scope is invalid")
    if args.resume != args.output_directory.exists():
        parser.error(
            "--resume must exactly match an existing output directory"
        )
    return args


def _utc(value, name):
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return parsed.astimezone(UTC)


def _load_plan(args):
    body = args.plan.read_bytes()
    if hashlib.sha256(body).hexdigest() != args.plan_file_sha256:
        raise ValueError("one-shot plan file identity changed")
    plan = json.loads(body)
    if (
        plan.get("contract") != "advanced-realtime-one-shot-plan-v1"
        or plan.get("database") != EXPECTED_V2_DATABASE_NAME
        or plan.get("session_date") != "2026-10-27"
        or plan.get("equity", {}).get("catch_up_mode") != "AUTO"
        or plan.get("equity", {}).get("universe_target") != 9
        or plan.get("equity", {}).get("rest_request_ceiling") != 18
        or plan.get("options", {}).get("rest_request_count") != 2
        or plan.get("options", {}).get("maximum_windows") != 4
        or plan.get("reference", {}).get("request_ceiling") != 43
        or plan.get("equity_product", {}).get("enabled") is not True
        or plan.get("equity_product", {}).get(
            "reference_refresh_requests"
        ) != 31
        or plan.get("forbidden_effects") != ZERO_FORBIDDEN_EFFECTS
    ):
        raise ValueError("one-shot plan contract changed")
    supersedes = plan.get("supersedes_plan_file_sha256")
    if supersedes is not None and (
        len(supersedes) != 64
        or any(value not in "0123456789abcdef" for value in supersedes)
    ):
        raise ValueError("one-shot plan supersession identity changed")
    clocks = {
        name: _utc(plan["clocks"][name], name)
        for name in (
            "preflight_start_utc",
            "reference_authorized_until_utc",
            "equity_cutoff_utc",
            "hard_stop_utc",
        )
    }
    if not (
        clocks["preflight_start_utc"]
        < clocks["reference_authorized_until_utc"]
        < clocks["equity_cutoff_utc"]
        < clocks["hard_stop_utc"]
    ):
        raise ValueError("one-shot plan clocks changed")
    return plan, clocks


def _write(path, value, *, exclusive=False):
    encoded = (
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    if exclusive:
        with path.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(encoded)
    temporary.replace(path)


def _status(output, phase, **values):
    _write(
        output / "RUN-STATUS.json",
        {
            "phase": phase,
            "observed_at_utc": datetime.now(UTC),
            **values,
        },
    )


def _run(
    argv,
    *,
    output,
    hard_stop,
    environment=None,
    check=True,
    timeout=300,
    allow_after_hard_stop=False,
):
    remaining = (hard_stop - datetime.now(UTC)).total_seconds()
    if remaining <= 0 and not allow_after_hard_stop:
        raise TimeoutError("one-shot hard stop reached")
    if allow_after_hard_stop:
        remaining = timeout
    completed = subprocess.run(
        [str(value) for value in argv],
        cwd=REPOSITORY_ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=max(1, min(timeout, int(remaining))),
        check=False,
    )
    with (output / "commands.log").open("ab") as stream:
        stream.write(
            (
                f"\n[{datetime.now(UTC).isoformat()}] "
                f"{Path(str(argv[0])).name} exit={completed.returncode}\n"
            ).encode("utf-8")
        )
        stream.write(completed.stdout[-12_000:])
        stream.write(completed.stderr[-12_000:])
    if check and completed.returncode:
        detail = (completed.stdout + b"\n" + completed.stderr).decode(
            "utf-8",
            errors="replace",
        )
        raise RuntimeError(
            f"command failed ({completed.returncode}): {detail[-4000:]}"
        )
    return completed


def _python(plan, script, *arguments):
    return (
        plan["python_executable"],
        "-B",
        str(BACKEND_DIR / "scripts" / script),
        *arguments,
    )


def _sleep_until(target, hard_stop, output, phase):
    while True:
        now = datetime.now(UTC)
        if now >= hard_stop:
            raise TimeoutError("one-shot hard stop reached while waiting")
        remaining = (target - now).total_seconds()
        if remaining <= 0:
            return
        _status(
            output,
            phase,
            target_utc=target,
            remaining_seconds=int(remaining),
        )
        time.sleep(min(remaining, 60))


def _provider():
    values = dotenv_values(BACKEND_DIR / ".env")
    dsn = str(
        os.getenv("REALTIME_V2_APP_DSN")
        or values.get("REALTIME_V2_APP_DSN")
        or ""
    ).strip()
    if not dsn:
        raise RuntimeError("REALTIME_V2_APP_DSN is required")
    pool = ThreadedConnectionPool(
        1,
        2,
        dsn=dsn,
        options="-c timezone=UTC",
    )
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
            statement_timeout_ms=30_000,
        ),
    )
    provider.verify()
    return provider, pool


def _wait_for_equity_receipt(plan, clocks, output):
    provider, pool = _provider()
    try:
        while datetime.now(UTC) < clocks["hard_stop_utc"]:
            with provider.transaction(
                APP_ROLE,
                read_only=True,
                dict_cursor=True,
            ) as unit:
                unit.cursor.execute(
                    "SELECT window_run.window_id::text,"
                    "max(receipt.receipt_id::text) FILTER (WHERE "
                    "receipt.receipt_kind='INPUT_READY') "
                    "AS input_receipt_id,"
                    "max(receipt.input_sha256) FILTER (WHERE "
                    "receipt.receipt_kind='INPUT_READY') "
                    "AS input_sha256,"
                    "max(receipt.receipt_id::text) FILTER (WHERE "
                    "receipt.receipt_kind='PORTAL_READY') "
                    "AS portal_receipt_id "
                    "FROM rt.window_runs AS window_run "
                    "JOIN rt.cycle_receipts AS receipt USING(window_id) "
                    "WHERE window_run.cohort_id=%s "
                    "AND window_run.cutoff=%s "
                    "AND window_run.state='COMPLETE' "
                    "GROUP BY window_run.window_id "
                    "HAVING count(*) FILTER (WHERE "
                    "receipt.receipt_kind='INPUT_READY')=1 "
                    "AND count(*) FILTER (WHERE "
                    "receipt.receipt_kind='PORTAL_READY')=1 "
                    "ORDER BY window_run.window_id DESC LIMIT 1",
                    (
                        plan["equity"]["cohort_id"],
                        clocks["equity_cutoff_utc"],
                    ),
                )
                row = unit.cursor.fetchone()
            if row is not None:
                return dict(row)
            _status(
                output,
                "WAITING_FOR_E",
                equity_cutoff_utc=clocks["equity_cutoff_utc"],
            )
            time.sleep(5)
    finally:
        pool.closeall()
    raise TimeoutError("exact target-9 E receipt was not completed")


def _equity_window_command(
    plan,
    output,
    history,
    *,
    suffix,
    report_name,
    publication=None,
):
    arguments = [
        "--database",
        plan["database"],
        "--confirm-database-name",
        plan["database"],
        "--cohort-id",
        plan["equity"]["cohort_id"],
        "--session-date",
        plan["session_date"],
        "--cutoff",
        plan["clocks"]["equity_cutoff_utc"],
        "--finality-delay-seconds",
        "5",
        "--completion-budget-seconds",
        "300",
        "--authorization-reference",
        f"{plan['authorization_reference']}:EQUITY-PRODUCT",
        f"--window-key-suffix={suffix}",
        "--retry-release-now",
        "--reuse-input-receipt",
        "--reuse-portal-receipt",
        "--include-alerts",
        "--include-strategies",
        "--history-binding-report",
        str(history["report"]),
        "--history-binding-file-sha256",
        history["file_sha256"],
        "--report-path",
        str(output / report_name),
        "--execute",
    ]
    if publication is not None:
        arguments.extend((
            "--publish",
            "--publication-reference",
            f"{plan['authorization_reference']}:EQUITY-P11",
            "--publication-authority",
            str(publication["path"]),
            "--publication-authority-semantic-sha256",
            publication["semantic_sha256"],
            "--publication-authority-file-sha256",
            publication["file_sha256"],
        ))
    return _python(
        plan,
        "run_realtime_v2_advanced_equity_window.py",
        *arguments,
    )


def _run_equity_product(
    plan,
    clocks,
    output,
    history,
    environment,
):
    product_report = output / "EQUITY-PRODUCT.json"
    _run(
        _equity_window_command(
            plan,
            output,
            history,
            suffix="-retry1",
            report_name=product_report.name,
        ),
        output=output,
        hard_stop=clocks["hard_stop_utc"],
        environment=environment,
        timeout=360,
    )
    product = json.loads(product_report.read_text(encoding="utf-8"))
    required = {
        "STOCK_ALERTS",
        "STRATEGY_PUBLISH",
    }
    completed = {
        stage
        for stage, state in product.get("executed") or ()
        if state == "COMPLETE"
    }
    if (
        product.get("window_state") != "COMPLETE"
        or not required.issubset(completed)
        or product.get("lane_errors")
    ):
        raise RuntimeError(
            "Equity alerts/strategy product stages did not complete"
        )
    authority_path = output / "EQUITY-P11-AUTHORITY.json"
    authority_result = _run(
        _python(
            plan,
            "prepare_advanced_equity_p11_publication_authority.py",
            "--database",
            plan["database"],
            "--confirm-database-name",
            plan["database"],
            "--cohort-id",
            plan["equity"]["cohort_id"],
            "--session-date",
            plan["session_date"],
            "--cutoff",
            plan["clocks"]["equity_cutoff_utc"],
            "--authorization-reference",
            f"{plan['authorization_reference']}:EQUITY-P11",
            "--expires-at",
            plan["clocks"]["hard_stop_utc"],
            "--output",
            str(authority_path),
        ),
        output=output,
        hard_stop=clocks["hard_stop_utc"],
        environment=environment,
    )
    authority = json.loads(authority_result.stdout)
    publication_report = output / "EQUITY-P11-PUBLICATION.json"
    try:
        _run(
            _equity_window_command(
                plan,
                output,
                history,
                suffix=authority["window_key_suffix"],
                report_name=publication_report.name,
                publication={
                    "path": authority_path,
                    "semantic_sha256": authority["semantic_sha256"],
                    "file_sha256": authority["file_sha256"],
                },
            ),
            output=output,
            hard_stop=clocks["hard_stop_utc"],
            environment=environment,
            timeout=360,
        )
        publication = json.loads(
            publication_report.read_text(encoding="utf-8")
        )
        if (
            publication.get("window_state") != "COMPLETE"
            or publication.get("pointer_advances") != 1
            or not publication.get("current_release_id")
            or publication.get("lane_errors")
        ):
            raise RuntimeError(
                "Equity P11 publication did not complete"
            )
        inspector = _run(
            _python(
                plan,
                "inspect_advanced_equity_p11_release.py",
                "--release-id",
                publication["current_release_id"],
                "--ticker",
                "SPY",
            ),
            output=output,
            hard_stop=clocks["hard_stop_utc"],
            environment=environment,
        )
        ticker = json.loads(inspector.stdout)
        if ticker.get("failed_checks"):
            raise RuntimeError("Equity P11 ticker reader gate failed")
        portal = _validate_equity_portal(
            plan,
            UUID(publication["current_release_id"]),
        )
    except BaseException:
        current_release_id = _equity_pointer(
            plan["equity"]["cohort_id"]
        )
        if current_release_id is not None:
            _run(
                _python(
                    plan,
                    "rollback_advanced_equity_p11_publication.py",
                    "--database",
                    plan["database"],
                    "--confirm-database-name",
                    plan["database"],
                    "--authority",
                    str(authority_path),
                    "--authority-semantic-sha256",
                    authority["semantic_sha256"],
                    "--authority-file-sha256",
                    authority["file_sha256"],
                    "--expected-current-release-id",
                    str(current_release_id),
                    "--report-path",
                    str(output / "EQUITY-P11-ROLLBACK.json"),
                ),
                output=output,
                hard_stop=clocks["hard_stop_utc"],
                environment=environment,
                allow_after_hard_stop=True,
            )
        raise
    return {
        "product": product,
        "publication": publication,
        "ticker_reader": ticker,
        "portal_reader": portal,
    }


def _equity_pointer(cohort_id):
    provider, pool = _provider()
    try:
        with provider.transaction(
            APP_ROLE,
            read_only=True,
            dict_cursor=True,
        ) as unit:
            unit.cursor.execute(
                "SELECT release_id::text "
                "FROM rt.portal_release_current WHERE cohort_id=%s "
                "AND provider_tier='MASSIVE_ADVANCED'",
                (cohort_id,),
            )
            row = unit.cursor.fetchone()
    finally:
        pool.closeall()
    return None if row is None else UUID(row["release_id"])


def _validate_equity_portal(plan, release_id):
    provider, pool = _provider()
    try:
        with provider.transaction(
            APP_ROLE,
            read_only=True,
            dict_cursor=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            cursor = unit.cursor
            cursor.execute(
                "SELECT current.release_id::text,current.cutoff "
                "FROM rt.portal_release_current AS current "
                "WHERE current.cohort_id=%s "
                "AND current.provider_tier='MASSIVE_ADVANCED'",
                (plan["equity"]["cohort_id"],),
            )
            pointer = cursor.fetchone()
            cursor.execute(
                "SELECT generation.generation_id::text,"
                "generation.status,generation.actual_members,"
                "generation.expected_members "
                "FROM rt.screening_current AS current "
                "JOIN rt.screening_generations AS generation "
                "USING(generation_id) WHERE current.cohort_id=%s",
                (plan["equity"]["cohort_id"],),
            )
            screening = cursor.fetchone()
            cursor.execute(
                "SELECT entry.value->>'snapshot_type' AS snapshot_type,"
                "entry.value->>'snapshot_key' AS snapshot_key,"
                "hot.payload FROM rt.portal_releases AS release "
                "JOIN LATERAL jsonb_array_elements("
                "release.manifest->'snapshots') AS entry(value) ON true "
                "JOIN rt.snapshot_catalog AS catalog "
                "ON catalog.snapshot_id="
                "(entry.value->>'snapshot_id')::uuid "
                "JOIN rt.portal_snapshot_hot AS hot "
                "ON hot.snapshot_id=catalog.snapshot_id "
                "AND hot.generated_at=catalog.generated_at "
                "WHERE release.release_id=%s",
                (str(release_id),),
            )
            snapshots = tuple(cursor.fetchall())
    finally:
        pool.closeall()
    by_key = {
        (row["snapshot_type"], row["snapshot_key"]): row["payload"]
        for row in snapshots
    }
    alerts = by_key.get(("STOCK_ALERTS_V1", "PLATFORM"))
    strategy_types = {
        snapshot_type
        for snapshot_type, key in by_key
        if key == "COHORT"
    }
    if (
        pointer is None
        or pointer["release_id"] != str(release_id)
        or screening is None
        or screening["status"] != "COMPLETE"
        or int(screening["actual_members"]) != 9
        or int(screening["expected_members"]) != 9
        or alerts is None
        or str(alerts.get("screening_generation_id"))
        != screening["generation_id"]
        or not {
            "DAILY_STRATEGIES_V1",
            "HOURLY_STRATEGIES_V1",
        }.issubset(strategy_types)
    ):
        raise RuntimeError(
            "Equity stored portal reader membership is incomplete"
        )
    return {
        "release_id": str(release_id),
        "screening_generation_id": screening["generation_id"],
        "screening_members": 9,
        "stock_alerts": True,
        "strategy_snapshot_types": sorted(strategy_types),
        "provider_fetches": 0,
        "detector_runs": 0,
    }


def _docker_snapshot(
    plan,
    output,
    hard_stop,
    *,
    allow_after_hard_stop=False,
):
    result = _run(
        (
            plan["docker_executable"],
            "ps",
            "--format",
            "{{.ID}}|{{.Names}}|{{.Image}}",
        ),
        output=output,
        hard_stop=hard_stop,
        timeout=30,
        allow_after_hard_stop=allow_after_hard_stop,
    )
    return {
        name: {"container_id": container_id, "image": image}
        for line in result.stdout.decode("utf-8").splitlines()
        for container_id, name, image in (line.split("|", 2),)
    }


def _compose(
    plan,
    environment,
    output,
    hard_stop,
    *arguments,
    check=True,
    allow_after_hard_stop=False,
):
    return _run(
        (
            plan["docker_executable"],
            "compose",
            "--file",
            REPOSITORY_ROOT / "docker-compose.realtime-v2.yml",
            "--profile",
            "realtime-v2-advanced-provider",
            "--profile",
            "realtime-v2-advanced",
            "--profile",
            "realtime-v3-options",
            *arguments,
        ),
        output=output,
        hard_stop=hard_stop,
        environment=environment,
        check=check,
        timeout=180,
        allow_after_hard_stop=allow_after_hard_stop,
    )


def _cleanup(plan, environment, output, hard_stop):
    services = (*EQUITY_SERVICES, *OPTIONS_SERVICES)
    stop = _compose(
        plan,
        environment,
        output,
        hard_stop,
        "stop",
        "--timeout",
        "60",
        *services,
        check=False,
        allow_after_hard_stop=True,
    ).returncode
    remove = _compose(
        plan,
        environment,
        output,
        hard_stop,
        "rm",
        "--force",
        *services,
        check=False,
        allow_after_hard_stop=True,
    ).returncode
    return {"stop": stop, "remove": remove}


def _source_identity(plan):
    path = REPOSITORY_ROOT / plan["qualification"]["report"]
    body = path.read_bytes()
    report = json.loads(body)
    if (
        hashlib.sha256(body).hexdigest()
        != plan["qualification"]["file_sha256"]
        or report.get("semantic_sha256")
        != plan["qualification"]["semantic_sha256"]
        or report.get("verdict") != "PASS"
    ):
        raise ValueError("provider-free qualification identity changed")
    drift = []
    for item in report["source"]["files"]:
        source = REPOSITORY_ROOT / item["path"]
        actual = hashlib.sha256(source.read_bytes()).hexdigest()
        if actual != item["sha256"]:
            drift.append(item["path"])
    if drift:
        raise ValueError(
            "qualified source drifted: " + ", ".join(drift)
        )


def _run_focused_tests(plan, clocks, output):
    _run(
        (
            plan["python_executable"],
            "-B",
            "-m",
            "pytest",
            "-p",
            "backend.tests.realtime.offline_io_guard",
            "backend/tests/realtime/test_advanced_ingress_plan.py",
            "backend/tests/realtime/test_realtime_v2_advanced_controller_script.py",
            "backend/tests/realtime/test_prepare_realtime_v2_advanced_ingress.py",
            "backend/tests/realtime/test_provision_realtime_v2_advanced_equity_scope.py",
            "backend/tests/realtime/test_advanced_options_read_pin.py",
            "backend/tests/realtime/test_run_advanced_options_v3_production.py",
            "backend/tests/realtime/test_advanced_options_v3_deployment.py",
            "-q",
            "--realtime-no-network",
            "--tb=short",
            "-p",
            "no:cacheprovider",
        ),
        output=output,
        hard_stop=clocks["hard_stop_utc"],
        timeout=180,
    )


def _reference_commands(plan, output):
    reference = plan["reference"]
    common = (
        "--database",
        plan["database"],
        "--confirm-database-name",
        plan["database"],
    )
    commands = [{
        "report": output / "TARGET9-REFERENCES.json",
        "provider_requests": 31,
        "command": _python(
            plan,
            "refresh_realtime_v2_references.py",
            *common,
            "--session-date",
            plan["session_date"],
            "--authorization-reference",
            f"{plan['authorization_reference']}:TARGET9-REFERENCES",
            "--provider-authorization-reference",
            "stocks-advanced",
            "--expected-cost",
            "included Massive Advanced reference entitlement",
            "--authorized-until",
            plan["clocks"]["reference_authorized_until_utc"],
            "--maximum-requests",
            "31",
            "--response-byte-limit",
            "2000000",
            "--total-byte-ceiling",
            "16000000",
            "--output",
            str(output / "TARGET9-REFERENCES.json"),
            "--execute",
        ),
    }]
    for basis in ("adjusted", "raw"):
        report_name = (
            "H-ADJUSTED.json"
            if basis == "adjusted"
            else "H-RAW.json"
        )
        arguments = [
            *common,
            "--cohort-id",
            reference["history_source_cohort_id"],
            "--session-date",
            reference["history_session_date"],
            "--authorized-until",
            plan["clocks"]["reference_authorized_until_utc"],
            "--maximum-provider-requests",
            "1",
            "--maximum-response-bytes",
            "2000001",
            "--response-byte-limit",
            "2000000",
            "--authorization-reference",
            f"{plan['authorization_reference']}:H:{basis.upper()}",
            "--provider-authorization-reference",
            "stocks-advanced",
            "--expected-cost",
            "included Massive Advanced grouped-daily entitlement",
            "--allow-late-identity-continuity",
            "--report-path",
            str(output / report_name),
            "--execute",
        ]
        if basis == "raw":
            arguments.append("--raw")
        commands.append({
            "report": output / report_name,
            "provider_requests": 1,
            "command": _python(
                plan,
                "repair_realtime_v2_adjusted_daily.py",
                *arguments,
            ),
        })
    commands.extend((
        {
            "report": output / "CORPORATE-ACTIONS.json",
            "provider_requests": 4,
            "command": _python(
            plan,
            "backfill_realtime_v2_corporate_actions.py",
            *common,
            "--cohort-id",
            plan["equity"]["cohort_id"],
            "--session-date",
            plan["session_date"],
            "--window-start",
            plan["session_date"],
            "--window-end",
            reference["corporate_action_window_end"],
            "--ticker",
            "QQQ",
            "--ticker",
            "SPY",
            "--authorized-until",
            plan["clocks"]["reference_authorized_until_utc"],
            "--maximum-provider-requests",
            "4",
            "--response-byte-limit",
            "5000000",
            "--total-byte-ceiling",
            "20000000",
            "--authorization-reference",
            f"{plan['authorization_reference']}:CORPORATE-ACTIONS",
            "--report-path",
            str(output / "CORPORATE-ACTIONS.json"),
            "--execute",
        ),
        },
        {
            "report": output / "TREASURY.json",
            "provider_requests": 1,
            "command": _python(
            plan,
            "backfill_realtime_v2_treasury_curve.py",
            *common,
            "--session-date",
            plan["session_date"],
            "--authorized-until",
            plan["clocks"]["reference_authorized_until_utc"],
            "--maximum-provider-requests",
            "1",
            "--response-byte-limit",
            "2000000",
            "--authorization-reference",
            f"{plan['authorization_reference']}:TREASURY",
            "--report-path",
            str(output / "TREASURY.json"),
            "--execute",
        ),
        },
        {
            "report": output / "A-CAPTURE-REPORT.json",
            "provider_requests": 2,
            "command": _python(
            plan,
            "prepare_advanced_options_v3_pre_session.py",
            *common,
            "--source-cohort-id",
            reference["selection_source_cohort_id"],
            "--session-date",
            plan["session_date"],
            "--authorized-until",
            plan["clocks"]["reference_authorized_until_utc"],
            "--reference-valid-until",
            plan["clocks"]["hard_stop_utc"],
            "--maximum-provider-requests",
            "2",
            "--maximum-response-decoded-bytes",
            "50000000",
            "--maximum-decoded-bytes",
            "100000000",
            "--authorization-reference",
            f"{plan['authorization_reference']}:A-CAPTURE",
            "--capture-path",
            str(output / "A-CAPTURE.json"),
            "--report-path",
            str(output / "A-CAPTURE-REPORT.json"),
            "--execute",
        ),
        },
    ))
    return commands


def _provider_effects(output):
    reports = (
        "TARGET9-REFERENCES.json",
        "H-ADJUSTED.json",
        "H-RAW.json",
        "CORPORATE-ACTIONS.json",
        "TREASURY.json",
        "A-CAPTURE-REPORT.json",
    )
    return sum(
        int(
            json.loads((output / name).read_text(encoding="utf-8"))
            .get("provider_requests", 0)
        )
        for name in reports
    )


def _run_reference_step(step, *, output, hard_stop):
    path = step["report"]
    if path.exists():
        report = json.loads(path.read_text(encoding="utf-8"))
        if (
            "COMPLETE" not in str(report.get("status", ""))
            or int(report.get("provider_requests") or 0)
            != step["provider_requests"]
        ):
            raise RuntimeError(
                f"retained reference step is not reusable: {path.name}"
            )
        return "REUSED"
    _run(
        step["command"],
        output=output,
        hard_stop=hard_stop,
    )
    report = json.loads(path.read_text(encoding="utf-8"))
    if (
        "COMPLETE" not in str(report.get("status", ""))
        or int(report.get("provider_requests") or 0)
        != step["provider_requests"]
    ):
        raise RuntimeError(
            f"reference step postcondition failed: {path.name}"
        )
    return "EXECUTED"


def _bind_equity_history(plan, clocks, output):
    path = output / "EQUITY-HISTORY-BINDING.json"
    _run(
        _python(
            plan,
            "bind_realtime_v2_advanced_history.py",
            "--target-cohort-id",
            plan["equity"]["cohort_id"],
            "--source-cohort-id",
            plan["equity_product"]["history_source_cohort_id"],
            "--session-date",
            plan["session_date"],
            "--authorization-reference",
            f"{plan['authorization_reference']}:EQUITY-HISTORY",
            "--report-path",
            str(path),
            "--execute",
        ),
        output=output,
        hard_stop=clocks["hard_stop_utc"],
        timeout=180,
    )
    report = json.loads(path.read_text(encoding="utf-8"))
    if (
        report.get("status") != "BOUND"
        or report.get("target_cohort_id")
        != plan["equity"]["cohort_id"]
        or report.get("provider_requests") != 0
    ):
        raise RuntimeError("target-9 Equity history binding failed")
    return {
        "report": path,
        "file_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "binding_sha256": report["binding_sha256"],
    }


def _repair_equity_weekly_history(plan, clocks, output):
    path = output / "EQUITY-WEEKLY-LINEAGE.json"
    if path.exists():
        report = json.loads(path.read_text(encoding="utf-8"))
    else:
        result = _run(
            _python(
                plan,
                "repair_realtime_v2_weekly_lineage.py",
                "--database",
                plan["database"],
                "--confirm-database-name",
                plan["database"],
                "--cohort-id",
                plan["equity_product"]["history_source_cohort_id"],
                "--session-date",
                plan["session_date"],
                "--authorization-reference",
                f"{plan['authorization_reference']}:WEEKLY-LINEAGE",
                "--report-path",
                str(path),
                "--execute",
            ),
            output=output,
            hard_stop=clocks["hard_stop_utc"],
            timeout=300,
        )
        report = json.loads(result.stdout)
    if (
        report.get("status")
        not in {"COMPLETE", "ALREADY_CURRENT"}
        or report.get("provider_requests") != 0
    ):
        raise RuntimeError("target-9 weekly lineage repair failed")
    return report


def _prepare_authorities(plan, clocks, output):
    capture = output / "A-CAPTURE.json"
    capture_body = capture.read_bytes()
    capture_value = json.loads(capture_body)
    capture_file_sha256 = hashlib.sha256(capture_body).hexdigest()
    _run(
        _python(
            plan,
            "prepare_advanced_options_v3_pre_session_authority.py",
            "--database",
            plan["database"],
            "--confirm-database-name",
            plan["database"],
            "--session-date",
            plan["session_date"],
            "--capture",
            str(capture),
            "--capture-semantic-sha256",
            capture_value["semantic_sha256"],
            "--capture-file-sha256",
            capture_file_sha256,
            "--authorization-reference",
            plan["authorization_reference"],
            "--package-nonce",
            plan["package_nonce"],
            "--hard-stop",
            plan["clocks"]["hard_stop_utc"],
            "--output",
            str(output / "A-AUTHORITY.json"),
            "--execute",
        ),
        output=output,
        hard_stop=clocks["hard_stop_utc"],
    )
    target_cohort_id = uuid5(
        NAMESPACE_URL,
        (
            f"stockscreener:{plan['authorization_reference']}:"
            f"{plan['package_nonce']}:MARKET_OPTIONS"
        ),
    )
    _run(
        _python(
            plan,
            "inspect_advanced_options_v3_history.py",
            "--database",
            plan["database"],
            "--confirm-database-name",
            plan["database"],
            "--session-date",
            plan["session_date"],
            "--target-cohort-id",
            str(target_cohort_id),
            "--known-at",
            datetime.now(UTC).isoformat(),
            "--authorization-reference",
            f"{plan['authorization_reference']}:H",
            "--output",
            str(output / "H-BINDING.json"),
        ),
        output=output,
        hard_stop=clocks["hard_stop_utc"],
    )


def _run_preflight(plan, clocks, output):
    path = output / "PREFLIGHT.json"
    _run(
        _python(
            plan,
            "audit_advanced_options_v3_operations.py",
            "--session-date",
            plan["session_date"],
            "--equity-cutoff-utc",
            plan["clocks"]["equity_cutoff_utc"],
            "--options-cutoff-utc",
            plan["clocks"]["options_cutoff_utc"],
            "--observed-at-utc",
            datetime.now(UTC).isoformat(),
            "--output",
            str(path),
        ),
        output=output,
        hard_stop=clocks["hard_stop_utc"],
        timeout=180,
    )
    report = json.loads(path.read_text(encoding="utf-8"))
    if report.get("verdict") != "GO_PROVIDER_AUTHORITY_REQUIRED":
        counts = {
            name: len(report["gap_report"]["buckets"][name])
            for name in (
                "MISSING_STORED",
                "STALE",
                "AMBIGUOUS",
                "REQUIRES_PROVIDER",
            )
        }
        raise RuntimeError(f"pre-session preflight blocked: {counts}")
    return report


def _equity_environment(plan):
    qualification_dir = (
        REPOSITORY_ROOT
        / plan["docker_qualification"]["support_directory"]
    )
    return {
        **os.environ,
        "DOCKER_CONTEXT": "default",
        "REALTIME_V2_BACKEND_IMAGE": plan["image_id"],
        "REALTIME_V2_DOCKER_SECRET_DIR": str(
            REPOSITORY_ROOT / plan["docker_secret_directory"]
        ),
        "REALTIME_V2_P8_PACKAGE_PATH": str(
            qualification_dir / "package.json"
        ),
        "REALTIME_V2_P8_BUNDLE_PATH": str(
            qualification_dir / "execution-bundle.json"
        ),
        "REALTIME_V2_ADVANCED_AUTHORIZATION_ID": (
            plan["equity"]["authorization_id"]
        ),
        "REALTIME_V2_ADVANCED_UNIVERSE_TARGET": "9",
        "REALTIME_V2_ADVANCED_STREAM_SESSIONS": "1",
        "REALTIME_V2_ADVANCED_CONNECTION_ATTEMPTS": "1",
        "REALTIME_V2_ADVANCED_DECODED_BYTES": "10485760",
        "REALTIME_V2_ADVANCED_REST_REPAIR_REQUESTS": "0",
        "REALTIME_V2_ADVANCED_REST_REPAIR_DECODED_BYTES": "0",
        "REALTIME_V2_ADVANCED_CATCH_UP_MODE": "AUTO",
        "REALTIME_V2_ADVANCED_PREFERRED_COHORT_ID": (
            plan["equity"]["cohort_id"]
        ),
        "REALTIME_V2_ADVANCED_COHORT_SELECTION_REFERENCE": (
            plan["equity"]["cohort_selection_reference"]
        ),
        "REALTIME_V2_ADVANCED_COHORT_ID": plan["equity"]["cohort_id"],
        "REALTIME_V2_ADVANCED_SESSION_DATE": plan["session_date"],
        "REALTIME_V2_ADVANCED_FIRST_CUTOFF": (
            plan["clocks"]["equity_cutoff_utc"]
        ),
        "REALTIME_V2_ADVANCED_AUTHORIZATION_REFERENCE": (
            plan["authorization_reference"]
        ),
    }


def _host_environment():
    loaded = {
        key: value
        for key, value in dotenv_values(BACKEND_DIR / ".env").items()
        if value is not None
    }
    python_path = os.pathsep.join(
        value
        for value in (
            str(BACKEND_DIR),
            os.environ.get("PYTHONPATH"),
        )
        if value
    )
    return {
        **loaded,
        **os.environ,
        "PYTHONPATH": python_path,
        "REALTIME_V2_EQUITY_PIPELINE": "MASSIVE_ADVANCED",
        "REALTIME_V2_OPTIONS_PIPELINE": "DISABLED",
    }


def _capture_l(plan, clocks, output):
    a_capture = json.loads(
        (output / "A-CAPTURE.json").read_text(encoding="utf-8")
    )
    admitted_contracts = tuple(sorted(
        a_capture["selected_snapshots"]
    ))
    if len(admitted_contracts) != 4:
        raise RuntimeError("A admitted contract membership changed")
    required_arguments = tuple(
        value
        for ticker in admitted_contracts
        for value in ("--required-contract-ticker", ticker)
    )
    _run(
        _python(
            plan,
            "prepare_advanced_options_v3_pre_session.py",
            "--database",
            plan["database"],
            "--confirm-database-name",
            plan["database"],
            "--source-cohort-id",
            plan["equity"]["cohort_id"],
            "--session-date",
            plan["session_date"],
            "--authorized-until",
            plan["clocks"]["options_rest_authorized_until_utc"],
            "--reference-valid-until",
            plan["clocks"]["hard_stop_utc"],
            "--maximum-provider-requests",
            "2",
            "--maximum-response-decoded-bytes",
            "50000000",
            "--maximum-decoded-bytes",
            "100000000",
            "--authorization-reference",
            f"{plan['authorization_reference']}:L",
            *required_arguments,
            "--capture-path",
            str(output / "L-CAPTURE.json"),
            "--report-path",
            str(output / "L-CAPTURE-REPORT.json"),
            "--execute",
        ),
        output=output,
        hard_stop=clocks["hard_stop_utc"],
    )


def _market_loop(plan, clocks, output, environment):
    def identity(path, semantic_field="semantic_sha256"):
        body = path.read_bytes()
        return (
            json.loads(body)[semantic_field],
            hashlib.sha256(body).hexdigest(),
        )

    qualification = REPOSITORY_ROOT / plan["qualification"]["report"]
    docker_report = (
        REPOSITORY_ROOT / plan["docker_qualification"]["report"]
    )
    l_capture = output / "L-CAPTURE.json"
    authority = output / "A-AUTHORITY.json"
    history = output / "H-BINDING.json"
    l_semantic, l_file = identity(l_capture)
    authority_semantic, authority_file = identity(authority)
    history_semantic, history_file = identity(history)
    result = _run(
        _python(
            plan,
            "run_advanced_options_v3_market_loop.py",
            "--database",
            plan["database"],
            "--confirm-database-name",
            plan["database"],
            "--session-date",
            plan["session_date"],
            "--hard-stop",
            plan["clocks"]["hard_stop_utc"],
            "--equity-cutoff",
            plan["clocks"]["equity_cutoff_utc"],
            "--equity-cohort-id",
            plan["equity"]["cohort_id"],
            "--image-id",
            plan["image_id"],
            "--docker-executable",
            plan["docker_executable"],
            "--qualification-report",
            str(qualification),
            "--qualification-semantic-sha256",
            plan["qualification"]["semantic_sha256"],
            "--qualification-file-sha256",
            plan["qualification"]["file_sha256"],
            "--docker-qualification-report",
            str(docker_report),
            "--docker-qualification-semantic-sha256",
            plan["docker_qualification"]["semantic_sha256"],
            "--docker-qualification-file-sha256",
            plan["docker_qualification"]["file_sha256"],
            "--pre-session-capture",
            str(l_capture),
            "--pre-session-capture-semantic-sha256",
            l_semantic,
            "--pre-session-capture-file-sha256",
            l_file,
            "--history-binding-report",
            str(history),
            "--history-binding-semantic-sha256",
            history_semantic,
            "--history-binding-file-sha256",
            history_file,
            "--pre-session-authority",
            str(authority),
            "--pre-session-authority-semantic-sha256",
            authority_semantic,
            "--pre-session-authority-file-sha256",
            authority_file,
            "--authorization-reference",
            plan["authorization_reference"],
            "--package-nonce",
            plan["package_nonce"],
            "--output-directory",
            str(output / "MARKET-LOOP"),
            "--maximum-window-attempts",
            "4",
            "--execute",
        ),
        output=output,
        hard_stop=clocks["hard_stop_utc"],
        environment=environment,
        timeout=3600,
    )
    return json.loads(result.stdout)


def execute(args):
    plan, clocks = _load_plan(args)
    output = args.output_directory
    if args.resume:
        status = json.loads(
            (output / "RUN-STATUS.json").read_text(encoding="utf-8")
        )
        previous = json.loads(
            Path(status["report"]).read_text(encoding="utf-8")
        )
        if (
            status.get("phase") != "BLOCKED"
            or previous.get("status") != "BLOCKED"
            or previous.get("plan_file_sha256") not in {
                args.plan_file_sha256,
                plan.get("supersedes_plan_file_sha256"),
            }
        ):
            raise ValueError("one-shot resume evidence changed")
    else:
        output.mkdir(parents=True)
    report = {
        "contract": "advanced-realtime-one-shot-report-v1",
        "plan_file_sha256": args.plan_file_sha256,
        "started_at_utc": datetime.now(UTC),
        "status": "RUNNING",
        "forbidden_effects": dict(ZERO_FORBIDDEN_EFFECTS),
    }
    environment = _equity_environment(plan)
    host_environment = _host_environment()
    baseline = {}
    cleanup = {"status": "NOT_ATTEMPTED"}
    try:
        _sleep_until(
            clocks["preflight_start_utc"],
            clocks["hard_stop_utc"],
            output,
            "WAITING_FOR_PREFLIGHT",
        )
        _source_identity(plan)
        _run_focused_tests(plan, clocks, output)
        baseline = _docker_snapshot(
            plan,
            output,
            clocks["hard_stop_utc"],
        )
        _status(output, "PREPARING_REFERENCES")
        reference_steps = {}
        for step in _reference_commands(plan, output):
            reference_steps[step["report"].name] = (
                _run_reference_step(
                    step,
                    output=output,
                    hard_stop=clocks["hard_stop_utc"],
                )
            )
        reference_requests = _provider_effects(output)
        if reference_requests > plan["reference"]["request_ceiling"]:
            raise RuntimeError(
                "pre-session reference request ceiling exceeded"
            )
        weekly_history = _repair_equity_weekly_history(
            plan,
            clocks,
            output,
        )
        history = _bind_equity_history(plan, clocks, output)
        _prepare_authorities(plan, clocks, output)
        _run_preflight(plan, clocks, output)
        _status(output, "STARTING_EQUITY")
        _compose(
            plan,
            environment,
            output,
            clocks["hard_stop_utc"],
            "up",
            "--detach",
            "--no-build",
            "--wait",
            *EQUITY_SERVICES,
        )
        equity_receipt = _wait_for_equity_receipt(
            plan,
            clocks,
            output,
        )
        _compose(
            plan,
            environment,
            output,
            clocks["hard_stop_utc"],
            "stop",
            "--timeout",
            "60",
            *EQUITY_SERVICES,
        )
        _compose(
            plan,
            environment,
            output,
            clocks["hard_stop_utc"],
            "rm",
            "--force",
            *EQUITY_SERVICES,
        )
        _status(output, "CAPTURING_L")
        _capture_l(plan, clocks, output)
        _status(
            output,
            "RUNNING_EQUITY_PRODUCT_AND_OPTIONS",
        )
        with ThreadPoolExecutor(max_workers=1) as executor:
            options_future = executor.submit(
                _market_loop,
                plan,
                clocks,
                output,
                host_environment,
            )
            equity_product = _run_equity_product(
                plan,
                clocks,
                output,
                history,
                host_environment,
            )
            market = options_future.result()
        if (
            market.get("verdict") != "PASS"
            or market.get("stored_reads", {}).get("status")
            != "DURABLE_PIN_ENABLED"
        ):
            raise RuntimeError(
                "Options market loop did not reach portal-ready reads"
            )
        report.update({
            "status": "PASS",
            "reference_provider_requests": reference_requests,
            "reference_steps": reference_steps,
            "equity_receipt": equity_receipt,
            "equity_history": {
                "binding_sha256": history["binding_sha256"],
                "file_sha256": history["file_sha256"],
                "weekly_status": weekly_history["status"],
            },
            "equity_product": equity_product,
            "market_loop": market,
        })
    except BaseException as error:
        report["status"] = "BLOCKED"
        report["error"] = {
            "type": type(error).__name__,
            "message": str(error),
        }
    finally:
        try:
            cleanup = _cleanup(
                plan,
                environment,
                output,
                clocks["hard_stop_utc"],
            )
            after = _docker_snapshot(
                plan,
                output,
                clocks["hard_stop_utc"],
                allow_after_hard_stop=True,
            )
            cleanup["unrelated_unchanged"] = all(
                name in after
                and after[name] == identity
                for name, identity in baseline.items()
                if "realtime-v2" not in name
            )
            cleanup["status"] = (
                "PASS"
                if cleanup["stop"] == 0
                and cleanup["remove"] == 0
                and cleanup["unrelated_unchanged"]
                else "FAILED"
            )
        except BaseException as cleanup_error:
            cleanup = {
                "status": "FAILED",
                "type": type(cleanup_error).__name__,
                "message": str(cleanup_error),
            }
        report["cleanup"] = cleanup
        report["completed_at_utc"] = datetime.now(UTC)
        report["semantic_sha256"] = canonical_sha256(report)
        final_path = output / (
            "advanced-realtime-one-shot-"
            f"{report['semantic_sha256'][:16]}.json"
        )
        _write(final_path, report, exclusive=True)
        _status(
            output,
            report["status"],
            report=str(final_path),
            semantic_sha256=report["semantic_sha256"],
            cleanup=cleanup,
        )
    return report


def main(argv=None):
    report = execute(parse_args(argv))
    print(json.dumps(report, indent=2, sort_keys=True, default=str))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
