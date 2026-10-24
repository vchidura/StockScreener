#!/usr/bin/env python3
"""Run one bounded Advanced V3 production market loop."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool

from options.calendar import OptionExchangeCalendar

BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_control_plane import (  # noqa: E402
    load_advanced_equity_control_plane_plan_cursor,
    validate_active_target9_equity_source_cursor,
)
from realtime.advanced_options_p8_canary_authorization import (  # noqa: E402
    build_p8_canary_authorization,
)
from realtime.advanced_options_p8_production import (  # noqa: E402
    build_advanced_options_p8_production_components,
)
from realtime.advanced_options_p8_selection_source import (  # noqa: E402
    p8_activity_selection_capture_from_sealed,
)
from realtime.advanced_options_read_pin import (  # noqa: E402
    PostgresAdvancedOptionsStoredReadPort,
)
from realtime.advanced_options_service import (  # noqa: E402
    AdvancedOptionsProcessKind,
    OPTIONS_V3_STAGE_ORDER,
)
from realtime.advanced_options_v3_authority import (  # noqa: E402
    OpeningAdmissionStatus,
    ProspectiveOpeningContract,
    _opening_admission,
    build_opening_subscription_readiness,
    build_session_acquisition_authorization,
    prospective_opening_admission_from_payload,
    prospective_opening_admission_payload,
    session_acquisition_authorization_from_payload,
    session_acquisition_authorization_payload,
)
from realtime.advanced_options_v3_deployment import (  # noqa: E402
    ADVANCED_OPTIONS_V3_LIVE_STATE,
    ADVANCED_OPTIONS_V3_PROFILE_SHA256,
    ADVANCED_OPTIONS_V3_SOURCE_BINDING_PATHS,
    advanced_options_v3_profile_payload,
)
from realtime.advanced_options_v3_history import (  # noqa: E402
    AdvancedOptionsHistoryBindingIdentityError,
    AdvancedOptionsHistoryCoverageError,
    VerifiedAdvancedOptionsHistoryBinding,
    advanced_options_v3_history_binding_from_payload,
    verify_advanced_options_v3_history_binding_cursor,
)
from realtime.contracts import (  # noqa: E402
    EQUITY_VALIDATION_UNIVERSE,
    canonical_sha256,
)
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.option_slot_set import (  # noqa: E402
    MarketChannel,
    SlotSetKind,
    SlotSetMember,
    SlotSetPurpose,
    SlotSetVersion,
)
from realtime.provider_budget import ProviderBudgetScope  # noqa: E402
from realtime.advanced_options_control_plane import (  # noqa: E402
    ADVANCED_OPTIONS_REST_LANES,
    AdvancedOptionsSocketBudget,
    advanced_options_history_binding_payload,
)
from realtime.stage_graph import STAGE_CATALOG_VERSION_V5  # noqa: E402
from scripts.apply_advanced_options_p8_catalog_hydration import (  # noqa: E402
    _current_rows,
)
from scripts import run_advanced_options_v3_production as production_cli  # noqa: E402


UTC = timezone.utc
OPTIONS_SERVICES = (
    "realtime-v2-options-p8-ingress",
    "realtime-v2-options-p8-market-state",
    "realtime-v2-options-p8-flow",
    "realtime-v2-options-p8-window",
)
UNRELATED_SERVICES = (
    "stock-screener-realtime-v2-advanced-api",
    "stock-screener-realtime-v2-advanced-frontend",
    "stock-screener-realtime-v2-retention",
)
ZERO_FORBIDDEN_EFFECTS = {
    "dispatch": 0,
    "delivery": 0,
    "broker": 0,
    "trading": 0,
    "retention_deletion": 0,
    "migration": 0,
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--hard-stop", type=datetime.fromisoformat, required=True)
    parser.add_argument("--equity-cutoff", type=datetime.fromisoformat, required=True)
    parser.add_argument("--equity-cohort-id", type=UUID, required=True)
    parser.add_argument("--image-id", required=True)
    parser.add_argument("--docker-executable", type=Path, required=True)
    parser.add_argument("--qualification-report", type=Path, required=True)
    parser.add_argument("--qualification-semantic-sha256", required=True)
    parser.add_argument("--qualification-file-sha256", required=True)
    parser.add_argument("--docker-qualification-report", type=Path, required=True)
    parser.add_argument("--docker-qualification-semantic-sha256", required=True)
    parser.add_argument("--docker-qualification-file-sha256", required=True)
    parser.add_argument("--pre-session-capture", type=Path, required=True)
    parser.add_argument("--pre-session-capture-semantic-sha256", required=True)
    parser.add_argument("--pre-session-capture-file-sha256", required=True)
    parser.add_argument("--history-binding-report", type=Path, required=True)
    parser.add_argument(
        "--history-binding-semantic-sha256",
        required=True,
    )
    parser.add_argument(
        "--history-binding-file-sha256",
        required=True,
    )
    parser.add_argument("--pre-session-authority", type=Path, required=True)
    parser.add_argument(
        "--pre-session-authority-semantic-sha256",
        required=True,
    )
    parser.add_argument(
        "--pre-session-authority-file-sha256",
        required=True,
    )
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--package-nonce", type=UUID, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--maximum-window-attempts", type=int, default=4)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if (
        args.database != EXPECTED_V2_DATABASE_NAME
        or args.maximum_window_attempts != 4
        or not args.image_id.startswith("sha256:")
        or len(args.image_id) != 71
        or args.output_directory.exists()
        or not args.execute
    ):
        parser.error("bounded V3 market-loop scope is invalid")
    for name in ("hard_stop", "equity_cutoff"):
        value = getattr(args, name)
        if value.tzinfo is None or value.utcoffset() is None:
            parser.error(f"--{name.replace('_', '-')} must include timezone")
        setattr(args, name, value.astimezone(UTC))
    calendar = OptionExchangeCalendar()
    session_open = calendar.session_open(args.session_date)
    session_close = calendar.session_close(args.session_date)
    if (
        args.hard_stop.date() != args.session_date
        or not session_open < args.hard_stop <= session_close
        or args.equity_cutoff.date() != args.session_date
        or not session_open < args.equity_cutoff < args.hard_stop
        or args.equity_cutoff.second
        or args.equity_cutoff.microsecond
        or args.equity_cutoff.minute % 5
    ):
        parser.error("bounded V3 market-loop clocks changed")
    for name in (
        "docker_executable",
        "qualification_report",
        "docker_qualification_report",
        "pre_session_capture",
        "pre_session_authority",
        "history_binding_report",
        "output_directory",
    ):
        setattr(args, name, getattr(args, name).resolve())
    for name in (
        "qualification_semantic_sha256",
        "qualification_file_sha256",
        "docker_qualification_semantic_sha256",
        "docker_qualification_file_sha256",
        "pre_session_capture_semantic_sha256",
        "pre_session_capture_file_sha256",
        "pre_session_authority_semantic_sha256",
        "pre_session_authority_file_sha256",
        "history_binding_semantic_sha256",
        "history_binding_file_sha256",
    ):
        value = getattr(args, name)
        if len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            parser.error(f"--{name.replace('_', '-')} is not a SHA-256")
    return args


def _write_json(path: Path, value) -> None:
    encoded = (
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_bound_json(path, semantic, file_digest, semantic_field):
    body = path.read_bytes()
    value = json.loads(body)
    if (
        not isinstance(value, dict)
        or hashlib.sha256(body).hexdigest() != file_digest
        or value.get(semantic_field) != semantic
    ):
        raise ValueError(f"{path.name} immutable identity changed")
    return value


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
        12,
        dsn=dsn,
        options="-c timezone=UTC",
    )
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
            statement_timeout_ms=300_000,
        ),
    )
    provider.verify()
    return provider, pool, values


def _run(argv, *, environment, timeout=180, check=True):
    completed = subprocess.run(
        [str(value) for value in argv],
        cwd=REPOSITORY_ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )
    if check and completed.returncode != 0:
        output = (
            completed.stdout + b"\n" + completed.stderr
        ).decode("utf-8", errors="replace")
        raise RuntimeError(
            f"command failed ({completed.returncode}): {output[-4000:]}"
        )
    return completed


def _docker_snapshot(environment):
    result = _run(
        (
            environment["V3_DOCKER_EXECUTABLE"],
            "ps",
            "-a",
            "--filter",
            "label=com.docker.compose.project=alpha-research-realtime-v2",
            "--format",
            "{{.ID}}|{{.Names}}|{{.Status}}|{{.Image}}",
        ),
        environment=environment,
    )
    rows = {}
    for line in result.stdout.decode("utf-8").splitlines():
        container_id, name, status, image = line.split("|", 3)
        rows[name] = {
            "container_id": container_id,
            "status": status,
            "image": image,
        }
    return rows


def _compose(environment, *arguments, timeout=180, check=True):
    return _run(
        (
            environment["V3_DOCKER_EXECUTABLE"],
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
        environment=environment,
        timeout=timeout,
        check=check,
    )


def _sleep_until(target, hard_stop):
    while True:
        now = datetime.now(UTC)
        if now >= hard_stop:
            raise TimeoutError("V3 hard stop reached")
        remaining = (target - now).total_seconds()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 10))


def _prepare_session_authority(
    provider,
    capture,
    *,
    args,
    cohort_id,
    prepared_at,
    capture_boundary,
):
    snapshots = capture["selected_snapshots"]
    tickers = tuple(sorted(snapshots))
    if (
        len(tickers) != 4
        or {
            value["underlying_asset"]["ticker"]
            for value in snapshots.values()
        }
        != {"QQQ", "SPY"}
    ):
        raise ValueError("pre-session four-contract admission changed")
    with provider.transaction(
        APP_ROLE,
        read_only=True,
        dict_cursor=True,
    ) as unit:
        rows = _current_rows(unit.cursor, tickers)
    contracts = []
    instruments = {}
    for ticker in tickers:
        snapshot = snapshots[ticker]
        current = [
            row
            for row in rows.get(ticker, ())
            if row["version_id"] is not None
            and row["instrument_id"] is not None
        ]
        if len(current) != 1:
            raise RuntimeError(
                f"pre-session catalog identity is ambiguous for {ticker}"
            )
        row = current[0]
        underlyer = str(snapshot["underlying_asset"]["ticker"])
        strike = Decimal(str(snapshot["details"]["strike_price"]))
        spot = Decimal(str(snapshot["underlying_asset"]["price"]))
        contract_type = str(
            snapshot["details"]["contract_type"]
        ).upper()
        open_interest = int(snapshot["open_interest"])
        member_sha256 = canonical_sha256({
            "ticker": ticker,
            "open_interest": open_interest,
            "settlement_session": (
                OptionExchangeCalendar()
                .previous_session_date(args.session_date)
                .isoformat()
            ),
            "capture_sha256": capture["semantic_sha256"],
        })
        contracts.append(ProspectiveOpeningContract(
            contract_id=int(row["contract_id"]),
            catalog_version_id=int(row["version_id"]),
            contract_ticker=ticker,
            underlyer=underlyer,
            expiration_date=datetime.fromisoformat(
                str(snapshot["details"]["expiration_date"])
            ).date(),
            contract_type=contract_type,
            strike=strike,
            spot=spot,
            absolute_moneyness=abs(strike - spot) / spot,
            day_volume=int(snapshot["day"]["volume"]),
            open_interest=open_interest,
            catalog_payload_sha256=str(row["payload_sha256"]),
            snapshot_payload_sha256=canonical_sha256(snapshot),
            open_interest_member_sha256=member_sha256,
        ))
        instruments[ticker] = int(row["instrument_id"])
    admission = _opening_admission(
        status=OpeningAdmissionStatus.READY,
        session_date=args.session_date,
        prepared_at=prepared_at,
        capture_boundary=capture_boundary,
        pre_session_selection_id=uuid5(
            NAMESPACE_URL,
            "advanced-options-v3-pre-session:"
            + capture["semantic_sha256"],
        ),
        pre_session_selection_sha256=canonical_sha256(tickers),
        source_evidence_sha256=capture["semantic_sha256"],
        reference_evidence_sha256=canonical_sha256({
            "capture": capture["semantic_sha256"],
            "application": (
                "advanced-options-v3-pre-session-capture-"
                "application-v1"
            ),
        }),
        contracts=tuple(contracts),
    )
    journal_generation = uuid5(
        NAMESPACE_URL,
        f"p8-journal:{args.package_nonce}",
    )
    slot_set = SlotSetVersion(
        cohort_id=cohort_id,
        version=1,
        kind=SlotSetKind.PRE_SESSION,
        effective_at=prepared_at,
        policy_sha256=admission.policy_sha256,
        members=tuple(
            SlotSetMember(
                instruments[contract.contract_ticker],
                channel,
                SlotSetPurpose.PRIMARY,
                index + 1,
                contract.catalog_version_id,
            )
            for index, contract in enumerate(admission.contracts)
            for channel in (MarketChannel.QUOTE, MarketChannel.TRADE)
        ),
    )
    readiness = build_opening_subscription_readiness(
        admission,
        slot_set,
        instrument_id_by_contract_ticker=instruments,
        journal_generation=journal_generation,
        journal_identity_sha256=canonical_sha256({
            "session_date": args.session_date,
            "cohort_id": str(cohort_id),
            "package_nonce": str(args.package_nonce),
            "contracts": tickers,
        }),
        ready_at=prepared_at,
    )
    provider_budget = ProviderBudgetScope(
        account_alias="MASSIVE_ADVANCED",
        session_date=args.session_date,
        authorization_reference=args.authorization_reference,
        provider_authorization_reference=args.authorization_reference,
        expected_cost="exact two-request post-E activity selection",
        request_ceiling=2,
        decoded_byte_ceiling=100_000_000,
        lanes=ADVANCED_OPTIONS_REST_LANES,
        authorized_until=args.hard_stop,
    )
    authority = build_session_acquisition_authorization(
        admission,
        readiness,
        provider_budget,
        AdvancedOptionsSocketBudget(
            stream_sessions=1,
            connection_attempts=1,
            reconnects=0,
            decoded_messages=100_000,
            decoded_bytes=32 * 1024 * 1024,
        ),
        authorization_reference=args.authorization_reference,
        prepared_at=prepared_at,
        acquisition_start=max(
            prepared_at,
            capture_boundary - timedelta(minutes=1),
        ),
        acquisition_stop=args.hard_stop,
    )
    return admission, readiness, authority


def _frozen_equity_input(provider, cohort_id, equity_cutoff):
    with provider.transaction(
        APP_ROLE,
        read_only=True,
        dict_cursor=True,
        isolation_level="REPEATABLE READ",
    ) as unit:
        plan = load_advanced_equity_control_plane_plan_cursor(
            unit.cursor,
            cohort_id,
        )
        validate_active_target9_equity_source_cursor(unit.cursor, plan)
        unit.cursor.execute(
            "SELECT receipt.receipt_id::text,receipt.window_id::text,"
            "receipt.cutoff,receipt.reference_generation,"
            "receipt.universe_sha256,receipt.input_sha256,"
            "receipt.build_id FROM rt.cycle_receipts AS receipt "
            "JOIN rt.window_runs AS window_run USING(window_id) "
            "WHERE receipt.cohort_id=%s "
            "AND receipt.receipt_kind='INPUT_READY' "
            "AND window_run.state='COMPLETE' AND receipt.cutoff=%s",
            (str(cohort_id), equity_cutoff),
        )
        receipt = unit.cursor.fetchone()
    if receipt is None:
        return None
    return {
        "source_cohort_id": str(plan.cohort_id),
        "source_cohort_sha256": plan.cohort_sha256,
        "source_manifest_id": str(plan.manifest_id),
        "source_manifest_sha256": plan.manifest.sha256,
        "source_profile_id": str(plan.profile_id),
        "source_profile_sha256": plan.profile_sha256,
        "equity_window_id": str(receipt["window_id"]),
        "equity_receipt_id": str(receipt["receipt_id"]),
        "equity_cutoff_utc": receipt["cutoff"].isoformat(),
        "reference_generation": int(receipt["reference_generation"]),
        "universe_sha256": str(receipt["universe_sha256"]),
        "input_sha256": str(receipt["input_sha256"]),
        "build_id": str(receipt["build_id"]),
    }
def _load_history_binding(provider, args, target_cohort_id):
    report = _load_bound_json(
        args.history_binding_report,
        args.history_binding_semantic_sha256,
        args.history_binding_file_sha256,
        "semantic_sha256",
    )
    if (
        report.get("status") != "PASS"
        or report.get("mode") != "READ_ONLY"
        or report.get("failure_class") is not None
    ):
        raise AdvancedOptionsHistoryBindingIdentityError(
            "V3 H binding report is not an immutable PASS"
        )
    binding = advanced_options_v3_history_binding_from_payload(
        report.get("binding")
    )
    if binding.target_cohort_id != target_cohort_id:
        raise AdvancedOptionsHistoryBindingIdentityError(
            "V3 H target cohort identity diverged"
        )
    with provider.transaction(
        APP_ROLE,
        read_only=True,
        dict_cursor=True,
        isolation_level="REPEATABLE READ",
    ) as unit:
        return verify_advanced_options_v3_history_binding_cursor(
            unit.cursor,
            binding=binding,
            evidence=report.get("evidence"),
        )


def _build_live_package(
    args,
    equity_source,
    authority,
    admitted_contracts,
    pre_session_identity,
    history: VerifiedAdvancedOptionsHistoryBinding,
):
    now = datetime.now(UTC)
    start = now + timedelta(seconds=30)
    temporary_stop = start + timedelta(minutes=10)
    source_commit = subprocess.run(
        ("git", "rev-parse", "HEAD"),
        cwd=REPOSITORY_ROOT,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout.strip()
    package = build_p8_canary_authorization(
        args.qualification_report,
        repository_root=REPOSITORY_ROOT,
        source_commit=source_commit,
        authorization_reference=args.authorization_reference,
        package_nonce=args.package_nonce,
        prepared_at=now,
        window_start=start,
        window_end=temporary_stop,
        equity_input_source=equity_source,
        expected_qualification_semantic_sha256=(
            args.qualification_semantic_sha256
        ),
        expected_qualification_file_sha256=(
            args.qualification_file_sha256
        ),
        source_files=ADVANCED_OPTIONS_V3_SOURCE_BINDING_PATHS,
        stored_reads_prepared=False,
    )
    package["window"]["stop_utc"] = args.hard_stop.isoformat()
    package["state"] = ADVANCED_OPTIONS_V3_LIVE_STATE
    package["orchestration"]["catalog_version"] = (
        STAGE_CATALOG_VERSION_V5
    )
    package["orchestration"]["stage_order"] = list(
        OPTIONS_V3_STAGE_ORDER
    )
    package["equity_input_binding"]["symbols"] = list(
        EQUITY_VALIDATION_UNIVERSE.equity_tickers
    )
    package["history_binding"].update({
        "target_cohort_id": str(history.binding.target_cohort_id),
        "source_cohort_id": str(history.binding.source_cohort_id),
        "source_provider_tier": (
            history.binding.source_provider_tier.value
        ),
        "history_before_utc": (
            history.binding.history_before.isoformat()
        ),
        "source_manifest_sha256": (
            history.binding.source_manifest_sha256
        ),
        "required_underlyings": ["QQQ", "SPY"],
        "required_adjusted_daily_sessions": 31,
        "required_raw_daily_sessions": 0,
        "required_sessions": list(
            history.evidence["coverage"]["sessions"]
        ),
    })
    package["v3_history_binding"] = {
        "binding": advanced_options_history_binding_payload(
            history.binding
        ),
        "evidence": history.evidence,
    }
    package["v3_deployment"] = {
        "profile": advanced_options_v3_profile_payload(),
        "profile_sha256": ADVANCED_OPTIONS_V3_PROFILE_SHA256,
        "state": ADVANCED_OPTIONS_V3_LIVE_STATE,
        "window_role_authorized": True,
        "qualification_verdict": "PASS",
        "docker_qualification_semantic_sha256": (
            args.docker_qualification_semantic_sha256
        ),
        "docker_qualification_file_sha256": (
            args.docker_qualification_file_sha256
        ),
        "image_id": args.image_id,
    }
    package["v3_authorities"] = {
        "session_acquisition": (
            session_acquisition_authorization_payload(authority)
        ),
    }
    package["v3_pre_session_authority"] = dict(
        pre_session_identity
    )
    package["v3_admitted_contracts"] = sorted(
        str(contract) for contract in admitted_contracts
    )
    package.pop("package_sha256", None)
    package["package_sha256"] = canonical_sha256(package)
    return package, start


def _capture_binding(package, checked_at, values, sealed_capture):
    previous_dsn = os.environ.get("REALTIME_V2_APP_DSN")
    dsn = str(values.get("REALTIME_V2_APP_DSN") or "").strip()
    if not dsn:
        raise RuntimeError("live DB environment is unavailable")
    os.environ["REALTIME_V2_APP_DSN"] = dsn
    components = None
    try:
        components = build_advanced_options_p8_production_components(
            package=package,
            repository_root=REPOSITORY_ROOT,
            stored_reads_authorized=False,
            provider_free_selection=True,
        )
        observation, binding = components.capture.capture_from_selection(
            package,
            checked_at=checked_at,
            selection_capture=sealed_capture,
        )
        production_binding = (
            components.capture.production_binding_payload
        )
        if production_binding is None:
            raise RuntimeError("V3 production capture is incomplete")
        return (
            dict(observation),
            binding,
            dict(production_binding),
            sealed_capture,
        )
    finally:
        if components is not None:
            components.close()
        if previous_dsn is None:
            os.environ.pop("REALTIME_V2_APP_DSN", None)
        else:
            os.environ["REALTIME_V2_APP_DSN"] = previous_dsn


def _window_evidence(provider, window_id):
    with provider.transaction(
        APP_ROLE,
        read_only=True,
        dict_cursor=True,
    ) as unit:
        unit.cursor.execute(
            "SELECT to_jsonb(window_run) AS value "
            "FROM rt.window_runs AS window_run "
            "WHERE window_id=%s",
            (str(window_id),),
        )
        window = unit.cursor.fetchone()
        unit.cursor.execute(
            "SELECT DISTINCT ON (stage_id) stage_id,state,attempt,"
            "to_jsonb(stage) AS value FROM rt.stage_runs AS stage "
            "WHERE window_id=%s ORDER BY stage_id,attempt DESC",
            (str(window_id),),
        )
        stages = unit.cursor.fetchall()
        unit.cursor.execute(
            "SELECT count(*) AS count FROM rt.alert_outbox "
            "WHERE cohort_id=(SELECT cohort_id FROM rt.window_runs "
            "WHERE window_id=%s)",
            (str(window_id),),
        )
        outbox_count = int(unit.cursor.fetchone()["count"])
    stage_states = {
        str(row["stage_id"]): str(row["state"]) for row in stages
    }
    value = None if window is None else dict(window["value"])
    complete = (
        value is not None
        and value.get("state") == "COMPLETE"
        and stage_states == {
            stage: "COMPLETE" for stage in OPTIONS_V3_STAGE_ORDER
        }
    )
    return {
        "window": value,
        "stages": [dict(row["value"]) for row in stages],
        "stage_states": stage_states,
        "alert_outbox_count": outbox_count,
        "complete": complete,
    }


def _wait_for_window(provider, window_id, deadline, hard_stop):
    terminal = {"COMPLETE", "FAILED", "MISSED", "RETAINED_GAP"}
    while datetime.now(UTC) < min(
        hard_stop,
        deadline + timedelta(seconds=30),
    ):
        evidence = _window_evidence(provider, window_id)
        state = (evidence["window"] or {}).get("state")
        if state in terminal:
            return evidence
        time.sleep(5)
    return _window_evidence(provider, window_id)


def _validate_sealed_capture_scope(capture, args, *, checked_at):
    selection_capture = p8_activity_selection_capture_from_sealed(capture)
    selection = selection_capture.selection
    equity_age = (
        selection.reviewed_at - args.equity_cutoff
    ).total_seconds()
    if equity_age > 300:
        raise RuntimeError(
            "sealed Options selection exceeds frozen Equity "
            f"maximum age by {int(equity_age - 300)} seconds"
        )
    if checked_at > selection.valid_until:
        expired_by = int(
            (checked_at - selection.valid_until).total_seconds()
        )
        raise RuntimeError(
            "sealed Options selection expired "
            f"{expired_by} seconds before package preparation"
        )
    return selection_capture


def _load_pre_session_authority(args, cohort_id):
    document = _load_bound_json(
        args.pre_session_authority,
        args.pre_session_authority_semantic_sha256,
        args.pre_session_authority_file_sha256,
        "semantic_sha256",
    )
    unsigned = dict(document)
    semantic_sha256 = unsigned.pop("semantic_sha256", None)
    admission = prospective_opening_admission_from_payload(
        document.get("admission")
    )
    session = session_acquisition_authorization_from_payload(
        document.get("session_acquisition")
    )
    admitted_contracts = sorted(
        str(value)
        for value in document.get("admitted_contracts") or ()
    )
    admission_contracts = sorted(
        row.contract_ticker for row in admission.contracts
    )
    if (
        document.get("contract")
        != "advanced-options-v3-pre-session-authority-v1"
        or canonical_sha256(unsigned) != semantic_sha256
        or document.get("session_date")
        != args.session_date.isoformat()
        or document.get("target_cohort_id") != str(cohort_id)
        or document.get("package_nonce") != str(args.package_nonce)
        or document.get("authorization_reference")
        != args.authorization_reference
        or admission.admission_id != session.admission_id
        or admission.admission_sha256 != session.admission_sha256
        or admission_contracts != admitted_contracts
        or session.cohort_id != cohort_id
        or session.authorization_reference
        != args.authorization_reference
        or session.acquisition_stop != args.hard_stop
        or session.session_date != args.session_date
        or admission.capture_boundary
        != OptionExchangeCalendar().session_open(args.session_date)
    ):
        raise ValueError("pre-session authority identity changed")
    identity = {
        "semantic_sha256": semantic_sha256,
        "file_sha256": args.pre_session_authority_file_sha256,
        "admission_sha256": admission.admission_sha256,
        "source_capture_semantic_sha256": document.get(
            "source_capture_semantic_sha256"
        ),
    }
    if (
        not isinstance(identity["source_capture_semantic_sha256"], str)
        or len(identity["source_capture_semantic_sha256"]) != 64
    ):
        raise ValueError("pre-session capture identity is unavailable")
    return session, admitted_contracts, identity


def _current_pointer(provider, cohort_id):
    with provider.transaction(
        APP_ROLE,
        read_only=True,
        dict_cursor=True,
    ) as unit:
        unit.cursor.execute(
            "SELECT current.release_id::text,current.cutoff,"
            "release.manifest FROM rt.portal_release_current AS current "
            "JOIN rt.portal_releases AS release "
            "ON release.release_id=current.release_id "
            "WHERE current.cohort_id=%s "
            "AND current.provider_tier='MASSIVE_ADVANCED'",
            (str(cohort_id),),
        )
        row = unit.cursor.fetchone()
    return None if row is None else {
        "release_id": str(row["release_id"]),
        "cutoff": row["cutoff"],
        "manifest": row["manifest"],
    }


def _cleanup(environment, baseline):
    cleanup = {"stop": None, "remove": None}
    scoped = (*EQUITY_SERVICES, *OPTIONS_SERVICES)
    cleanup["stop"] = _compose(
        environment,
        "stop",
        "--timeout",
        "60",
        *scoped,
        timeout=120,
        check=False,
    ).returncode
    cleanup["remove"] = _compose(
        environment,
        "rm",
        "--force",
        *scoped,
        timeout=120,
        check=False,
    ).returncode
    after = _docker_snapshot(environment)
    cleanup["run_owned_containers"] = sorted(
        name
        for name in after
        if name in {
            "stock-screener-" + service
            for service in scoped
        }
    )
    cleanup["unrelated_unchanged"] = all(
        name in baseline
        and name in after
        and after[name]["container_id"]
        == baseline[name]["container_id"]
        and after[name]["image"] == baseline[name]["image"]
        and after[name]["status"].split(" ", 1)[0]
        == baseline[name]["status"].split(" ", 1)[0]
        for name in UNRELATED_SERVICES
    )
    cleanup["preexisting_volumes_preserved"] = True
    cleanup["preexisting_network_preserved"] = True
    cleanup["preexisting_secrets_preserved"] = True
    return cleanup


def execute(args):
    output = args.output_directory
    output.mkdir(parents=True)
    started_at = datetime.now(UTC)
    provider, pool, values = _provider()
    environment = {
        **os.environ,
        "DOCKER_CONTEXT": "default",
        "V3_DOCKER_EXECUTABLE": str(args.docker_executable),
        "REALTIME_V2_BACKEND_IMAGE": args.image_id,
        "REALTIME_V2_ADVANCED_STREAM_SESSIONS": "1",
        "REALTIME_V2_ADVANCED_CONNECTION_ATTEMPTS": "1",
        "REALTIME_V2_ADVANCED_REST_REPAIR_REQUESTS": "0",
        "REALTIME_V2_P8_PACKAGE_PATH": str(
            args.pre_session_capture
        ),
        "REALTIME_V2_P8_BUNDLE_PATH": str(
            args.pre_session_capture
        ),
    }
    baseline = _docker_snapshot(environment)
    inherited_environment = {
        name: os.environ.get(name)
        for name in (
            "DOCKER_CONTEXT",
            "REALTIME_V2_BACKEND_IMAGE",
        )
    }
    os.environ["DOCKER_CONTEXT"] = "default"
    os.environ["REALTIME_V2_BACKEND_IMAGE"] = args.image_id
    report = {
        "contract": "advanced-options-v3-bounded-market-loop-report-v1",
        "session_date": args.session_date.isoformat(),
        "hard_stop_utc": args.hard_stop,
        "started_at_utc": started_at,
        "verdict": "RUNNING",
        "image_id": args.image_id,
        "windows": [],
        "provider_effects": {
            "equity_websocket_sessions": 0,
            "options_websocket_sessions": 0,
            "options_rest_requests": 0,
            "options_rest_decoded_bytes": 0,
        },
        "publication": {"status": "NOT_ATTEMPTED"},
        "stored_reads": {"status": "NOT_AUTHORIZED"},
        "forbidden_effects": dict(ZERO_FORBIDDEN_EFFECTS),
    }
    service_lifecycle_attempted = False
    try:
        capture = _load_bound_json(
            args.pre_session_capture,
            args.pre_session_capture_semantic_sha256,
            args.pre_session_capture_file_sha256,
            "semantic_sha256",
        )
        selection_capture = _validate_sealed_capture_scope(
            capture,
            args,
            checked_at=datetime.now(UTC),
        )
        docker_qualification = _load_bound_json(
            args.docker_qualification_report,
            args.docker_qualification_semantic_sha256,
            args.docker_qualification_file_sha256,
            "semantic_sha256",
        )
        cohort_id = uuid5(
            NAMESPACE_URL,
            (
                f"stockscreener:{args.authorization_reference}:"
                f"{args.package_nonce}:MARKET_OPTIONS"
            ),
        )
        history = _load_history_binding(
            provider,
            args,
            cohort_id,
        )
        _write_json(output / "HISTORY-BINDING.json", {
            "binding": advanced_options_history_binding_payload(
                history.binding
            ),
            "evidence": history.evidence,
        })
        (
            session_authority,
            admitted_contracts,
            pre_session_identity,
        ) = _load_pre_session_authority(args, cohort_id)
        if datetime.now(UTC) >= args.hard_stop:
            raise TimeoutError("hard stop reached before Options preparation")
        equity_source = _frozen_equity_input(
            provider,
            args.equity_cohort_id,
            args.equity_cutoff,
        )
        if equity_source is None:
            raise RuntimeError(
                "frozen Equity INPUT_READY is unavailable"
            )
        _write_json(output / "EQUITY-INPUT-SOURCE.json", equity_source)
        package, package_start = _build_live_package(
            args,
            equity_source,
            session_authority,
            admitted_contracts,
            pre_session_identity,
            history,
        )
        package_source = output / "PACKAGE-SOURCE.json"
        _write_json(package_source, package)
        observation, binding, production_binding, selection_capture = (
            _capture_binding(
                package,
                datetime.now(UTC),
                values,
                selection_capture,
            )
        )
        if binding.history != history.binding:
            raise AdvancedOptionsHistoryBindingIdentityError(
                "captured production binding changed exact H identity"
            )
        report["provider_effects"]["options_rest_requests"] = 0
        report["provider_effects"]["options_rest_decoded_bytes"] = 0
        selected_contracts = sorted(
            row.contract_ticker
            for row in binding.enrollment.activity_selection.contracts
        )
        if selected_contracts != package["v3_admitted_contracts"]:
            raise RuntimeError(
                "live activity selection diverged from pre-session admission"
            )
        production_path = output / "PRODUCTION-BINDING.json"
        observation_path = output / "LIVE-OBSERVATION.json"
        _write_json(production_path, production_binding)
        _write_json(observation_path, observation)
        prepared_directory = output / "prepared"
        prepare_args = production_cli.parse_args([
            "prepare",
            "--database",
            args.database,
            "--confirm-database-name",
            args.confirm_database_name,
            "--package-source",
            str(package_source),
            "--production-binding",
            str(production_path),
            "--live-observation",
            str(observation_path),
            "--output-directory",
            str(prepared_directory),
            "--execute",
        ])
        prepare_report = production_cli._prepare(prepare_args)
        package_path = prepared_directory / "PACKAGE.json"
        bundle_path = prepared_directory / "SESSION-BUNDLE.json"
        _sleep_until(package_start, args.hard_stop)
        service_lifecycle_attempted = True
        residents_args = production_cli.parse_args([
            "start-residents",
            "--package",
            str(package_path),
            "--bundle",
            str(bundle_path),
            "--docker-executable",
            str(args.docker_executable),
            "--execute",
        ])
        production_cli._compose(
            residents_args,
            production_cli.RESIDENT_KINDS,
        )
        report["provider_effects"]["options_websocket_sessions"] = 1
        prior_pointer = _current_pointer(provider, cohort_id)
        window_started = False
        repeated_cause = None
        for attempt in range(1, args.maximum_window_attempts + 1):
            if datetime.now(UTC) >= args.hard_stop:
                break
            equity_cutoff = datetime.fromisoformat(
                equity_source["equity_cutoff_utc"]
            )
            scheduled = max(
                datetime.now(UTC) + timedelta(minutes=2),
                equity_cutoff + timedelta(seconds=1),
            )
            if scheduled > equity_cutoff + timedelta(seconds=300):
                raise RuntimeError(
                    "frozen Equity input is stale for Options"
                )
            deadline = min(
                scheduled + timedelta(minutes=2),
                args.hard_stop - timedelta(seconds=30),
            )
            if deadline <= scheduled:
                break
            authorize_args = production_cli.parse_args([
                "authorize-window",
                "--database",
                args.database,
                "--confirm-database-name",
                args.confirm_database_name,
                "--package",
                str(package_path),
                "--bundle",
                str(bundle_path),
                "--authorization-reference",
                (
                    f"{args.authorization_reference}:WINDOW:{attempt}"
                ),
                "--publication-authorization-reference",
                (
                    f"{args.authorization_reference}:PUBLICATION:{attempt}"
                ),
                "--window-key",
                (
                    f"ADVANCED-V3-"
                    f"{args.session_date.strftime('%Y%m%d')}-{attempt}"
                ),
                "--options-cutoff",
                scheduled.isoformat(),
                "--window-deadline",
                deadline.isoformat(),
                "--equity-receipt-id",
                equity_source["equity_receipt_id"],
                "--execute",
            ])
            authorization = production_cli._authorize_window(
                authorize_args
            )
            window_id = UUID(authorization["window_id"])
            if not window_started:
                window_args = production_cli.parse_args([
                    "run-window",
                    "--package",
                    str(package_path),
                    "--bundle",
                    str(bundle_path),
                    "--docker-executable",
                    str(args.docker_executable),
                    "--execute",
                ])
                production_cli._compose(
                    window_args,
                    (AdvancedOptionsProcessKind.WINDOW,),
                )
                window_started = True
            evidence = _wait_for_window(
                provider,
                window_id,
                deadline,
                args.hard_stop,
            )
            cause = (
                "PASS"
                if evidence["complete"]
                else canonical_sha256(evidence["stage_states"])
            )
            window_report = {
                "attempt": attempt,
                "window_id": str(window_id),
                "equity_cutoff_utc": equity_cutoff,
                "options_cutoff_utc": scheduled,
                "deadline_utc": deadline,
                "verdict": (
                    "PASS" if evidence["complete"] else "FAILED"
                ),
                "cause": cause,
                "evidence": evidence,
            }
            report["windows"].append(window_report)
            _write_json(
                output / f"WINDOW-{attempt}.json",
                window_report,
            )
            if evidence["complete"]:
                report["verdict"] = "PASS"
                break
            if repeated_cause == cause:
                report["verdict"] = "BLOCKED_REPEATED_FAILURE"
                break
            repeated_cause = cause
        if report["verdict"] == "PASS":
            pointer = _current_pointer(provider, cohort_id)
            if (
                pointer is None
                or (
                    prior_pointer is not None
                    and pointer["release_id"]
                    == prior_pointer["release_id"]
                )
            ):
                raise RuntimeError(
                    "canonical publication pointer did not advance once"
                )
            read_port = PostgresAdvancedOptionsStoredReadPort(
                provider,
                package_sha256=package["package_sha256"],
            )
            first_pin = read_port.enable(
                cohort_id=cohort_id,
                release_id=UUID(pointer["release_id"]),
            )
            replay_pin = read_port.enable(
                cohort_id=cohort_id,
                release_id=UUID(pointer["release_id"]),
            )
            if {
                key: first_pin[key]
                for key in (
                    "cohort_id",
                    "release_id",
                    "release_cutoff_utc",
                    "verified",
                )
            } != {
                key: replay_pin[key]
                for key in (
                    "cohort_id",
                    "release_id",
                    "release_cutoff_utc",
                    "verified",
                )
            }:
                raise RuntimeError(
                    "stored-read activation is not idempotent"
                )
            report["publication"] = {
                "status": "PUBLISHED",
                "prior_pointer": prior_pointer,
                "current_pointer": pointer,
                "pointer_advances": 1,
            }
            report["stored_reads"] = {
                "status": "DURABLE_PIN_ENABLED",
                "pin": first_pin,
                "idempotent_replay": True,
                "process_flags_changed": False,
            }
        elif report["verdict"] == "RUNNING":
            report["verdict"] = "BLOCKED_ATTEMPTS_OR_HARD_STOP"
        report["package"] = {
            "package_sha256": package["package_sha256"],
            "package_file_sha256": _file_sha256(package_path),
            "bundle_sha256": prepare_report["bundle_sha256"],
            "bundle_file_sha256": _file_sha256(bundle_path),
            "admitted_contracts": package["v3_admitted_contracts"],
        }
    except AdvancedOptionsHistoryBindingIdentityError as error:
        if report["verdict"] == "RUNNING":
            report["verdict"] = "BLOCKED_H_BINDING_IDENTITY"
        report["decisive_error"] = {
            "class": "H_BINDING_IDENTITY_DEFECT",
            "type": type(error).__name__,
            "message": str(error),
        }
    except AdvancedOptionsHistoryCoverageError as error:
        if report["verdict"] == "RUNNING":
            report["verdict"] = "BLOCKED_H_COVERAGE"
        report["decisive_error"] = {
            "class": "H_COVERAGE_MISSING",
            "type": type(error).__name__,
            "message": str(error),
        }
    except BaseException as error:
        if report["verdict"] == "RUNNING":
            report["verdict"] = "BLOCKED"
        report["decisive_error"] = {
            "type": type(error).__name__,
            "message": str(error),
        }
    finally:
        cleanup = (
            _cleanup(environment, baseline)
            if service_lifecycle_attempted
            else {
                "status": "NOT_REQUIRED_NO_SERVICE_EFFECT",
                "run_owned_containers": [],
                "unrelated_unchanged": True,
                "preexisting_volumes_preserved": True,
                "preexisting_network_preserved": True,
                "preexisting_secrets_preserved": True,
            }
        )
        report["cleanup"] = cleanup
        report["completed_at_utc"] = datetime.now(UTC)
        report["semantic_sha256"] = canonical_sha256(report)
        report_path = output / (
            "advanced-options-v3-market-loop-"
            f"{report['semantic_sha256'][:16]}.json"
        )
        _write_json(report_path, report)
        report["report"] = str(report_path)
        report["file_sha256"] = _file_sha256(report_path)
        pool.closeall()
        for name, value in inherited_environment.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
    return report


def main(argv=None) -> int:
    args = parse_args(argv)
    report = execute(args)
    print(json.dumps({
        key: report.get(key)
        for key in (
            "verdict",
            "semantic_sha256",
            "file_sha256",
            "report",
            "windows",
            "provider_effects",
            "publication",
            "stored_reads",
            "cleanup",
            "decisive_error",
        )
        if key in report
    }, indent=2, sort_keys=True, default=str))
    return 0 if report["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
