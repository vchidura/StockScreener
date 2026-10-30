from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from .advanced_options_development_fixture import (
    validate_development_fixture_e,
)
from .advanced_options_validation_mode import (
    AdvancedOptionsValidationMode,
    advanced_options_projection_policy,
)
from .contracts import canonical_sha256


DEVELOPMENT_DIAGNOSTIC_PLAN_SCHEMA = (
    "advanced-options-v3-development-diagnostic-plan-v1"
)


def build_development_diagnostic_plan(
    *,
    fixture_e: Mapping[str, object],
    fixture_e_file_sha256: str,
    live_options_report: Mapping[str, object],
    live_options_report_file_sha256: str,
    bound_journal_path: Path,
    bound_journal_sha256: str,
    authorization_reference: str,
    environment: Mapping[str, str],
) -> dict[str, object]:
    policy = advanced_options_projection_policy(environment)
    validate_development_fixture_e(
        fixture_e,
        environment=environment,
    )
    reference = authorization_reference.strip()
    contracts = tuple(sorted(
        str(value)
        for value in live_options_report.get("contracts") or ()
    ))
    if (
        policy.validation_mode
        is not AdvancedOptionsValidationMode.DEVELOPMENT_DIAGNOSTIC
        or not reference
        or any(
            len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
            for value in (
                fixture_e_file_sha256,
                live_options_report_file_sha256,
                bound_journal_sha256,
            )
        )
        or live_options_report.get("contract")
        != "advanced-options-v3-live-socket-canary-v1"
        or live_options_report.get("status") != "PASS"
        or live_options_report.get(
            "authority_consumed_for_diagnostic"
        ) is not True
        or live_options_report.get("production_database_writes") != 0
        or live_options_report.get("publication") != 0
        or live_options_report.get("serving") != 0
        or len(contracts) != 4
        or not bound_journal_path.is_absolute()
    ):
        raise ValueError(
            "development diagnostic plan source identity changed"
        )
    identity = {
        "schema": DEVELOPMENT_DIAGNOSTIC_PLAN_SCHEMA,
        "validation_mode": policy.validation_mode.value,
        "authorization_reference": reference,
        "fixture_e_semantic_sha256": fixture_e["semantic_sha256"],
        "fixture_e_file_sha256": fixture_e_file_sha256,
        "live_options_report_semantic_sha256": (
            live_options_report["semantic_sha256"]
        ),
        "live_options_report_file_sha256": (
            live_options_report_file_sha256
        ),
        "bound_journal_path": str(bound_journal_path),
        "bound_journal_sha256": bound_journal_sha256,
        "contracts": list(contracts),
        "equity_stage_order": list(policy.equity_stage_order),
        "equity_terminal_receipt": policy.equity_terminal_receipt,
        "options_stage_order": list(policy.options_stage_order),
        "options_terminal_receipt": policy.options_terminal_receipt,
        "release_allowed": policy.release_allowed,
        "pointer_allowed": policy.pointer_allowed,
        "read_pin_allowed": policy.read_pin_allowed,
        "normal_portal_serving_allowed": (
            policy.normal_portal_serving_allowed
        ),
    }
    plan_sha256 = canonical_sha256(identity)
    equity_cohort_id = uuid5(
        NAMESPACE_URL,
        f"development-diagnostic-equity:{plan_sha256}",
    )
    options_cohort_id = uuid5(
        NAMESPACE_URL,
        f"development-diagnostic-options:{plan_sha256}",
    )
    equity_window_id = uuid5(
        NAMESPACE_URL,
        f"development-diagnostic-equity-window:{plan_sha256}",
    )
    options_window_id = uuid5(
        NAMESPACE_URL,
        f"development-diagnostic-options-window:{plan_sha256}",
    )
    result = {
        **identity,
        "plan_sha256": plan_sha256,
        "equity_cohort_id": str(equity_cohort_id),
        "options_cohort_id": str(options_cohort_id),
        "equity_window_id": str(equity_window_id),
        "options_window_id": str(options_window_id),
        "database_target": "stocks_realtime_v2",
        "database_execution_authorized": False,
        "service_execution_authorized": False,
        "provider_effects_authorized": False,
        "production_eligible": False,
    }
    result["semantic_sha256"] = canonical_sha256(result)
    return result
