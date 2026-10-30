"""Provider-free operating contracts for the Advanced V3 session."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import StrEnum
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from .advanced_realtime_session_scheduler import (
    ADVANCED_SESSION_EQUITY_SYMBOLS,
    build_advanced_session_windows,
)
from .contracts import canonical_sha256


UTC = timezone.utc
PREOPEN_FREEZE_SCHEMA = "advanced-realtime-v3-preopen-freeze-v1"
PREOPEN_FREEZE_REPORT_SCHEMA = (
    "advanced-realtime-v3-preopen-freeze-report-v1"
)
DIAGNOSTIC_AUTHORIZATION_SCHEMA = (
    "advanced-realtime-v3-diagnostic-non-serving-v1"
)
DOWNSTREAM_TRANSITION_SCHEMA = (
    "advanced-realtime-v3-downstream-transition-v1"
)
SESSION_EVIDENCE_SCHEMA = "advanced-realtime-v3-session-evidence-v1"
SESSION_STATUS_SCHEMA = "advanced-realtime-v3-session-status-v1"
TARGET_EQUITY_SYMBOLS = tuple(ADVANCED_SESSION_EQUITY_SYMBOLS)

ACQUISITION_SERVICES = (
    "realtime-v2-advanced-controller",
    "realtime-v2-advanced-ingress",
    "realtime-v2-advanced-bridge",
    "realtime-v2-advanced-scheduler",
    "realtime-v2-advanced-finalizer",
    "realtime-v3-options-acquisition",
)
DOWNSTREAM_SERVICES = (
    "realtime-v2-advanced-detectors",
    "realtime-v2-canonical-stock-alerts",
    "realtime-v2-options-p8-ingress",
    "realtime-v2-options-p8-market-state",
    "realtime-v2-options-p8-flow",
    "realtime-v2-options-p8-window",
    "realtime-v2-advanced-api",
)
ZERO_NON_SERVING_EFFECTS = {
    "provider_rest": 0,
    "provider_websocket": 0,
    "production_database_writes": 0,
    "release": 0,
    "pointer": 0,
    "read_pin": 0,
    "dispatch": 0,
    "delivery": 0,
    "broker": 0,
    "trading": 0,
}


class AdvancedSessionOperation(StrEnum):
    PRIMARY_SERVING = "PRIMARY_SERVING"
    DIAGNOSTIC_NON_SERVING = "DIAGNOSTIC_NON_SERVING"
    CONTROLLED_DOWNSTREAM_TRANSITION = (
        "CONTROLLED_DOWNSTREAM_TRANSITION"
    )


def _sha256(value: object, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


def _session_windows(session_date: date):
    if not isinstance(session_date, date):
        raise ValueError("session_date must be an XNYS session")
    try:
        windows = build_advanced_session_windows(session_date)
    except ValueError as exc:
        raise ValueError("session_date must be an XNYS session") from exc
    if not windows:
        raise ValueError("session_date has no decision windows")
    return windows


def expected_session_cutoffs(session_date: date) -> tuple[str, ...]:
    return tuple(
        window.equity_cutoff.isoformat()
        for window in _session_windows(session_date)
    )


def _image_id(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.startswith("sha256:"):
        raise ValueError(f"{name} must be an immutable image ID")
    _sha256(value.removeprefix("sha256:"), name)
    return value


def _text(value: object, name: str, *, maximum: int = 160) -> str:
    if not isinstance(value, str) or not 1 <= len(value.strip()) <= maximum:
        raise ValueError(f"{name} is invalid")
    return value.strip()


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be an object")
    return value


def _path(
    value: Mapping[str, object],
    dotted: str,
) -> object:
    current: object = value
    for part in dotted.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def preopen_freeze_template(session_date: date) -> dict[str, object]:
    windows = _session_windows(session_date)
    expected_cutoffs = expected_session_cutoffs(session_date)
    return {
        "schema": PREOPEN_FREEZE_SCHEMA,
        "session_date": session_date.isoformat(),
        "operator": "",
        "approver": "",
        "freeze_completed_at_utc": "",
        "normalization": {
            "runtime_profile_version": "advanced-equity-ingress-9-v5",
            "stored_active_profile_sha256": "",
            "rendered_source_profile_sha256": "",
            "normalizer_version": "massive_market_event_v6",
            "normalizer_source_sha256": "",
            "manifest_cohort_sha256": "",
            "journal_generation": "",
            "feed": "STOCKS",
            "channels": ["AM"],
            "symbols": list(TARGET_EQUITY_SYMBOLS),
            "settings": {
                "am_finality_deferral_seconds": 2,
                "provider_clock_skew_seconds": 5,
                "maximum_message_bytes": 2_097_152,
                "buffer_event_limit": 128,
                "buffer_byte_limit": 8_388_608,
                "buffer_max_age_seconds": 30,
                "journal_batch_messages": 100,
                "journal_batch_bytes": 8_388_608,
                "bridge_batch_limit": 1_000,
                "websocket_library_max_queue": 2,
                "minimum_journal_free_bytes": 5_368_709_120,
                "minimum_journal_free_percent": 20,
                "heartbeat_interval_seconds": 20,
                "stale_after_seconds": 60,
                "reconnect_initial_backoff_seconds": 0.5,
                "reconnect_backoff_multiplier": 2,
                "reconnect_maximum_backoff_seconds": 30,
                "reconnect_jitter_ceiling_seconds": 30,
                "shutdown_deadline_seconds": 30,
                "drain_deadline_seconds": 30,
            },
            "evidence": {
                "provider_free_am_fixture_passed": False,
                "zero_finality_pending": False,
                "zero_quarantined_run_owned_journals": False,
                "zero_unresolved_required_gaps": False,
                "normalizer_versions_equal": False,
                "offset_checksum_continuity": False,
                "exact_target_admission": False,
                "all_relevant_am_records_good": False,
                "single_session_generation_owner_timestamp_basis": False,
            },
        },
        "finalization": {
            "base_bar": "RAW_UNADJUSTED_FINALIZED_1M_AM",
            "decision_interval_minutes": 5,
            "settings": {
                "scheduler_finality_delay_seconds": 5,
                "window_completion_budget_seconds": 300,
                "serving_lag_seconds": 30,
                "claim_limit": 1,
                "lease_seconds": 60,
                "maximum_attempts": 3,
                "retry_delay_seconds": 30,
                "poll_seconds": 2,
                "statement_timeout_ms": 5_000,
            },
            "cutoff_count": len(expected_cutoffs),
            "first_cutoff_utc": expected_cutoffs[0],
            "last_cutoff_utc": expected_cutoffs[-1],
            "expected_cutoff_membership_sha256": (
                canonical_sha256(expected_cutoffs)
            ),
            "policy_command_sha256": "",
            "evidence": {
                "bridge_finality_contract_passed": False,
                "zero_failed_or_blocked_jobs": False,
                "zero_bridge_offset_lag": False,
                "zero_unresolved_required_gaps": False,
                "finalizer_ready": False,
                "provider_free_membership_retry_fixture_passed": False,
                "zero_manual_receipts_or_final_bars": False,
            },
        },
        "provider_budgets": {
            "equity": {
                "account_tier": "MASSIVE_ADVANCED",
                "symbols": list(TARGET_EQUITY_SYMBOLS),
                "stream_sessions": 1,
                "connection_attempts": None,
                "websocket_decoded_byte_ceiling": None,
                "catch_up_mode": "",
                "repair_request_ceiling": None,
                "repair_response_byte_ceiling": None,
                "repair_total_decoded_byte_ceiling": None,
                "scope_sha256": "",
                "authority_unconsumed": False,
            },
            "options": {
                "socket_contract_count": 4,
                "connection_attempts": None,
                "socket_message_ceiling": None,
                "socket_decoded_byte_ceiling": None,
                "rest_lane": "OPTIONS_ALIGNMENT",
                "endpoint": "/v3/snapshot",
                "requests_per_cycle": 1,
                "request_ceiling": len(windows),
                "response_byte_ceiling": None,
                "total_rest_decoded_byte_ceiling": None,
                "authorized_until": (
                    windows[-1].latest_options_cutoff.isoformat()
                ),
                "pagination_allowed": False,
                "scope_sha256": "",
                "requests_reserved": 0,
                "bytes_reserved": 0,
            },
            "evidence": {
                "no_conflicting_ceilings": False,
                "no_stale_reserved_request": False,
                "account_day_coordination_available": False,
                "no_prior_date_or_asset_class_copy": False,
            },
        },
        "identities": {
            "acquisition_image_id": "",
            "equity_authority_scope_sha256": "",
            "options_session_authority_sha256": "",
            "rendered_compose_sha256": "",
            "package_sha256": "",
            "bundle_sha256": "",
            "pre_open_audit_report_sha256": "",
        },
    }


def build_preopen_freeze_report(
    value: object,
) -> dict[str, object]:
    checks: list[dict[str, object]] = []
    freeze = value if isinstance(value, Mapping) else {}
    try:
        session_date = date.fromisoformat(str(freeze.get("session_date")))
        windows = _session_windows(session_date)
        expected_cutoffs = expected_session_cutoffs(session_date)
    except (TypeError, ValueError) as exc:
        raise ValueError("freeze session_date is invalid") from exc
    session_open = (
        windows[0].equity_cutoff - timedelta(minutes=5)
    )
    freeze_deadline = session_open - timedelta(minutes=5)
    authorized_until = windows[-1].latest_options_cutoff.isoformat()
    window_count = len(windows)

    def check(
        name: str,
        passed: bool,
        requirement: str,
    ) -> None:
        checks.append({
            "field": name,
            "status": "PASS" if passed else "FAIL",
            "requirement": requirement,
        })

    def exact(path: str, expected: object) -> None:
        check(path, _path(freeze, path) == expected, f"exact {expected!r}")

    def present(path: str) -> None:
        actual = _path(freeze, path)
        check(
            path,
            isinstance(actual, str) and bool(actual.strip()),
            "nonblank",
        )

    def digest(path: str) -> None:
        actual = _path(freeze, path)
        check(
            path,
            isinstance(actual, str)
            and len(actual) == 64
            and all(
                character in "0123456789abcdef"
                for character in actual
            ),
            "SHA-256 digest",
        )

    exact("schema", PREOPEN_FREEZE_SCHEMA)
    exact("session_date", session_date.isoformat())
    present("operator")
    present("approver")
    completed = _path(freeze, "freeze_completed_at_utc")
    try:
        completed_at = datetime.fromisoformat(str(completed))
        completed_valid = (
            completed_at.tzinfo is not None
            and completed_at.astimezone(UTC).date()
            == session_date
            and completed_at.astimezone(UTC)
            <= freeze_deadline
        )
    except ValueError:
        completed_valid = False
    check(
        "freeze_completed_at_utc",
        completed_valid,
        (
            "timezone-aware session timestamp no later than "
            f"{freeze_deadline.isoformat()}"
        ),
    )

    exact(
        "normalization.runtime_profile_version",
        "advanced-equity-ingress-9-v5",
    )
    for path in (
        "normalization.stored_active_profile_sha256",
        "normalization.rendered_source_profile_sha256",
        "normalization.normalizer_source_sha256",
        "normalization.manifest_cohort_sha256",
    ):
        digest(path)
    check(
        "normalization.profile_sha_match",
        _path(freeze, "normalization.stored_active_profile_sha256")
        == _path(
            freeze,
            "normalization.rendered_source_profile_sha256",
        )
        and bool(
            _path(
                freeze,
                "normalization.stored_active_profile_sha256",
            )
        ),
        "stored active profile SHA equals rendered source profile SHA",
    )
    exact(
        "normalization.normalizer_version",
        "massive_market_event_v6",
    )
    journal_generation = _path(
        freeze,
        "normalization.journal_generation",
    )
    try:
        journal_generation_valid = UUID(str(journal_generation)).int != 0
    except ValueError:
        journal_generation_valid = False
    check(
        "normalization.journal_generation",
        journal_generation_valid,
        "nonzero UUID",
    )
    exact("normalization.feed", "STOCKS")
    exact("normalization.channels", ["AM"])
    exact("normalization.symbols", list(TARGET_EQUITY_SYMBOLS))

    exact_settings = {
        "maximum_message_bytes": 2_097_152,
        "buffer_event_limit": 128,
        "buffer_byte_limit": 8_388_608,
        "buffer_max_age_seconds": 30,
        "journal_batch_messages": 100,
        "journal_batch_bytes": 8_388_608,
        "bridge_batch_limit": 1_000,
        "websocket_library_max_queue": 2,
        "heartbeat_interval_seconds": 20,
        "stale_after_seconds": 60,
        "reconnect_initial_backoff_seconds": 0.5,
        "reconnect_backoff_multiplier": 2,
        "reconnect_maximum_backoff_seconds": 30,
        "reconnect_jitter_ceiling_seconds": 30,
        "shutdown_deadline_seconds": 30,
        "drain_deadline_seconds": 30,
    }
    for name, expected in exact_settings.items():
        exact(f"normalization.settings.{name}", expected)
    for name, maximum in (
        ("am_finality_deferral_seconds", 2),
        ("provider_clock_skew_seconds", 5),
    ):
        actual = _path(freeze, f"normalization.settings.{name}")
        check(
            f"normalization.settings.{name}",
            type(actual) in {int, float}
            and 0 <= actual <= maximum,
            f"between 0 and {maximum}",
        )
    for name, minimum in (
        ("minimum_journal_free_bytes", 5_368_709_120),
        ("minimum_journal_free_percent", 20),
    ):
        actual = _path(freeze, f"normalization.settings.{name}")
        check(
            f"normalization.settings.{name}",
            type(actual) is int and actual >= minimum,
            f"at least {minimum}",
        )
    for name in (
        "provider_free_am_fixture_passed",
        "zero_finality_pending",
        "zero_quarantined_run_owned_journals",
        "zero_unresolved_required_gaps",
        "normalizer_versions_equal",
        "offset_checksum_continuity",
        "exact_target_admission",
        "all_relevant_am_records_good",
        "single_session_generation_owner_timestamp_basis",
    ):
        exact(f"normalization.evidence.{name}", True)

    exact("finalization.base_bar", "RAW_UNADJUSTED_FINALIZED_1M_AM")
    exact("finalization.decision_interval_minutes", 5)
    for name, expected in {
        "scheduler_finality_delay_seconds": 5,
        "window_completion_budget_seconds": 300,
        "serving_lag_seconds": 30,
        "claim_limit": 1,
        "lease_seconds": 60,
        "maximum_attempts": 3,
        "retry_delay_seconds": 30,
        "poll_seconds": 2,
        "statement_timeout_ms": 5_000,
    }.items():
        exact(f"finalization.settings.{name}", expected)
    exact("finalization.cutoff_count", window_count)
    exact("finalization.first_cutoff_utc", expected_cutoffs[0])
    exact("finalization.last_cutoff_utc", expected_cutoffs[-1])
    exact(
        "finalization.expected_cutoff_membership_sha256",
        canonical_sha256(expected_cutoffs),
    )
    digest("finalization.policy_command_sha256")
    for name in (
        "bridge_finality_contract_passed",
        "zero_failed_or_blocked_jobs",
        "zero_bridge_offset_lag",
        "zero_unresolved_required_gaps",
        "finalizer_ready",
        "provider_free_membership_retry_fixture_passed",
        "zero_manual_receipts_or_final_bars",
    ):
        exact(f"finalization.evidence.{name}", True)

    exact("provider_budgets.equity.account_tier", "MASSIVE_ADVANCED")
    exact(
        "provider_budgets.equity.symbols",
        list(TARGET_EQUITY_SYMBOLS),
    )
    exact("provider_budgets.equity.stream_sessions", 1)
    for name in (
        "connection_attempts",
        "websocket_decoded_byte_ceiling",
    ):
        actual = _path(freeze, f"provider_budgets.equity.{name}")
        check(
            f"provider_budgets.equity.{name}",
            type(actual) is int and actual > 0,
            "positive granted integer",
        )
    catch_up = _path(freeze, "provider_budgets.equity.catch_up_mode")
    check(
        "provider_budgets.equity.catch_up_mode",
        catch_up in {"AUTO", "DISABLED"},
        "AUTO or DISABLED",
    )
    expected_repairs = (
        (18, 1_048_576, 18_874_386)
        if catch_up == "AUTO"
        else (0, 0, 0)
        if catch_up == "DISABLED"
        else None
    )
    actual_repairs = tuple(
        _path(freeze, f"provider_budgets.equity.{name}")
        for name in (
            "repair_request_ceiling",
            "repair_response_byte_ceiling",
            "repair_total_decoded_byte_ceiling",
        )
    )
    check(
        "provider_budgets.equity.repair_contract",
        expected_repairs is not None
        and actual_repairs == expected_repairs,
        "AUTO=(18,1048576,18874386), DISABLED=(0,0,0)",
    )
    equity_scope_payload = {
        "schema": (
            "advanced-realtime-v3-equity-provider-budget-freeze-v1"
        ),
        "session_date": session_date.isoformat(),
        "account_tier": _path(
            freeze,
            "provider_budgets.equity.account_tier",
        ),
        "symbols": _path(
            freeze,
            "provider_budgets.equity.symbols",
        ),
        "stream_sessions": _path(
            freeze,
            "provider_budgets.equity.stream_sessions",
        ),
        "connection_attempts": _path(
            freeze,
            "provider_budgets.equity.connection_attempts",
        ),
        "websocket_decoded_byte_ceiling": _path(
            freeze,
            "provider_budgets.equity.websocket_decoded_byte_ceiling",
        ),
        "catch_up_mode": catch_up,
        "repair_request_ceiling": actual_repairs[0],
        "repair_response_byte_ceiling": actual_repairs[1],
        "repair_total_decoded_byte_ceiling": actual_repairs[2],
    }
    equity_scope_sha256 = canonical_sha256(equity_scope_payload)
    exact(
        "provider_budgets.equity.scope_sha256",
        equity_scope_sha256,
    )
    exact("provider_budgets.equity.authority_unconsumed", True)

    exact("provider_budgets.options.socket_contract_count", 4)
    for name in (
        "connection_attempts",
        "socket_message_ceiling",
        "socket_decoded_byte_ceiling",
        "response_byte_ceiling",
    ):
        actual = _path(freeze, f"provider_budgets.options.{name}")
        check(
            f"provider_budgets.options.{name}",
            type(actual) is int and actual > 0,
            "positive granted integer",
        )
    exact("provider_budgets.options.rest_lane", "OPTIONS_ALIGNMENT")
    exact("provider_budgets.options.endpoint", "/v3/snapshot")
    exact("provider_budgets.options.requests_per_cycle", 1)
    exact("provider_budgets.options.request_ceiling", window_count)
    exact(
        "provider_budgets.options.authorized_until",
        authorized_until,
    )
    exact("provider_budgets.options.pagination_allowed", False)
    response_ceiling = _path(
        freeze,
        "provider_budgets.options.response_byte_ceiling",
    )
    total_ceiling = _path(
        freeze,
        "provider_budgets.options.total_rest_decoded_byte_ceiling",
    )
    check(
        "provider_budgets.options.total_rest_decoded_byte_ceiling",
        type(response_ceiling) is int
        and response_ceiling > 0
        and total_ceiling == window_count * response_ceiling,
        f"exactly {window_count} * response_byte_ceiling",
    )
    options_scope_payload = {
        "schema": (
            "advanced-realtime-v3-options-provider-budget-freeze-v1"
        ),
        "session_date": session_date.isoformat(),
        "socket_contract_count": _path(
            freeze,
            "provider_budgets.options.socket_contract_count",
        ),
        "connection_attempts": _path(
            freeze,
            "provider_budgets.options.connection_attempts",
        ),
        "socket_message_ceiling": _path(
            freeze,
            "provider_budgets.options.socket_message_ceiling",
        ),
        "socket_decoded_byte_ceiling": _path(
            freeze,
            "provider_budgets.options.socket_decoded_byte_ceiling",
        ),
        "rest_lane": _path(
            freeze,
            "provider_budgets.options.rest_lane",
        ),
        "endpoint": _path(
            freeze,
            "provider_budgets.options.endpoint",
        ),
        "requests_per_cycle": _path(
            freeze,
            "provider_budgets.options.requests_per_cycle",
        ),
        "request_ceiling": _path(
            freeze,
            "provider_budgets.options.request_ceiling",
        ),
        "response_byte_ceiling": response_ceiling,
        "total_rest_decoded_byte_ceiling": total_ceiling,
        "authorized_until": _path(
            freeze,
            "provider_budgets.options.authorized_until",
        ),
        "pagination_allowed": _path(
            freeze,
            "provider_budgets.options.pagination_allowed",
        ),
    }
    options_scope_sha256 = canonical_sha256(options_scope_payload)
    exact(
        "provider_budgets.options.scope_sha256",
        options_scope_sha256,
    )
    exact("provider_budgets.options.requests_reserved", 0)
    exact("provider_budgets.options.bytes_reserved", 0)
    for name in (
        "no_conflicting_ceilings",
        "no_stale_reserved_request",
        "account_day_coordination_available",
        "no_prior_date_or_asset_class_copy",
    ):
        exact(f"provider_budgets.evidence.{name}", True)

    acquisition_image = _path(
        freeze,
        "identities.acquisition_image_id",
    )
    check(
        "identities.acquisition_image_id",
        isinstance(acquisition_image, str)
        and acquisition_image.startswith("sha256:")
        and len(acquisition_image) == 71
        and all(
            character in "0123456789abcdef"
            for character in acquisition_image.removeprefix("sha256:")
        ),
        "immutable sha256 image ID",
    )
    for name in (
        "equity_authority_scope_sha256",
        "options_session_authority_sha256",
        "rendered_compose_sha256",
        "package_sha256",
        "bundle_sha256",
        "pre_open_audit_report_sha256",
    ):
        digest(f"identities.{name}")

    failed = [
        str(row["field"])
        for row in checks
        if row["status"] == "FAIL"
    ]
    decision = "GO" if not failed else "NO_GO"
    report: dict[str, object] = {
        "schema": PREOPEN_FREEZE_REPORT_SCHEMA,
        "session_date": session_date.isoformat(),
        "decision": decision,
        "checks": checks,
        "failed_fields": failed,
        "computed_budget_scope_sha256": {
            "equity": equity_scope_sha256,
            "options": options_scope_sha256,
        },
        "frozen_identities": {
            "acquisition_image_id": acquisition_image,
            "equity_authority_scope_sha256": _path(
                freeze,
                "identities.equity_authority_scope_sha256",
            ),
            "options_session_authority_sha256": _path(
                freeze,
                "identities.options_session_authority_sha256",
            ),
            "rendered_compose_sha256": _path(
                freeze,
                "identities.rendered_compose_sha256",
            ),
            "package_sha256": _path(
                freeze,
                "identities.package_sha256",
            ),
            "bundle_sha256": _path(
                freeze,
                "identities.bundle_sha256",
            ),
            "pre_open_audit_report_sha256": _path(
                freeze,
                "identities.pre_open_audit_report_sha256",
            ),
        },
        "provider_effects": 0,
        "production_effects": 0,
        "freeze_sha256": (
            canonical_sha256(freeze) if isinstance(value, Mapping) else None
        ),
    }
    report["semantic_sha256"] = canonical_sha256(report)
    return report


@dataclass(frozen=True, slots=True)
class DiagnosticNonServingAuthorization:
    authorization_id: UUID
    authorization_sha256: str
    authorization_reference: str
    owner: str
    session_date: date
    cohort_id: UUID
    source_sha256: str
    backend_image_id: str
    input_evidence_sha256: str

    def __post_init__(self) -> None:
        try:
            _session_windows(self.session_date)
        except ValueError as exc:
            raise ValueError(
                "diagnostic ownership scope is invalid"
            ) from exc
        if (
            not isinstance(self.authorization_id, UUID)
            or not self.authorization_id.int
            or not isinstance(self.cohort_id, UUID)
            or not self.cohort_id.int
        ):
            raise ValueError("diagnostic ownership scope is invalid")
        _text(self.authorization_reference, "authorization_reference")
        _text(self.owner, "owner")
        _sha256(self.source_sha256, "source_sha256")
        _image_id(self.backend_image_id, "backend_image_id")
        _sha256(self.input_evidence_sha256, "input_evidence_sha256")
        expected = canonical_sha256(
            diagnostic_authorization_payload(
                self,
                include_identity=False,
            )
        )
        expected_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:diagnostic-non-serving:{expected}",
        )
        if (
            self.authorization_sha256 != expected
            or self.authorization_id != expected_id
        ):
            raise ValueError("diagnostic identity changed")


def diagnostic_authorization_payload(
    authorization: DiagnosticNonServingAuthorization,
    *,
    include_identity: bool = True,
) -> dict[str, object]:
    value: dict[str, object] = {
        "schema": DIAGNOSTIC_AUTHORIZATION_SCHEMA,
        "operation": (
            AdvancedSessionOperation.DIAGNOSTIC_NON_SERVING.value
        ),
        "authorization_reference": (
            authorization.authorization_reference
        ),
        "owner": authorization.owner,
        "session_date": authorization.session_date.isoformat(),
        "cohort_id": str(authorization.cohort_id),
        "source_sha256": authorization.source_sha256,
        "backend_image_id": authorization.backend_image_id,
        "input_evidence_sha256": (
            authorization.input_evidence_sha256
        ),
        "effects": dict(ZERO_NON_SERVING_EFFECTS),
    }
    if include_identity:
        value["authorization_id"] = str(
            authorization.authorization_id
        )
        value["authorization_sha256"] = (
            authorization.authorization_sha256
        )
    return value


def build_diagnostic_authorization(
    *,
    authorization_reference: str,
    owner: str,
    session_date: date,
    cohort_id: UUID,
    source_sha256: str,
    backend_image_id: str,
    input_evidence_sha256: str,
) -> DiagnosticNonServingAuthorization:
    unsigned = DiagnosticNonServingAuthorization.__new__(
        DiagnosticNonServingAuthorization
    )
    values = {
        "authorization_reference": authorization_reference,
        "owner": owner,
        "session_date": session_date,
        "cohort_id": cohort_id,
        "source_sha256": source_sha256,
        "backend_image_id": backend_image_id,
        "input_evidence_sha256": input_evidence_sha256,
    }
    object.__setattr__(unsigned, "authorization_id", UUID(int=1))
    object.__setattr__(unsigned, "authorization_sha256", "0" * 64)
    for name, item in values.items():
        object.__setattr__(unsigned, name, item)
    digest = canonical_sha256(
        diagnostic_authorization_payload(
            unsigned,
            include_identity=False,
        )
    )
    return DiagnosticNonServingAuthorization(
        authorization_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:diagnostic-non-serving:{digest}",
        ),
        authorization_sha256=digest,
        **values,
    )


def diagnostic_authorization_from_payload(
    value: object,
) -> DiagnosticNonServingAuthorization:
    payload = _mapping(value, "diagnostic authorization")
    if (
        payload.get("schema") != DIAGNOSTIC_AUTHORIZATION_SCHEMA
        or payload.get("operation")
        != AdvancedSessionOperation.DIAGNOSTIC_NON_SERVING.value
        or payload.get("effects") != ZERO_NON_SERVING_EFFECTS
    ):
        raise ValueError("diagnostic non-serving contract changed")
    authorization = DiagnosticNonServingAuthorization(
        authorization_id=UUID(str(payload["authorization_id"])),
        authorization_sha256=str(
            payload["authorization_sha256"]
        ),
        authorization_reference=str(
            payload["authorization_reference"]
        ),
        owner=str(payload["owner"]),
        session_date=date.fromisoformat(str(payload["session_date"])),
        cohort_id=UUID(str(payload["cohort_id"])),
        source_sha256=str(payload["source_sha256"]),
        backend_image_id=str(payload["backend_image_id"]),
        input_evidence_sha256=str(
            payload["input_evidence_sha256"]
        ),
    )
    if diagnostic_authorization_payload(authorization) != dict(payload):
        raise ValueError("diagnostic authorization payload changed")
    return authorization


@dataclass(frozen=True, slots=True)
class DownstreamBuildSegment:
    segment_id: str
    source_sha256: str
    backend_image_id: str
    first_window_index: int
    last_window_index: int

    def __post_init__(self) -> None:
        _text(self.segment_id, "segment_id", maximum=96)
        _sha256(self.source_sha256, "source_sha256")
        _image_id(self.backend_image_id, "backend_image_id")
        if (
            type(self.first_window_index) is not int
            or type(self.last_window_index) is not int
            or not 1
            <= self.first_window_index
            <= self.last_window_index
            <= 78
        ):
            raise ValueError("downstream build segment bounds are invalid")


@dataclass(frozen=True, slots=True)
class ControlledDownstreamTransition:
    transition_id: UUID
    transition_sha256: str
    authorization_reference: str
    session_date: date
    completed_window_index: int
    completed_cycle_report_sha256: str
    preserved_acquisition_identity_sha256: str
    preserved_package_sha256: str
    preserved_bundle_sha256: str
    old_segment: DownstreamBuildSegment
    new_segment: DownstreamBuildSegment
    services: tuple[str, ...]
    tier_a_qualification_sha256: str
    tier_b_qualification_sha256: str

    def __post_init__(self) -> None:
        services = tuple(sorted(set(self.services)))
        try:
            window_count = len(_session_windows(self.session_date))
        except ValueError as exc:
            raise ValueError(
                "controlled downstream transition scope is invalid"
            ) from exc
        if (
            not isinstance(self.transition_id, UUID)
            or not self.transition_id.int
            or type(self.completed_window_index) is not int
            or not 1 <= self.completed_window_index < window_count
            or not services
            or any(
                service not in DOWNSTREAM_SERVICES
                for service in services
            )
            or self.old_segment.last_window_index
            != self.completed_window_index
            or self.new_segment.first_window_index
            != self.completed_window_index + 1
            or self.old_segment.first_window_index != 1
            or self.new_segment.last_window_index != window_count
            or self.old_segment.segment_id
            == self.new_segment.segment_id
            or self.old_segment.source_sha256
            == self.new_segment.source_sha256
            or self.old_segment.backend_image_id
            == self.new_segment.backend_image_id
        ):
            raise ValueError(
                "controlled downstream transition scope is invalid"
            )
        _text(self.authorization_reference, "authorization_reference")
        for name in (
            "completed_cycle_report_sha256",
            "preserved_acquisition_identity_sha256",
            "preserved_package_sha256",
            "preserved_bundle_sha256",
            "tier_a_qualification_sha256",
            "tier_b_qualification_sha256",
        ):
            _sha256(getattr(self, name), name)
        object.__setattr__(self, "services", services)
        expected = canonical_sha256(
            downstream_transition_payload(
                self,
                include_identity=False,
            )
        )
        expected_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:downstream-transition:{expected}",
        )
        if (
            self.transition_sha256 != expected
            or self.transition_id != expected_id
        ):
            raise ValueError("downstream transition identity changed")


def _segment_payload(
    segment: DownstreamBuildSegment,
) -> dict[str, object]:
    return {
        "segment_id": segment.segment_id,
        "source_sha256": segment.source_sha256,
        "backend_image_id": segment.backend_image_id,
        "first_window_index": segment.first_window_index,
        "last_window_index": segment.last_window_index,
    }


def downstream_transition_payload(
    transition: ControlledDownstreamTransition,
    *,
    include_identity: bool = True,
) -> dict[str, object]:
    value: dict[str, object] = {
        "schema": DOWNSTREAM_TRANSITION_SCHEMA,
        "operation": (
            AdvancedSessionOperation
            .CONTROLLED_DOWNSTREAM_TRANSITION.value
        ),
        "authorization_reference": (
            transition.authorization_reference
        ),
        "session_date": transition.session_date.isoformat(),
        "completed_window_index": (
            transition.completed_window_index
        ),
        "completed_cycle_report_sha256": (
            transition.completed_cycle_report_sha256
        ),
        "preserved_acquisition_identity_sha256": (
            transition.preserved_acquisition_identity_sha256
        ),
        "preserved_package_sha256": (
            transition.preserved_package_sha256
        ),
        "preserved_bundle_sha256": (
            transition.preserved_bundle_sha256
        ),
        "old_segment": _segment_payload(transition.old_segment),
        "new_segment": _segment_payload(transition.new_segment),
        "services": list(transition.services),
        "tier_a_qualification_sha256": (
            transition.tier_a_qualification_sha256
        ),
        "tier_b_qualification_sha256": (
            transition.tier_b_qualification_sha256
        ),
        "forbidden_changes": {
            "provider": False,
            "acquisition": False,
            "journal": False,
            "normalizer": False,
            "bridge": False,
            "finalizer": False,
            "budget": False,
            "session_authority": False,
        },
    }
    if include_identity:
        value["transition_id"] = str(transition.transition_id)
        value["transition_sha256"] = transition.transition_sha256
    return value


def build_downstream_transition(
    *,
    authorization_reference: str,
    session_date: date,
    completed_window_index: int,
    completed_cycle_report_sha256: str,
    preserved_acquisition_identity_sha256: str,
    preserved_package_sha256: str,
    preserved_bundle_sha256: str,
    old_segment: DownstreamBuildSegment,
    new_segment: DownstreamBuildSegment,
    services: Sequence[str],
    tier_a_qualification_sha256: str,
    tier_b_qualification_sha256: str,
) -> ControlledDownstreamTransition:
    unsigned = ControlledDownstreamTransition.__new__(
        ControlledDownstreamTransition
    )
    values = {
        "authorization_reference": authorization_reference,
        "session_date": session_date,
        "completed_window_index": completed_window_index,
        "completed_cycle_report_sha256": (
            completed_cycle_report_sha256
        ),
        "preserved_acquisition_identity_sha256": (
            preserved_acquisition_identity_sha256
        ),
        "preserved_package_sha256": preserved_package_sha256,
        "preserved_bundle_sha256": preserved_bundle_sha256,
        "old_segment": old_segment,
        "new_segment": new_segment,
        "services": tuple(services),
        "tier_a_qualification_sha256": (
            tier_a_qualification_sha256
        ),
        "tier_b_qualification_sha256": (
            tier_b_qualification_sha256
        ),
    }
    object.__setattr__(unsigned, "transition_id", UUID(int=1))
    object.__setattr__(unsigned, "transition_sha256", "0" * 64)
    for name, item in values.items():
        object.__setattr__(unsigned, name, item)
    digest = canonical_sha256(
        downstream_transition_payload(
            unsigned,
            include_identity=False,
        )
    )
    return ControlledDownstreamTransition(
        transition_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:downstream-transition:{digest}",
        ),
        transition_sha256=digest,
        **values,
    )


def downstream_transition_from_payload(
    value: object,
) -> ControlledDownstreamTransition:
    payload = _mapping(value, "downstream transition")
    old = _mapping(payload.get("old_segment"), "old_segment")
    new = _mapping(payload.get("new_segment"), "new_segment")
    expected_forbidden = {
        "provider": False,
        "acquisition": False,
        "journal": False,
        "normalizer": False,
        "bridge": False,
        "finalizer": False,
        "budget": False,
        "session_authority": False,
    }
    if (
        payload.get("schema") != DOWNSTREAM_TRANSITION_SCHEMA
        or payload.get("operation")
        != AdvancedSessionOperation
        .CONTROLLED_DOWNSTREAM_TRANSITION.value
        or payload.get("forbidden_changes") != expected_forbidden
    ):
        raise ValueError("controlled transition contract changed")

    def segment(row: Mapping[str, object]) -> DownstreamBuildSegment:
        return DownstreamBuildSegment(
            segment_id=str(row["segment_id"]),
            source_sha256=str(row["source_sha256"]),
            backend_image_id=str(row["backend_image_id"]),
            first_window_index=int(row["first_window_index"]),
            last_window_index=int(row["last_window_index"]),
        )

    transition = ControlledDownstreamTransition(
        transition_id=UUID(str(payload["transition_id"])),
        transition_sha256=str(payload["transition_sha256"]),
        authorization_reference=str(
            payload["authorization_reference"]
        ),
        session_date=date.fromisoformat(str(payload["session_date"])),
        completed_window_index=int(payload["completed_window_index"]),
        completed_cycle_report_sha256=str(
            payload["completed_cycle_report_sha256"]
        ),
        preserved_acquisition_identity_sha256=str(
            payload["preserved_acquisition_identity_sha256"]
        ),
        preserved_package_sha256=str(
            payload["preserved_package_sha256"]
        ),
        preserved_bundle_sha256=str(
            payload["preserved_bundle_sha256"]
        ),
        old_segment=segment(old),
        new_segment=segment(new),
        services=tuple(str(item) for item in payload["services"]),
        tier_a_qualification_sha256=str(
            payload["tier_a_qualification_sha256"]
        ),
        tier_b_qualification_sha256=str(
            payload["tier_b_qualification_sha256"]
        ),
    )
    if downstream_transition_payload(transition) != dict(payload):
        raise ValueError("controlled transition payload changed")
    return transition


def evaluate_session_evidence(
    value: object,
) -> dict[str, object]:
    evidence = _mapping(value, "session evidence")
    if evidence.get("schema") != SESSION_EVIDENCE_SCHEMA:
        raise ValueError("session evidence contract changed")
    try:
        session_date = date.fromisoformat(str(evidence.get("session_date")))
        expected_cutoffs = expected_session_cutoffs(session_date)
    except (TypeError, ValueError) as exc:
        raise ValueError("session evidence contract changed") from exc
    window_count = len(expected_cutoffs)
    acquisition = _mapping(
        evidence.get("acquisition"),
        "acquisition evidence",
    )
    cycles_value = evidence.get("cycles")
    segments_value = evidence.get("build_segments")
    transitions_value = evidence.get("transitions")
    cleanup = _mapping(evidence.get("cleanup"), "cleanup evidence")
    if (
        not isinstance(cycles_value, list)
        or not isinstance(segments_value, list)
        or not isinstance(transitions_value, list)
    ):
        raise TypeError("session evidence collections are invalid")
    cycles = [
        _mapping(row, "cycle evidence")
        for row in cycles_value
    ]
    segments = [
        _mapping(row, "build segment evidence")
        for row in segments_value
    ]
    transitions = [
        _mapping(row, "transition evidence")
        for row in transitions_value
    ]

    indices = [
        int(_path(row, "window.index") or 0)
        for row in cycles
    ]
    exact_cycles = (
        len(cycles) == window_count
        and sorted(indices) == list(range(1, window_count + 1))
        and all(
            row.get("status") == "COMPLETE_PUBLISHED_PINNED"
            and row.get("release_id") == row.get("read_pin_release_id")
            and row.get("stored_reader_count") == 4
            and row.get("stock_alert_evaluation_count") == 9
            and row.get("stock_alert_unavailable_count") == 9
            and row.get("stock_alert_reason_codes_exact") is True
            and type(row.get("e_o_seconds")) in {int, float}
            and 0 <= row["e_o_seconds"] <= 300
            and all(
                effect == 0
                for effect in _mapping(
                    row.get("forbidden_effects", {}),
                    "cycle forbidden effects",
                ).values()
            )
            for row in cycles
        )
    )
    acquisition_pass = (
        acquisition.get("preopen_identity_sha256")
        == acquisition.get("close_identity_sha256")
        and isinstance(
            acquisition.get("preopen_identity_sha256"),
            str,
        )
        and len(str(acquisition["preopen_identity_sha256"])) == 64
        and acquisition.get("opening_capture_complete") is True
        and acquisition.get("final_capture_complete") is True
        and acquisition.get("cutoffs") == list(expected_cutoffs)
        and acquisition.get("forbidden_changes") == 0
    )
    cleanup_pass = (
        cleanup.get("containers") == []
        and cleanup.get("stop_returncode", 0) == 0
        and cleanup.get("remove_returncode", 0) == 0
    )

    segment_results = []
    for segment in segments:
        first = int(segment.get("first_window_index", 0))
        last = int(segment.get("last_window_index", 0))
        segment_id = segment.get("segment_id")
        segment_indices = [
            int(_path(row, "window.index") or 0)
            for row in cycles
            if row.get("downstream_build_segment_id") == segment_id
        ]
        passed = (
            isinstance(segment_id, str)
            and first >= 1
            and last >= first + 2
            and last <= window_count
            and sorted(segment_indices)
            == list(range(first, last + 1))
            and segment.get("restart_rehydration_passed") is True
            and segment.get("publication_passed") is True
            and segment.get("read_pin_passed") is True
            and segment.get("stored_readers_passed") is True
        )
        segment_results.append({
            "segment_id": segment_id,
            "status": "PASS" if passed else "NOT_QUALIFIED",
            "first_window_index": first,
            "last_window_index": last,
        })
    end_to_end_pass = any(
        row["status"] == "PASS"
        for row in segment_results
    )
    transition_pass = (
        bool(transitions)
        and acquisition_pass
        and all(
            row.get("status") == "COMPLETE"
            and row.get("acquisition_containers_preserved") is True
            and row.get("cycle_backdated") is False
            and row.get("old_segment_id")
            != row.get("new_segment_id")
            for row in transitions
        )
        and len(segments) >= 2
        and all(
            row["status"] == "PASS"
            for row in segment_results
        )
    )
    single_build_pass = (
        acquisition_pass
        and exact_cycles
        and cleanup_pass
        and len(segments) == 1
        and segment_results
        and segment_results[0]["status"] == "PASS"
        and not transitions
    )

    def verdict(
        name: str,
        passed: bool,
        reasons: list[str],
    ) -> dict[str, object]:
        return {
            "verdict": name,
            "status": "PASS" if passed else "NOT_QUALIFIED",
            "reasons": [] if passed else reasons,
        }

    verdicts = {
        "PASS_ACQUISITION_FULL_SESSION": verdict(
            "PASS_ACQUISITION_FULL_SESSION",
            acquisition_pass,
            ["acquisition identity or opening-to-close coverage is incomplete"],
        ),
        "PASS_END_TO_END_SEGMENT": verdict(
            "PASS_END_TO_END_SEGMENT",
            end_to_end_pass,
            ["no explicit three-cycle build segment passed all gates"],
        ),
        "PASS_CONTROLLED_TRANSITION": verdict(
            "PASS_CONTROLLED_TRANSITION",
            transition_pass,
            ["transition, acquisition preservation or segment evidence is incomplete"],
        ),
        "PASS_FULL_SESSION_SINGLE_BUILD": verdict(
            "PASS_FULL_SESSION_SINGLE_BUILD",
            bool(single_build_pass),
            ["single-build cycles, effects, readers or cleanup are incomplete"],
        ),
    }
    report: dict[str, Any] = {
        "schema": SESSION_STATUS_SCHEMA,
        "session_date": session_date.isoformat(),
        "verdicts": verdicts,
        "segment_results": segment_results,
        "cycle_count": len(cycles),
        "complete_cycle_count": sum(
            row.get("status") == "COMPLETE_PUBLISHED_PINNED"
            for row in cycles
        ),
        "effects": dict(
            evidence.get("effects", ZERO_NON_SERVING_EFFECTS)
        ),
    }
    report["semantic_sha256"] = canonical_sha256(report)
    return report
