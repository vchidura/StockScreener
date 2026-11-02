"""Production dependency composition for the Advanced Options P8 factory."""
from __future__ import annotations

import os
import time
from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

from options.data.polygon_http import RequestsPolygonHttpTransport
from psycopg2.pool import ThreadedConnectionPool

from .advanced_equity_control_plane import (
    build_advanced_equity_control_plane_plan,
    load_advanced_equity_control_plane_plan_cursor,
    validate_active_target9_equity_source_cursor,
)
from .advanced_options_control_plane import (
    ADVANCED_OPTIONS_REST_LANES,
    AdvancedOptionsEquityInputBinding,
    AdvancedOptionsFeedScope,
    AdvancedOptionsOwnerNames,
    AdvancedOptionsPolicyPins,
    AdvancedOptionsReceiptInputs,
    AdvancedOptionsSocketBudget,
    advanced_options_equity_input_binding_payload,
    advanced_options_history_binding_payload,
    build_advanced_options_control_plane_plan,
    build_advanced_options_equity_input_binding,
    build_advanced_options_enrollment_plan,
    build_advanced_options_history_binding,
    build_advanced_options_prospective_reference_identity,
    load_advanced_options_equity_input_binding_cursor,
)
from .advanced_options_equity_input import (
    validate_advanced_options_equity_dependencies_cursor,
)
from .advanced_options_p8_canary_authorization import (
    LIVE_OBSERVATION_SCHEMA,
    PAGE_RELEASE_OBSERVATION_SCHEMA,
    PREPARED_AUTHORITIES,
    canonical_sha256,
)
from .advanced_options_p8_canary_executor import P8CanaryExecutionBinding
from .advanced_options_p8_selection_source import (
    MassiveAdvancedOptionsSelectionSource,
    P8ActivitySelectionCapture,
)
from .advanced_options_read_pin import (
    PostgresAdvancedOptionsStoredReadPort,
)
from .advanced_options_underlying_snapshot import (
    UnderlyingSnapshotBinding,
    binding_from_selection_capture,
    underlying_snapshot_binding_from_payload,
    underlying_snapshot_binding_payload,
)
from .advanced_options_service import (
    OPTIONS_LATE_CANARY_V4_STAGE_ORDER,
    OPTIONS_NON_SERVING_STAGE_ORDER,
    OPTIONS_V3_STAGE_ORDER,
)
from .advanced_options_projection import (
    OPTIONS_PAGE_SNAPSHOT_TYPES,
    OPTIONS_PROJECTION_TYPES,
    AdvancedOptionsPublicationAuthorization,
    AdvancedOptionsCanaryPublicationAuthorization,
    authorize_advanced_options_canary_publication,
    build_advanced_options_publisher_handoff,
)
from .advanced_options_v3_authority import (
    AdvancedOptionsWindowStageEffectAuthorizer,
    load_window_cycle_authorization_cursor,
    session_acquisition_authorization_from_payload,
)
from .advanced_options_v3_history import (
    AdvancedOptionsHistoryBindingIdentityError,
)
from .contracts import MarketChannel
from .database_access import (
    RealtimeDatabasePrincipal,
    RealtimeDatabaseRole,
    RealtimePoolBudget,
    RealtimeProcessDatabasePlan,
    RoleScopedConnectionProvider,
)
from .database_access_v2 import APP_ROLE, V2AccessConfig, V2ConnectionProvider
from .migrations import (
    EXPECTED_V2_DATABASE_NAME,
)
from .option_session_manifest import (
    BoundedOptionManifestPolicy,
    OptionActivityContract,
    OptionActivitySelectionRecord,
    option_activity_selection_from_payload,
    option_activity_selection_payload,
)
from .option_slot_set import SlotSetMember, SlotSetPurpose
from .orchestration_repository import PostgresOrchestrationRepository
from .portal_publisher import (
    CanonicalPortalPublisher,
    PortalPublicationBatch,
    PortalPublicationConfig,
)
from .preflight import DetectorFamily, ProductEntitlementProfile, ProviderTier
from .provider_budget import ProviderBudgetScope
from .acquisition_ledger import ProviderRequestLane
from .runner_environment import RunnerEnvironment
from .stage_graph import STAGES_V4, STAGE_CATALOG_VERSION_V4
from .stage_orchestrator import StageOrchestrator
from .stage_runners_v2 import build_advanced_options_non_serving_runners
from .worker_database import RealtimeProcessDatabase


UTC = timezone.utc
BUILD_ID = "advanced-options-p8-live-canary-v1"
P8_PRODUCTION_BINDING_SCHEMA = "advanced-options-p8-production-binding-v3"


def _open_interest_sha256(selected) -> str:
    return canonical_sha256(tuple(
        (
            row.contract_ticker,
            row.open_interest,
            row.market_data_time.isoformat(),
            row.source_sha256,
        )
        for row in selected
    ))


def _p8_manifest_policy() -> BoundedOptionManifestPolicy:
    return BoundedOptionManifestPolicy(
        version="advanced-options-p8-live-canary-v1",
        target_dtes=(0,),
        maximum_target_dte_deviation=0,
        strikes_per_side_per_expiry=1,
        minimum_dte=0,
        maximum_dte=0,
        strike_corridor_fraction=Decimal("0.05"),
        minimum_day_volume=100,
        minimum_open_interest=100,
        maximum_contracts_per_underlying=2,
        maximum_option_quote_contracts=4,
        reserved_option_quote_contracts=0,
        required_detectors=(
            DetectorFamily.O1,
            DetectorFamily.O2,
            DetectorFamily.O3,
            DetectorFamily.OS1,
            DetectorFamily.OS2,
        ),
    )


def _cycle_cutoff(
    capture,
    checked_at: datetime,
    stop: datetime,
    underlying_valid_until: datetime | None = None,
) -> datetime:
    cutoff = min(
        capture.selection.valid_until,
        stop - timedelta(minutes=1),
        *(
            ()
            if underlying_valid_until is None
            else (
                underlying_valid_until
                - timedelta(microseconds=1),
            )
        ),
    )
    if cutoff <= checked_at:
        raise RuntimeError("P8 canonical cycle cutoff is not prospective")
    return cutoff


def p8_options_cycle_cutoff_from_capture(
    capture: P8ActivitySelectionCapture,
    *,
    checked_at: datetime,
    stop: datetime,
) -> datetime:
    if not isinstance(capture, P8ActivitySelectionCapture):
        raise TypeError("P8 activity selection capture is invalid")
    underlying = binding_from_selection_capture(capture)
    return _cycle_cutoff(
        capture,
        checked_at.astimezone(UTC),
        stop.astimezone(UTC),
        underlying.valid_until,
    )


class P8ProductionConfigurationError(RuntimeError):
    """Production composition was refused before any provider or database effect."""


class _DockerWindowOrchestrator:
    def __init__(self, provider, package):
        self._provider = provider
        self._package = package
        self._plan_id = UUID(str(package["orchestration"]["plan_id"]))

    def start(self, plan_id):
        if plan_id != self._plan_id:
            raise ValueError("P8 WINDOW plan identity changed")
        plan = PostgresOrchestrationRepository(
            self._provider
        ).load_plan(plan_id)
        if (
            plan.stage_catalog_version != STAGE_CATALOG_VERSION_V4
            or plan.enabled_stages
            != frozenset(OPTIONS_NON_SERVING_STAGE_ORDER)
        ):
            raise ValueError("P8 WINDOW stage membership changed")
        return SimpleNamespace(enabled=plan.enabled_stages)

    def run_window(self, window_id):
        stop = datetime.fromisoformat(
            str(self._package["window"]["stop_utc"])
        )
        while datetime.now(UTC) < stop:
            with self._provider.transaction(
                APP_ROLE,
                read_only=True,
                dict_cursor=True,
            ) as unit:
                unit.cursor.execute(
                    "SELECT state FROM rt.window_runs "
                    "WHERE window_id=%s AND plan_id=%s",
                    (str(window_id), str(self._plan_id)),
                )
                row = unit.cursor.fetchone()
                if row is None:
                    raise RuntimeError("P8 WINDOW row is unavailable")
                state = str(row["state"])
                if state == "COMPLETE":
                    unit.cursor.execute(
                        "SELECT DISTINCT ON (stage_id) stage_id,state "
                        "FROM rt.stage_runs WHERE window_id=%s "
                        "ORDER BY stage_id,attempt DESC",
                        (str(window_id),),
                    )
                    stages = {
                        str(value["stage_id"]): str(value["state"])
                        for value in unit.cursor.fetchall()
                    }
                    if stages != {
                        name: "COMPLETE"
                        for name in OPTIONS_NON_SERVING_STAGE_ORDER
                    }:
                        raise RuntimeError(
                            "P8 WINDOW completed without exact stages"
                        )
                    return SimpleNamespace(
                        status="FINISHED",
                        window_state="COMPLETE",
                    )
                if state in {
                    "FAILED",
                    "MISSED",
                    "RETAINED_GAP",
                    "UNAVAILABLE",
                }:
                    return SimpleNamespace(
                        status="FINISHED",
                        window_state=state,
                    )
            time.sleep(0.1)
        raise RuntimeError("P8 WINDOW result missed the package bound")


def run_p8_production_role(**kwargs):
    from .advanced_options_validation_mode import (
        AdvancedOptionsValidationMode,
        advanced_options_validation_mode,
    )

    environment = kwargs.get("environment")
    mode = advanced_options_validation_mode(
        environment if isinstance(environment, Mapping) else None
    )
    if mode is AdvancedOptionsValidationMode.DEVELOPMENT_DIAGNOSTIC:
        from .advanced_options_v3_qualification import (
            run_provider_free_v3_role,
        )

        kwargs.pop("session_state", None)
        return run_provider_free_v3_role(**kwargs)
    from .advanced_options_p8_roles import run_p8_role

    return run_p8_role(**kwargs)


def _unavailable_serial_window(
    *,
    provider,
    binding,
    stage_order,
    flow_resolution,
    dependency_evidence,
):
    unavailable = [
        {
            "contract_ticker": row.contract_ticker,
            "underlyer": row.underlyer,
            "state": "UNAVAILABLE",
            "reason_code": row.reason_code,
        }
        for row in flow_resolution.unavailable
    ]
    stage_results = {}
    for stage_id in stage_order:
        evidence_sha256 = canonical_sha256({
            "contract": "advanced-options-unavailable-stage-v1",
            "window_id": str(binding.window_id),
            "stage_id": stage_id,
            "flow_unavailable_contracts": unavailable,
            "dependency_evidence": dependency_evidence,
        })
        stage_results[stage_id] = {
            "state": "UNAVAILABLE",
            "reason_codes": [
                "FLOW_POLICY_NO_ELIGIBLE_CONTRACTS",
                f"UNAVAILABLE_EVIDENCE:{evidence_sha256}",
            ],
            "evidence_sha256": evidence_sha256,
        }
    persisted = PostgresOrchestrationRepository(
        provider
    ).finish_unavailable_window(
        binding.window_id,
        stage_results,
        now=datetime.now(UTC),
    )
    return {
        "status": "FINISHED" if persisted else "NOT_CLAIMED",
        "window_state": "UNAVAILABLE",
        "window_id": str(binding.window_id),
        "stage_order": list(stage_order),
        "stage_results": stage_results,
        "flow_state": "UNAVAILABLE",
        "flow_unavailable_contracts": unavailable,
        "dependency_evidence": dependency_evidence,
        "persisted": persisted,
    }


def run_p8_serial_window(
    *,
    provider,
    binding,
    package,
    environment,
    dependency_evidence,
):
    from .advanced_journal_registry import (
        load_advanced_journal_registration_cursor,
    )
    from .advanced_options_ingress import AdvancedOptionsJournalSource
    from .advanced_options_p8_roles import (
        _flow_environment,
        _journal_root,
    )
    from .advanced_options_p8_window import (
        build_and_run_p8_serial_window,
    )

    control = binding.enrollment.control_plane
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        registration = load_advanced_journal_registration_cursor(
            unit.cursor,
            control.source_flow.journal_generation,
        )
    if (
        registration is None
        or registration.status not in {"DRAINING", "DRAINED"}
    ):
        raise RuntimeError("P8 WINDOW requires a sealed journal")
    source = AdvancedOptionsJournalSource(
        root=_journal_root(environment),
        registration=registration,
        source_flow=control.source_flow,
    )
    flow_owner_epoch = int(
        dependency_evidence["FLOW"].get("owner_epoch", 1)
    )
    (
        flow_environment,
        flow_resolution,
        _advanced_policy,
    ) = _flow_environment(
        provider,
        binding,
        source,
        flow_owner_epoch,
    )
    if flow_environment is None:
        return _unavailable_serial_window(
            provider=provider,
            binding=binding,
            stage_order=OPTIONS_LATE_CANARY_V4_STAGE_ORDER,
            flow_resolution=flow_resolution,
            dependency_evidence=dependency_evidence,
        )
    return build_and_run_p8_serial_window(
        provider=provider,
        binding=binding,
        package=package,
        flow_environment=flow_environment,
        contract_scope=flow_environment.options_flow_contract_scope,
        source_flow=flow_environment.options_flow_source.source_flow,
        dependency_evidence=dependency_evidence,
    )


def run_v3_serial_window(
    *,
    provider,
    pool,
    binding,
    package,
    bundle,
    environment,
    dependency_evidence,
):
    from .advanced_journal_registry import (
        load_advanced_journal_registration_cursor,
    )
    from .advanced_options_ingress import AdvancedOptionsJournalSource
    from .advanced_options_p8_roles import (
        _flow_environment,
        _journal_root,
    )
    from .advanced_options_p8_window import (
        build_and_run_p8_serial_window,
    )
    from .stage_graph import STAGE_CATALOG_VERSION_V5

    authorities = bundle.get("v3_authorities")
    references = bundle.get("v3_authority_references")
    if (
        not isinstance(authorities, Mapping)
        or set(authorities) != {"session_acquisition"}
        or not isinstance(references, Mapping)
    ):
        raise RuntimeError("V3 durable authority references are unavailable")
    session_authority = session_acquisition_authorization_from_payload(
        authorities.get("session_acquisition")
    )
    control = binding.enrollment.control_plane
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        cycle_authority = load_window_cycle_authorization_cursor(
            unit.cursor,
            target_window_id=binding.window_id,
            session_authorization_id=(
                session_authority.authorization_id
            ),
        )
        equity_input = load_advanced_options_equity_input_binding_cursor(
            unit.cursor,
            binding.window_id,
        )
    publication = references.get("canonical_publication")
    publication_authorization_id = uuid5(
        NAMESPACE_URL,
        (
            "advanced-options-v3-window-publication:"
            f"{publication['authorization_id']}:"
            f"{cycle_authority.authorization_id}"
        ),
    ) if isinstance(publication, Mapping) else None
    if (
        equity_input != binding.equity_input
        or cycle_authority.target_window_id != binding.window_id
        or cycle_authority.target_cohort_id != control.cohort_id
        or cycle_authority.session_authorization_sha256
        != session_authority.authorization_sha256
        or not isinstance(publication, Mapping)
        or publication.get("session_authorization_id")
        != str(session_authority.authorization_id)
        or not isinstance(publication_authorization_id, UUID)
        or cycle_authority.publication_authorization_reference is None
    ):
        raise RuntimeError("V3 durable split authority scope changed")
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        registration = load_advanced_journal_registration_cursor(
            unit.cursor,
            control.source_flow.journal_generation,
        )
    if (
        registration is None
        or registration.status
        not in {"WRITING", "DRAINING", "DRAINED"}
    ):
        raise RuntimeError("V3 WINDOW journal is unavailable")
    source = AdvancedOptionsJournalSource(
        root=_journal_root(environment),
        registration=registration,
        source_flow=control.source_flow,
    )
    flow_owner_epoch = int(
        dependency_evidence["FLOW"].get("owner_epoch", 1)
    )
    (
        flow_environment,
        flow_resolution,
        _advanced_policy,
    ) = _flow_environment(
        provider,
        binding,
        source,
        flow_owner_epoch,
    )
    if flow_environment is None:
        return _unavailable_serial_window(
            provider=provider,
            binding=binding,
            stage_order=OPTIONS_V3_STAGE_ORDER,
            flow_resolution=flow_resolution,
            dependency_evidence=dependency_evidence,
        )
    role_provider = RoleScopedConnectionProvider(
        {RealtimeDatabasePrincipal.RUNTIME: pool},
        {
            RealtimeDatabaseRole.PUBLICATION: (
                f"{EXPECTED_V2_DATABASE_NAME}_app"
            ),
        },
    )
    publisher_database = RealtimeProcessDatabase(
        RealtimeProcessDatabasePlan(
            "worker:portal_publisher:advanced_options_v3",
            RealtimeDatabaseRole.PUBLICATION,
            RealtimePoolBudget(0, 1),
        ),
        role_provider,
    )
    publisher = CanonicalPortalPublisher(
        publisher_database,
        PortalPublicationConfig(
            cohort_id=control.cohort_id,
            provider_tier=ProviderTier.MASSIVE_ADVANCED,
            owner_id=uuid5(
                NAMESPACE_URL,
                (
                    "advanced-options-v3-publisher:"
                    f"{publication['authorization_id']}"
                ),
            ),
            owner_epoch=1,
            dispatch_authorized=False,
        ),
    )

    def publication_authority(context, handoff):
        with provider.transaction(APP_ROLE, read_only=True) as unit:
            unit.cursor.execute(
                "SELECT receipt_id::text,input_sha256 "
                "FROM rt.cycle_receipts WHERE window_id=%s "
                "AND cohort_id=%s "
                "AND receipt_kind='OPTIONS_PORTAL_READY'",
                (
                    str(context.window.window_id),
                    str(context.plan.cohort_id),
                ),
            )
            row = unit.cursor.fetchone()
        if row is None or str(row[1]) != handoff.batch_sha256:
            raise RuntimeError(
                "V3 canonical publication receipt identity changed"
            )
        return AdvancedOptionsPublicationAuthorization(
            authorization_id=UUID(
                str(publication_authorization_id)
            ),
            cohort_id=control.cohort_id,
            provider_tier=ProviderTier.MASSIVE_ADVANCED,
            batch_sha256=handoff.batch_sha256,
            receipt_id=UUID(str(row[0])),
            scope_reference=(
                cycle_authority.publication_authorization_reference
            ),
        )

    return build_and_run_p8_serial_window(
        provider=provider,
        binding=binding,
        package=package,
        flow_environment=flow_environment,
        contract_scope=flow_environment.options_flow_contract_scope,
        source_flow=flow_environment.options_flow_source.source_flow,
        dependency_evidence=dependency_evidence,
        catalog_version=STAGE_CATALOG_VERSION_V5,
        publication_authority_loader=publication_authority,
        canonical_publisher=publisher.publish,
        window_stage_effect_authorizer=(
            AdvancedOptionsWindowStageEffectAuthorizer(
                cycle_authority
            )
        ),
        window_cutoff=cycle_authority.options_cutoff,
    )


def load_v3_pending_window_bindings(
    *,
    provider,
    binding,
    bundle,
    available_at: datetime,
):
    authorities = bundle.get("v3_authorities")
    if not isinstance(authorities, Mapping):
        raise RuntimeError("V3 session authority is unavailable")
    session = session_acquisition_authorization_from_payload(
        authorities.get("session_acquisition")
    )
    checked = available_at.astimezone(UTC)
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT cycle.target_window_id::text "
            "FROM rt.advanced_options_window_cycle_authorizations AS cycle "
            "JOIN rt.window_runs AS window "
            "ON window.window_id=cycle.target_window_id "
            "WHERE cycle.session_authorization_id=%s "
            "AND cycle.target_cohort_id=%s "
            "AND window.state IN ('PENDING','RUNNING') "
            "AND window.release_at<=%s "
            "ORDER BY cycle.options_cutoff,cycle.target_window_id",
            (
                str(session.authorization_id),
                str(binding.enrollment.control_plane.cohort_id),
                checked,
            ),
        )
        window_ids = tuple(
            UUID(str(row[0])) for row in unit.cursor.fetchall()
        )
        values = []
        for window_id in window_ids:
            equity_input = load_advanced_options_equity_input_binding_cursor(
                unit.cursor,
                window_id,
            )
            if equity_input is None:
                continue
            values.append(replace(
                binding,
                equity_input=equity_input,
                window_id=window_id,
            ))
    return tuple(values)


def _manifest_policy_payload(policy):
    return {
        "version": policy.version,
        "target_dtes": list(policy.target_dtes),
        "maximum_target_dte_deviation": (
            policy.maximum_target_dte_deviation
        ),
        "strikes_per_side_per_expiry": policy.strikes_per_side_per_expiry,
        "minimum_dte": policy.minimum_dte,
        "maximum_dte": policy.maximum_dte,
        "strike_corridor_fraction": str(policy.strike_corridor_fraction),
        "minimum_day_volume": policy.minimum_day_volume,
        "minimum_open_interest": policy.minimum_open_interest,
        "maximum_contracts_per_underlying": (
            policy.maximum_contracts_per_underlying
        ),
        "maximum_option_quote_contracts": (
            policy.maximum_option_quote_contracts
        ),
        "reserved_option_quote_contracts": (
            policy.reserved_option_quote_contracts
        ),
        "required_detectors": [
            value.value for value in policy.required_detectors
        ],
    }


def _production_binding_payload(
    *,
    package,
    binding,
    equity,
    control,
    reference,
    receipt_inputs,
    selection,
):
    value = {
        "schema": P8_PRODUCTION_BINDING_SCHEMA,
        "package_sha256": package["package_sha256"],
        "window_id": str(binding.window_id),
        "enrollment_id": str(binding.enrollment.enrollment_id),
        "enrollment_sha256": binding.enrollment.enrollment_sha256,
        "equity": {
            "session_date": equity.session_date.isoformat(),
            "security_ids": [
                [ticker, str(security_id)]
                for ticker, security_id in equity.security_ids
            ],
            "entitlement_evidence_sha256": (
                equity.entitlement_evidence_sha256
            ),
            "authorization_reference": equity.authorization_reference,
            "checked_at": equity.checked_at.isoformat(),
            "expires_at": equity.expires_at.isoformat(),
            "schema_sha256": equity.schema_sha256,
            "security_policy_sha256": equity.security_policy_sha256,
            "configuration_sha256": equity.configuration_sha256,
        },
        "control": {
            "option_underlyings": list(
                control.universe.option_underlyings
            ),
            "options_entitlement_evidence_sha256": (
                control.options_entitlement_evidence_sha256
            ),
            "options_authorization_reference": (
                control.options_authorization_reference
            ),
            "checked_at": control.checked_at.isoformat(),
            "expires_at": control.expires_at.isoformat(),
            "planned_at": control.planned_at.isoformat(),
            "equity_input_cutoff": (
                binding.equity_input.equity_cutoff.isoformat()
            ),
            "manifest_policy": _manifest_policy_payload(
                control.manifest_policy
            ),
            "slot_members": [
                {
                    "instrument_id": row.instrument_id,
                    "channel": row.channel.value,
                    "purpose": row.purpose.value,
                    "priority": row.priority,
                    "catalog_version_id": row.catalog_version_id,
                }
                for row in control.slot_set.members
            ],
            "slot_effective_at": control.slot_set.effective_at.isoformat(),
            "pins": {
                name: getattr(control.pins, name)
                for name in (
                    "condition_semantics_sha256",
                    "flow_policy_sha256",
                    "valuation_policy_sha256",
                    "model_bundle_sha256",
                    "catalog_sha256",
                    "slot_set_policy_sha256",
                )
            },
            "socket_budget": {
                name: getattr(control.socket_budget, name)
                for name in (
                    "stream_sessions",
                    "connection_attempts",
                    "reconnects",
                    "decoded_messages",
                    "decoded_bytes",
                )
            },
            "provider_budget": {
                "account_alias": control.provider_budget.account_alias,
                "session_date": (
                    control.provider_budget.session_date.isoformat()
                ),
                "authorization_reference": (
                    control.provider_budget.authorization_reference
                ),
                "provider_authorization_reference": (
                    control.provider_budget.provider_authorization_reference
                ),
                "expected_cost": control.provider_budget.expected_cost,
                "request_ceiling": control.provider_budget.request_ceiling,
                "decoded_byte_ceiling": (
                    control.provider_budget.decoded_byte_ceiling
                ),
                "lanes": sorted(
                    lane.value for lane in control.provider_budget.lanes
                ),
                "authorized_until": (
                    control.provider_budget.authorized_until.isoformat()
                ),
                "policy_version": control.provider_budget.policy_version,
                **(
                    {}
                    if control.provider_budget.account_request_ceiling
                    is None
                    else {
                        "account_request_ceiling": (
                            control.provider_budget
                            .account_request_ceiling
                        ),
                        "account_decoded_byte_ceiling": (
                            control.provider_budget
                            .account_decoded_byte_ceiling
                        ),
                    }
                ),
            },
            "journal_generation": str(
                control.source_flow.journal_generation
            ),
            "stream_session_ids": [
                str(row) for row in control.source_flow.stream_session_ids
            ],
            "receipt_inputs": {
                "cutoff": receipt_inputs.cutoff.isoformat(),
                "reference": {
                    "reference_generation": (
                        reference.reference_generation
                    ),
                    "session_date": reference.session_date.isoformat(),
                    "known_at": reference.known_at.isoformat(),
                    "valid_until": reference.valid_until.isoformat(),
                    "underlyings": list(reference.underlyings),
                    "open_interest_sha256": (
                        reference.open_interest_sha256
                    ),
                },
                "equity_input_receipt_id": str(
                    receipt_inputs.equity_input_receipt_id
                ),
                **{
                    name: getattr(receipt_inputs, name)
                    for name in (
                        "equity_input_sha256",
                        "discovery_batches_sha256",
                        "open_interest_sha256",
                        "consumer_checkpoints_sha256",
                        "unresolved_gaps_sha256",
                        "market_state_sha256",
                        "source_clocks_sha256",
                        "detector_outcomes_sha256",
                        "projection_sha256",
                        "build_id",
                    )
                },
            },
            "authorized_cohort_id": str(control.cohort_id),
            "authorized_plan_id": str(control.plan_id),
        },
        "selection": option_activity_selection_payload(selection),
        "history_binding": advanced_options_history_binding_payload(
            binding.history
        ),
        "equity_input_binding": (
            advanced_options_equity_input_binding_payload(
                binding.equity_input
            )
        ),
        "underlying_price_binding": (
            underlying_snapshot_binding_payload(
                binding.underlying_price_binding
            )
        ),
    }
    value["binding_sha256"] = canonical_sha256(value)
    return value


def p8_production_binding_from_payload(value):
    if not isinstance(value, dict):
        raise TypeError("P8 production binding payload must be an object")
    digest = value.get("binding_sha256")
    unsigned = dict(value)
    unsigned.pop("binding_sha256", None)
    if (
        value.get("schema") != P8_PRODUCTION_BINDING_SCHEMA
        or digest != canonical_sha256(unsigned)
    ):
        raise ValueError("P8 production binding identity changed")
    equity_value = value["equity"]
    control_value = value["control"]
    equity = build_advanced_equity_control_plane_plan(
        session_date=datetime.fromisoformat(
            equity_value["session_date"]
        ).date(),
        security_ids={
            ticker: UUID(security_id)
            for ticker, security_id in equity_value["security_ids"]
        },
        entitlement_evidence_sha256=equity_value[
            "entitlement_evidence_sha256"
        ],
        authorization_reference=equity_value["authorization_reference"],
        checked_at=datetime.fromisoformat(equity_value["checked_at"]),
        expires_at=datetime.fromisoformat(equity_value["expires_at"]),
        schema_sha256=equity_value["schema_sha256"],
        security_policy_sha256=equity_value[
            "security_policy_sha256"
        ],
        configuration_sha256=equity_value["configuration_sha256"],
    )
    policy_value = control_value["manifest_policy"]
    policy = BoundedOptionManifestPolicy(
        version=policy_value["version"],
        target_dtes=tuple(policy_value["target_dtes"]),
        maximum_target_dte_deviation=policy_value[
            "maximum_target_dte_deviation"
        ],
        strikes_per_side_per_expiry=policy_value[
            "strikes_per_side_per_expiry"
        ],
        minimum_dte=policy_value["minimum_dte"],
        maximum_dte=policy_value["maximum_dte"],
        strike_corridor_fraction=Decimal(
            policy_value["strike_corridor_fraction"]
        ),
        minimum_day_volume=policy_value["minimum_day_volume"],
        minimum_open_interest=policy_value["minimum_open_interest"],
        maximum_contracts_per_underlying=policy_value[
            "maximum_contracts_per_underlying"
        ],
        maximum_option_quote_contracts=policy_value[
            "maximum_option_quote_contracts"
        ],
        reserved_option_quote_contracts=policy_value[
            "reserved_option_quote_contracts"
        ],
        required_detectors=tuple(
            DetectorFamily(name)
            for name in policy_value["required_detectors"]
        ),
    )
    receipt_value = control_value["receipt_inputs"]
    reference_value = receipt_value["reference"]
    reference = build_advanced_options_prospective_reference_identity(
        reference_generation=reference_value["reference_generation"],
        session_date=datetime.fromisoformat(
            reference_value["session_date"]
        ).date(),
        known_at=datetime.fromisoformat(reference_value["known_at"]),
        valid_until=datetime.fromisoformat(
            reference_value["valid_until"]
        ),
        underlyings=tuple(reference_value["underlyings"]),
        open_interest_sha256=reference_value["open_interest_sha256"],
    )
    receipt_inputs = AdvancedOptionsReceiptInputs(
        cutoff=datetime.fromisoformat(receipt_value["cutoff"]),
        reference_state=reference,
        equity_input_receipt_id=UUID(
            receipt_value["equity_input_receipt_id"]
        ),
        **{
            name: receipt_value[name]
            for name in (
                "equity_input_sha256",
                "discovery_batches_sha256",
                "open_interest_sha256",
                "consumer_checkpoints_sha256",
                "unresolved_gaps_sha256",
                "market_state_sha256",
                "source_clocks_sha256",
                "detector_outcomes_sha256",
                "projection_sha256",
                "build_id",
            )
        },
    )
    provider_value = control_value["provider_budget"]
    control = build_advanced_options_control_plane_plan(
        equity_plan=equity,
        entitlement=ProductEntitlementProfile(
            ProviderTier.MASSIVE_ADVANCED,
            ProviderTier.MASSIVE_ADVANCED,
        ),
        option_underlyings=tuple(control_value["option_underlyings"]),
        options_entitlement_evidence_sha256=control_value[
            "options_entitlement_evidence_sha256"
        ],
        options_authorization_reference=control_value[
            "options_authorization_reference"
        ],
        checked_at=datetime.fromisoformat(control_value["checked_at"]),
        expires_at=datetime.fromisoformat(control_value["expires_at"]),
        planned_at=datetime.fromisoformat(control_value["planned_at"]),
        manifest_policy=policy,
        slot_members=tuple(
            SlotSetMember(
                row["instrument_id"],
                MarketChannel(row["channel"]),
                SlotSetPurpose(row["purpose"]),
                row["priority"],
                row["catalog_version_id"],
            )
            for row in control_value["slot_members"]
        ),
        slot_effective_at=datetime.fromisoformat(
            control_value["slot_effective_at"]
        ),
        feed_scope=AdvancedOptionsFeedScope(),
        pins=AdvancedOptionsPolicyPins(**control_value["pins"]),
        socket_budget=AdvancedOptionsSocketBudget(
            **control_value["socket_budget"]
        ),
        provider_budget=ProviderBudgetScope(
            account_alias=provider_value["account_alias"],
            session_date=datetime.fromisoformat(
                provider_value["session_date"]
            ).date(),
            authorization_reference=provider_value[
                "authorization_reference"
            ],
            provider_authorization_reference=provider_value[
                "provider_authorization_reference"
            ],
            expected_cost=provider_value["expected_cost"],
            request_ceiling=provider_value["request_ceiling"],
            decoded_byte_ceiling=provider_value[
                "decoded_byte_ceiling"
            ],
            lanes=frozenset(
                ProviderRequestLane(name)
                for name in provider_value["lanes"]
            ),
            authorized_until=datetime.fromisoformat(
                provider_value["authorized_until"]
            ),
            policy_version=provider_value["policy_version"],
            account_request_ceiling=provider_value.get(
                "account_request_ceiling"
            ),
            account_decoded_byte_ceiling=provider_value.get(
                "account_decoded_byte_ceiling"
            ),
        ),
        owners=AdvancedOptionsOwnerNames(),
        journal_generation=UUID(control_value["journal_generation"]),
        stream_session_ids=tuple(
            UUID(row) for row in control_value["stream_session_ids"]
        ),
        receipt_inputs=receipt_inputs,
        equity_input_cutoff=datetime.fromisoformat(
            control_value["equity_input_cutoff"]
        ),
        authorized_cohort_id=UUID(
            control_value["authorized_cohort_id"]
        ),
        authorized_plan_id=UUID(control_value["authorized_plan_id"]),
    )
    selection = option_activity_selection_from_payload(
        value["selection"],
        required_at=control.planned_at,
        expected_underlyings=control.universe.option_underlyings,
        expected_quote_ceiling=(
            control.manifest_policy.maximum_option_quote_contracts
        ),
    )
    enrollment = build_advanced_options_enrollment_plan(
        control_plane=control,
        equity_plan=equity,
        activity_selection=selection,
    )
    if (
        str(enrollment.enrollment_id) != value["enrollment_id"]
        or enrollment.enrollment_sha256 != value["enrollment_sha256"]
    ):
        raise ValueError("P8 rebuilt enrollment identity changed")
    return P8CanaryExecutionBinding(
        enrollment,
        build_advanced_options_history_binding(**{
            "target_cohort_id": UUID(
                value["history_binding"]["target_cohort_id"]
            ),
            "source_cohort_id": UUID(
                value["history_binding"]["source_cohort_id"]
            ),
            "history_before": datetime.fromisoformat(
                value["history_binding"]["history_before"]
            ),
            "source_manifest_sha256": value[
                "history_binding"
            ]["source_manifest_sha256"],
            "authorization_reference": value[
                "history_binding"
            ]["authorization_reference"],
            "evidence_sha256": value["history_binding"].get(
                "evidence_sha256"
            ),
        }),
        build_advanced_options_equity_input_binding(**{
            "target_options_window_id": UUID(
                value["equity_input_binding"][
                    "target_options_window_id"
                ]
            ),
            "target_cohort_id": UUID(
                value["equity_input_binding"]["target_cohort_id"]
            ),
            "source_cohort_id": UUID(
                value["equity_input_binding"]["source_cohort_id"]
            ),
            "source_cohort_sha256": value[
                "equity_input_binding"
            ]["source_cohort_sha256"],
            "source_manifest_id": UUID(
                value["equity_input_binding"]["source_manifest_id"]
            ),
            "source_manifest_sha256": value[
                "equity_input_binding"
            ]["source_manifest_sha256"],
            "source_profile_id": UUID(
                value["equity_input_binding"]["source_profile_id"]
            ),
            "source_profile_sha256": value[
                "equity_input_binding"
            ]["source_profile_sha256"],
            "equity_window_id": UUID(
                value["equity_input_binding"]["equity_window_id"]
            ),
            "equity_receipt_id": UUID(
                value["equity_input_binding"]["equity_receipt_id"]
            ),
            "equity_cutoff": datetime.fromisoformat(
                value["equity_input_binding"]["equity_cutoff"]
            ),
            "reference_generation": value[
                "equity_input_binding"
            ]["reference_generation"],
            "universe_sha256": value[
                "equity_input_binding"
            ]["universe_sha256"],
            "input_sha256": value[
                "equity_input_binding"
            ]["input_sha256"],
            "build_id": value["equity_input_binding"]["build_id"],
            "maximum_age_seconds": value[
                "equity_input_binding"
            ]["maximum_age_seconds"],
            "authorization_reference": value[
                "equity_input_binding"
            ]["authorization_reference"],
        }),
        UUID(value["window_id"]),
        underlying_snapshot_binding_from_payload(
            value["underlying_price_binding"]
        ),
    )


def _required_environment(name: str) -> str:
    value = str(os.environ.get(name, "")).strip()
    if not value:
        raise P8ProductionConfigurationError(f"{name} is required")
    return value


class _PostgresSelectionSpots:
    def __init__(
        self,
        provider: V2ConnectionProvider,
        source_cohort_id: UUID,
    ) -> None:
        self._provider = provider
        self._source_cohort_id = source_cohort_id

    def load_spots(self, *, underlyings, available_before):
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
            dict_cursor=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            unit.cursor.execute(
                "SELECT DISTINCT ON (revision.ticker) revision.ticker,"
                "revision.close_price,revision.observed_at "
                "FROM rt.bar_current AS current "
                "JOIN rt.bar_revisions AS revision "
                "ON revision.bar_revision_id=current.bar_revision_id "
                "AND revision.observed_at=current.observed_at "
                "WHERE current.cohort_id=%s "
                "AND revision.ticker=ANY(%s) "
                "AND current.interval='1m' "
                "AND current.adjustment_basis='RAW_UNADJUSTED' "
                "AND revision.observed_at<=%s "
                "ORDER BY revision.ticker,revision.bar_end DESC,"
                "revision.observed_at DESC",
                (
                    str(self._source_cohort_id),
                    list(underlyings),
                    available_before,
                ),
            )
            rows = tuple(unit.cursor.fetchall())
        spots = {
            str(row["ticker"]): Decimal(str(row["close_price"]))
            for row in rows
        }
        if set(spots) != set(underlyings):
            raise RuntimeError("P8 retained spot coverage is incomplete")
        return spots


class _P8ProductionCapture:
    def __init__(
        self,
        provider: V2ConnectionProvider,
        package: Mapping[str, object],
        api_key: str | None,
        stored_reads_authorized: bool,
    ) -> None:
        self._provider = provider
        self._package = package
        self._stored_reads_authorized = stored_reads_authorized
        source_cohort_id = UUID(
            str(package["history_binding"]["source_cohort_id"])
        )
        self._selection = (
            None
            if api_key is None
            else MassiveAdvancedOptionsSelectionSource(
                RequestsPolygonHttpTransport(
                    api_key,
                    maximum_response_bytes=100_000_000,
                ),
                _PostgresSelectionSpots(provider, source_cohort_id),
                clock=lambda: datetime.now(UTC),
            )
        )
        self.binding: P8CanaryExecutionBinding | None = None
        self.checked_at: datetime | None = None
        self.cycle_cutoff: datetime | None = None
        self.production_binding_payload: Mapping[str, object] | None = None
        self.selection_capture = None
        self.underlying_price_binding: UnderlyingSnapshotBinding | None = None
        self.equity_input_readiness: Mapping[str, object] | None = None

    def _catalog(self, capture, checked_at):
        tickers = list(capture.selection.contracts)
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
            dict_cursor=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            unit.cursor.execute(
                "SELECT DISTINCT ON (contract.contract_ticker) "
                "instrument.instrument_id,contract.contract_id,"
                "version.version_id AS catalog_version_id,"
                "contract.contract_ticker,security.ticker AS underlyer,"
                "version.call_put,version.expiration,version.strike,"
                "version.exercise_style,version.multiplier,"
                "version.deliverables,version.eligibility,"
                "version.effective_from,version.effective_to,"
                "version.observed_at,version.payload_sha256 "
                "FROM ref.option_contracts AS contract "
                "JOIN ref.option_contract_versions AS version "
                "USING (contract_id) "
                "JOIN rt.instruments AS instrument "
                "ON instrument.contract_id=contract.contract_id "
                "JOIN LATERAL ("
                "SELECT revision.ticker FROM ref.security_revisions AS revision "
                "WHERE revision.security_id=contract.underlying_security_id "
                "AND revision.effective_from<=%s "
                "AND (revision.effective_to IS NULL "
                "OR revision.effective_to>%s) "
                "AND revision.observed_at<=%s "
                "ORDER BY revision.effective_from DESC,"
                "revision.observed_at DESC LIMIT 1"
                ") AS security ON true "
                "WHERE contract.contract_ticker=ANY(%s) "
                "AND version.effective_from<=%s "
                "AND version.effective_to IS NULL "
                "AND version.observed_at<=%s "
                "ORDER BY contract.contract_ticker,"
                "version.effective_from DESC,version.observed_at DESC",
                (
                    checked_at,
                    checked_at,
                    checked_at,
                    tickers,
                    checked_at,
                    checked_at,
                ),
            )
            rows = tuple(unit.cursor.fetchall())
        if (
            len(rows) != len(tickers)
            or {str(row["contract_ticker"]) for row in rows} != set(tickers)
        ):
            raise RuntimeError(
                "P8 selected contract catalog identity is unavailable"
            )
        by_ticker = {
            row.contract_ticker: row
            for row in capture.selection.selected
        }
        contracts = []
        for row in rows:
            selected = by_ticker[str(row["contract_ticker"])]
            deliverables = tuple(row["deliverables"] or ())
            contracts.append(OptionActivityContract(
                instrument_id=int(row["instrument_id"]),
                contract_id=int(row["contract_id"]),
                catalog_version_id=int(row["catalog_version_id"]),
                contract_ticker=str(row["contract_ticker"]),
                underlyer=str(row["underlyer"]),
                call_put=str(row["call_put"]),
                expiration_date=row["expiration"],
                strike=Decimal(str(row["strike"])),
                exercise_style=str(row["exercise_style"]),
                multiplier=Decimal(str(row["multiplier"])),
                deliverables=deliverables,
                eligibility=str(row["eligibility"]),
                catalog_effective_from=row["effective_from"],
                catalog_effective_to=row["effective_to"],
                catalog_observed_at=row["observed_at"],
                catalog_payload_sha256=str(row["payload_sha256"]),
                market_data_time=selected.market_data_time,
                source_sha256=selected.source_sha256,
            ))
        return tuple(contracts)

    def _history_manifest(self):
        history_scope = self._package["history_binding"]
        source_cohort = history_scope["source_cohort_id"]
        v3_history = self._package.get("v3_history_binding")
        v3_binding = (
            v3_history.get("binding")
            if isinstance(v3_history, Mapping)
            else None
        )
        if isinstance(self._package.get("v3_deployment"), Mapping) and (
            not isinstance(v3_history, Mapping)
            or not isinstance(v3_history.get("evidence"), Mapping)
            or not isinstance(v3_binding, Mapping)
            or v3_binding.get("source_cohort_id")
            != str(source_cohort)
            or v3_binding.get(
                "source_manifest_sha256"
            )
            != history_scope.get("source_manifest_sha256")
        ):
            raise AdvancedOptionsHistoryBindingIdentityError(
                "V3 H binding package identity is absent or divergent"
            )
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
            dict_cursor=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            unit.cursor.execute(
                "SELECT manifest.manifest_sha256,profile.provider_tier "
                "FROM rt.cohorts AS cohort "
                "JOIN rt.subscription_manifests AS manifest "
                "USING (manifest_id) "
                "JOIN rt.capability_profiles AS profile "
                "ON profile.profile_id=manifest.profile_id "
                "WHERE cohort.cohort_id=%s",
                (str(source_cohort),),
            )
            row = unit.cursor.fetchone()
        if (
            row is None
            or str(row["provider_tier"]) != "MASSIVE_DEVELOPER"
        ):
            raise RuntimeError("P8 history source identity is unavailable")
        return str(row["manifest_sha256"])

    def _live_state(self, checked_at, capture):
        package = self._package
        start = datetime.fromisoformat(str(package["window"]["start_utc"]))
        stop = datetime.fromisoformat(str(package["window"]["stop_utc"]))
        if self.underlying_price_binding is None:
            raise RuntimeError("P8 underlying snapshot binding is unavailable")
        _cycle_cutoff(
            capture,
            checked_at,
            stop,
            self.underlying_price_binding.valid_until,
        )
        resources = [
            str(row["lease_resource_key"])
            for row in package["owners"]["processes"]
        ]
        cohort_id = str(package["cohort"]["cohort_id"])
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
            dict_cursor=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            unit.cursor.execute(
                "SELECT resource_key,owner_id::text,owner_epoch,lease_until "
                "FROM rt.worker_leases WHERE resource_key=ANY(%s) "
                "AND lease_until>%s ORDER BY resource_key",
                (resources, checked_at),
            )
            peers = [dict(row) for row in unit.cursor.fetchall()]
            unit.cursor.execute(
                "SELECT release_id::text FROM rt.portal_releases "
                "WHERE cohort_id=%s",
                (cohort_id,),
            )
            releases = [str(row["release_id"]) for row in unit.cursor.fetchall()]
            unit.cursor.execute(
                "SELECT release_id::text FROM rt.portal_release_current "
                "WHERE cohort_id=%s",
                (cohort_id,),
            )
            pointers = [str(row["release_id"]) for row in unit.cursor.fetchall()]
            unit.cursor.execute(
                "SELECT release_at AS start_utc,deadline AS stop_utc "
                "FROM rt.window_runs WHERE cohort_id<>%s "
                "AND provider_tier='MASSIVE_ADVANCED' "
                "AND release_at<%s AND deadline>%s "
                "AND state IN ('PENDING','RUNNING') "
                "ORDER BY release_at",
                (cohort_id, stop, start),
            )
            windows = [
                {
                    "start_utc": row["start_utc"].isoformat(),
                    "stop_utc": row["stop_utc"].isoformat(),
                }
                for row in unit.cursor.fetchall()
            ]
            unit.cursor.execute(
                "SELECT resource_key FROM rt.worker_leases "
                "WHERE resource_key LIKE 'retention:%' "
                "AND lease_until>%s ORDER BY resource_key",
                (checked_at,),
            )
            retention = [
                str(row["resource_key"]) for row in unit.cursor.fetchall()
            ]
            unit.cursor.execute(
                "SELECT release_id::text FROM "
                "rt.advanced_options_read_pins "
                "WHERE cohort_id=%s AND enabled",
                (cohort_id,),
            )
            read_pins = [
                str(row["release_id"]) for row in unit.cursor.fetchall()
            ]
        return peers, releases, pointers, windows, retention, read_pins

    def _equity_source(
        self,
        checked_at,
        binding: AdvancedOptionsEquityInputBinding | None = None,
    ):
        if binding is None:
            value = self._package["equity_input_binding"]
            binding = build_advanced_options_equity_input_binding(
                target_options_window_id=UUID(str(
                    value["target_options_window_id"]
                )),
                target_cohort_id=UUID(str(value["target_cohort_id"])),
                source_cohort_id=UUID(str(value["source_cohort_id"])),
                source_cohort_sha256=str(value["source_cohort_sha256"]),
                source_manifest_id=UUID(str(value["source_manifest_id"])),
                source_manifest_sha256=str(
                    value["source_manifest_sha256"]
                ),
                source_profile_id=UUID(str(value["source_profile_id"])),
                source_profile_sha256=str(
                    value["source_profile_sha256"]
                ),
                equity_window_id=UUID(str(value["equity_window_id"])),
                equity_receipt_id=UUID(str(value["equity_receipt_id"])),
                equity_cutoff=datetime.fromisoformat(
                    str(value["equity_cutoff"])
                ),
                reference_generation=int(
                    value["reference_generation"]
                ),
                universe_sha256=str(value["universe_sha256"]),
                input_sha256=str(value["input_sha256"]),
                build_id=str(value["build_id"]),
                maximum_age_seconds=int(
                    value["maximum_age_seconds"]
                ),
                authorization_reference=str(
                    value["authorization_reference"]
                ),
            )
        elif not isinstance(binding, AdvancedOptionsEquityInputBinding):
            raise TypeError("P8 per-E Equity binding is invalid")
        if (
            binding.equity_cutoff > checked_at
            or (
                checked_at - binding.equity_cutoff
            ).total_seconds() > binding.maximum_age_seconds
        ):
            raise RuntimeError(
                "P8 completed Equity input is stale before provider capture"
            )
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
            dict_cursor=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            plan = load_advanced_equity_control_plane_plan_cursor(
                unit.cursor,
                binding.source_cohort_id,
            )
            validate_active_target9_equity_source_cursor(
                unit.cursor,
                plan,
            )
            unit.cursor.execute(
                "SELECT receipt.window_id::text,receipt.cohort_id::text,"
                "receipt.receipt_kind,receipt.cutoff,"
                "receipt.reference_generation,receipt.universe_sha256,"
                "receipt.input_sha256,receipt.build_id,window_run.state "
                "FROM rt.cycle_receipts AS receipt "
                "JOIN rt.window_runs AS window_run "
                "ON window_run.window_id=receipt.window_id "
                "WHERE receipt.receipt_id=%s",
                (str(binding.equity_receipt_id),),
            )
            receipt = unit.cursor.fetchone()
            readiness = validate_advanced_options_equity_dependencies_cursor(
                unit.cursor,
                equity_plan=plan,
                equity_input=binding,
                history_source_cohort_id=UUID(str(
                    self._package["history_binding"]["source_cohort_id"]
                )),
                underlyings=tuple(
                    self._package["cohort"]["underlyings"]
                ),
            )
        if (
            plan.cohort_id != binding.source_cohort_id
            or plan.cohort_sha256 != binding.source_cohort_sha256
            or plan.manifest_id != binding.source_manifest_id
            or plan.manifest.sha256 != binding.source_manifest_sha256
            or plan.profile_id != binding.source_profile_id
            or plan.profile_sha256 != binding.source_profile_sha256
            or plan.universe.sha256 != binding.universe_sha256
            or not set(self._package["cohort"]["underlyings"]).issubset(
                set(plan.universe.equity_tickers)
            )
            or receipt is None
            or tuple(receipt.values()) != (
                str(binding.equity_window_id),
                str(binding.source_cohort_id),
                "INPUT_READY",
                binding.equity_cutoff,
                binding.reference_generation,
                binding.universe_sha256,
                binding.input_sha256,
                binding.build_id,
                "COMPLETE",
            )
        ):
            raise RuntimeError(
                "P8 completed Equity input binding is unavailable"
            )
        self.equity_input_readiness = readiness
        return plan, binding

    def _binding(
        self,
        capture,
        checked_at,
        history_manifest,
        equity,
        equity_input,
        *,
        target_window_id=None,
    ):
        package = self._package
        stop = datetime.fromisoformat(str(package["window"]["stop_utc"]))
        underlying_price_binding = binding_from_selection_capture(capture)
        checked_at = max(
            checked_at,
            underlying_price_binding.reviewed_at,
        )
        cycle_cutoff = _cycle_cutoff(
            capture,
            checked_at,
            stop,
            underlying_price_binding.valid_until,
        )
        contracts = self._catalog(capture, checked_at)
        selection = OptionActivitySelectionRecord(
            authorization_reference=package["authorization_reference"],
            reviewed_at=capture.selection.reviewed_at,
            valid_until=capture.selection.valid_until,
            policy_sha256=capture.selection.policy.sha256,
            source_sha256=capture.selection.source_sha256,
            quote_ceiling=4,
            contracts=contracts,
        )
        oi_sha256 = _open_interest_sha256(
            capture.selection.selected
        )
        reference = build_advanced_options_prospective_reference_identity(
            reference_generation=1,
            session_date=checked_at.date(),
            known_at=capture.source_snapshot_at,
            valid_until=stop + timedelta(minutes=30),
            underlyings=tuple(package["cohort"]["underlyings"]),
            open_interest_sha256=oi_sha256,
        )
        if (
            equity.session_date != checked_at.date()
            or equity_input.equity_cutoff > cycle_cutoff
            or (
                cycle_cutoff - equity_input.equity_cutoff
            ).total_seconds() > equity_input.maximum_age_seconds
        ):
            raise RuntimeError(
                "P8 completed Equity input is stale for Options"
            )
        hashes = package["policy_hashes"]
        policy = _p8_manifest_policy()
        slots = tuple(
            SlotSetMember(
                row.instrument_id,
                channel,
                SlotSetPurpose.PRIMARY,
                100,
                row.catalog_version_id,
            )
            for row in contracts
            for channel in (MarketChannel.TRADE, MarketChannel.QUOTE)
        )
        receipt = AdvancedOptionsReceiptInputs(
            cutoff=cycle_cutoff,
            reference_state=reference,
            equity_input_receipt_id=equity_input.equity_receipt_id,
            equity_input_sha256=equity_input.input_sha256,
            discovery_batches_sha256=capture.evidence["source_snapshot_sha256"],
            open_interest_sha256=oi_sha256,
            consumer_checkpoints_sha256=canonical_sha256((package["package_sha256"], "checkpoints")),
            unresolved_gaps_sha256=canonical_sha256([]),
            market_state_sha256=canonical_sha256((package["package_sha256"], "market-state")),
            source_clocks_sha256=selection.source_clocks_sha256,
            detector_outcomes_sha256=canonical_sha256((package["package_sha256"], "detectors")),
            projection_sha256=canonical_sha256((package["package_sha256"], "projection")),
            build_id=BUILD_ID,
        )
        ingress = package["websocket_ingress"]
        control = build_advanced_options_control_plane_plan(
            equity_plan=equity,
            entitlement=ProductEntitlementProfile(
                ProviderTier.MASSIVE_ADVANCED,
                ProviderTier.MASSIVE_ADVANCED,
            ),
            option_underlyings=tuple(package["cohort"]["underlyings"]),
            options_entitlement_evidence_sha256=canonical_sha256({
                "package_sha256": package["package_sha256"],
                "product": ingress["product"],
            }),
            options_authorization_reference=package["authorization_reference"],
            checked_at=checked_at,
            expires_at=stop + timedelta(minutes=30),
            planned_at=checked_at,
            manifest_policy=policy,
            slot_members=slots,
            slot_effective_at=checked_at,
            feed_scope=AdvancedOptionsFeedScope(),
            pins=AdvancedOptionsPolicyPins(
                condition_semantics_sha256=hashes["condition_semantics_sha256"],
                flow_policy_sha256=hashes["flow_policy_sha256"],
                valuation_policy_sha256=hashes["valuation_policy_sha256"],
                model_bundle_sha256=canonical_sha256((package["package_sha256"], "model")),
                catalog_sha256=canonical_sha256(tuple(
                    row.catalog_payload_sha256 for row in contracts
                )),
                slot_set_policy_sha256=hashes["cohort_template_sha256"],
            ),
            socket_budget=AdvancedOptionsSocketBudget(
                stream_sessions=int(ingress["stream_sessions"]),
                connection_attempts=int(ingress["connection_attempts"]),
                reconnects=int(ingress["reconnects"]),
                decoded_messages=int(ingress["decoded_messages"]),
                decoded_bytes=int(ingress["decoded_bytes"]),
            ),
            provider_budget=ProviderBudgetScope(
                account_alias="MASSIVE_ADVANCED",
                session_date=checked_at.date(),
                authorization_reference=package["authorization_reference"],
                provider_authorization_reference=package["authorization_reference"],
                expected_cost="exact two-request P8 activity selection",
                request_ceiling=2,
                decoded_byte_ceiling=100_000_000,
                lanes=ADVANCED_OPTIONS_REST_LANES,
                authorized_until=stop,
            ),
            owners=AdvancedOptionsOwnerNames(),
            journal_generation=uuid5(
                NAMESPACE_URL,
                f"p8-journal:{package['package_nonce']}",
            ),
            stream_session_ids=(uuid5(
                NAMESPACE_URL,
                f"p8-stream:{package['package_nonce']}",
            ),),
            receipt_inputs=receipt,
            equity_input_cutoff=equity_input.equity_cutoff,
            authorized_cohort_id=UUID(str(package["cohort"]["cohort_id"])),
            authorized_plan_id=UUID(str(package["orchestration"]["plan_id"])),
        )
        enrollment = build_advanced_options_enrollment_plan(
            control_plane=control,
            equity_plan=equity,
            activity_selection=selection,
        )
        history = build_advanced_options_history_binding(
            target_cohort_id=control.cohort_id,
            source_cohort_id=UUID(
                str(package["history_binding"]["source_cohort_id"])
            ),
            history_before=datetime.fromisoformat(
                str(package["history_binding"]["history_before_utc"])
            ),
            source_manifest_sha256=history_manifest,
            authorization_reference=package["authorization_reference"],
            evidence_sha256=(
                None
                if package.get("v3_history_binding") is None
                else package["v3_history_binding"]["evidence"][
                    "evidence_sha256"
                ]
            ),
        )
        window_id = (
            uuid5(
                NAMESPACE_URL,
                f"p8-window:{package['package_nonce']}",
            )
            if target_window_id is None
            else target_window_id
        )
        if not isinstance(window_id, UUID) or not window_id.int:
            raise ValueError("P8 target window identity is invalid")
        if equity_input.target_options_window_id != window_id:
            raise RuntimeError("P8 target Options window binding changed")
        self.underlying_price_binding = underlying_price_binding
        binding = P8CanaryExecutionBinding(
            enrollment,
            history,
            equity_input,
            window_id,
            underlying_price_binding,
        )
        self.production_binding_payload = _production_binding_payload(
            package=package,
            binding=binding,
            equity=equity,
            control=control,
            reference=reference,
            receipt_inputs=receipt,
            selection=selection,
        )
        self.cycle_cutoff = cycle_cutoff
        rebuilt = p8_production_binding_from_payload(
            self.production_binding_payload
        )
        if (
            rebuilt.enrollment != binding.enrollment
            or rebuilt.history != binding.history
            or rebuilt.equity_input != binding.equity_input
            or rebuilt.window_id != binding.window_id
            or rebuilt.underlying_price_binding
            != binding.underlying_price_binding
        ):
            raise RuntimeError(
                "P8 production binding did not round-trip exactly"
            )
        return binding

    def _capture_selection(
        self,
        package,
        checked_at,
        capture,
        *,
        equity_input_binding=None,
        target_window_id=None,
    ):
        if package is not self._package and dict(package) != dict(self._package):
            raise ValueError("P8 production package identity changed")
        checked = checked_at.astimezone(UTC)
        history_manifest = self._history_manifest()
        equity, equity_input = self._equity_source(
            checked,
            equity_input_binding,
        )
        if not isinstance(capture, P8ActivitySelectionCapture):
            raise TypeError("P8 activity selection capture is invalid")
        self.selection_capture = capture
        binding = self._binding(
            capture,
            checked,
            history_manifest,
            equity,
            equity_input,
            target_window_id=target_window_id,
        )
        captured_at = binding.enrollment.control_plane.planned_at
        (
            peers,
            releases,
            pointers,
            windows,
            retention,
            read_pins,
        ) = self._live_state(captured_at, capture)
        control = binding.enrollment.control_plane
        observation = {
            "schema": LIVE_OBSERVATION_SCHEMA,
            "checked_at_utc": captured_at.isoformat(),
            "current_source_commit": package["source"]["commit"],
            "source_files": package["source"]["files"],
            "policy_hashes": package["policy_hashes"],
            "orchestration": package["orchestration"],
            "selection": dict(capture.evidence),
            "advanced_enrollment": {
                "enrollment_id": str(binding.enrollment.enrollment_id),
                "enrollment_sha256": binding.enrollment.enrollment_sha256,
                "control_plane_plan_id": str(control.plan_id),
                "control_plane_plan_sha256": control.plan_sha256,
                "cohort_id": str(control.cohort_id),
                "cohort_sha256": control.cohort_sha256,
                "profile_id": str(control.profile_id),
                "profile_sha256": control.profile_sha256,
                "manifest_id": str(control.manifest_id),
                "manifest_sha256": control.manifest_sha256,
                "slot_set_sha256": control.slot_set_sha256,
                "selected_contracts": [
                    row.contract_ticker
                    for row in binding.enrollment.activity_selection.contracts
                ],
            },
            "history_source": {
                "cohort_id": package["history_binding"]["source_cohort_id"],
                "provider_tier": "MASSIVE_DEVELOPER",
                "manifest_sha256": history_manifest,
                "immutable": True,
                "detector_ready": False,
                "repaired_sessions": package["history_binding"][
                    "required_sessions"
                ],
                "coverage_requirements": {
                    "underlyings": package["history_binding"][
                        "required_underlyings"
                    ],
                    "adjusted_daily_sessions": package[
                        "history_binding"
                    ]["required_adjusted_daily_sessions"],
                    "raw_daily_sessions": package[
                        "history_binding"
                    ]["required_raw_daily_sessions"],
                },
            },
            "equity_input_source": (
                advanced_options_equity_input_binding_payload(
                    binding.equity_input
                )
            ),
            "equity_input_readiness": dict(
                self.equity_input_readiness or {}
            ),
            "underlying_price_binding": (
                underlying_snapshot_binding_payload(
                    binding.underlying_price_binding
                )
            ),
            "equity_windows": windows,
            "running_peer_owners": peers,
            "retention_dependencies": retention,
            "existing_options_releases": releases,
            "existing_options_pointers": pointers,
            "stored_read_flags": {
                "backend": bool(read_pins),
                "frontend": bool(read_pins),
            },
            "owner_authorization": {
                "authorization_reference": package[
                    "authorization_reference"
                ],
                "package_sha256": package["package_sha256"],
                "granted_at_utc": captured_at.isoformat(),
                "granted_authorities": list(PREPARED_AUTHORITIES),
                "stored_reads": self._stored_reads_authorized,
                "dispatch_delivery": False,
                "broker_trading": False,
            },
        }
        self.binding = binding
        self.checked_at = captured_at
        return observation, binding

    def capture(self, package, *, checked_at):
        if self._selection is None:
            raise RuntimeError(
                "P8 provider selection is disabled for this capture"
            )
        checked = checked_at.astimezone(UTC)
        capture = self._selection.capture(
            reviewed_at=checked,
            session_date=checked.date(),
        )
        return self._capture_selection(
            package,
            checked,
            capture,
        )

    def capture_from_selection(
        self,
        package,
        *,
        checked_at,
        selection_capture,
        equity_input_binding=None,
        target_window_id=None,
    ):
        return self._capture_selection(
            package,
            checked_at,
            selection_capture,
            equity_input_binding=equity_input_binding,
            target_window_id=target_window_id,
        )

    def persist_selection_cursor(self, cursor, binding):
        capture = self.selection_capture
        if capture is None or self.checked_at is None:
            raise RuntimeError("P8 selection persistence lacks live capture")
        control = binding.enrollment.control_plane
        security_ids = dict(binding.enrollment.equity_plan.security_ids)
        selected = {
            row.contract_ticker: row
            for row in capture.selection.selected
        }
        contracts = {
            row.contract_ticker: row
            for row in binding.enrollment.activity_selection.contracts
        }
        for underlyer in control.universe.option_underlyings:
            candidate = next(
                row for row in capture.selection.selected
                if row.underlyer == underlyer
            )
            batch_id = uuid5(
                NAMESPACE_URL,
                (
                    "p8-discovery:"
                    f"{self._package['package_nonce']}:"
                    f"{binding.window_id}:{underlyer}"
                ),
            )
            lower = candidate.spot * Decimal("0.95")
            upper = candidate.spot * Decimal("1.05")
            evidence_sha256 = canonical_sha256({
                "selection_sha256": capture.evidence["selection_sha256"],
                "underlyer": underlyer,
                "snapshot_sha256": candidate.source_sha256,
            })
            cursor.execute(
                "INSERT INTO rt.option_discovery_batches ("
                "discovery_batch_id,underlying_security_id,profile_id,"
                "profile_sha256,universe_sha256,provider_tier,provider,"
                "underlyer,scheduled_cycle,expiration_from,"
                "expiration_through,maximum_dte,spot_price,"
                "strike_corridor_fraction,strike_min,strike_max,"
                "request_filter_sha256,policy_sha256,started_at,"
                "completed_at,page_count,row_count,response_bytes,"
                "evidence_sha256) VALUES ("
                "%s,%s,%s,%s,%s,'MASSIVE_ADVANCED','massive',%s,%s,"
                "%s,%s,2,%s,0.05,%s,%s,%s,%s,%s,%s,1,1,%s,%s) "
                "ON CONFLICT (discovery_batch_id) DO NOTHING",
                (
                    str(batch_id),
                    str(security_ids[underlyer]),
                    str(control.profile_id),
                    control.profile_sha256,
                    control.universe.sha256,
                    underlyer,
                    control.planned_at,
                    control.session_date,
                    control.session_date + timedelta(days=2),
                    candidate.spot,
                    lower,
                    upper,
                    capture.evidence["source_snapshot_sha256"],
                    control.manifest_policy.sha256,
                    capture.source_snapshot_at,
                    self.checked_at,
                    max(1, capture.decoded_bytes // 2),
                    evidence_sha256,
                ),
            )
        for ticker, candidate in selected.items():
            contract = contracts[ticker]
            baseline_id = uuid5(
                NAMESPACE_URL,
                (
                    "p8-oi:"
                    f"{self._package['package_nonce']}:"
                    f"{binding.window_id}:{ticker}"
                ),
            )
            cursor.execute(
                "INSERT INTO ref.oi_baselines ("
                "baseline_id,contract_id,settlement_session,"
                "open_interest,source,date_basis,observed_session,"
                "observed_at,received_at,payload_sha256) VALUES ("
                "%s,%s,%s,%s,'massive',"
                "'PROVIDER_CHAIN_SNAPSHOT_PRIOR_SESSION',%s,%s,%s,%s) "
                "ON CONFLICT (baseline_id) DO NOTHING",
                (
                    str(baseline_id),
                    contract.contract_id,
                    control.session_date - timedelta(days=1),
                    candidate.open_interest,
                    control.session_date,
                    candidate.market_data_time,
                    self.checked_at,
                    candidate.source_sha256,
                ),
            )


class _EnrollmentConnection:
    def __init__(self, connection, capture, package):
        self._connection = connection
        self._capture = capture
        self._package = package

    def cursor(self, *args, **kwargs):
        return self._connection.cursor(*args, **kwargs)

    def rollback(self):
        return self._connection.rollback()

    def commit(self):
        binding = self._capture.binding
        checked_at = self._capture.checked_at
        if binding is None or checked_at is None:
            raise RuntimeError("P8 enrollment commit lacks its live binding")
        control = binding.enrollment.control_plane
        with self._connection.cursor() as cursor:
            self._capture.persist_selection_cursor(cursor, binding)
            cursor.execute(
                "INSERT INTO rt.service_plans ("
                "plan_id,cohort_id,session_date,stage_catalog_version,"
                "enabled_stages,config_sha256,deadline,scope_reference"
                ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (plan_id) DO NOTHING",
                (
                    str(control.plan_id),
                    str(control.cohort_id),
                    control.session_date,
                    STAGE_CATALOG_VERSION_V4,
                    list(OPTIONS_NON_SERVING_STAGE_ORDER),
                    binding.enrollment.enrollment_sha256,
                    datetime.fromisoformat(
                        str(self._package["window"]["stop_utc"])
                    ),
                    self._package["authorization_reference"],
                ),
            )
            cutoff = self._capture.cycle_cutoff
            if cutoff is None:
                raise RuntimeError("P8 canonical cutoff is unavailable")
            cursor.execute(
                "INSERT INTO rt.window_runs ("
                "window_id,plan_id,cohort_id,provider_tier,window_key,"
                "release_at,cutoff,deadline,state"
                ") VALUES (%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,'PENDING') "
                "ON CONFLICT (window_id) DO NOTHING",
                (
                    str(binding.window_id),
                    str(control.plan_id),
                    str(control.cohort_id),
                    f"p8-{self._package['package_nonce']}",
                    cutoff,
                    cutoff,
                    datetime.fromisoformat(
                        str(self._package["window"]["stop_utc"])
                    ),
                ),
            )
            if load_advanced_options_equity_input_binding_cursor(
                cursor,
                binding.window_id,
            ) != binding.equity_input:
                raise RuntimeError(
                    "P8 persisted Equity input binding changed"
                )
        self._connection.commit()


def build_advanced_options_p8_production_components(
    *,
    package: Mapping[str, object],
    repository_root: Path,
    stored_reads_authorized: bool,
    provider_free_selection: bool = False,
):
    from .advanced_options_p8_runtime import P8RuntimeComponents

    app_dsn = _required_environment("REALTIME_V2_APP_DSN")
    api_key = (
        None
        if provider_free_selection
        else _required_environment("POLYGON_API_KEY")
    )
    pool = ThreadedConnectionPool(
        1,
        12,
        dsn=app_dsn,
        options="-c timezone=UTC",
    )
    connection = None
    try:
        provider = V2ConnectionProvider(
            {APP_ROLE: pool},
            V2AccessConfig(
                security_mode="hardened",
                role_prefix=EXPECTED_V2_DATABASE_NAME,
                statement_timeout_ms=300_000,
            ),
        )
        provider.verify()
        capture = _P8ProductionCapture(
            provider,
            package,
            api_key,
            stored_reads_authorized,
        )
        connection = pool.getconn()
        enrollment_connection = _EnrollmentConnection(
            connection,
            capture,
            package,
        )
        orchestrator = _DockerWindowOrchestrator(provider, package)
        role_provider = RoleScopedConnectionProvider(
            {RealtimeDatabasePrincipal.RUNTIME: pool},
            {
                RealtimeDatabaseRole.PUBLICATION: (
                    f"{EXPECTED_V2_DATABASE_NAME}_app"
                ),
            },
        )
        publisher_database = RealtimeProcessDatabase(
            RealtimeProcessDatabasePlan(
                "worker:portal_publisher:p8_canary",
                RealtimeDatabaseRole.PUBLICATION,
                RealtimePoolBudget(0, 1),
            ),
            role_provider,
        )
        publication_config = PortalPublicationConfig(
            cohort_id=UUID(str(package["cohort"]["cohort_id"])),
            provider_tier=ProviderTier.MASSIVE_ADVANCED,
            owner_id=uuid5(
                NAMESPACE_URL,
                f"p8-publisher:{package['package_nonce']}",
            ),
            owner_epoch=1,
            dispatch_authorized=False,
        )
        publication_state = {}

        def publication_batch(binding, stage_result):
            if (
                stage_result.get("status") != "FINISHED"
                or stage_result.get("window_state") != "COMPLETE"
                or capture.checked_at is None
            ):
                raise RuntimeError(
                    "P8 publication requires completed serial stages"
                )
            with provider.transaction(
                APP_ROLE,
                read_only=True,
                dict_cursor=True,
            ) as unit:
                unit.cursor.execute(
                    "SELECT receipt_id::text,cutoff,"
                    "reference_generation,universe_sha256,input_sha256,"
                    "build_id FROM rt.cycle_receipts "
                    "WHERE window_id=%s "
                    "AND receipt_kind='OPTIONS_PORTAL_READY'",
                    (str(binding.window_id),),
                )
                row = unit.cursor.fetchone()
                if row is None:
                    raise RuntimeError(
                        "P8 publication receipt is unavailable"
                    )
                unit.cursor.execute(
                    "SELECT catalog.snapshot_id::text,catalog.cohort_id::text,"
                    "catalog.provider_tier,catalog.snapshot_type,"
                    "catalog.snapshot_key,catalog.source_generation,"
                    "catalog.source_manifest_sha256,hot.source_manifest,"
                    "catalog.payload_sha256,hot.payload,"
                    "catalog.generated_at,catalog.valid_until "
                    "FROM rt.snapshot_catalog AS catalog "
                    "JOIN rt.portal_snapshot_hot AS hot "
                    "ON hot.snapshot_id=catalog.snapshot_id "
                    "AND hot.generated_at=catalog.generated_at "
                    "WHERE catalog.cohort_id=%s "
                    "AND catalog.provider_tier='MASSIVE_ADVANCED' "
                    "AND catalog.snapshot_type=ANY(%s) "
                    "AND catalog.snapshot_key='CURRENT' "
                    "AND catalog.generated_at=%s "
                    "ORDER BY catalog.snapshot_type",
                    (
                        str(binding.enrollment.control_plane.cohort_id),
                        list(OPTIONS_PROJECTION_TYPES)
                        + list(OPTIONS_PAGE_SNAPSHOT_TYPES.values()),
                        row["cutoff"],
                    ),
                )
                snapshot_rows = tuple(unit.cursor.fetchall())
            expected_types = set(OPTIONS_PROJECTION_TYPES) | set(
                OPTIONS_PAGE_SNAPSHOT_TYPES.values()
            )
            if {
                str(value["snapshot_type"]) for value in snapshot_rows
            } != expected_types:
                raise RuntimeError(
                    "P8 canonical projection snapshots are incomplete"
                )
            from .portal_projection import RealtimePortalSnapshot
            from .portal_projection_repository import PortalReleaseReceipt

            snapshots = tuple(
                RealtimePortalSnapshot(
                    snapshot_id=UUID(value["snapshot_id"]),
                    cohort_id=UUID(value["cohort_id"]),
                    provider_tier=ProviderTier(value["provider_tier"]),
                    snapshot_type=str(value["snapshot_type"]),
                    snapshot_key=str(value["snapshot_key"]),
                    source_generation=int(value["source_generation"]),
                    source_manifest_sha256=str(
                        value["source_manifest_sha256"]
                    ),
                    source_manifest=value["source_manifest"],
                    payload_sha256=str(value["payload_sha256"]),
                    payload=value["payload"],
                    generated_at=value["generated_at"],
                    valid_until=value["valid_until"],
                )
                for value in snapshot_rows
            )
            receipt = PortalReleaseReceipt(
                UUID(row["receipt_id"]),
                binding.enrollment.control_plane.cohort_id,
                ProviderTier.MASSIVE_ADVANCED,
                row["cutoff"],
                str(row["universe_sha256"]),
            )
            publication_state["snapshots"] = snapshots
            return PortalPublicationBatch(
                snapshots,
                receipt=receipt,
            )

        def publication_observation(binding, pointer, checked_at):
            snapshots = publication_state.get("snapshots")
            if snapshots is None:
                raise RuntimeError(
                    "P8 publication observation lacks its projection"
                )
            page_snapshots = {
                row.payload["artifact_kind"]: row
                for row in snapshots
                if row.snapshot_type in OPTIONS_PAGE_SNAPSHOT_TYPES.values()
            }
            if set(page_snapshots) != set(OPTIONS_PAGE_SNAPSHOT_TYPES):
                raise RuntimeError(
                    "P8 publication page artifact membership changed"
                )
            scope_sha256s = {
                row.payload["scope_sha256"]
                for row in page_snapshots.values()
            }
            if len(scope_sha256s) != 1:
                raise RuntimeError(
                    "P8 publication page scope changed"
                )
            with provider.transaction(
                APP_ROLE,
                read_only=True,
                dict_cursor=True,
                isolation_level="REPEATABLE READ",
            ) as unit:
                unit.cursor.execute(
                    "SELECT generation.generation_id::text,"
                    "generation.status,generation.generated_at "
                    "FROM rt.screening_current AS current "
                    "JOIN rt.screening_generations AS generation "
                    "USING (generation_id) "
                    "WHERE current.cohort_id=%s",
                    (str(binding.enrollment.control_plane.cohort_id),),
                )
                screening = unit.cursor.fetchone()
                unit.cursor.execute(
                    "SELECT catalog.snapshot_id::text,"
                    "catalog.generated_at,hot.payload "
                    "FROM rt.portal_releases AS release "
                    "JOIN LATERAL jsonb_array_elements("
                    "release.manifest->'snapshots') AS entry(value) ON true "
                    "JOIN rt.snapshot_catalog AS catalog "
                    "ON catalog.snapshot_id="
                    "(entry.value->>'snapshot_id')::uuid "
                    "JOIN rt.portal_snapshot_hot AS hot "
                    "ON hot.snapshot_id=catalog.snapshot_id "
                    "AND hot.generated_at=catalog.generated_at "
                    "WHERE release.release_id=%s "
                    "AND entry.value->>'snapshot_type'='STOCK_ALERTS_V1' "
                    "AND entry.value->>'snapshot_key'='PLATFORM'",
                    (str(pointer.release_id),),
                )
                alerts = unit.cursor.fetchone()
            if screening is None or alerts is None:
                raise RuntimeError(
                    "P8 stock page release evidence is unavailable"
                )
            alert_payload = alerts["payload"]
            screening_id = str(screening["generation_id"])
            if (
                screening["status"] != "COMPLETE"
                or str(alert_payload.get("screening_generation_id"))
                != screening_id
            ):
                raise RuntimeError(
                    "P8 stock page release identity changed"
                )
            valid_until = min(
                datetime.fromisoformat(str(row.payload["valid_until"]))
                for row in page_snapshots.values()
            )
            pages = {
                "FLOW": ["OPTIONS_ACTIVITY", "OPTIONS_FLOW"],
                "SCREENER": ["OPTIONS_SCREENER"],
                "ALERTS": ["OPTIONS_ALERTS"],
            }
            return {
                "schema": PAGE_RELEASE_OBSERVATION_SCHEMA,
                "checked_at_utc": checked_at.astimezone(UTC).isoformat(),
                "cohort_id": str(
                    binding.enrollment.control_plane.cohort_id
                ),
                "scope_sha256": next(iter(scope_sha256s)),
                "release_id": str(pointer.release_id),
                "published_at_utc": pointer.cutoff.isoformat(),
                "valid_until_utc": valid_until.isoformat(),
                "artifacts": [
                    {
                        **artifact,
                        "snapshot_id": str(
                            page_snapshots[
                                artifact["artifact_kind"]
                            ].snapshot_id
                        ),
                        "cohort_id": str(
                            binding.enrollment.control_plane.cohort_id
                        ),
                        "release_id": str(pointer.release_id),
                        "scope_sha256": next(iter(scope_sha256s)),
                        "pages": pages[artifact["artifact_kind"]],
                    }
                    for artifact in package["publication"][
                        "page_artifacts"
                    ]
                ],
                "portal_projection_diagnostics": list(
                    OPTIONS_PROJECTION_TYPES
                ),
                "request_time_effects": {
                    "provider_fetches": 0,
                    "detector_runs": 0,
                    "publication_retries": 0,
                    "dispatches": 0,
                    "deliveries": 0,
                    "broker_actions": 0,
                    "trades": 0,
                },
                "stock_publications": {
                    "screening": {
                        "page": "STOCK_SCREENER",
                        "contract": "SCREENING_GENERATION",
                        "generation_id": screening_id,
                        "cohort_id": str(
                            binding.enrollment.control_plane.cohort_id
                        ),
                        "provider_tier": "MASSIVE_ADVANCED",
                        "status": screening["status"],
                        "generated_at_utc": (
                            screening["generated_at"].isoformat()
                        ),
                    },
                    "alerts": {
                        "page": "STOCK_ALERTS",
                        "contract": "STOCK_ALERTS_V1/PLATFORM",
                        "release_id": str(pointer.release_id),
                        "snapshot_id": str(alerts["snapshot_id"]),
                        "screening_generation_id": screening_id,
                        "cohort_id": str(
                            binding.enrollment.control_plane.cohort_id
                        ),
                        "provider_tier": "MASSIVE_ADVANCED",
                        "generated_at_utc": (
                            alerts["generated_at"].isoformat()
                        ),
                    },
                },
            }

        def close():
            if connection is not None:
                pool.putconn(connection)
            pool.closeall()

        return P8RuntimeComponents(
            capture=capture,
            enrollment_connection=enrollment_connection,
            database=provider,
            orchestrator=orchestrator,
            publisher_database=publisher_database,
            publication_config=publication_config,
            publication_batch=publication_batch,
            publication_observation=publication_observation,
            stored_reads=PostgresAdvancedOptionsStoredReadPort(
                provider,
                package_sha256=str(package["package_sha256"]),
            ),
            close=close,
            clock=lambda: datetime.now(UTC),
        )
    except Exception:
        if connection is not None:
            pool.putconn(connection)
        pool.closeall()
        raise
