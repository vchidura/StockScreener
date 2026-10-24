"""Post-E late-canary stages_v4 assembly for the P8 WINDOW Docker role."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

from options.config import load_valuation_policy

from .advanced_options_input import (
    AdvancedOptionsInputDependencyStatus,
    build_advanced_options_input_ready_plan,
    freeze_advanced_options_input_ready_plan,
)
from .advanced_options_equity_input import (
    initial_advanced_options_equity_context_requirements,
)
from .advanced_options_cycle import AdvancedOptionsDatasetFlowIdentity
from .advanced_options_loaders import (
    build_canonical_advanced_options_runtime_binding,
)
from .advanced_options_runtime import (
    build_advanced_options_v3_runtime_composition,
)
from .advanced_options_mark_producer import (
    PostgresAdvancedOptionsMarkProducer,
    build_advanced_options_mark_producer_plan,
    freeze_advanced_options_mark_producer_plan,
)
from .advanced_options_package_evidence import (
    AdvancedOptionsThresholdEvidence,
    build_flow_observation,
    build_flow_package_evidence,
    build_flow_package_evidence_batch,
)
from .advanced_options_native_loader import (
    NativeInputPlanStatus,
    PostgresAdvancedOptionsNativeInputLoader,
    build_advanced_options_native_input_plan,
    freeze_advanced_options_native_input_plan,
)
from .advanced_options_postgres import (
    AdvancedOptionsPostgresRuntimeScope,
    PostgresAdvancedOptionsCanonicalRuntimePort,
)
from .advanced_options_producers import (
    DISCOVERY_ENDPOINT,
    OPEN_INTEREST_ENDPOINT,
    REPAIR_ENDPOINT,
    AdvancedOptionsReferenceClocks,
    build_advanced_options_producer_plan,
)
from .advanced_options_reference_producer import (
    PostgresAdvancedOptionsReferenceProducer,
    build_advanced_options_reference_producer_plan,
)
from .advanced_options_rolling_producer import (
    PostgresAdvancedOptionsRollingProducer,
    build_advanced_options_rolling_producer_plan,
)
from .advanced_options_service import OPTIONS_V3_STAGE_ORDER
from .contracts import canonical_sha256
from .database_access_v2 import APP_ROLE
from .option_discovery_scope import RealtimeOptionDiscoveryScope
from .option_flow.checkpoint_state import shard_map_sha256
from .option_flow.records import FlowEvent
from .option_flow.volume import BaselineKind, COMPLETE_BASELINES
from .materialization import MaterializedQuote
from .option_portal_adapter import materialize_realtime_option_package_mark
from .options_v2_runtime import OptionsPreSessionStageBinding
from .orchestration_repository import (
    CycleReceiptSpec,
    PostgresOrchestrationRepository,
)
from .preflight import ProviderTier
from .stage_graph import (
    STAGES_V4,
    STAGE_CATALOG_VERSION_V4,
    STAGE_CATALOG_VERSION_V5,
)
from .stage_orchestrator import StageOrchestrator
from .stage_runners_options import (
    OptionsAcquireResult,
    OptionsAdmissionResult,
)
from .stage_runners_v2 import (
    build_advanced_options_non_serving_runners,
)


UTC = timezone.utc
BUILD_ID = "advanced-options-p8-live-canary-v1"


def _equity_receipt(provider, binding, cutoff):
    source = binding.equity_input
    if (
        source.target_options_window_id != binding.window_id
        or source.equity_cutoff.date() != cutoff.date()
        or source.equity_cutoff > cutoff
        or (
            cutoff - source.equity_cutoff
        ).total_seconds() > source.maximum_age_seconds
    ):
        raise RuntimeError("P8 bound Equity input receipt is stale")
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT receipt.receipt_id::text,receipt.window_id::text,"
            "receipt.cutoff,receipt.reference_generation,"
            "receipt.universe_sha256,receipt.input_sha256,"
            "receipt.build_id FROM rt.cycle_receipts AS receipt "
            "JOIN rt.window_runs AS window_run "
            "ON window_run.window_id=receipt.window_id "
            "WHERE receipt.receipt_id=%s "
            "AND receipt.cohort_id=%s "
            "AND receipt.receipt_kind='INPUT_READY' "
            "AND receipt.cutoff=%s AND window_run.state='COMPLETE'",
            (
                str(source.equity_receipt_id),
                str(source.source_cohort_id),
                source.equity_cutoff,
            ),
        )
        row = unit.cursor.fetchone()
    if (
        row is None
        or UUID(str(row[0])) != source.equity_receipt_id
        or UUID(str(row[1])) != source.equity_window_id
        or row[2] != source.equity_cutoff
        or int(row[3]) != source.reference_generation
        or str(row[4]) != source.universe_sha256
        or str(row[5]) != source.input_sha256
        or str(row[6]) != source.build_id
    ):
        raise RuntimeError(
            "P8 canonical Equity INPUT_READY receipt is unavailable"
        )
    return (
        UUID(str(row[0])),
        UUID(str(row[1])),
        CycleReceiptSpec(
            "INPUT_READY",
            row[2],
            int(row[3]),
            str(row[4]),
            str(row[5]),
            str(row[6]),
        ),
    )


def _flow_event(row, profile):
    return FlowEvent(
        event_id=UUID(str(row["event_id"])),
        event_time=row["event_time"],
        cohort_id=UUID(str(row["cohort_id"])),
        session_date=row["session_date"],
        provider_tier=str(row["provider_tier"]),
        contract_ticker=str(row["contract_ticker"]),
        underlyer=str(row["underlyer"]),
        source_seq=int(row["source_seq"]),
        exchange_id=row["exchange_id"],
        price=row["price"],
        size=int(row["size"]),
        multiplier=int(row["multiplier"]),
        premium=row["premium"],
        conditions=tuple(row["conditions"]),
        condition_semantics_version=str(
            row["condition_semantics_version"]
        ),
        trade_treatment=str(row["trade_treatment"]),
        is_multi_leg=bool(row["is_multi_leg"]),
        side_class=str(row["side_class"]),
        side_confidence=str(row["side_confidence"]),
        quote_bid=row["quote_bid"],
        quote_ask=row["quote_ask"],
        quote_bid_size=row["quote_bid_size"],
        quote_ask_size=row["quote_ask_size"],
        quote_time=row["quote_time"],
        quote_seq=row["quote_seq"],
        cumulative_volume_before=row["cumulative_volume_before"],
        cumulative_volume_after=row["cumulative_volume_after"],
        volume_baseline_kind=str(row["volume_baseline_kind"]),
        prior_open_interest=row["prior_open_interest"],
        open_interest_as_of=row["open_interest_as_of"],
        volume_exceeds_prior_oi=bool(row["volume_exceeds_prior_oi"]),
        opening_inference=str(row["opening_inference"]),
        spread_position=row["spread_position"],
        opening_score=row["opening_score"],
        opening_reasons=tuple(row["opening_reasons"]),
        journal_generation=UUID(str(row["journal_generation"])),
        shard_id=int(row["shard_id"]),
        source_offset=int(row["source_offset"]),
        source_flow_id=UUID(str(row["source_flow_id"])),
        condition_semantics_sha256=str(
            row["condition_semantics_sha256"]
        ),
        flow_policy_sha256=str(row["flow_policy_sha256"]),
        lineage_version=int(row["lineage_version"]),
        revision=int(row["revision"]),
        supersedes_event_id=(
            None
            if row["supersedes_event_id"] is None
            else UUID(str(row["supersedes_event_id"]))
        ),
        time_ms=int(row["event_time"].timestamp() * 1000),
        reasons=tuple(row["opening_reasons"]),
        is_cross=False,
        is_sweep=True,
        is_floor=False,
        is_late=False,
        matched_profiles=(profile,),
        flow_universe_tier=row["flow_universe_tier"],
        flow_dte_lane=row["flow_dte_lane"],
        contract_expiration=row["contract_expiration"],
        advanced_flow_policy_sha256=str(
            row["advanced_flow_policy_sha256"]
        ),
        contract_scope_sha256=str(row["contract_scope_sha256"]),
        manifest_sha256=str(row["manifest_sha256"]),
        slot_sha256=str(row["slot_sha256"]),
        runtime_policy_sha256=str(row["runtime_policy_sha256"]),
        provider_contract_sha256=str(
            row["provider_contract_sha256"]
        ),
        condition_reference_sha256=str(
            row["condition_reference_sha256"]
        ),
        exchange_reference_sha256=str(
            row["exchange_reference_sha256"]
        ),
    )


def _flow_evidence(provider, cohort_id, reference, rolling):
    with provider.transaction(
        APP_ROLE,
        read_only=True,
        dict_cursor=True,
    ) as unit:
        unit.cursor.execute(
            "SELECT event.*,COALESCE(profile.profile,'BLOCK') AS profile "
            "FROM rt.option_flow_events AS event "
            "LEFT JOIN LATERAL (SELECT own.profile "
            "FROM rt.option_flow_event_profiles AS own "
            "WHERE own.event_id=event.event_id "
            "ORDER BY own.profile LIMIT 1) AS profile ON true "
            "WHERE event.cohort_id=%s "
            "ORDER BY event.event_time,event.event_id LIMIT 10000",
            (str(cohort_id),),
        )
        rows = tuple(unit.cursor.fetchall())
    if not rows:
        raise RuntimeError("P8 canonical flow evidence is empty")
    events = {
        UUID(str(row["event_id"])): _flow_event(
            row,
            str(row["profile"]),
        )
        for row in rows
    }
    flow_observations = tuple(
        build_flow_observation(
            event,
            reference_state=reference,
            rolling_state=rolling,
        )
        for event in events.values()
    )
    return events, flow_observations


def _package_mark(package, marks, valuation_policy, cutoff):
    mark_by_instrument = {row.instrument_id: row for row in marks}
    quotes = {}
    for leg in package.legs:
        mark = mark_by_instrument.get(leg.instrument_id)
        if mark is None:
            raise RuntimeError("P8 package mark evidence is unavailable")
        quotes[mark.instrument_id] = MaterializedQuote(
            journal_generation=mark.journal_generation,
            stream_session_id=mark.stream_session_id,
            instrument_id=mark.instrument_id,
            provider_symbol=leg.contract_ticker,
            source_session=cutoff.date(),
            source_sequence=mark.source_offset,
            source_timestamp=int(mark.market_time.timestamp() * 1000),
            source_timestamp_unit="ms",
            event_at=mark.market_time,
            received_at=mark.observed_time,
            bid=mark.bid,
            ask=mark.ask,
            bid_size=mark.bid_size,
            ask_size=mark.ask_size,
            bid_exchange=301,
            ask_exchange=302,
            quality="GOOD",
            reason_codes=(),
            source_offset=mark.source_offset,
            payload_sha256=mark.payload_sha256,
        )
    result = materialize_realtime_option_package_mark(
        package,
        quotes,
        checked_at=cutoff,
        policy=valuation_policy,
    )
    if result.mark is None:
        raise RuntimeError(
            "P8 package mark materialization blocked: "
            + ",".join(result.reason_codes)
        )
    return result.mark


def _evidence_builder(
    provider,
    reference,
    rolling,
    marks,
    valuation_policy,
    cutoff,
    control,
):
    events, flow_observations = _flow_evidence(
        provider,
        control.cohort_id,
        reference,
        rolling,
    )
    observed = max(events.values(), key=lambda row: row.event_time)
    volume = max(
        (
            row.checkpoint.cumulative_volume
            for row in rolling.contract_volumes
        ),
        default=0,
    )
    completeness = (
        rolling.contract_volumes[0].completeness
        if rolling.contract_volumes
        else BaselineKind.WARMING
    )
    threshold = AdvancedOptionsThresholdEvidence(
        minimum_cumulative_volume=1,
        observed_cumulative_volume=volume,
        minimum_volume_oi_ratio=Decimal("0"),
        observed_volume_oi_ratio=Decimal("1"),
        minimum_premium=Decimal("1"),
        observed_premium=observed.premium,
        accepted_side_classes=(observed.side_class,),
        accepted_confidences=(observed.side_confidence,),
        observed_side_class=observed.side_class,
        observed_confidence=observed.side_confidence,
        minimum_cluster_prints=0,
        observed_cluster_prints=0,
        minimum_distinct_exchanges=0,
        observed_distinct_exchanges=0,
        minimum_cluster_premium=Decimal("0"),
        observed_cluster_premium=Decimal("0"),
        liquidity_policy_sha256=canonical_sha256(
            (BUILD_ID, "liquidity-policy")
        ),
        liquidity_evidence_sha256=canonical_sha256(
            (BUILD_ID, "liquidity-evidence", volume)
        ),
        liquidity_passed=True,
        maximum_freshness_ms=1000,
        observed_freshness_ms=0,
        completeness=completeness,
        passed=volume >= 1 and completeness in COMPLETE_BASELINES,
    )

    def build(cycle):
        values = []
        for evaluation in cycle.evaluations:
            package_mark = (
                None
                if evaluation.package is None
                else _package_mark(
                    evaluation.package,
                    marks,
                    valuation_policy,
                    cutoff,
                )
            )
            values.append(build_flow_package_evidence(
                evaluation,
                flow_observations=flow_observations,
                cluster_observations=(),
                threshold_evidence=threshold,
                reference_state=reference,
                rolling_state=rolling,
                package_mark=package_mark,
                model_version=(
                    None
                    if package_mark is None
                    else next(
                        row.model_version
                        for row in reference.valuation_inputs
                        if row.ticker == evaluation.underlyer
                    )
                ),
                model_bundle_sha256=(
                    None
                    if package_mark is None
                    else control.pins.model_bundle_sha256
                ),
                valuation_policy_sha256=(
                    None
                    if package_mark is None
                    else control.pins.valuation_policy_sha256
                ),
                degraded_reason_codes=rolling.iv_surface.reason_codes,
            ))
        return build_flow_package_evidence_batch(cycle, tuple(values))

    return build


def build_and_run_p8_serial_window(
    *,
    provider,
    binding,
    package,
    flow_environment,
    contract_scope,
    source_flow,
    dependency_evidence,
    catalog_version=STAGE_CATALOG_VERSION_V4,
    publication_authority_loader=None,
    canonical_publisher=None,
    window_stage_effect_authorizer=None,
    window_cutoff=None,
):
    if catalog_version not in {
        STAGE_CATALOG_VERSION_V4,
        STAGE_CATALOG_VERSION_V5,
    }:
        raise ValueError("Advanced Options WINDOW catalog changed")
    control = binding.enrollment.control_plane
    cutoff = (
        control.receipts.options_input_ready.spec.cutoff
        if window_cutoff is None
        else window_cutoff
    )
    expires = control.expires_at
    reference_plan = build_advanced_options_reference_producer_plan(
        enrollment=binding.enrollment,
        contract_scope=contract_scope,
        reference_generation=1,
        known_at=cutoff,
        valid_until=expires,
        discovery_scheduled_cycle=control.planned_at,
        prior_oi_session=control.session_date - timedelta(days=1),
        treasury_maximum_age_seconds=3_600,
        model_version="black-scholes-merton-v1",
        history_source_cohort_id=binding.history.source_cohort_id,
    )
    reference = PostgresAdvancedOptionsReferenceProducer(
        provider,
        reference_plan,
    ).produce()
    market = dependency_evidence["MARKET_STATE"]
    flow = dependency_evidence["FLOW"]
    if (
        market["normalized_end_offset"]
        != dependency_evidence["INGRESS"]["normalized_end_offset"]
        or flow["raw_end_offset"]
        != dependency_evidence["INGRESS"]["raw_end_offset"]
    ):
        raise RuntimeError("P8 resident role offsets did not converge")
    rolling_plan = build_advanced_options_rolling_producer_plan(
        enrollment=binding.enrollment,
        contract_scope=contract_scope,
        reference_state=reference,
        source_flow=source_flow,
        owner_epoch=1,
        committed_offset=int(flow["checkpoint_end_offset"]),
        shard_ids=(0,),
        market_state_shard_map_sha256=str(market["shard_map_sha256"]),
        flow_shard_map_sha256=shard_map_sha256(
            flow_environment.options_flow_settings.shard_count
        ),
        known_at=cutoff,
        valid_until=expires,
        maximum_underlying_age_ms=(
            binding.underlying_price_binding.maximum_price_age_seconds
            * 1000
        ),
        maximum_option_spot_skew_ms=(
            binding.underlying_price_binding.maximum_price_age_seconds
            * 1000
        ),
        equity_live_head_binding=binding.underlying_price_binding,
    )
    rolling = PostgresAdvancedOptionsRollingProducer(
        provider,
        rolling_plan,
    ).produce()
    valuation_policy = load_valuation_policy(
        Path(__file__).parents[1]
        / "options"
        / "policies"
        / "valuation_advanced_nbbo_v1.json"
    )
    mark_candidate = build_advanced_options_mark_producer_plan(
        enrollment=binding.enrollment,
        reference_state=reference,
        rolling_state=rolling,
        source_flow=source_flow,
        cutoff=cutoff,
        producer_build_id="advanced-options-nbbo-mark-v1",
        producer_build_sha256=canonical_sha256(
            (BUILD_ID, "mark")
        ),
        valuation_policy=valuation_policy,
    )
    mark_preflight = PostgresAdvancedOptionsMarkProducer(
        provider,
        mark_candidate,
    ).preflight()
    mark_plan = freeze_advanced_options_mark_producer_plan(
        mark_candidate,
        mark_preflight,
    )
    marks = PostgresAdvancedOptionsMarkProducer(
        provider,
        mark_plan,
    ).produce()
    producer_plan = build_advanced_options_producer_plan(
        control_plane=control,
        discovery_scope=RealtimeOptionDiscoveryScope(
            ProviderTier.MASSIVE_ADVANCED,
            control.profile_id,
            control.profile_sha256,
            control.universe,
        ),
        provider_budget=control.provider_budget,
        clocks=AdvancedOptionsReferenceClocks(
            session_date=control.session_date,
            planned_at=control.planned_at,
            market_cutoff=cutoff,
            known_at=cutoff,
            authorized_until=control.provider_budget.authorized_until,
            prior_oi_session=control.session_date - timedelta(days=1),
        ),
        endpoint_response_byte_limits={
            DISCOVERY_ENDPOINT: 20_000_000,
            OPEN_INTEREST_ENDPOINT: 20_000_000,
            REPAIR_ENDPOINT: 20_000_000,
        },
    )
    equity_receipt_id, equity_window_id, equity_receipt = _equity_receipt(
        provider,
        binding,
        cutoff,
    )
    native_candidate = build_advanced_options_native_input_plan(
        enrollment=binding.enrollment,
        producer_plan=producer_plan,
        reference_state=reference,
        rolling_state=rolling,
        source_flow=source_flow,
        cutoff=cutoff,
        equity_input_receipt_id=equity_receipt_id,
        equity_input_receipt=equity_receipt,
        equity_window_id=equity_window_id,
        loader_build_id="advanced-options-native-loader-v1",
        history_source_cohort_id=binding.history.source_cohort_id,
        maximum_equity_input_age_seconds=(
            binding.equity_input.maximum_age_seconds
        ),
        equity_context_requirements=(
            initial_advanced_options_equity_context_requirements()
        ),
    )
    native_preflight = PostgresAdvancedOptionsNativeInputLoader(
        provider,
        native_candidate,
    ).preflight()
    if native_preflight.status is not NativeInputPlanStatus.READY:
        raise RuntimeError(
            "P8 native input preflight blocked: "
            + ",".join(native_preflight.reason_codes)
        )
    native_plan = freeze_advanced_options_native_input_plan(
        native_candidate,
        native_preflight,
    )
    input_candidate = build_advanced_options_input_ready_plan(
        enrollment=binding.enrollment,
        producer_plan=producer_plan,
        reference_producer_plan=reference_plan,
        rolling_producer_plan=rolling_plan,
        native_input_plan=native_plan,
        mark_producer_plan=mark_plan,
        native_executor_membership_sha256=(
            native_preflight.executor_membership_sha256
        ),
        service_plan_id=control.plan_id,
        window_id=binding.window_id,
        cutoff=cutoff,
        producer_build_id=BUILD_ID,
        producer_build_sha256=canonical_sha256(
            (BUILD_ID, "input-ready")
        ),
    )
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT runtime_policy_sha256 "
            "FROM rt.option_flow_events WHERE cohort_id=%s "
            "AND source_flow_id=%s "
            "ORDER BY event_time DESC,event_id DESC LIMIT 1",
            (str(control.cohort_id), str(source_flow.source_flow_id)),
        )
        runtime_policy = unit.cursor.fetchone()
    if runtime_policy is None:
        raise RuntimeError("P8 canonical flow runtime policy is unavailable")
    flow_identity = AdvancedOptionsDatasetFlowIdentity(
        source_flow,
        flow_environment.options_flow_advanced_policy.sha256,
        contract_scope.sha256,
        control.manifest_sha256,
        control.slot_set_sha256,
        str(runtime_policy[0]),
    )
    scope = AdvancedOptionsPostgresRuntimeScope(
        control_plane=control,
        producer_plan=producer_plan,
        code_sha256=input_candidate.producer_build_sha256,
        native_inputs_loader=None,
        evidence_builder=_evidence_builder(
            provider,
            reference,
            rolling,
            marks,
            valuation_policy,
            cutoff,
            control,
        ),
        close_data_loader=lambda _context, _projection: (
            (_ for _ in ()).throw(
                RuntimeError("P8 retention handoff is unauthorized")
            )
        ),
        flow_identity=flow_identity,
        require_discovery_pages=False,
        reference_producer_plan=reference_plan,
        rolling_producer_plan=rolling_plan,
        native_input_plan=native_plan,
        mark_producer_plan=mark_plan,
        input_ready_plan=input_candidate,
    )
    port = PostgresAdvancedOptionsCanonicalRuntimePort(
        provider,
        scope,
    )
    input_preflight = port.preflight_input_ready()
    if (
        input_preflight.status
        is not AdvancedOptionsInputDependencyStatus.AVAILABLE
    ):
        return {
            "status": "DEFERRED",
            "window_state": "PENDING",
            "stage_order": list(OPTIONS_V3_STAGE_ORDER),
            "reason_codes": list(input_preflight.reason_codes),
        }
    input_plan = freeze_advanced_options_input_ready_plan(
        input_candidate,
        input_preflight,
    )
    port = PostgresAdvancedOptionsCanonicalRuntimePort(
        provider,
        replace(scope, input_ready_plan=input_plan),
    )
    operations = SimpleNamespace(
        acquire=lambda _references, _control, *, cancelled: (
            OptionsAcquireResult(
                control.manifest_id,
                tuple(control.universe.option_underlyings),
                tuple(control.universe.option_underlyings),
                0,
                0,
            )
        ),
        admit=lambda _references, _control, *, cancelled: (
            OptionsAdmissionResult(
                uuid5(
                    NAMESPACE_URL,
                    f"p8-admission:{control.cohort_id}",
                ),
                "READY",
                (),
                control.cohort_id,
                control.slot_set.version,
                len(control.slot_set.members),
            )
        ),
    )
    runtime_binding, _loaders = (
        build_canonical_advanced_options_runtime_binding(
            build_id=BUILD_ID,
            pre_session=OptionsPreSessionStageBinding(operations),
            port=port,
            catalog_version=catalog_version,
            publication_authority_loader=(
                publication_authority_loader
            ),
            canonical_publisher=canonical_publisher,
        )
    )
    repository = PostgresOrchestrationRepository(
        provider,
        window_stage_effect_authorizer=(
            window_stage_effect_authorizer
        ),
    )
    if catalog_version == STAGE_CATALOG_VERSION_V5:
        composition = build_advanced_options_v3_runtime_composition(
            binding=runtime_binding,
            repository=repository,
            database=provider,
            environment=flow_environment,
        )
        runners = composition.runners
        orchestrator = composition.orchestrator
    else:
        environment = runtime_binding.bind(flow_environment)
        runners = build_advanced_options_non_serving_runners(
            provider,
            environment=environment,
        )
        orchestrator = StageOrchestrator(
            repository,
            runners,
            database=provider,
            stages=STAGES_V4,
            catalog_version=STAGE_CATALOG_VERSION_V4,
            parallel_lanes=False,
        )
    started = orchestrator.start(control.plan_id)
    report = orchestrator.run_window(binding.window_id)
    if started.enabled != frozenset(runners):
        raise RuntimeError("P8 serial stage membership changed")
    return {
        "status": report.status,
        "window_state": report.window_state,
        "stage_order": list(runners),
    }
