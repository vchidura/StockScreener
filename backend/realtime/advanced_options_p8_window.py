"""Post-E late-canary stages_v4 assembly for the P8 WINDOW Docker role."""
from __future__ import annotations

import json
import sys
import logging
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

from options.config import load_valuation_policy
from options.calendar import OptionExchangeCalendar

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
    PACKAGE_MARK_UNAVAILABLE,
    AdvancedOptionsThresholdEvidence,
    build_flow_observation,
    build_flow_package_evidence,
    build_flow_package_evidence_batch,
)
from .advanced_options_native_loader import (
    AdvancedOptionsNativePreflightBlocked,
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
    _engine_config,
)
from .stage_runners_v2 import (
    build_advanced_options_non_serving_runners,
)


UTC = timezone.utc
_LOGGER = logging.getLogger(__name__)


def _resident_owner_epoch(flow: Mapping[str, object]) -> int:
    owner_epoch = flow.get("owner_epoch")
    if type(owner_epoch) is not int or owner_epoch <= 0:
        raise RuntimeError("P8 flow resident owner epoch changed")
    return owner_epoch


def _journal_owner_epoch(flow_environment) -> int:
    try:
        owner_epoch = (
            flow_environment.options_flow_source.registration.owner_epoch
        )
    except AttributeError as error:
        raise RuntimeError("P8 journal owner epoch changed") from error
    if type(owner_epoch) is not int or owner_epoch <= 0:
        raise RuntimeError("P8 journal owner epoch changed")
    return owner_epoch


def _resident_shard_expectations(
    market: Mapping[str, object],
    flow: Mapping[str, object],
    *,
    flow_shard_count: int,
) -> tuple[tuple[int, ...], tuple[int, ...], str, str]:
    values = []
    for kind, evidence in (
        ("market-state", market),
        ("flow", flow),
    ):
        shard_ids = evidence.get("shard_ids")
        shard_map = evidence.get("shard_map_sha256")
        if (
            not isinstance(shard_ids, list)
            or not shard_ids
            or any(type(value) is not int for value in shard_ids)
            or tuple(shard_ids) != tuple(range(len(shard_ids)))
            or not isinstance(shard_map, str)
            or len(shard_map) != 64
            or any(
                character not in "0123456789abcdef"
                for character in shard_map
            )
        ):
            raise RuntimeError(
                f"P8 {kind} resident shard evidence changed"
            )
        values.append((tuple(shard_ids), shard_map))
    market_value, flow_value = values
    expected_flow_shard_ids = tuple(range(flow_shard_count))
    if flow_value[0] != expected_flow_shard_ids:
        raise RuntimeError("P8 flow resident shard layout changed")
    return (
        market_value[0],
        flow_value[0],
        market_value[1],
        flow_value[1],
    )
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
            # Consumers can pass this window's barrier once it ends, so read only up
            # to the flow checkpoint it holds. Newest first: at the limit, the
            # oldest prints are the ones to lose.
            "WHERE event.cohort_id=%s AND event.source_offset<=%s "
            "ORDER BY event.event_time DESC,event.event_id DESC LIMIT 10000",
            (str(cohort_id), rolling.committed_offset),
        )
        rows = tuple(reversed(unit.cursor.fetchall()))
    # One event the evidence contract refuses must not stop the whole Options window.
    events = {}
    flow_observations = []
    rejected = {}
    for row in rows:
        try:
            event = _flow_event(row, str(row["profile"]))
            observation = build_flow_observation(
                event,
                reference_state=reference,
                rolling_state=rolling,
            )
        except ValueError as error:
            reason = str(error).splitlines()[-1][:120] if str(error) else type(error).__name__
            rejected[reason] = rejected.get(reason, 0) + 1
            continue
        events[UUID(str(row["event_id"]))] = event
        flow_observations.append(observation)
    if rejected:
        print(
            json.dumps({"event": "advanced_options_flow_events_rejected", "reasons": rejected}),
            file=sys.stderr,
            flush=True,
        )
    return events, tuple(flow_observations)


def _flow_runtime_policy_sha256(
    provider,
    flow_environment,
    control,
    source_flow,
):
    """A session with no qualifying print yet has no event to read the policy
    from. That is a normal window, and the engine's own policy is the same value."""
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT runtime_policy_sha256 "
            "FROM rt.option_flow_events WHERE cohort_id=%s "
            "AND source_flow_id=%s "
            "ORDER BY event_time DESC,event_id DESC LIMIT 1",
            (str(control.cohort_id), str(source_flow.source_flow_id)),
        )
        stored = unit.cursor.fetchone()
    if stored is not None:
        return str(stored[0])
    return _engine_config(
        flow_environment,
        flow_environment.options_flow_settings,
        control.cohort_id,
        source_flow.journal_generation,
        source_flow=source_flow,
    ).policy_sha256()


def _package_mark(package, marks, valuation_policy, cutoff):
    """The package's mark at the cutoff, or None when it cannot be marked (Nov 10: a package that cannot be marked,
    for a missing, stale or refused quote, is shown with an UNAVAILABLE mark instead of failing the whole window)."""
    mark_by_instrument = {row.instrument_id: row for row in marks}
    quotes = {}
    for leg in package.legs:
        mark = mark_by_instrument.get(leg.instrument_id)
        if mark is None:
            _LOGGER.warning(
                "Options package %s not marked at %s: no mark for instrument %s",
                package.candidate_id, cutoff.isoformat(), leg.instrument_id,
            )
            return None
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
            # A quote from before the cutoff can arrive just after it.
            received_at=min(mark.observed_time, cutoff),
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
        _LOGGER.warning(
            "Options package %s not marked at %s: %s",
            package.candidate_id, cutoff.isoformat(), ",".join(result.reason_codes),
        )
        return None
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
    observed = (
        None
        if not events
        else max(events.values(), key=lambda row: row.event_time)
    )
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
        observed_volume_oi_ratio=(
            None if observed is None else Decimal("1")
        ),
        minimum_premium=Decimal("1"),
        observed_premium=(
            Decimal("0") if observed is None else observed.premium
        ),
        accepted_side_classes=(
            () if observed is None else (observed.side_class,)
        ),
        accepted_confidences=(
            () if observed is None else (observed.side_confidence,)
        ),
        observed_side_class=(
            "NO_QUALIFYING_FLOW_EVENT"
            if observed is None
            else observed.side_class
        ),
        observed_confidence=(
            "NO_QUALIFYING_FLOW_EVENT"
            if observed is None
            else observed.side_confidence
        ),
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
        passed=(
            observed is not None
            and volume >= 1
            and completeness in COMPLETE_BASELINES
        ),
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
            try:
                values.append(evidence(evaluation, package_mark))
            except (ValueError, StopIteration) as error:
                if evaluation.package is None or package_mark is None:
                    raise
                # Nov 10: incomplete leg evidence for one selected package records it unmarked instead of failing
                # every evaluation in the window.
                _LOGGER.warning(
                    "Options package %s evidence incomplete at %s (%s); recorded without a mark",
                    evaluation.package.candidate_id, cutoff.isoformat(), error,
                )
                values.append(evidence(evaluation, None, "PACKAGE_EVIDENCE_INCOMPLETE"))
        return build_flow_package_evidence_batch(cycle, tuple(values))

    def evidence(evaluation, package_mark, *extra_reasons):
        return build_flow_package_evidence(
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
                degraded_reason_codes=(
                    *rolling.iv_surface.reason_codes,
                    *rolling.degraded_reason_codes,
                    *(
                        (PACKAGE_MARK_UNAVAILABLE,)
                        if evaluation.package is not None and package_mark is None
                        else ()
                    ),
                    *extra_reasons,
                ),
            )

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
    reference_generation = 1
    if control.stored_identity is not None:
        with provider.transaction(APP_ROLE, read_only=True) as unit:
            unit.cursor.execute(
                "SELECT COALESCE(MAX(reference_generation),0)+1 "
                "FROM rt.advanced_options_reference_states"
            )
            reference_generation = int(unit.cursor.fetchone()[0])
    prior_session = OptionExchangeCalendar("XNYS").previous_session_date(
        control.session_date
    )
    reference_plan = build_advanced_options_reference_producer_plan(
        enrollment=binding.enrollment,
        contract_scope=contract_scope,
        reference_generation=reference_generation,
        known_at=cutoff,
        valid_until=expires,
        discovery_scheduled_cycle=control.planned_at,
        prior_oi_session=prior_session,
        model_version="black-scholes-merton-v1",
        history_source_cohort_id=binding.history.source_cohort_id,
    )
    reference = PostgresAdvancedOptionsReferenceProducer(
        provider,
        reference_plan,
    ).produce()
    market = dependency_evidence["MARKET_STATE"]
    flow = dependency_evidence["FLOW"]
    owner_epoch = _journal_owner_epoch(flow_environment)
    if (
        market["normalized_end_offset"]
        != dependency_evidence["INGRESS"]["normalized_end_offset"]
        or flow["raw_end_offset"]
        != dependency_evidence["INGRESS"]["raw_end_offset"]
        or flow["checkpoint_end_offset"]
        != dependency_evidence["INGRESS"]["raw_end_offset"]
        or flow["normalized_end_offset"]
        != market["normalized_end_offset"]
    ):
        raise RuntimeError("P8 resident role offsets did not converge")
    (
        market_state_shard_ids,
        flow_shard_ids,
        market_state_shard_map_sha256,
        flow_shard_map_sha256,
    ) = _resident_shard_expectations(
        market,
        flow,
        flow_shard_count=(
            flow_environment.options_flow_settings.shard_count
        ),
    )
    rolling_plan = build_advanced_options_rolling_producer_plan(
        enrollment=binding.enrollment,
        contract_scope=contract_scope,
        reference_state=reference,
        source_flow=source_flow,
        owner_epoch=owner_epoch,
        market_state_committed_offset=int(
            market["normalized_end_offset"]
        ),
        flow_committed_offset=int(flow["checkpoint_end_offset"]),
        market_state_shard_ids=market_state_shard_ids,
        flow_shard_ids=flow_shard_ids,
        market_state_shard_map_sha256=(
            market_state_shard_map_sha256
        ),
        flow_shard_map_sha256=flow_shard_map_sha256,
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
        market_state_committed_offset=int(market["normalized_end_offset"]),
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
    marked = {row.instrument_id for row in marks}
    unmarked_instrument_ids = tuple(sorted(
        row.instrument_id
        for row in binding.enrollment.activity_selection.contracts
        if row.instrument_id not in marked
    ))
    if rolling.degraded_reason_codes:
        _LOGGER.warning(
            "advanced_options_window_degraded window_id=%s reasons=%s "
            "excluded_contracts=%d first_excluded=%s",
            binding.window_id,
            ",".join(rolling.degraded_reason_codes),
            len(rolling.excluded_contracts),
            ",".join(row[0] for row in rolling.excluded_contracts[:20]),
        )
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
            prior_oi_session=prior_session,
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
        unmarked_instrument_ids=unmarked_instrument_ids,
    )
    native_preflight = PostgresAdvancedOptionsNativeInputLoader(
        provider,
        native_candidate,
    ).preflight()
    if native_preflight.status is not NativeInputPlanStatus.READY:
        raise AdvancedOptionsNativePreflightBlocked(native_preflight)
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
    flow_identity = AdvancedOptionsDatasetFlowIdentity(
        source_flow,
        flow_environment.options_flow_advanced_policy.sha256,
        contract_scope.sha256,
        control.manifest_sha256,
        control.slot_set_sha256,
        _flow_runtime_policy_sha256(
            provider,
            flow_environment,
            control,
            source_flow,
        ),
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
    # OPTIONS_ACQUIRE needs REFERENCE ready for this cohort, and no resident role
    # reports it. This attempt has just built and verified its reference state.
    repository.upsert_readiness(
        control.cohort_id,
        "REFERENCE",
        catalog_version,
        "READY",
        (),
        f"advanced-options-reference-state:{reference.state_id}",
        datetime.now(UTC),
    )
    started = orchestrator.start(control.plan_id)
    report = orchestrator.run_window(binding.window_id)
    if started.enabled != frozenset(runners):
        raise RuntimeError("P8 serial stage membership changed")
    return {
        "status": report.status,
        "window_state": report.window_state,
        "stage_order": list(runners),
        "executed": [list(row) for row in report.executed],
        "lane_errors": dict(report.lane_errors),
    }
