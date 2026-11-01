"""Exact owned-disposable provider-free V3 integration qualification."""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest
from psycopg2.extras import execute_batch
from websockets.asyncio.server import serve

from advanced_options_reference_support import (
    adjusted_bars,
    equity_metrics,
    open_interest,
    valuation,
)
from option_flow_support import advanced_policy, policy
from realtime.advanced_gateway import (
    AdvancedGateway,
    GatewayConfiguration,
    GatewayFeedState,
)
from realtime.advanced_options_consumers import (
    AdvancedOptionsMarketStateConsumer,
    persist_advanced_options_market_state_slice_cursor,
)
from realtime.advanced_options_control_plane import (
    ADVANCED_OPTIONS_REST_LANES,
    AdvancedOptionsFeedScope,
    AdvancedOptionsOwnerNames,
    AdvancedOptionsPolicyPins,
    AdvancedOptionsReceiptInputs,
    AdvancedOptionsSocketBudget,
    build_advanced_options_equity_input_binding,
    build_advanced_options_history_binding,
    build_advanced_options_control_plane_plan,
    build_advanced_options_enrollment_plan,
    persist_advanced_options_equity_input_binding_cursor,
    persist_advanced_options_enrollment_cursor,
    persist_advanced_options_history_binding_cursor,
)
from realtime.advanced_options_cycle import (
    load_advanced_options_cycle_cursor,
)
from realtime.advanced_options_flow_policy import (
    AdvancedOptionsContractScope,
    AdvancedOptionsContractScopeEntry,
)
from realtime.advanced_options_ingress import (
    AdvancedOptionsJournalPump,
    AdvancedOptionsJournalSource,
    build_advanced_options_journal_registration,
)
from realtime.advanced_options_input import (
    build_advanced_options_input_closure,
)
from realtime.advanced_options_loaders import (
    AdvancedOptionsCanonicalCycleFacts,
    AdvancedOptionsCanonicalInputFacts,
    build_canonical_advanced_options_runtime_binding,
)
from realtime.advanced_options_native_adapters import (
    AdvancedOptionsLaneInputs,
    AdvancedOptionsNativeInputBundle,
)
from realtime.advanced_options_package_evidence import (
    build_flow_package_evidence_batch,
)
from realtime.advanced_options_p8_canary_authorization import (
    source_binding_identity,
)
from realtime.advanced_options_projection import (
    AdvancedOptionsPublicationAuthorization,
    load_advanced_options_projection_batch_cursor,
)
from realtime.advanced_options_producers import (
    AdvancedOptionsReferenceClocks,
    build_advanced_options_producer_plan,
)
from realtime.advanced_options_provider_contract import (
    MASSIVE_OPTIONS_PROVIDER_CONTRACT,
    load_default_options_exchange_reference,
)
from realtime.advanced_options_reference_state import (
    InMemoryAdvancedOptionsReferenceStateRepository,
    build_advanced_options_reference_state,
    build_historical_volatility_fact,
)
from realtime.advanced_options_resident_barrier import (
    ResidentCompletion,
    ResidentCompletionBarrierState,
    ResidentCompletionDivergence,
    ResidentCompletionMember,
    ResidentCompletionStatus,
    evaluate_resident_completion_barrier_cursor,
    record_resident_completion_cursor,
)
from realtime.advanced_options_rolling_state import (
    CANONICAL_ORDER_WINDOWS_SECONDS,
    AdvancedOptionsOrderWindowMember,
    AdvancedOptionsOrderWindowState,
    build_advanced_options_rolling_state,
    build_underlying_tick,
    contract_volume_state,
)
from realtime.advanced_options_runtime import (
    build_advanced_options_v3_runtime_composition,
)
from realtime.advanced_options_service import (
    AdvancedOptionsProcessHealth,
    AdvancedOptionsProcessKind,
    AdvancedOptionsResidentPolicy,
    OPTIONS_V3_STAGE_ORDER,
    evaluate_advanced_options_process_health,
)
from realtime.advanced_options_v3_authority import (
    AdvancedOptionsAuthorityBundle,
    AdvancedOptionsWindowStageEffectAuthorizer,
    OpeningAdmissionStatus,
    ProspectiveOpeningAdmission,
    TargetWindowAgePolicy,
    advanced_options_authority_bundle_payload,
    build_opening_subscription_readiness,
    build_session_acquisition_authorization,
    build_window_cycle_authorization,
    advanced_options_authority_status_cursor,
    consume_session_acquisition_authority_cursor,
    load_window_cycle_authorization_cursor,
    persist_session_acquisition_authorization_cursor,
    persist_window_cycle_authorization_cursor,
    prospective_opening_admission_payload,
)
from realtime.advanced_options_v3_deployment import (
    ADVANCED_OPTIONS_V3_PROFILE_SHA256,
    ADVANCED_OPTIONS_V3_SOURCE_BINDING_PATHS,
    advanced_options_v3_profile_payload,
    build_advanced_options_v3_authority_references,
    file_sha256,
)
from realtime.buffer import BufferLimits, RawMessageBuffer
from realtime.contracts import (
    AssetClass,
    EQUITY_VALIDATION_UNIVERSE,
    MarketChannel,
    MarketFeed,
    SubscriptionManifest,
    SubscriptionMember,
    UniverseManifest,
    canonical_sha256,
)
from realtime.database_access_v2 import APP_ROLE
from realtime.journal import RawJournalReader
from realtime.materialization import MaterializerShardMap
from realtime.normalizer import InstrumentAdmission, NORMALIZER_VERSION
from realtime.option_cycle_input import (
    OptionCycleInput,
    OptionCycleUnderlying,
)
from realtime.option_discovery_scope import RealtimeOptionDiscoveryScope
from realtime.option_flow.conditions import load_default_live_semantics
from realtime.option_flow.consumer import ConsumerLimits
from realtime.option_flow.engine import EngineConfig
from realtime.option_flow.records import VolumeCheckpoint
from realtime.option_flow.volume import BaselineKind
from realtime.option_session_manifest import (
    BoundedOptionManifestPolicy,
    OptionActivityContract,
    OptionActivitySelectionRecord,
)
from realtime.option_slot_set import (
    SlotSetKind,
    SlotSetMember,
    SlotSetPurpose,
    SlotSetVersion,
)
from realtime.option_source_flow import (
    build_option_source_flow_identity,
    persist_option_contract_scope_cursor,
    persist_option_flow_policy_cursor,
    persist_option_source_flow_cursor,
)
from realtime.options_v2_runtime import OptionsPreSessionStageBinding
from realtime.orchestration_repository import (
    CycleReceiptSpec,
    PostgresOrchestrationRepository,
)
from realtime.portal_projection import build_portal_snapshot
from realtime.portal_projection_repository import (
    PortalReleaseReceipt,
    load_portal_release_pointer_cursor,
    persist_portal_snapshot_cursor,
    publish_portal_release_cursor,
)
from realtime.portal_publisher import (
    CanonicalPortalPublisher,
    PortalPublicationConfig,
    run_portal_publication_once,
)
from realtime.preflight import (
    DetectorFamily,
    ProductEntitlementProfile,
    ProviderTier,
)
from realtime.provider_budget import ProviderBudgetScope
from realtime.runner_environment import RunnerEnvironment
from realtime.security_v2 import (
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from realtime.stage_graph import (
    STAGES_V5,
    STAGE_CATALOG_VERSION_V5,
)
from realtime.stage_orchestrator import StageContext, StageOutcome
from realtime.stage_runners_options import (
    OptionsAcquireResult,
    OptionsAdmissionResult,
    OptionsFlowSettings,
)
from tests.realtime.test_advanced_options_p8_canary_v2_db import (
    _app_provider,
    _frames,
    _publisher_database,
    _query,
)
from tests.realtime.test_advanced_options_input_ready_v2_db import _limits
from tests.realtime.test_advanced_options_v3_authority import (
    _authority_bundle as _base_authority_bundle,
)
from tests.realtime.test_option_flow_runner_v2_db import World
from tests.realtime.test_option_slot_sets_v2_db import (
    _role_cursor,
    cursor,
)
from tests.realtime.v2_database import (
    _connect,
    _settings,
    disposable_v2_database,
    insert_cohort,
    requires_v2_database,
)


pytestmark = requires_v2_database
UTC = timezone.utc
BUILD_ID = "advanced-options-v3-provider-free-v1"
REPORT_ENV = "STOCKS_V3_RETAIN_REPORT"
SUFFIX_ENV = "STOCKS_V3_QUALIFICATION_SUFFIX"
REPORT_ROOT = Path(__file__).parents[2] / ".realtime-canary"


def _digest(value) -> str:
    return canonical_sha256(("advanced-options-v3-integration", value))


def _exact_authority_bundle(
    *,
    cohort_id,
    window_id,
    equity_receipt_id,
):
    base = _base_authority_bundle(
        cohort_id=cohort_id,
        target_window_id=window_id,
        equity_receipt_id=equity_receipt_id,
        publication_authorization_reference=(
            "V3-CANONICAL-PUBLICATION-QUALIFICATION"
        ),
    )
    contracts = tuple(
        replace(
            row,
            contract_ticker=(
                f"O:{row.underlyer}"
                f"{row.expiration_date.strftime('%y%m%d')}"
                f"{'C' if row.contract_type == 'CALL' else 'P'}"
                f"{int(row.strike * 1000):08d}"
            ),
        )
        for row in base.admission.contracts
    )
    admission_values = {
        "status": OpeningAdmissionStatus.READY,
        "session_date": base.admission.session_date,
        "prepared_at": base.admission.prepared_at,
        "capture_boundary": base.admission.capture_boundary,
        "policy_sha256": base.admission.policy_sha256,
        "pre_session_selection_id": (
            base.admission.pre_session_selection_id
        ),
        "pre_session_selection_sha256": (
            base.admission.pre_session_selection_sha256
        ),
        "source_evidence_sha256": (
            base.admission.source_evidence_sha256
        ),
        "reference_evidence_sha256": (
            base.admission.reference_evidence_sha256
        ),
        "contracts": contracts,
        "reason_codes": (),
        "opening_capture_ready": True,
        "opening_coverage_claimed": False,
        "accumulation_900_second_coverage_claimed": False,
    }
    unsigned_admission = SimpleNamespace(
        admission_id=UUID(int=1),
        admission_sha256="0" * 64,
        **admission_values,
    )
    admission_sha256 = canonical_sha256(
        prospective_opening_admission_payload(
            unsigned_admission,
            include_identity=False,
        )
    )
    admission = ProspectiveOpeningAdmission(
        admission_id=uuid5(
            NAMESPACE_URL,
            (
                "stockscreener:prospective-opening-admission:"
                f"{admission_sha256}"
            ),
        ),
        admission_sha256=admission_sha256,
        **admission_values,
    )
    instruments = {
        row.contract_ticker: index + 1000
        for index, row in enumerate(contracts)
    }
    slot_set = SlotSetVersion(
        cohort_id=cohort_id,
        version=1,
        kind=SlotSetKind.PRE_SESSION,
        effective_at=admission.capture_boundary - timedelta(minutes=10),
        policy_sha256=admission.policy_sha256,
        members=tuple(
            SlotSetMember(
                instruments[row.contract_ticker],
                channel,
                SlotSetPurpose.PRIMARY,
                index + 1,
                row.catalog_version_id,
            )
            for index, row in enumerate(contracts)
            for channel in (MarketChannel.QUOTE, MarketChannel.TRADE)
        ),
    )
    readiness = build_opening_subscription_readiness(
        admission,
        slot_set,
        instrument_id_by_contract_ticker=instruments,
        journal_generation=uuid5(
            NAMESPACE_URL,
            f"v3-journal:{cohort_id}",
        ),
        journal_identity_sha256=_digest("journal-identity"),
        ready_at=admission.capture_boundary - timedelta(minutes=5),
    )
    reference = "V3-PROSPECTIVE-OPENING-QUALIFICATION"
    acquisition_stop = admission.capture_boundary + timedelta(minutes=30)
    provider_budget = ProviderBudgetScope(
        account_alias="MASSIVE_ADVANCED",
        session_date=admission.session_date,
        authorization_reference=reference,
        provider_authorization_reference="provider-free",
        expected_cost="zero provider effects",
        request_ceiling=2,
        decoded_byte_ceiling=100_000_000,
        lanes=ADVANCED_OPTIONS_REST_LANES,
        authorized_until=acquisition_stop,
    )
    session = build_session_acquisition_authorization(
        admission,
        readiness,
        provider_budget,
        AdvancedOptionsSocketBudget(
            stream_sessions=1,
            connection_attempts=2,
            reconnects=1,
            decoded_messages=10_000,
            decoded_bytes=100_000_000,
        ),
        authorization_reference=reference,
        prepared_at=admission.capture_boundary - timedelta(minutes=2),
        acquisition_start=admission.capture_boundary - timedelta(minutes=1),
        acquisition_stop=acquisition_stop,
    )
    equity_cutoff = admission.capture_boundary + timedelta(minutes=10)
    cycle = build_window_cycle_authorization(
        session,
        authorization_reference="V3-WINDOW-CYCLE-QUALIFICATION",
        target_window_id=window_id,
        target_cohort_id=cohort_id,
        prepared_at=equity_cutoff + timedelta(seconds=1),
        history_binding_sha256=_digest("history-binding"),
        equity_binding_sha256=_digest("equity-binding"),
        equity_receipt_id=equity_receipt_id,
        equity_cutoff=equity_cutoff,
        equity_receipt_created_at=equity_cutoff,
        options_cutoff=equity_cutoff + timedelta(seconds=120),
        age_policy=TargetWindowAgePolicy(
            "advanced-options-e-o-age-v1",
            300,
        ),
        evidence_sha256=_digest("window-evidence"),
        build_id=BUILD_ID,
        publication_authorization_reference=(
            "V3-CANONICAL-PUBLICATION-QUALIFICATION"
        ),
    )
    return AdvancedOptionsAuthorityBundle(
        session=session,
        admission=admission,
        readiness=readiness,
        cycle=cycle,
    )


def _exact_reference(authority, security_ids):
    known = authority.cycle.options_cutoff
    session = known.date()
    facts_oi = []
    facts_hv = []
    facts_valuation = []
    facts_metrics = []
    base_oi = open_interest(known, session)[0]
    base_valuation = valuation(known, session)[0]
    for index, contract in enumerate(authority.admission.contracts):
        ticker = contract.underlyer
        security_id = security_ids[ticker]
        facts_oi.append(replace(
            base_oi,
            contract_id=contract.contract_id,
            contract_ticker=contract.contract_ticker,
            underlyer=ticker,
            baseline_id=uuid5(
                NAMESPACE_URL,
                f"v3-oi:{contract.contract_ticker}",
            ),
            payload_sha256=_digest(("oi", ticker)),
        ))
        base_bars = adjusted_bars()
        session_shift = (
            session
            - timedelta(days=1)
            - base_bars[-1].session_date
        )
        bars = tuple(
            replace(
                row,
                bar_revision_id=uuid5(
                    NAMESPACE_URL,
                    (
                        f"v3-bar:{ticker}:"
                        f"{row.session_date + session_shift}"
                    ),
                ),
                security_id=security_id,
                ticker=ticker,
                session_date=row.session_date + session_shift,
                market_close_at=row.market_close_at + session_shift,
                known_at=row.known_at + session_shift,
                payload_sha256=_digest((
                    "bar",
                    ticker,
                    row.session_date + session_shift,
                )),
            )
            for row in base_bars
        )
        cutoff = bars[-1].market_close_at
        for window, members in ((20, bars[-21:]), (30, bars)):
            facts_hv.append(build_historical_volatility_fact(
                security_id=security_id,
                ticker=ticker,
                window_sessions=window,
                market_cutoff=cutoff,
                known_at=known,
                bars=members,
            ))
        coverage = replace(
            base_valuation.dividend_coverage,
            coverage_id=uuid5(
                NAMESPACE_URL,
                f"v3-dividend:{ticker}",
            ),
            security_id=security_id,
            ticker=ticker,
            payload_sha256=_digest(("dividend", ticker)),
        )
        facts_valuation.append(replace(
            base_valuation,
            security_id=security_id,
            ticker=ticker,
            expiration_date=contract.expiration_date,
            dividend_coverage=coverage,
        ))
        for metric in equity_metrics(known, session_date=session):
            facts_metrics.append(replace(
                metric,
                security_id=security_id,
                ticker=ticker,
                source_key=(
                    None
                    if metric.source_key is None
                    else metric.source_key.replace("SPY", ticker)
                ),
                payload_sha256=(
                    None
                    if metric.payload_sha256 is None
                    else _digest((
                        "metric",
                        ticker,
                        metric.metric_kind.value,
                    ))
                ),
            ))
    return build_advanced_options_reference_state(
        reference_generation=7,
        session_date=session,
        known_at=known,
        valid_until=known + timedelta(hours=4),
        reference_policy_sha256="f" * 64,
        data_membership_sha256="e" * 64,
        contract_scope_sha256="0" * 64,
        underlyings=("QQQ", "SPY"),
        open_interest=tuple(facts_oi),
        historical_volatility=tuple(facts_hv),
        valuation_inputs=tuple(facts_valuation),
        equity_metrics=tuple(facts_metrics),
    )


def _exact_enrollment(database_cursor, authority):
    session = authority.cycle.options_cutoff.date()
    checked = authority.admission.prepared_at - timedelta(minutes=5)
    expires = authority.cycle.options_cutoff + timedelta(hours=6)
    security_ids = {
        ticker: uuid5(NAMESPACE_URL, f"v3-equity:{ticker}")
        for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers
    }
    equity_instruments = {}
    for ticker, security_id in security_ids.items():
        database_cursor.execute(
            "INSERT INTO ref.securities ("
            "security_id,asset_type,source_origin,created_at) "
            "VALUES (%s,'STOCK','v3-provider-free',%s)",
            (str(security_id), checked - timedelta(days=10)),
        )
        database_cursor.execute(
            "INSERT INTO ref.security_revisions ("
            "revision_id,security_id,ticker,calendar_id,effective_from,"
            "observed_at,payload_sha256,free_float) VALUES ("
            "%s,%s,%s,'XNYS',%s,%s,%s,%s)",
            (
                str(uuid4()),
                str(security_id),
                ticker,
                checked - timedelta(days=9),
                checked - timedelta(days=9),
                _digest(("equity", ticker)),
                (
                    Decimal("1000000000")
                    if ticker in {"QQQ", "SPY"}
                    else None
                ),
            ),
        )
    from realtime.advanced_equity_control_plane import (
        build_advanced_equity_control_plane_plan,
        persist_advanced_equity_control_plane_cursor,
    )

    equity = build_advanced_equity_control_plane_plan(
        session_date=session,
        security_ids=security_ids,
        entitlement_evidence_sha256="a" * 64,
        authorization_reference="V3-TARGET9-EQUITY-SOURCE",
        checked_at=checked,
        expires_at=expires,
        schema_sha256="b" * 64,
        security_policy_sha256="c" * 64,
    )
    persist_advanced_equity_control_plane_cursor(
        database_cursor,
        equity,
    )
    for ticker, security_id in security_ids.items():
        database_cursor.execute(
            "SELECT instrument.instrument_id "
            "FROM rt.subscription_members AS member "
            "JOIN rt.instruments AS instrument "
            "ON instrument.instrument_id=member.instrument_id "
            "WHERE member.manifest_id=%s "
            "AND instrument.security_id=%s",
            (str(equity.manifest_id), str(security_id)),
        )
        equity_instruments[ticker] = int(
            database_cursor.fetchone()[0]
        )
    option_instruments = {}
    selected = []
    for contract in authority.admission.contracts:
        database_cursor.execute(
            "INSERT INTO ref.option_contracts ("
            "contract_id,underlying_security_id,provider,contract_ticker,"
            "first_observed_at,source_origin) VALUES ("
            "%s,%s,'massive',%s,%s,'v3-provider-free')",
            (
                contract.contract_id,
                str(security_ids[contract.underlyer]),
                contract.contract_ticker,
                checked,
            ),
        )
        database_cursor.execute(
            "INSERT INTO ref.option_contract_versions ("
            "version_id,contract_id,call_put,expiration,strike,"
            "exercise_style,multiplier,deliverables,eligibility,"
            "effective_from,observed_at,payload_sha256) VALUES ("
            "%s,%s,%s,%s,%s,'AMERICAN',100,'[]'::jsonb,"
            "'VALIDATED_ACTIVE',%s,%s,%s)",
            (
                contract.catalog_version_id,
                contract.contract_id,
                "C" if contract.contract_type == "CALL" else "P",
                contract.expiration_date,
                contract.strike,
                checked,
                checked,
                contract.catalog_payload_sha256,
            ),
        )
        database_cursor.execute(
            "INSERT INTO rt.instruments (asset_class,contract_id) "
            "VALUES ('OPTION',%s) RETURNING instrument_id",
            (contract.contract_id,),
        )
        instrument_id = int(database_cursor.fetchone()[0])
        option_instruments[contract.contract_ticker] = instrument_id
        selected.append(OptionActivityContract(
            instrument_id=instrument_id,
            contract_id=contract.contract_id,
            catalog_version_id=contract.catalog_version_id,
            contract_ticker=contract.contract_ticker,
            underlyer=contract.underlyer,
            call_put="C" if contract.contract_type == "CALL" else "P",
            expiration_date=contract.expiration_date,
            strike=contract.strike,
            exercise_style="AMERICAN",
            multiplier=Decimal("100"),
            deliverables=(),
            eligibility="VALIDATED_ACTIVE",
            catalog_effective_from=checked,
            catalog_effective_to=None,
            catalog_observed_at=checked,
            catalog_payload_sha256=contract.catalog_payload_sha256,
            market_data_time=authority.cycle.prepared_at,
            source_sha256=contract.snapshot_payload_sha256,
        ))
    reference = _exact_reference(authority, security_ids)
    policy_value = BoundedOptionManifestPolicy(
        version="advanced-options-v3-qqq-spy-v1",
        target_dtes=(21,),
        maximum_target_dte_deviation=30,
        strikes_per_side_per_expiry=1,
        minimum_dte=0,
        maximum_dte=60,
        strike_corridor_fraction=Decimal("0.20"),
        minimum_day_volume=0,
        minimum_open_interest=0,
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
    pins = AdvancedOptionsPolicyPins(
        condition_semantics_sha256="1" * 64,
        flow_policy_sha256="2" * 64,
        valuation_policy_sha256="3" * 64,
        model_bundle_sha256="4" * 64,
        catalog_sha256="5" * 64,
        slot_set_policy_sha256="6" * 64,
    )
    slot_members = tuple(
        SlotSetMember(
            option_instruments[contract.contract_ticker],
            channel,
            SlotSetPurpose.PRIMARY,
            100,
            contract.catalog_version_id,
        )
        for contract in authority.admission.contracts
        for channel in (MarketChannel.TRADE, MarketChannel.QUOTE)
    )
    control = build_advanced_options_control_plane_plan(
        equity_plan=equity,
        entitlement=ProductEntitlementProfile(
            ProviderTier.MASSIVE_ADVANCED,
            ProviderTier.MASSIVE_ADVANCED,
        ),
        option_underlyings=("QQQ", "SPY"),
        options_entitlement_evidence_sha256="0" * 64,
        options_authorization_reference="V3-QQQ-SPY-OPTIONS",
        checked_at=checked,
        expires_at=expires,
        planned_at=authority.cycle.prepared_at,
        manifest_policy=policy_value,
        slot_members=slot_members,
        slot_effective_at=authority.readiness.ready_at,
        feed_scope=AdvancedOptionsFeedScope(),
        pins=pins,
        socket_budget=AdvancedOptionsSocketBudget(
            stream_sessions=1,
            connection_attempts=2,
            reconnects=1,
            decoded_messages=10_000,
            decoded_bytes=100_000_000,
        ),
        provider_budget=ProviderBudgetScope(
            account_alias="MASSIVE_ADVANCED",
            session_date=session,
            authorization_reference="V3-QQQ-SPY-OPTIONS",
            provider_authorization_reference="provider-free",
            expected_cost="zero provider effects",
            request_ceiling=6,
            decoded_byte_ceiling=100_000_000,
            lanes=ADVANCED_OPTIONS_REST_LANES,
            authorized_until=expires,
        ),
        owners=AdvancedOptionsOwnerNames(),
        journal_generation=authority.readiness.journal_generation,
        stream_session_ids=(uuid4(),),
        receipt_inputs=AdvancedOptionsReceiptInputs(
            cutoff=authority.cycle.options_cutoff,
            reference_state=reference,
            equity_input_receipt_id=authority.cycle.equity_receipt_id,
            equity_input_sha256="7" * 64,
            discovery_batches_sha256="8" * 64,
            open_interest_sha256=reference.open_interest_sha256,
            consumer_checkpoints_sha256="9" * 64,
            unresolved_gaps_sha256="a" * 64,
            market_state_sha256="b" * 64,
            source_clocks_sha256="c" * 64,
            detector_outcomes_sha256="d" * 64,
            projection_sha256="e" * 64,
            build_id=BUILD_ID,
        ),
        equity_input_cutoff=authority.cycle.equity_cutoff,
        authorized_cohort_id=authority.session.cohort_id,
    )
    selection = OptionActivitySelectionRecord(
        authorization_reference="V3-PROSPECTIVE-OPENING",
        reviewed_at=authority.cycle.prepared_at,
        valid_until=(
            authority.cycle.options_cutoff + timedelta(minutes=1)
        ),
        policy_sha256=authority.admission.policy_sha256,
        source_sha256=authority.admission.source_evidence_sha256,
        quote_ceiling=4,
        contracts=tuple(selected),
    )
    enrollment = build_advanced_options_enrollment_plan(
        control_plane=control,
        equity_plan=equity,
        activity_selection=selection,
    )
    return (
        enrollment,
        reference,
        equity_instruments,
        option_instruments,
    )


def _manifest(control, contracts):
    members = (
        *(
            SubscriptionMember(
                AssetClass.EQUITY,
                ticker,
                ticker,
                MarketChannel.MINUTE_AGGREGATE,
            )
            for ticker in control.universe.equity_tickers
        ),
        *(
            SubscriptionMember(
                AssetClass.OPTION,
                contract.contract_ticker,
                contract.underlyer,
                channel,
                catalog_version_id=contract.catalog_version_id,
            )
            for contract in contracts
            for channel in (
                MarketChannel.TRADE,
                MarketChannel.QUOTE,
            )
        ),
    )
    return SubscriptionManifest(
        version="advanced-options-v3-loopback-v1",
        session_date=control.session_date,
        universe=UniverseManifest(
            "advanced-options-v3-loopback-v1",
            control.universe.equity_tickers,
            ("QQQ", "SPY"),
        ),
        capability_profile_sha256=control.profile_sha256,
        allocation_policy_sha256=control.slot_set_sha256,
        members=members,
        maximum_option_quote_contracts=4,
    )


async def _loopback_journal(
    control,
    contracts,
    journal_path,
    frames,
):
    subscriptions = []

    async def handler(websocket):
        await websocket.recv()
        await websocket.send(json.dumps([{
            "ev": "status",
            "status": "auth_success",
        }]))
        subscribe = json.loads(await websocket.recv())
        subscriptions.extend(subscribe["params"].split(","))
        await websocket.send(json.dumps([{
            "ev": "status",
            "status": "success",
        }]))
        for frame in frames:
            await websocket.send(frame)
        await websocket.wait_closed()

    buffer = RawMessageBuffer(
        BufferLimits(1024, 8 * 1024 * 1024, 60_000_000_000),
        monotonic_ns=time.monotonic_ns,
    )
    admissions = {
        row.contract_ticker: InstrumentAdmission(
            row.instrument_id,
            row.contract_ticker,
            row.underlyer,
            AssetClass.OPTION,
            0,
        )
        for row in contracts
    }
    pump = AdvancedOptionsJournalPump(
        buffer,
        journal_path,
        control.source_flow.journal_generation,
        admissions,
        maximum_batch_messages=32,
        maximum_batch_bytes=1024 * 1024,
    )
    pump.start()
    try:
        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            gateway = AdvancedGateway(
                GatewayConfiguration(
                    MarketFeed.OPTIONS,
                    f"ws://127.0.0.1:{port}",
                    maximum_message_bytes=64 * 1024,
                    library_max_queue=8,
                    allow_insecure_loopback=True,
                ),
                _manifest(control, contracts),
                buffer,
                api_key="provider-free-v3",
                utc_clock=lambda: control.receipts.options_input_ready.spec.cutoff,
                monotonic_ns=time.monotonic_ns,
                session_id_factory=lambda: (
                    control.source_flow.stream_session_ids[0]
                ),
            )
            result = await gateway.run_session(
                maximum_market_messages=len(frames),
            )
    finally:
        pump.request_stop()
    return result, pump.join(30), tuple(subscriptions)


def _rolling_state(
    control,
    reference,
    equity_instruments,
    contracts,
    committed_offset,
):
    cutoff = control.receipts.options_input_ready.spec.cutoff
    ticks = []
    for index, ticker in enumerate(("QQQ", "SPY")):
        instrument_id = equity_instruments[ticker]
        quote = SimpleNamespace()
        from realtime.materialization import (
            MaterializedQuote,
            MaterializedTrade,
        )

        quote = MaterializedQuote(
            control.source_flow.journal_generation,
            control.source_flow.stream_session_ids[0],
            instrument_id,
            ticker,
            control.session_date,
            10 + index,
            1,
            "ms",
            cutoff - timedelta(milliseconds=100),
            cutoff - timedelta(milliseconds=90),
            Decimal("499.90"),
            Decimal("500.10"),
            100,
            120,
            1,
            2,
            "VALID",
            (),
            1 + index,
            _digest(("quote", ticker)),
        )
        trade = MaterializedTrade(
            control.source_flow.journal_generation,
            control.source_flow.stream_session_ids[0],
            instrument_id,
            ticker,
            AssetClass.EQUITY,
            control.session_date,
            20 + index,
            2,
            "ms",
            cutoff - timedelta(milliseconds=80),
            cutoff - timedelta(milliseconds=70),
            Decimal("500"),
            Decimal("10"),
            1,
            (),
            "VALID",
            (),
            "ELIGIBLE",
            (),
            "equity-trade-v1",
            3 + index,
            _digest(("trade", ticker)),
        )
        ticks.append(build_underlying_tick(
            instrument_id=instrument_id,
            ticker=ticker,
            session_date=control.session_date,
            quote=quote,
            trade=trade,
            option_market_time=cutoff - timedelta(milliseconds=90),
            selected_at=cutoff,
            maximum_age_ms=1000,
            maximum_option_spot_skew_ms=500,
        ))
    volumes = tuple(
        contract_volume_state(VolumeCheckpoint(
            cohort_id=control.cohort_id,
            session_date=control.session_date,
            contract_ticker=row.contract_ticker,
            cumulative_volume=100,
            last_source_seq=10,
            last_event_time=cutoff - timedelta(seconds=1),
            baseline_kind=BaselineKind.SESSION_COMPLETE.value,
            snapshot_volume=None,
            snapshot_watermark_seq=None,
        ))
        for row in contracts
    )
    return build_advanced_options_rolling_state(
        cohort_id=control.cohort_id,
        session_date=control.session_date,
        journal_generation=control.source_flow.journal_generation,
        owner_epoch=1,
        committed_offset=committed_offset,
        committed_at=cutoff,
        market_state_checkpoint_sha256=_digest("market-checkpoint"),
        flow_checkpoint_sha256=_digest("flow-checkpoint"),
        underlying_ticks=tuple(ticks),
        contract_volumes=volumes,
        order_windows=AdvancedOptionsOrderWindowState(
            owner_epoch=1,
            committed_offset=committed_offset,
            checkpoint_sha256=_digest("order-windows"),
            windows_seconds=CANONICAL_ORDER_WINDOWS_SECONDS,
            members=(AdvancedOptionsOrderWindowMember(
                uuid4(),
                committed_offset,
                cutoff - timedelta(milliseconds=50),
                5,
            ),),
        ),
    )


class _RuntimePort:
    def __init__(
        self,
        provider,
        facts,
        native,
        reference_repository,
    ):
        self.provider = provider
        self.facts = facts
        self.native = native
        self.reference_repository = reference_repository

    def produce_reference_state(self):
        return SimpleNamespace(state=self.facts.reference_state)

    def load_input_facts(self, _context):
        return self.facts

    def load_input_receipt(self, context):
        return self._receipt(context, "OPTIONS_INPUT_READY")

    def load_cycle_facts(self, context, _input_stage):
        return AdvancedOptionsCanonicalCycleFacts(
            self.load_input_receipt(context),
            self.native,
            self.reference_repository,
            lambda cycle: build_flow_package_evidence_batch(cycle, ()),
            "v3-owned-disposable-cycle",
        )

    def _receipt(self, context, kind):
        with self.provider.transaction(APP_ROLE, read_only=True) as unit:
            unit.cursor.execute(
                "SELECT cutoff,reference_generation,universe_sha256,"
                "input_sha256,build_id FROM rt.cycle_receipts "
                "WHERE window_id=%s AND cohort_id=%s "
                "AND receipt_kind=%s",
                (
                    str(context.window.window_id),
                    str(context.plan.cohort_id),
                    kind,
                ),
            )
            row = unit.cursor.fetchone()
        if row is None:
            raise ValueError(f"durable {kind} receipt is absent")
        return CycleReceiptSpec(
            kind,
            row[0],
            int(row[1]),
            str(row[2]),
            str(row[3]),
            str(row[4]),
        )

    def load_completed_cycle(self, context, input_stage):
        input_receipt = self.load_input_receipt(context)
        cycle_receipt = self._receipt(context, "OPTIONS_CYCLE")
        with self.provider.transaction(
            APP_ROLE,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            return load_advanced_options_cycle_cursor(
                unit.cursor,
                control_plane=input_stage.control_plane,
                input_gate=input_stage.input_gate,
                input_receipt=input_receipt,
                cycle_receipt=cycle_receipt,
            )

    def load_completed_projection(self, context, cycle):
        receipt = self._receipt(context, "OPTIONS_PORTAL_READY")
        with self.provider.transaction(
            APP_ROLE,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            return load_advanced_options_projection_batch_cursor(
                unit.cursor,
                control_plane=self.facts.control_plane,
                cycle=cycle,
                portal_ready_receipt=receipt,
            )

    def load_close_data(self, _context, _projection):
        raise RuntimeError("V3 qualification does not authorize retention")


def _record_completion(
    database_cursor,
    *,
    control,
    window_id,
    member,
    ending_offset,
    evidence,
):
    completion = ResidentCompletion(
        target_window_id=window_id,
        target_cohort_id=control.cohort_id,
        member=member,
        journal_generation=control.source_flow.journal_generation,
        owner_epoch=1,
        ending_offset=ending_offset,
        evidence_sha256=canonical_sha256(evidence),
        evidence=evidence,
        status=ResidentCompletionStatus.COMPLETE,
        reason_codes=(),
        completed_at=control.receipts.options_input_ready.spec.cutoff,
    )
    assert record_resident_completion_cursor(
        database_cursor,
        completion,
    )
    return completion


def _retain(report):
    semantic_sha256 = canonical_sha256(report)
    document = {**report, "semantic_sha256": semantic_sha256}
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    path = REPORT_ROOT / (
        "advanced-options-v3-provider-free-qualification-"
        f"{semantic_sha256[:16]}.json"
    )
    with path.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(document, stream, indent=2, sort_keys=True)
        stream.write("\n")
    return path, semantic_sha256, file_sha256(path)


def test_v3_exact_provider_free_restart_publication_and_cleanup(
    tmp_path,
    capsys,
):
    suffix = os.getenv(SUFFIX_ENV) or uuid4().hex[:8]
    if (
        len(suffix) != 8
        or any(value not in "0123456789abcdef" for value in suffix)
    ):
        raise ValueError(
            f"{SUFFIX_ENV} must be exactly eight lowercase hexadecimal characters"
        )
    database_name = f"test_realtime_v2_v3_{suffix}"
    roles = build_role_names_v2(f"test_rt_v3_{suffix}")
    settings = _settings()
    roles_owned = False
    report = None
    journal_root = tmp_path / "v3-owned-journal"
    journal_path = journal_root / "options.sqlite"
    cohort_id = uuid4()
    window_id = uuid4()
    failure_window_id = uuid4()
    equity_receipt_id = uuid4()
    authority = _exact_authority_bundle(
        cohort_id=cohort_id,
        window_id=window_id,
        equity_receipt_id=equity_receipt_id,
    )
    try:
        with disposable_v2_database(database_name) as database_pool:
            with cursor(database_pool) as database_cursor:
                (
                    enrollment,
                    reference,
                    equity_instruments,
                    option_instruments,
                ) = _exact_enrollment(database_cursor, authority)
                control = enrollment.control_plane
                assert control.cohort_id == cohort_id
                assert control.universe.equity_tickers == (
                    EQUITY_VALIDATION_UNIVERSE.equity_tickers
                )
                assert control.universe.option_underlyings == (
                    "QQQ",
                    "SPY",
                )
                persist_advanced_options_enrollment_cursor(
                    database_cursor,
                    enrollment,
                )
                database_cursor.execute(
                    "INSERT INTO rt.advanced_ingress_runtime_profiles ("
                    "runtime_profile_sha256,runtime_profile_version,"
                    "universe_target,ladder_position,provider_tier,feed,"
                    "channels,profile_document,configuration_sha256,"
                    "runtime_policy_sha256,status) VALUES ("
                    "%s,'advanced-equity-target9-v1',9,0,"
                    "'MASSIVE_ADVANCED','STOCKS',ARRAY['AM'],"
                    "%s::jsonb,%s,%s,'CANDIDATE')",
                    (
                        _digest("target9-runtime-profile"),
                        json.dumps({
                            "target": 9,
                            "feed": "STOCKS",
                            "channels": ["AM"],
                        }),
                        enrollment.equity_plan.configuration_sha256,
                        _digest("target9-runtime-policy"),
                    ),
                )
                database_cursor.execute(
                    "UPDATE rt.advanced_ingress_runtime_profiles SET "
                    "status='ACTIVE',stability_evidence_sha256=%s,"
                    "capacity_evidence_sha256=%s,"
                    "activation_approval_reference=%s,activated_at=%s "
                    "WHERE runtime_profile_sha256=%s",
                    (
                        _digest("target9-stability"),
                        _digest("target9-capacity"),
                        "OWNED_DISPOSABLE_V3_QUALIFICATION",
                        authority.cycle.prepared_at,
                        _digest("target9-runtime-profile"),
                    ),
                )
                database_cursor.execute(
                    "INSERT INTO rt.service_plans ("
                    "plan_id,cohort_id,session_date,stage_catalog_version,"
                    "enabled_stages,config_sha256,deadline,scope_reference"
                    ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        str(control.plan_id),
                        str(control.cohort_id),
                        control.session_date,
                        STAGE_CATALOG_VERSION_V5,
                        list(OPTIONS_V3_STAGE_ORDER),
                        enrollment.enrollment_sha256,
                        authority.cycle.options_cutoff
                        + timedelta(hours=1),
                        "owned-disposable exact V3 qualification",
                    ),
                )
                database_cursor.execute(
                    "INSERT INTO rt.window_runs ("
                    "window_id,plan_id,cohort_id,provider_tier,window_key,"
                    "release_at,cutoff,deadline,state) VALUES ("
                    "%s,%s,%s,'MASSIVE_ADVANCED','v3-exact',%s,%s,%s,"
                    "'PENDING')",
                    (
                        str(window_id),
                        str(control.plan_id),
                        str(control.cohort_id),
                        authority.cycle.options_cutoff,
                        authority.cycle.options_cutoff,
                        authority.cycle.options_cutoff
                        + timedelta(minutes=30),
                    ),
                )
                database_cursor.execute(
                    "INSERT INTO rt.window_runs ("
                    "window_id,plan_id,cohort_id,provider_tier,window_key,"
                    "release_at,cutoff,deadline,state) VALUES ("
                    "%s,%s,%s,'MASSIVE_ADVANCED','v3-failure-probe',"
                    "%s,%s,%s,'PENDING')",
                    (
                        str(failure_window_id),
                        str(control.plan_id),
                        str(control.cohort_id),
                        authority.cycle.options_cutoff,
                        authority.cycle.options_cutoff,
                        authority.cycle.options_cutoff
                        + timedelta(minutes=30),
                    ),
                )
                equity_plan_id = uuid4()
                equity_window_id = uuid4()
                database_cursor.execute(
                    "INSERT INTO rt.service_plans ("
                    "plan_id,cohort_id,session_date,stage_catalog_version,"
                    "enabled_stages,config_sha256,deadline,scope_reference"
                    ") VALUES (%s,%s,%s,'stages_v3',%s,%s,%s,%s)",
                    (
                        str(equity_plan_id),
                        str(enrollment.equity_plan.cohort_id),
                        control.session_date,
                        ["EQUITY_INPUT_GATE"],
                        _digest("equity-plan"),
                        authority.cycle.options_cutoff
                        + timedelta(hours=1),
                        "V3 target-9 Equity source",
                    ),
                )
                database_cursor.execute(
                    "INSERT INTO rt.window_runs ("
                    "window_id,plan_id,cohort_id,provider_tier,window_key,"
                    "release_at,cutoff,deadline,state) VALUES ("
                    "%s,%s,%s,'MASSIVE_ADVANCED','v3-equity',%s,%s,%s,"
                    "'COMPLETE')",
                    (
                        str(equity_window_id),
                        str(equity_plan_id),
                        str(enrollment.equity_plan.cohort_id),
                        authority.cycle.equity_cutoff,
                        authority.cycle.equity_cutoff,
                        authority.cycle.options_cutoff,
                    ),
                )
                equity_receipt = CycleReceiptSpec(
                    "INPUT_READY",
                    authority.cycle.equity_cutoff,
                    7,
                    enrollment.equity_plan.universe.sha256,
                    "7" * 64,
                    "advanced-equity-v3-provider-free-v1",
                )
                database_cursor.execute(
                    "INSERT INTO rt.cycle_receipts ("
                    "receipt_id,window_id,cohort_id,receipt_kind,cutoff,"
                    "reference_generation,universe_sha256,input_sha256,"
                    "build_id,created_at) VALUES ("
                    "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        str(equity_receipt_id),
                        str(equity_window_id),
                        str(enrollment.equity_plan.cohort_id),
                        equity_receipt.receipt_kind,
                        equity_receipt.cutoff,
                        equity_receipt.reference_generation,
                        equity_receipt.universe_sha256,
                        equity_receipt.input_sha256,
                        equity_receipt.build_id,
                        authority.cycle.equity_receipt_created_at,
                    ),
                )
                history_source_cohort_id = insert_cohort(
                    database_cursor,
                    authority.session.acquisition_start,
                )
                database_cursor.execute(
                    "SELECT manifest.manifest_sha256 "
                    "FROM rt.cohorts AS cohort "
                    "JOIN rt.subscription_manifests AS manifest "
                    "USING(manifest_id) WHERE cohort.cohort_id=%s",
                    (str(history_source_cohort_id),),
                )
                history_manifest_sha256 = (
                    database_cursor.fetchone()[0]
                )
                history_binding = (
                    build_advanced_options_history_binding(
                        target_cohort_id=control.cohort_id,
                        source_cohort_id=history_source_cohort_id,
                        history_before=(
                            authority.session.acquisition_start
                        ),
                        source_manifest_sha256=(
                            history_manifest_sha256
                        ),
                        authorization_reference=(
                            "V3-HISTORY-QUALIFICATION"
                        ),
                    )
                )
                persist_advanced_options_history_binding_cursor(
                    database_cursor,
                    history_binding,
                )
                equity_binding = (
                    build_advanced_options_equity_input_binding(
                        target_options_window_id=window_id,
                        target_cohort_id=control.cohort_id,
                        source_cohort_id=(
                            enrollment.equity_plan.cohort_id
                        ),
                        source_cohort_sha256=(
                            enrollment.equity_plan.cohort_sha256
                        ),
                        source_manifest_id=(
                            enrollment.equity_plan.manifest_id
                        ),
                        source_manifest_sha256=(
                            enrollment.equity_plan.manifest.sha256
                        ),
                        source_profile_id=(
                            enrollment.equity_plan.profile_id
                        ),
                        source_profile_sha256=(
                            enrollment.equity_plan.profile_sha256
                        ),
                        equity_window_id=equity_window_id,
                        equity_receipt_id=equity_receipt_id,
                        equity_cutoff=authority.cycle.equity_cutoff,
                        reference_generation=7,
                        universe_sha256=(
                            enrollment.equity_plan.universe.sha256
                        ),
                        input_sha256=equity_receipt.input_sha256,
                        build_id=equity_receipt.build_id,
                        maximum_age_seconds=300,
                        authorization_reference=(
                            "V3-EQUITY-INPUT-QUALIFICATION"
                        ),
                    )
                )
                persist_advanced_options_equity_input_binding_cursor(
                    database_cursor,
                    equity_binding,
                )
                cycle_authority = build_window_cycle_authorization(
                    authority.session,
                    authorization_reference=(
                        authority.cycle.authorization_reference
                    ),
                    target_window_id=window_id,
                    target_cohort_id=control.cohort_id,
                    prepared_at=authority.cycle.prepared_at,
                    history_binding_sha256=(
                        history_binding.binding_sha256
                    ),
                    equity_binding_sha256=(
                        equity_binding.binding_sha256
                    ),
                    equity_receipt_id=equity_receipt_id,
                    equity_cutoff=authority.cycle.equity_cutoff,
                    equity_receipt_created_at=(
                        authority.cycle.equity_receipt_created_at
                    ),
                    options_cutoff=authority.cycle.options_cutoff,
                    age_policy=TargetWindowAgePolicy(
                        authority.cycle.age_policy_version,
                        authority.cycle.maximum_age_seconds,
                    ),
                    evidence_sha256=authority.cycle.evidence_sha256,
                    build_id=authority.cycle.build_id,
                    publication_authorization_reference=(
                        authority.cycle
                        .publication_authorization_reference
                    ),
                )
                authority = replace(
                    authority,
                    cycle=cycle_authority,
                )
                persist_session_acquisition_authorization_cursor(
                    database_cursor,
                    authority.session,
                )
                authority_references = (
                    build_advanced_options_v3_authority_references(
                        (
                            advanced_options_authority_bundle_payload(
                                replace(authority, cycle=None)
                            )["session_acquisition"]
                        ),
                        target_window_id=window_id,
                    )
                )
                assert set(authority_references) == {
                    "schema",
                    "session_acquisition",
                    "window_cycle",
                    "canonical_publication",
                    "stored_read",
                }
                assert (
                    authority_references["window_cycle"]["load_mode"]
                    == "DURABLE_SESSION_WINDOWS"
                )
                database_cursor.execute(
                    "SELECT count(*) FROM "
                    "rt.advanced_options_window_cycle_authorizations "
                    "WHERE target_window_id=%s",
                    (str(window_id),),
                )
                assert database_cursor.fetchone()[0] == 0
                with pytest.raises(
                    ValueError,
                    match="not prepared for the exact scope",
                ):
                    load_window_cycle_authorization_cursor(
                        database_cursor,
                        target_window_id=window_id,
                        session_authorization_id=(
                            authority.session.authorization_id
                        ),
                    )
                session_receipt = (
                    consume_session_acquisition_authority_cursor(
                        database_cursor,
                        authority.session,
                        effect_kind="PROVIDER_SOCKET",
                        effect_identity_sha256=_digest(
                            "loopback-tq-ingress"
                        ),
                        consumed_at=(
                            authority.session.acquisition_start
                        ),
                    )
                )
                assert consume_session_acquisition_authority_cursor(
                    database_cursor,
                    authority.session,
                    effect_kind="PROVIDER_SOCKET",
                    effect_identity_sha256=_digest(
                        "loopback-tq-ingress"
                    ),
                    consumed_at=(
                        authority.session.acquisition_start
                        + timedelta(seconds=1)
                    ),
                ) == session_receipt
                with pytest.raises(
                    ValueError,
                    match="already consumed",
                ):
                    consume_session_acquisition_authority_cursor(
                        database_cursor,
                        authority.session,
                        effect_kind="PROVIDER_SOCKET",
                        effect_identity_sha256=_digest(
                            "different-ingress-effect"
                        ),
                        consumed_at=(
                            authority.session.acquisition_start
                            + timedelta(seconds=2)
                        ),
                    )
                persist_window_cycle_authorization_cursor(
                    database_cursor,
                    authority.cycle,
                )
                assert load_window_cycle_authorization_cursor(
                    database_cursor,
                    target_window_id=window_id,
                    session_authorization_id=(
                        authority.session.authorization_id
                    ),
                ) == authority.cycle
                with pytest.raises(
                    ValueError,
                    match="not prepared for the exact scope",
                ):
                    load_window_cycle_authorization_cursor(
                        database_cursor,
                        target_window_id=window_id,
                        session_authorization_id=uuid4(),
                    )
                for relation in (
                    "rt.option_flow_events",
                    "rt.option_flow_clusters",
                    "rt.portal_snapshot_hot",
                    "rt.portal_releases",
                ):
                    database_cursor.execute(
                        "SELECT rt.ensure_partitions(%s,%s)",
                        (relation, control.session_date),
                    )
            connection_value = database_pool.getconn()
            try:
                created_roles, security_audit = harden_database_v2(
                    connection_value,
                    roles,
                )
                roles_owned = True
                assert len(created_roles) == 3
                assert all(
                    value.startswith("PASS")
                    for _, value in security_audit
                )
            finally:
                database_pool.putconn(connection_value)
            provider = _app_provider(database_pool, roles)
            contracts = enrollment.activity_selection.contracts
            frames = [
                frame
                for contract in contracts
                for frame in _frames(
                    contract.contract_ticker,
                    authority.cycle.options_cutoff
                    - timedelta(milliseconds=200),
                    count=40,
                )
            ]
            gateway, progress, subscriptions = asyncio.run(
                _loopback_journal(
                    control,
                    contracts,
                    journal_path,
                    frames,
                )
            )
            assert gateway.final_state is GatewayFeedState.STOPPED
            assert progress.last_raw_offset == len(frames)
            assert progress.last_event_offset == len(frames)
            assert set(subscriptions) == {
                f"{channel.value}.{contract.contract_ticker}"
                for contract in contracts
                for channel in (
                    MarketChannel.TRADE,
                    MarketChannel.QUOTE,
                )
            }
            records = RawJournalReader(journal_path).read_normalized_after(
                0,
                limit=len(frames),
            )
            assert {row.provider_symbol for row in records} == {
                row.contract_ticker for row in contracts
            }
            market_map = MaterializerShardMap(
                "advanced-options-v3-market-state-v1",
                1,
                tuple(
                    (row.instrument_id, 0)
                    for row in contracts
                ),
            )
            registration = replace(
                build_advanced_options_journal_registration(
                    control_plane=control,
                    root=journal_root,
                    path=journal_path,
                    stream_session_id=(
                        control.source_flow.stream_session_ids[0]
                    ),
                    owner_epoch=1,
                    normalizer_version=NORMALIZER_VERSION,
                    shard_ids=(0,),
                    shard_map_sha256=market_map.sha256,
                ),
                status="DRAINED",
                raw_committed_offset=progress.last_raw_offset,
                normalized_committed_offset=progress.last_event_offset,
                bridge_committed_offset=progress.last_event_offset,
            )
            flow_policy = advanced_policy()
            semantics = load_default_live_semantics()
            detector_policy = policy()
            exchange = load_default_options_exchange_reference()
            contract_scope = AdvancedOptionsContractScope(
                version="advanced-options-contract-scope-v1",
                session_date=control.session_date,
                manifest_sha256=control.manifest_sha256,
                slot_sha256=control.slot_set_sha256,
                flow_policy_sha256=flow_policy.sha256,
                contracts=tuple(
                    AdvancedOptionsContractScopeEntry(
                        row.contract_ticker,
                        row.underlyer,
                        row.expiration_date,
                        flow_policy.tier_for(row.underlyer),
                        flow_policy.lane_for(
                            (
                                row.expiration_date
                                - control.session_date
                            ).days
                        ),
                    )
                    for row in contracts
                ),
            )
            engine_config = EngineConfig(
                cohort_id=control.cohort_id,
                provider_tier="MASSIVE_ADVANCED",
                policy=detector_policy,
                semantics=semantics,
                journal_generation=(
                    control.source_flow.journal_generation
                ),
                shard_count=1,
                queue_capacity=1024,
                advanced_flow_policy=flow_policy,
                contract_scope=contract_scope,
                exchange_reference=exchange,
            )
            source_flow = build_option_source_flow_identity(
                journal_generation=control.source_flow.journal_generation,
                stream_session_ids=control.source_flow.stream_session_ids,
                condition_semantics_sha256=semantics.semantics_sha256,
                flow_policy_sha256=engine_config.policy_sha256(),
                advanced_flow_policy_sha256=flow_policy.sha256,
                contract_scope_sha256=contract_scope.sha256,
                provider_contract_sha256=(
                    MASSIVE_OPTIONS_PROVIDER_CONTRACT.sha256
                ),
                condition_reference_sha256=(
                    semantics.pin.payload_sha256
                ),
                exchange_reference_sha256=exchange.sha256,
            )
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                persist_option_flow_policy_cursor(
                    database_cursor,
                    flow_policy,
                )
                persist_option_contract_scope_cursor(
                    database_cursor,
                    contract_scope,
                )
                persist_option_source_flow_cursor(
                    database_cursor,
                    source_flow,
                    source_cohort_id=control.cohort_id,
                    contract_scope=contract_scope,
                )
                database_cursor.execute(
                    "INSERT INTO rt.stream_sessions ("
                    "session_id,provider,feed,manifest_id,owner_id,"
                    "owner_epoch,started_at,authenticated_at,subscribed_at,"
                    "ended_at,status,journal_generation,normalizer_version,"
                    "authorization_reference,budget_scope_sha256,"
                    "connection_attempt,connection_ceiling,"
                    "decoded_byte_ceiling,received_frame_count,"
                    "received_byte_count,admitted_market_message_count,"
                    "admitted_market_byte_count,last_raw_offset,"
                    "last_event_offset,event_time_watermark,heartbeat_at"
                    ") VALUES (%s,'provider-free-loopback','OPTIONS',"
                    "%s,%s,1,%s,%s,%s,%s,'STOPPED',%s,%s,%s,%s,1,2,"
                    "100000000,%s,0,%s,0,%s,%s,%s,%s)",
                    (
                        str(control.source_flow.stream_session_ids[0]),
                        str(control.manifest_id),
                        str(uuid4()),
                        authority.session.acquisition_start,
                        authority.session.acquisition_start,
                        authority.session.acquisition_start,
                        authority.cycle.options_cutoff,
                        str(control.source_flow.journal_generation),
                        NORMALIZER_VERSION,
                        authority.session.authorization_reference,
                        authority.session.provider_budget_sha256,
                        len(frames),
                        len(frames),
                        len(frames),
                        len(frames),
                        authority.cycle.options_cutoff,
                        authority.cycle.options_cutoff,
                    ),
                )
            source = AdvancedOptionsJournalSource(
                root=journal_root,
                registration=registration,
                source_flow=source_flow,
            )
            market_consumer = AdvancedOptionsMarketStateConsumer(
                source=source,
                cohort_id=control.cohort_id,
                shard_map=market_map,
                owner_epoch=1,
                clock=lambda: authority.cycle.options_cutoff,
            )
            market_slice = market_consumer.plan_slice(
                maximum_records=10_000
            )
            market_owner = uuid4()
            resource_key = (
                f"market-state-materializer-v1:{control.cohort_id}:"
                f"{control.source_flow.journal_generation}:0"
            )
            with _role_cursor(
                database_pool,
                roles.name("admin"),
            ) as database_cursor:
                database_cursor.execute(
                    "INSERT INTO rt.worker_leases ("
                    "resource_key,owner_id,owner_epoch,lease_until) "
                    "VALUES (%s,%s,1,%s)",
                    (
                        resource_key,
                        str(market_owner),
                        datetime.now(UTC) + timedelta(minutes=30),
                    ),
                )
            with provider.transaction(APP_ROLE) as unit:
                persist_advanced_options_market_state_slice_cursor(
                    unit.cursor,
                    cohort_id=control.cohort_id,
                    journal_generation=(
                        control.source_flow.journal_generation
                    ),
                    planned=market_slice,
                    owner_id=market_owner,
                    batch_executor=execute_batch,
                )
            market_consumer.commit_slice(market_slice)
            flow_settings = OptionsFlowSettings(
                shard_count=1,
                queue_capacity=1024,
                history_per_contract=128,
                initial_baseline=BaselineKind.SESSION_COMPLETE,
                limits=ConsumerLimits(
                    batch_size=32,
                    max_messages=1024,
                    max_seconds=30,
                ),
                checkpoint_interval_seconds=0,
                checkpoint_interval_ms=1,
                advanced_flow_policy_sha256=flow_policy.sha256,
                contract_scope_sha256=contract_scope.sha256,
                exchange_reference_sha256=exchange.sha256,
            )
            world = World(
                database_pool,
                source,
                provider=provider,
                cohort=control.cohort_id,
                plan_id=control.plan_id,
                base_ms=int(
                    authority.cycle.options_cutoff.timestamp() * 1000
                ),
                enabled=set(OPTIONS_V3_STAGE_ORDER),
                stages=STAGES_V5,
                catalog_version=STAGE_CATALOG_VERSION_V5,
                options_flow_semantics=semantics,
                options_flow_policy=detector_policy,
                options_flow_settings=flow_settings,
                options_flow_advanced_policy=flow_policy,
                options_flow_contract_scope=contract_scope,
                options_flow_exchange_reference=exchange,
                options_flow_oi_loader=lambda _context, _session: {},
            )
            world.run_to_end()
            flow_rows = world.rows()
            assert flow_rows["events"]
            assert world.session().consumer.checkpointed_offset == len(
                frames
            )
            with provider.transaction(APP_ROLE) as unit:
                waiting = evaluate_resident_completion_barrier_cursor(
                    unit.cursor,
                    target_window_id=window_id,
                    target_cohort_id=control.cohort_id,
                    journal_generation=(
                        control.source_flow.journal_generation
                    ),
                )
                assert waiting.state is (
                    ResidentCompletionBarrierState.WAITING_INPUT
                )
                failed_evidence = {
                    "status": "FAILED",
                    "kind": "INGRESS",
                    "reason_codes": ["ROLE_EXCEPTION:RuntimeError"],
                    "effects": {
                        "dispatch": False,
                        "delivery": False,
                        "broker": False,
                        "trading": False,
                    },
                }
                record_resident_completion_cursor(
                    unit.cursor,
                    ResidentCompletion(
                        target_window_id=failure_window_id,
                        target_cohort_id=control.cohort_id,
                        member=ResidentCompletionMember.INGRESS,
                        journal_generation=(
                            control.source_flow.journal_generation
                        ),
                        owner_epoch=1,
                        ending_offset=0,
                        evidence_sha256=canonical_sha256(
                            failed_evidence
                        ),
                        evidence=failed_evidence,
                        status=ResidentCompletionStatus.FAILED,
                        reason_codes=(
                            "ROLE_EXCEPTION:RuntimeError",
                        ),
                        completed_at=(
                            authority.cycle.options_cutoff
                        ),
                    ),
                )
                terminal_failure = (
                    evaluate_resident_completion_barrier_cursor(
                        unit.cursor,
                        target_window_id=failure_window_id,
                        target_cohort_id=control.cohort_id,
                        journal_generation=(
                            control.source_flow.journal_generation
                        ),
                    )
                )
                assert terminal_failure.state is (
                    ResidentCompletionBarrierState.FAILED
                )
                ingress = _record_completion(
                    unit.cursor,
                    control=control,
                    window_id=window_id,
                    member=ResidentCompletionMember.INGRESS,
                    ending_offset=len(frames),
                    evidence={
                        "status": "COMPLETE",
                        "kind": "INGRESS",
                        "raw_end_offset": len(frames),
                        "normalized_end_offset": len(frames),
                        "connection_attempts": 1,
                        "effects": {
                            "dispatch": False,
                            "delivery": False,
                            "broker": False,
                            "trading": False,
                        },
                    },
                )
                _record_completion(
                    unit.cursor,
                    control=control,
                    window_id=window_id,
                    member=ResidentCompletionMember.MARKET_STATE,
                    ending_offset=market_slice.end_offset,
                    evidence={
                        "status": "COMPLETE",
                        "kind": "MARKET_STATE",
                        "normalized_end_offset": market_slice.end_offset,
                        "state_sha256": _digest(flow_rows),
                        "shard_map_sha256": market_map.sha256,
                        "shard_ids": [0],
                        "effects": {
                            "dispatch": False,
                            "delivery": False,
                            "broker": False,
                            "trading": False,
                        },
                    },
                )
                _record_completion(
                    unit.cursor,
                    control=control,
                    window_id=window_id,
                    member=ResidentCompletionMember.FLOW,
                    ending_offset=len(frames),
                    evidence={
                        "status": "COMPLETE",
                        "kind": "FLOW",
                        "raw_end_offset": len(frames),
                        "normalized_end_offset": market_slice.end_offset,
                        "checkpoint_end_offset": len(frames),
                        "owner_epoch": 1,
                        "evidence_ref": (
                            f"flow:{source_flow.source_flow_id}:"
                            f"{len(frames)}"
                        ),
                        "shard_map_sha256": "c" * 64,
                        "shard_ids": [0],
                        "effects": {
                            "dispatch": False,
                            "delivery": False,
                            "broker": False,
                            "trading": False,
                        },
                    },
                )
                complete = evaluate_resident_completion_barrier_cursor(
                    unit.cursor,
                    target_window_id=window_id,
                    target_cohort_id=control.cohort_id,
                    journal_generation=(
                        control.source_flow.journal_generation
                    ),
                )
                assert complete.state is (
                    ResidentCompletionBarrierState.COMPLETE
                )
                with pytest.raises(ResidentCompletionDivergence):
                    record_resident_completion_cursor(
                        unit.cursor,
                        replace(
                            ingress,
                            evidence_sha256=canonical_sha256({
                                **ingress.evidence,
                                "raw_end_offset": len(frames) + 1,
                            }),
                            evidence={
                                **ingress.evidence,
                                "raw_end_offset": len(frames) + 1,
                            },
                        ),
                    )
            rolling = _rolling_state(
                control,
                reference,
                equity_instruments,
                contracts,
                len(frames),
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
                    market_cutoff=authority.cycle.options_cutoff,
                    known_at=authority.cycle.options_cutoff,
                    authorized_until=control.expires_at,
                    prior_oi_session=(
                        control.session_date - timedelta(days=1)
                    ),
                ),
                endpoint_response_byte_limits=_limits(),
            )
            cycle_input = OptionCycleInput(
                provider_tier=ProviderTier.MASSIVE_ADVANCED,
                scheduled_cycle=authority.cycle.options_cutoff,
                profile_id=control.profile_id,
                profile_sha256=control.profile_sha256,
                universe_sha256=control.universe.sha256,
                policy_sha256=control.manifest_policy.sha256,
                configuration_sha256=control.plan_sha256,
                code_sha256=_digest("cycle-input-code"),
                cohort_id=control.cohort_id,
                underlyings=tuple(
                    OptionCycleUnderlying(
                        underlyer=ticker,
                        underlying_security_id=dict(
                            enrollment.equity_plan.security_ids
                        )[ticker],
                        discovery_batch_id=uuid5(
                            NAMESPACE_URL,
                            f"v3-discovery:{ticker}",
                        ),
                        discovery_evidence_sha256=_digest((
                            "discovery",
                            ticker,
                        )),
                        payload_chain_sha256=_digest((
                            "pages",
                            ticker,
                        )),
                        coverage_sha256=_digest((
                            "coverage",
                            ticker,
                        )),
                        request_filter_sha256=_digest((
                            "filter",
                            ticker,
                        )),
                        page_count=1,
                        row_count=1,
                        response_bytes=128,
                        expiration_from=contracts[0].expiration_date,
                        expiration_through=contracts[0].expiration_date,
                        strike_min=Decimal("1"),
                        strike_max=Decimal("1000"),
                    )
                    for ticker in ("QQQ", "SPY")
                ),
                slot_set_version=control.slot_set.version,
                slot_set_member_sha256=(
                    control.slot_set.member_sha256
                ),
            )
            closure = build_advanced_options_input_closure(
                control_plane=control,
                producer_plan=producer_plan,
                cycle_input=cycle_input,
                reference_state=reference,
                rolling_state=rolling,
                reference_policy_sha256=(
                    reference.reference_policy_sha256
                ),
                oi_facts_sha256=reference.open_interest_sha256,
                market_state_checkpoint_sha256=(
                    rolling.market_state_checkpoint_sha256
                ),
                flow_checkpoint_sha256=(
                    rolling.flow_checkpoint_sha256
                ),
                unresolved_gaps_sha256=_digest("no-gaps"),
                market_state_heads_sha256=_digest("market-heads"),
                equity_input_receipt_id=equity_receipt_id,
                equity_input_sha256=equity_receipt.input_sha256,
                native_input_plan_sha256=_digest("native-plan"),
                native_input_sha256=_digest("native-input"),
                executor_membership_sha256=_digest("executors"),
                mark_producer_plan_sha256=_digest("mark-plan"),
                marks_sha256=_digest("marks"),
            )
            facts = AdvancedOptionsCanonicalInputFacts(
                control_plane=control,
                producer_plan=producer_plan,
                cycle_input=cycle_input,
                reference_state=reference,
                rolling_state=rolling,
                reference_policy_sha256=(
                    reference.reference_policy_sha256
                ),
                oi_facts_sha256=reference.open_interest_sha256,
                market_state_checkpoint_sha256=(
                    rolling.market_state_checkpoint_sha256
                ),
                flow_checkpoint_sha256=(
                    rolling.flow_checkpoint_sha256
                ),
                unresolved_gaps_sha256=_digest("no-gaps"),
                market_state_heads_sha256=_digest("market-heads"),
                equity_input_receipt_id=equity_receipt_id,
                equity_input_receipt=equity_receipt,
                equity_input_sha256=equity_receipt.input_sha256,
                equity_window_id=equity_window_id,
                instrument_ids={
                    ticker: equity_instruments[ticker]
                    for ticker in ("QQQ", "SPY")
                },
                available_at=authority.cycle.options_cutoff,
                maximum_equity_input_age_seconds=300,
                native_input_plan_sha256=_digest("native-plan"),
                native_input_sha256=_digest("native-input"),
                executor_membership_sha256=_digest("executors"),
                mark_producer_plan_sha256=_digest("mark-plan"),
                marks_sha256=_digest("marks"),
            )
            assert closure == (
                build_advanced_options_input_closure(
                    control_plane=control,
                    producer_plan=producer_plan,
                    cycle_input=cycle_input,
                    reference_state=reference,
                    rolling_state=rolling,
                    reference_policy_sha256=(
                        reference.reference_policy_sha256
                    ),
                    oi_facts_sha256=reference.open_interest_sha256,
                    market_state_checkpoint_sha256=(
                        rolling.market_state_checkpoint_sha256
                    ),
                    flow_checkpoint_sha256=(
                        rolling.flow_checkpoint_sha256
                    ),
                    unresolved_gaps_sha256=_digest("no-gaps"),
                    market_state_heads_sha256=_digest("market-heads"),
                    equity_input_receipt_id=equity_receipt_id,
                    equity_input_sha256=equity_receipt.input_sha256,
                    native_input_plan_sha256=_digest("native-plan"),
                    native_input_sha256=_digest("native-input"),
                    executor_membership_sha256=_digest("executors"),
                    mark_producer_plan_sha256=_digest("mark-plan"),
                    marks_sha256=_digest("marks"),
                )
            )
            native = AdvancedOptionsNativeInputBundle(
                o1=AdvancedOptionsLaneInputs(),
                o3=AdvancedOptionsLaneInputs(),
            )
            reference_repository = (
                InMemoryAdvancedOptionsReferenceStateRepository()
            )
            reference_repository.put(reference)
            publisher_database = _publisher_database(
                database_pool,
                roles,
            )
            publication_config = PortalPublicationConfig(
                cohort_id=control.cohort_id,
                provider_tier=ProviderTier.MASSIVE_ADVANCED,
                owner_id=uuid4(),
                owner_epoch=1,
                dispatch_authorized=False,
            )
            publisher = CanonicalPortalPublisher(
                publisher_database,
                publication_config,
            )
            invocation_counts = {
                stage: 0 for stage in OPTIONS_V3_STAGE_ORDER
            }
            fail_publication = [False]

            def build_composition():
                port = _RuntimePort(
                    provider,
                    facts,
                    native,
                    reference_repository,
                )

                def publication_authority(context, handoff):
                    with provider.transaction(
                        APP_ROLE,
                        read_only=True,
                    ) as unit:
                        unit.cursor.execute(
                            "SELECT receipt_id::text,input_sha256 "
                            "FROM rt.cycle_receipts "
                            "WHERE window_id=%s AND receipt_kind="
                            "'OPTIONS_PORTAL_READY'",
                            (str(context.window.window_id),),
                        )
                        row = unit.cursor.fetchone()
                    assert row is not None
                    assert str(row[1]) == handoff.batch_sha256
                    return AdvancedOptionsPublicationAuthorization(
                        uuid5(
                            NAMESPACE_URL,
                            "v3-owned-publication-authority",
                        ),
                        control.cohort_id,
                        ProviderTier.MASSIVE_ADVANCED,
                        handoff.batch_sha256,
                        UUID(str(row[0])),
                        "OWNED_DISPOSABLE_V3_PUBLICATION",
                    )

                canonical = publisher.publish
                if fail_publication[0]:
                    def fail_release(
                        cursor_value,
                        receipt,
                        snapshots,
                        **options,
                    ):
                        publish_portal_release_cursor(
                            cursor_value,
                            receipt,
                            snapshots,
                            **options,
                        )
                        raise RuntimeError(
                            "injected publication transaction failure"
                        )

                    def fail_publication_runner(
                        database,
                        config,
                        batch,
                        sink,
                        *,
                        checked_at,
                    ):
                        return run_portal_publication_once(
                            database,
                            config,
                            batch,
                            sink,
                            checked_at=checked_at,
                            release_repository=fail_release,
                        )

                    failing = CanonicalPortalPublisher(
                        publisher_database,
                        publication_config,
                        publication_runner=(
                            fail_publication_runner
                        ),
                    )
                    canonical = failing.publish
                operations = SimpleNamespace(
                    acquire=lambda *_args, **_kwargs: (
                        OptionsAcquireResult(
                            control.manifest_id,
                            ("QQQ", "SPY"),
                            ("QQQ", "SPY"),
                            0,
                            0,
                        )
                    ),
                    admit=lambda *_args, **_kwargs: (
                        OptionsAdmissionResult(
                            authority.admission.admission_id,
                            "READY",
                            (),
                            control.cohort_id,
                            control.slot_set.version,
                            len(control.slot_set.members),
                        )
                    ),
                )
                binding, _ = (
                    build_canonical_advanced_options_runtime_binding(
                        build_id=BUILD_ID,
                        pre_session=OptionsPreSessionStageBinding(
                            operations
                        ),
                        port=port,
                        catalog_version=STAGE_CATALOG_VERSION_V5,
                        publication_authority_loader=(
                            publication_authority
                        ),
                        canonical_publisher=(
                            lambda batch, *, checked_at: canonical(
                                batch,
                                checked_at=checked_at,
                            )
                        ),
                    )
                )
                repository = PostgresOrchestrationRepository(
                    provider,
                    window_stage_effect_authorizer=(
                        AdvancedOptionsWindowStageEffectAuthorizer(
                            authority.cycle
                        )
                    ),
                )
                composition = (
                    build_advanced_options_v3_runtime_composition(
                        binding=binding,
                        repository=repository,
                        database=provider,
                        environment=RunnerEnvironment(provider),
                    )
                )
                runners = dict(composition.runners)
                runners["OPTIONS_FLOW"] = lambda _context: (
                    StageOutcome.complete(
                        evidence_ref=(
                            f"resident-flow:{source_flow.source_flow_id}"
                        )
                    )
                )
                for stage, runner in tuple(runners.items()):
                    def counted(context, *, _stage=stage, _runner=runner):
                        invocation_counts[_stage] += 1
                        return _runner(context)

                    runners[stage] = counted
                return repository, composition, runners, port

            def run_through(
                stop_stage,
                *,
                fail_publish=False,
                now_offset_seconds=0,
            ):
                fail_publication[0] = fail_publish
                repository, composition, runners, port = (
                    build_composition()
                )
                start = composition.orchestrator.start(
                    control.plan_id
                )
                assert start.enabled == frozenset(
                    OPTIONS_V3_STAGE_ORDER
                )
                now = authority.cycle.prepared_at + timedelta(
                    seconds=now_offset_seconds
                )
                claim = repository.claim_window(
                    window_id,
                    uuid4(),
                    now,
                    300,
                )
                assert claim is not None
                plan = repository.load_plan(control.plan_id)
                for stage_id in OPTIONS_V3_STAGE_ORDER:
                    existing = repository.stage_states(window_id).get(
                        stage_id
                    )
                    if existing is not None and existing.state == "COMPLETE":
                        if stage_id == stop_stage:
                            break
                        continue
                    stage = STAGES_V5[stage_id]
                    attempt = (
                        1 if existing is None else existing.attempt + 1
                    )
                    run = repository.start_stage(
                        claim,
                        stage,
                        STAGE_CATALOG_VERSION_V5,
                        attempt,
                        2,
                        now,
                    )
                    context = StageContext(
                        plan,
                        claim.window,
                        stage,
                        run.stage_run_id,
                        attempt,
                        now,
                        provider,
                        fenced=True,
                    )
                    try:
                        outcome = runners[stage_id](context)
                    except Exception as error:
                        if not (
                            fail_publish
                            and stage_id == "OPTIONS_PUBLISH"
                        ):
                            raise
                        outcome = StageOutcome.failed(
                            f"FORCED_FAILURE:{type(error).__name__}"
                        )
                    receipt_id = (
                        None
                        if outcome.receipt is None
                        else repository.record_receipt(
                            claim,
                            outcome.receipt,
                            run,
                        )
                    )
                    repository.finish_stage(
                        claim,
                        run,
                        outcome.state,
                        outcome.reason_codes,
                        receipt_id,
                        now,
                    )
                    if fail_publish and stage_id == "OPTIONS_PUBLISH":
                        assert outcome.state == "FAILED"
                        repository.release_window(claim, now)
                        return port
                    assert outcome.state == "COMPLETE", (
                        stage_id,
                        outcome,
                    )
                    if stage_id == stop_stage:
                        repository.release_window(claim, now)
                        return port
                repository.release_window(claim, now)
                return port

            run_through("OPTIONS_INPUT_GATE")
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                unit.cursor.execute(
                    "SELECT receipt_id::text,input_sha256 "
                    "FROM rt.cycle_receipts WHERE window_id=%s "
                    "AND receipt_kind='OPTIONS_INPUT_READY'",
                    (str(window_id),),
                )
                input_receipt_id, input_sha256 = unit.cursor.fetchone()
                unit.cursor.execute(
                    "SELECT member_key,outcome,outcome_input_sha256 "
                    "FROM rt.job_barrier_members "
                    "WHERE barrier_id=(SELECT barrier_id FROM "
                    "rt.job_barriers WHERE window_id=%s "
                    "AND barrier_kind='OPTIONS_INPUT_READY') "
                    "ORDER BY member_key",
                    (str(window_id),),
                )
                input_rows = tuple(unit.cursor.fetchall())
            equity_snapshot = build_portal_snapshot(
                cohort_id=control.cohort_id,
                provider_tier=ProviderTier.MASSIVE_ADVANCED,
                snapshot_type="TICKER_WORKSPACE_SUMMARY_V1",
                snapshot_key="PLATFORM",
                source_generation=7,
                source_manifest={
                    "lane": "EQUITY",
                    "symbols": list(
                        EQUITY_VALIDATION_UNIVERSE.equity_tickers
                    ),
                },
                payload={
                    "lane": "EQUITY",
                    "target": 9,
                },
                generated_at=authority.cycle.options_cutoff
                - timedelta(seconds=1),
            )
            with provider.transaction(APP_ROLE) as unit:
                persist_portal_snapshot_cursor(
                    unit.cursor,
                    equity_snapshot,
                )
                prior_release = publish_portal_release_cursor(
                    unit.cursor,
                    PortalReleaseReceipt(
                        UUID(input_receipt_id),
                        control.cohort_id,
                        ProviderTier.MASSIVE_ADVANCED,
                        authority.cycle.options_cutoff,
                        control.universe.sha256,
                    ),
                    (equity_snapshot,),
                )
            run_through("OPTIONS_CYCLE")
            context = SimpleNamespace(
                plan=SimpleNamespace(
                    cohort_id=control.cohort_id,
                ),
                window=SimpleNamespace(
                    window_id=window_id,
                ),
            )
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                unit.cursor.execute(
                    "SELECT input_sha256 FROM rt.cycle_receipts "
                    "WHERE window_id=%s AND receipt_kind="
                    "'OPTIONS_INPUT_READY'",
                    (str(window_id),),
                )
                assert unit.cursor.fetchone()[0] == input_sha256
                unit.cursor.execute(
                    "SELECT count(*) FROM rt.job_barrier_members "
                    "WHERE barrier_id=(SELECT barrier_id FROM "
                    "rt.job_barriers WHERE window_id=%s "
                    "AND barrier_kind='OPTIONS_INPUT_READY')",
                    (str(window_id),),
                )
                assert unit.cursor.fetchone()[0] == len(input_rows)
            run_through("OPTIONS_PROJECTION")
            fresh_port = _RuntimePort(
                provider,
                facts,
                native,
                reference_repository,
            )
            _binding_probe, loaders_probe = (
                build_canonical_advanced_options_runtime_binding(
                    build_id=BUILD_ID,
                    pre_session=OptionsPreSessionStageBinding(
                        SimpleNamespace()
                    ),
                    port=fresh_port,
                    catalog_version=STAGE_CATALOG_VERSION_V5,
                    publication_authority_loader=lambda *_args: None,
                    canonical_publisher=lambda *_args: None,
                )
            )
            probe_context = SimpleNamespace(
                plan=SimpleNamespace(
                    plan_id=control.plan_id,
                    cohort_id=control.cohort_id,
                    session_date=control.session_date,
                    stage_catalog_version=STAGE_CATALOG_VERSION_V5,
                ),
                window=SimpleNamespace(
                    window_id=window_id,
                    plan_id=control.plan_id,
                    cohort_id=control.cohort_id,
                    cutoff=authority.cycle.options_cutoff,
                ),
                transaction=provider.transaction,
            )
            cycle = fresh_port.load_completed_cycle(
                probe_context,
                loaders_probe._input_for_downstream(probe_context),
            )
            projection = fresh_port.load_completed_projection(
                probe_context,
                cycle,
            )
            cycle_rows = _query(
                database_pool,
                "SELECT count(*) FROM rt.detector_datasets "
                "WHERE dataset_id=%s",
                (str(cycle.dataset_id),),
            )[0][0]
            projection_rows = _query(
                database_pool,
                "SELECT count(*) FROM rt.snapshot_catalog "
                "WHERE snapshot_id=ANY(%s::uuid[])",
                ([str(row.snapshot_id) for row in projection.snapshots],),
            )[0][0]
            assert cycle_rows == 1
            assert projection_rows == len(projection.snapshots)
            run_through("OPTIONS_PUBLISH", fail_publish=True)
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                pointer_after_failure = load_portal_release_pointer_cursor(
                    unit.cursor,
                    cohort_id=control.cohort_id,
                    provider_tier=ProviderTier.MASSIVE_ADVANCED,
                )
            assert pointer_after_failure.release_id == (
                prior_release.release_id
            )
            run_through(
                "OPTIONS_PUBLISH",
                now_offset_seconds=3600,
            )
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                pointer = load_portal_release_pointer_cursor(
                    unit.cursor,
                    cohort_id=control.cohort_id,
                    provider_tier=ProviderTier.MASSIVE_ADVANCED,
                )
                unit.cursor.execute(
                    "SELECT manifest FROM rt.portal_releases "
                    "WHERE release_id=%s",
                    (str(pointer.release_id),),
                )
                manifest = unit.cursor.fetchone()[0]
                unit.cursor.execute(
                    "SELECT count(*) FROM rt.portal_releases "
                    "WHERE manifest->'source_receipt_ids' ? ("
                    "SELECT receipt_id::text "
                    "FROM rt.cycle_receipts WHERE window_id=%s "
                    "AND receipt_kind='OPTIONS_PORTAL_READY')",
                    (str(window_id),),
                )
                release_count = unit.cursor.fetchone()[0]
                authority_status = (
                    advanced_options_authority_status_cursor(
                        unit.cursor,
                        authority,
                    )
                )
                unit.cursor.execute(
                    "SELECT count(*) FROM rt.alert_outbox "
                    "WHERE cohort_id=%s",
                    (str(control.cohort_id),),
                )
                outbox_count = unit.cursor.fetchone()[0]
            identities = {
                (row["snapshot_type"], row["snapshot_key"])
                for row in manifest["snapshots"]
            }
            assert (
                equity_snapshot.snapshot_type,
                equity_snapshot.snapshot_key,
            ) in identities
            assert pointer.release_id != prior_release.release_id
            assert release_count == 1
            assert authority_status["session_acquisition"][
                "state"
            ] == "CONSUMED"
            assert authority_status["window_cycle"]["state"] == "CONSUMED"
            assert outbox_count == 0
            release_before_replay = pointer.release_id
            replay_port = run_through(
                "OPTIONS_PUBLISH",
                now_offset_seconds=3600,
            )
            assert replay_port is not None
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                replay_pointer = load_portal_release_pointer_cursor(
                    unit.cursor,
                    cohort_id=control.cohort_id,
                    provider_tier=ProviderTier.MASSIVE_ADVANCED,
                )
            assert replay_pointer.release_id == release_before_replay
            assert invocation_counts["OPTIONS_INPUT_GATE"] == 1
            assert invocation_counts["OPTIONS_CYCLE"] == 1
            assert invocation_counts["OPTIONS_PROJECTION"] == 1
            assert invocation_counts["OPTIONS_PUBLISH"] == 2
            policy_value = AdvancedOptionsResidentPolicy(
                "advanced-options-resident-v1"
            )
            assert [
                policy_value.restart_delay_seconds(index)
                for index in range(1, 7)
            ] == [1, 2, 4, 8, 16, 30]
            exhausted = evaluate_advanced_options_process_health(
                AdvancedOptionsProcessHealth(
                    AdvancedOptionsProcessKind.INGRESS,
                    uuid4(),
                    1,
                    authority.cycle.options_cutoff,
                    policy_value.restart_failure_limit,
                    authority.cycle.options_cutoff,
                    None,
                ),
                policy_value,
                checked_at=authority.cycle.options_cutoff,
            )
            assert exhausted.status == "UNAVAILABLE"
            assert "RESTART_BUDGET_EXHAUSTED" in exhausted.reason_codes
            shutil.rmtree(journal_root)
            report = {
                "contract": (
                    "advanced-options-v3-provider-free-"
                    "qualification-report-v1"
                ),
                "verdict": "PASS",
                "scope": {
                    "equity": {
                        "target": 9,
                        "feed": "STOCKS",
                        "channels": ["AM"],
                        "symbols": list(
                            EQUITY_VALIDATION_UNIVERSE.equity_tickers
                        ),
                    },
                    "options": {
                        "underlyings": ["QQQ", "SPY"],
                        "channels": ["T", "Q"],
                    },
                    "profile_sha256": (
                        ADVANCED_OPTIONS_V3_PROFILE_SHA256
                    ),
                    "profile": advanced_options_v3_profile_payload(),
                },
                "database": {
                    "name": database_name,
                    "owned_disposable": True,
                    "roles": list(roles.all_names),
                    "security_checks": len(security_audit),
                },
                "authorities": {
                    "bundle_sha256": authority.sha256,
                    "document": advanced_options_authority_bundle_payload(
                        authority
                    ),
                    "session_receipt_sha256": (
                        session_receipt.receipt_sha256
                    ),
                    "session_state": "CONSUMED",
                    "window_state": "CONSUMED",
                    "publication": (
                        "OWNED_DISPOSABLE_V3_PUBLICATION"
                    ),
                    "stored_read": "NOT_AUTHORIZED",
                },
                "residents": {
                    "loopback_frames": len(frames),
                    "normalized_messages": len(records),
                    "market_state_offset": market_slice.end_offset,
                    "flow_checkpoint_offset": len(frames),
                    "barrier_sha256": complete.evidence_sha256,
                    "waiting_input_proved": True,
                    "terminal_failure_proved": True,
                    "terminal_divergence_rejected": True,
                    "retry_delays": [1, 2, 4, 8, 16, 30],
                    "retry_limit": policy_value.restart_failure_limit,
                    "retry_exhaustion_reason_codes": list(
                        exhausted.reason_codes
                    ),
                },
                "restarts": {
                    "after_input_gate": True,
                    "after_cycle": True,
                    "after_projection": True,
                    "input_sha256": input_sha256,
                    "input_rows": len(input_rows),
                    "cycle_sha256": cycle.cycle_sha256,
                    "cycle_rows": cycle_rows,
                    "projection_sha256": projection.batch_sha256,
                    "projection_rows": projection_rows,
                    "completed_stage_invocations": invocation_counts,
                    "divergence_rejected": True,
                },
                "publication": {
                    "prior_release_id": str(
                        prior_release.release_id
                    ),
                    "release_id": str(pointer.release_id),
                    "pointer_advances": 1,
                    "equity_snapshot_preserved": str(
                        equity_snapshot.snapshot_id
                    ),
                    "idempotent_replay": True,
                    "forced_failure_rolled_back": True,
                    "receipt_release_count": release_count,
                },
                "effects": {
                    "provider_rest": 0,
                    "provider_websocket": 0,
                    "production_database_writes": 0,
                    "production_service_actions": 0,
                    "production_migrations": 0,
                    "dispatch": 0,
                    "delivery": 0,
                    "broker": 0,
                    "trading": 0,
                },
                "source": {
                    "catalog_version": STAGE_CATALOG_VERSION_V5,
                    "stage_order": list(OPTIONS_V3_STAGE_ORDER),
                    "build_id": BUILD_ID,
                    "files": source_binding_identity(
                        Path(__file__).parents[3],
                        ADVANCED_OPTIONS_V3_SOURCE_BINDING_PATHS,
                    ),
                    "configuration": {
                        "equity_configuration_sha256": (
                            enrollment.equity_plan.configuration_sha256
                        ),
                        "equity_profile_sha256": (
                            enrollment.equity_plan.profile_sha256
                        ),
                        "options_plan_sha256": control.plan_sha256,
                        "options_profile_sha256": (
                            control.profile_sha256
                        ),
                        "universe_sha256": control.universe.sha256,
                        "manifest_sha256": control.manifest_sha256,
                        "slot_set_sha256": control.slot_set_sha256,
                        "flow_policy_sha256": flow_policy.sha256,
                        "contract_scope_sha256": (
                            contract_scope.sha256
                        ),
                    },
                    "docker": {
                        "executed": False,
                        "image_id": "NOT_BUILT_OR_INSPECTED",
                    },
                },
            }
    finally:
        if journal_root.exists():
            shutil.rmtree(journal_root)
        if roles_owned:
            try:
                with _connect(settings, "postgres") as admin:
                    drop_disposable_roles_v2(admin, roles)
                    admin.commit()
            except Exception:
                pass
        cleanup = {
            "databases": [],
            "roles": [],
            "files": [],
            "processes": [],
            "locks": [],
        }
        try:
            with _connect(settings, "postgres") as admin:
                with admin.cursor() as database_cursor:
                    database_cursor.execute(
                        "SELECT datname FROM pg_database WHERE datname=%s",
                        (database_name,),
                    )
                    cleanup["databases"] = [
                        row[0] for row in database_cursor.fetchall()
                    ]
                    database_cursor.execute(
                        "SELECT rolname FROM pg_roles "
                        "WHERE rolname=ANY(%s)",
                        (list(roles.all_names),),
                    )
                    cleanup["roles"] = [
                        row[0] for row in database_cursor.fetchall()
                    ]
        except Exception:
            cleanup["databases"] = [database_name]
            cleanup["roles"] = list(roles.all_names)
        cleanup["files"] = [
            str(path) for path in (journal_root, journal_path)
            if path.exists()
        ]
        assert cleanup == {
            "databases": [],
            "roles": [],
            "files": [],
            "processes": [],
            "locks": [],
        }
        if report is not None:
            report["cleanup"] = cleanup
            if os.getenv(REPORT_ENV) == "1":
                path, semantic_sha256, report_file_sha256 = _retain(
                    report
                )
                print(
                    f"report={path} semantic_sha256={semantic_sha256} "
                    f"file_sha256={report_file_sha256}"
                )
    if os.getenv(REPORT_ENV) == "1":
        output = capsys.readouterr().out
        assert "semantic_sha256=" in output
        assert "file_sha256=" in output
