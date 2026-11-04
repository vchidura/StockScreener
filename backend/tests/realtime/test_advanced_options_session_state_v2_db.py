from __future__ import annotations

import json
import math
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest
from options.calendar import OptionExchangeCalendar
from options.data.polygon_http import PolygonHttpResponse
from psycopg2 import errors
from psycopg2.extras import Json, RealDictCursor

from realtime.advanced_options_acquisition_runtime import (
    build_acquisition_binding,
)
from realtime.advanced_equity_control_plane import (
    build_advanced_equity_control_plane_plan,
    persist_advanced_equity_control_plane_cursor,
)
from realtime.advanced_options_continuous_scheduler import (
    run_continuous_options_schedule_cycle,
)
from realtime.advanced_options_control_plane import (
    ADVANCED_OPTIONS_REST_LANES,
    AdvancedOptionsSocketBudget,
    advanced_options_history_binding_payload,
    build_advanced_options_history_binding,
)
from realtime.advanced_options_p8_runtime import (
    build_p8_downstream_binding,
    p8_downstream_binding_payload,
)
from realtime.advanced_options_session_state import (
    PostgresContinuousSessionStateRepository,
)
from realtime.advanced_options_lifecycle import (
    AdvancedOptionsPreparationRetryable,
    AdvancedOptionsProviderRequestPlan,
    AdvancedOptionsProviderRequestResult,
    AdvancedOptionsStageProviderBudget,
    AdvancedOptionsStageResult,
    claim_advanced_options_preparation_cursor,
    reconcile_advanced_options_lifecycle_cursor,
    run_advanced_options_preparation_cycle,
)
from realtime.advanced_options_automatic_preparation import (
    PostgresAutomaticOptionsPreparationExecutor,
)
from realtime.advanced_options_activity_momentum_c6 import (
    LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION,
)
from realtime.advanced_options_p8_selection_source import (
    SEALED_CAPTURE_EXACT_FOUR_PROFILE,
    SEALED_CAPTURE_GENERALIZED_C6_PROFILE,
    p8_activity_selection_capture_from_sealed,
)
from realtime.advanced_realtime_desired_state import (
    DesiredMemberKind,
    desired_member_intent_id,
)
from scripts.report_advanced_realtime_v3_status import (
    build_advanced_realtime_v3_status_cursor,
)
from realtime import advanced_options_automatic_preparation as preparation_module
from realtime import advanced_options_session_state as session_state_module
from realtime.advanced_options_v3_authority import (
    AdvancedOptionsAuthorityBundle,
    OpeningAdmissionStatus,
    OpeningOpenInterestEvidence,
    OpeningReferenceEvidence,
    ProspectiveOpeningContract,
    _opening_admission,
    build_opening_subscription_readiness,
    build_session_acquisition_authorization,
    prospective_opening_admission_payload,
    session_acquisition_authorization_payload,
)
from realtime.contracts import (
    EQUITY_VALIDATION_UNIVERSE,
    MarketChannel,
    UniverseManifest,
    canonical_sha256,
)
from realtime.database_access_v2 import (
    APP_ROLE,
    OWNER_POOL,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.stage_graph import STAGE_CATALOG_VERSION_V5
from realtime.option_slot_set import (
    SlotSetKind,
    SlotSetMember,
    SlotSetPurpose,
    SlotSetVersion,
)
from realtime.provider_budget import (
    ProviderBudgetScope,
    reserve_coordinated_provider_request_cursor,
)
from realtime.acquisition_ledger import (
    ProviderRequestLane,
    ProviderRequestReservation,
)
from realtime.reference_adapters import (
    _corporate_action_coverage_fact,
    _security_id as reference_security_id,
)
from realtime.reference_repository import persist_reference_facts_cursor
from realtime.reference_service import ReferenceSeedRequest
from realtime.advanced_options_reference_repository import (
    PostgresAdvancedOptionsReferenceStateRepository,
    persist_advanced_options_reference_state_cursor,
)
from realtime.advanced_options_reference_state import (
    AdvancedOptionsAdjustedDailyBarIdentity,
    AdvancedOptionsCorporateActionIdentity,
    AdvancedOptionsDividendCoverageReference,
    AdvancedOptionsEquityMetric,
    AdvancedOptionsOpenInterestReference,
    AdvancedOptionsRateReference,
    AdvancedOptionsValuationReference,
    EquityMetricKind,
    ReferenceAvailability,
    build_advanced_options_reference_state,
    build_historical_volatility_fact,
)
from tests.realtime.test_advanced_options_acquisition_runtime import (
    _production_binding,
)
from tests.realtime.test_advanced_options_v3_authority import (
    OPENING,
)
from tests.realtime.test_option_session_manifest import SESSION_DATE
from tests.realtime.test_option_slot_sets_v2_db import cursor
from tests.realtime.v2_database import (
    disposable_v2_database,
    insert_cohort,
    requires_v2_database,
)


pytestmark = requires_v2_database


def _digest(label: str) -> str:
    return canonical_sha256({"c5": label})


def _security_id(ticker: str) -> UUID:
    return reference_security_id(ticker)


def _sealed_selection(admission):
    market_time = admission.prepared_at - timedelta(seconds=1)
    market_ns = int(market_time.timestamp() * 1_000_000_000)
    snapshots = {}
    for contract in admission.contracts:
        snapshots[contract.contract_ticker] = {
            "details": {
                "ticker": contract.contract_ticker,
                "contract_type": contract.contract_type.casefold(),
                "expiration_date": (
                    contract.expiration_date.isoformat()
                ),
                "strike_price": str(contract.strike),
            },
            "last_quote": {
                "bid": 1,
                "ask": 1.1,
                "last_updated": market_ns,
            },
            "day": {"volume": contract.day_volume},
            "open_interest": contract.open_interest,
            "underlying_asset": {
                "ticker": contract.underlyer,
                "price": str(contract.spot),
                "last_updated": market_ns,
                "timeframe": "REAL-TIME",
            },
        }
    document = {
        "contract": "advanced-options-v3-pre-session-capture-v1",
        "compatibility_profile": SEALED_CAPTURE_EXACT_FOUR_PROFILE,
        "configured_underlyings": ["QQQ", "SPY"],
        "captured_at_utc": admission.prepared_at.isoformat(),
        "session_date": admission.session_date.isoformat(),
        "provider_requests": 2,
        "decoded_bytes": 4096,
        "responses": [
            {"underlyer": "QQQ", "request_id": "c5-qqq"},
            {"underlyer": "SPY", "request_id": "c5-spy"},
        ],
        "selected_snapshots": snapshots,
    }
    document["semantic_sha256"] = canonical_sha256(document)
    return document


def _authority_fixture(options_cohort_id):
    contract_specs = (
        ("QQQ", "CALL", "O:QQQ261005C00600000", Decimal("600")),
        ("QQQ", "PUT", "O:QQQ261005P00600000", Decimal("600")),
        ("SPY", "CALL", "O:SPY261005C00670000", Decimal("670")),
        ("SPY", "PUT", "O:SPY261005P00670000", Decimal("670")),
    )
    reference_members = tuple(
        OpeningOpenInterestEvidence(
            contract_id=20_000 + index,
            contract_ticker=ticker,
            underlyer=underlyer,
            open_interest=1_000 + index,
            known_at=OPENING - timedelta(minutes=12),
            member_sha256=_digest(f"oi:{ticker}"),
        )
        for index, (underlyer, _kind, ticker, _spot) in enumerate(
            contract_specs,
            start=1,
        )
    )
    reference = OpeningReferenceEvidence(
        state_id=uuid5(NAMESPACE_URL, "c5-reference-state"),
        state_sha256=_digest("reference-state"),
        reference_generation=1,
        session_date=SESSION_DATE,
        known_at=OPENING - timedelta(minutes=12),
        valid_until=OPENING + timedelta(hours=1),
        reference_policy_sha256=_digest("reference-policy"),
        contract_scope_sha256=_digest("contract-scope"),
        open_interest_sha256=canonical_sha256(
            tuple(row.member_sha256 for row in reference_members)
        ),
        open_interest=reference_members,
    )
    contracts = tuple(
        ProspectiveOpeningContract(
            contract_id=reference_member.contract_id,
            catalog_version_id=30_000 + index,
            contract_ticker=ticker,
            underlyer=underlyer,
            expiration_date=SESSION_DATE,
            contract_type=kind,
            strike=spot,
            spot=spot,
            absolute_moneyness=Decimal("0"),
            day_volume=500 + index,
            open_interest=reference_member.open_interest,
            catalog_payload_sha256=_digest(f"catalog:{ticker}"),
            snapshot_payload_sha256=_digest(f"snapshot:{ticker}"),
            open_interest_member_sha256=(
                reference_member.member_sha256
            ),
        )
        for index, (
            (underlyer, kind, ticker, spot),
            reference_member,
        ) in enumerate(
            zip(contract_specs, reference_members, strict=True),
            start=1,
        )
    )
    admission = _opening_admission(
        status=OpeningAdmissionStatus.READY,
        session_date=SESSION_DATE,
        prepared_at=OPENING - timedelta(minutes=10),
        capture_boundary=OPENING,
        pre_session_selection_id=uuid5(
            NAMESPACE_URL,
            "c5-pre-session-selection",
        ),
        pre_session_selection_sha256=_digest(
            "pre-session-selection"
        ),
        source_evidence_sha256=_digest("selection-source"),
        reference_evidence_sha256=reference.sha256,
        contracts=contracts,
    )
    instruments = {
        contract.contract_ticker: 40_000 + index
        for index, contract in enumerate(contracts, start=1)
    }
    slot_set = SlotSetVersion(
        cohort_id=options_cohort_id,
        version=1,
        kind=SlotSetKind.PRE_SESSION,
        effective_at=OPENING - timedelta(minutes=9),
        policy_sha256=admission.policy_sha256,
        members=tuple(
            SlotSetMember(
                instrument_id=instruments[
                    contract.contract_ticker
                ],
                channel=channel,
                purpose=SlotSetPurpose.PRIMARY,
                priority=index,
                catalog_version_id=contract.catalog_version_id,
            )
            for index, contract in enumerate(contracts, start=1)
            for channel in (
                MarketChannel.QUOTE,
                MarketChannel.TRADE,
            )
        ),
    )
    readiness = build_opening_subscription_readiness(
        admission,
        slot_set,
        instrument_id_by_contract_ticker=instruments,
        journal_generation=uuid5(
            NAMESPACE_URL,
            "c5-journal-generation",
        ),
        journal_identity_sha256=_digest("journal-identity"),
        ready_at=OPENING - timedelta(minutes=5),
    )
    authorization_reference = "C5-EXACT-FOUR-SESSION"
    session = build_session_acquisition_authorization(
        admission,
        readiness,
        ProviderBudgetScope(
            account_alias="MASSIVE_ADVANCED",
            session_date=SESSION_DATE,
            authorization_reference=authorization_reference,
            provider_authorization_reference="c5-provider-grant",
            expected_cost="provider-free fixture",
            request_ceiling=2,
            decoded_byte_ceiling=100_000_000,
            lanes=ADVANCED_OPTIONS_REST_LANES,
            authorized_until=OPENING + timedelta(minutes=30),
        ),
        AdvancedOptionsSocketBudget(
            stream_sessions=1,
            connection_attempts=2,
            reconnects=1,
            decoded_messages=100_000,
            decoded_bytes=100_000_000,
        ),
        authorization_reference=authorization_reference,
        prepared_at=OPENING - timedelta(minutes=2),
        acquisition_start=OPENING - timedelta(minutes=1),
        acquisition_stop=OPENING + timedelta(minutes=30),
    )
    return admission, readiness, session, reference


def _seed_session(
    cursor,
    *,
    stale_catalog: bool = False,
    extra_equity_tickers: tuple[str, ...] = (),
    options_ready: bool = True,
    materialize_desired_snapshot: bool = True,
    canonical_equity: bool = False,
):
    desired_cohort_id = insert_cohort(
        cursor,
        OPENING - timedelta(minutes=20),
    )
    options_cohort_id = insert_cohort(
        cursor,
        OPENING - timedelta(minutes=10),
    )
    cursor.execute(
        "SELECT manifest_id::text FROM rt.cohorts WHERE cohort_id=%s",
        (str(desired_cohort_id),),
    )
    desired_manifest_id = UUID(str(cursor.fetchone()[0]))
    cursor.execute(
        "SELECT manifest_id::text FROM rt.cohorts WHERE cohort_id=%s",
        (str(options_cohort_id),),
    )
    options_manifest_id = UUID(str(cursor.fetchone()[0]))

    security_ids = {}
    security_revision_ids = {}
    equity_instrument_ids = {}
    equity_tickers = (
        *EQUITY_VALIDATION_UNIVERSE.equity_tickers,
        *extra_equity_tickers,
    )
    for ticker in equity_tickers:
        security_id = _security_id(ticker)
        security_revision_id = uuid4()
        security_ids[ticker] = security_id
        security_revision_ids[ticker] = security_revision_id
        cursor.execute(
            "INSERT INTO ref.securities ("
            "security_id,asset_type,source_origin,created_at"
            ") VALUES (%s,%s,'c5-fixture',%s)",
            (
                str(security_id),
                "ETF" if ticker in {"SPY", "QQQ", "IWM"} else "STOCK",
                OPENING - timedelta(days=30),
            ),
        )
        cursor.execute(
            "INSERT INTO ref.security_revisions ("
            "revision_id,security_id,ticker,exchange,calendar_id,"
            "effective_from,observed_at,payload_sha256,"
            "security_type,active"
            ") VALUES (%s,%s,%s,'XNYS','XNYS',%s,%s,%s,%s,true)",
            (
                str(security_revision_id),
                str(security_id),
                ticker,
                OPENING - timedelta(days=30),
                OPENING - timedelta(days=30),
                _digest(f"security:{ticker}"),
                "ETF" if ticker in {"SPY", "QQQ", "IWM"} else "CS",
            ),
        )
        cursor.execute(
            "INSERT INTO rt.instruments (asset_class,security_id) "
            "VALUES ('EQUITY',%s) RETURNING instrument_id",
            (str(security_id),),
        )
        equity_instrument_ids[ticker] = int(cursor.fetchone()[0])
    for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers:
        cursor.execute(
            "INSERT INTO rt.subscription_members ("
            "manifest_id,instrument_id,channel,priority,purpose,"
            "option_underlying) VALUES (%s,%s,'AM',1,'PRIMARY',false)",
            (
                str(desired_manifest_id),
                equity_instrument_ids[ticker],
            ),
        )

    if canonical_equity:
        equity = build_advanced_equity_control_plane_plan(
            session_date=SESSION_DATE,
            security_ids=security_ids,
            entitlement_evidence_sha256=_digest("canonical-equity-entitlement"),
            authorization_reference="c8-canonical-equity",
            checked_at=OPENING - timedelta(minutes=30),
            expires_at=OptionExchangeCalendar("XNYS").session_close(
                SESSION_DATE
            ) + timedelta(minutes=30),
            schema_sha256=_digest("canonical-schema"),
            security_policy_sha256=_digest("canonical-security"),
        )
        persist_advanced_equity_control_plane_cursor(cursor, equity)
        desired_cohort_id = equity.cohort_id
        desired_manifest_id = equity.manifest_id

    revision_id = uuid5(NAMESPACE_URL, "c5-desired-revision")
    recorded_at = OPENING - timedelta(minutes=30)
    cursor.execute(
        "INSERT INTO rt.realtime_desired_revisions ("
        "revision_id,revision_sha256,effective_session_date,"
        "source_cohort_id,selection_policy_reference,"
        "selection_policy_sha256,enabled,recorded_at"
        ") VALUES (%s,%s,%s,%s,'c5-exact-target9-four',%s,true,%s)",
        (
            str(revision_id),
            _digest("desired-revision"),
            SESSION_DATE,
            str(desired_cohort_id),
            _digest("selection-policy"),
            recorded_at,
        ),
    )
    members = [
        (ticker, "EQUITY")
        for ticker in equity_tickers
    ]
    members.extend((ticker, "OPTION_UNDERLYING") for ticker in ("QQQ", "SPY"))
    for priority, (ticker, kind) in enumerate(members, start=1):
        readiness_status = (
            "WARMING"
            if kind == "OPTION_UNDERLYING" and not options_ready
            else "READY"
        )
        intent_id = desired_member_intent_id(
            revision_id,
            ticker,
            DesiredMemberKind(kind),
        )
        cursor.execute(
            "INSERT INTO rt.realtime_desired_members ("
            "revision_id,security_id,ticker,member_kind,priority,enabled"
            ") VALUES (%s,%s,%s,%s,%s,true)",
            (
                str(revision_id),
                str(security_ids[ticker]),
                ticker,
                kind,
                priority,
            ),
        )
        cursor.execute(
            "INSERT INTO rt.realtime_desired_member_readiness ("
            "revision_id,security_id,member_kind,status,checked_at"
            ") VALUES (%s,%s,%s,%s,%s)",
            (
                str(revision_id),
                str(security_ids[ticker]),
                kind,
                readiness_status,
                recorded_at,
            ),
        )
        cursor.execute(
            "INSERT INTO rt.realtime_desired_member_intents ("
            "intent_id,revision_id,ticker,member_kind,priority,"
            "enabled,requested_at"
            ") VALUES (%s,%s,%s,%s,%s,true,%s)",
            (
                str(intent_id),
                str(revision_id),
                ticker,
                kind,
                priority,
                recorded_at,
            ),
        )
        cursor.execute(
            "INSERT INTO rt.realtime_desired_intent_readiness ("
            "intent_id,status,checked_at"
            ") VALUES (%s,%s,%s)",
            (str(intent_id), readiness_status, recorded_at),
        )
        cursor.execute(
            "INSERT INTO rt.realtime_desired_member_bindings ("
            "intent_id,revision_id,security_id,security_revision_id,"
            "instrument_id,identity_sha256,source_lineage,resolved_at"
            ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                str(intent_id),
                str(revision_id),
                str(security_ids[ticker]),
                str(security_revision_ids[ticker]),
                equity_instrument_ids[ticker],
                _digest(f"identity:{ticker}:{kind}"),
                Json({"fixture": "c5"}),
                recorded_at,
            ),
        )
    cursor.execute(
        "INSERT INTO rt.realtime_desired_current ("
        "scope_key,revision_id,effective_session_date,"
        "revision_recorded_at,selected_at"
        ") VALUES ('ADVANCED_REALTIME',%s,%s,%s,%s)",
        (
            str(revision_id),
            SESSION_DATE,
            recorded_at,
            recorded_at,
        ),
    )
    if materialize_desired_snapshot:
        cursor.execute(
            "INSERT INTO rt.realtime_desired_session_snapshots ("
            "revision_id,session_date,manifest_id,cohort_id,"
            "snapshot_sha256,materialized_at"
            ") VALUES (%s,%s,%s,%s,%s,%s)",
            (
                str(revision_id),
                SESSION_DATE,
                str(desired_manifest_id),
                str(desired_cohort_id),
                _digest("desired-snapshot"),
                recorded_at,
            ),
        )

    admission, readiness, session, reference = _authority_fixture(
        options_cohort_id
    )
    for contract_index, (
        instrument_id,
        contract_id,
        contract_ticker,
        catalog_version_id,
        _channels,
    ) in enumerate(readiness.slots):
        contract = next(
            row
            for row in admission.contracts
            if row.contract_ticker == contract_ticker
        )
        cursor.execute(
            "INSERT INTO ref.option_contracts ("
            "contract_id,underlying_security_id,provider,"
            "contract_ticker,first_observed_at,source_origin"
            ") VALUES (%s,%s,'massive',%s,%s,'c5-fixture')",
            (
                contract_id,
                str(security_ids[contract.underlyer]),
                contract_ticker,
                admission.prepared_at - timedelta(days=2),
            ),
        )
        cursor.execute(
            "INSERT INTO ref.option_contract_versions ("
            "version_id,contract_id,call_put,expiration,strike,"
            "exercise_style,multiplier,deliverables,eligibility,"
            "effective_from,observed_at,payload_sha256"
            ") VALUES (%s,%s,%s,%s,%s,'AMERICAN',100,'[]'::jsonb,"
            "'VALIDATED_ACTIVE',%s,%s,%s)",
            (
                catalog_version_id,
                contract_id,
                contract.contract_type[0],
                contract.expiration_date,
                contract.strike,
                admission.prepared_at - timedelta(days=2),
                admission.prepared_at - timedelta(days=2),
                (
                    _digest("stale-catalog")
                    if stale_catalog and contract_index == 0
                    else contract.catalog_payload_sha256
                ),
            ),
        )
        cursor.execute(
            "INSERT INTO rt.instruments ("
            "instrument_id,asset_class,contract_id"
            ") VALUES (%s,'OPTION',%s)",
            (instrument_id, contract_id),
        )
    cursor.execute(
        "INSERT INTO rt.slot_set_versions ("
        "cohort_id,version,kind,effective_at,policy_sha256,"
        "member_sha256,primary_contract_count,reserve_contract_count"
        ") VALUES (%s,1,'PRE_SESSION',%s,%s,%s,4,0)",
        (
            str(options_cohort_id),
            readiness.ready_at,
            readiness.slot_set_policy_sha256,
            readiness.slot_set_member_sha256,
        ),
    )
    for priority, (
        instrument_id,
        _contract_id,
        _ticker,
        catalog_version_id,
        channels,
    ) in enumerate(readiness.slots, start=1):
        for channel in channels:
            cursor.execute(
                "INSERT INTO rt.slot_set_members ("
                "cohort_id,version,instrument_id,channel,purpose,"
                "priority,catalog_version_id"
                ") VALUES (%s,1,%s,%s,'PRIMARY',%s,%s)",
                (
                    str(options_cohort_id),
                    instrument_id,
                    channel,
                    priority,
                    catalog_version_id,
                ),
            )
    cursor.execute(
        "INSERT INTO rt.slot_set_current ("
        "cohort_id,version,activated_at"
        ") VALUES (%s,1,%s)",
        (str(options_cohort_id), readiness.ready_at),
    )

    plan_id = uuid5(NAMESPACE_URL, "c5-options-plan")
    cursor.execute(
        "INSERT INTO rt.service_plans ("
        "plan_id,cohort_id,session_date,stage_catalog_version,"
        "enabled_stages,config_sha256,deadline,scope_reference"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,'c5-options-session')",
        (
            str(plan_id),
            str(options_cohort_id),
            SESSION_DATE,
            STAGE_CATALOG_VERSION_V5,
            ["OPTIONS_ACQUIRE", "OPTIONS_FLOW", "OPTIONS_INPUT_GATE"],
            _digest("options-plan"),
            session.acquisition_stop,
        ),
    )
    equity_plan_id = uuid5(NAMESPACE_URL, "c5-equity-plan")
    cursor.execute(
        "INSERT INTO rt.service_plans ("
        "plan_id,cohort_id,session_date,stage_catalog_version,"
        "enabled_stages,config_sha256,deadline,scope_reference"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,'c5-equity-session')",
        (
            str(equity_plan_id),
            str(desired_cohort_id),
            SESSION_DATE,
            STAGE_CATALOG_VERSION_V5,
            ["EQUITY_MATERIALIZE"],
            _digest("equity-plan"),
            session.acquisition_stop,
        ),
    )

    cursor.execute(
        "SELECT manifest_sha256 FROM rt.subscription_manifests "
        "WHERE manifest_id=%s",
        (str(options_manifest_id),),
    )
    options_manifest_sha256 = str(cursor.fetchone()[0])
    flow_policy_sha256 = _digest("flow-policy")
    cursor.execute(
        "INSERT INTO rt.option_flow_policies ("
        "policy_sha256,policy_version,policy_document,"
        "provider_contract_sha256,source_path"
        ") VALUES (%s,%s,%s,%s,'c5-sealed-fixture')",
        (
            flow_policy_sha256,
            f"c5-flow-policy-{uuid4()}",
            Json({"contract": "c5-flow-policy"}),
            _digest("flow-provider-contract"),
        ),
    )
    cursor.execute(
        "INSERT INTO rt.option_flow_contract_scopes ("
        "contract_scope_sha256,scope_version,session_date,"
        "manifest_sha256,slot_sha256,advanced_flow_policy_sha256"
        ") VALUES (%s,'advanced-options-contract-scope-v1',%s,%s,%s,%s)",
        (
            reference.contract_scope_sha256,
            SESSION_DATE,
            options_manifest_sha256,
            readiness.slot_set_member_sha256,
            flow_policy_sha256,
        ),
    )
    cursor.execute(
        "INSERT INTO rt.advanced_options_reference_states ("
        "state_id,state_sha256,state_version,reference_generation,"
        "session_date,known_at,valid_until,reference_policy_sha256,"
        "contract_scope_sha256,underlyings,member_count,state_document"
        ") VALUES (%s,%s,'advanced-options-reference-state-v1',"
        "%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            str(reference.state_id),
            reference.state_sha256,
            reference.reference_generation,
            reference.session_date,
            reference.known_at,
            reference.valid_until,
            reference.reference_policy_sha256,
            reference.contract_scope_sha256,
            ["QQQ", "SPY"],
            len(reference.open_interest),
            Json({"contract": "c5-sealed-reference-fixture"}),
        ),
    )
    for row in reference.open_interest:
        cursor.execute(
            "INSERT INTO rt.advanced_options_reference_state_members ("
            "state_id,member_kind,member_key,member_sha256,"
            "availability,member_document"
            ") VALUES (%s,'OPEN_INTEREST',%s,%s,'AVAILABLE',%s)",
            (
                str(reference.state_id),
                str(row.contract_id),
                row.member_sha256,
                Json({
                    "contract_id": row.contract_id,
                    "contract_ticker": row.contract_ticker,
                    "underlyer": row.underlyer,
                    "open_interest": row.open_interest,
                }),
            ),
        )

    cursor.execute(
        "SELECT manifest_sha256 FROM rt.subscription_manifests "
        "WHERE manifest_id=%s",
        (str(desired_manifest_id),),
    )
    source_manifest_sha256 = str(cursor.fetchone()[0])
    previous_session = OptionExchangeCalendar().previous_session_date(
        SESSION_DATE
    )
    history_evidence = {
        "schema": "advanced-options-v3-history-evidence-v1",
        "session_date": SESSION_DATE.isoformat(),
        "known_at_utc": admission.prepared_at.isoformat(),
        "readiness": {
            "status": "READY",
            "required_through_session": previous_session.isoformat(),
            "latest_covered_session": previous_session.isoformat(),
        },
    }
    history_evidence["evidence_sha256"] = canonical_sha256(
        history_evidence
    )
    history_binding = build_advanced_options_history_binding(
        target_cohort_id=options_cohort_id,
        source_cohort_id=desired_cohort_id,
        history_before=OptionExchangeCalendar().session_open(
            SESSION_DATE
        ),
        source_manifest_sha256=source_manifest_sha256,
        authorization_reference="c5-history",
        evidence_sha256=history_evidence["evidence_sha256"],
    )
    cursor.execute(
        "INSERT INTO rt.cohort_history_bindings ("
        "target_cohort_id,source_cohort_id,source_provider_tier,"
        "history_before,source_manifest_sha256,"
        "authorization_reference,binding_sha256"
        ") VALUES (%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s)",
        (
            str(options_cohort_id),
            str(desired_cohort_id),
            history_binding.history_before,
            history_binding.source_manifest_sha256,
            history_binding.authorization_reference,
            history_binding.binding_sha256,
        ),
    )

    sealed_selection = _sealed_selection(admission)
    nonce = uuid5(NAMESPACE_URL, "c5-options-package")
    prefix = (
        f"advanced-options:{options_cohort_id}:"
        f"{SESSION_DATE.isoformat()}"
    )
    package = {
        "schema": "advanced-options-v3-continuous-session-v1",
        "validation_mode": "DEVELOPMENT_DIAGNOSTIC",
        "package_nonce": str(nonce),
        "window": {
            "session_date": SESSION_DATE.isoformat(),
            "start_utc": session.acquisition_start.isoformat(),
            "stop_utc": session.acquisition_stop.isoformat(),
        },
        "cohort": {
            "cohort_id": str(options_cohort_id),
            "underlyings": ["QQQ", "SPY"],
        },
        "orchestration": {
            "plan_id": str(plan_id),
            "catalog_version": STAGE_CATALOG_VERSION_V5,
        },
        "owners": {
            "processes": [
                {
                    "kind": "INGRESS",
                    "owner_name": "advanced-options-ingress-v1",
                    "lease_resource_key": "massive:options:websocket",
                },
                {
                    "kind": "MARKET_STATE",
                    "owner_name": "market-state-materializer-v1",
                    "lease_resource_key": f"{prefix}:market-state",
                },
                {
                    "kind": "FLOW",
                    "owner_name": "option_flow_stream_v1",
                    "lease_resource_key": f"{prefix}:flow",
                },
                {
                    "kind": "WINDOW",
                    "owner_name": "advanced-options-window-v1",
                    "lease_resource_key": f"{prefix}:window",
                },
            ],
        },
        "equity_input_binding": {
            "symbols": list(EQUITY_VALIDATION_UNIVERSE.equity_tickers),
        },
        "v3_pre_session_authority": {
            "source_capture_semantic_sha256": sealed_selection[
                "semantic_sha256"
            ],
        },
        "v3_history_binding": {
            "binding": advanced_options_history_binding_payload(
                history_binding
            ),
            "evidence": history_evidence,
        },
        "v3_authorities": {
            "session_acquisition": (
                session_acquisition_authorization_payload(session)
            ),
        },
        "v3_admitted_contracts": [
            row.contract_ticker for row in admission.contracts
        ],
    }
    package["package_sha256"] = canonical_sha256(package)
    acquisition = build_acquisition_binding(
        source_sha256=sealed_selection["semantic_sha256"],
        backend_image_id=f"sha256:{_digest('backend-image')}",
        authority=session,
        contract_tickers=tuple(
            row.contract_ticker for row in admission.contracts
        ),
        connection_attempt_ceiling=3,
        decoded_message_ceiling=100_000,
        decoded_byte_ceiling=32 * 1024 * 1024,
    )
    return {
        "desired_cohort_id": desired_cohort_id,
        "desired_manifest_id": desired_manifest_id,
        "options_cohort_id": options_cohort_id,
        "options_manifest_id": options_manifest_id,
        "plan_id": plan_id,
        "equity_plan_id": equity_plan_id,
        "authority_bundle": AdvancedOptionsAuthorityBundle(
            session,
            admission,
            readiness,
        ),
        "reference": reference,
        "sealed_selection": sealed_selection,
        "package": package,
        "acquisition": acquisition,
        "history_binding": history_binding,
    }


def _seed_c8_equity_history(
    cursor,
    seed,
    *,
    underlyings=("QQQ", "SPY"),
):
    calendar = OptionExchangeCalendar("XNYS")
    sessions = calendar.trailing_sessions_before(SESSION_DATE, 31)
    cursor.execute(
        "SELECT intent.ticker,binding.security_id::text,"
        "binding.instrument_id "
        "FROM rt.realtime_desired_member_intents AS intent "
        "JOIN rt.realtime_desired_member_bindings AS binding "
        "USING(intent_id) "
        "WHERE intent.ticker=ANY(%s) "
        "AND intent.member_kind='EQUITY'",
        (list(underlyings),),
    )
    rows = tuple(cursor.fetchall())
    instruments = {
        str(row[0]): (str(row[1]), int(row[2]))
        for row in rows
    }
    generation = uuid4()
    stream_session = uuid4()
    cursor.execute(
        "INSERT INTO rt.stream_sessions ("
        "session_id,provider,feed,manifest_id,owner_id,owner_epoch,"
        "started_at,ended_at,status"
        ") VALUES (%s,'c8-provider-free','STOCKS',%s,%s,1,%s,%s,"
        "'STOPPED')",
        (
            str(stream_session),
            str(seed["desired_manifest_id"]),
            str(uuid4()),
            calendar.session_open(sessions[0]),
            calendar.session_close(sessions[-1]),
        ),
    )
    source_offset = 1
    for session in sessions:
        opened = calendar.session_open(session)
        closed = calendar.session_close(session)
        cursor.execute(
            "SELECT rt.ensure_partitions('rt.bar_revisions',%s)",
            (session,),
        )
        for ticker, (security_id, instrument_id) in instruments.items():
            bases = (
                ("ADJUSTED",)
                if session != sessions[-1]
                else ("ADJUSTED", "RAW_UNADJUSTED")
            )
            for basis in bases:
                revision_id = uuid4()
                payload_sha256 = _digest(
                    f"c8-history:{ticker}:{session}:{basis}"
                )
                cursor.execute(
                    "INSERT INTO rt.bar_catalog ("
                    "bar_revision_id,cohort_id,provider_tier,"
                    "instrument_id,interval,bar_start,bar_end,"
                    "observed_at,session_date"
                    ") VALUES (%s,%s,'MASSIVE_ADVANCED',%s,'1d',"
                    "%s,%s,%s,%s)",
                    (
                        str(revision_id),
                        str(seed["desired_cohort_id"]),
                        instrument_id,
                        opened,
                        closed,
                        closed,
                        session,
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.bar_revisions ("
                    "bar_revision_id,cohort_id,provider_tier,"
                    "security_id,instrument_id,ticker,interval,"
                    "session_date,session_scope,adjustment_basis,"
                    "bar_start,bar_end,open_price,high_price,"
                    "low_price,close_price,volume,vwap,"
                    "transaction_count,source_kind,"
                    "availability_mode,is_final,observed_at,"
                    "replay_available_at,journal_generation,"
                    "stream_session_id,source_offset,payload_sha256,"
                    "created_at"
                    ") VALUES (%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,"
                    "'1d',%s,'RTH',%s,%s,%s,500,501,499,500,"
                    "1000000,500,100,'REALTIME_STREAM',"
                    "'HISTORICAL_RECONSTRUCTED',true,%s,%s,%s,%s,"
                    "%s,%s,%s)",
                    (
                        str(revision_id),
                        str(seed["desired_cohort_id"]),
                        security_id,
                        instrument_id,
                        ticker,
                        session,
                        basis,
                        opened,
                        closed,
                        closed,
                        closed,
                        str(generation),
                        str(stream_session),
                        source_offset,
                        payload_sha256,
                        closed,
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.bar_current ("
                    "cohort_id,provider_tier,instrument_id,interval,"
                    "bar_start,session_scope,adjustment_basis,"
                    "source_kind,bar_revision_id,bar_end,observed_at,"
                    "updated_at"
                    ") VALUES (%s,'MASSIVE_ADVANCED',%s,'1d',%s,"
                    "'RTH',%s,'REALTIME_STREAM',%s,%s,%s,%s)",
                    (
                        str(seed["desired_cohort_id"]),
                        instrument_id,
                        opened,
                        basis,
                        str(revision_id),
                        closed,
                        closed,
                        closed,
                    ),
                )
                source_offset += 1


def _add_desired_option_underlying(cursor, ticker):
    cursor.execute(
        "SELECT current.revision_id::text,binding.security_id::text,"
        "binding.security_revision_id::text,binding.instrument_id "
        "FROM rt.realtime_desired_current AS current "
        "JOIN rt.realtime_desired_member_intents AS intent "
        "USING(revision_id) "
        "JOIN rt.realtime_desired_member_bindings AS binding "
        "USING(intent_id) "
        "WHERE current.scope_key='ADVANCED_REALTIME' "
        "AND intent.ticker=%s "
        "AND intent.member_kind='EQUITY'",
        (ticker,),
    )
    row = cursor.fetchone()
    revision_id = UUID(str(row[0]))
    security_id = UUID(str(row[1]))
    security_revision_id = UUID(str(row[2]))
    instrument_id = int(row[3])
    intent_id = desired_member_intent_id(
        revision_id,
        ticker,
        DesiredMemberKind.OPTION_UNDERLYING,
    )
    cursor.execute(
        "SELECT COALESCE(MAX(priority),0)+1 "
        "FROM rt.realtime_desired_member_intents "
        "WHERE revision_id=%s",
        (str(revision_id),),
    )
    priority = int(cursor.fetchone()[0])
    cursor.execute(
        "INSERT INTO rt.realtime_desired_members ("
        "revision_id,security_id,ticker,member_kind,priority,enabled"
        ") VALUES (%s,%s,%s,'OPTION_UNDERLYING',%s,true)",
        (
            str(revision_id),
            str(security_id),
            ticker,
            priority,
        ),
    )
    cursor.execute(
        "INSERT INTO rt.realtime_desired_member_readiness ("
        "revision_id,security_id,member_kind,status,checked_at"
        ") SELECT %s,%s,'OPTION_UNDERLYING','WARMING',recorded_at "
        "FROM rt.realtime_desired_revisions WHERE revision_id=%s",
        (str(revision_id), str(security_id), str(revision_id)),
    )
    cursor.execute(
        "INSERT INTO rt.realtime_desired_member_intents ("
        "intent_id,revision_id,ticker,member_kind,priority,enabled,"
        "requested_at"
        ") SELECT %s,%s,%s,'OPTION_UNDERLYING',%s,true,recorded_at "
        "FROM rt.realtime_desired_revisions WHERE revision_id=%s",
        (
            str(intent_id),
            str(revision_id),
            ticker,
            priority,
            str(revision_id),
        ),
    )
    cursor.execute(
        "INSERT INTO rt.realtime_desired_intent_readiness ("
        "intent_id,status,checked_at"
        ") SELECT %s,'WARMING',recorded_at "
        "FROM rt.realtime_desired_revisions WHERE revision_id=%s",
        (str(intent_id), str(revision_id)),
    )
    cursor.execute(
        "INSERT INTO rt.realtime_desired_member_bindings ("
        "intent_id,revision_id,security_id,security_revision_id,"
        "instrument_id,identity_sha256,source_lineage,resolved_at"
        ") SELECT %s,%s,%s,%s,%s,%s,%s,recorded_at "
        "FROM rt.realtime_desired_revisions WHERE revision_id=%s",
        (
            str(intent_id),
            str(revision_id),
            str(security_id),
            str(security_revision_id),
            instrument_id,
            _digest(f"c8-option-identity:{ticker}"),
            Json({"fixture": "c8-generalized"}),
            str(revision_id),
        ),
    )


def _seed_complete_c8_reference(
    cursor,
    seed,
    *,
    underlyings=("QQQ", "SPY"),
    snapshots=None,
):
    calendar = OptionExchangeCalendar("XNYS")
    session_open = calendar.session_open(SESSION_DATE)
    known_at = session_open - timedelta(minutes=2)
    sessions = calendar.trailing_sessions_before(SESSION_DATE, 31)
    cursor.execute(
        "SELECT intent.ticker,binding.security_id::text "
        "FROM rt.realtime_desired_member_intents AS intent "
        "JOIN rt.realtime_desired_member_bindings AS binding "
        "USING(intent_id) WHERE intent.ticker=ANY(%s) "
        "AND intent.member_kind='EQUITY'",
        (list(underlyings),),
    )
    security_ids = {
        str(row[0]): UUID(str(row[1]))
        for row in cursor.fetchall()
    }
    bars_by_ticker = {}
    volatility = []
    valuations = []
    metrics = []
    for ticker in underlyings:
        bars = tuple(
            AdvancedOptionsAdjustedDailyBarIdentity(
                bar_revision_id=uuid5(
                    NAMESPACE_URL,
                    f"c8-reference-bar:{ticker}:{session}",
                ),
                security_id=security_ids[ticker],
                ticker=ticker,
                session_date=session,
                adjusted_close=Decimal("500") + index,
                market_close_at=calendar.session_close(session),
                known_at=min(
                    known_at,
                    calendar.session_close(session)
                    + timedelta(minutes=1),
                ),
                adjustment_lineage_sha256=_digest(
                    f"c8-lineage:{ticker}:{session}"
                ),
                finality_sha256=_digest(
                    f"c8-finality:{ticker}:{session}"
                ),
                payload_sha256=_digest(
                    f"c8-payload:{ticker}:{session}"
                ),
            )
            for index, session in enumerate(sessions)
        )
        bars_by_ticker[ticker] = bars
        volatility.extend((
            build_historical_volatility_fact(
                security_id=security_ids[ticker],
                ticker=ticker,
                window_sessions=20,
                market_cutoff=bars[-1].market_close_at,
                known_at=known_at,
                bars=bars[-21:],
            ),
            build_historical_volatility_fact(
                security_id=security_ids[ticker],
                ticker=ticker,
                window_sessions=30,
                market_cutoff=bars[-1].market_close_at,
                known_at=known_at,
                bars=bars,
            ),
        ))
        expiration = SESSION_DATE + timedelta(days=30)
        action = AdvancedOptionsCorporateActionIdentity(
            action_id=uuid5(
                NAMESPACE_URL,
                f"c8-action:{ticker}",
            ),
            payload_sha256=_digest(f"c8-action:{ticker}"),
            observed_at=known_at - timedelta(minutes=4),
            received_at=known_at - timedelta(minutes=3),
        )
        coverage = AdvancedOptionsDividendCoverageReference(
            coverage_id=uuid5(
                NAMESPACE_URL,
                f"c8-dividend:{ticker}",
            ),
            security_id=security_ids[ticker],
            ticker=ticker,
            window_start=SESSION_DATE,
            window_end=expiration,
            status="AVAILABLE",
            observed_at=known_at - timedelta(minutes=4),
            received_at=known_at - timedelta(minutes=3),
            payload_sha256=_digest(f"c8-dividend:{ticker}"),
            response_sha256=_digest(
                f"c8-dividend-response:{ticker}"
            ),
            actions=(action,),
        )
        rates = tuple(
            AdvancedOptionsRateReference(
                observation_id=uuid5(
                    NAMESPACE_URL,
                    f"c8-rate:{ticker}:{tenor}",
                ),
                rate_date=sessions[-1],
                tenor_days=tenor,
                annual_rate=Decimal("0.04"),
                source="us_treasury_par_yield_v1",
                source_key=f"{sessions[-1]}:{tenor}d",
                observed_at=known_at - timedelta(minutes=5),
                received_at=known_at - timedelta(minutes=4),
                payload_sha256=_digest(
                    f"c8-rate:{ticker}:{tenor}"
                ),
            )
            for tenor in (30, 60)
        )
        valuations.append(AdvancedOptionsValuationReference(
            security_id=security_ids[ticker],
            ticker=ticker,
            valuation_date=SESSION_DATE,
            expiration_date=expiration,
            risk_free_rate=Decimal("0.04"),
            dividend_yield=Decimal("0.01"),
            rate_observations=rates,
            dividend_coverage=coverage,
            model_version="black-scholes-merton-v1",
            model_bundle_sha256=_digest(
                f"c8-model:{ticker}"
            ),
            valuation_policy_sha256=_digest(
                f"c8-valuation:{ticker}"
            ),
            known_at=known_at,
        ))
        metric_sessions = sessions[-30:]
        metrics.extend((
            AdvancedOptionsEquityMetric(
                security_id=security_ids[ticker],
                ticker=ticker,
                metric_kind=(
                    EquityMetricKind.SHORT_INTEREST_PERCENT
                ),
                as_of_session=sessions[-1],
                availability=ReferenceAvailability.AVAILABLE,
                known_at=known_at,
                value=Decimal("0.08"),
                source="provider-free",
                source_key=f"{ticker}:short-interest",
                observed_at=known_at - timedelta(minutes=3),
                received_at=known_at - timedelta(minutes=2),
                payload_sha256=_digest(
                    f"c8-short-interest:{ticker}"
                ),
            ),
            AdvancedOptionsEquityMetric(
                security_id=security_ids[ticker],
                ticker=ticker,
                metric_kind=EquityMetricKind.TOTAL_FLOAT,
                as_of_session=sessions[-1],
                availability=ReferenceAvailability.UNAVAILABLE,
                known_at=known_at,
                reason_codes=(
                    "REVIEWED_FLOAT_SOURCE_UNAVAILABLE",
                ),
            ),
            AdvancedOptionsEquityMetric(
                security_id=security_ids[ticker],
                ticker=ticker,
                metric_kind=EquityMetricKind.AVERAGE_DAILY_VOLUME_30,
                as_of_session=sessions[-1],
                availability=ReferenceAvailability.AVAILABLE,
                known_at=known_at,
                value=Decimal("1000000"),
                source="finalized_adjusted_daily_bars",
                source_key=f"{ticker}:ADV30",
                observed_at=known_at - timedelta(minutes=3),
                received_at=known_at - timedelta(minutes=2),
                payload_sha256=_digest(f"c8-adv30:{ticker}"),
                calculation_version="adv30-v1",
                member_sessions=metric_sessions,
                member_sha256=_digest(
                    f"c8-adv30-members:{ticker}"
                ),
            ),
        ))
    if snapshots is None:
        contract_inputs = tuple(
            (
                row.contract_id,
                row.contract_ticker,
                row.underlyer,
                row.open_interest,
            )
            for row in seed["authority_bundle"].admission.contracts
        )
    else:
        contract_inputs = []
        for ticker, raw in sorted(snapshots.items()):
            cursor.execute(
                "SELECT contract.contract_id "
                "FROM ref.option_contracts AS contract "
                "WHERE contract.contract_ticker=%s",
                (ticker,),
            )
            contract_inputs.append((
                int(cursor.fetchone()[0]),
                ticker,
                str(raw["underlying_asset"]["ticker"]),
                (
                    None
                    if raw.get("open_interest") is None
                    else int(raw["open_interest"])
                ),
            ))
        contract_inputs = tuple(contract_inputs)

    def open_interest_reference(
        contract_id,
        contract_ticker,
        underlyer,
        open_interest_value,
    ):
        if open_interest_value is None:
            return AdvancedOptionsOpenInterestReference(
                contract_id=contract_id,
                contract_ticker=contract_ticker,
                underlyer=underlyer,
                availability=ReferenceAvailability.UNAVAILABLE,
                known_at=known_at,
                reason_codes=("MISSING_PRIOR_OPEN_INTEREST",),
            )
        return AdvancedOptionsOpenInterestReference(
            contract_id=contract_id,
            contract_ticker=contract_ticker,
            underlyer=underlyer,
            availability=ReferenceAvailability.AVAILABLE,
            known_at=known_at,
            baseline_id=uuid5(
                NAMESPACE_URL,
                f"c8-reference-oi:{contract_ticker}",
            ),
            settlement_session=sessions[-1],
            observed_session=SESSION_DATE,
            open_interest=open_interest_value,
            source="provider-free",
            date_basis="PROVIDER_CHAIN_SNAPSHOT_PRIOR_SESSION",
            observed_at=known_at - timedelta(minutes=3),
            received_at=known_at - timedelta(minutes=2),
            payload_sha256=_digest(
                f"c8-reference-oi:{contract_ticker}"
            ),
        )
    open_interest = tuple(
        open_interest_reference(
            contract_id,
            contract_ticker,
            underlyer,
            open_interest_value,
        )
        for (
            contract_id,
            contract_ticker,
            underlyer,
            open_interest_value,
        ) in contract_inputs
    )
    cursor.execute(
        "SELECT COALESCE(MAX(reference_generation),0)+1 "
        "FROM rt.advanced_options_reference_states"
    )
    generation = int(cursor.fetchone()[0])
    state = build_advanced_options_reference_state(
        reference_generation=generation,
        session_date=SESSION_DATE,
        known_at=known_at,
        valid_until=calendar.session_close(SESSION_DATE),
        reference_policy_sha256=_digest(
            "c8-complete-reference-policy"
        ),
        data_membership_sha256=None,
        contract_scope_sha256=(
            seed["reference"].contract_scope_sha256
        ),
        underlyings=tuple(underlyings),
        open_interest=open_interest,
        historical_volatility=tuple(volatility),
        valuation_inputs=tuple(valuations),
        equity_metrics=tuple(metrics),
    )
    persist_advanced_options_reference_state_cursor(cursor, state)
    cursor.execute(
        "INSERT INTO rt.advanced_options_reference_session_freezes ("
        "session_date,state_id,first_cycle_id,consumed_at"
        ") VALUES (%s,%s,%s,%s)",
        (
            SESSION_DATE,
            str(state.state_id),
            str(uuid4()),
            known_at,
        ),
    )


def _provider(pool):
    return V2ConnectionProvider(
        {OWNER_POOL: pool},
        V2AccessConfig(allow_disposable=True),
    )


class _RecordingSerializationProvider:
    def __init__(self, provider, *, fail_first=False):
        self.provider = provider
        self.fail_first = fail_first
        self.calls = []

    @contextmanager
    def transaction(self, *args, **kwargs):
        self.calls.append((args, dict(kwargs)))
        if (
            self.fail_first
            and kwargs.get("isolation_level") == "SERIALIZABLE"
        ):
            self.fail_first = False
            raise errors.SerializationFailure(
                "injected activation serialization failure"
            )
        with self.provider.transaction(*args, **kwargs) as unit:
            yield unit


def _activate(pool, seed):
    repository = PostgresContinuousSessionStateRepository(
        _provider(pool),
        session_date=SESSION_DATE,
        head_poll_interval_seconds=0.001,
    )
    session_binding = repository.activate_session(
        package=seed["package"],
        authority_bundle=seed["authority_bundle"],
        acquisition_binding=seed["acquisition"],
        reference=seed["reference"],
        sealed_selection=seed["sealed_selection"],
        written_at=seed["authority_bundle"].session.prepared_at,
    )
    return repository, session_binding


def _seed_downstream_binding(
    cursor,
    seed,
    session_binding,
    *,
    sequence,
    previous_binding_sha256=None,
):
    finalized_cutoff = OPENING + timedelta(minutes=sequence * 5)
    receipt_created_at = finalized_cutoff + timedelta(seconds=30)
    options_cutoff = finalized_cutoff + timedelta(seconds=120)
    target_window_id = uuid5(
        NAMESPACE_URL,
        f"c5-options-window:{sequence}",
    )
    equity_window_id = uuid5(
        NAMESPACE_URL,
        f"c5-equity-window:{sequence}",
    )
    receipt_id = uuid5(
        NAMESPACE_URL,
        f"c5-equity-receipt:{sequence}",
    )
    cursor.execute(
        "INSERT INTO rt.window_runs ("
        "window_id,plan_id,cohort_id,provider_tier,window_key,"
        "release_at,cutoff,deadline,state"
        ") VALUES (%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,'COMPLETE')",
        (
            str(equity_window_id),
            str(seed["equity_plan_id"]),
            str(seed["desired_cohort_id"]),
            f"C5-E-{sequence}",
            finalized_cutoff,
            finalized_cutoff,
            finalized_cutoff + timedelta(minutes=2),
        ),
    )
    cursor.execute(
        "INSERT INTO rt.window_runs ("
        "window_id,plan_id,cohort_id,provider_tier,window_key,"
        "release_at,cutoff,deadline,state"
        ") VALUES (%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,'COMPLETE')",
        (
            str(target_window_id),
            str(seed["plan_id"]),
            str(seed["options_cohort_id"]),
            f"C5-O-{sequence}",
            options_cutoff,
            options_cutoff,
            options_cutoff + timedelta(minutes=2),
        ),
    )
    input_sha256 = _digest(f"equity-input:{sequence}")
    cursor.execute(
        "INSERT INTO rt.cycle_receipts ("
        "receipt_id,window_id,cohort_id,receipt_kind,cutoff,"
        "reference_generation,universe_sha256,input_sha256,"
        "build_id,created_at"
        ") VALUES (%s,%s,%s,'INPUT_READY',%s,1,%s,%s,%s,%s)",
        (
            str(receipt_id),
            str(equity_window_id),
            str(seed["desired_cohort_id"]),
            finalized_cutoff,
            _digest("equity-universe"),
            input_sha256,
            "c5-equity-build",
            receipt_created_at,
        ),
    )
    cursor.execute(
        "SELECT cohort.cohort_sha256,manifest.manifest_sha256,"
        "manifest.profile_id::text,profile.profile_sha256 "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
        "JOIN rt.capability_profiles AS profile USING(profile_id) "
        "WHERE cohort.cohort_id=%s",
        (str(seed["desired_cohort_id"]),),
    )
    source = cursor.fetchone()
    equity_binding_sha256 = _digest(f"equity-binding:{sequence}")
    cursor.execute(
        "INSERT INTO rt.advanced_options_equity_input_bindings ("
        "target_options_window_id,target_cohort_id,source_cohort_id,"
        "source_cohort_sha256,source_manifest_id,"
        "source_manifest_sha256,source_profile_id,"
        "source_profile_sha256,equity_window_id,equity_receipt_id,"
        "equity_cutoff,reference_generation,universe_sha256,"
        "input_sha256,build_id,maximum_age_seconds,"
        "authorization_reference,binding_sha256"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s,%s,%s,300,%s,%s)",
        (
            str(target_window_id),
            str(seed["options_cohort_id"]),
            str(seed["desired_cohort_id"]),
            str(source[0]),
            str(seed["desired_manifest_id"]),
            str(source[1]),
            str(source[2]),
            str(source[3]),
            str(equity_window_id),
            str(receipt_id),
            finalized_cutoff,
            _digest("equity-universe"),
            input_sha256,
            "c5-equity-input",
            f"c5-equity-authorization-{sequence}",
            equity_binding_sha256,
        ),
    )
    window_authorization_sha256 = _digest(
        f"window-authorization:{sequence}"
    )
    publication_reference = f"c5-publication-denied-{sequence}"
    cursor.execute(
        "INSERT INTO rt.advanced_options_window_cycle_authorizations ("
        "authorization_id,session_authorization_id,target_window_id,"
        "target_cohort_id,authorization_reference,prepared_at,"
        "history_binding_sha256,equity_binding_sha256,"
        "equity_receipt_id,equity_cutoff,"
        "equity_receipt_created_at,options_cutoff,"
        "age_policy_version,age_policy_sha256,maximum_age_seconds,"
        "evidence_sha256,build_id,"
        "publication_authorization_reference,"
        "authorization_sha256,authorization_document"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
        "'advanced-options-e-o-age-v1',%s,300,%s,%s,%s,%s,%s)",
        (
            str(uuid5(
                NAMESPACE_URL,
                f"c5-window-authorization:{sequence}",
            )),
            str(session_binding.session_authorization_id),
            str(target_window_id),
            str(seed["options_cohort_id"]),
            f"c5-window-authority-{sequence}",
            finalized_cutoff + timedelta(seconds=45),
            seed["history_binding"].binding_sha256,
            equity_binding_sha256,
            str(receipt_id),
            finalized_cutoff,
            receipt_created_at,
            options_cutoff,
            _digest("e-o-age-policy"),
            _digest(f"window-evidence:{sequence}"),
            "c5-window-build",
            publication_reference,
            window_authorization_sha256,
            Json({"fixture": "c5-window-authority"}),
        ),
    )
    production_binding = _production_binding(
        window_id=target_window_id,
        receipt_id=receipt_id,
        input_sha256=input_sha256,
        equity_cutoff=finalized_cutoff,
        options_cutoff=options_cutoff,
        history_sha256=seed["history_binding"].binding_sha256,
        equity_sha256=equity_binding_sha256,
    )
    binding = build_p8_downstream_binding(
        session_binding=session_binding,
        sequence=sequence,
        previous_binding_sha256=previous_binding_sha256,
        target_window_id=target_window_id,
        equity_receipt_id=receipt_id,
        equity_input_sha256=input_sha256,
        finalized_cutoff=finalized_cutoff,
        receipt_created_at=receipt_created_at,
        options_cutoff=options_cutoff,
        history_binding_sha256=(
            seed["history_binding"].binding_sha256
        ),
        equity_binding_sha256=equity_binding_sha256,
        options_activity_evidence_sha256=_digest(
            f"options-activity:{sequence}"
        ),
        window_cycle_authorization_sha256=(
            window_authorization_sha256
        ),
        publication_authorization_reference=publication_reference,
        production_binding=production_binding,
    )
    return binding


def test_session_activation_is_idempotent_typed_and_current_session():
    with disposable_v2_database(
        f"test_realtime_v2_c5_session_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(database_cursor)
            database_cursor.connection.commit()
        providers = [
            _RecordingSerializationProvider(
                _provider(pool),
                fail_first=index == 0,
            )
            for index in range(2)
        ]
        repositories = [
            PostgresContinuousSessionStateRepository(
                provider,
                session_date=SESSION_DATE,
            )
            for provider in providers
        ]
        with ThreadPoolExecutor(max_workers=2) as executor:
            activated = tuple(executor.map(
                lambda item: item[0].activate_session(
                    package=seed["package"],
                    authority_bundle=seed["authority_bundle"],
                    acquisition_binding=seed["acquisition"],
                    reference=seed["reference"],
                    sealed_selection=seed["sealed_selection"],
                    written_at=(
                        seed["authority_bundle"].session.prepared_at
                        + timedelta(minutes=item[1])
                    ),
                ),
                zip(repositories, (0, 7), strict=True),
            ))
        repository = repositories[0]
        first = activated[0]
        assert activated == (first, first)
        serializable_calls = [
            kwargs
            for provider in providers
            for _args, kwargs in provider.calls
            if kwargs.get("isolation_level") is not None
        ]
        assert len(serializable_calls) >= 3
        assert {
            call["isolation_level"]
            for call in serializable_calls
        } == {"SERIALIZABLE"}
        repeated = repository.activate_session(
            package=seed["package"],
            authority_bundle=seed["authority_bundle"],
            acquisition_binding=seed["acquisition"],
            reference=seed["reference"],
            sealed_selection=seed["sealed_selection"],
            written_at=(
                seed["authority_bundle"].session.prepared_at
                + timedelta(hours=1)
            ),
        )

        assert repeated == first
        assert repository.load_acquisition_binding() == seed["acquisition"]
        bundle = repository.load_execution_bundle()
        assert bundle["written_at_utc"] == (
            seed["authority_bundle"].session.prepared_at.isoformat()
        )
        assert repository.load_preopen_session_binding(bundle) == first
        with cursor(pool) as database_cursor:
            database_cursor.execute(
                "SELECT count(*) FROM "
                "rt.advanced_options_continuous_sessions"
            )
            assert database_cursor.fetchone()[0] == 1
            database_cursor.execute(
                "SELECT count(*) FROM "
                "rt.advanced_options_downstream_bindings"
            )
            assert database_cursor.fetchone()[0] == 0


class _AutomaticC8Executor:
    def __init__(self, pool, seed, *, fail_stage=None):
        self.repository = PostgresContinuousSessionStateRepository(
            _provider(pool),
            session_date=SESSION_DATE,
        )
        self.seed = seed
        self.fail_stage = fail_stage
        self.failed_once = False
        self.binding = None

    def provider_budget(self, _claim):
        return None

    def execute(self, claim, *, provider_request_ids):
        assert provider_request_ids == ()
        if (
            claim.stage == self.fail_stage
            and not self.failed_once
        ):
            self.failed_once = True
            raise AdvancedOptionsPreparationRetryable(
                f"fixture interruption at {claim.stage}"
            )
        if claim.stage == "SESSION_ACTIVATION":
            self.binding = self.repository.activate_session(
                package=self.seed["package"],
                authority_bundle=self.seed["authority_bundle"],
                acquisition_binding=self.seed["acquisition"],
                reference=self.seed["reference"],
                sealed_selection=self.seed["sealed_selection"],
                written_at=(
                    self.seed["authority_bundle"].session.prepared_at
                ),
            )
        return AdvancedOptionsStageResult(
            watermark={
                "stage": claim.stage,
                "attempt": claim.attempt,
            },
            evidence={
                "stage": claim.stage,
                "ticker": claim.ticker,
                "fixture": "owned-disposable-c8",
            },
            session_binding_id=(
                None if self.binding is None else self.binding.binding_id
            ),
        )


@pytest.mark.parametrize(
    "fail_stage",
    (
        None,
        "CATALOG_QUOTE",
        "REFERENCE_OI",
        "HISTORY",
        "CAPACITY_SHARDS",
        "SESSION_ACTIVATION",
    ),
)
def test_c8_lifecycle_automates_stage_recovery_and_activation(fail_stage):
    with disposable_v2_database(
        f"test_realtime_v2_c8_lifecycle_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(
                database_cursor,
                options_ready=False,
            )
            prepared = reconcile_advanced_options_lifecycle_cursor(
                database_cursor,
                target_session_date=SESSION_DATE,
                checked_at=(
                    seed["authority_bundle"].session.prepared_at
                ),
            )
            database_cursor.execute(
                "SELECT DISTINCT intent.member_kind "
                "FROM rt.advanced_options_preparation_work AS work "
                "JOIN rt.realtime_desired_member_intents AS intent "
                "USING(intent_id)"
            )
            scheduled_kinds = {
                str(row[0]) for row in database_cursor.fetchall()
            }
            database_cursor.execute(
                "SELECT count(*) "
                "FROM rt.advanced_options_preparation_work"
            )
            scheduled_count = int(database_cursor.fetchone()[0])
            database_cursor.connection.commit()

        assert prepared is not None
        assert prepared.status == "PREPARING"
        assert prepared.current_stage == "CATALOG_QUOTE"
        assert scheduled_kinds == {"OPTION_UNDERLYING"}
        assert scheduled_count == (
            len(prepared.option_underlyings) * 4 + 1
        )

        executor = _AutomaticC8Executor(
            pool,
            seed,
            fail_stage=fail_stage,
        )
        owner_id = uuid4()
        checked_at = seed["authority_bundle"].session.prepared_at
        reports = []
        for attempt in range(20):
            cycle_at = checked_at + timedelta(
                minutes=attempt * 10
            )
            report = run_advanced_options_preparation_cycle(
                _provider(pool),
                executor,
                owner_id=owner_id,
                checked_at=cycle_at,
                clock=lambda value=cycle_at: value,
            )
            reports.append(report)
            with cursor(pool) as database_cursor:
                active = reconcile_advanced_options_lifecycle_cursor(
                    database_cursor,
                    target_session_date=SESSION_DATE,
                    checked_at=cycle_at,
                )
                database_cursor.connection.commit()
            if active is not None and active.status == "ACTIVE":
                break
        else:
            with cursor(pool) as database_cursor:
                database_cursor.execute(
                    "SELECT last_error FROM "
                    "rt.advanced_options_preparation_sessions"
                )
                lifecycle_error = database_cursor.fetchone()[0]
            raise AssertionError(
                "C8 lifecycle did not activate: "
                f"reports={reports!r}, active={active!r}, "
                f"error={lifecycle_error!r}"
            )

        if fail_stage is not None:
            assert any(
                row["status"] == "RETRY"
                and row["stage"] == fail_stage
                for row in reports
            )
        assert executor.binding is not None

        with cursor(pool) as database_cursor:
            status_at = (
                seed["authority_bundle"].session.prepared_at
                + timedelta(hours=4)
            )
            active = reconcile_advanced_options_lifecycle_cursor(
                database_cursor,
                target_session_date=SESSION_DATE,
                checked_at=status_at,
            )
            summary = build_advanced_realtime_v3_status_cursor(
                database_cursor,
                checked_at=status_at,
            )
            database_cursor.execute(
                "SELECT count(*) FROM "
                "rt.advanced_options_preparation_evidence"
            )
            evidence_count = int(database_cursor.fetchone()[0])
            database_cursor.execute(
                "SELECT DISTINCT readiness.status "
                "FROM rt.realtime_desired_intent_readiness AS readiness "
                "JOIN rt.realtime_desired_member_intents AS intent "
                "USING(intent_id) "
                "WHERE intent.member_kind='OPTION_UNDERLYING'"
            )
            option_readiness = {
                str(row[0]) for row in database_cursor.fetchall()
            }
            database_cursor.execute(
                "SELECT count(*) FROM "
                "rt.realtime_equity_preparation_work"
            )
            equity_preparation_count = int(
                database_cursor.fetchone()[0]
            )
            database_cursor.connection.commit()

        assert active is not None
        assert active.status == "ACTIVE"
        assert active.current_stage == "COMPLETE"
        assert active.session_binding_id == executor.binding.binding_id
        assert evidence_count == scheduled_count
        assert option_readiness == {"READY"}
        assert equity_preparation_count == 0
        assert summary["current_processing"]["session_date"] == (
            SESSION_DATE.isoformat()
        )
        assert summary["future_preparation"]["records"][0][
            "status"
        ] == "ACTIVE"
        assert summary["physical_feeds"] == []
        assert summary["publication"] == {}


def _provider_free_selection_capture(seed, ticker):
    snapshots = {}
    for contract_ticker, raw in seed[
        "sealed_selection"
    ]["selected_snapshots"].items():
        if raw["underlying_asset"]["ticker"] != ticker:
            continue
        snapshots[contract_ticker] = dict(raw)
    if not snapshots:
        market_time = (
            seed["authority_bundle"].session.prepared_at
            - timedelta(seconds=1)
        )
        market_ns = int(
            market_time.timestamp() * 1_000_000_000
        )
        expiration = SESSION_DATE + timedelta(days=7)
        for option_type, side in (("call", "C"), ("put", "P")):
            contract_ticker = (
                f"O:{ticker}{expiration:%y%m%d}{side}00200000"
            )
            snapshots[contract_ticker] = {
                "details": {
                    "ticker": contract_ticker,
                    "contract_type": option_type,
                    "expiration_date": expiration.isoformat(),
                    "strike_price": "200",
                },
                "last_quote": {
                    "bid": 1,
                    "ask": 1.1,
                    "last_updated": market_ns,
                },
                "day": {"volume": 10_000},
                "open_interest": 5_000,
                "underlying_asset": {
                    "ticker": ticker,
                    "price": "200",
                    "last_updated": market_ns,
                    "timeframe": "REAL-TIME",
                },
            }
    document = {
        "contract": "advanced-options-v3-pre-session-capture-v1",
        "compatibility_profile": SEALED_CAPTURE_GENERALIZED_C6_PROFILE,
        "configured_underlyings": [ticker],
        "captured_at_utc": seed[
            "authority_bundle"
        ].session.prepared_at.isoformat(),
        "session_date": SESSION_DATE.isoformat(),
        "provider_requests": 1,
        "decoded_bytes": 2048,
        "responses": [
            {
                "underlyer": ticker,
                "request_id": f"provider-free-{ticker}",
                "page_count": 1,
            },
        ],
        "selected_snapshots": snapshots,
    }
    document["semantic_sha256"] = canonical_sha256(document)
    return p8_activity_selection_capture_from_sealed(
        document,
        compatibility_profile=SEALED_CAPTURE_GENERALIZED_C6_PROFILE,
    )


class _ProviderFreeSelectionSource:
    def __init__(self, provider, capture, state):
        self.provider = provider
        self.capture_value = capture
        self.state = state

    def capture(self, **_scope):
        if not self.state["interrupted"]:
            self.state["interrupted"] = True
            raise AdvancedOptionsPreparationRetryable(
                "provider-free injected interruption"
            )
        with self.provider.transaction(read_only=True) as unit:
            unit.cursor.execute(
                "SELECT count(*) FROM rt.provider_request_ledger "
                "WHERE state='RESERVED'"
            )
            self.state["reserved_before_io"] = (
                int(unit.cursor.fetchone()[0]) > 0
            )
        return self.capture_value


def test_c8_real_executor_provider_free_end_to_end_with_retry():
    with disposable_v2_database(
        f"test_realtime_v2_c8_real_executor_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(
                database_cursor,
                options_ready=False,
                canonical_equity=True,
            )
            _seed_c8_equity_history(database_cursor, seed)
            _seed_complete_c8_reference(database_cursor, seed)
            checked_at = seed[
                "authority_bundle"
            ].session.prepared_at
            reconcile_advanced_options_lifecycle_cursor(
                database_cursor,
                target_session_date=SESSION_DATE,
                checked_at=checked_at,
            )
            database_cursor.connection.commit()

        provider = _provider(pool)
        source_state = {
            "interrupted": False,
            "reserved_before_io": False,
        }
        captures = {
            ticker: _provider_free_selection_capture(seed, ticker)
            for ticker in ("QQQ", "SPY")
        }
        executor = PostgresAutomaticOptionsPreparationExecutor(
            provider,
            api_key="provider-free",
            backend_image_id="sha256:" + "f" * 64,
            maximum_provider_requests_per_underlying=1,
            maximum_decoded_bytes_per_underlying=4096,
            admission_profile_version=(
                LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION
            ),
            exact_four_compatibility=True,
            clock=lambda: checked_at,
            selection_source_factory=lambda claim: (
                _ProviderFreeSelectionSource(
                    provider,
                    captures[claim.ticker],
                    source_state,
                )
            ),
            combined_capture_factory=(
                lambda _claim, _captures: dict(
                    seed["sealed_selection"]
                )
            ),
        )
        owner_id = uuid4()
        reports = []
        for attempt in range(20):
            cycle_at = checked_at + timedelta(minutes=attempt * 10)
            report = run_advanced_options_preparation_cycle(
                provider,
                executor,
                owner_id=owner_id,
                checked_at=cycle_at,
                clock=lambda: checked_at,
            )
            reports.append(report)
            with cursor(pool) as database_cursor:
                lifecycle = reconcile_advanced_options_lifecycle_cursor(
                    database_cursor,
                    target_session_date=SESSION_DATE,
                    checked_at=cycle_at,
                )
                database_cursor.connection.commit()
            if lifecycle is not None and lifecycle.status == "ACTIVE":
                break
        else:
            raise AssertionError(
                "real C8 executor did not activate: "
                f"reports={reports!r}, lifecycle={lifecycle!r}"
            )

        assert any(row["status"] == "RETRY" for row in reports)
        assert source_state["reserved_before_io"] is True
        assert lifecycle.session_binding_id is not None
        with cursor(pool) as database_cursor:
            database_cursor.execute(
                "SELECT count(*) FROM "
                "rt.advanced_options_continuous_sessions"
            )
            session_count = int(database_cursor.fetchone()[0])
            database_cursor.execute(
                "SELECT count(*) FILTER (WHERE state='COMPLETE'),"
                "count(*) FILTER (WHERE state='FAILED') "
                "FROM rt.provider_request_ledger"
            )
            provider_states = database_cursor.fetchone()
            database_cursor.execute(
                "SELECT count(*) FROM "
                "rt.advanced_options_preparation_evidence"
            )
            evidence_count = int(database_cursor.fetchone()[0])
            database_cursor.execute(
                "SELECT admission_profile_version,"
                "allocation_document "
                "FROM rt.advanced_options_selection_generations"
            )
            exact_selection = database_cursor.fetchone()

        assert session_count == 1
        assert provider_states[0] >= 2
        assert provider_states[1] >= 1
        assert evidence_count == 9
        assert exact_selection[0] == "c6_exact_four_regression_v1"
        assert exact_selection[1]["schema"] == (
            "c6_exact_four_regression_v1"
        )
        late_cycle = run_advanced_options_preparation_cycle(
            provider,
            executor,
            owner_id=uuid4(),
            checked_at=(
                OptionExchangeCalendar("XNYS").session_close(
                    SESSION_DATE
                )
                + timedelta(minutes=6)
            ),
            clock=lambda: checked_at,
        )
        assert late_cycle["status"] == "IDLE"
        assert late_cycle["expired_sessions"] == []
        with cursor(pool) as database_cursor:
            database_cursor.execute(
                "SELECT status,expiration_state FROM "
                "rt.advanced_options_preparation_sessions"
            )
            assert database_cursor.fetchone() == ("ACTIVE", None)


def test_c8_real_executor_uses_v2_allocation_for_generalized_membership():
    underlyings = ("IWM", "QQQ", "SPY")
    with disposable_v2_database(
        f"test_realtime_v2_c8_generalized_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(
                database_cursor,
                options_ready=False,
                canonical_equity=True,
            )
            _add_desired_option_underlying(database_cursor, "IWM")
            _seed_c8_equity_history(
                database_cursor,
                seed,
                underlyings=underlyings,
            )
            checked_at = seed[
                "authority_bundle"
            ].session.prepared_at
            reconcile_advanced_options_lifecycle_cursor(
                database_cursor,
                target_session_date=SESSION_DATE,
                checked_at=checked_at,
            )
            database_cursor.connection.commit()

        provider = _provider(pool)
        source_state = {
            "interrupted": True,
            "reserved_before_io": False,
        }
        captures = {
            ticker: _provider_free_selection_capture(seed, ticker)
            for ticker in underlyings
        }
        spy_capture = captures["SPY"]
        captures["SPY"] = replace(
            spy_capture,
            selected_snapshots={
                contract_ticker: {
                    key: value
                    for key, value in raw.items()
                    if key != "open_interest"
                }
                for contract_ticker, raw
                in spy_capture.selected_snapshots.items()
            },
        )
        executor = PostgresAutomaticOptionsPreparationExecutor(
            provider,
            api_key="provider-free",
            backend_image_id="sha256:" + "e" * 64,
            maximum_provider_requests_per_underlying=1,
            maximum_decoded_bytes_per_underlying=4096,
            clock=lambda: checked_at,
            selection_source_factory=lambda claim: (
                _ProviderFreeSelectionSource(
                    provider,
                    captures[claim.ticker],
                    source_state,
                )
            ),
        )
        owner_id = uuid4()
        reference_seeded = False
        for attempt in range(30):
            cycle_at = checked_at + timedelta(minutes=attempt * 10)
            report = run_advanced_options_preparation_cycle(
                provider,
                executor,
                owner_id=owner_id,
                checked_at=cycle_at,
                clock=lambda: checked_at,
            )
            with cursor(pool) as database_cursor:
                if not reference_seeded:
                    database_cursor.execute(
                        "SELECT count(*) FROM "
                        "rt.advanced_options_preparation_work "
                        "WHERE stage='CATALOG_QUOTE' "
                        "AND status='COMPLETE'"
                    )
                    if int(database_cursor.fetchone()[0]) == 3:
                        snapshots = {
                            ticker: raw
                            for capture in captures.values()
                            for ticker, raw
                            in capture.selected_snapshots.items()
                        }
                        _seed_complete_c8_reference(
                            database_cursor,
                            seed,
                            underlyings=underlyings,
                            snapshots=snapshots,
                        )
                        reference_seeded = True
                lifecycle = reconcile_advanced_options_lifecycle_cursor(
                    database_cursor,
                    target_session_date=SESSION_DATE,
                    checked_at=cycle_at,
                )
                database_cursor.connection.commit()
            if lifecycle is not None and lifecycle.status == "ACTIVE":
                break
        else:
            raise AssertionError(
                "generalized C8 executor did not activate: "
                f"report={report!r}, lifecycle={lifecycle!r}"
            )

        assert source_state["reserved_before_io"] is True
        with cursor(pool) as database_cursor:
            database_cursor.execute(
                "SELECT selection_generation_sha256,"
                "admission_profile_version,"
                "admission_profile_sha256,allocation_sha256,"
                "allocation_document,capacity_document "
                "FROM rt.advanced_options_selection_generations"
            )
            generation = database_cursor.fetchone()
            database_cursor.execute(
                "SELECT underlying,contract_ticker "
                "FROM rt.advanced_options_selection_members "
                "ORDER BY underlying,contract_ticker"
            )
            selected_rows = tuple(database_cursor.fetchall())
            database_cursor.execute(
                "SELECT count(*) FROM "
                "rt.advanced_options_physical_subscriptions"
            )
            physical_count = int(database_cursor.fetchone()[0])
            database_cursor.execute(
                "SELECT open_interest_state,reason_codes "
                "FROM rt.advanced_options_underlying_readiness "
                "WHERE underlying='SPY'"
            )
            spy_readiness = database_cursor.fetchone()
        allocation = dict(generation[4])
        selected_tickers = {
            str(row[1]) for row in selected_rows
        }
        by_underlying = {
            ticker: sum(
                str(row[0]) == ticker for row in selected_rows
            )
            for ticker in underlyings
        }

        assert generation[1] == "c6_activity_momentum_v2"
        assert generation[2] == allocation["profile_sha256"]
        assert generation[3] == allocation["allocation_sha256"]
        assert allocation["active_contract_budget"] <= 8 * len(
            underlyings
        )
        assert allocation["selected_contract_count"] <= allocation[
            "active_contract_budget"
        ]
        assert all(value <= 12 for value in by_underlying.values())
        assert allocation["admission_evidence"][
            "configured_contracts_per_underlying"
        ] == 8
        assert allocation["admission_evidence"][
            "effective_per_underlying_limit"
        ] == 12
        assert allocation["admission_evidence"][
            "effective_total_budget"
        ] == min(
            value
            for value in (
                8 * len(underlyings),
                allocation["admission_evidence"][
                    "configured_total_budget"
                ],
                allocation["admission_evidence"][
                    "effective_provider_project_ceiling"
                ],
                allocation["admission_evidence"][
                    "established_measured_capacity"
                ],
            )
            if value is not None
        )
        assert allocation["consumer_eligibility"]
        assert spy_readiness == (
            "NOT_REQUIRED",
            ["MISSING_PRIOR_OPEN_INTEREST"],
        )
        assert all(
            set(group["contract_tickers"]).issubset(
                selected_tickers
            )
            for group in allocation["selected_groups"]
        )
        assert physical_count == 1

        first = PostgresContinuousSessionStateRepository(
            _provider(pool),
            session_date=SESSION_DATE,
        ).load_c6_selection_plan()
        restarted = PostgresContinuousSessionStateRepository(
            _provider(pool),
            session_date=SESSION_DATE,
        ).load_c6_selection_plan()

        assert first is not None
        assert restarted is not None
        assert first.generation_sha256 == generation[0]
        assert restarted.generation_sha256 == first.generation_sha256


FULL_SIZE_SPOTS = {"QQQ": 600, "SPY": 680}
FULL_SIZE_CHAIN_ROWS = {"QQQ": 3_840, "SPY": 4_352}


def _seed_c8_minute_spots(cursor, seed):
    calendar = OptionExchangeCalendar("XNYS")
    session = calendar.trailing_sessions_before(SESSION_DATE, 1)[-1]
    closed = calendar.session_close(session)
    opened = closed - timedelta(minutes=1)
    cursor.execute(
        "SELECT intent.ticker,binding.security_id::text,"
        "binding.instrument_id "
        "FROM rt.realtime_desired_member_intents AS intent "
        "JOIN rt.realtime_desired_member_bindings AS binding "
        "USING(intent_id) "
        "WHERE intent.ticker=ANY(%s) "
        "AND intent.member_kind='EQUITY'",
        (list(FULL_SIZE_SPOTS),),
    )
    instruments = tuple(cursor.fetchall())
    stream_session = uuid4()
    cursor.execute(
        "INSERT INTO rt.stream_sessions ("
        "session_id,provider,feed,manifest_id,owner_id,owner_epoch,"
        "started_at,ended_at,status"
        ") VALUES (%s,'c8-provider-free','STOCKS',%s,%s,1,%s,%s,"
        "'STOPPED')",
        (
            str(stream_session),
            str(seed["desired_manifest_id"]),
            str(uuid4()),
            opened,
            closed,
        ),
    )
    for source_offset, (ticker, security_id, instrument_id) in enumerate(
        instruments,
        start=1,
    ):
        spot = FULL_SIZE_SPOTS[str(ticker)]
        revision_id = uuid4()
        cursor.execute(
            "INSERT INTO rt.bar_catalog ("
            "bar_revision_id,cohort_id,provider_tier,instrument_id,"
            "interval,bar_start,bar_end,observed_at,session_date"
            ") VALUES (%s,%s,'MASSIVE_ADVANCED',%s,'1m',%s,%s,%s,%s)",
            (
                str(revision_id),
                str(seed["desired_cohort_id"]),
                int(instrument_id),
                opened,
                closed,
                closed,
                session,
            ),
        )
        cursor.execute(
            "INSERT INTO rt.bar_revisions ("
            "bar_revision_id,cohort_id,provider_tier,"
            "security_id,instrument_id,ticker,interval,"
            "session_date,session_scope,adjustment_basis,"
            "bar_start,bar_end,open_price,high_price,"
            "low_price,close_price,volume,vwap,"
            "transaction_count,source_kind,"
            "availability_mode,is_final,observed_at,"
            "replay_available_at,journal_generation,"
            "stream_session_id,source_offset,payload_sha256,"
            "created_at"
            ") VALUES (%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,"
            "'1m',%s,'RTH','RAW_UNADJUSTED',%s,%s,%s,%s,%s,%s,"
            "1000000,%s,100,'REALTIME_STREAM',"
            "'HISTORICAL_RECONSTRUCTED',true,%s,%s,%s,%s,"
            "%s,%s,%s)",
            (
                str(revision_id),
                str(seed["desired_cohort_id"]),
                str(security_id),
                int(instrument_id),
                str(ticker),
                session,
                opened,
                closed,
                spot,
                spot,
                spot,
                spot,
                spot,
                closed,
                closed,
                str(uuid4()),
                str(stream_session),
                source_offset,
                _digest(f"c8-minute-spot:{ticker}"),
                closed,
            ),
        )
        cursor.execute(
            "INSERT INTO rt.bar_current ("
            "cohort_id,provider_tier,instrument_id,interval,"
            "bar_start,session_scope,adjustment_basis,"
            "source_kind,bar_revision_id,bar_end,observed_at,"
            "updated_at"
            ") VALUES (%s,'MASSIVE_ADVANCED',%s,'1m',%s,"
            "'RTH','RAW_UNADJUSTED','REALTIME_STREAM',%s,%s,%s,%s)",
            (
                str(seed["desired_cohort_id"]),
                int(instrument_id),
                opened,
                str(revision_id),
                closed,
                closed,
                closed,
            ),
        )


def _seed_c8_treasury_curve(cursor, rate_date, *, received_at=None):
    received_at = received_at or OptionExchangeCalendar("XNYS").session_open(
        SESSION_DATE
    ) - timedelta(hours=2)
    for tenor_days, annual_rate in (
        (30, "0.0410"),
        (60, "0.0405"),
        (90, "0.0400"),
        (180, "0.0390"),
        (365, "0.0380"),
    ):
        cursor.execute(
            "INSERT INTO ref.risk_free_rate_observations ("
            "observation_id,rate_date,tenor_days,annual_rate,source,"
            "source_key,source_observed_at,received_at,payload_sha256"
            ") VALUES (%s,%s,%s,%s,'us_treasury_par_yield_v1',%s,%s,%s,%s)",
            (
                str(uuid4()),
                rate_date,
                tenor_days,
                annual_rate,
                f"{rate_date.isoformat()}:{tenor_days}d",
                received_at - timedelta(minutes=1),
                received_at,
                _digest(f"c8-treasury:{rate_date}:{tenor_days}"),
            ),
        )


def _seed_c8_dividend_coverage(cursor):
    """What the Equity REFERENCE stage leaves for each Options underlying."""
    calendar = OptionExchangeCalendar("XNYS")
    rate_date = calendar.previous_session_date(SESSION_DATE)
    received_at = calendar.session_open(SESSION_DATE) - timedelta(hours=2)
    request = ReferenceSeedRequest(
        universe=UniverseManifest(
            version="c8-reference-inputs",
            equity_tickers=tuple(FULL_SIZE_SPOTS),
            option_underlyings=(),
        ),
        session_date=rate_date,
        created_at=received_at,
        effective_at=received_at,
        valid_until=received_at + timedelta(days=7),
    )
    persist_reference_facts_cursor(
        cursor,
        tuple(
            _corporate_action_coverage_fact(
                ticker,
                action_type,
                (),
                rate_date - timedelta(days=180),
                rate_date + timedelta(days=365),
                request,
            )
            for ticker in FULL_SIZE_SPOTS
            for action_type in ("SPLIT", "DIVIDEND")
        ),
    )


def _full_size_chain(ticker, quote_at, *, underlying_at):
    spot = FULL_SIZE_SPOTS[ticker]
    quote_ns = int(quote_at.timestamp() * 1_000_000_000)
    underlying_ns = int(underlying_at.timestamp() * 1_000_000_000)
    expirations = tuple(
        SESSION_DATE + timedelta(days=days)
        for days in range(22)
        if (SESSION_DATE + timedelta(days=days)).weekday() < 5
    )
    rows = []
    for expiration in expirations:
        for strike in range(int(spot * 0.9) + 1, int(spot * 1.1) + 1):
            distance = (strike - spot) / spot
            for option_type, side, signed in (
                ("call", "C", -distance),
                ("put", "P", distance),
            ):
                delta = 1 / (1 + math.exp(-signed * 40))
                midpoint = max(0.05, delta * spot * 0.03)
                half_spread = 0.01 + 0.002 * midpoint + 0.5 * abs(distance)
                rows.append({
                    "break_even_price": strike + midpoint,
                    "day": {
                        "volume": 500 if abs(distance) < 0.03 else 5,
                        "last_updated": quote_ns,
                    },
                    "details": {
                        "contract_type": option_type,
                        "exercise_style": "american",
                        "expiration_date": expiration.isoformat(),
                        "shares_per_contract": 100,
                        "strike_price": strike,
                        "ticker": (
                            f"O:{ticker}{expiration:%y%m%d}{side}"
                            f"{strike * 1000:08d}"
                        ),
                    },
                    "greeks": {
                        "delta": round(
                            delta if side == "C" else -delta,
                            4,
                        ),
                        "gamma": 0.01,
                        "theta": -0.1,
                        "vega": 0.2,
                    },
                    "implied_volatility": 0.2,
                    "last_quote": {
                        "bid": round(max(0.01, midpoint - half_spread), 2),
                        "ask": round(midpoint + half_spread, 2),
                        "last_updated": quote_ns,
                    },
                    "open_interest": 1_000,
                    "underlying_asset": {
                        "ticker": ticker,
                        "price": spot,
                        "last_updated": underlying_ns,
                        "timeframe": "REAL-TIME",
                    },
                })
    rows.sort(key=lambda row: row["details"]["ticker"])
    return rows


class _FullSizeChainTransport:
    """Serves one 250-row page per call, as the provider does."""

    instances = []

    def __init__(self, *_args, **_kwargs):
        self.rows = None
        self.offset = 0
        self.calls = 0
        self.bytes = 0
        type(self).instances.append(self)

    def get(self, url, params):
        ticker = urlsplit(url).path.rsplit("/", 1)[1]
        if self.rows is None:
            self.ticker = ticker
            self.params = dict(params)
            self.rows = self.chain_for(ticker)
        self.calls += 1
        page = self.rows[self.offset:self.offset + 250]
        self.offset += 250
        document = {
            "status": "OK",
            "request_id": f"provider-free-{ticker}-{self.calls}",
            "results": page,
        }
        if self.offset < len(self.rows):
            document["next_url"] = (
                f"https://api.massive.com/v3/snapshot/options/{ticker}"
                f"?cursor={self.offset}"
            )
        body = json.dumps(document).encode("utf-8")
        self.bytes += len(body)
        return PolygonHttpResponse(status_code=200, body=body)


def test_c8_real_executor_activates_from_full_size_paginated_chains(
    monkeypatch,
):
    calendar = OptionExchangeCalendar("XNYS")
    prior_close = calendar.session_close(
        calendar.trailing_sessions_before(SESSION_DATE, 1)[-1]
    )
    captures = {ticker: 0 for ticker in FULL_SIZE_SPOTS}
    transports = type(
        "_Transport",
        (_FullSizeChainTransport,),
        {"instances": []},
    )
    with disposable_v2_database(
        f"test_realtime_v2_c8_full_size_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(
                database_cursor,
                options_ready=False,
                canonical_equity=True,
            )
            _seed_c8_equity_history(database_cursor, seed)
            _seed_c8_minute_spots(database_cursor, seed)
            _seed_c8_dividend_coverage(database_cursor)
            # As on the live ledger: the latest curve is older than the prior
            # session, so activation must wait for it without failing terminally.
            _seed_c8_treasury_curve(
                database_cursor,
                calendar.previous_session_date(
                    calendar.previous_session_date(SESSION_DATE)
                ),
            )
            checked_at = seed[
                "authority_bundle"
            ].session.prepared_at
            reconcile_advanced_options_lifecycle_cursor(
                database_cursor,
                target_session_date=SESSION_DATE,
                checked_at=checked_at,
            )
            database_cursor.connection.commit()

        now = {"value": checked_at}

        def chain_for(_transport, ticker):
            # First round: every quote still carries the prior close, so
            # allocation must refresh. Second round: quotes are current.
            captures[ticker] += 1
            quote_at = (
                prior_close if captures[ticker] == 1 else now["value"]
            )
            rows = _full_size_chain(
                ticker,
                quote_at,
                underlying_at=now["value"],
            )
            assert len(rows) == FULL_SIZE_CHAIN_ROWS[ticker]
            return rows

        transports.chain_for = chain_for
        monkeypatch.setattr(
            preparation_module,
            "SingleAttemptPolygonHttpTransport",
            transports,
        )
        provider = _provider(pool)
        executor = PostgresAutomaticOptionsPreparationExecutor(
            provider,
            api_key="provider-free",
            backend_image_id="sha256:" + "c" * 64,
            maximum_provider_requests_per_underlying=32,
            clock=lambda: now["value"],
        )
        owner_id = uuid4()
        reports = []
        lifecycle = None
        replacement_received_at = None
        frozen_capture = None
        for attempt in range(40):
            now["value"] = checked_at + timedelta(minutes=attempt * 10)
            report = run_advanced_options_preparation_cycle(
                provider,
                executor,
                owner_id=owner_id,
                checked_at=now["value"],
                clock=lambda: now["value"],
            )
            reports.append((
                report["status"],
                report.get("stage"),
                report.get("reason"),
            ))
            with cursor(pool) as database_cursor:
                if (
                    report["status"] == "RETRY"
                    and report["stage"] == "SESSION_ACTIVATION"
                ):
                    assert replacement_received_at is None
                    replacement_received_at = now["value"] + timedelta(seconds=1)
                    assert replacement_received_at > calendar.session_open(
                        SESSION_DATE
                    )
                    database_cursor.execute(
                        "SELECT evidence FROM "
                        "rt.advanced_options_preparation_work "
                        "WHERE stage='CAPACITY_SHARDS' AND status='COMPLETE' "
                        "ORDER BY ticker LIMIT 1"
                    )
                    frozen_capture = database_cursor.fetchone()[0]
                    _seed_c8_treasury_curve(
                        database_cursor,
                        calendar.previous_session_date(SESSION_DATE),
                        received_at=replacement_received_at,
                    )
                lifecycle = reconcile_advanced_options_lifecycle_cursor(
                    database_cursor,
                    target_session_date=SESSION_DATE,
                    checked_at=now["value"],
                )
                database_cursor.connection.commit()
            if lifecycle is not None and lifecycle.status == "ACTIVE":
                break
        else:
            raise AssertionError(
                "full-size C8 executor did not activate: "
                f"reports={reports!r}, lifecycle={lifecycle!r}"
            )

        assert captures == {"QQQ": 2, "SPY": 2}
        assert {
            transport.ticker: transport.calls
            for transport in transports.instances
        } == {"QQQ": 16, "SPY": 18}
        for transport in transports.instances:
            spot = Decimal(FULL_SIZE_SPOTS[transport.ticker])
            assert transport.params["expiration_date.gte"] == (
                SESSION_DATE.isoformat()
            )
            assert transport.params["expiration_date.lte"] == (
                SESSION_DATE + timedelta(days=21)
            ).isoformat()
            assert Decimal(transport.params["strike_price.gte"]) == (
                spot * Decimal("0.90")
            )
            assert Decimal(transport.params["strike_price.lte"]) == (
                spot * Decimal("1.10")
            )
            assert transport.bytes < 25_000_000
        assert any(row[0] == "REFRESH_REQUIRED" for row in reports)
        assert [
            row[2] for row in reports if row[0] == "RETRY"
        ] == [
            "opening reference inputs rejected: "
            "Treasury curve identity, freshness, or clocks changed"
        ]
        with cursor(pool) as database_cursor:
            database_cursor.execute(
                "SELECT count(*) FROM ref.option_contracts "
                "WHERE contract_ticker LIKE ANY(%s)",
                (["O:QQQ%", "O:SPY%"],),
            )
            contract_count = int(database_cursor.fetchone()[0])
            database_cursor.execute(
                "SELECT state,count(*) FROM rt.provider_request_ledger "
                "WHERE authorization_reference LIKE "
                "'advanced-realtime-v3-options:%%' GROUP BY state"
            )
            ledger = {
                str(row[0]): int(row[1])
                for row in database_cursor.fetchall()
            }
            database_cursor.execute(
                "SELECT allocation_document "
                "FROM rt.advanced_options_selection_generations"
            )
            allocation = dict(database_cursor.fetchone()[0])
            database_cursor.execute(
                "SELECT underlying,count(*) "
                "FROM rt.advanced_options_selection_members "
                "GROUP BY underlying"
            )
            selected = dict(database_cursor.fetchall())

        # Two rounds of 32 slots per underlying, inside the K*N*3 allowance.
        assert ledger == {
            "COMPLETE": 2 * (16 + 18),
            "FAILED": 2 * (2 * 32 - 16 - 18),
        }
        assert sum(ledger.values()) <= 32 * 2 * 3
        assert contract_count >= sum(FULL_SIZE_CHAIN_ROWS.values())
        assert all(0 < value <= 12 for value in selected.values())
        reference = PostgresAdvancedOptionsReferenceStateRepository(
            _provider(pool)
        ).load_for_session(SESSION_DATE, known_at=now["value"])
        assert {
            (row.ticker, row.window_sessions)
            for row in reference.historical_volatility
        } == {
            (ticker, window)
            for ticker in FULL_SIZE_SPOTS
            for window in (20, 30)
        }
        assert {row.ticker for row in reference.valuation_inputs} == set(
            FULL_SIZE_SPOTS
        )
        assert len(reference.equity_metrics) == 3 * len(FULL_SIZE_SPOTS)
        assert len(reference.open_interest) == sum(selected.values())
        assert reference.known_at == now["value"]
        assert reference.known_at > replacement_received_at
        assert all(
            rate.received_at == replacement_received_at
            for valuation in reference.valuation_inputs
            for rate in valuation.rate_observations
        )
        assert set(selected) == set(FULL_SIZE_SPOTS)
        assert allocation["selected_contract_count"] == sum(
            selected.values()
        )
        restarted = PostgresContinuousSessionStateRepository(
            _provider(pool),
            session_date=SESSION_DATE,
        )
        assert restarted.load_c6_selection_plan() is not None
        acquisition = restarted.load_acquisition_binding()
        assert len(acquisition.contract_tickers) == sum(selected.values())
        assert frozen_capture is not None
        captured_at = preparation_module.datetime.fromisoformat(
            frozen_capture["capture"]["captured_at_utc"]
        )
        assert replacement_received_at > captured_at
        assert acquisition.source_sha256 == frozen_capture["capture"][
            "semantic_sha256"
        ]
        assert all(
            row.received_at == captured_at
            for row in reference.open_interest
            if row.availability is ReferenceAvailability.AVAILABLE
        )
        assert restarted.load_package()["v3_admitted_contracts"]
        assert restarted.load_preopen_session_binding(
            restarted.load_bundle_document()
        ) is not None
        _exercise_continuous_scheduler(pool, seed, restarted, now["value"], monkeypatch)


def _exercise_continuous_scheduler(pool, seed, state, activated_at, monkeypatch):
    session = state.load_preopen_session_binding(state.load_bundle_document())
    tickers = state.load_acquisition_binding().contract_tickers
    clock = {"value": activated_at}
    calls = []
    interrupted_bindings = []

    def advancing_clock():
        value = clock["value"]
        clock["value"] += timedelta(milliseconds=5)
        return value
    with cursor(pool) as database_cursor:
        database_cursor.execute(
            "SELECT rt.ensure_partitions('rt.raw_payloads',%s)",
            (SESSION_DATE,),
        )
        database_cursor.execute(
            "SELECT rt.ensure_partitions('rt.raw_payload_chunks',%s)",
            (SESSION_DATE,),
        )
        database_cursor.connection.commit()

    class SnapshotTransport:
        def __init__(self, _api_key, **_kwargs):
            pass

        def get(self, url, params):
            calls.append((url, params))
            assert tuple(params["ticker.any_of"].split(",")) == tickers
            rows = [
                row
                for ticker in FULL_SIZE_SPOTS
                for row in _full_size_chain(
                    ticker, clock["value"], underlying_at=clock["value"]
                )
                if row["details"]["ticker"] in tickers
            ]
            return PolygonHttpResponse(
                200, json.dumps({"status": "OK", "results": rows}).encode()
            )

    first_cutoff = activated_at.replace(
        minute=(activated_at.minute // 5) * 5, second=0, microsecond=0,
    ) + timedelta(minutes=5)
    for sequence in (1, 2):
        cutoff = first_cutoff + timedelta(minutes=5 * (sequence - 1))
        clock["value"] = cutoff + timedelta(seconds=20)
        with cursor(pool) as database_cursor:
            window_id, receipt_id = uuid4(), uuid4()
            database_cursor.execute(
                "INSERT INTO rt.window_runs (window_id,plan_id,cohort_id,"
                "provider_tier,window_key,release_at,cutoff,deadline,state) "
                "VALUES (%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,'COMPLETE')",
                (str(window_id), str(seed["equity_plan_id"]),
                 str(seed["desired_cohort_id"]), f"continuous-E-{sequence}",
                 cutoff, cutoff, cutoff + timedelta(minutes=5)),
            )
            database_cursor.execute(
                "SELECT universe_sha256 FROM rt.subscription_manifests "
                "WHERE manifest_id=%s", (str(seed["desired_manifest_id"]),),
            )
            universe_sha256 = database_cursor.fetchone()[0]
            database_cursor.execute(
                "INSERT INTO rt.cycle_receipts (receipt_id,window_id,cohort_id,"
                "receipt_kind,cutoff,reference_generation,universe_sha256,"
                "input_sha256,build_id,created_at) "
                "VALUES (%s,%s,%s,'INPUT_READY',%s,1,%s,%s,'continuous-E',%s)",
                (str(receipt_id), str(window_id), str(seed["desired_cohort_id"]),
                 cutoff, universe_sha256, _digest(f"continuous-E-{sequence}"),
                 cutoff + timedelta(seconds=10)),
            )
            database_cursor.execute(
                "INSERT INTO rt.stage_runs (stage_run_id,window_id,stage_id,"
                "stage_version,state,finished_at) "
                "SELECT %s,%s,stage_id,version,'COMPLETE',%s "
                "FROM rt.stage_catalog WHERE stage_id='EQUITY_MATERIALIZE' "
                "ORDER BY version DESC LIMIT 1",
                (str(uuid4()), str(window_id), clock["value"]),
            )
            database_cursor.connection.commit()
        if sequence == 1:
            def interrupted_attach(*_args, **_kwargs):
                interrupted_bindings.append(_args[2])
                raise RuntimeError("interrupted before downstream append")

            with monkeypatch.context() as interruption:
                interruption.setattr(
                    PostgresContinuousSessionStateRepository,
                    "attach_downstream_binding", interrupted_attach,
                )
                with pytest.raises(RuntimeError, match="interrupted before"):
                    run_continuous_options_schedule_cycle(
                        _provider(pool), session_date=SESSION_DATE, api_key="provider-free",
                        clock=advancing_clock, transport_factory=SnapshotTransport,
                    )
            clock["value"] += timedelta(seconds=1)
            with cursor(pool) as database_cursor:
                database_cursor.execute(
                    "SELECT requested_to,completed_at FROM rt.provider_request_ledger "
                    "WHERE endpoint_class='OPTIONS_UNIFIED_SNAPSHOT'"
                )
                selected_at, completed_at = database_cursor.fetchone()
                assert selected_at < completed_at
        result = run_continuous_options_schedule_cycle(
            _provider(pool), session_date=SESSION_DATE, api_key="provider-free",
            clock=advancing_clock, transport_factory=SnapshotTransport,
        )
        assert result["status"] == "ATTACHED"
        assert result["sequence"] == sequence
        assert run_continuous_options_schedule_cycle(
            _provider(pool), session_date=SESSION_DATE, api_key="provider-free",
            clock=advancing_clock, transport_factory=SnapshotTransport,
        )["status"] == "ALREADY_ATTACHED"
    assert len(calls) == 2
    chain = PostgresContinuousSessionStateRepository(
        _provider(pool), session_date=SESSION_DATE,
    ).load_downstream_bindings(session)
    assert len(chain) == 2
    assert chain[0] == interrupted_bindings[0]
    assert chain[1].previous_binding_sha256 == chain[0].binding_sha256
    from realtime.advanced_options_p8_production import (
        load_v3_pending_window_bindings, p8_production_binding_from_payload,
    )
    template = p8_production_binding_from_payload(chain[0].production_binding)
    pending = load_v3_pending_window_bindings(
        provider=_provider(pool), binding=template,
        bundle=state.load_bundle_document(),
        available_at=clock["value"] + timedelta(minutes=5),
    )
    assert len(pending) == 2
    assert [row.enrollment for row in pending] == [
        p8_production_binding_from_payload(row.production_binding).enrollment
        for row in chain
    ]
    from realtime.advanced_options_p8_roles import _contract_scope
    from realtime.advanced_options_flow_policy import load_advanced_options_flow_policy
    from realtime.option_source_flow import persist_option_contract_scope_cursor
    from realtime.advanced_options_reference_producer import (
        PostgresAdvancedOptionsReferenceProducer,
        build_advanced_options_reference_producer_plan,
    )
    for window in pending:
        from realtime.advanced_options_postgres import (
            PostgresAdvancedOptionsCanonicalRuntimePort,
        )
        control = window.enrollment.control_plane
        with cursor(pool) as database_cursor:
            PostgresAdvancedOptionsCanonicalRuntimePort._verify_control_plane_cursor(
                SimpleNamespace(_scope=SimpleNamespace(control_plane=control)),
                database_cursor,
                SimpleNamespace(plan=SimpleNamespace(
                    config_sha256=control.stored_identity.service_plan_sha256,
                )),
            )
            input_plan = SimpleNamespace(
                window_id=window.window_id, control_plane=control,
                enrollment=window.enrollment, service_plan_id=control.plan_id,
                cutoff=control.receipts.options_input_ready.spec.cutoff,
            )
            context = PostgresAdvancedOptionsCanonicalRuntimePort._input_ready_context_cursor(
                SimpleNamespace(_scope=SimpleNamespace(input_ready_plan=input_plan)),
                database_cursor, lock_window=False,
            )
            assert context.plan.config_sha256 == control.stored_identity.service_plan_sha256
        scope = _contract_scope(window, load_advanced_options_flow_policy())
        with cursor(pool) as database_cursor:
            persist_option_contract_scope_cursor(database_cursor, scope)
            database_cursor.execute(
                "SELECT COALESCE(MAX(reference_generation),0)+1 "
                "FROM rt.advanced_options_reference_states"
            )
            generation = int(database_cursor.fetchone()[0])
            database_cursor.connection.commit()
        plan = build_advanced_options_reference_producer_plan(
            enrollment=window.enrollment, contract_scope=scope,
            reference_generation=generation,
            known_at=window.enrollment.control_plane.receipts.options_input_ready.spec.cutoff,
            valid_until=window.enrollment.control_plane.expires_at,
            discovery_scheduled_cycle=window.enrollment.control_plane.planned_at,
            prior_oi_session=OptionExchangeCalendar("XNYS").previous_session_date(SESSION_DATE),
            model_version="black-scholes-merton-v1",
            history_source_cohort_id=window.history.source_cohort_id,
        )
        reference = PostgresAdvancedOptionsReferenceProducer(
            _provider(pool), plan,
        ).produce()
        assert reference.reference_generation == generation
        assert len(reference.open_interest) == len(tickers)
        from realtime.advanced_options_equity_input import (
            initial_advanced_options_equity_context_requirements,
        )
        from realtime.advanced_options_native_loader import _load_equity_context
        from realtime.preflight import DetectorFamily
        with cursor(pool) as database_cursor:
            with database_cursor.connection.cursor(
                cursor_factory=RealDictCursor
            ) as context_cursor:
                reasons, _, _ = _load_equity_context(
                    context_cursor,
                    SimpleNamespace(
                        enrollment=window.enrollment,
                        control_plane=window.enrollment.control_plane,
                        history_source_cohort_id=window.history.source_cohort_id,
                        equity_input_receipt=SimpleNamespace(
                            cutoff=window.equity_input.equity_cutoff,
                        ),
                        equity_context_requirements=(
                            initial_advanced_options_equity_context_requirements()
                        ),
                    ),
                    [],
                )
        assert set(reasons) == {
            DetectorFamily.O1, DetectorFamily.O3, DetectorFamily.OS1, DetectorFamily.OS2,
        }
    from realtime import advanced_options_continuous_scheduler as scheduler_module

    failed_calls = []
    error_body = b'{"error":"provider-free failure"}'

    class FailedSnapshotTransport:
        def __init__(self, _api_key, **_kwargs):
            pass

        def get(self, url, params):
            failed_calls.append((url, params))
            return PolygonHttpResponse(503, error_body)

    acquisition = state.load_acquisition_binding()
    budget = scheduler_module._scheduler_budget(
        acquisition, state.load_package()["authorization_reference"],
    )
    failed_receipt = (str(uuid4()), first_cutoff + timedelta(minutes=5))

    def failed_capture():
        return scheduler_module._capture(
            _provider(pool), state=state, receipt=failed_receipt,
            budget=budget, api_key="provider-free", clock=advancing_clock,
            transport_factory=FailedSnapshotTransport,
            deadline=failed_receipt[1] + timedelta(seconds=300),
        )

    for _ in range(3):
        with pytest.raises(RuntimeError, match="HTTP 503"):
            failed_capture()
    with pytest.raises(RuntimeError, match="retry budget exhausted"):
        failed_capture()
    assert len(failed_calls) == 3
    with cursor(pool) as database_cursor:
        database_cursor.execute(
            "SELECT count(*),SUM(response_bytes),MIN(http_status),"
            "count(payload_id),MIN(reserved_response_bytes) "
            "FROM rt.provider_request_ledger "
            "WHERE endpoint_class='OPTIONS_UNIFIED_SNAPSHOT' AND state='FAILED'"
        )
        assert database_cursor.fetchone() == (
            3, 3 * len(error_body), 503, 3,
            scheduler_module.ACCOUNTED_RESPONSE_BYTE_CEILING,
        )


def test_c8_stale_capture_refresh_survives_restart_before_allocation_freeze():
    underlyings = ("QQQ", "SPY")
    with disposable_v2_database(
        f"test_realtime_v2_c8_refresh_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(
                database_cursor,
                options_ready=False,
                canonical_equity=True,
            )
            _seed_c8_equity_history(
                database_cursor,
                seed,
                underlyings=underlyings,
            )
            checked_at = seed[
                "authority_bundle"
            ].session.prepared_at
            reconcile_advanced_options_lifecycle_cursor(
                database_cursor,
                target_session_date=SESSION_DATE,
                checked_at=checked_at,
            )
            database_cursor.connection.commit()

        provider = _provider(pool)
        fresh_captures = {
            ticker: _provider_free_selection_capture(seed, ticker)
            for ticker in underlyings
        }
        stale_spy = fresh_captures["SPY"]
        stale_captures = {
            **fresh_captures,
            "SPY": replace(
                stale_spy,
                selected_snapshots={
                    contract_ticker: {
                        **raw,
                        "last_quote": {
                            **raw["last_quote"],
                            "last_updated": int(
                                (
                                    checked_at - timedelta(hours=2)
                                ).timestamp()
                                * 1_000_000_000
                            ),
                        },
                    }
                    for contract_ticker, raw
                    in stale_spy.selected_snapshots.items()
                },
            ),
        }
        source_state = {
            "interrupted": True,
            "reserved_before_io": False,
        }
        source_calls = {ticker: 0 for ticker in underlyings}

        def source_factory(claim):
            source_calls[claim.ticker] += 1
            capture = (
                stale_captures[claim.ticker]
                if source_calls[claim.ticker] == 1
                else fresh_captures[claim.ticker]
            )
            return _ProviderFreeSelectionSource(
                provider,
                capture,
                source_state,
            )

        def build_executor():
            return PostgresAutomaticOptionsPreparationExecutor(
                provider,
                api_key="provider-free",
                backend_image_id="sha256:" + "d" * 64,
                maximum_provider_requests_per_underlying=1,
                maximum_decoded_bytes_per_underlying=4096,
                clock=lambda: checked_at,
                selection_source_factory=source_factory,
            )

        executor = build_executor()
        owner_id = uuid4()
        reference_seeded = False
        refresh_report = None
        lifecycle = None
        for attempt in range(40):
            cycle_at = checked_at + timedelta(minutes=attempt * 10)
            report = run_advanced_options_preparation_cycle(
                provider,
                executor,
                owner_id=owner_id,
                checked_at=cycle_at,
                clock=lambda: checked_at,
            )
            if report["status"] == "REFRESH_REQUIRED":
                refresh_report = report
                executor = build_executor()
                owner_id = uuid4()
            with cursor(pool) as database_cursor:
                if not reference_seeded:
                    database_cursor.execute(
                        "SELECT count(*) FROM "
                        "rt.advanced_options_preparation_work "
                        "WHERE stage='CATALOG_QUOTE' "
                        "AND status='COMPLETE'"
                    )
                    if int(database_cursor.fetchone()[0]) == 2:
                        snapshots = {
                            ticker: raw
                            for capture in fresh_captures.values()
                            for ticker, raw
                            in capture.selected_snapshots.items()
                        }
                        _seed_complete_c8_reference(
                            database_cursor,
                            seed,
                            underlyings=underlyings,
                            snapshots=snapshots,
                        )
                        reference_seeded = True
                lifecycle = reconcile_advanced_options_lifecycle_cursor(
                    database_cursor,
                    target_session_date=SESSION_DATE,
                    checked_at=cycle_at,
                )
                database_cursor.connection.commit()
            if lifecycle is not None and lifecycle.status == "ACTIVE":
                break
        else:
            raise AssertionError(
                "refreshed C8 executor did not activate: "
                f"report={report!r}, lifecycle={lifecycle!r}"
            )

        assert refresh_report is not None
        assert refresh_report["affected_underlyings"] == ["SPY"]
        assert source_state["reserved_before_io"] is True
        assert source_calls == {"QQQ": 1, "SPY": 2}
        with cursor(pool) as database_cursor:
            database_cursor.execute(
                "SELECT requested_at,retry_at,backoff_seconds,"
                "request_document->>'retry_at' FROM "
                "rt.advanced_options_preparation_refresh_requests"
            )
            refresh_request = database_cursor.fetchone()
            database_cursor.execute(
                "SELECT count(*) FROM "
                "rt.advanced_options_preparation_evidence_history "
                "WHERE stage='CATALOG_QUOTE' AND intent_id=("
                "SELECT intent_id FROM "
                "rt.advanced_options_preparation_work "
                "WHERE ticker='SPY' AND stage='CATALOG_QUOTE')"
            )
            refreshed_catalog_evidence = int(
                database_cursor.fetchone()[0]
            )
            database_cursor.execute(
                "SELECT count(*) FROM "
                "rt.advanced_options_selection_generations"
            )
            allocation_count = int(database_cursor.fetchone()[0])

        assert refresh_request[1] > refresh_request[0]
        assert refresh_request[2] == 30
        assert refresh_request[3] == refresh_request[1].isoformat()
        assert refresh_report["retry_at_utc"] == (
            refresh_request[1].isoformat()
        )
        assert refreshed_catalog_evidence == 2
        assert allocation_count == 1

        calls_after_freeze = dict(source_calls)
        idle = run_advanced_options_preparation_cycle(
            provider,
            build_executor(),
            owner_id=uuid4(),
            checked_at=checked_at + timedelta(hours=12),
            clock=lambda: checked_at,
        )
        assert idle["status"] == "IDLE"
        assert source_calls == calls_after_freeze


def test_c8_stage_worker_reclaims_crashed_attempt_with_new_epoch():
    with disposable_v2_database(
        f"test_realtime_v2_c8_crash_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(database_cursor)
            checked_at = seed[
                "authority_bundle"
            ].session.prepared_at
            reconcile_advanced_options_lifecycle_cursor(
                database_cursor,
                target_session_date=SESSION_DATE,
                checked_at=checked_at,
            )
            crashed = claim_advanced_options_preparation_cursor(
                database_cursor,
                owner_id=uuid4(),
                checked_at=checked_at,
                lease_seconds=10,
            )
            database_cursor.connection.commit()

        assert crashed is not None
        assert crashed.stage == "CATALOG_QUOTE"
        assert crashed.attempt == 1
        assert crashed.owner_epoch == 1

        restarted_at = checked_at + timedelta(seconds=11)
        report = run_advanced_options_preparation_cycle(
            _provider(pool),
            _AutomaticC8Executor(pool, seed),
            owner_id=uuid4(),
            checked_at=restarted_at,
            lease_seconds=10,
            clock=lambda: restarted_at,
        )

        assert report["status"] == "COMPLETE"
        assert report["stage"] == "CATALOG_QUOTE"
        assert report["attempt"] == 2
        assert report["owner_epoch"] == 2


class _BudgetProbeExecutor:
    def __init__(self, provider):
        self.provider = provider
        self.reserved_before_io = False

    def provider_budget(self, claim):
        return AdvancedOptionsStageProviderBudget(
            ProviderBudgetScope(
                account_alias="MASSIVE_ADVANCED",
                session_date=claim.session_date,
                authorization_reference="c8-budget-probe",
                provider_authorization_reference="c8-budget-probe",
                expected_cost="owned provider-free reservation",
                request_ceiling=8,
                decoded_byte_ceiling=8_000,
                lanes=frozenset({
                    ProviderRequestLane.OPTIONS_CHAIN,
                }),
                authorized_until=(
                    claim.lease_until + timedelta(hours=1)
                ),
            ),
            (
                AdvancedOptionsProviderRequestPlan(
                    ProviderRequestLane.OPTIONS_CHAIN,
                    "OPTIONS_CHAIN_SNAPSHOT",
                    _digest("c8-budget-request"),
                    1_000,
                ),
            ),
        )

    def execute(self, claim, *, provider_request_ids):
        assert len(provider_request_ids) == 1
        with self.provider.transaction(
            read_only=True,
        ) as unit:
            unit.cursor.execute(
                "SELECT state,reserved_response_bytes "
                "FROM rt.provider_request_ledger "
                "WHERE request_id=%s",
                (str(provider_request_ids[0]),),
            )
            self.reserved_before_io = (
                unit.cursor.fetchone() == ("RESERVED", 1_000)
            )
        return AdvancedOptionsStageResult(
            watermark={"stage": claim.stage, "reserved": True},
            evidence={"status": "READY", "provider_io": "BLOCKED"},
            provider_results=(
                AdvancedOptionsProviderRequestResult(
                    provider_request_ids[0],
                    100,
                    2,
                ),
            ),
        )


class _ExhaustedBudgetExecutor:
    def __init__(self, session_date, authorized_until):
        self.scope = ProviderBudgetScope(
            account_alias="MASSIVE_ADVANCED",
            session_date=session_date,
            authorization_reference="c8-budget-exhausted",
            provider_authorization_reference="c8-budget-exhausted",
            expected_cost="owned provider-free exhaustion",
            request_ceiling=1,
            decoded_byte_ceiling=2_000,
            lanes=frozenset({
                ProviderRequestLane.OPTIONS_CHAIN,
            }),
            authorized_until=authorized_until,
        )
        self.executed = False

    def provider_budget(self, _claim):
        return AdvancedOptionsStageProviderBudget(
            self.scope,
            (
                AdvancedOptionsProviderRequestPlan(
                    ProviderRequestLane.OPTIONS_CHAIN,
                    "OPTIONS_CHAIN_SNAPSHOT",
                    _digest("c8-budget-exhausted-request"),
                    1_000,
                ),
            ),
        )

    def execute(self, _claim, *, provider_request_ids):
        self.executed = True
        raise AssertionError(
            f"executor received exhausted reservations: "
            f"{provider_request_ids!r}"
        )


def test_c8_provider_budget_is_reserved_before_stage_io():
    with disposable_v2_database(
        f"test_realtime_v2_c8_budget_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(database_cursor)
            checked_at = seed[
                "authority_bundle"
            ].session.prepared_at
            reconcile_advanced_options_lifecycle_cursor(
                database_cursor,
                target_session_date=SESSION_DATE,
                checked_at=checked_at,
            )
            database_cursor.connection.commit()
        provider = _provider(pool)
        executor = _BudgetProbeExecutor(provider)

        report = run_advanced_options_preparation_cycle(
            provider,
            executor,
            owner_id=uuid4(),
            checked_at=checked_at,
            clock=lambda: checked_at,
        )

        assert report["status"] == "COMPLETE"
        assert report["provider_requests"] == 1
        assert executor.reserved_before_io is True
        with cursor(pool) as database_cursor:
            database_cursor.execute(
                "SELECT state,response_bytes FROM "
                "rt.provider_request_ledger"
            )
            assert database_cursor.fetchone() == ("COMPLETE", 100)


def test_c8_budget_exhaustion_persists_wait_and_releases_claim():
    with disposable_v2_database(
        f"test_realtime_v2_c8_budget_wait_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(database_cursor)
            checked_at = seed[
                "authority_bundle"
            ].session.prepared_at
            reconcile_advanced_options_lifecycle_cursor(
                database_cursor,
                target_session_date=SESSION_DATE,
                checked_at=checked_at,
            )
            database_cursor.connection.commit()
        provider = _provider(pool)
        authorized_until = checked_at + timedelta(hours=2)
        executor = _ExhaustedBudgetExecutor(
            SESSION_DATE,
            authorized_until,
        )
        with provider.transaction(
            APP_ROLE,
            isolation_level="SERIALIZABLE",
        ) as unit:
            reserve_coordinated_provider_request_cursor(
                unit.cursor,
                executor.scope,
                ProviderRequestReservation(
                    request_id=uuid4(),
                    account_alias="MASSIVE_ADVANCED",
                    lane=ProviderRequestLane.OPTIONS_CHAIN,
                    endpoint_class="OPTIONS_CHAIN_SNAPSHOT",
                    request_sha256=_digest(
                        "c8-budget-existing-reservation"
                    ),
                    reserved_at=checked_at - timedelta(minutes=1),
                    attempt=1,
                ),
                reserved_response_bytes=1_000,
                account_wide=True,
            )

        report = run_advanced_options_preparation_cycle(
            provider,
            executor,
            owner_id=uuid4(),
            checked_at=checked_at,
            clock=lambda: checked_at,
        )

        assert report["status"] == "WAITING_BUDGET"
        assert report["provider_requests"] == 0
        assert report["retry_at_utc"] == authorized_until.isoformat()
        assert executor.executed is False
        with cursor(pool) as database_cursor:
            lifecycle = reconcile_advanced_options_lifecycle_cursor(
                database_cursor,
                target_session_date=SESSION_DATE,
                checked_at=checked_at + timedelta(seconds=1),
            )
            database_cursor.execute(
                "SELECT status,next_retry_at,lease_owner,lease_until,"
                "failure_streak,retry_reason FROM "
                "rt.advanced_options_preparation_work "
                "WHERE stage='CATALOG_QUOTE' "
                "ORDER BY ticker LIMIT 1"
            )
            work = database_cursor.fetchone()
            database_cursor.execute(
                "SELECT status,current_stage,next_retry_at,last_error "
                "FROM rt.advanced_options_preparation_sessions"
            )
            session = database_cursor.fetchone()
            database_cursor.execute(
                "SELECT count(*) FROM rt.provider_request_ledger"
            )
            ledger_count = int(database_cursor.fetchone()[0])
            database_cursor.connection.commit()

        assert work == (
            "RETRY",
            authorized_until,
            None,
            None,
            1,
            "PROVIDER_BUDGET_EXHAUSTED",
        )
        assert session == (
            "DEGRADED",
            "CATALOG_QUOTE",
            authorized_until,
            "PROVIDER_BUDGET_EXHAUSTED:CATALOG_QUOTE:"
            f"{authorized_until.isoformat()}",
        )
        assert ledger_count == 1
        assert lifecycle is not None
        assert lifecycle.status == "DEGRADED"
        assert lifecycle.reason_codes == (
            "WAITING_PROVIDER_BUDGET:CATALOG_QUOTE",
        )


def test_c8_rejected_chain_is_accounted_and_cannot_drain_the_budget(
    monkeypatch,
):
    """Replays 2026-11-02: every row rejected from a pre-open capture."""
    calendar = OptionExchangeCalendar("XNYS")
    session_open = calendar.session_open(SESSION_DATE)
    hard_stop = calendar.session_close(SESSION_DATE) + timedelta(minutes=5)
    transports = type(
        "_Transport",
        (_FullSizeChainTransport,),
        {"instances": []},
    )
    with disposable_v2_database(
        f"test_realtime_v2_c8_rejected_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(
                database_cursor,
                options_ready=False,
                canonical_equity=True,
            )
            _seed_c8_equity_history(database_cursor, seed)
            _seed_c8_minute_spots(database_cursor, seed)
            _seed_c8_dividend_coverage(database_cursor)
            # 2026-11-02 failed four minutes before the open.
            checked_at = session_open - timedelta(minutes=4)
            reconcile_advanced_options_lifecycle_cursor(
                database_cursor,
                target_session_date=SESSION_DATE,
                checked_at=checked_at,
            )
            database_cursor.connection.commit()

        now = {"value": checked_at}

        def chain_for(_transport, ticker):
            rows = _full_size_chain(
                ticker,
                now["value"],
                underlying_at=now["value"],
            )[:500]
            for row in rows:
                row["last_quote"].update(bid=0, ask=0)
            return rows

        transports.chain_for = chain_for
        monkeypatch.setattr(
            preparation_module,
            "SingleAttemptPolygonHttpTransport",
            transports,
        )
        provider = _provider(pool)
        executor = PostgresAutomaticOptionsPreparationExecutor(
            provider,
            api_key="provider-free",
            backend_image_id="sha256:" + "c" * 64,
            maximum_provider_requests_per_underlying=4,
            clock=lambda: now["value"],
        )
        owner_id = uuid4()
        retry_floor = session_open + timedelta(seconds=90)

        def cycle(at):
            now["value"] = at
            return run_advanced_options_preparation_cycle(
                provider,
                executor,
                owner_id=owner_id,
                checked_at=at,
                clock=lambda: at,
            )

        first = cycle(checked_at)
        assert first["status"] == "RETRY"
        assert first["ticker"] == "QQQ"
        assert first["provider_requests"] == 2
        assert first["retry_at_utc"] == retry_floor.isoformat()
        assert first["reason"] == (
            "NO_USABLE_CONTRACTS:QQQ:rows=500:NON_POSITIVE_ASK=500"
        )
        assert first["response_bytes"] > 0

        with cursor(pool) as database_cursor:
            database_cursor.execute(
                "SELECT state,outcome,http_status,response_bytes,row_count,"
                "pages_fetched,failure_code FROM rt.provider_request_ledger "
                "ORDER BY pages_fetched DESC,failure_code"
            )
            ledger = database_cursor.fetchall()
            database_cursor.execute(
                "SELECT status,next_retry_at,retry_reason,"
                "provider_request_count,response_bytes "
                "FROM rt.advanced_options_preparation_work "
                "WHERE ticker='QQQ' AND stage='CATALOG_QUOTE'"
            )
            work = database_cursor.fetchone()
            lifecycle = reconcile_advanced_options_lifecycle_cursor(
                database_cursor,
                target_session_date=SESSION_DATE,
                checked_at=checked_at,
            )
            database_cursor.execute(
                "SELECT last_error FROM "
                "rt.advanced_options_preparation_sessions"
            )
            last_error = database_cursor.fetchone()[0]
            database_cursor.connection.commit()
        used = [row for row in ledger if row[5] == 1]
        unused = [row for row in ledger if row[5] == 0]
        assert len(used) == 2
        assert {row[:3] for row in used} == {("FAILED", "INVALID", 200)}
        assert sum(row[3] for row in used) == first["response_bytes"]
        assert [row[4] for row in used] == [250, 250]
        assert len(unused) == 2
        assert {row[6] for row in unused} == {"RESERVATION_NOT_USED"}
        assert {row[3] for row in unused} == {0}
        assert work == (
            "RETRY",
            retry_floor,
            first["reason"],
            2,
            first["response_bytes"],
        )
        assert lifecycle.status == "PREPARING"
        assert last_error.startswith(first["reason"])

        # Before the open neither underlying can be retried, so a restart
        # loop finds no work and reserves nothing more.
        assert cycle(checked_at + timedelta(seconds=1))["ticker"] == "SPY"
        for offset in (2, 3, 4):
            idle = cycle(checked_at + timedelta(seconds=offset))
            assert idle["status"] == "IDLE"
        with cursor(pool) as database_cursor:
            database_cursor.execute(
                "SELECT count(*) FROM rt.provider_request_ledger"
            )
            assert database_cursor.fetchone()[0] == 8
            database_cursor.connection.commit()

        # After the open each underlying spends only its own three rounds.
        statuses = []
        at = retry_floor
        for _ in range(12):
            report = cycle(at)
            statuses.append((report["status"], report.get("ticker")))
            at += timedelta(minutes=10)
        assert statuses.count(("RETRY", "QQQ")) == 2
        assert statuses.count(("RETRY", "SPY")) == 2
        assert ("WAITING_BUDGET", "QQQ") in statuses
        assert ("WAITING_BUDGET", "SPY") in statuses
        with cursor(pool) as database_cursor:
            database_cursor.execute(
                "SELECT count(*),count(*) FILTER (WHERE pages_fetched=1),"
                "count(DISTINCT budget_scope_sha256) "
                "FROM rt.provider_request_ledger"
            )
            assert database_cursor.fetchone() == (24, 12, 1)
            database_cursor.execute(
                "SELECT ticker,next_retry_at,retry_reason "
                "FROM rt.advanced_options_preparation_work "
                "WHERE stage='CATALOG_QUOTE' ORDER BY ticker"
            )
            assert database_cursor.fetchall() == [
                ("QQQ", hard_stop, "PROVIDER_BUDGET_EXHAUSTED"),
                ("SPY", hard_stop, "PROVIDER_BUDGET_EXHAUSTED"),
            ]
            database_cursor.connection.commit()


def _exact_equity_snapshot(cursor, extra_ticker, *, identity_until):
    """A desired snapshot whose manifest declares exactly its AM members."""
    cursor.execute(
        "SELECT revision_id::text,recorded_at "
        "FROM rt.realtime_desired_revisions LIMIT 1"
    )
    revision_id, recorded_at = cursor.fetchone()
    cursor.execute(
        "SELECT manifest_id::text FROM rt.subscription_members "
        "WHERE channel='AM' GROUP BY manifest_id "
        "ORDER BY count(*) DESC LIMIT 1"
    )
    source_manifest_id = cursor.fetchone()[0]
    security_id = _security_id(extra_ticker)
    cursor.execute(
        "INSERT INTO ref.securities ("
        "security_id,asset_type,source_origin,created_at"
        ") VALUES (%s,'STOCK','c4-gate-fixture',%s)",
        (str(security_id), recorded_at - timedelta(days=1)),
    )
    cursor.execute(
        "INSERT INTO ref.security_revisions ("
        "revision_id,security_id,ticker,exchange,calendar_id,"
        "effective_from,effective_to,observed_at,payload_sha256,"
        "security_type,active"
        ") VALUES (%s,%s,%s,'XNYS','XNYS',%s,%s,%s,%s,'CS',true)",
        (
            str(uuid4()),
            str(security_id),
            extra_ticker,
            recorded_at - timedelta(hours=1),
            identity_until,
            recorded_at - timedelta(hours=1),
            _digest(f"security:{extra_ticker}"),
        ),
    )
    cursor.execute(
        "INSERT INTO rt.instruments (asset_class,security_id) "
        "VALUES ('EQUITY',%s) RETURNING instrument_id",
        (str(security_id),),
    )
    extra_instrument_id = int(cursor.fetchone()[0])
    cursor.execute(
        "SELECT count(*) FROM rt.subscription_members "
        "WHERE manifest_id=%s AND channel='AM'",
        (source_manifest_id,),
    )
    member_count = int(cursor.fetchone()[0]) + 1
    manifest_id = uuid4()
    cursor.execute(
        "INSERT INTO rt.subscription_manifests ("
        "manifest_id,manifest_sha256,universe_sha256,session_date,"
        "effective_at,profile_id,manifest_kind,stock_count,"
        "option_underlying_count,member_count,allocation_policy_sha256"
        ") SELECT %s,%s,universe_sha256,session_date,effective_at,"
        "profile_id,'EQUITY_VALIDATION',%s,0,%s,allocation_policy_sha256 "
        "FROM rt.subscription_manifests WHERE manifest_id=%s",
        (
            str(manifest_id),
            _digest(f"exact-manifest:{manifest_id}"),
            member_count,
            member_count,
            source_manifest_id,
        ),
    )
    cursor.execute(
        "INSERT INTO rt.subscription_members ("
        "manifest_id,instrument_id,channel,priority,purpose,"
        "option_underlying) SELECT %s,instrument_id,channel,priority,"
        "purpose,false FROM rt.subscription_members "
        "WHERE manifest_id=%s AND channel='AM'",
        (str(manifest_id), source_manifest_id),
    )
    cursor.execute(
        "INSERT INTO rt.subscription_members ("
        "manifest_id,instrument_id,channel,priority,purpose,"
        "option_underlying) VALUES (%s,%s,'AM',1,'PRIMARY',false)",
        (str(manifest_id), extra_instrument_id),
    )
    cohort_id = uuid4()
    cursor.execute(
        "INSERT INTO rt.cohorts ("
        "cohort_id,cohort_sha256,manifest_id,configuration_sha256,"
        "model_bundle_sha256,valuation_sha256,effective_at,mode"
        ") SELECT %s,%s,%s,configuration_sha256,model_bundle_sha256,"
        "valuation_sha256,effective_at,mode FROM rt.cohorts "
        "WHERE manifest_id=%s LIMIT 1",
        (
            str(cohort_id),
            _digest(f"exact-cohort:{cohort_id}"),
            str(manifest_id),
            source_manifest_id,
        ),
    )
    snapshot_sha256 = _digest(f"exact-snapshot:{cohort_id}")
    cursor.execute(
        "INSERT INTO rt.realtime_desired_session_snapshots ("
        "revision_id,session_date,manifest_id,cohort_id,"
        "snapshot_sha256,materialized_at"
        ") VALUES (%s,%s,%s,%s,%s,%s)",
        (
            revision_id,
            SESSION_DATE,
            str(manifest_id),
            str(cohort_id),
            snapshot_sha256,
            recorded_at,
        ),
    )
    cursor.execute(
        "SELECT DISTINCT reference.ticker,reference.security_id::text "
        "FROM ref.security_revisions AS reference "
        "JOIN rt.instruments AS instrument USING(security_id) "
        "JOIN rt.subscription_members AS member USING(instrument_id) "
        "WHERE member.manifest_id=%s AND member.channel='AM'",
        (str(manifest_id),),
    )
    return (
        str(cohort_id),
        snapshot_sha256,
        recorded_at,
        dict(cursor.fetchall()),
    )


def test_equity_reference_gate_uses_member_facts_at_the_cutoff():
    from scripts.run_realtime_v2_advanced_equity_window import (
        _continuous_reference_evidence_cursor as evidence,
    )

    extra = "ZGATE"
    with disposable_v2_database(
        f"test_realtime_v2_c8_reference_gate_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            _seed_session(
                database_cursor,
                materialize_desired_snapshot=False,
            )
            database_cursor.execute(
                "SELECT recorded_at FROM rt.realtime_desired_revisions LIMIT 1"
            )
            recorded_at = database_cursor.fetchone()[0]
            cutoff = recorded_at + timedelta(minutes=5)
            cohort_id, snapshot_sha256, materialized_at, members = (
                _exact_equity_snapshot(
                    database_cursor,
                    extra,
                    identity_until=cutoff + timedelta(hours=1),
                )
            )
            assert extra in members and len(members) > 1
            fresh = recorded_at - timedelta(minutes=30)

            def insert_coverage(ticker, action_type, *, at):
                suffix = uuid4().hex
                database_cursor.execute(
                    "INSERT INTO ref.corporate_action_coverage ("
                    "coverage_id,security_id,action_type,window_start,"
                    "window_end,source,source_key,response_action_count,"
                    "response_sha256,observed_at,received_at,payload_sha256"
                    ") VALUES (%s,%s,%s,%s,%s,'massive',%s,0,%s,%s,%s,%s)",
                    (
                        str(uuid4()),
                        members[ticker],
                        action_type,
                        SESSION_DATE - timedelta(days=180),
                        SESSION_DATE + timedelta(days=365),
                        f"{ticker}:{action_type}:{suffix}",
                        _digest(f"response-{suffix}"),
                        at,
                        at,
                        _digest(f"payload-{suffix}"),
                    ),
                )

            def gate(at=cutoff, cohort=cohort_id):
                return evidence(
                    database_cursor,
                    SimpleNamespace(
                        cohort_id=cohort,
                        session_date=SESSION_DATE,
                        cutoff=at,
                    ),
                )

            for ticker in members:
                if ticker != extra:
                    insert_coverage(ticker, "SPLIT", at=fresh)
                    insert_coverage(ticker, "DIVIDEND", at=fresh)
            insert_coverage(extra, "SPLIT", at=fresh)
            # One member lacks DIVIDEND coverage: closed, and no seed fallback.
            with pytest.raises(ValueError, match="continuous member"):
                gate()
            # Coverage older than the member's current identity is stale.
            insert_coverage(
                extra,
                "DIVIDEND",
                at=recorded_at - timedelta(hours=2),
            )
            with pytest.raises(ValueError, match="continuous member"):
                gate()
            # Coverage received after the cutoff was not yet known.
            insert_coverage(
                extra,
                "DIVIDEND",
                at=cutoff + timedelta(seconds=1),
            )
            with pytest.raises(ValueError, match="continuous member"):
                gate()
            insert_coverage(extra, "DIVIDEND", at=fresh)
            assert gate() == f"advanced-reference-c4:{snapshot_sha256}"
            # The member's identity expires on its own clock.
            with pytest.raises(ValueError, match="continuous member"):
                gate(at=cutoff + timedelta(hours=1))
            # A cohort that is not a desired snapshot stays on the seed gate.
            assert gate(cohort=str(uuid4())) is None
            # A snapshot is not authority before it was materialized.
            assert gate(at=materialized_at - timedelta(seconds=1)) is None
            database_cursor.connection.rollback()


def test_c8_obsolete_unfinished_session_expires_once_and_is_not_reclaimed():
    with disposable_v2_database(
        f"test_realtime_v2_c8_expired_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(
                database_cursor,
                options_ready=False,
            )
            checked_at = seed[
                "authority_bundle"
            ].session.prepared_at
            initial = reconcile_advanced_options_lifecycle_cursor(
                database_cursor,
                target_session_date=SESSION_DATE,
                checked_at=checked_at,
            )
            database_cursor.connection.commit()
        provider = _provider(pool)
        executor = _AutomaticC8Executor(pool, seed)
        expired_at = (
            OptionExchangeCalendar("XNYS").session_close(
                SESSION_DATE
            )
            + timedelta(minutes=6)
        )

        first = run_advanced_options_preparation_cycle(
            provider,
            executor,
            owner_id=uuid4(),
            checked_at=expired_at,
            clock=lambda: expired_at,
        )
        second = run_advanced_options_preparation_cycle(
            provider,
            executor,
            owner_id=uuid4(),
            checked_at=expired_at + timedelta(minutes=10),
            clock=lambda: expired_at,
        )
        with cursor(pool) as database_cursor:
            lifecycle = reconcile_advanced_options_lifecycle_cursor(
                database_cursor,
                target_session_date=SESSION_DATE,
                checked_at=expired_at + timedelta(minutes=10),
            )
            summary = build_advanced_realtime_v3_status_cursor(
                database_cursor,
                checked_at=expired_at + timedelta(minutes=10),
            )
            database_cursor.execute(
                "SELECT DISTINCT status,expiration_state,"
                "expiration_reason,lease_owner,lease_until "
                "FROM rt.advanced_options_preparation_work"
            )
            work = tuple(database_cursor.fetchall())
            database_cursor.execute(
                "SELECT status,expiration_state,expiration_reason "
                "FROM rt.advanced_options_preparation_sessions"
            )
            session = database_cursor.fetchone()
            database_cursor.execute(
                "SELECT count(*) FROM "
                "rt.advanced_options_preparation_evidence"
            )
            evidence_count = int(database_cursor.fetchone()[0])
            database_cursor.connection.commit()

        assert first["status"] == "IDLE"
        assert first["expired_sessions"] == [{
            "revision_id": str(initial.revision_id),
            "session_date": SESSION_DATE.isoformat(),
            "expiration_state": "EXPIRED",
            "expired_work_count": 9,
        }]
        assert second["status"] == "IDLE"
        assert second["expired_sessions"] == []
        assert work == ((
            "BLOCKED",
            "EXPIRED",
            f"EXPIRED_OPTIONS_PREPARATION:{SESSION_DATE.isoformat()}",
            None,
            None,
        ),)
        assert session == (
            "DEGRADED",
            "EXPIRED",
            f"EXPIRED_OPTIONS_PREPARATION:{SESSION_DATE.isoformat()}",
        )
        assert evidence_count == 0
        assert lifecycle is not None
        assert lifecycle.status == "EXPIRED"
        expired_record = summary["expired_preparation"][0]
        assert expired_record["session_date"] == SESSION_DATE.isoformat()
        assert expired_record["expiration_state"] == "EXPIRED"
        assert expired_record["expired_at_utc"] == expired_at.isoformat()


def test_equity_only_addition_does_not_change_options_activation():
    with disposable_v2_database(
        f"test_realtime_v2_c5_equity_superset_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(
                database_cursor,
                extra_equity_tickers=("XLF",),
            )
            database_cursor.connection.commit()

        repository = PostgresContinuousSessionStateRepository(
            _provider(pool),
            session_date=SESSION_DATE,
        )
        binding = repository.activate_session(
            package=seed["package"],
            authority_bundle=seed["authority_bundle"],
            acquisition_binding=seed["acquisition"],
            reference=seed["reference"],
            sealed_selection=seed["sealed_selection"],
            written_at=seed["authority_bundle"].session.prepared_at,
        )

        assert binding.cohort_id == seed[
            "authority_bundle"
        ].session.cohort_id
        assert binding.contract_tickers == seed[
            "acquisition"
        ].contract_tickers
        assert tuple(
            seed["package"]["equity_input_binding"]["symbols"]
        ) == EQUITY_VALIDATION_UNIVERSE.equity_tickers
        assert "XLF" not in seed[
            "package"
        ]["equity_input_binding"]["symbols"]


def test_options_activation_requires_equity_context_for_each_underlying():
    with disposable_v2_database(
        f"test_realtime_v2_c5_missing_equity_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(database_cursor)
            database_cursor.connection.commit()
        package = {
            **seed["package"],
            "equity_input_binding": {"symbols": ["SPY"]},
        }
        package.pop("package_sha256")
        package["package_sha256"] = canonical_sha256(package)
        repository = PostgresContinuousSessionStateRepository(
            _provider(pool),
            session_date=SESSION_DATE,
        )

        with pytest.raises(
            ValueError,
            match="READY desired and package Equity context",
        ):
            repository.activate_session(
                package=package,
                authority_bundle=seed["authority_bundle"],
                acquisition_binding=seed["acquisition"],
                reference=seed["reference"],
                sealed_selection=seed["sealed_selection"],
                written_at=(
                    seed["authority_bundle"].session.prepared_at
                ),
            )


def test_document_activation_reconstructs_exact_readiness(
    monkeypatch,
):
    with disposable_v2_database(
        f"test_realtime_v2_c5_documents_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(database_cursor)
            database_cursor.connection.commit()
        monkeypatch.setattr(
            session_state_module.PostgresAdvancedOptionsReferenceStateRepository,
            "load_for_session",
            lambda *_args, **_kwargs: object(),
        )
        monkeypatch.setattr(
            session_state_module.OpeningReferenceEvidence,
            "from_reference_state",
            classmethod(
                lambda _cls, _state: seed["reference"]
            ),
        )
        repository = PostgresContinuousSessionStateRepository(
            _provider(pool),
            session_date=SESSION_DATE,
        )
        authority = seed["authority_bundle"]

        session_binding = repository.activate_session_from_documents(
            package=seed["package"],
            pre_session_authority={
                "session_date": SESSION_DATE.isoformat(),
                "target_cohort_id": str(
                    authority.session.cohort_id
                ),
                "admission": prospective_opening_admission_payload(
                    authority.admission
                ),
                "readiness_sha256": (
                    authority.readiness.readiness_sha256
                ),
                "session_acquisition": (
                    session_acquisition_authorization_payload(
                        authority.session
                    )
                ),
            },
            acquisition_binding=seed["acquisition"],
            sealed_selection=seed["sealed_selection"],
            written_at=authority.session.prepared_at,
        )

        assert session_binding == (
            repository.load_preopen_session_binding(
                repository.load_execution_bundle()
            )
        )


def test_prior_session_reference_is_rejected_before_activation():
    with disposable_v2_database(
        f"test_realtime_v2_c5_stale_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(database_cursor)
            database_cursor.connection.commit()
        stale = seed["reference"].__class__(
            state_id=seed["reference"].state_id,
            state_sha256=seed["reference"].state_sha256,
            reference_generation=seed["reference"].reference_generation,
            session_date=OptionExchangeCalendar().previous_session_date(
                SESSION_DATE
            ),
            known_at=seed["reference"].known_at,
            valid_until=seed["reference"].valid_until,
            reference_policy_sha256=(
                seed["reference"].reference_policy_sha256
            ),
            contract_scope_sha256=(
                seed["reference"].contract_scope_sha256
            ),
            open_interest_sha256=(
                seed["reference"].open_interest_sha256
            ),
            open_interest=seed["reference"].open_interest,
        )
        repository = PostgresContinuousSessionStateRepository(
            _provider(pool),
            session_date=SESSION_DATE,
        )

        with pytest.raises(ValueError, match="reference/OI"):
            repository.activate_session(
                package=seed["package"],
                authority_bundle=seed["authority_bundle"],
                acquisition_binding=seed["acquisition"],
                reference=stale,
                sealed_selection=seed["sealed_selection"],
                written_at=seed["authority_bundle"].session.prepared_at,
            )
        with cursor(pool) as database_cursor:
            database_cursor.execute(
                "SELECT count(*) FROM "
                "rt.advanced_options_continuous_sessions"
            )
            assert database_cursor.fetchone()[0] == 0


def test_current_session_catalog_and_oi_divergence_are_rejected():
    with disposable_v2_database(
        f"test_realtime_v2_c5_freshness_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(
                database_cursor,
                stale_catalog=True,
            )
            database_cursor.connection.commit()
        repository = PostgresContinuousSessionStateRepository(
            _provider(pool),
            session_date=SESSION_DATE,
        )
        with pytest.raises(ValueError, match="catalog version"):
            repository.activate_session(
                package=seed["package"],
                authority_bundle=seed["authority_bundle"],
                acquisition_binding=seed["acquisition"],
                reference=seed["reference"],
                sealed_selection=seed["sealed_selection"],
                written_at=seed["authority_bundle"].session.prepared_at,
            )

    with disposable_v2_database(
        f"test_realtime_v2_c5_oi_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(database_cursor)
            database_cursor.connection.commit()
        changed_member = replace(
            seed["reference"].open_interest[0],
            open_interest=(
                seed["reference"].open_interest[0].open_interest + 1
            ),
        )
        changed_reference = replace(
            seed["reference"],
            open_interest=(
                changed_member,
                *seed["reference"].open_interest[1:],
            ),
        )
        repository = PostgresContinuousSessionStateRepository(
            _provider(pool),
            session_date=SESSION_DATE,
        )
        with pytest.raises(ValueError, match="reference/OI"):
            repository.activate_session(
                package=seed["package"],
                authority_bundle=seed["authority_bundle"],
                acquisition_binding=seed["acquisition"],
                reference=changed_reference,
                sealed_selection=seed["sealed_selection"],
                written_at=seed["authority_bundle"].session.prepared_at,
            )


def test_downstream_append_replay_prefix_concurrent_and_rehydrates():
    with disposable_v2_database(
        f"test_realtime_v2_c5_append_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(database_cursor)
            database_cursor.connection.commit()
        repository, session_binding = _activate(pool, seed)
        with cursor(pool) as database_cursor:
            first = _seed_downstream_binding(
                database_cursor,
                seed,
                session_binding,
                sequence=1,
            )
            second = _seed_downstream_binding(
                database_cursor,
                seed,
                session_binding,
                sequence=2,
                previous_binding_sha256=first.binding_sha256,
            )
            database_cursor.connection.commit()

        assert repository.load_latest_downstream_binding(
            session_binding
        ) is None
        assert repository.attach_downstream_binding(
            session_binding,
            first,
        ) == first
        workers = [
            PostgresContinuousSessionStateRepository(
                _provider(pool),
                session_date=SESSION_DATE,
                head_poll_interval_seconds=0.001,
            )
            for _ in range(2)
        ]
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = tuple(executor.map(
                lambda state: state.attach_downstream_binding(
                    session_binding,
                    second,
                ),
                workers,
            ))

        assert results == (second, second)
        restarted = PostgresContinuousSessionStateRepository(
            _provider(pool),
            session_date=SESSION_DATE,
            head_poll_interval_seconds=0.001,
        )
        assert restarted.load_downstream_bindings(
            session_binding
        ) == (first, second)
        assert restarted.load_latest_downstream_binding(
            session_binding
        ) == second
        assert restarted.attach_downstream_binding(
            session_binding,
            first,
        ) == first
        assert restarted.load_latest_downstream_binding(
            session_binding
        ) == second
        replay_repository, replay_session = _activate(pool, seed)
        assert replay_session == session_binding
        assert replay_repository.load_latest_downstream_binding(
            session_binding
        ) == second
        with cursor(pool) as database_cursor:
            database_cursor.execute(
                "SELECT sequence,binding_sha256 FROM "
                "rt.advanced_options_downstream_heads"
            )
            assert database_cursor.fetchone() == (
                2,
                second.binding_sha256,
            )


def test_downstream_crash_retry_and_unpointed_tail_recovery():
    with disposable_v2_database(
        f"test_realtime_v2_c5_recovery_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(database_cursor)
            database_cursor.connection.commit()
        repository, session_binding = _activate(pool, seed)
        with cursor(pool) as database_cursor:
            binding = _seed_downstream_binding(
                database_cursor,
                seed,
                session_binding,
                sequence=1,
            )
            database_cursor.connection.commit()

        connection = pool.getconn()
        try:
            with connection.cursor() as database_cursor:
                database_cursor.execute(
                    "INSERT INTO rt.advanced_options_downstream_bindings ("
                    "session_binding_id,sequence,binding_id,"
                    "binding_sha256,previous_binding_sha256,"
                    "target_window_id,equity_receipt_id,"
                    "finalized_cutoff,options_cutoff,binding_document"
                    ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        str(session_binding.binding_id),
                        binding.sequence,
                        str(binding.binding_id),
                        binding.binding_sha256,
                        binding.previous_binding_sha256,
                        str(binding.target_window_id),
                        str(binding.equity_receipt_id),
                        binding.finalized_cutoff,
                        binding.options_cutoff,
                        Json(p8_downstream_binding_payload(binding)),
                    ),
                )
            connection.rollback()
        finally:
            pool.putconn(connection)

        assert repository.attach_downstream_binding(
            session_binding,
            binding,
        ) == binding
        with cursor(pool) as database_cursor:
            database_cursor.execute(
                "UPDATE rt.advanced_options_downstream_heads "
                "SET sequence=0,binding_sha256=NULL "
                "WHERE session_binding_id=%s",
                (str(session_binding.binding_id),),
            )
            database_cursor.connection.commit()

        recovery = PostgresContinuousSessionStateRepository(
            _provider(pool),
            session_date=SESSION_DATE,
            head_poll_interval_seconds=0.001,
        )
        assert recovery.attach_downstream_binding(
            session_binding,
            binding,
        ) == binding
        assert recovery.load_latest_downstream_binding(
            session_binding
        ) == binding
        with cursor(pool) as database_cursor:
            database_cursor.execute(
                "SELECT count(*) FROM "
                "rt.advanced_options_downstream_bindings"
            )
            assert database_cursor.fetchone()[0] == 1
