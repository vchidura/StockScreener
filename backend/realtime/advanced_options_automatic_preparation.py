"""Automatic C8 Options preparation using stored Equity context."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import Json, RealDictCursor
from dataclasses import replace

from options.calendar import OptionExchangeCalendar
from options.config import load_valuation_policy
from options.data.polygon_http import SingleAttemptPolygonHttpTransport

from scripts.apply_advanced_options_p8_catalog_hydration import (
    _current_rows,
    _diff,
    _entries,
    apply_manifest_cursor,
)
from scripts.apply_advanced_options_v3_pre_session_capture import (
    _hydration_manifest,
)
from scripts.prepare_advanced_options_v3_pre_session import (
    _PostgresSelectionSpots,
    _activity_candidate,
    _expiry_calendar_evidence,
)
from scripts.run_advanced_options_v3_market_loop import (
    _prepare_session_authority,
)

from .acquisition_ledger import ProviderRequestLane
from .advanced_options_acquisition_runtime import (
    acquisition_binding_payload,
    build_acquisition_binding,
)
from .advanced_options_control_plane import (
    ADVANCED_OPTIONS_REST_LANES,
    AdvancedOptionsSocketBudget,
    advanced_options_history_binding_payload,
    build_advanced_options_history_binding,
)
from .advanced_options_flow_policy import (
    EXPECTED_UNDERLYINGS,
    load_advanced_options_flow_policy,
)
from .advanced_options_activity_momentum_c6 import (
    ACTIVITY_MOMENTUM_PROFILE_VERSION,
    LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    activity_momentum_allocation_from_payload,
    activity_momentum_allocation_payload,
    allocate_activity_momentum,
    c6_coverage_from_activity_allocation,
    c6_members_from_activity_allocation,
    load_activity_momentum_profile,
    resolve_activity_momentum_selection,
)
from .advanced_options_lifecycle import (
    AdvancedOptionsPreparationClaim,
    AdvancedOptionsPreparationExecutor,
    AdvancedOptionsPreparationProviderFailed,
    AdvancedOptionsPreparationRefreshRequired,
    AdvancedOptionsPreparationRetryable,
    AdvancedOptionsPreparationTerminal,
    AdvancedOptionsProviderRequestAttempt,
    AdvancedOptionsProviderRequestPlan,
    AdvancedOptionsProviderRequestResult,
    AdvancedOptionsStageProviderBudget,
    AdvancedOptionsStageResult,
)
from .advanced_options_p7_selection import P7TransportSelectionPolicy
from .advanced_options_p8_selection_source import (
    MassiveAdvancedOptionsSelectionSource,
    P8SelectionNoUsableContracts,
    SEALED_CAPTURE_EXACT_FOUR_PROFILE,
    SEALED_CAPTURE_GENERALIZED_C6_PROFILE,
    p8_activity_selection_capture_from_sealed,
)
from .advanced_options_reference_producer import (
    load_opening_reference_inputs_cursor,
)
from .equity_history import (
    EquityHistoryIdentityError,
    equity_history_coordinates,
    load_equity_history_cursor,
)
from .advanced_options_reference_repository import (
    decode_advanced_options_reference_state,
    persist_advanced_options_reference_state_cursor,
)
from .advanced_options_reference_state import (
    AdvancedOptionsOpenInterestReference,
    ReferenceAvailability,
    build_advanced_options_reference_state,
)
from .advanced_options_selection_c6 import (
    C6ConnectionCapacity,
    C6ReadinessState,
    C6SelectionMember,
    C6UnderlyingReadiness,
    OptionSide,
    build_c6_selection_plan,
    load_advanced_options_selection_c6_policy,
    load_strategy_policy_identity,
)
from .advanced_options_service import OPTIONS_V3_STAGE_ORDER
from .advanced_options_session_state import (
    PostgresContinuousSessionStateRepository,
)
from .advanced_options_v3_authority import (
    AdvancedOptionsAuthorityBundle,
    OpeningOpenInterestEvidence,
    OpeningReferenceEvidence,
    _opening_admission,
    advanced_options_authority_bundle_payload,
    build_opening_subscription_readiness,
    build_session_acquisition_authorization,
    prospective_opening_admission_payload,
)
from .contracts import UniverseManifest, canonical_sha256
from .preflight import ProviderTier, TierState
from .tier_cohort import (
    TierCohortBinding,
    persist_tier_cohort_binding_cursor,
    product_capabilities,
    product_disabled_detectors,
)
from .database_access_v2 import APP_ROLE, V2ConnectionProvider
from .option_source_flow import persist_option_flow_policy_cursor
from .provider_budget import (
    MASSIVE_ADVANCED_ACCOUNT_DECODED_BYTE_CEILING,
    MASSIVE_ADVANCED_ACCOUNT_REQUEST_CEILING,
    ProviderBudgetScope,
)
from .reference_repository import persist_reference_facts_cursor
from .reference_facts import (
    ReferenceFact,
    ReferenceFactRelation,
)
from .reference_seed import ReferenceKind
from .stage_graph import STAGE_CATALOG_VERSION_V5
from .option_slot_set import (
    MarketChannel,
    SlotSetKind,
    SlotSetMember,
    SlotSetPurpose,
    SlotSetVersion,
)


UTC = timezone.utc
_C6_STRATEGY_POLICY_PATH = (
    Path(__file__).resolve().parents[1]
    / "options"
    / "policies"
    / "strategy_o3_credit_v2.json"
)
_C8_MODEL_BUNDLE_SHA256 = canonical_sha256({"model": "C8_OPTIONS_V3"})
_C8_VALUATION_POLICY_SHA256 = load_valuation_policy(
    Path(__file__).resolve().parents[1]
    / "options" / "policies" / "valuation_advanced_nbbo_v1.json"
).policy_sha256


def c8_options_tier_binding(*, cohort_id, profile_id, manifest_sha256, effective_at):
    """The MASSIVE_ADVANCED tier binding of a V3 Options (C8) cohort."""
    tier = ProviderTier.MASSIVE_ADVANCED
    return TierCohortBinding(
        cohort_id=cohort_id,
        profile_id=profile_id,
        provider_tier=tier,
        ready_state=TierState.ADVANCED_READY,
        source_manifest_sha256=manifest_sha256,
        valuation_policy_sha256=_C8_VALUATION_POLICY_SHA256,
        capabilities=product_capabilities(tier, tier),
        disabled_detectors=product_disabled_detectors(tier, tier),
        effective_at=effective_at,
        stocks_tier=tier,
        options_tier=tier,
        options_enabled=True,
    )


_LOGGER = logging.getLogger(__name__)


def _isolated_activity_candidates(
    rows,
    snapshots,
    *,
    session_date,
    observed_at,
    flow_policy,
    build=None,
):
    """R16 follow-up (INV-7): a contract the activity candidate refuses
    (Nov 10: a provider delta of 1.0000000000047) is left out of the
    allocation and logged with its reason, instead of failing every
    underlying's CAPACITY_SHARDS and restarting the lifecycle. Returns the
    kept candidates in row order."""
    build = _activity_candidate if build is None else build
    candidates = []
    rejected: dict[str, int] = {}
    examples: list[str] = []
    for row in rows:
        try:
            candidates.append(build(
                row,
                snapshots[row.contract_ticker],
                session_date=session_date,
                observed_at=observed_at,
                flow_policy=flow_policy,
            ))
        except (ArithmeticError, KeyError, TypeError, ValueError) as error:
            reason = type(error).__name__ + ": " + str(error)[:120]
            rejected[reason] = rejected.get(reason, 0) + 1
            if len(examples) < 10:
                examples.append(str(row.contract_ticker))
    if rejected:
        _LOGGER.warning(
            "advanced_options_activity_candidates_rejected session=%s "
            "kept=%d rejected=%d reasons=%s examples=%s",
            session_date,
            len(candidates),
            sum(rejected.values()),
            json.dumps(rejected, sort_keys=True),
            ",".join(examples),
        )
    return tuple(candidates)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _catalog_failure_reason(error: Exception) -> str:
    if isinstance(error, P8SelectionNoUsableContracts):
        return error.reason_code
    if isinstance(error, (ValueError, RuntimeError)):
        return f"{type(error).__name__}:{error}"
    return type(error).__name__


class PostgresAutomaticOptionsPreparationExecutor(
    AdvancedOptionsPreparationExecutor
):
    def __init__(
        self,
        provider: V2ConnectionProvider,
        *,
        api_key: str,
        backend_image_id: str,
        maximum_provider_requests_per_underlying: int = 4,
        maximum_decoded_bytes_per_underlying: int = 25_000_000,
        maximum_contracts_per_logical_shard: int = 250,
        admission_profile_version: str = (
            ACTIVITY_MOMENTUM_PROFILE_VERSION
        ),
        active_quote_contract_budget: int | None = None,
        per_underlying_contract_limit: int | None = None,
        measured_capacity_limit: int | None = None,
        effective_provider_project_ceiling: int | None = None,
        enabled_strategies: tuple[str, ...] = (),
        exact_four_compatibility: bool = False,
        capture_after_open_seconds: int | None = None,
        connection_attempt_ceiling: int = 12,
        decoded_message_ceiling: int = 1_000_000,
        decoded_byte_ceiling: int = 1_073_741_824,
        clock=lambda: datetime.now(UTC),
        selection_source_factory=None,
        combined_capture_factory=None,
    ) -> None:
        if (
            not isinstance(provider, V2ConnectionProvider)
            or not api_key.strip()
            or not backend_image_id.strip()
            or not 1 <= maximum_provider_requests_per_underlying <= 32
            or maximum_decoded_bytes_per_underlying <= 0
            or maximum_contracts_per_logical_shard <= 0
            or admission_profile_version
            not in {
                LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION,
                ACTIVITY_MOMENTUM_PROFILE_VERSION,
                SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION,
            }
            or (
                exact_four_compatibility
                and admission_profile_version
                != LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION
            )
            or type(connection_attempt_ceiling) is not int
            or connection_attempt_ceiling <= 0
            or type(decoded_message_ceiling) is not int
            or decoded_message_ceiling <= 0
            or type(decoded_byte_ceiling) is not int
            or decoded_byte_ceiling <= 0
        ):
            raise ValueError(
                "automatic Options preparation configuration is invalid"
            )
        self._provider = provider
        self._api_key = api_key.strip()
        self._backend_image_id = backend_image_id.strip()
        self._request_limit = maximum_provider_requests_per_underlying
        self._byte_limit = maximum_decoded_bytes_per_underlying
        self._capture_after_open_seconds = capture_after_open_seconds
        self._maximum_contracts_per_logical_shard = (
            maximum_contracts_per_logical_shard
        )
        self._admission_profile_version = admission_profile_version
        self._active_quote_contract_budget = (
            active_quote_contract_budget
        )
        self._per_underlying_contract_limit = (
            per_underlying_contract_limit
        )
        self._measured_capacity_limit = measured_capacity_limit
        self._effective_provider_project_ceiling = (
            effective_provider_project_ceiling
        )
        self._enabled_strategies = tuple(enabled_strategies)
        self._exact_four_compatibility = exact_four_compatibility
        self._connection_attempt_ceiling = connection_attempt_ceiling
        self._decoded_message_ceiling = decoded_message_ceiling
        self._decoded_byte_ceiling = decoded_byte_ceiling
        self._clock = clock
        self._selection_source_factory = selection_source_factory
        self._combined_capture_factory = combined_capture_factory
        self._catalog_capture = None

    def provider_attempts(
        self,
        claim: AdvancedOptionsPreparationClaim,
    ) -> tuple[AdvancedOptionsProviderRequestAttempt, ...] | None:
        """Exact requests of this claim's capture; None when unknowable."""
        if (
            self._catalog_capture is None
            or self._catalog_capture[0] is not claim
        ):
            return None
        attempts = getattr(
            self._catalog_capture[1],
            "provider_attempts",
            None,
        )
        if attempts is None:
            return None
        return tuple(
            AdvancedOptionsProviderRequestAttempt(
                row.http_status,
                row.response_bytes,
                row.row_count,
            )
            for row in attempts
        )

    def provider_budget(
        self,
        claim: AdvancedOptionsPreparationClaim,
    ) -> AdvancedOptionsStageProviderBudget | None:
        if claim.stage != "CATALOG_QUOTE":
            return None
        if not self._capture_is_open(claim):
            return None
        request_count = self._request_limit
        scope = ProviderBudgetScope(
            account_alias="MASSIVE_ADVANCED",
            session_date=claim.session_date,
            authorization_reference=(
                f"advanced-realtime-v3-options:{claim.revision_id}"
            ),
            provider_authorization_reference=(
                "advanced-realtime-v3-options-preparation"
            ),
            expected_cost="bounded Options catalog/quote preparation",
            request_ceiling=(
                self._request_limit
                * len(claim.option_underlyings)
                * 3
            ),
            decoded_byte_ceiling=(
                self._byte_limit
                * len(claim.option_underlyings)
                * 3
            ),
            account_request_ceiling=(
                MASSIVE_ADVANCED_ACCOUNT_REQUEST_CEILING
            ),
            account_decoded_byte_ceiling=(
                MASSIVE_ADVANCED_ACCOUNT_DECODED_BYTE_CEILING
            ),
            lanes=frozenset({ProviderRequestLane.OPTIONS_CHAIN}),
            authorized_until=(
                OptionExchangeCalendar("XNYS").session_close(
                    claim.session_date
                )
                + timedelta(minutes=5)
            ),
        )
        return AdvancedOptionsStageProviderBudget(
            scope,
            tuple(
                AdvancedOptionsProviderRequestPlan(
                    ProviderRequestLane.OPTIONS_CHAIN,
                    "OPTIONS_CHAIN_SNAPSHOT",
                    canonical_sha256({
                        "revision_id": claim.revision_id,
                        "ticker": claim.ticker,
                        "stage": claim.stage,
                        "slot": index,
                    }),
                    max(1, self._byte_limit // request_count),
                )
                for index in range(1, request_count + 1)
            ),
            underlying_request_ceiling=self._request_limit * 3,
        )

    def _source(self, claim):
        if self._selection_source_factory is not None:
            return self._selection_source_factory(claim)
        selection_policy = load_advanced_options_selection_c6_policy()
        profile = load_activity_momentum_profile(
            selection_policy=selection_policy,
            version=self._admission_profile_version,
        )
        discovery_maximum_dte = (
            profile.strategy_discovery_maximum_dte
            if self._enabled_strategies
            else profile.default_discovery_maximum_dte
        )
        discovery_moneyness = (
            max(
                criterion.maximum_moneyness_fraction
                for criterion in profile.selection_criteria
                if criterion.minimum_dte <= discovery_maximum_dte
                and criterion.maximum_dte
                >= profile.default_discovery_minimum_dte
            )
            if profile.selection_criteria
            else Decimal("0.05")
        )
        hard_stop = (
            OptionExchangeCalendar("XNYS").session_close(
                claim.session_date
            )
            + timedelta(minutes=5)
        )
        # One HTTP attempt per reserved slot: a transparent retry would be a
        # request the ledger and the provider budget never see.
        transport = SingleAttemptPolygonHttpTransport(
            self._api_key,
            deadline=hard_stop,
            timeout_seconds=30,
            maximum_response_bytes=self._byte_limit,
            clock=self._clock,
        )
        return MassiveAdvancedOptionsSelectionSource(
            transport,
            _PostgresSelectionSpots(
                self._provider,
                claim.source_equity_cohort_id,
            ),
            policy=P7TransportSelectionPolicy(
                version="advanced-realtime-v3-c8-preparation-v1",
                underlyings=(claim.ticker,),
                minimum_dte=(
                    profile.default_discovery_minimum_dte
                ),
                maximum_dte=discovery_maximum_dte,
                maximum_moneyness_fraction=min(
                    discovery_moneyness,
                    Decimal("0.10"),
                ),
            ),
            clock=self._clock,
            maximum_response_decoded_bytes=self._byte_limit,
            maximum_total_decoded_bytes=self._byte_limit,
            maximum_provider_requests=self._request_limit,
            retain_candidate_pool=not self._exact_four_compatibility,
            query_minimum_dte=(
                profile.default_discovery_minimum_dte
            ),
            query_maximum_dte=discovery_maximum_dte,
            query_maximum_moneyness_fraction=discovery_moneyness,
            deadline=hard_stop,
        )

    def _capture_document(self, claim, capture):
        reviewed_at = capture.selection.reviewed_at
        responses = [{
            "underlyer": claim.ticker,
            "page_count": capture.request_count,
            "row_count": len(capture.selected_snapshots),
            "response_sha256": canonical_sha256(
                capture.evidence
            ),
        }]
        document = {
            "contract": "advanced-options-v3-pre-session-capture-v1",
            "compatibility_profile": (
                SEALED_CAPTURE_EXACT_FOUR_PROFILE
                if self._exact_four_compatibility
                else SEALED_CAPTURE_GENERALIZED_C6_PROFILE
            ),
            "configured_underlyings": [claim.ticker],
            "captured_at_utc": reviewed_at.isoformat(),
            "session_date": claim.session_date.isoformat(),
            "source_cohort_id": str(
                claim.source_equity_cohort_id
            ),
            "provider_requests": capture.request_count,
            "decoded_bytes": capture.decoded_bytes,
            "responses": responses,
            "selection_sha256": (
                capture.selection.selection_sha256
            ),
            "selected_snapshots": dict(
                capture.selected_snapshots
            ),
        }
        document["semantic_sha256"] = canonical_sha256(document)
        return document

    def _capture_opens_at(self, claim) -> datetime | None:
        if self._capture_after_open_seconds is None:
            return None
        return OptionExchangeCalendar("XNYS").session_open(
            claim.session_date
        ) + timedelta(seconds=self._capture_after_open_seconds)

    def _capture_is_open(self, claim) -> bool:
        checked = _utc(self._clock(), "preparation clock")
        opens_at = self._capture_opens_at(claim)
        return checked.date() == claim.session_date and (
            opens_at is None or checked >= opens_at
        )

    def _capture_wait(self, claim):
        """Anything captured before the open would go stale, so spend no budget
        yet, but still report now whether the history the session needs exists."""
        try:
            self._history(claim)
        except AdvancedOptionsPreparationRetryable as error:
            detail = str(error)
        else:
            detail = "history ready"
        opens_at = self._capture_opens_at(claim)
        checked = _utc(self._clock(), "preparation clock")
        return AdvancedOptionsPreparationRetryable(
            f"target-session provider window is not open; {detail}",
            retry_at=(
                None
                if opens_at is None
                else min(opens_at, checked + timedelta(seconds=300))
            ),
        )

    def _catalog_quote(self, claim, provider_request_ids):
        if not self._capture_is_open(claim):
            raise self._capture_wait(claim)
        if not provider_request_ids:
            raise AdvancedOptionsPreparationTerminal(
                "catalog stage has no provider reservations"
            )
        source = self._source(claim)
        self._catalog_capture = (claim, source)
        try:
            return self._capture_catalog(
                claim,
                source,
                provider_request_ids,
            )
        except (
            AdvancedOptionsPreparationRetryable,
            AdvancedOptionsPreparationTerminal,
        ):
            raise
        except Exception as error:
            raise AdvancedOptionsPreparationProviderFailed(
                _catalog_failure_reason(error)
            ) from error

    def _capture_catalog(self, claim, source, provider_request_ids):
        capture = source.capture(
            reviewed_at=_utc(self._clock(), "selection clock"),
            session_date=claim.session_date,
        )
        if capture.request_count > len(provider_request_ids):
            raise AdvancedOptionsPreparationTerminal(
                "provider requests exceeded pre-reserved scope"
            )
        document = self._capture_document(claim, capture)
        hydration_document = {
            **document,
            "selected_snapshots": {
                ticker: {
                    **raw,
                    "details": {
                        **raw["details"],
                        "exercise_style": raw["details"].get(
                            "exercise_style",
                            "american",
                        ),
                        "shares_per_contract": raw["details"].get(
                            "shares_per_contract",
                            100,
                        ),
                    },
                }
                for ticker, raw in document[
                    "selected_snapshots"
                ].items()
            },
        }
        manifest = _hydration_manifest(hydration_document)
        captured_at = datetime.fromisoformat(
            document["captured_at_utc"]
        )
        settlement = OptionExchangeCalendar(
            "XNYS"
        ).previous_session_date(claim.session_date)
        with self._provider.transaction(
            APP_ROLE,
            isolation_level="SERIALIZABLE",
            dict_cursor=True,
        ) as unit:
            entries = _entries(
                manifest,
                expected_candidate_count=len(
                    document["selected_snapshots"]
                ),
            )
            current = _current_rows(
                unit.cursor,
                tuple(
                    entry.catalog.contract_ticker
                    for entry, _security_id in entries
                ),
            )
            expected = _diff(entries, current)
            if expected["conflicts"]:
                raise AdvancedOptionsPreparationTerminal(
                    "catalog identity conflicts"
                )
            effects = apply_manifest_cursor(
                unit.cursor,
                manifest,
                expected_contract_inserts=expected[
                    "contract_inserts"
                ],
                expected_version_inserts=expected[
                    "version_inserts"
                ],
                expected_instrument_inserts=expected[
                    "instrument_inserts"
                ],
                expected_candidate_count=len(entries),
            )
            oi_facts = []
            for ticker, raw in sorted(
                document["selected_snapshots"].items()
            ):
                unit.cursor.execute(
                    "SELECT contract_id FROM ref.option_contracts "
                    "WHERE contract_ticker=%s "
                    "AND provider IN ('massive','polygon')",
                    (ticker,),
                )
                contract_rows = unit.cursor.fetchall()
                if len(contract_rows) != 1:
                    raise AdvancedOptionsPreparationTerminal(
                        "catalog contract identity is ambiguous"
                    )
                open_interest = raw.get("open_interest")
                if (
                    not isinstance(open_interest, int)
                    or open_interest < 0
                ):
                    continue
                underlyer = str(
                    raw["underlying_asset"]["ticker"]
                ).upper()
                digest = canonical_sha256(raw)
                oi_facts.append(ReferenceFact.build(
                    ReferenceFactRelation.OI_BASELINE,
                    ReferenceKind.OI_COVERAGE.value,
                    underlyer,
                    baseline_id=uuid5(
                        NAMESPACE_URL,
                        "advanced-options-v3-oi:"
                        f"{ticker}:{settlement}:"
                        f"{captured_at.isoformat()}:{digest}",
                    ),
                    contract_id=int(
                        contract_rows[0]["contract_id"]
                    ),
                    settlement_session=settlement,
                    open_interest=open_interest,
                    source="massive",
                    date_basis=(
                        "PROVIDER_CHAIN_SNAPSHOT_PRIOR_SESSION"
                    ),
                    observed_session=claim.session_date,
                    observed_at=captured_at,
                    received_at=captured_at,
                    payload_sha256=digest,
                    supersedes_baseline_id=None,
                ))
            persist_reference_facts_cursor(
                unit.cursor,
                tuple(oi_facts),
            )
        used_ids = provider_request_ids[:capture.request_count]
        base_bytes, remainder = divmod(
            capture.decoded_bytes,
            capture.request_count,
        )
        results = tuple(
            AdvancedOptionsProviderRequestResult(
                request_id,
                base_bytes + (1 if index < remainder else 0),
                (
                    len(capture.selected_snapshots)
                    if index == 0
                    else 0
                ),
            )
            for index, request_id in enumerate(used_ids)
        )
        return AdvancedOptionsStageResult(
            watermark={
                "capture_sha256": document["semantic_sha256"],
                "settlement_session": settlement.isoformat(),
            },
            evidence={
                "capture": document,
                "catalog_effects": effects,
                "oi_fact_count": len(oi_facts),
            },
            provider_results=results,
        )

    def _load_completed_evidence(self, claim, stage, ticker=None):
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
        ) as unit:
            unit.cursor.execute(
                "SELECT evidence FROM "
                "rt.advanced_options_preparation_work "
                "WHERE revision_id=%s AND ticker=%s AND stage=%s "
                "AND status='COMPLETE'",
                (
                    str(claim.revision_id),
                    ticker or claim.ticker,
                    stage,
                ),
            )
            row = unit.cursor.fetchone()
        if row is None:
            raise AdvancedOptionsPreparationRetryable(
                f"{stage} evidence is unavailable"
            )
        return dict(row[0])

    def _reference_oi(self, claim):
        catalog = self._load_completed_evidence(
            claim,
            "CATALOG_QUOTE",
        )
        capture = catalog["capture"]
        missing = [
            ticker
            for ticker, row in capture["selected_snapshots"].items()
            if not isinstance(row.get("open_interest"), int)
            or row["open_interest"] < 0
        ]
        return AdvancedOptionsStageResult(
            watermark={
                "capture_sha256": capture["semantic_sha256"],
                "oi_contract_count": len(
                    capture["selected_snapshots"]
                ),
                "oi_available_contract_count": (
                    len(capture["selected_snapshots"]) - len(missing)
                ),
                "oi_unavailable_contract_count": len(missing),
            },
            evidence={
                "capture_sha256": capture["semantic_sha256"],
                "contracts": sorted(capture["selected_snapshots"]),
                "oi_unavailable_contracts": sorted(missing),
                "status": (
                    "READY"
                    if not missing
                    else "READY_WITH_UNAVAILABLE_CONTEXT"
                ),
            },
        )

    def _history(self, claim):
        calendar = OptionExchangeCalendar("XNYS")
        sessions = calendar.trailing_sessions_before(
            claim.session_date,
            31,
        )
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
            dict_cursor=True,
        ) as unit:
            unit.cursor.execute(
                "SELECT DISTINCT binding.security_id "
                "FROM rt.realtime_desired_member_intents AS intent "
                "JOIN rt.realtime_desired_member_bindings AS binding USING(intent_id) "
                "WHERE intent.revision_id=%s AND intent.ticker=%s "
                "AND intent.member_kind IN ('EQUITY','SECTOR_ETF')",
                (str(claim.revision_id), claim.ticker),
            )
            identities = tuple(unit.cursor.fetchall())
            if len(identities) != 1:
                raise AdvancedOptionsPreparationRetryable(
                    f"required Equity identity is unavailable for {claim.ticker}"
                )
            checked_at = _utc(self._clock(), "history clock")
            try:
                history = {
                    basis: load_equity_history_cursor(
                        unit.cursor,
                        cohort_id=claim.source_equity_cohort_id,
                        security_id=UUID(str(identities[0]["security_id"])),
                        ticker=claim.ticker,
                        interval="1d",
                        adjusted=basis == "ADJUSTED",
                        sessions=selected,
                        checked_at=checked_at,
                    )
                    for basis, selected in (
                        ("ADJUSTED", sessions),
                        ("RAW_UNADJUSTED", sessions[-1:]),
                    )
                }
            except EquityHistoryIdentityError as error:
                raise AdvancedOptionsPreparationTerminal(str(error)) from error
        adjusted = history["ADJUSTED"].rows
        raw = history["RAW_UNADJUSTED"].rows
        if (
            tuple(row["session_date"] for row in adjusted) != sessions
            or tuple(row["session_date"] for row in raw) != sessions[-1:]
            or any(row["stale_adjustment_lineage"] for row in adjusted)
            or any(
                (row["bar_start"], row["bar_end"])
                != equity_history_coordinates(calendar, "1d", row["session_date"])[0]
                for row in (*adjusted, *raw)
            )
        ):
            raise AdvancedOptionsPreparationRetryable(
                f"required Options history is incomplete for {claim.ticker}"
            )
        return AdvancedOptionsStageResult(
            watermark={
                "latest_session": sessions[-1].isoformat(),
                "adjusted_sessions": len(adjusted),
                "raw_sessions": len(raw),
            },
            evidence={
                "status": "READY",
                "source_cohort_id": str(claim.source_equity_cohort_id),
                "resolved_history": {
                    basis: value.lineage for basis, value in history.items()
                },
                "sessions": [
                    session.isoformat() for session in sessions
                ],
            },
        )

    def _capacity(self, claim):
        allocation_ticker = claim.option_underlyings[0]
        if claim.ticker != allocation_ticker:
            frozen = self._load_completed_evidence(
                claim,
                "CAPACITY_SHARDS",
                ticker=allocation_ticker,
            )
            return AdvancedOptionsStageResult(
                watermark=dict(frozen["watermark"]),
                evidence={
                    "status": "READY",
                    "source_capacity_ticker": allocation_ticker,
                    "allocation_sha256": frozen[
                        "allocation_sha256"
                    ],
                },
            )
        capture, allocation = self._allocate_capture(
            claim,
            self._combined_capture(claim),
        )
        tickers = tuple(sorted(capture["selected_snapshots"]))
        logical_shards = max(
            1,
            (
                len(tickers)
                + self._maximum_contracts_per_logical_shard
                - 1
            )
            // self._maximum_contracts_per_logical_shard,
        )
        allocation_document = (
            None
            if allocation is None
            else activity_momentum_allocation_payload(allocation)
        )
        allocation_sha256 = (
            canonical_sha256({
                "profile": "EXACT_FOUR_COMPATIBILITY",
                "contracts": tickers,
            })
            if allocation is None
            else allocation.allocation_sha256
        )
        return AdvancedOptionsStageResult(
            watermark={
                "contract_count": len(tickers),
                "logical_shards": logical_shards,
            },
            evidence={
                "watermark": {
                    "contract_count": len(tickers),
                    "logical_shards": logical_shards,
                },
                "capture": capture,
                "allocation": allocation_document,
                "allocation_sha256": allocation_sha256,
                "contracts": list(tickers),
                "maximum_contracts_per_logical_shard": (
                    self._maximum_contracts_per_logical_shard
                ),
                "status": "READY",
            },
        )

    def execute(
        self,
        claim: AdvancedOptionsPreparationClaim,
        *,
        provider_request_ids: tuple[UUID, ...],
    ) -> AdvancedOptionsStageResult:
        if claim.stage == "CATALOG_QUOTE":
            return self._catalog_quote(claim, provider_request_ids)
        if (
            provider_request_ids
            and claim.stage != "SESSION_ACTIVATION"
        ):
            raise AdvancedOptionsPreparationTerminal(
                "provider-free stage received provider reservations"
            )
        if claim.stage == "REFERENCE_OI":
            return self._reference_oi(claim)
        if claim.stage == "HISTORY":
            return self._history(claim)
        if claim.stage == "CAPACITY_SHARDS":
            return self._capacity(claim)
        if claim.stage == "SESSION_ACTIVATION":
            return self._activate_session(claim)
        raise AdvancedOptionsPreparationTerminal(
            f"unknown Options preparation stage: {claim.stage}"
        )

    def _combined_capture(self, claim):
        captures = [
            self._load_completed_evidence(
                claim,
                "CATALOG_QUOTE",
                ticker=ticker,
            )["capture"]
            for ticker in claim.option_underlyings
        ]
        if self._combined_capture_factory is not None:
            document = self._combined_capture_factory(
                claim,
                tuple(captures),
            )
            if not isinstance(document, dict):
                document = dict(document)
            return document
        document = {
            "contract": "advanced-options-v3-pre-session-capture-v1",
            "compatibility_profile": (
                SEALED_CAPTURE_EXACT_FOUR_PROFILE
                if self._exact_four_compatibility
                else SEALED_CAPTURE_GENERALIZED_C6_PROFILE
            ),
            "configured_underlyings": list(
                claim.option_underlyings
            ),
            "captured_at_utc": max(
                row["captured_at_utc"] for row in captures
            ),
            "session_date": claim.session_date.isoformat(),
            "source_cohort_id": str(
                claim.source_equity_cohort_id
            ),
            "provider_requests": sum(
                int(row["provider_requests"]) for row in captures
            ),
            "decoded_bytes": sum(
                int(row["decoded_bytes"]) for row in captures
            ),
            "responses": [
                response
                for row in captures
                for response in row["responses"]
            ],
            "selection_sha256": canonical_sha256(tuple(
                row["selection_sha256"] for row in captures
            )),
            "selected_snapshots": {
                ticker: snapshot
                for row in captures
                for ticker, snapshot
                in row["selected_snapshots"].items()
            },
        }
        document["semantic_sha256"] = canonical_sha256(document)
        return document

    def _allocate_capture(self, claim, capture):
        compatibility_profile = (
            SEALED_CAPTURE_EXACT_FOUR_PROFILE
            if self._exact_four_compatibility
            else SEALED_CAPTURE_GENERALIZED_C6_PROFILE
        )
        capture = {
            **capture,
            "compatibility_profile": compatibility_profile,
            "configured_underlyings": list(
                claim.option_underlyings
            ),
        }
        capture.pop("semantic_sha256", None)
        capture["semantic_sha256"] = canonical_sha256(capture)
        selection = p8_activity_selection_capture_from_sealed(
            capture,
            compatibility_profile=compatibility_profile,
        )
        if self._exact_four_compatibility:
            return capture, None
        selection_policy = (
            load_advanced_options_selection_c6_policy()
        )
        profile = load_activity_momentum_profile(
            selection_policy=selection_policy,
            version=self._admission_profile_version,
        )
        flow_policy = load_advanced_options_flow_policy()
        candidates = _isolated_activity_candidates(
            selection.selection.selected,
            selection.selected_snapshots,
            session_date=claim.session_date,
            observed_at=selection.selection.reviewed_at,
            flow_policy=flow_policy,
        )
        refresh_underlyings = []
        for underlying in claim.option_underlyings:
            scoped = tuple(
                candidate
                for candidate in candidates
                if candidate.underlying == underlying
                and any(
                    criterion.minimum_dte
                    <= candidate.dte_for(claim.session_date)
                    <= criterion.maximum_dte
                    for criterion in profile.selection_criteria
                )
            )
            if scoped and not any(
                candidate.bid is not None
                and candidate.ask is not None
                and candidate.quote_as_of is not None
                and any(
                    criterion.minimum_dte
                    <= candidate.dte_for(claim.session_date)
                    <= criterion.maximum_dte
                    and (
                        candidate.observed_at
                        - candidate.quote_as_of
                    ).total_seconds()
                    <= criterion.maximum_quote_age_seconds
                    for criterion in profile.selection_criteria
                )
                for candidate in scoped
            ):
                refresh_underlyings.append(underlying)
        if refresh_underlyings:
            raise AdvancedOptionsPreparationRefreshRequired(
                "CATALOG_QUOTE_CAPTURE_STALE",
                tuple(refresh_underlyings),
            )
        resolution_arguments = {}
        if (
            profile.version
            == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
        ):
            resolution_arguments = {
                "preparation_at": selection.selection.reviewed_at,
                "expiry_calendars": _expiry_calendar_evidence(
                    OptionExchangeCalendar("XNYS"),
                    session_date=claim.session_date,
                    activity_candidates=candidates,
                    conventions={},
                ),
                "same_day_enabled": profile.same_day_enabled,
            }
        resolution = resolve_activity_momentum_selection(
            profile,
            session_date=claim.session_date,
            candidates=candidates,
            enabled_strategies=self._enabled_strategies,
            **resolution_arguments,
        )
        allocation = allocate_activity_momentum(
            profile,
            session_date=claim.session_date,
            rotation_key=f"advanced-realtime-v3:{claim.revision_id}",
            ready_underlyings=claim.option_underlyings,
            groups=resolution.groups,
            active_contract_budget=(
                self._active_quote_contract_budget
            ),
            per_underlying_contract_limit=(
                self._per_underlying_contract_limit
            ),
            measured_capacity_limit=self._measured_capacity_limit,
            effective_provider_project_ceiling=(
                self._effective_provider_project_ceiling
            ),
            resolution=resolution,
        )
        selected = set(allocation.selected_contract_tickers)
        if not selected:
            raise AdvancedOptionsPreparationRetryable(
                "activity/momentum allocation selected no contracts"
            )
        selected_underlyings = {
            str(
                capture["selected_snapshots"][ticker][
                    "underlying_asset"
                ]["ticker"]
            ).upper()
            for ticker in selected
        }
        filtered_responses = [
            row
            for row in capture["responses"]
            if str(row.get("underlyer", "")).upper()
            in selected_underlyings
        ]
        filtered = {
            **capture,
            "selected_snapshots": {
                ticker: raw
                for ticker, raw in capture[
                    "selected_snapshots"
                ].items()
                if ticker in selected
            },
            "activity_momentum_allocation": (
                activity_momentum_allocation_payload(allocation)
            ),
            "responses": filtered_responses,
            "provider_requests": sum(
                int(row.get("page_count", 1))
                for row in filtered_responses
            ),
        }
        filtered.pop("semantic_sha256", None)
        filtered["semantic_sha256"] = canonical_sha256(filtered)
        return filtered, allocation

    def _activate_session(self, claim):
        if (
            claim.desired_snapshot_sha256 is None
            or claim.desired_cohort_id is None
        ):
            raise AdvancedOptionsPreparationRetryable(
                "desired snapshot is not materialized"
            )
        capacity = self._load_completed_evidence(
            claim,
            "CAPACITY_SHARDS",
            ticker=claim.option_underlyings[0],
        )
        capture = dict(capacity["capture"])
        allocation = (
            None
            if capacity.get("allocation") is None
            else activity_momentum_allocation_from_payload(
                capacity["allocation"]
            )
        )
        captured_at = datetime.fromisoformat(
            capture["captured_at_utc"]
        )
        reference_known_at = self._clock()
        if (
            reference_known_at.tzinfo is None
            or reference_known_at.utcoffset() is None
            or reference_known_at < captured_at
        ):
            raise AdvancedOptionsPreparationTerminal(
                "reference observation clock precedes capture"
            )
        calendar = OptionExchangeCalendar("XNYS")
        session_open = calendar.session_open(claim.session_date)
        prepared_at = min(
            captured_at,
            session_open - timedelta(microseconds=1),
        )
        hard_stop = (
            calendar.session_close(claim.session_date)
            + timedelta(minutes=5)
        )
        package_nonce = uuid5(
            NAMESPACE_URL,
            f"stockscreener:c8-options-package:{claim.revision_id}",
        )
        authorization_reference = (
            f"advanced-realtime-v3:{claim.revision_id}"
        )
        options_cohort_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:{authorization_reference}:"
            f"{package_nonce}:MARKET_OPTIONS",
        )
        admission, readiness, session = _prepare_session_authority(
            self._provider,
            capture,
            args=SimpleNamespace(
                session_date=claim.session_date,
                package_nonce=package_nonce,
                authorization_reference=authorization_reference,
                hard_stop=hard_stop,
            ),
            cohort_id=options_cohort_id,
            prepared_at=prepared_at,
            capture_boundary=session_open,
            acquisition_start=min(prepared_at, session_open),
        )
        (
            plan_id,
            history_binding,
            history_evidence,
            reference,
        ) = self._persist_session_prerequisites(
            claim,
            admission=admission,
            readiness=readiness,
            options_cohort_id=options_cohort_id,
            prepared_at=prepared_at,
            reference_known_at=reference_known_at,
            hard_stop=hard_stop,
            authorization_reference=authorization_reference,
            capture=capture,
        )
        final_prepared_at = max(
            admission.prepared_at,
            reference.known_at,
            datetime.fromisoformat(capture["captured_at_utc"]),
        )
        final_capture_boundary = max(
            session_open,
            final_prepared_at + timedelta(microseconds=1),
        )
        if final_capture_boundary > hard_stop:
            raise AdvancedOptionsPreparationRetryable(
                "reference state was not known before hard stop"
            )
        reference_by_ticker = {
            row.contract_ticker: row
            for row in reference.open_interest
        }
        final_contracts = tuple(
            replace(
                row,
                open_interest_member_sha256=(
                    None
                    if row.open_interest is None
                    else reference_by_ticker[
                        row.contract_ticker
                    ].member_sha256
                ),
            )
            for row in admission.contracts
        )
        final_admission = _opening_admission(
            status=admission.status,
            session_date=admission.session_date,
            prepared_at=final_prepared_at,
            capture_boundary=final_capture_boundary,
            pre_session_selection_id=(
                admission.pre_session_selection_id
            ),
            pre_session_selection_sha256=(
                admission.pre_session_selection_sha256
            ),
            source_evidence_sha256=(
                admission.source_evidence_sha256
            ),
            reference_evidence_sha256=reference.sha256,
            contracts=final_contracts,
            reason_codes=admission.reason_codes,
            policy_sha256=admission.policy_sha256,
        )
        slot_set = SlotSetVersion(
            cohort_id=options_cohort_id,
            version=1,
            kind=SlotSetKind.PRE_SESSION,
            effective_at=final_prepared_at,
            policy_sha256=final_admission.policy_sha256,
            members=tuple(
                SlotSetMember(
                    instrument_id,
                    MarketChannel(str(channel)),
                    SlotSetPurpose.PRIMARY,
                    priority,
                    catalog_version_id,
                )
                for priority, (
                    instrument_id,
                    _contract_id,
                    _ticker,
                    catalog_version_id,
                    channels,
                ) in enumerate(readiness.slots, start=1)
                for channel in channels
            ),
        )
        instrument_ids = {
            ticker: instrument_id
            for (
                instrument_id,
                _contract_id,
                ticker,
                _catalog_version_id,
                _channels,
            ) in readiness.slots
        }
        final_readiness = build_opening_subscription_readiness(
            final_admission,
            slot_set,
            instrument_id_by_contract_ticker=instrument_ids,
            journal_generation=session.journal_generation,
            journal_identity_sha256=(
                session.journal_identity_sha256
            ),
            ready_at=final_prepared_at,
        )
        final_budget = ProviderBudgetScope(
            account_alias="MASSIVE_ADVANCED",
            session_date=claim.session_date,
            authorization_reference=authorization_reference,
            provider_authorization_reference=authorization_reference,
            expected_cost="bounded desired-state Options preparation",
            request_ceiling=int(capture["provider_requests"]),
            decoded_byte_ceiling=int(capture["decoded_bytes"]),
            account_request_ceiling=(
                MASSIVE_ADVANCED_ACCOUNT_REQUEST_CEILING
            ),
            account_decoded_byte_ceiling=(
                MASSIVE_ADVANCED_ACCOUNT_DECODED_BYTE_CEILING
            ),
            lanes=ADVANCED_OPTIONS_REST_LANES,
            authorized_until=hard_stop,
        )
        final_session = build_session_acquisition_authorization(
            final_admission,
            final_readiness,
            final_budget,
            AdvancedOptionsSocketBudget(
                stream_sessions=1,
                connection_attempts=self._connection_attempt_ceiling,
                reconnects=self._connection_attempt_ceiling - 1,
                decoded_messages=self._decoded_message_ceiling,
                decoded_bytes=self._decoded_byte_ceiling,
            ),
            authorization_reference=authorization_reference,
            prepared_at=final_prepared_at,
            acquisition_start=max(
                session.acquisition_start,
                final_prepared_at,
            ),
            acquisition_stop=hard_stop,
        )
        authority_bundle = AdvancedOptionsAuthorityBundle(
            session=final_session,
            admission=final_admission,
            readiness=final_readiness,
        )
        authority_payload = advanced_options_authority_bundle_payload(
            authority_bundle
        )
        pre_session_authority = {
            "contract": (
                "advanced-options-v3-pre-session-authority-v1"
            ),
            "session_date": claim.session_date.isoformat(),
            "target_cohort_id": str(options_cohort_id),
            "package_nonce": str(package_nonce),
            "authorization_reference": authorization_reference,
            "source_capture_semantic_sha256": (
                capture["semantic_sha256"]
            ),
            "admission": prospective_opening_admission_payload(
                final_admission
            ),
            "admitted_contracts": [
                row.contract_ticker
                for row in final_admission.contracts
            ],
            "readiness_sha256": final_readiness.readiness_sha256,
            "session_acquisition": authority_payload[
                "session_acquisition"
            ],
        }
        prefix = (
            f"advanced-options:{options_cohort_id}:"
            f"{claim.session_date.isoformat()}"
        )
        package = {
            "schema": "advanced-options-v3-continuous-session-v1",
            "authorization_reference": authorization_reference,
            "validation_mode": "PRODUCTION_STRICT",
            "package_nonce": str(package_nonce),
            "window": {
                "session_date": claim.session_date.isoformat(),
                "start_utc": (
                    final_session.acquisition_start.isoformat()
                ),
                "stop_utc": final_session.acquisition_stop.isoformat(),
            },
            "cohort": {
                "cohort_id": str(options_cohort_id),
                "underlyings": list(claim.option_underlyings),
            },
            "orchestration": {
                "plan_id": str(plan_id),
                "catalog_version": STAGE_CATALOG_VERSION_V5,
                "stage_order": list(OPTIONS_V3_STAGE_ORDER),
            },
            "owners": {
                "processes": [
                    {
                        "kind": "INGRESS",
                        "owner_name": "advanced-options-ingress-v1",
                        "lease_resource_key": (
                            "massive:options:websocket"
                        ),
                    },
                    {
                        "kind": "MARKET_STATE",
                        "owner_name": (
                            "market-state-materializer-v1"
                        ),
                        "lease_resource_key": (
                            f"{prefix}:market-state"
                        ),
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
                "symbols": list(claim.option_underlyings),
            },
            "v3_history_binding": {
                "binding": advanced_options_history_binding_payload(
                    history_binding
                ),
                "evidence": history_evidence,
            },
            "history_binding": {
                "source_cohort_id": str(history_binding.source_cohort_id),
                "source_manifest_sha256": (
                    history_binding.source_manifest_sha256
                ),
                "history_before_utc": history_binding.history_before.isoformat(),
            },
            "v3_pre_session_authority": pre_session_authority,
            "v3_authorities": {
                "session_acquisition": authority_payload[
                    "session_acquisition"
                ],
            },
            "v3_admitted_contracts": [
                row.contract_ticker
                for row in final_admission.contracts
            ],
        }
        package["package_sha256"] = canonical_sha256(package)
        acquisition = build_acquisition_binding(
            source_sha256=capture["semantic_sha256"],
            backend_image_id=self._backend_image_id,
            authority=final_session,
            contract_tickers=tuple(
                row.contract_ticker
                for row in final_admission.contracts
            ),
            connection_attempt_ceiling=self._connection_attempt_ceiling,
            decoded_message_ceiling=self._decoded_message_ceiling,
            decoded_byte_ceiling=self._decoded_byte_ceiling,
        )
        selection_plan = self._selection_plan(
            claim,
            admission=final_admission,
            readiness=final_readiness,
            reference=reference,
            acquisition=acquisition,
            capture=capture,
            allocation=allocation,
        )
        repository = PostgresContinuousSessionStateRepository(
            self._provider,
            session_date=claim.session_date,
        )
        session_binding = repository.activate_session_from_documents(
            package=package,
            pre_session_authority=pre_session_authority,
            acquisition_binding=acquisition,
            sealed_selection=capture,
            written_at=final_prepared_at,
            selection_plan=selection_plan,
            package_path_reference="postgres://advanced-options/session",
        )
        return AdvancedOptionsStageResult(
            watermark={
                "session_binding_id": str(session_binding.binding_id),
                "session_binding_sha256": (
                    session_binding.binding_sha256
                ),
            },
            evidence={
                "package_sha256": package["package_sha256"],
                "acquisition_binding": (
                    acquisition_binding_payload(acquisition)
                ),
                "selection_generation_sha256": (
                    selection_plan.generation_sha256
                ),
                "selection_profile": (
                    selection_plan.admission_profile_version
                ),
                "status": "ACTIVE",
            },
            session_binding_id=session_binding.binding_id,
        )

    def _persist_session_prerequisites(
        self,
        claim,
        *,
        admission,
        readiness,
        options_cohort_id,
        prepared_at,
        reference_known_at,
        hard_stop,
        authorization_reference,
        capture,
    ):
        with self._provider.transaction(
            APP_ROLE,
            isolation_level="SERIALIZABLE",
        ) as unit:
            cursor = unit.cursor
            cursor.execute(
                "SELECT manifest.manifest_id::text,"
                "manifest.manifest_sha256,manifest.profile_id::text,"
                "profile.schema_sha256,profile.capabilities_sha256,"
                "profile.security_policy_sha256 "
                "FROM rt.cohorts AS cohort "
                "JOIN rt.subscription_manifests AS manifest "
                "USING(manifest_id) "
                "JOIN rt.capability_profiles AS profile "
                "USING(profile_id) WHERE cohort.cohort_id=%s",
                (str(claim.source_equity_cohort_id),),
            )
            source = cursor.fetchone()
            if source is None:
                raise AdvancedOptionsPreparationRetryable(
                    "READY Equity source cohort is unavailable"
                )
            cursor.execute(
                "SELECT intent.ticker,binding.instrument_id "
                "FROM rt.realtime_desired_member_intents AS intent "
                "JOIN rt.realtime_desired_member_bindings AS binding "
                "USING(intent_id) "
                "WHERE intent.revision_id=%s "
                "AND intent.member_kind IN ('EQUITY','SECTOR_ETF') "
                "AND EXISTS (SELECT 1 FROM rt.cohorts AS source "
                "JOIN rt.subscription_members AS member USING(manifest_id) "
                "WHERE source.cohort_id=%s "
                "AND member.instrument_id=binding.instrument_id "
                "AND member.channel='AM')",
                (
                    str(claim.revision_id),
                    str(claim.source_equity_cohort_id),
                ),
            )
            equity_instruments = {
                str(row[0]): int(row[1]) for row in cursor.fetchall()
            }
            if not set(claim.option_underlyings).issubset(equity_instruments):
                raise AdvancedOptionsPreparationRetryable(
                    "READY Equity instrument bindings are incomplete"
                )
            universe_sha256 = UniverseManifest(
                version=(
                    f"market-options-{len(equity_instruments)}-"
                    f"{len(claim.option_underlyings)}-v1"
                ),
                equity_tickers=tuple(sorted(equity_instruments)),
                option_underlyings=claim.option_underlyings,
            ).sha256
            profile_id = uuid5(
                NAMESPACE_URL,
                f"stockscreener:c8-options-profile:{claim.revision_id}",
            )
            profile_sha256 = canonical_sha256({
                "profile_id": profile_id,
                "universe_sha256": universe_sha256,
                "prepared_at": prepared_at,
            })
            cursor.execute(
                "INSERT INTO rt.capability_profiles ("
                "profile_id,config_sha256,schema_sha256,account_alias,"
                "capabilities_sha256,universe_sha256,checked_at,"
                "expires_at,status,profile_sha256,provider_tier,"
                "security_policy_sha256,preflight_mode,"
                "provider_requests,action_authorized"
                ") VALUES (%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,"
                "'PASS',%s,'MASSIVE_ADVANCED',%s,'PASSIVE',false,false) "
                "ON CONFLICT (profile_id) DO NOTHING",
                (
                    str(profile_id),
                    canonical_sha256({
                        "revision_id": claim.revision_id,
                        "kind": "OPTIONS",
                    }),
                    source[3],
                    source[4],
                    universe_sha256,
                    prepared_at,
                    hard_stop,
                    profile_sha256,
                    source[5],
                ),
            )
            manifest_id = uuid5(
                NAMESPACE_URL,
                f"stockscreener:c8-options-manifest:{claim.revision_id}",
            )
            manifest_sha256 = canonical_sha256({
                "manifest_id": manifest_id,
                "underlyings": claim.option_underlyings,
                "profile_sha256": profile_sha256,
            })
            cursor.execute(
                "INSERT INTO rt.subscription_manifests ("
                "manifest_id,manifest_sha256,universe_sha256,"
                "session_date,effective_at,profile_id,manifest_kind,"
                "stock_count,option_underlying_count,member_count,"
                "allocation_policy_sha256"
                ") VALUES (%s,%s,%s,%s,%s,%s,'MARKET_OPTIONS',"
                "%s,%s,%s,%s) ON CONFLICT (manifest_id) DO NOTHING",
                (
                    str(manifest_id),
                    manifest_sha256,
                    universe_sha256,
                    claim.session_date,
                    prepared_at,
                    str(profile_id),
                    len(claim.option_underlyings),
                    len(claim.option_underlyings),
                    len(claim.option_underlyings),
                    canonical_sha256({
                        "kind": "C8_OPTIONS_ALLOCATION",
                        "revision_id": claim.revision_id,
                    }),
                ),
            )
            for priority, ticker in enumerate(
                claim.option_underlyings,
                start=1,
            ):
                cursor.execute(
                    "INSERT INTO rt.subscription_members ("
                    "manifest_id,instrument_id,channel,priority,"
                    "purpose,option_underlying"
                    ") VALUES (%s,%s,'AM',%s,'PRIMARY',true) "
                    "ON CONFLICT DO NOTHING",
                    (
                        str(manifest_id),
                        equity_instruments[ticker],
                        priority,
                    ),
                )
            cohort_sha256 = canonical_sha256({
                "cohort_id": options_cohort_id,
                "manifest_sha256": manifest_sha256,
                "revision_id": claim.revision_id,
            })
            cursor.execute(
                "INSERT INTO rt.cohorts ("
                "cohort_id,cohort_sha256,manifest_id,"
                "configuration_sha256,model_bundle_sha256,"
                "valuation_sha256,effective_at,mode"
                ") VALUES (%s,%s,%s,%s,%s,%s,%s,'LIVE') "
                "ON CONFLICT (cohort_id) DO NOTHING",
                (
                    str(options_cohort_id),
                    cohort_sha256,
                    str(manifest_id),
                    canonical_sha256({
                        "revision_id": claim.revision_id,
                        "configuration": "C8",
                    }),
                    _C8_MODEL_BUNDLE_SHA256,
                    _C8_VALUATION_POLICY_SHA256,
                    prepared_at,
                ),
            )
            # R18 (Nov 10): the Options cohort's provider-tier binding, as the
            # P8 enrollment writes it. rt.option_packages references it, so
            # without it the first selected O1 package failed its window.
            persist_tier_cohort_binding_cursor(
                cursor,
                c8_options_tier_binding(
                    cohort_id=options_cohort_id,
                    profile_id=profile_id,
                    manifest_sha256=manifest_sha256,
                    effective_at=prepared_at,
                ),
            )
            cursor.execute(
                "INSERT INTO rt.slot_set_versions ("
                "cohort_id,version,kind,effective_at,policy_sha256,"
                "member_sha256,primary_contract_count,"
                "reserve_contract_count"
                ") VALUES (%s,1,'PRE_SESSION',%s,%s,%s,%s,0) "
                "ON CONFLICT (cohort_id,version) DO NOTHING",
                (
                    str(options_cohort_id),
                    readiness.ready_at,
                    readiness.slot_set_policy_sha256,
                    readiness.slot_set_member_sha256,
                    len(readiness.slots),
                ),
            )
            for priority, slot in enumerate(
                readiness.slots,
                start=1,
            ):
                (
                    instrument_id,
                    _contract_id,
                    _ticker,
                    catalog_version_id,
                    channels,
                ) = slot
                for channel in channels:
                    cursor.execute(
                        "INSERT INTO rt.slot_set_members ("
                        "cohort_id,version,instrument_id,channel,"
                        "purpose,priority,catalog_version_id"
                        ") VALUES (%s,1,%s,%s,'PRIMARY',%s,%s) "
                        "ON CONFLICT DO NOTHING",
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
                ") VALUES (%s,1,%s) ON CONFLICT (cohort_id) "
                "DO UPDATE SET version=EXCLUDED.version,"
                "activated_at=EXCLUDED.activated_at",
                (str(options_cohort_id), readiness.ready_at),
            )
            plan_id = uuid5(
                NAMESPACE_URL,
                f"stockscreener:c8-options-plan:{claim.revision_id}",
            )
            cursor.execute(
                "INSERT INTO rt.service_plans ("
                "plan_id,cohort_id,session_date,"
                "stage_catalog_version,enabled_stages,config_sha256,"
                "deadline,scope_reference"
                ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (plan_id) DO NOTHING",
                (
                    str(plan_id),
                    str(options_cohort_id),
                    claim.session_date,
                    STAGE_CATALOG_VERSION_V5,
                    list(OPTIONS_V3_STAGE_ORDER),
                    canonical_sha256({
                        "revision_id": claim.revision_id,
                        "plan": "C8_OPTIONS",
                    }),
                    hard_stop,
                    authorization_reference,
                ),
            )
            history_evidence = self._history_evidence(
                cursor,
                claim,
                options_cohort_id=options_cohort_id,
                prepared_at=prepared_at,
            )
            history_source_cohort_id = UUID(
                history_evidence["source_cohort_id"]
            )
            history_source_manifest_sha256 = history_evidence[
                "source_manifest"
            ]["manifest_sha256"]
            history_binding = build_advanced_options_history_binding(
                target_cohort_id=options_cohort_id,
                source_cohort_id=history_source_cohort_id,
                history_before=OptionExchangeCalendar(
                    "XNYS"
                ).session_open(claim.session_date),
                source_manifest_sha256=(
                    history_source_manifest_sha256
                ),
                authorization_reference=authorization_reference,
                evidence_sha256=history_evidence["evidence_sha256"],
                source_provider_tier=ProviderTier(
                    history_evidence["source_manifest"]["provider_tier"]
                ),
            )
            cursor.execute(
                "INSERT INTO rt.cohort_history_bindings ("
                "target_cohort_id,source_cohort_id,"
                "source_provider_tier,history_before,"
                "source_manifest_sha256,authorization_reference,"
                "binding_sha256"
                ") VALUES (%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (target_cohort_id) DO NOTHING",
                (
                    str(options_cohort_id),
                    str(history_binding.source_cohort_id),
                    history_binding.source_provider_tier.value,
                    history_binding.history_before,
                    history_binding.source_manifest_sha256,
                    history_binding.authorization_reference,
                    history_binding.binding_sha256,
                ),
            )
            reference = self._persist_reference_state(
                cursor,
                claim,
                admission=admission,
                readiness=readiness,
                manifest_sha256=manifest_sha256,
                capture=capture,
                known_at=reference_known_at,
                hard_stop=hard_stop,
                history_source_cohort_id=history_source_cohort_id,
            )
        return plan_id, history_binding, history_evidence, reference

    def _history_evidence(
        self,
        cursor,
        claim,
        *,
        options_cohort_id,
        prepared_at,
    ):
        history = {
            ticker: self._load_completed_evidence(
                claim,
                "HISTORY",
                ticker=ticker,
            )
            for ticker in claim.option_underlyings
        }
        latest = min(
            row["sessions"][-1] for row in history.values()
        )
        source_cohort_ids = {
            row["source_cohort_id"] for row in history.values()
        }
        if len(source_cohort_ids) != 1:
            raise AdvancedOptionsPreparationTerminal(
                "Options history sources diverged"
            )
        source_cohort_id = next(iter(source_cohort_ids))
        cursor.execute(
            "SELECT manifest.manifest_sha256,profile.provider_tier "
            "FROM rt.cohorts AS cohort "
            "JOIN rt.subscription_manifests AS manifest "
            "USING(manifest_id) "
            "JOIN rt.capability_profiles AS profile USING(profile_id) "
            "WHERE cohort.cohort_id=%s",
            (source_cohort_id,),
        )
        source_manifest = cursor.fetchone()
        if source_manifest is None:
            raise AdvancedOptionsPreparationRetryable(
                "Options history source manifest is unavailable"
            )
        evidence = {
            "schema": "advanced-options-v3-history-evidence-v1",
            "target_cohort_id": str(options_cohort_id),
            "source_cohort_id": source_cohort_id,
            "session_date": claim.session_date.isoformat(),
            "known_at_utc": prepared_at.isoformat(),
            "source_manifest": {
                "manifest_sha256": str(source_manifest[0]),
                "provider_tier": str(source_manifest[1]),
            },
            "coverage": {
                "underlyings": list(claim.option_underlyings),
                "members": history,
            },
            "readiness": {
                "status": "READY",
                "required_through_session": latest,
                "latest_covered_session": latest,
            },
        }
        evidence["evidence_sha256"] = canonical_sha256(evidence)
        return evidence

    def _persist_reference_state(
        self,
        cursor,
        claim,
        *,
        admission,
        readiness,
        manifest_sha256,
        capture,
        known_at,
        hard_stop,
        history_source_cohort_id,
    ):
        cursor.execute(
            "SELECT state.state_id::text,state.state_sha256,"
            "state.reference_generation,state.session_date,"
            "state.known_at,state.valid_until,"
            "state.reference_policy_sha256,"
            "state.contract_scope_sha256 "
            "FROM rt.advanced_options_reference_session_freezes "
            "AS session_freeze "
            "JOIN rt.advanced_options_reference_states AS state "
            "USING(state_id) WHERE session_freeze.session_date=%s",
            (claim.session_date,),
        )
        existing_row = cursor.fetchone()
        if existing_row is not None:
            cursor.execute(
                "SELECT state_document FROM "
                "rt.advanced_options_reference_states "
                "WHERE state_id=%s",
                (str(existing_row[0]),),
            )
            document_row = cursor.fetchone()
            try:
                decoded_existing = (
                    decode_advanced_options_reference_state(
                        state_id=UUID(str(existing_row[0])),
                        state_sha256=str(existing_row[1]),
                        document=document_row[0],
                    )
                )
            except ValueError:
                decoded_existing = None
            if (
                decoded_existing is not None
                and set(decoded_existing.underlyings)
                == set(claim.option_underlyings)
                and {
                    row.contract_ticker
                    for row in decoded_existing.open_interest
                }
                == {
                    row.contract_ticker
                    for row in admission.contracts
                }
            ):
                return OpeningReferenceEvidence.from_reference_state(
                    decoded_existing
                )
            cursor.execute(
                "SELECT member_sha256,member_document,availability "
                "FROM rt.advanced_options_reference_state_members "
                "WHERE state_id=%s AND member_kind='OPEN_INTEREST' "
                "ORDER BY member_key",
                (str(existing_row[0]),),
            )
            stored_oi = tuple(cursor.fetchall())
            existing_oi = tuple(
                OpeningOpenInterestEvidence(
                    contract_id=int(row[1]["contract_id"]),
                    contract_ticker=str(
                        row[1]["contract_ticker"]
                    ),
                    underlyer=str(row[1]["underlyer"]),
                    open_interest=int(row[1]["open_interest"]),
                    known_at=existing_row[4],
                    member_sha256=str(row[0]),
                )
                for row in stored_oi
                if str(row[2]) == "AVAILABLE"
            )
            if (
                {
                    str(row[1]["contract_ticker"])
                    for row in stored_oi
                }
                == {
                    row.contract_ticker
                    for row in admission.contracts
                }
            ):
                return OpeningReferenceEvidence(
                    state_id=UUID(str(existing_row[0])),
                    state_sha256=str(existing_row[1]),
                    reference_generation=int(existing_row[2]),
                    session_date=existing_row[3],
                    known_at=existing_row[4],
                    valid_until=existing_row[5],
                    reference_policy_sha256=str(existing_row[6]),
                    contract_scope_sha256=str(existing_row[7]),
                    open_interest_sha256=canonical_sha256(tuple(
                        str(row[0])
                        for row in sorted(
                            stored_oi,
                            key=lambda value: int(
                                value[1]["contract_id"]
                            ),
                        )
                    )),
                    open_interest=existing_oi,
                )
            raise AdvancedOptionsPreparationRetryable(
                "stored reference membership differs: "
                f"stored={sorted(str(row[1]['contract_ticker']) for row in stored_oi)},"
                f"prepared={sorted(row.contract_ticker for row in admission.contracts)}"
            )
        # The flow role only records the policy after activation, and the
        # contract scope below references it, so activation records it first.
        flow_policy = load_advanced_options_flow_policy()
        persist_option_flow_policy_cursor(cursor, flow_policy)
        flow_policy_sha256 = flow_policy.sha256
        contract_scope_sha256 = canonical_sha256({
            "session_date": claim.session_date,
            "manifest_sha256": manifest_sha256,
            "slot_sha256": readiness.slot_set_member_sha256,
            "flow_policy_sha256": flow_policy_sha256,
        })
        cursor.execute(
            "INSERT INTO rt.option_flow_contract_scopes ("
            "contract_scope_sha256,scope_version,session_date,"
            "manifest_sha256,slot_sha256,"
            "advanced_flow_policy_sha256"
            ") VALUES (%s,'advanced-options-contract-scope-v1',"
            "%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (
                contract_scope_sha256,
                claim.session_date,
                manifest_sha256,
                readiness.slot_set_member_sha256,
                flow_policy_sha256,
            ),
        )
        snapshots = capture["selected_snapshots"]
        captured_at = datetime.fromisoformat(capture["captured_at_utc"])
        settlement = OptionExchangeCalendar(
            "XNYS"
        ).previous_session_date(claim.session_date)
        def open_interest_reference(row):
            if row.open_interest is None:
                return AdvancedOptionsOpenInterestReference(
                    contract_id=row.contract_id,
                    contract_ticker=row.contract_ticker,
                    underlyer=row.underlyer,
                    availability=ReferenceAvailability.UNAVAILABLE,
                    known_at=known_at,
                    reason_codes=("MISSING_PRIOR_OPEN_INTEREST",),
                )
            return AdvancedOptionsOpenInterestReference(
                contract_id=row.contract_id,
                contract_ticker=row.contract_ticker,
                underlyer=row.underlyer,
                availability=ReferenceAvailability.AVAILABLE,
                known_at=known_at,
                baseline_id=uuid5(
                    NAMESPACE_URL,
                    f"stockscreener:c8-oi:{row.contract_ticker}:"
                    f"{claim.session_date}",
                ),
                settlement_session=settlement,
                observed_session=claim.session_date,
                open_interest=row.open_interest,
                source="massive",
                date_basis="PROVIDER_CHAIN_SNAPSHOT_PRIOR_SESSION",
                observed_at=captured_at,
                received_at=captured_at,
                payload_sha256=canonical_sha256(
                    snapshots[row.contract_ticker]
                ),
            )
        oi = tuple(
            open_interest_reference(row)
            for row in admission.contracts
        )
        volatility, valuations, metrics = self._opening_reference_inputs(
            cursor,
            claim,
            admission=admission,
            known_at=known_at,
            hard_stop=hard_stop,
            history_source_cohort_id=history_source_cohort_id,
        )
        cursor.execute(
            "SELECT COALESCE(MAX(reference_generation),0)+1 "
            "FROM rt.advanced_options_reference_states"
        )
        generation = int(cursor.fetchone()[0])
        state = build_advanced_options_reference_state(
            reference_generation=generation,
            session_date=claim.session_date,
            known_at=known_at,
            valid_until=hard_stop,
            reference_policy_sha256=canonical_sha256({
                "policy": "C8_OPTIONS_REFERENCE",
                "flow_policy_sha256": flow_policy_sha256,
            }),
            data_membership_sha256=None,
            contract_scope_sha256=contract_scope_sha256,
            underlyings=claim.option_underlyings,
            open_interest=oi,
            historical_volatility=volatility,
            valuation_inputs=valuations,
            equity_metrics=metrics,
        )
        persist_advanced_options_reference_state_cursor(cursor, state)
        first_cycle_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:c8-reference-freeze:{claim.session_date}",
        )
        cursor.execute(
            "INSERT INTO rt.advanced_options_reference_session_freezes ("
            "session_date,state_id,first_cycle_id,consumed_at"
            ") VALUES (%s,%s,%s,%s) ON CONFLICT (session_date) "
            "DO NOTHING",
            (
                claim.session_date,
                str(state.state_id),
                str(first_cycle_id),
                known_at,
            ),
        )
        return OpeningReferenceEvidence.from_reference_state(state)

    def _opening_reference_inputs(
        self,
        cursor,
        claim,
        *,
        admission,
        known_at,
        hard_stop,
        history_source_cohort_id,
    ):
        cursor.execute(
            "SELECT intent.ticker,binding.security_id::text "
            "FROM rt.realtime_desired_member_intents AS intent "
            "JOIN rt.realtime_desired_member_bindings AS binding "
            "USING(intent_id) "
            "WHERE intent.revision_id=%s "
            "AND intent.ticker=ANY(%s) "
            "AND intent.member_kind IN ('EQUITY','SECTOR_ETF')",
            (
                str(claim.revision_id),
                list(claim.option_underlyings),
            ),
        )
        security_ids = {
            str(row[0]): UUID(str(row[1])) for row in cursor.fetchall()
        }
        spots = {
            row.underlyer: (row.spot, security_ids[row.underlyer])
            for row in admission.contracts
        }
        if set(spots) != set(claim.option_underlyings):
            raise AdvancedOptionsPreparationRetryable(
                "opening reference needs an admitted contract "
                "per Options underlying"
            )
        with cursor.connection.cursor(
            cursor_factory=RealDictCursor,
        ) as reference_cursor:
            try:
                (
                    volatility,
                    valuations,
                    metrics,
                    missing,
                ) = load_opening_reference_inputs_cursor(
                    reference_cursor,
                    session_date=claim.session_date,
                    known_at=known_at,
                    valid_until=hard_stop,
                    underlyings=claim.option_underlyings,
                    contracts=admission.contracts,
                    spots=spots,
                    history_cohort_id=UUID(str(history_source_cohort_id)),
                    model_bundle_sha256=_C8_MODEL_BUNDLE_SHA256,
                    valuation_policy_sha256=_C8_VALUATION_POLICY_SHA256,
                )
            except ValueError as error:
                raise AdvancedOptionsPreparationRetryable(
                    f"opening reference inputs rejected: {error}"
                ) from error
        if missing:
            raise AdvancedOptionsPreparationRetryable(
                "opening reference inputs unavailable: "
                + ",".join(missing)
            )
        return volatility, valuations, metrics

    def _selection_plan(
        self,
        claim,
        *,
        admission,
        readiness,
        reference,
        acquisition,
        capture,
        allocation,
    ):
        policy = load_advanced_options_selection_c6_policy()
        flow_policy = load_advanced_options_flow_policy()
        catalog_members = {
            row.contract_ticker: C6SelectionMember(
                contract_id=row.contract_id,
                contract_ticker=row.contract_ticker,
                underlying=row.underlyer,
                expiration=row.expiration_date,
                strike=row.strike,
                option_side=(
                    OptionSide.CALL
                    if row.contract_type == "CALL"
                    else OptionSide.PUT
                ),
                multiplier=Decimal("100"),
                deliverable="STANDARD",
                catalog_version_id=row.catalog_version_id,
                catalog_payload_sha256=row.catalog_payload_sha256,
                matching_strategies=(),
                contract_roles=("ALLOCATED_CONTRACT",),
                selection_reasons=("C8_DESIRED_STATE",),
                evidence_references=tuple(
                    value
                    for value in (
                        row.snapshot_payload_sha256,
                        row.open_interest_member_sha256,
                    )
                    if value is not None
                ),
                flow_eligible=(
                    row.underlyer in EXPECTED_UNDERLYINGS
                    and flow_policy.lane_for(
                        (row.expiration_date - claim.session_date).days
                    )
                    is not None
                ),
            )
            for row in admission.contracts
        }
        if allocation is None:
            if (
                not self._exact_four_compatibility
                or len(catalog_members) != 4
            ):
                raise AdvancedOptionsPreparationTerminal(
                    "unprofiled selection is limited to explicit "
                    "exact-four compatibility"
                )
            members = tuple(catalog_members.values())
            active_budget = len(members)
            quote_contract_ceiling = 1000
            quote_slot_reserve = (
                quote_contract_ceiling - active_budget
            )
            admission_profile_version = None
            admission_profile_sha256 = None
            allocation_document = None
            allocation_coverage = ()
        else:
            members = c6_members_from_activity_allocation(
                allocation,
                catalog_members=catalog_members,
            )
            if {
                row.contract_ticker for row in members
            } != {
                row.contract_ticker
                for row in admission.contracts
            }:
                raise AdvancedOptionsPreparationTerminal(
                    "allocated membership diverges from opening admission"
                )
            active_budget = allocation.active_contract_budget
            quote_contract_ceiling = (
                allocation.quote_contract_ceiling
            )
            quote_slot_reserve = allocation.quote_slot_reserve
            admission_profile_version = allocation.profile_version
            admission_profile_sha256 = allocation.profile_sha256
            allocation_document = (
                activity_momentum_allocation_payload(allocation)
            )
            allocation_coverage = (
                c6_coverage_from_activity_allocation(allocation)
            )
        capacity = C6ConnectionCapacity(
            account_alias="MASSIVE_ADVANCED",
            provider_cluster="OPTIONS",
            configured_cluster_connection_limit=1,
            provider_cluster_connection_limit=1,
            active_cluster_connections=0,
            proposed_physical_connections=1,
            quote_contract_ceiling=quote_contract_ceiling,
            active_quote_contract_budget=active_budget,
            quote_slot_reserve=quote_slot_reserve,
            maximum_contracts_per_logical_shard=(
                self._maximum_contracts_per_logical_shard
            ),
        )
        selection_observed_at = max(
            admission.prepared_at,
            reference.known_at,
        )
        missing_oi_underlyings = {
            row.underlyer
            for row in admission.contracts
            if row.open_interest is None
        }
        return build_c6_selection_plan(
            session_date=claim.session_date,
            desired_revision_id=claim.revision_id,
            desired_snapshot_sha256=claim.desired_snapshot_sha256,
            selection_policy=policy,
            members=members,
            readiness=tuple(
                C6UnderlyingReadiness(
                    underlying=ticker,
                    catalog=C6ReadinessState.AVAILABLE,
                    open_interest=(
                        C6ReadinessState.NOT_REQUIRED
                        if ticker in missing_oi_underlyings
                        else C6ReadinessState.AVAILABLE
                    ),
                    history=C6ReadinessState.AVAILABLE,
                    feed_states=(
                        ("OPTION_QUOTE", C6ReadinessState.AVAILABLE),
                        ("OPTION_TRADE", C6ReadinessState.AVAILABLE),
                    ),
                    reason_codes=(
                        ("MISSING_PRIOR_OPEN_INTEREST",)
                        if ticker in missing_oi_underlyings
                        else ()
                    ),
                    observed_at=selection_observed_at,
                    source_as_of=reference.known_at,
                )
                for ticker in claim.option_underlyings
            ),
            capacity=capacity,
            strategy_policies=(
                load_strategy_policy_identity(
                    _C6_STRATEGY_POLICY_PATH
                ),
            ),
            catalog_identity=readiness.slot_set_member_sha256,
            reference_identity=reference.state_sha256,
            source_as_of=selection_observed_at,
            admission_profile_version=admission_profile_version,
            admission_profile_sha256=admission_profile_sha256,
            allocation_document=allocation_document,
            allocation_coverage=allocation_coverage,
            physical_owner_id=acquisition.owner_id,
            journal_generation=acquisition.authority.journal_generation,
        )
