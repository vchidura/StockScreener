from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from math import isfinite
from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

import requests
from options.calendar import OptionExchangeCalendar
from options.data.polygon_http import PolygonHttpResponse
from provider_endpoints import REST_BASE_URL

from .contracts import DEVELOPER_FORWARD_UNIVERSE, EQUITY_VALIDATION_UNIVERSE, UniverseManifest, canonical_sha256
from .database_access import RealtimeDatabaseRole
from .equity_portal_history import build_realtime_equity_portal_history_plan
from .equity_validation_history import persist_equity_validation_facts_cursor
from .equity_validation_source import EquityValidationIdentity, EquityValidationSourceLimits, PolygonEquityValidationHistorySource
from .forward_bindings import (
    _load_context_bars, _load_open_interest, _load_split_coverage, load_developer_collection_facts_cursor,
)
from .forward_launcher import DeveloperForwardEnrollmentReceipt
from .forward_runtime import DeveloperForwardScheduler, developer_forward_session_windows, _require_role
from .forward_storage import _database_clock
from .option_manifest_repository import option_subscription_manifest_id
from .preflight import ProviderTier, TierState
from .reference_facts import ReferenceFact, ReferenceFactRelation
from .reference_repository import persist_reference_facts_cursor
from .reference_seed import SeedStatus
from .reference_service import ReferenceSeedRequest, ReferenceSeedService
from .reference_transports import ReferenceTransportLimits, build_live_reference_service
from .tier_cohort import (
    DEVELOPER_CAPABILITIES, DEVELOPER_DISABLED_DETECTORS, TierCohortBinding,
    persist_tier_cohort_binding_cursor,
)


@dataclass(frozen=True, slots=True)
class DeveloperForwardBootstrapPlan:
    enrollment: DeveloperForwardEnrollmentReceipt
    history_session: date
    expected_history_requests: int
    expected_history_facts: int
    maximum_requests: int
    maximum_response_bytes: int
    deadline: datetime

    def __post_init__(self):
        count = len(self.enrollment.result.plan.scope.universe.equity_tickers)
        expected = build_realtime_equity_portal_history_plan(self.history_session, equity_ticker_count=count)
        if (self.enrollment.cohort_id is None or self.maximum_requests < self.expected_history_requests + 2 * count
            or self.expected_history_requests != expected.expected_provider_requests
            or self.expected_history_facts != expected.expected_total_facts
                or not 1 <= self.maximum_requests <= 100_000
                or not 1 <= self.maximum_response_bytes <= 64 * 1024 * 1024 * 1024
                or self.deadline.utcoffset() is None or self.deadline <= self.enrollment.effective_at):
            raise ValueError("bootstrap plan identity, budgets or deadline are invalid")

    @property
    def sha256(self):
        return canonical_sha256(dict(version="developer-forward-bootstrap-v1",
            cohort_id=self.enrollment.cohort_id, enrollment_plan_sha256=self.enrollment.process_plan_sha256,
            history_session=self.history_session, expected_history_requests=self.expected_history_requests,
            expected_history_facts=self.expected_history_facts, maximum_requests=self.maximum_requests,
            maximum_response_bytes=self.maximum_response_bytes, deadline=self.deadline))


@dataclass(frozen=True, slots=True)
class DeveloperForwardCurrentPlan:
    enrollment: DeveloperForwardEnrollmentReceipt
    maximum_requests: int
    maximum_response_bytes: int
    deadline: datetime

    def __post_init__(self):
        count = len(self.enrollment.result.plan.scope.universe.equity_tickers)
        byte_limit = min(64 * 1024 * 1024 * ((count + 8) // 9), 512 * 1024 * 1024)
        if (self.enrollment.cohort_id is None or not 2 * count <= self.maximum_requests <= 4 * count
            or not 1 <= self.maximum_response_bytes <= byte_limit
                or self.deadline.utcoffset() is None or self.deadline <= self.enrollment.effective_at):
            raise ValueError("current-input plan identity, budgets or deadline are invalid")

    @property
    def sha256(self):
        return canonical_sha256(dict(version="developer-forward-current-inputs-v1",
            cohort_id=self.enrollment.cohort_id,
            enrollment_plan_sha256=self.enrollment.process_plan_sha256,
            maximum_requests=self.maximum_requests,
            maximum_response_bytes=self.maximum_response_bytes, deadline=self.deadline))


def build_developer_forward_bootstrap_plan(enrollment, *, history_session, maximum_requests,
                                         maximum_response_bytes, deadline):
    if (not isinstance(enrollment, DeveloperForwardEnrollmentReceipt) or enrollment.cohort_id is None
            or not isinstance(history_session, date) or history_session >= enrollment.result.plan.session_date
            or deadline.utcoffset() is None or deadline <= enrollment.effective_at
            or type(maximum_requests) is not int or not 1 <= maximum_requests <= 100_000
            or type(maximum_response_bytes) is not int or not 1 <= maximum_response_bytes <= 64 * 1024 * 1024 * 1024):
        raise ValueError("bootstrap plan requires exact enrollment, prior completed history and explicit bounded budgets")
    count = len(enrollment.result.plan.scope.universe.equity_tickers)
    history = build_realtime_equity_portal_history_plan(history_session, equity_ticker_count=count)
    if maximum_requests < history.expected_provider_requests + 2 * count:
        raise ValueError("bootstrap request budget cannot cover history plus current spot/context requests")
    return DeveloperForwardBootstrapPlan(enrollment, history_session, history.expected_provider_requests,
        history.expected_total_facts, maximum_requests, maximum_response_bytes, deadline)


def build_developer_forward_current_plan(enrollment, *, maximum_requests,
                                         maximum_response_bytes, deadline):
    if not isinstance(enrollment, DeveloperForwardEnrollmentReceipt) or enrollment.cohort_id is None:
        raise ValueError("current-input plan requires an exact ready reviewed enrollment")
    return DeveloperForwardCurrentPlan(enrollment, maximum_requests, maximum_response_bytes, deadline)


@dataclass(frozen=True, slots=True)
class _BootstrapAccountBudget:
    maximum_requests: int
    maximum_response_bytes: int
    requests_per_second: Decimal = Decimal("2")
    maximum_attempts: int = 3


class _BootstrapPacedHttp:
    def __init__(self, session, scheduler, *, available_after, deadline, massive_api_key):
        self.session, self.scheduler = session, scheduler
        self.available_after, self.deadline = available_after, deadline
        self._massive_api_key = massive_api_key

    def get(self, url, params, headers=None):
        active_headers = dict(headers) if headers is not None else {"Authorization": f"Bearer {self._massive_api_key}"}
        def operation():
            response = self.session.get(url, params=dict(params), headers=active_headers,
                timeout=(5, 30), allow_redirects=False)
            return PolygonHttpResponse(response.status_code, response.content, tuple(response.headers.items()))
        return self.scheduler.request_with_retry(operation, available_after=self.available_after, deadline=self.deadline)


@dataclass(slots=True)
class DeveloperForwardBootstrapSources:
    reference_service: ReferenceSeedService
    history_source: PolygonEquityValidationHistorySource
    ledger: DeveloperForwardScheduler
    session: object

    def close(self):
        self.session.close()


def build_developer_forward_bootstrap_sources(plan, identities, *, environ, clock, wait, session_factory=requests.Session):
    universe = plan.enrollment.result.plan.scope.universe
    count = len(universe.equity_tickers)
    names = ("POLYGON_API_KEY", "FINNHUB_API_KEY")
    if (not all(str(environ.get(name) or "").strip() for name in names)
            or tuple(row.ticker for row in identities) != universe.equity_tickers
            or not callable(clock) or not callable(wait)):
        raise ValueError("bootstrap sources require exact identities, credentials and reviewed clock/pacing")
    started_at = clock()
    if started_at.utcoffset() is None or started_at >= plan.deadline:
        raise ValueError("bootstrap sources are outside the reviewed deadline")
    ledger = DeveloperForwardScheduler(_BootstrapAccountBudget(plan.maximum_requests, plan.maximum_response_bytes),
        clock=clock, wait=wait)
    session = session_factory()
    try:
        if session.get_adapter(REST_BASE_URL).max_retries.total != 0:
            raise ValueError("bootstrap HTTP adapters must not retry outside the account ledger")
        http = _BootstrapPacedHttp(session, ledger, available_after=started_at,
            deadline=plan.deadline, massive_api_key=str(environ["POLYGON_API_KEY"]))
        references = build_live_reference_service(massive_api_key=str(environ["POLYGON_API_KEY"]),
            finnhub_api_key=str(environ["FINNHUB_API_KEY"]), etf_tickers=frozenset({"SPY", "QQQ", "IWM"}),
            http=http, limits=ReferenceTransportLimits(maximum_requests=68), finnhub_sleep=wait,
            corporate_action_lookback_days=(plan.enrollment.result.plan.session_date
                - build_realtime_equity_portal_history_plan(plan.history_session, equity_ticker_count=count).adjusted_sessions[0]).days + 1)
        history = PolygonEquityValidationHistorySource(http, tuple(identities), cohort_id=plan.enrollment.cohort_id,
            limits=EquityValidationSourceLimits(maximum_requests=plan.maximum_requests,
                maximum_response_bytes=plan.maximum_response_bytes, maximum_workers=1, maximum_retries_per_request=0),
            clock=clock, sleep=wait, universe=universe)
        return DeveloperForwardBootstrapSources(references, history, ledger, session)
    except Exception:
        session.close()
        raise


def build_developer_forward_current_sources(plan, identities, *, environ, clock, wait,
                                            session_factory=requests.Session):
    universe = plan.enrollment.result.plan.scope.universe
    if (not isinstance(plan, DeveloperForwardCurrentPlan)
            or not str(environ.get("POLYGON_API_KEY") or "").strip()
        or tuple(row.ticker for row in identities) != universe.equity_tickers
            or not callable(clock) or not callable(wait)):
        raise ValueError("current inputs require exact identities, provider credential and reviewed clock/pacing")
    started_at = clock()
    if started_at.utcoffset() is None or started_at >= plan.deadline:
        raise ValueError("current-input sources are outside the reviewed deadline")
    ledger = DeveloperForwardScheduler(_BootstrapAccountBudget(plan.maximum_requests, plan.maximum_response_bytes),
        clock=clock, wait=wait)
    session = session_factory()
    try:
        if session.get_adapter(REST_BASE_URL).max_retries.total != 0:
            raise ValueError("current-input HTTP adapters must not retry outside the account ledger")
        http = _BootstrapPacedHttp(session, ledger, available_after=started_at,
            deadline=plan.deadline, massive_api_key=str(environ["POLYGON_API_KEY"]))
        history = PolygonEquityValidationHistorySource(http, tuple(identities),
            cohort_id=plan.enrollment.cohort_id,
            limits=EquityValidationSourceLimits(maximum_requests=plan.maximum_requests,
                maximum_response_bytes=plan.maximum_response_bytes, maximum_workers=1,
                maximum_retries_per_request=0), clock=clock, sleep=wait,
            universe=universe)
        return DeveloperForwardBootstrapSources(None, history, ledger, session)
    except Exception:
        session.close()
        raise


def _verify_enrollment_cursor(cursor, enrollment):
    cursor.execute("SELECT manifest_id::text,configuration_sha256,model_bundle_sha256,valuation_sha256,"
        "effective_at,mode FROM rt.cohorts WHERE cohort_id=%s", (str(enrollment.cohort_id),))
    binding = enrollment.cohort_binding
    expected = (str(option_subscription_manifest_id(enrollment.result.plan)), binding.configuration_sha256,
        binding.model_bundle_sha256, binding.valuation_sha256, enrollment.effective_at, "SHADOW")
    row = cursor.fetchone()
    if row is None or tuple(row) != expected:
        raise ValueError("bootstrap target cohort/enrollment hashes are absent or drifted")


def load_bootstrap_equity_identities_cursor(cursor, enrollment, *, selected_at):
    _verify_enrollment_cursor(cursor, enrollment)
    cursor.execute("SELECT DISTINCT ON (ticker) revision.*,revision.revision_id AS security_revision_id,instrument.instrument_id "
        "FROM ref.security_revisions AS revision JOIN rt.instruments AS instrument USING(security_id) "
        "WHERE ticker=ANY(%s) AND effective_from<=%s AND (effective_to IS NULL OR effective_to>%s) "
        "AND observed_at<=%s AND active ORDER BY ticker,observed_at DESC,revision_id DESC LIMIT %s",
        (list(enrollment.result.plan.scope.universe.equity_tickers), selected_at, selected_at, selected_at,
            len(enrollment.result.plan.scope.universe.equity_tickers) + 1))
    from .ticker_projection import _security

    rows = cursor.fetchall()
    by_ticker = {row["ticker"]: EquityValidationIdentity(
        row["ticker"], _security(row), row["instrument_id"]) for row in rows}
    expected_tickers = enrollment.result.plan.scope.universe.equity_tickers
    if (set(by_ticker) != set(expected_tickers)
            or len({row.security.security_id for row in by_ticker.values()}) != len(by_ticker)):
        raise ValueError("bootstrap dated equity identities are incomplete or duplicated")
    return tuple(by_ticker[ticker] for ticker in expected_tickers)


def dated_open_interest_facts(enrollment, *, received_at):
    if not enrollment.result.snapshots or received_at.utcoffset() is None:
        raise ValueError("bootstrap OI requires complete source snapshots and actual receipt clocks")
    session = enrollment.result.plan.session_date
    prior = OptionExchangeCalendar().previous_session(session)
    admitted = enrollment.matrix_scope(SimpleNamespace(cutoff=received_at, available_after=received_at,
        deadline=received_at + timedelta(seconds=1)), reference_sha256="a" * 64, action_sha256="b" * 64)
    contract_ids = {row.contract_id for row in admitted.catalog}
    facts = []
    for snapshot in enrollment.result.snapshots:
        if snapshot.contract_id not in contract_ids:
            continue
        observed_at = snapshot.revised_observed_at or snapshot.first_observed_at
        if (type(snapshot.open_interest) is not int or snapshot.open_interest < 0
                or observed_at.utcoffset() is None or not snapshot.market_data_time <= observed_at <= received_at
                or snapshot.market_data_time.astimezone(ZoneInfo("America/New_York")).date() != session):
            raise ValueError("bootstrap OI source/value/session clocks are unavailable or invalid")
        digest = canonical_sha256(dict(contract_id=snapshot.contract_id, settlement_session=prior,
            observed_session=session, open_interest=snapshot.open_interest, observed_at=observed_at,
            received_at=received_at, raw_payload_sha256=snapshot.raw_payload_sha256,
            normalized_payload_sha256=snapshot.normalized_payload_sha256))
        facts.append(ReferenceFact.build(ReferenceFactRelation.OI_BASELINE, "OI_BASELINE", snapshot.underlyer,
            baseline_id=str(uuid5(NAMESPACE_URL, f"developer-forward-oi:{digest}")), contract_id=snapshot.contract_id,
            settlement_session=prior, open_interest=snapshot.open_interest, source="massive",
            date_basis="PROVIDER_CHAIN_SNAPSHOT_PRIOR_SESSION", observed_session=session,
            observed_at=observed_at, received_at=received_at, payload_sha256=digest, supersedes_baseline_id=None))
    if {dict(row.values)["contract_id"] for row in facts} != contract_ids:
        raise ValueError("bootstrap OI does not cover every admitted contract")
    return tuple(facts)


def run_developer_forward_historical_bootstrap(plan, *, references_database, compute_database,
                                             sources, split_coverage=None, split_actions=(), clock):
    _require_role(references_database, RealtimeDatabaseRole.REFERENCES)
    _require_role(compute_database, RealtimeDatabaseRole.COMPUTE)
    if not isinstance(sources, DeveloperForwardBootstrapSources):
        raise ValueError("bootstrap requires concrete shared-ledger sources")
    reference_service, history_source, ledger = sources.reference_service, sources.history_source, sources.ledger
    cohort_id = plan.enrollment.cohort_id
    universe = plan.enrollment.result.plan.scope.universe
    equity_reference_universe = (EQUITY_VALIDATION_UNIVERSE
        if universe.equity_tickers == EQUITY_VALIDATION_UNIVERSE.equity_tickers
        else UniverseManifest(f"{universe.version}-equity-only", universe.equity_tickers, ()))
    if (not isinstance(reference_service, ReferenceSeedService)
            or not isinstance(history_source, PolygonEquityValidationHistorySource)
            or history_source.cohort_id != cohort_id
            or history_source.universe != universe):
        raise ValueError("bootstrap requires native exact-cohort reference and history sources")
    now = clock()
    if now.utcoffset() is None or now >= plan.deadline:
        raise ValueError("bootstrap is outside its reviewed deadline")
    with compute_database.transaction(read_only=True, isolation_level="REPEATABLE READ") as cursor:
        _verify_enrollment_cursor(cursor, plan.enrollment)
    references = reference_service.run_once(ReferenceSeedRequest(equity_reference_universe,
        plan.enrollment.result.plan.session_date, now, now, now + timedelta(hours=6)))
    if (references.manifest.status is not SeedStatus.READY
            or references.manifest.universe_sha256 != equity_reference_universe.sha256
            or references.manifest.session_date != plan.enrollment.result.plan.session_date):
        raise ValueError("bootstrap references are incomplete; no ready seed is fabricated")
    if any(fact.relation is ReferenceFactRelation.OPTION_CONTRACT for fact in references.facts):
        raise ValueError("equity reference warm-up cannot replace the enrolled option catalog")
    with references_database.transaction() as cursor:
        persist_reference_facts_cursor(cursor, references.facts)
    if split_coverage is None:
        from scripts.run_realtime_equity_validation import _load_split_evidence

        history_plan = build_realtime_equity_portal_history_plan(plan.history_session,
            equity_ticker_count=len(universe.equity_tickers))
        split_coverage, split_actions = _load_split_evidence(compute_database,
            universe.equity_tickers, raw_start=history_plan.raw_start,
            raw_end=history_plan.raw_end, adjusted_start=history_plan.adjusted_sessions[0], checked_at=clock())
    history = history_source.collect_portal_history(plan.history_session,
        split_coverage=split_coverage, split_actions=split_actions)
    if (history.plan.session_date != plan.history_session or len(history.facts) != plan.expected_history_facts
            or any(fact.cohort_id != cohort_id for fact in history.facts)
            or {fact.revision.ticker for fact in history.facts} != set(universe.equity_tickers)
            or ledger.requests > plan.maximum_requests or ledger.response_bytes > plan.maximum_response_bytes
            or clock() > plan.deadline):
        raise ValueError("bootstrap history changed target/session/coverage/budget or deadline")
    with compute_database.transaction() as cursor:
        persist_equity_validation_facts_cursor(cursor, history.facts)
    oi = dated_open_interest_facts(plan.enrollment, received_at=clock())
    with references_database.transaction() as cursor:
        persist_reference_facts_cursor(cursor, oi)
    action_facts = tuple(row for row in references.facts if row.relation in {
        ReferenceFactRelation.CORPORATE_ACTION, ReferenceFactRelation.CORPORATE_ACTION_COVERAGE})
    return dict(status="HISTORICAL_WARMUP_STORED_NOT_DATA_READY", cohort_id=str(cohort_id),
        plan_sha256=plan.sha256, reference_sha256=references.manifest.seed_sha256,
        action_sha256=canonical_sha256(action_facts),
        historical_fact_count=len(history.facts), oi_contract_count=len(oi),
        provider_requests=ledger.requests, response_bytes=ledger.response_bytes, readiness_persisted=False)


def run_developer_forward_current_spot_bootstrap(plan, *, scope, compute_database, sources, clock):
    _require_role(compute_database, RealtimeDatabaseRole.COMPUTE)
    if scope.cohort_id != plan.enrollment.cohort_id or sources.history_source.cohort_id != scope.cohort_id:
        raise ValueError("current spot bootstrap changed the reviewed cohort")
    http = sources.history_source.transport
    original_deadline = http.deadline
    http.deadline = min(original_deadline, scope.deadline)
    try:
        facts = sources.history_source.collect_forward_minute_inputs(scope.cutoff)
        facts += sources.history_source.collect_forward_context_inputs(scope.cutoff)
    finally:
        http.deadline = original_deadline
    if (any(fact.cohort_id != scope.cohort_id for fact in facts)
            or sources.ledger.requests > plan.maximum_requests
            or sources.ledger.response_bytes > plan.maximum_response_bytes or clock() >= scope.deadline):
        raise ValueError("current spot bootstrap changed target/budget or missed the cutoff deadline")
    with compute_database.transaction() as cursor:
        persist_equity_validation_facts_cursor(cursor, facts)
    return dict(status="CURRENT_SPOTS_CONTEXT_STORED_NOT_DATA_READY", cohort_id=str(scope.cohort_id),
        cutoff=scope.cutoff.isoformat(), fact_count=len(facts), provider_requests=sources.ledger.requests,
        response_bytes=sources.ledger.response_bytes, readiness_persisted=False)


def _require_native_scope_window(scope):
    completion = (scope.deadline - scope.available_after).total_seconds()
    if (not completion.is_integer() or not 1 <= completion <= 90
            or not any(window.cutoff == scope.cutoff and window.available_after == scope.available_after
                and window.deadline == scope.deadline for window in developer_forward_session_windows(
                    scope.selection.session_date, completion_budget_seconds=int(completion), serving_lag_seconds=1))):
        raise ValueError("bootstrap scope changed the native exchange/release/deadline window")


def accept_developer_forward_bootstrap(enrollment, configuration, *, scope, compute_database,
                                      control_database):
    _require_role(compute_database, RealtimeDatabaseRole.COMPUTE)
    _require_role(control_database, RealtimeDatabaseRole.CONTROL)
    _require_native_scope_window(scope)
    if (scope.cohort_id != enrollment.cohort_id or scope.selection != enrollment.result.plan
            or configuration.configuration_sha256 != enrollment.cohort_binding.configuration_sha256
            or configuration.settings.risk_free_rate_source != "us_treasury_par_yield_v1"
            or configuration.settings.dividend_input_source not in (None, "polygon_corporate_actions_v1")):
        raise ValueError("bootstrap acceptance changed exact scope/configuration or valuation inputs")
    with compute_database.transaction(dict_cursor=True, read_only=True, isolation_level="REPEATABLE READ") as cursor:
        selected_at = _database_clock(cursor)
        if not scope.available_after <= selected_at < scope.deadline:
            raise ValueError("bootstrap acceptance is outside the original cutoff deadline")
        for symbol in scope.underlyings:
            facts = load_developer_collection_facts_cursor(cursor, scope, symbol, selected_at=selected_at,
                valuation_maximum_age_seconds=configuration.settings.dividend_input_max_age_seconds,
                dividend_input_source=configuration.settings.dividend_input_source,
                default_dividend_yield=configuration.settings.default_dividend_yield)
            security = SimpleNamespace(security_id=facts["security_id"], ticker=symbol)
            bars = _load_context_bars(cursor, scope, security, selected_at)
            if (sum(bar.interval == "30m" for bar, _ in bars) < 202
                    or sum(bar.interval == "1d" for bar, _ in bars) < 21
                    or not _load_split_coverage(cursor, scope, security, bars, selected_at)):
                raise ValueError("bootstrap live-mode trend/liquidity/split inputs are incomplete")
        admitted_ids = {row.contract_id for row in scope.catalog}
        matrices = (SimpleNamespace(snapshots=tuple(row for row in enrollment.result.snapshots
            if row.contract_id in admitted_ids)),)
        oi = _load_open_interest(cursor, scope, matrices, selected_at, OptionExchangeCalendar())
        if {row.record.contract_id for row in oi} != {row.contract_id for row in scope.catalog}:
            raise ValueError("bootstrap settled OI coverage is incomplete")
    with control_database.transaction() as cursor:
        _verify_enrollment_cursor(cursor, enrollment)
        accepted_at = _database_clock(cursor)
        if not selected_at <= accepted_at < scope.deadline:
            raise ValueError("bootstrap acceptance completed outside the original deadline")
        binding = TierCohortBinding(enrollment.cohort_id, enrollment.result.plan.scope.profile_id,
            ProviderTier.MASSIVE_DEVELOPER, TierState.DEVELOPER_READY, enrollment.result.plan.manifest.sha256,
            configuration.valuation_policy_sha256, DEVELOPER_CAPABILITIES, DEVELOPER_DISABLED_DETECTORS, accepted_at)
        persist_tier_cohort_binding_cursor(cursor, binding)
    return binding