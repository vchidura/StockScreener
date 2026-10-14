from __future__ import annotations

from dataclasses import dataclass
import json
import threading
from datetime import date, datetime, timedelta
from decimal import Decimal
from uuid import UUID
from email.utils import parsedate_to_datetime
from requests.exceptions import ConnectionError as ProviderConnectionError, Timeout as ProviderTimeout

from .database_access import RealtimeDatabaseRole
from .forward_ingestion import (
    admit_developer_matrix, load_forward_detector_context, normalize_developer_matrix, seal_offline_forward_publication,
)
from .forward_storage import (
    _database_clock, claim_forward_work_cursor, complete_forward_work_cursor, decode_forward_artifact,
    enqueue_forward_work_cursor, load_forward_inputs_cursor, persist_forward_inputs_cursor, publish_forward_release_cursor,
)
from .option_alert_projection import (
    ForwardAlertCycleSource, technical_detector_dataset_id, seal_offline_indicative_alert_publication, seal_offline_option_alert_publication,
)
from .option_flow_projection import seal_offline_option_flow_publications
from .option_package_context import ForwardO1PackageSource, ForwardTradeWindow, load_forward_technical_package_context
from .option_screener_projection import seal_offline_eligible_chain_publication
from .preflight import ProviderTier
from .worker_database import RealtimeProcessDatabase


@dataclass(frozen=True, slots=True)
class PackageInputFacts:
    underlyer: str
    references: tuple
    raw_spot_bars: tuple
    structural_bars: tuple
    structural_policy_sha256: str
    calendars: tuple
    planned_entry_at: datetime
    entry_deadline: datetime
    exit_deadline: datetime

    def bind(self, scope, matrix, context):
        if self.underlyer != matrix.analysis.underlying.underlyer:
            raise ValueError("package input facts changed their exact underlying")
        return ForwardO1PackageSource(scope.sha256, matrix.content_sha256, context.content_sha256,
            "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER, self.references, self.raw_spot_bars,
            self.structural_bars, self.structural_policy_sha256, self.calendars,
            self.planned_entry_at, self.entry_deadline, self.exit_deadline)


@dataclass(frozen=True, slots=True)
class PackageInputFactsWithTrades(PackageInputFacts):
    trade_window: ForwardTradeWindow | None = None


def _require_role(database, role):
    if not isinstance(database, RealtimeProcessDatabase) or database.process_plan.role is not role:
        raise ValueError(f"Developer forward stage requires target-local {role.value} authority")


def ingest_developer_forward_inputs(database, scope, sources, **facts):
    _require_role(database, RealtimeDatabaseRole.INGRESS)
    with database.transaction(dict_cursor=True) as cursor:
        received_at = _database_clock(cursor)
        return persist_forward_inputs_cursor(cursor, scope, sources, received_at=received_at, **facts)


def enqueue_developer_forward_inputs(database, input_id):
    _require_role(database, RealtimeDatabaseRole.COMPUTE)
    with database.transaction(dict_cursor=True) as cursor:
        enqueue_forward_work_cursor(cursor, input_id)


def _prior_alert_publications(cursor, scope, configuration):
    cursor.execute("SELECT artifact.payload_text,artifact.payload_sha256 FROM rt.developer_forward_releases AS release "
        "JOIN rt.developer_forward_inputs AS input USING(input_id) "
        "JOIN rt.developer_forward_artifacts AS artifact USING(input_id) WHERE release.cohort_id=%s "
        "AND input.selection_sha256=%s AND input.configuration_sha256=%s AND input.cutoff<%s "
        "AND artifact.artifact_kind='ALERTS' ORDER BY input.cutoff DESC LIMIT 96",
        (str(scope.cohort_id), scope.selection.selection_sha256, scope.configuration_sha256, scope.cutoff))
    for row in cursor.fetchall():
        publication = decode_forward_artifact(row["payload_text"], row["payload_sha256"])
        if publication.dataset_id == technical_detector_dataset_id(scope, configuration):
            return (publication,)
    return ()


class _PriorEvaluations:
    def __init__(self, publications, prior_context=None):
        self.publications = publications
        self.prior_context = prior_context

    def prior_selected(self, **kwargs):
        return {}

    def prior_o1_observed(self, **kwargs):
        return {row.recurrence_sha256 for row in self.prior_context.inputs.o1_observations} if self.prior_context else set()


def materialize_developer_forward_once(database, configuration, *, cohort_id, owner_id):
    _require_role(database, RealtimeDatabaseRole.COMPUTE)
    with database.transaction(dict_cursor=True) as cursor:
        claim = claim_forward_work_cursor(cursor, cohort_id=cohort_id, owner_id=owner_id)
        if claim is None:
            return None
    with database.transaction(dict_cursor=True, isolation_level="REPEATABLE READ") as cursor:
        inputs = load_forward_inputs_cursor(cursor, claim.input_id)
        scope = inputs["scope"]
        if scope.cohort_id != cohort_id or scope.configuration_sha256 != configuration.configuration_sha256:
            raise ValueError("forward compute enrollment/configuration scope changed")
        matrices = tuple(normalize_developer_matrix(admit_developer_matrix(scope, source), configuration)
            for source in inputs["sources"])
        selected_at = _database_clock(cursor)
        missing = tuple((symbol, ("MATRIX_INPUT_UNAVAILABLE",)) for symbol in scope.underlyings
            if symbol not in {matrix.analysis.underlying.underlyer for matrix in matrices})
        context = load_forward_detector_context(scope, matrices, configuration, inputs["detector_source"], selected_at=selected_at) if inputs["detector_source"] else None
        facts_by_symbol = {row.underlyer: row for row in inputs["package_sources"]}
        if len(facts_by_symbol) != len(inputs["package_sources"]):
            raise ValueError("forward package input facts are ambiguous")
        packages = tuple(load_forward_technical_package_context(scope, matrix, configuration, context,
            facts_by_symbol[matrix.analysis.underlying.underlyer].bind(scope, matrix, context), selected_at=selected_at,
            trade_window=getattr(facts_by_symbol[matrix.analysis.underlying.underlyer], "trade_window", None))
            for matrix in matrices if context is not None and matrix.analysis.underlying.underlyer in facts_by_symbol)
        prior = _prior_alert_publications(cursor, scope, configuration)
        cursor.execute("SELECT artifact.payload_text,artifact.payload_sha256 FROM rt.developer_forward_releases AS release "
            "JOIN rt.developer_forward_inputs AS input USING(input_id) JOIN rt.developer_forward_artifacts AS artifact USING(input_id) "
            "WHERE release.cohort_id=%s AND input.selection_sha256=%s AND input.configuration_sha256=%s "
            "AND input.cutoff<%s AND artifact.artifact_kind='CONTEXT' ORDER BY input.cutoff DESC LIMIT 1",
            (str(scope.cohort_id), scope.selection.selection_sha256, scope.configuration_sha256, scope.cutoff))
        retained_context = cursor.fetchone()
        prior_context = decode_forward_artifact(retained_context["payload_text"], retained_context["payload_sha256"]) if retained_context else None
        forward = seal_offline_forward_publication(scope, matrices, configuration, selected_at=selected_at,
            detector_context=context, unavailable_underlyers=missing, prior_evaluations=_PriorEvaluations(prior, prior_context))
        source = ForwardAlertCycleSource(scope, forward, "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER, context, packages)
        if packages:
            alerts = seal_offline_indicative_alert_publication(scope, configuration, selected_at=selected_at,
                cycles=(source,), prior_publications=prior)
        else:
            alerts = seal_offline_option_alert_publication(scope, configuration, selected_at=selected_at, cycles=(source,))
        artifacts = dict(MATRICES=matrices, CONTEXT=context, PACKAGES=packages, ALERTS=alerts,
            FLOW=seal_offline_option_flow_publications(scope, matrices, configuration, selected_at=selected_at,
                chain_evidence=inputs["chain_evidence"], unavailable_underlyers=missing),
            SCREENER=seal_offline_eligible_chain_publication(scope, matrices, configuration, selected_at=selected_at,
                chain_evidence=inputs["chain_evidence"], unavailable_underlyers=missing))
        complete_forward_work_cursor(cursor, claim, scope, artifacts)
    return claim.input_id


def publish_developer_forward_release(database, input_id, *, valid_until):
    _require_role(database, RealtimeDatabaseRole.PUBLICATION)
    with database.transaction(dict_cursor=True, isolation_level="REPEATABLE READ") as cursor:
        return publish_forward_release_cursor(cursor, input_id, valid_until=valid_until)


@dataclass(frozen=True, slots=True)
class DeveloperForwardBudget:
    maximum_requests: int = 24
    maximum_response_bytes: int = 33554432
    maximum_inflight: int = 1
    requests_per_second: Decimal = Decimal("2")
    maximum_attempts: int = 3

    def __post_init__(self):
        if (not 1 <= self.maximum_requests <= 1000 or not 1 <= self.maximum_response_bytes <= 67108864
                or not 1 <= self.maximum_inflight <= 2 or not Decimal("0") < self.requests_per_second <= Decimal("2")
                or not 1 <= self.maximum_attempts <= 3):
            raise ValueError("Developer acquisition requires an explicit bounded reviewed budget")


class DeveloperForwardScheduler:
    def __init__(self, budget, *, clock, wait):
        self.budget = budget
        self.clock = clock
        self.wait = wait
        self.next_request_at = None
        self.requests = 0
        self.response_bytes = 0
        self._request_lock = threading.Lock()

    def request(self, operation, *, available_after, deadline):
        if not self._request_lock.acquire(blocking=False):
            raise ValueError("Developer scheduler owns one bounded synchronous in-flight request")
        try:
            return self._request(operation, available_after=available_after, deadline=deadline)
        finally:
            self._request_lock.release()

    def _request(self, operation, *, available_after, deadline):
        now = self.clock()
        if now.utcoffset() is None or not available_after <= now < deadline:
            raise ValueError("Developer request is before release or past deadline")
        if self.requests >= self.budget.maximum_requests:
            raise ValueError("Developer request budget exhausted")
        if self.next_request_at and now < self.next_request_at:
            delay = (self.next_request_at - now).total_seconds()
            if self.next_request_at >= deadline:
                raise ValueError("Developer pacing cannot meet immutable cutoff deadline")
            self.wait(delay)
            now = self.clock()
            if now < self.next_request_at or now >= deadline:
                raise ValueError("Developer pacing clock did not advance inside the release/deadline window")
        self.requests += 1
        self.next_request_at = now + timedelta(seconds=float(Decimal("1") / self.budget.requests_per_second))
        result = operation()
        size = getattr(result, "response_bytes", getattr(result, "body", None))
        size = len(size) if isinstance(size, bytes) else size
        if type(size) is not int or size < 0:
            raise ValueError("Developer source must report exact response byte evidence")
        self.response_bytes += size
        if self.response_bytes > self.budget.maximum_response_bytes or self.clock() > deadline:
            raise ValueError("Developer response exceeded byte/deadline budget")
        return result

    def request_with_retry(self, operation, *, available_after, deadline):
        for attempt in range(self.budget.maximum_attempts):
            try:
                result = self.request(operation, available_after=available_after, deadline=deadline)
            except (TimeoutError, ConnectionError, ProviderTimeout, ProviderConnectionError):
                if attempt + 1 == self.budget.maximum_attempts:
                    raise ValueError("Developer bounded timeout retry budget exhausted")
                retry_at = self.clock() + timedelta(seconds=2 ** attempt)
                if retry_at >= deadline:
                    raise ValueError("Developer timeout retry exceeds immutable deadline")
                self.next_request_at = max(self.next_request_at, retry_at)
                continue
            status = getattr(result, "status_code", 200)
            if status not in (429, 500, 502, 503, 504):
                if status in (401, 403):
                    raise ValueError("Developer entitlement/auth failure blocks acquisition")
                return result
            if attempt + 1 == self.budget.maximum_attempts:
                raise ValueError("Developer bounded transport retry budget exhausted")
            headers = getattr(result, "headers", {})
            headers = dict(headers or {})
            retry_after = getattr(result, "retry_after_seconds", None)
            if retry_after is None:
                value = result.header("Retry-After") if hasattr(result, "header") else headers.get("Retry-After", headers.get("retry-after", 2 ** attempt))
                try:
                    retry_after = float(value) if value is not None else 2 ** attempt
                except ValueError:
                    retry_after = max(0, (parsedate_to_datetime(value) - self.clock()).total_seconds())
            if retry_after < 0 or self.clock() + timedelta(seconds=retry_after) >= deadline:
                raise ValueError("Developer Retry-After exceeds immutable deadline")
            self.next_request_at = max(self.next_request_at, self.clock() + timedelta(seconds=retry_after))


class PacedDeveloperHttp:
    def __init__(self, http, scheduler, scope):
        self.http, self.scheduler, self.scope = http, scheduler, scope

    def get(self, url, params):
        return self.scheduler.request_with_retry(lambda: self.http.get(url, params),
            available_after=self.scope.available_after, deadline=self.scope.deadline)


def build_paced_developer_discovery(http, scheduler, scope, configuration):
    from .polygon_option_discovery import PolygonDeveloperOptionDiscoverySource

    return PolygonDeveloperOptionDiscoverySource(PacedDeveloperHttp(http, scheduler, scope), configuration,
        scope.selection.scope, clock=scheduler.clock)


def collect_developer_forward_cycle(scope, configuration, *, scheduler, discovery_source, facts_loader):
    from options.data.normalizer import DeveloperNormalizationInput, parse_polygon_snapshot
    from .forward_ingestion import CompletedDeveloperMatrixSource, DeveloperMatrixPage, PolicyChainMatrixScope
    from .option_flow_projection import DeveloperFlowChainEvidence
    from .option_discovery_repository import RealtimeOptionDiscoveryEvidence
    from options.domain import DataQualityFlag
    from options.model_inputs import ExpirationValuationCandidate

    if not isinstance(scope, PolicyChainMatrixScope) or discovery_source.scope != scope.selection.scope:
        raise ValueError("runtime REST collection requires explicit approved full-policy scope")
    sources, chains = [], []
    for symbol in scope.underlyings:
        facts = facts_loader(symbol, scope)
        if (facts["scope_sha256"] != scope.sha256 or facts["database_name"] != "stocks_realtime_v1"
                or facts["provider_tier"] != ProviderTier.MASSIVE_DEVELOPER):
            raise ValueError("Developer source facts cannot fall back across target/tier/scope")
        discovered = discovery_source.discover(symbol, scope.cutoff, facts["spot_price"])
        inputs, pages = [], []
        catalog = {row.contract_ticker: row for row in scope.catalog if row.underlyer == symbol}
        for page in discovered.batch.pages:
            values = json.loads(page.response_bytes)["results"]
            page_inputs = []
            for value in values:
                raw = parse_polygon_snapshot(value, page.received_at)
                if raw.contract_ticker not in catalog:
                    raise ValueError("Developer full-chain discovery lacks admitted target-local references")
                entry = catalog[raw.contract_ticker]
                candidate = facts["valuation_candidates"][entry.expiration_date]
                covered = isinstance(candidate, ExpirationValuationCandidate)
                item = DeveloperNormalizationInput(raw, entry, tuple(facts["underlying_bars"]),
                    facts["expiration_cutoffs"][entry.contract_id], candidate.risk_free_rate,
                    candidate.dividend_yield,
                    dividend_cash_flows=candidate.dividend_cash_flows if covered else (),
                    dividend_coverage_available=covered,
                    input_quality_flags=(() if covered else (DataQualityFlag.DIVIDEND_YIELD_DEFAULTED,)),
                    normalized_observed_at=page.received_at,
                    valuation_candidate=candidate if covered else None)
                page_inputs.append(item)
                inputs.append(item)
            pages.append(DeveloperMatrixPage(page.request_cursor_sha256, page.next_cursor_sha256, page.payload_sha256, tuple(page_inputs)))
        completed = scheduler.clock()
        source = CompletedDeveloperMatrixSource(scope.cohort_id, scope.selection.selection_sha256, symbol, scope.cutoff,
            discovered.batch.started_at, completed, tuple(pages))
        admit_developer_matrix(scope, source)
        evidence = RealtimeOptionDiscoveryEvidence(scope.selection.scope, facts["security_id"], discovered)
        sources.append(source)
        chains.append(DeveloperFlowChainEvidence(scope.sha256, "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER, evidence, tuple(inputs)))
    return tuple(sources), tuple(chains)


@dataclass(frozen=True, slots=True)
class DeveloperForwardWindow:
    cutoff: datetime
    available_after: datetime
    deadline: datetime
    serving_expires_at: datetime


def developer_forward_session_windows(session_date: date, *, completion_budget_seconds: int,
                                      serving_lag_seconds: int, calendar=None):
    from options.calendar import OptionExchangeCalendar

    if (type(completion_budget_seconds) is not int or not 1 <= completion_budget_seconds <= 90
            or type(serving_lag_seconds) is not int or not 1 <= serving_lag_seconds <= 90):
        raise ValueError("Developer schedule requires reviewed bounded completion/serving budgets")
    calendar = calendar or OptionExchangeCalendar()
    opened, closed = calendar.session_open(session_date), calendar.session_close(session_date)
    cutoff = opened + timedelta(minutes=15)
    windows = []
    while cutoff <= closed:
        release = cutoff + timedelta(minutes=15)
        windows.append(DeveloperForwardWindow(cutoff, release, release + timedelta(seconds=completion_budget_seconds),
            release + timedelta(minutes=15, seconds=serving_lag_seconds)))
        cutoff += timedelta(minutes=15)
    return tuple(windows)


def run_developer_forward_cycle(scope, configuration, *, ingress_database, compute_database, publication_database,
                                scheduler, discovery_source, facts_loader, detector_source, package_sources,
                                owner_id, valid_until, trade_source=None):
    from .forward_ingestion import PolicyChainMatrixScope

    if not isinstance(scope, PolicyChainMatrixScope):
        raise ValueError("live Developer orchestration requires explicit full-policy matrix scope")
    _require_role(ingress_database, RealtimeDatabaseRole.INGRESS)
    _require_role(compute_database, RealtimeDatabaseRole.COMPUTE)
    _require_role(publication_database, RealtimeDatabaseRole.PUBLICATION)
    sources, chains = collect_developer_forward_cycle(scope, configuration, scheduler=scheduler,
        discovery_source=discovery_source, facts_loader=facts_loader)
    if callable(detector_source) or callable(package_sources):
        with compute_database.transaction(dict_cursor=True, read_only=True, isolation_level="REPEATABLE READ") as cursor:
            detector_source = detector_source(cursor, scope, sources) if callable(detector_source) else detector_source
            package_sources = package_sources(cursor, scope, sources) if callable(package_sources) else package_sources
    if trade_source is not None:
        from dataclasses import fields

        matrices = {matrix.analysis.underlying.underlyer: matrix for matrix in (
            normalize_developer_matrix(admit_developer_matrix(scope, source), configuration) for source in sources)}
        package_sources = tuple(PackageInputFactsWithTrades(
            **{field.name: getattr(facts, field.name) for field in fields(PackageInputFacts)},
            trade_window=trade_source.collect(scope, matrices[facts.underlyer])) for facts in package_sources)
    if detector_source is not None and (detector_source.source if hasattr(detector_source, "source") else detector_source).scope_sha256 != scope.sha256:
        raise ValueError("runtime target-local context loader changed the accepted source scope")
    identity = ingest_developer_forward_inputs(ingress_database, scope, sources,
        detector_source=detector_source, chain_evidence=chains, package_sources=package_sources)
    enqueue_developer_forward_inputs(compute_database, identity)
    completed = materialize_developer_forward_once(compute_database, configuration, cohort_id=scope.cohort_id, owner_id=owner_id)
    if completed != identity:
        raise ValueError("Developer orchestration did not complete its exact accepted cycle")
    return publish_developer_forward_release(publication_database, identity, valid_until=valid_until)
