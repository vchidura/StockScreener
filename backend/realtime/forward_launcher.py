from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from math import isfinite
import threading
from uuid import UUID
from zoneinfo import ZoneInfo

from psycopg2 import ProgrammingError
from psycopg2.extensions import parse_dsn

from .database_access import (
    RealtimeDatabaseRole, RealtimePoolBudget, RealtimeProcessDatabasePlan, planned_process_connection_ceiling,
)
from .contracts import DEVELOPER_FORWARD_UNIVERSE
from .forward_bindings import TargetDeveloperFactsLoader, TargetDeveloperSourceBindings
from .forward_ingestion import PolicyChainMatrixScope
from .forward_runtime import (
    DeveloperForwardBudget, DeveloperForwardScheduler, build_paced_developer_discovery,
    developer_forward_session_windows, run_developer_forward_cycle,
)
from .preflight import ProviderTier
from .option_manifest_cohort_repository import DeveloperForwardCohortBinding, developer_forward_cohort_id
from .option_pre_session_producer import OptionPreSessionProducerResult
from .option_session_manifest import ContractSelectionStatus, ManifestSelectionStatus
from .security import build_role_names
from .worker_database import RealtimeProcessDatabase


FORWARD_ROLES = (RealtimeDatabaseRole.INGRESS, RealtimeDatabaseRole.COMPUTE, RealtimeDatabaseRole.PUBLICATION)


@dataclass(frozen=True, slots=True)
class DeveloperForwardEnrollmentReceipt:
    process_plan_sha256: str
    result: OptionPreSessionProducerResult
    cohort_binding: DeveloperForwardCohortBinding
    effective_at: datetime

    @property
    def cohort_id(self):
        if self.result.plan.status is not ManifestSelectionStatus.READY:
            return None
        return developer_forward_cohort_id(self.result.plan, self.cohort_binding, effective_at=self.effective_at)

    def matrix_scope(self, window, *, reference_sha256, action_sha256):
        if self.cohort_id is None or not self.result.catalog:
            raise ValueError("forward matrix scope requires a complete ready enrollment receipt")
        admitted_ids = self.result.plan.policy_admitted_contract_ids
        catalog = tuple(row for row in self.result.catalog if row.contract_id in admitted_ids)
        return PolicyChainMatrixScope(self.cohort_id, self.result.plan, catalog,
            self.cohort_binding.configuration_sha256, reference_sha256, action_sha256,
            window.cutoff, window.available_after, window.deadline)


class DeveloperForwardEnrollmentLauncher:
    def __init__(self, process_plan, configuration, *, environ, backend_dir, session_date, deadline,
                 pool_factory, clock, wait,
                 server_max_connections, reserved_connections, runtime_factory=None, transport_factory=None):
        if (process_plan.universe != DEVELOPER_FORWARD_UNIVERSE
                or process_plan.cohort_binding != DeveloperForwardCohortBinding.from_configuration(configuration)
                or not isinstance(session_date, date) or not isinstance(deadline, datetime)
                or deadline.utcoffset() is None or not callable(pool_factory)
                or not callable(clock) or not callable(wait)
                or configuration.settings.risk_free_rate_source != "us_treasury_par_yield_v1"
                or configuration.settings.dividend_input_source not in (None, "polygon_corporate_actions_v1")):
            raise ValueError("forward enrollment requires exact reviewed scope, configuration, clocks and valuation")
        settings = dict(environ)
        try:
            parameters = parse_dsn(str(settings.get("REALTIME_RUNTIME_DSN") or ""))
        except ProgrammingError:
            raise ValueError("forward enrollment runtime DSN is invalid") from None
        if (parameters.get("dbname") != "stocks_realtime_v1" or parameters.get("user") != "stocks_realtime_runtime"
                or any(name in parameters for name in ("service", "servicefile", "options"))
                or not str(settings.get("POLYGON_API_KEY") or "").strip()
                or settings.get("OPTION_DATA_ENGINE") != "polygon_developer"
                or str(settings.get("OPTION_START_READ_ONLY") or "").lower() != "true"
                or str(settings.get("OPTION_TRADE_INGESTION_ENABLED") or "false").lower() != "false"):
            raise ValueError("forward enrollment requires the isolated Developer read-only runtime target")
        budget = DeveloperForwardBudget(maximum_requests=process_plan.maximum_provider_requests,
            maximum_response_bytes=process_plan.maximum_response_bytes,
            maximum_attempts=min(3, configuration.policy.provider_requests.maximum_rate_limit_retries + 1))
        plans = tuple(RealtimeProcessDatabasePlan(f"worker:developer_forward_enrollment:{role.value.lower()}",
            role, RealtimePoolBudget(1, 1)) for role in (RealtimeDatabaseRole.REFERENCES, RealtimeDatabaseRole.CONTROL))
        planned_process_connection_ceiling(plans, server_max_connections=server_max_connections,
            reserved_connections=reserved_connections)
        self.process_plan, self.configuration = process_plan, configuration
        self.environ, self.backend_dir, self.session_date, self.deadline = settings, backend_dir, session_date, deadline
        self.pool_factory, self.clock, self.wait, self.budget = pool_factory, clock, wait, budget
        self.runtime_factory, self.transport_factory = runtime_factory, transport_factory
        self.last_provider_requests, self.last_response_bytes = 0, 0
        self._used, self._lock = False, threading.Lock()

    def run_once(self):
        if not self._lock.acquire(blocking=False):
            raise ValueError("forward enrollment already owns its single preparation pass")
        try:
            if self._used:
                raise ValueError("forward enrollment cannot reactivate its preparation pass")
            started_at = self.clock()
            if (started_at.utcoffset() is None or started_at >= self.deadline
                    or started_at.astimezone(ZoneInfo("America/New_York")).date() != self.session_date):
                raise ValueError("forward enrollment is outside its reviewed session/deadline window")
            self._used = True
            from .option_pre_session_runtime import build_option_pre_session_runtime

            scheduler = DeveloperForwardScheduler(self.budget, clock=self.clock, wait=self.wait)
            bindings = dict(scheduled_cycle=started_at, effective_at=started_at, environ=self.environ,
                backend_dir=self.backend_dir, pool_factory=self.pool_factory, clock=self.clock,
                request_scheduler=scheduler, source_deadline=self.deadline)
            if self.transport_factory is not None:
                bindings["transport_factory"] = self.transport_factory
            runtime = None
            try:
                runtime = (self.runtime_factory or build_option_pre_session_runtime)(self.process_plan, **bindings)
                result = runtime.runner.run_once(runtime.target_plan)
                if (not isinstance(result, OptionPreSessionProducerResult)
                        or result.plan.scope.universe != DEVELOPER_FORWARD_UNIVERSE
                    or result.plan.scope != runtime.target_plan.scope
                    or result.plan.session_date != self.session_date
                        or result.plan.policy.sha256 != self.process_plan.manifest_policy.sha256
                        or (result.provider_requests, result.response_bytes) != (scheduler.requests, scheduler.response_bytes)
                        or (result.plan.status is ManifestSelectionStatus.READY and not result.catalog)
                        or self.clock() > self.deadline):
                    raise ValueError("forward enrollment receipt changed scope, policy, ledger or deadline")
                return DeveloperForwardEnrollmentReceipt(self.process_plan.sha256, result,
                    self.process_plan.cohort_binding, started_at)
            finally:
                self.last_provider_requests = scheduler.requests
                self.last_response_bytes = scheduler.response_bytes
                if runtime is not None:
                    runtime.close()
        finally:
            self._lock.release()


@dataclass(frozen=True, slots=True)
class DeveloperForwardPoolSpec:
    role: RealtimeDatabaseRole
    role_name: str
    dsn: str = field(repr=False)
    budget: RealtimePoolBudget


class DeveloperForwardLauncher:
    def __init__(self, scope, configuration, *, pool_specs, role_prefix, pool_factory, http, budget,
                 clock, wait, planned_entry_at, entry_deadline, exit_deadline,
                 owner_id, valid_until, server_max_connections, reserved_connections, trade_policy=None):
        if (not isinstance(scope, PolicyChainMatrixScope)
                or scope.selection.scope.provider_tier is not ProviderTier.MASSIVE_DEVELOPER
                or configuration.configuration_sha256 != scope.configuration_sha256
                or not isinstance(budget, DeveloperForwardBudget)
                or not isinstance(owner_id, UUID)
                or not callable(pool_factory) or not callable(clock) or not callable(wait)
                or not callable(getattr(http, "get", None))):
            raise ValueError("Developer launcher requires explicit full-policy scope/configuration/dependencies")
        specs = tuple(pool_specs)
        if (len(specs) != len(FORWARD_ROLES) or any(not isinstance(row, DeveloperForwardPoolSpec) for row in specs)
                or {row.role for row in specs} != set(FORWARD_ROLES)):
            raise ValueError("Developer launcher requires exactly three isolated role pool specifications")
        names = build_role_names(role_prefix)
        plans = {}
        for spec in specs:
            parameters = parse_dsn(spec.dsn)
            if (parameters.get("dbname") != "stocks_realtime_v1" or parameters.get("user") != names.runtime
                    or any(name in parameters for name in ("service", "servicefile", "options"))
                    or spec.role_name != getattr(names, spec.role.value.lower())):
                raise ValueError("Developer pool target/principal/capability binding drift")
            plans[spec.role] = RealtimeProcessDatabasePlan(f"worker:developer_forward:{spec.role.value.lower()}",
                spec.role, spec.budget)
        planned_process_connection_ceiling(plans.values(), server_max_connections=server_max_connections,
            reserved_connections=reserved_connections)
        if (configuration.settings.risk_free_rate_source != "us_treasury_par_yield_v1"
            or configuration.settings.dividend_input_source not in (None, "polygon_corporate_actions_v1")
            or any(value.utcoffset() is None for value in (planned_entry_at, entry_deadline, exit_deadline, valid_until))
                or not scope.deadline < planned_entry_at <= entry_deadline < exit_deadline
                or not scope.deadline < valid_until):
            raise ValueError("Developer launcher requires reviewed exact valuation/holding/serving inputs")
        self.scope, self.configuration = scope, configuration
        self.pool_specs = tuple(sorted(specs, key=lambda row: FORWARD_ROLES.index(row.role)))
        self.process_plans = plans
        self.pool_factory, self.http, self.budget = pool_factory, http, budget
        self.clock, self.wait = clock, wait
        self.planned_entry_at, self.entry_deadline, self.exit_deadline = planned_entry_at, entry_deadline, exit_deadline
        self.owner_id, self.valid_until = owner_id, valid_until
        self.trade_policy = trade_policy
        self._lock, self._used = threading.Lock(), False

    def run_once(self):
        if not self._lock.acquire(blocking=False):
            raise ValueError("Developer launcher already owns its single cycle")
        pools = []
        cleanup = ExitStack()
        try:
            if self._used:
                raise ValueError("Developer launcher cannot reactivate its single cycle")
            now = self.clock()
            if now.utcoffset() is None or not self.scope.available_after <= now < self.scope.deadline:
                raise ValueError("Developer launcher is outside its release/deadline window")
            self._used = True
            databases = {}
            for spec in self.pool_specs:
                pool = self.pool_factory(spec.budget.minimum_connections, spec.budget.maximum_connections, dsn=spec.dsn)
                if any(pool is existing for existing in pools):
                    raise ValueError("Developer stage pools must be distinct, never shared")
                pools.append(pool)
                cleanup.callback(pool.closeall)
                databases[spec.role] = RealtimeProcessDatabase.wire(self.process_plans[spec.role],
                    principal_pool=pool, role_name=spec.role_name)
            compute = databases[RealtimeDatabaseRole.COMPUTE]
            facts = TargetDeveloperFactsLoader(compute, self.scope, self.configuration)
            sources = TargetDeveloperSourceBindings(facts, self.configuration, planned_entry_at=self.planned_entry_at,
                entry_deadline=self.entry_deadline, exit_deadline=self.exit_deadline)
            scheduler = DeveloperForwardScheduler(self.budget, clock=self.clock, wait=self.wait)
            discovery = build_paced_developer_discovery(self.http, scheduler, self.scope, self.configuration)
            from .polygon_option_discovery import PolygonDeveloperTradeWindowSource
            from .forward_runtime import PacedDeveloperHttp

            trades = (PolygonDeveloperTradeWindowSource(PacedDeveloperHttp(self.http, scheduler, self.scope),
                self.trade_policy, clock=self.clock) if self.trade_policy is not None and self.trade_policy.enabled else None)
            return run_developer_forward_cycle(self.scope, self.configuration,
                ingress_database=databases[RealtimeDatabaseRole.INGRESS], compute_database=compute,
                publication_database=databases[RealtimeDatabaseRole.PUBLICATION], scheduler=scheduler,
                discovery_source=discovery, facts_loader=facts, detector_source=sources.detector_source,
                package_sources=sources.package_sources, owner_id=self.owner_id, valid_until=self.valid_until, trade_source=trades)
        finally:
            try:
                cleanup.close()
            finally:
                self._lock.release()


def build_developer_forward_launcher(scope, configuration, **bindings):
    return DeveloperForwardLauncher(scope, configuration, **bindings)


def run_developer_forward_enrolled_cycle(receipt, configuration, *, verification_database, window,
                                        reference_sha256, action_sha256, **launcher_bindings):
    if (not isinstance(receipt, DeveloperForwardEnrollmentReceipt)
            or receipt.result.plan.scope.provider_tier is not ProviderTier.MASSIVE_DEVELOPER
            or receipt.result.plan.scope.universe != DEVELOPER_FORWARD_UNIVERSE
            or receipt.cohort_binding != DeveloperForwardCohortBinding.from_configuration(configuration)):
        raise ValueError("forward enrolled cycle requires the exact reviewed enrollment/configuration")
    if (not isinstance(verification_database, RealtimeProcessDatabase)
            or verification_database.process_plan.role is not RealtimeDatabaseRole.COMPUTE):
        raise ValueError("forward enrolled cycle requires a verified read-only compute handle")
    scope = receipt.matrix_scope(window, reference_sha256=reference_sha256, action_sha256=action_sha256)
    completion_seconds = (window.deadline - window.available_after).total_seconds()
    serving_seconds = (window.serving_expires_at - window.available_after - timedelta(minutes=15)).total_seconds()
    if (not completion_seconds.is_integer() or not serving_seconds.is_integer()
            or window not in developer_forward_session_windows(receipt.result.plan.session_date,
                completion_budget_seconds=int(completion_seconds), serving_lag_seconds=int(serving_seconds))
            or launcher_bindings.get("valid_until", window.serving_expires_at) != window.serving_expires_at):
        raise ValueError("forward enrolled cycle changed the reviewed exchange/release/expiry window")
    launcher_bindings["valid_until"] = window.serving_expires_at
    from .option_manifest_repository import option_subscription_manifest_id

    with verification_database.transaction(read_only=True, isolation_level="REPEATABLE READ") as cursor:
        cursor.execute("SELECT cohort.manifest_id::text,cohort.configuration_sha256,cohort.model_bundle_sha256,"
            "cohort.valuation_sha256,cohort.mode,tier.profile_id::text,tier.provider_tier,tier.stocks_tier,"
            "tier.options_tier,tier.ready_state,tier.source_manifest_sha256,tier.valuation_policy_sha256,"
            "cohort.effective_at,tier.effective_at FROM rt.cohorts AS cohort "
            "JOIN rt.provider_tier_cohorts AS tier USING(cohort_id) WHERE cohort.cohort_id=%s", (str(scope.cohort_id),))
        row = cursor.fetchone()
    binding = receipt.cohort_binding
    expected = (str(option_subscription_manifest_id(receipt.result.plan)), binding.configuration_sha256,
        binding.model_bundle_sha256, binding.valuation_sha256, "SHADOW", str(receipt.result.plan.scope.profile_id),
        "MASSIVE_DEVELOPER", "MASSIVE_DEVELOPER", "MASSIVE_DEVELOPER", "DEVELOPER_READY",
        receipt.result.plan.manifest.sha256, configuration.valuation_policy_sha256)
    if row is None or len(row) != 14 or tuple(row[:12]) != expected:
        raise ValueError("target forward cohort/native hashes/provider-tier binding are absent or drifted")
    if (any(not isinstance(value, datetime) or value.utcoffset() is None for value in row[12:])
            or row[12] != receipt.effective_at or not receipt.effective_at <= row[13] <= scope.cutoff):
        raise ValueError("target forward enrollment effective clocks are not eligible for the cutoff")
    launcher = build_developer_forward_launcher(scope, configuration, **launcher_bindings)
    return launcher.run_once()