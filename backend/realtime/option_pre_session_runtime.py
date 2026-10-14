from __future__ import annotations

from dataclasses import dataclass
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from functools import partial
from math import isfinite
from pathlib import Path
from typing import Any, Callable, Mapping

import psycopg2
import requests
from psycopg2 import sql

from options.config import load_option_runtime_configuration
from options.calendar import OptionExchangeCalendar
from options.data.normalizer import DeveloperOptionNormalizer
from options.data.polygon_developer import PolygonDeveloperEngine
from options.data.polygon_http import RequestsPolygonHttpTransport
from options.domain import AssetType, OptionContractCatalogEntry, OptionContractReference, validate_standard_contract

from .contracts import CANARY_UNIVERSE, DEVELOPER_FORWARD_UNIVERSE, canonical_sha256
from .option_manifest_cohort_repository import DeveloperForwardCohortBinding
from .option_manifest_repository import persist_option_manifest_selection_cursor
from .database_access import RealtimeDatabaseRole, RealtimePoolBudget, RealtimeProcessDatabasePlan
from .option_discovery_scope import RealtimeOptionDiscoveryScope
from .option_pre_session_target import OptionPreSessionTargetPlan, TargetOptionPreSessionRunner
from .polygon_option_discovery import PolygonDeveloperOptionDiscoverySource
from .polygon_pre_session_source import (
    AdmittedOptionCatalogEntry,
    PolygonOptionPreSessionSource,
    PreSessionBarsResult,
    PreSessionCatalogResult,
    PreSessionSpotResult,
)
from .preflight import ProviderTier
from .preflight import (
    EXPECTED_SCHEMA_VERSIONS,
    PassiveHostEvidence,
    PassivePreflightConfiguration,
    PassivePreflightEvidence,
    PassiveSchemaEvidence,
    run_passive_preflight,
)
from .preflight_repository import persist_preflight_profile
from .migrations import load_realtime_migrations, schema_fingerprint
from .security import build_role_names, security_policy_sha256
from .worker_database import RealtimeProcessDatabase


class _InertIngestionRepository:
    pass


class MeteredPolygonTransport:
    def __init__(self, delegate, *, scheduler=None, available_after=None, deadline=None) -> None:
        self.delegate = delegate
        self.provider_requests = 0
        self.response_bytes = 0
        self.scheduler = scheduler
        self.available_after = available_after
        self.deadline = deadline

    def get(self, url, params):
        if self.scheduler is not None:
            return self.scheduler.request_with_retry(lambda: self.delegate.get(url, params),
                available_after=self.available_after, deadline=self.deadline)
        self.provider_requests += 1
        response = self.delegate.get(url, params)
        self.response_bytes += len(response.body)
        return response

    def snapshot(self) -> tuple[int, int]:
        if self.scheduler is not None:
            return self.scheduler.requests, self.scheduler.response_bytes
        return self.provider_requests, self.response_bytes


class StableEngineTransport:
    def __init__(self, engine: PolygonDeveloperEngine) -> None:
        self.engine = engine

    def get(self, url, params):
        return self.engine._request_response(url, params)


def refresh_blocked_developer_profile(
    dsn: str,
    role_name: str,
    checked_at: datetime,
    universe=CANARY_UNIVERSE,
    *,
    connection_factory=psycopg2.connect,
):
    schema_sha256 = schema_fingerprint(load_realtime_migrations())
    policy_sha256 = security_policy_sha256()
    configuration = PassivePreflightConfiguration(
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        account_alias="polygon-developer-shadow",
        universe=universe,
        expected_schema_sha256=schema_sha256,
        expected_security_policy_sha256=policy_sha256,
    )
    evidence = PassivePreflightEvidence(
        schema=PassiveSchemaEvidence(
            "stocks_realtime_v1",
            EXPECTED_SCHEMA_VERSIONS,
            schema_sha256,
            policy_sha256,
            True,
        ),
        host=PassiveHostEvidence(
            local_filesystem=False,
            journal_mode="UNVERIFIED",
            journal_synchronous=0,
            planned_journal_bytes=1,
            usable_disk_bytes=1,
            clock_verified=False,
        ),
    )
    profile = run_passive_preflight(
        configuration,
        evidence,
        checked_at=checked_at,
    )
    connection = connection_factory(dsn=dsn)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(role_name)))
        persist_preflight_profile(connection, profile)
    finally:
        connection.close()
    return profile


@dataclass(slots=True)
class OptionPreSessionRuntime:
    runner: TargetOptionPreSessionRunner
    target_plan: OptionPreSessionTargetPlan
    pools: tuple[Any, ...]
    resources: tuple[Any, ...] = ()

    def close(self) -> None:
        with ExitStack() as cleanup:
            for resource in self.resources:
                cleanup.callback(resource.close)
            for pool in self.pools:
                cleanup.callback(pool.closeall)


def _positive_bigint(identity: str) -> int:
    return (int(canonical_sha256(identity)[:16], 16) & ((1 << 63) - 1)) or 1


def _admitted_catalog_entry(
    reference: OptionContractReference, *, snapshot_expires_at: datetime | None = None
) -> AdmittedOptionCatalogEntry:
    validation = validate_standard_contract(reference)
    effective_from, effective_to = reference.valid_from, reference.valid_to
    observed_at = reference.revised_observed_at or reference.first_observed_at
    if snapshot_expires_at is not None:
        if observed_at.utcoffset() is None or snapshot_expires_at.utcoffset() is None:
            raise ValueError("pre-session catalog snapshot clocks must be aware")
        effective_from = reference.first_observed_at
        effective_to = min(effective_to, snapshot_expires_at) if effective_to else snapshot_expires_at
        if max(effective_from, observed_at) >= effective_to:
            raise ValueError("pre-session catalog snapshot is outside its source session")
    version_identity = (
        f"massive-option-version:{reference.contract_ticker}:{effective_from.isoformat()}:"
        f"{reference.payload_sha256}"
    )
    if snapshot_expires_at is not None:
        version_identity += f":session-snapshot-v1:{effective_to.isoformat()}"
    return AdmittedOptionCatalogEntry(
        _positive_bigint(version_identity),
        OptionContractCatalogEntry(
            contract_id=_positive_bigint(f"massive-option-contract:{reference.contract_ticker}"),
            contract_ticker=reference.contract_ticker,
            underlyer=reference.underlyer,
            asset_type=reference.asset_type,
            provider="massive",
            provider_version=reference.provider_version,
            contract_type=validation.contract_type,
            expiration_date=reference.expiration_date,
            strike=reference.strike,
            exercise_style=validation.exercise_style,
            shares_per_contract=reference.shares_per_contract,
            primary_exchange=reference.primary_exchange,
            eligibility_status=validation.eligibility_status,
            exclusion_reasons=validation.exclusion_reasons,
            valid_from=effective_from,
            valid_to=effective_to,
            first_observed_at=reference.first_observed_at,
            revised_observed_at=reference.revised_observed_at,
            payload_sha256=reference.payload_sha256,
        ),
    )


def _measured_call(metrics, call):
    before = metrics.snapshot()
    value = call()
    after = metrics.snapshot()
    return value, after[0] - before[0], after[1] - before[1]


def _load_scope(references_database, scheduled_cycle, universe=CANARY_UNIVERSE):
    with references_database.transaction(
        read_only=True, isolation_level="REPEATABLE READ"
    ) as cursor:
        cursor.execute(
            "SELECT profile_id::text, profile_sha256, provider_tier, universe_sha256 "
            "FROM rt.capability_profiles WHERE provider_tier='MASSIVE_DEVELOPER' "
            "AND universe_sha256=%s AND status IN ('FAIL', 'UNAVAILABLE') "
            "AND checked_at<=%s AND expires_at>%s "
            "ORDER BY checked_at DESC, profile_id DESC LIMIT 1",
            (universe.sha256, scheduled_cycle, scheduled_cycle),
        )
        row = cursor.fetchone()
    if row is None:
        raise ValueError("pre-session runtime requires a current blocked Developer profile")
    from uuid import UUID

    if row[2:] != (ProviderTier.MASSIVE_DEVELOPER.value, universe.sha256):
        raise ValueError("pre-session runtime profile identity drifted")
    return RealtimeOptionDiscoveryScope(
        ProviderTier.MASSIVE_DEVELOPER, UUID(row[0]), row[1], universe
    )


def build_option_pre_session_runtime(
    process_plan,
    *,
    scheduled_cycle: datetime,
    effective_at: datetime,
    environ: Mapping[str, str],
    backend_dir: Path,
    pool_factory: Callable,
    transport_factory: Callable = RequestsPolygonHttpTransport,
    profile_refresher: Callable = refresh_blocked_developer_profile,
    clock: Callable | None = None,
    risk_free_rate: float | None = None,
    dividend_yields: Mapping[str, float] | None = None,
    request_scheduler=None,
    source_deadline: datetime | None = None,
    prepared_target_plan: OptionPreSessionTargetPlan | None = None,
) -> OptionPreSessionRuntime:
    universe = process_plan.universe
    if universe.sha256 != process_plan.universe_sha256 or universe.option_underlyings != process_plan.option_underlyings:
        raise ValueError("pre-session process universe identity drifted")
    dsn = str(environ.get("REALTIME_RUNTIME_DSN") or "").strip()
    api_key = str(environ.get("POLYGON_API_KEY") or "").strip()
    if not dsn or not api_key:
        raise RuntimeError("pre-session runtime requires REALTIME_RUNTIME_DSN and POLYGON_API_KEY")
    runtime = load_option_runtime_configuration(dict(environ), backend_dir)
    if set(runtime.settings.underlyers) != set(universe.equity_tickers):
        raise ValueError("pre-session runtime configuration changed the reviewed universe")
    if runtime.developer_policy_sha256 != process_plan.discovery_policy_sha256:
        raise ValueError("pre-session runtime discovery policy drifted")
    if runtime.valuation_policy_sha256 != process_plan.valuation_policy_sha256:
        raise ValueError("pre-session runtime valuation policy drifted")
    forward_binding = process_plan.cohort_binding
    forward_mode = forward_binding is not None
    if forward_mode:
        if (forward_binding != DeveloperForwardCohortBinding.from_configuration(runtime, universe)
                or not callable(clock) or risk_free_rate is not None or dividend_yields is not None
                or runtime.settings.risk_free_rate_source != "us_treasury_par_yield_v1"
                or runtime.settings.dividend_input_source not in (None, "polygon_corporate_actions_v1")
                or request_scheduler is None or source_deadline is None):
            raise ValueError("nine-name preparation requires stored provider valuation, clocks and account ledger")
    active_clock = clock or (lambda: scheduled_cycle)
    collection_deadline = source_deadline
    calendar = OptionExchangeCalendar()
    session_date = calendar.session_for_market_time(scheduled_cycle)
    cycle = scheduled_cycle.astimezone(timezone.utc)
    source_deadline = calendar.session_close(session_date) + timedelta(
        seconds=runtime.valuation_policy.maximum_source_age_seconds
    )
    if cycle < calendar.session_open(session_date) or cycle > source_deadline:
        raise ValueError(
            "pre-session scheduled cycle cannot satisfy stable mark freshness"
        )
    roles = build_role_names("stocks_realtime")
    if prepared_target_plan is None:
        profile = profile_refresher(dsn, roles.control, scheduled_cycle, universe)
        if (profile.provider_tier is not ProviderTier.MASSIVE_DEVELOPER
                or profile.universe_sha256 != universe.sha256
                or profile.readiness.status.value != "BLOCKED"):
            raise ValueError("pre-session profile refresh did not remain blocked Developer")
    elif (not isinstance(prepared_target_plan, OptionPreSessionTargetPlan)
            or prepared_target_plan.scope.universe != universe
            or prepared_target_plan.scope.provider_tier is not ProviderTier.MASSIVE_DEVELOPER
            or prepared_target_plan.policy != process_plan.manifest_policy
            or prepared_target_plan.scheduled_cycle != scheduled_cycle
            or prepared_target_plan.effective_at != effective_at
            or prepared_target_plan.maximum_provider_requests != process_plan.maximum_provider_requests
            or prepared_target_plan.maximum_response_bytes != process_plan.maximum_response_bytes):
        raise ValueError("prepared pre-session target plan identity drifted")
    pools = []
    http_session = None
    try:
        references_pool = pool_factory(1, 1, dsn=dsn)
        pools.append(references_pool)
        control_pool = pool_factory(1, 1, dsn=dsn)
        pools.append(control_pool)
        if control_pool is references_pool:
            raise ValueError("pre-session stage pools must be distinct")
        references_database = RealtimeProcessDatabase.wire(
            RealtimeProcessDatabasePlan(
                "worker:option_pre_session:references",
                RealtimeDatabaseRole.REFERENCES,
                RealtimePoolBudget(1, 1),
            ),
            principal_pool=references_pool,
            role_name=roles.references,
        )
        control_database = RealtimeProcessDatabase.wire(
            RealtimeProcessDatabasePlan(
                "worker:option_pre_session:control",
                RealtimeDatabaseRole.CONTROL,
                RealtimePoolBudget(1, 1),
            ),
            principal_pool=control_pool,
            role_name=roles.control,
        )
        scope = (prepared_target_plan.scope if prepared_target_plan is not None
            else _load_scope(references_database, scheduled_cycle, universe))
        transport_options = dict(timeout_seconds=runtime.policy.provider_requests.request_timeout_seconds)
        if forward_mode and transport_factory is RequestsPolygonHttpTransport:
            http_session = requests.Session()
            transport_options["session_factory"] = lambda: http_session
        raw_transport = transport_factory(api_key, **transport_options)
        metrics = MeteredPolygonTransport(raw_transport, scheduler=request_scheduler,
            available_after=scheduled_cycle, deadline=collection_deadline)
        engine = PolygonDeveloperEngine(
            runtime,
            _InertIngestionRepository(),
            transport=metrics,
            clock=active_clock,
        )
        discovery_source = PolygonDeveloperOptionDiscoverySource(
            StableEngineTransport(engine), runtime, scope, clock=active_clock
        )

        def spot_source(underlyer, as_of):
            spot, requests, response_bytes = _measured_call(
                metrics, lambda: engine.get_spot_price(underlyer, as_of)
            )
            return PreSessionSpotResult(spot.price, requests, response_bytes)

        def catalog_source(underlyer, as_of, expiration_through, strike_min, strike_max):
            asset_type = (
                AssetType.ETF
                if underlyer in runtime.settings.fixed_etf_underlyers
                else AssetType.STOCK
            )
            references, requests, response_bytes = _measured_call(
                metrics,
                lambda: engine.list_option_references(
                    underlyer,
                    as_of,
                    expiration_through,
                    asset_type,
                    strike_min,
                    strike_max,
                ),
            )
            entries = [
                _admitted_catalog_entry(reference, snapshot_expires_at=(
                    calendar.session_close(session_date) if forward_mode else None
                ))
                for reference in references
            ]
            return PreSessionCatalogResult(tuple(entries), requests, response_bytes)

        raw_spot_basis = runtime.valuation_policy.underlying_price_basis == "RAW_FINALIZED_MINUTE_CLOSE_V1"

        def bars_source(underlyer, start, end):
            # The read uses the basis the valuation policy pins, with bar-end market time.
            (bars, coverage), requests, response_bytes = _measured_call(
                metrics,
                lambda: engine.get_underlying_minute_bars_with_coverage(
                    underlyer, start, end, raw_finalized=raw_spot_basis
                ),
            )
            return PreSessionBarsResult(bars, requests, response_bytes, coverage)

        def valuation_source(underlyer, security_id, spot, valuation_date, expirations):
            from .forward_bindings import (
                _load_defaulted_expiration_valuation_inputs,
                _load_expiration_valuation_candidates,
            )
            from .forward_storage import _database_clock

            with references_database.transaction(dict_cursor=True, read_only=True,
                    isolation_level="REPEATABLE READ") as cursor:
                selected_at = _database_clock(cursor)
                if selected_at >= collection_deadline:
                    raise ValueError("pre-session stored valuation selection missed its deadline")
                if runtime.settings.dividend_input_source == "polygon_corporate_actions_v1":
                    return _load_expiration_valuation_candidates(cursor, security_id=security_id,
                        symbol=underlyer, spot=spot, valuation_date=valuation_date,
                        expirations=expirations, selected_at=selected_at,
                        maximum_age_seconds=runtime.settings.dividend_input_max_age_seconds)
                return _load_defaulted_expiration_valuation_inputs(cursor,
                    valuation_date=valuation_date, expirations=expirations,
                    selected_at=selected_at,
                    maximum_age_seconds=runtime.settings.dividend_input_max_age_seconds,
                    default_dividend_yield=runtime.settings.default_dividend_yield)

        source = PolygonOptionPreSessionSource(
            discovery_source,
            DeveloperOptionNormalizer(runtime.policy, runtime.valuation_policy),
            spot_source=spot_source,
            catalog_source=catalog_source,
            bars_source=bars_source,
            risk_free_rate=(None if forward_mode else
                float(runtime.settings.risk_free_rate) if risk_free_rate is None else risk_free_rate),
            dividend_yields=(None if forward_mode else
                dict(dividend_yields) if dividend_yields is not None else {
                ticker: float(runtime.settings.default_dividend_yield)
                for ticker in scope.universe.option_underlyings
            }), valuation_source=valuation_source if forward_mode else None,
            calendar=calendar,
            clock=active_clock,
            discovery_metrics=metrics.snapshot,
        )
        source.request_transport = metrics
        target_plan = prepared_target_plan or OptionPreSessionTargetPlan(
            scope=scope,
            policy=process_plan.manifest_policy,
            session_date=session_date,
            scheduled_cycle=scheduled_cycle,
            effective_at=effective_at,
            maximum_provider_requests=process_plan.maximum_provider_requests,
            maximum_response_bytes=process_plan.maximum_response_bytes,
        )
        return OptionPreSessionRuntime(
            TargetOptionPreSessionRunner(references_database, control_database, source,
                selection_repository=partial(persist_option_manifest_selection_cursor, cohort_binding=forward_binding)),
            target_plan,
            tuple(pools),
            (http_session,) if http_session is not None else (),
        )
    except Exception:
        with ExitStack() as cleanup:
            if http_session is not None:
                cleanup.callback(http_session.close)
            for active_pool in {id(pool): pool for pool in pools}.values():
                cleanup.callback(active_pool.closeall)
        raise
