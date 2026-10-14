from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

from .database_access import RealtimeDatabaseRole
from .option_discovery_repository import persist_realtime_option_discovery_cursor
from .option_discovery_scope import RealtimeOptionDiscoveryScope
from .option_manifest_repository import persist_option_manifest_selection_cursor
from .option_pre_session_producer import (
    CompletedOptionDiscoveryPackage,
    OptionPreSessionProducer,
    OptionPreSessionProducerResult,
    OptionPreSessionRequest,
    OptionPreSessionSource,
)
from .option_session_manifest import BoundedOptionManifestPolicy
from .worker_database import RealtimeProcessDatabase


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class OptionPreSessionTargetPlan:
    scope: RealtimeOptionDiscoveryScope
    policy: BoundedOptionManifestPolicy
    session_date: date
    scheduled_cycle: datetime
    effective_at: datetime
    maximum_provider_requests: int
    maximum_response_bytes: int

    def __post_init__(self) -> None:
        scheduled = _utc(self.scheduled_cycle, "scheduled_cycle")
        effective = _utc(self.effective_at, "effective_at")
        if scheduled.date() != self.session_date or effective.date() != self.session_date:
            raise ValueError("target pre-session clocks must belong to the manifest session")
        if effective > scheduled:
            raise ValueError("target pre-session effective time follows collection")
        if (
            type(self.maximum_provider_requests) is not int
            or self.maximum_provider_requests <= 0
            or type(self.maximum_response_bytes) is not int
            or self.maximum_response_bytes <= 0
        ):
            raise ValueError("target pre-session source limits must be positive integers")
        object.__setattr__(self, "scheduled_cycle", scheduled)
        object.__setattr__(self, "effective_at", effective)


def load_option_pre_session_request_cursor(
    cursor,
    plan: OptionPreSessionTargetPlan,
) -> OptionPreSessionRequest:
    scope = plan.scope
    cursor.execute(
        "SELECT profile_sha256, provider_tier, universe_sha256 "
        "FROM rt.capability_profiles WHERE profile_id=%s",
        (str(scope.profile_id),),
    )
    expected_profile = (
        scope.profile_sha256,
        scope.provider_tier.value,
        scope.universe.sha256,
    )
    if cursor.fetchone() != expected_profile:
        raise ValueError("target pre-session capability profile identity mismatch")
    cursor.execute(
        "SELECT DISTINCT ON (ticker) ticker, security_id::text FROM ref.security_revisions "
        "WHERE ticker=ANY(%s) AND effective_from <= %s "
        "AND (effective_to IS NULL OR effective_to > %s) "
        "ORDER BY ticker,observed_at DESC,revision_id DESC",
        (
            list(scope.universe.option_underlyings),
            plan.scheduled_cycle,
            plan.scheduled_cycle,
        ),
    )
    by_ticker = {str(ticker): security_id for ticker, security_id in cursor.fetchall()}
    if (set(by_ticker) != set(scope.universe.option_underlyings)
            or len(set(by_ticker.values())) != len(by_ticker)):
        raise ValueError("target pre-session security identities are incomplete or duplicated")
    identities = tuple((ticker, by_ticker[ticker]) for ticker in scope.universe.option_underlyings)
    from uuid import UUID

    return OptionPreSessionRequest(
        scope=scope,
        policy=plan.policy,
        session_date=plan.session_date,
        scheduled_cycle=plan.scheduled_cycle,
        effective_at=plan.effective_at,
        underlying_security_ids=tuple(
            (ticker, UUID(security_id)) for ticker, security_id in identities
        ),
        maximum_provider_requests=plan.maximum_provider_requests,
        maximum_response_bytes=plan.maximum_response_bytes,
    )


def persist_option_pre_session_package_cursor(
    cursor,
    package: CompletedOptionDiscoveryPackage,
) -> None:
    evidence = package.evidence
    persist_realtime_option_discovery_cursor(cursor, evidence)
    for candidate in package.candidates:
        catalog = candidate.catalog
        cursor.execute(
            "INSERT INTO ref.option_contracts ("
            "contract_id, underlying_security_id, provider, contract_ticker, "
            "first_observed_at, source_origin) VALUES ("
            "%s, %s, %s, %s, %s, 'PRE_SESSION_DISCOVERY') "
            "ON CONFLICT (contract_id) DO NOTHING",
            (
                catalog.contract_id,
                str(evidence.underlying_security_id),
                catalog.provider,
                catalog.contract_ticker,
                catalog.first_observed_at,
            ),
        )
        cursor.execute(
            "SELECT underlying_security_id::text, provider, contract_ticker "
            "FROM ref.option_contracts WHERE contract_id=%s",
            (catalog.contract_id,),
        )
        if cursor.fetchone() != (
            str(evidence.underlying_security_id),
            catalog.provider,
            catalog.contract_ticker,
        ):
            raise ValueError("pre-session option contract identity conflict")
        cursor.execute(
            "INSERT INTO ref.option_contract_versions ("
            "version_id, contract_id, call_put, expiration, strike, exercise_style, "
            "multiplier, deliverables, eligibility, effective_from, effective_to, "
            "observed_at, payload_sha256) VALUES ("
            "%s, %s, %s, %s, %s, %s, %s, '[]'::jsonb, %s, %s, %s, %s, %s) "
            "ON CONFLICT (version_id) DO NOTHING",
            (
                candidate.catalog_version_id,
                catalog.contract_id,
                "C" if catalog.contract_type.value == "CALL" else "P",
                catalog.expiration_date,
                catalog.strike,
                catalog.exercise_style.value,
                catalog.shares_per_contract,
                catalog.eligibility_status.value,
                catalog.valid_from,
                catalog.valid_to,
                catalog.revised_observed_at or catalog.first_observed_at,
                catalog.payload_sha256,
            ),
        )
        cursor.execute(
            "SELECT contract_id, payload_sha256 FROM ref.option_contract_versions "
            "WHERE version_id=%s",
            (candidate.catalog_version_id,),
        )
        if cursor.fetchone() != (catalog.contract_id, catalog.payload_sha256):
            raise ValueError("pre-session option catalog version conflict")
        cursor.execute(
            "INSERT INTO rt.instruments (asset_class, contract_id) "
            "VALUES ('OPTION', %s) ON CONFLICT DO NOTHING",
            (catalog.contract_id,),
        )
        cursor.execute(
            "SELECT instrument_id FROM rt.instruments WHERE contract_id=%s",
            (catalog.contract_id,),
        )
        instrument = cursor.fetchone()
        if instrument is None or int(instrument[0]) <= 0:
            raise ValueError("pre-session option instrument persistence did not reconcile")


class TargetOptionPreSessionRunner:
    def __init__(
        self,
        references_database: RealtimeProcessDatabase,
        control_database: RealtimeProcessDatabase,
        source: OptionPreSessionSource,
        *,
        package_repository=persist_option_pre_session_package_cursor,
        selection_repository=persist_option_manifest_selection_cursor,
        loader=load_option_pre_session_request_cursor,
    ) -> None:
        if not isinstance(references_database, RealtimeProcessDatabase):
            raise TypeError("pre-session references database is invalid")
        if references_database.process_plan.role is not RealtimeDatabaseRole.REFERENCES:
            raise ValueError("pre-session discovery requires the references database role")
        if not isinstance(control_database, RealtimeProcessDatabase):
            raise TypeError("pre-session control database is invalid")
        if control_database.process_plan.role is not RealtimeDatabaseRole.CONTROL:
            raise ValueError("pre-session selection requires the control database role")
        self.references_database = references_database
        self.control_database = control_database
        self.source = source
        self.package_repository = package_repository
        self.selection_repository = selection_repository
        self.loader = loader

    def run_once(
        self,
        plan: OptionPreSessionTargetPlan,
    ) -> OptionPreSessionProducerResult:
        if not isinstance(plan, OptionPreSessionTargetPlan):
            raise TypeError("target pre-session plan is invalid")
        with self.references_database.transaction(
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as cursor:
            request = self.loader(cursor, plan)

        def persist_packages(packages):
            with self.references_database.transaction() as cursor:
                for package in packages:
                    self.package_repository(cursor, package)

        def persist_selection(selection_plan, effective_at):
            with self.control_database.transaction() as cursor:
                return self.selection_repository(
                    cursor,
                    selection_plan,
                    effective_at=effective_at,
                )

        producer = OptionPreSessionProducer(
            self.source,
            persist_packages=persist_packages,
            persist_selection=persist_selection,
        )
        return producer.run_once(request)