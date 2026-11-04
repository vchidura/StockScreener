from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

from options.calendar import OptionExchangeCalendar
from psycopg2.extras import Json, execute_values

from .advanced_realtime_desired_state import (
    DEFAULT_OPTION_UNDERLYINGS,
    DesiredMemberIntent,
    DesiredMemberKind,
    DesiredMemberPreparation,
    DesiredSessionSnapshot,
    DesiredStateChange,
    DesiredStateRevision,
    MemberPreparationStatus,
    build_desired_state_revision,
    desired_member_intent_id,
    effective_next_session,
)
from .contracts import (
    AssetClass,
    EQUITY_VALIDATION_UNIVERSE,
    MarketChannel,
    SubscriptionManifest,
    SubscriptionMember,
    SubscriptionPurpose,
    UniverseManifest,
    canonical_sha256,
)
from .equity_portal_history import (
    REALTIME_EQUITY_PORTAL_HISTORY_POLICY,
    build_realtime_equity_portal_history_plan,
)
from .advanced_ingress_runtime_profile import (
    AdvancedIngressRuntimeCatalog,
    load_advanced_ingress_runtime_catalog,
)
from .advanced_equity_control_plane import (
    ensure_equity_instrument_cursor,
)
from .option_manifest_cohort_repository import (
    equity_universe_manifest_id,
    option_universe_manifest_id,
    persist_equity_universe_manifest_cursor,
    persist_option_universe_manifest_cursor,
    persist_shadow_cohort_cursor,
)
from .option_slot_set import (
    SlotSetKind,
    SlotSetMember,
    SlotSetPurpose,
    SlotSetVersion,
    option_universe_manifest_sha256,
)
from .option_slot_set_repository import persist_and_activate_slot_set_cursor
from .preflight import ProviderTier, TierState
from .tier_cohort import (
    TierCohortBinding,
    persist_tier_cohort_binding_cursor,
    product_capabilities,
    product_disabled_detectors,
)

DESIRED_STATE_SCOPE = "ADVANCED_REALTIME"
_DESIRED_MEMBER_BATCH_SIZE = 1_000
CAPABILITY_REFRESH_OWNER = "REFERENCE"
DEFAULT_ADVANCED_INGRESS_RUNTIME_POLICY = (
    Path(__file__).resolve().parent
    / "policies"
    / "advanced_equity_ingress_v1.json"
)
EQUITY_PREPARATION_STAGES = (
    ("REFERENCE", 10),
    ("ADJUSTED_DAILY", 20),
    ("NATIVE_INTRADAY", 30),
    ("DERIVED_INTERVALS", 40),
    ("TECHNICAL_WARMUP", 50),
)


@dataclass(frozen=True, slots=True)
class DesiredStateReconciliation:
    state: str
    revision: DesiredStateRevision
    preparations: tuple[DesiredMemberPreparation, ...]
    snapshot: DesiredSessionSnapshot | None
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _SourceControlPlane:
    manifest_id: UUID
    manifest_sha256: str
    stock_count: int
    profile_id: UUID
    profile_sha256: str
    provider_tier: ProviderTier
    universe_sha256: str
    capability_config_sha256: str
    configuration_sha256: str
    model_bundle_sha256: str
    valuation_sha256: str


@dataclass(frozen=True, slots=True)
class _StoredOptionMember:
    instrument_id: int
    security_id: UUID
    contract_ticker: str
    call_put: str
    channel: MarketChannel
    purpose: SlotSetPurpose
    priority: int
    catalog_version_id: int


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _runtime_configuration_sha256(
    source: _SourceControlPlane,
    *,
    universe_target: int,
    runtime_catalog: AdvancedIngressRuntimeCatalog | None,
) -> str:
    if universe_target == source.stock_count:
        return source.configuration_sha256
    catalog = runtime_catalog or load_advanced_ingress_runtime_catalog(
        DEFAULT_ADVANCED_INGRESS_RUNTIME_POLICY
    )
    return catalog.profile_for_target(
        universe_target
    ).configuration_sha256(catalog.scale_policy.sha256)


def _load_source_control_plane_cursor(
    cursor,
    cohort_id: UUID,
) -> _SourceControlPlane:
    cursor.execute(
        "SELECT manifest.manifest_id::text,manifest.manifest_sha256,"
        "manifest.stock_count,manifest.profile_id::text,"
        "profile.profile_sha256,profile.provider_tier,"
        "profile.universe_sha256,profile.config_sha256,"
        "cohort.configuration_sha256,cohort.model_bundle_sha256,"
        "cohort.valuation_sha256 "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest "
        "ON manifest.manifest_id=cohort.manifest_id "
        "JOIN rt.capability_profiles AS profile "
        "ON profile.profile_id=manifest.profile_id "
        "WHERE cohort.cohort_id=%s",
        (str(cohort_id),),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("desired state source cohort is unavailable")
    return _SourceControlPlane(
        manifest_id=UUID(str(row[0])),
        manifest_sha256=str(row[1]),
        stock_count=int(row[2]),
        profile_id=UUID(str(row[3])),
        profile_sha256=str(row[4]),
        provider_tier=ProviderTier(str(row[5])),
        universe_sha256=str(row[6]),
        capability_config_sha256=str(row[7]),
        configuration_sha256=str(row[8]),
        model_bundle_sha256=str(row[9]),
        valuation_sha256=str(row[10]),
    )


def _fresh_capability_profile_cursor(
    cursor,
    source: _SourceControlPlane,
    *,
    desired_universe_sha256: str,
    desired_configuration_sha256: str,
    required_through: datetime,
    allow_source_universe: bool = False,
) -> tuple[UUID, str] | None:
    required = _utc(required_through, "capability required_through")
    accepted_universes = [desired_universe_sha256]
    accepted_configurations = [desired_configuration_sha256]
    if (
        desired_universe_sha256 == source.universe_sha256
        or allow_source_universe
    ):
        accepted_universes.append(source.universe_sha256)
        accepted_configurations.append(
            source.capability_config_sha256
        )
    cursor.execute(
        "SELECT profile_id::text,profile_sha256 "
        "FROM rt.capability_profiles "
        "WHERE provider_tier=%s AND config_sha256=ANY(%s) "
        "AND universe_sha256=ANY(%s) AND status='PASS' "
        "AND expires_at>=%s "
        "ORDER BY (universe_sha256=%s) DESC,"
        "expires_at DESC,checked_at DESC,profile_id DESC "
        "LIMIT 1",
        (
            source.provider_tier.value,
            accepted_configurations,
            accepted_universes,
            required,
            desired_universe_sha256,
        ),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return UUID(str(row[0])), str(row[1])


def _selection_policy_cursor(
    cursor,
    change: DesiredStateChange,
    source: _SourceControlPlane,
) -> tuple[str, str]:
    if change.source_slot_version is None:
        cursor.execute(
            "SELECT allocation_policy_sha256 "
            "FROM rt.subscription_manifests WHERE manifest_id=%s",
            (str(source.manifest_id),),
        )
        row = cursor.fetchone()
        if row is None:
            raise ValueError("desired state source manifest is unavailable")
        return (
            f"rt.subscription_manifests:{source.manifest_id}:allocation",
            str(row[0]),
        )
    cursor.execute(
        "SELECT policy_sha256 FROM rt.slot_set_versions "
        "WHERE cohort_id=%s AND version=%s",
        (str(change.source_cohort_id), change.source_slot_version),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("desired state stored slot-set reference is unavailable")
    return (
        "rt.slot_set_versions:"
        f"{change.source_cohort_id}:{change.source_slot_version}",
        str(row[0]),
    )


def _stored_security_identities_cursor(
    cursor,
    members: tuple[DesiredMemberIntent, ...],
    *,
    observed_at: datetime,
    allow_missing: bool = False,
) -> tuple[dict[str, UUID], dict[str, int]]:
    security_ids: dict[str, UUID] = {}
    instrument_ids: dict[str, int] = {}
    kinds_by_ticker: dict[str, set[DesiredMemberKind]] = {}
    for member in members:
        kinds_by_ticker.setdefault(member.ticker, set()).add(member.kind)
    tickers = tuple(sorted(kinds_by_ticker))
    cursor.execute(
        "SELECT identity.ticker,identity.security_id::text,"
        "identity.asset_type,identity.security_type,"
        "identity.instrument_id "
        "FROM ("
        "SELECT DISTINCT ON (revision.ticker,revision.security_id,"
        "instrument.instrument_id) "
        "revision.ticker,revision.security_id,security.asset_type,"
        "revision.security_type,instrument.instrument_id,"
        "revision.effective_from,revision.observed_at "
        "FROM ref.security_revisions AS revision "
        "JOIN ref.securities AS security "
        "ON security.security_id=revision.security_id "
        "JOIN rt.instruments AS instrument "
        "ON instrument.asset_class='EQUITY' "
        "AND instrument.security_id=revision.security_id "
        "WHERE revision.ticker=ANY(%s::text[]) "
        "AND revision.active=true AND revision.calendar_id='XNYS' "
        "AND revision.effective_from<=%s "
        "AND (revision.effective_to IS NULL OR revision.effective_to>%s) "
        "AND revision.observed_at<=%s "
        "ORDER BY revision.ticker,revision.security_id,"
        "instrument.instrument_id,revision.effective_from DESC,"
        "revision.observed_at DESC"
        ") AS identity "
        "ORDER BY identity.ticker,identity.security_id,"
        "identity.instrument_id",
        (list(tickers), observed_at, observed_at, observed_at),
    )
    rows_by_ticker: dict[str, list[tuple[object, ...]]] = {
        ticker: [] for ticker in tickers
    }
    for row in cursor.fetchall():
        rows_by_ticker.setdefault(str(row[0]), []).append(tuple(row[1:]))
    for ticker, kinds in sorted(kinds_by_ticker.items()):
        rows = rows_by_ticker[ticker]
        if not rows and allow_missing:
            continue
        if len(rows) != 1:
            raise ValueError(
                f"desired member {ticker} does not resolve to one active stored identity"
            )
        security_id, asset_type, security_type, instrument_id = rows[0]
        if (
            DesiredMemberKind.SECTOR_ETF in kinds
            and str(asset_type) != "ETF"
            and str(security_type) != "ETF"
        ):
            raise ValueError(f"desired sector ETF {ticker} is not a stored ETF identity")
        security_ids[ticker] = UUID(str(security_id))
        instrument_ids[ticker] = int(instrument_id)
    return security_ids, instrument_ids


def _source_member_security_cursor(
    cursor,
    source: _SourceControlPlane,
) -> tuple[set[UUID], set[UUID]]:
    cursor.execute(
        "SELECT instrument.security_id::text,member.option_underlying "
        "FROM rt.subscription_members AS member "
        "JOIN rt.instruments AS instrument "
        "ON instrument.instrument_id=member.instrument_id "
        "WHERE member.manifest_id=%s AND instrument.asset_class='EQUITY'",
        (str(source.manifest_id),),
    )
    base: set[UUID] = set()
    underlyings: set[UUID] = set()
    for security_id, option_underlying in cursor.fetchall():
        identity = UUID(str(security_id))
        base.add(identity)
        if bool(option_underlying):
            underlyings.add(identity)
    return base, underlyings


def _source_option_members_cursor(
    cursor,
    *,
    cohort_id: UUID,
    version: int,
) -> tuple[_StoredOptionMember, ...]:
    cursor.execute(
        "SELECT member.instrument_id,contract.underlying_security_id::text,"
        "contract.contract_ticker,version_row.call_put,member.channel,"
        "member.purpose,member.priority,member.catalog_version_id "
        "FROM rt.slot_set_members AS member "
        "JOIN rt.instruments AS instrument "
        "ON instrument.instrument_id=member.instrument_id "
        "JOIN ref.option_contracts AS contract "
        "ON contract.contract_id=instrument.contract_id "
        "JOIN ref.option_contract_versions AS version_row "
        "ON version_row.version_id=member.catalog_version_id "
        "AND version_row.contract_id=contract.contract_id "
        "WHERE member.cohort_id=%s AND member.version=%s "
        "ORDER BY contract.contract_ticker,member.channel",
        (str(cohort_id), version),
    )
    return tuple(
        _StoredOptionMember(
            instrument_id=int(row[0]),
            security_id=UUID(str(row[1])),
            contract_ticker=str(row[2]),
            call_put=str(row[3]).strip(),
            channel=MarketChannel(str(row[4])),
            purpose=SlotSetPurpose(str(row[5])),
            priority=int(row[6]),
            catalog_version_id=int(row[7]),
        )
        for row in cursor.fetchall()
    )


def _ready_option_securities(
    members: tuple[_StoredOptionMember, ...],
) -> set[UUID]:
    channels: dict[tuple[UUID, str], set[MarketChannel]] = {}
    for member in members:
        channels.setdefault(
            (member.security_id, member.contract_ticker),
            set(),
        ).add(member.channel)
    paired = {MarketChannel.TRADE, MarketChannel.QUOTE}
    return {
        security_id
        for security_id in {key[0] for key in channels}
        if channels
        and all(
            member_channels == paired
            for (member_security, _contract), member_channels in channels.items()
            if member_security == security_id
        )
    }


def _validate_default_exact_four(
    revision: DesiredStateRevision,
    option_members: tuple[_StoredOptionMember, ...],
) -> None:
    enabled_base = {
        row.ticker
        for row in revision.members
        if row.enabled
        and row.kind in {
            DesiredMemberKind.EQUITY,
            DesiredMemberKind.SECTOR_ETF,
        }
    }
    enabled_options = {
        row.ticker
        for row in revision.members
        if row.enabled and row.kind is DesiredMemberKind.OPTION_UNDERLYING
    }
    if (
        enabled_base != set(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
        or enabled_options != set(DEFAULT_OPTION_UNDERLYINGS)
    ):
        return
    ticker_by_security = dict(revision.security_ids)
    security_to_ticker = {security_id: ticker for ticker, security_id in ticker_by_security.items()}
    exact_contracts = {
        (security_to_ticker.get(row.security_id), row.call_put, row.contract_ticker)
        for row in option_members
    }
    identities = {(ticker, call_put) for ticker, call_put, _contract in exact_contracts}
    contracts = {contract for _ticker, _call_put, contract in exact_contracts}
    channels = {
        contract: {
            row.channel
            for row in option_members
            if row.contract_ticker == contract
        }
        for contract in contracts
    }
    if identities != {
        ("QQQ", "C"),
        ("QQQ", "P"),
        ("SPY", "C"),
        ("SPY", "P"),
    } or len(contracts) != 4 or any(
        contract_channels
        != {MarketChannel.TRADE, MarketChannel.QUOTE}
        for contract_channels in channels.values()
    ):
        raise ValueError(
            "default Advanced realtime desired state requires exact-four QQQ/SPY contracts"
        )


def load_desired_state_revision_cursor(
    cursor,
    revision_id: UUID,
) -> DesiredStateRevision:
    cursor.execute(
        "SELECT revision_sha256,effective_session_date,source_cohort_id::text,"
        "source_slot_version,selection_policy_reference,"
        "selection_policy_sha256,enabled,recorded_at "
        "FROM rt.realtime_desired_revisions WHERE revision_id=%s",
        (str(revision_id),),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("desired state revision is unavailable")
    cursor.execute(
        "SELECT intent.ticker,intent.member_kind,intent.enabled,"
        "intent.priority,binding.security_id::text "
        "FROM rt.realtime_desired_member_intents AS intent "
        "LEFT JOIN rt.realtime_desired_member_bindings AS binding "
        "ON binding.intent_id=intent.intent_id "
        "WHERE intent.revision_id=%s "
        "ORDER BY intent.ticker,intent.member_kind",
        (str(revision_id),),
    )
    member_rows = cursor.fetchall()
    if not member_rows:
        raise ValueError("desired state revision has no members")
    members = tuple(
        DesiredMemberIntent(
            ticker=str(member[0]),
            kind=DesiredMemberKind(str(member[1])),
            enabled=bool(member[2]),
            priority=int(member[3]),
        )
        for member in member_rows
    )
    ids_by_ticker: dict[str, set[UUID]] = {}
    for member in member_rows:
        if member[4] is not None:
            ids_by_ticker.setdefault(str(member[0]), set()).add(
                UUID(str(member[4]))
            )
    if any(len(values) != 1 for values in ids_by_ticker.values()):
        raise ValueError("desired state revision member identities diverged")
    security_ids = {
        ticker: next(iter(values))
        for ticker, values in ids_by_ticker.items()
    }
    return DesiredStateRevision(
        revision_id=revision_id,
        revision_sha256=str(row[0]),
        effective_session_date=row[1],
        source_cohort_id=UUID(str(row[2])),
        source_slot_version=(
            None if row[3] is None else int(row[3])
        ),
        selection_policy_reference=str(row[4]),
        selection_policy_sha256=str(row[5]),
        enabled=bool(row[6]),
        recorded_at=_utc(row[7], "stored desired state recorded_at"),
        members=members,
        security_ids=tuple(sorted(security_ids.items())),
    )


def load_desired_member_preparations_cursor(
    cursor,
    revision_id: UUID,
) -> tuple[DesiredMemberPreparation, ...]:
    cursor.execute(
        "SELECT binding.security_id::text,intent.ticker,"
        "intent.member_kind,intent.enabled,readiness.status,"
        "readiness.reasons,readiness.checked_at "
        "FROM rt.realtime_desired_member_intents AS intent "
        "JOIN rt.realtime_desired_intent_readiness AS readiness "
        "ON readiness.intent_id=intent.intent_id "
        "LEFT JOIN rt.realtime_desired_member_bindings AS binding "
        "ON binding.intent_id=intent.intent_id "
        "WHERE intent.revision_id=%s "
        "ORDER BY intent.ticker,intent.member_kind",
        (str(revision_id),),
    )
    return tuple(
        DesiredMemberPreparation(
            revision_id=revision_id,
            security_id=(
                None if row[0] is None else UUID(str(row[0]))
            ),
            ticker=str(row[1]),
            kind=DesiredMemberKind(str(row[2])),
            enabled=bool(row[3]),
            status=MemberPreparationStatus(str(row[4])),
            reasons=tuple(str(reason) for reason in row[5]),
            checked_at=_utc(row[6], "stored desired member checked_at"),
        )
        for row in cursor.fetchall()
    )


def load_current_desired_state_revision_cursor(
    cursor,
    *,
    scope_key: str = DESIRED_STATE_SCOPE,
) -> DesiredStateRevision | None:
    scope = scope_key.strip()
    if not scope:
        raise ValueError("desired state current scope cannot be blank")
    cursor.execute(
        "SELECT revision_id::text,effective_session_date,"
        "revision_recorded_at FROM rt.realtime_desired_current "
        "WHERE scope_key=%s",
        (scope,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    revision = load_desired_state_revision_cursor(cursor, UUID(str(row[0])))
    if (
        revision.effective_session_date != row[1]
        or revision.recorded_at
        != _utc(row[2], "stored current revision recorded_at")
    ):
        raise ValueError("current desired state pointer metadata diverged")
    return revision


def load_effective_desired_state_reconciliation_cursor(
    cursor,
    *,
    session_date: date,
    scope_key: str = DESIRED_STATE_SCOPE,
) -> DesiredStateReconciliation | None:
    scope = scope_key.strip()
    if not scope:
        raise ValueError("desired state current scope cannot be blank")
    cursor.execute(
        "SELECT current.effective_session_date "
        "FROM rt.realtime_desired_current AS current "
        "WHERE current.scope_key=%s",
        (scope,),
    )
    current = cursor.fetchone()
    if current is None:
        return None
    if current[0] <= session_date:
        revision = load_current_desired_state_revision_cursor(
            cursor,
            scope_key=scope,
        )
        if revision is None:
            return None
    else:
        cursor.execute(
            "SELECT revision.revision_id::text "
            "FROM rt.realtime_desired_revisions AS revision "
            "JOIN rt.realtime_desired_session_snapshots AS snapshot "
            "USING(revision_id) "
            "WHERE revision.effective_session_date<=%s "
            "ORDER BY revision.effective_session_date DESC,"
            "revision.recorded_at DESC,revision.revision_id::text DESC "
            "LIMIT 1",
            (session_date,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        revision = load_desired_state_revision_cursor(
            cursor,
            UUID(str(row[0])),
        )
    snapshot = _load_desired_snapshot_cursor(
        cursor,
        revision.revision_id,
    )
    if snapshot is None:
        return DesiredStateReconciliation(
            state="MISSING_SNAPSHOT",
            revision=revision,
            preparations=load_desired_member_preparations_cursor(
                cursor,
                revision.revision_id,
            ),
            snapshot=None,
            reason_codes=("IMMUTABLE_SNAPSHOT_MISSING",),
        )
    if (
        snapshot.session_date != revision.effective_session_date
        or snapshot.session_date > session_date
    ):
        raise ValueError("effective desired snapshot metadata diverged")
    return DesiredStateReconciliation(
        state="READY",
        revision=revision,
        preparations=load_desired_member_preparations_cursor(
            cursor,
            revision.revision_id,
        ),
        snapshot=snapshot,
    )


def _select_current_desired_state_revision_cursor(
    cursor,
    revision: DesiredStateRevision,
    *,
    selected_at: datetime,
    scope_key: str = DESIRED_STATE_SCOPE,
) -> None:
    selected = _utc(selected_at, "desired state selected_at")
    if selected < revision.recorded_at:
        raise ValueError("desired state selection cannot precede its revision")
    scope = scope_key.strip()
    if not scope:
        raise ValueError("desired state current scope cannot be blank")
    cursor.execute(
        "INSERT INTO rt.realtime_desired_current ("
        "scope_key,revision_id,effective_session_date,"
        "revision_recorded_at,selected_at) VALUES (%s,%s,%s,%s,%s) "
        "ON CONFLICT (scope_key) DO UPDATE SET "
        "revision_id=EXCLUDED.revision_id,"
        "effective_session_date=EXCLUDED.effective_session_date,"
        "revision_recorded_at=EXCLUDED.revision_recorded_at,"
        "selected_at=EXCLUDED.selected_at,updated_at=clock_timestamp() "
        "WHERE EXCLUDED.effective_session_date "
        "> rt.realtime_desired_current.effective_session_date "
        "OR (EXCLUDED.effective_session_date "
        "= rt.realtime_desired_current.effective_session_date "
        "AND EXCLUDED.revision_recorded_at "
        "> rt.realtime_desired_current.revision_recorded_at) "
        "OR (EXCLUDED.effective_session_date "
        "= rt.realtime_desired_current.effective_session_date "
        "AND EXCLUDED.revision_recorded_at "
        "= rt.realtime_desired_current.revision_recorded_at "
        "AND EXCLUDED.revision_id::text "
        "> rt.realtime_desired_current.revision_id::text)",
        (
            scope,
            str(revision.revision_id),
            revision.effective_session_date,
            revision.recorded_at,
            selected,
        ),
    )


def _persist_desired_members_cursor(
    cursor,
    revision: DesiredStateRevision,
    *,
    security_ids: dict[str, UUID],
    instrument_ids: dict[str, int],
    source_base: set[UUID],
    source_underlyings: set[UUID],
    ready_option_securities: set[UUID],
    checked_at: datetime,
) -> None:
    member_values = []
    readiness_values = []
    intent_values = []
    intent_readiness_values = []
    binding_values = []
    if security_ids:
        cursor.execute(
            "SELECT DISTINCT ON (security_id) security_id::text,"
            "revision_id::text FROM ref.security_revisions "
            "WHERE security_id=ANY(%s::uuid[]) AND observed_at<=%s "
            "ORDER BY security_id,observed_at DESC,effective_from DESC,"
            "revision_id DESC",
            (
                [str(value) for value in security_ids.values()],
                checked_at,
            ),
        )
        security_revision_ids = {
            UUID(str(row[0])): UUID(str(row[1]))
            for row in cursor.fetchall()
        }
    else:
        security_revision_ids = {}
    for member in revision.members:
        security_id = security_ids.get(member.ticker)
        intent_id = desired_member_intent_id(
            revision.revision_id,
            member.ticker,
            member.kind,
        )
        intent_values.append((
            str(intent_id),
            str(revision.revision_id),
            member.ticker,
            member.kind.value,
            member.priority,
            member.enabled,
            checked_at,
        ))
        resolved = security_id is not None
        if resolved:
            security_revision_id = security_revision_ids.get(
                security_id
            )
            instrument_id = instrument_ids.get(member.ticker)
            if security_revision_id is None or instrument_id is None:
                raise ValueError(
                    "desired member stored identity binding is incomplete"
                )
            identity_sha256 = canonical_sha256({
                "version": "advanced-realtime-member-binding-v1",
                "intent_id": intent_id,
                "security_id": security_id,
                "security_revision_id": security_revision_id,
                "instrument_id": instrument_id,
            })
            binding_values.append((
                str(intent_id),
                str(revision.revision_id),
                str(security_id),
                str(security_revision_id),
                instrument_id,
                identity_sha256,
                Json({"source": "STORED_REFERENCE"}),
                checked_at,
            ))
        else:
            instrument_id = None
        ready = (
            not member.enabled
            or (
                resolved
                and member.kind in {
                    DesiredMemberKind.EQUITY,
                    DesiredMemberKind.SECTOR_ETF,
                }
                and security_id in source_base
            )
            or (
                resolved
                and member.kind is DesiredMemberKind.OPTION_UNDERLYING
                and security_id in source_underlyings
                and security_id in ready_option_securities
            )
        )
        readiness_status = (
            MemberPreparationStatus.READY.value
            if ready
            else MemberPreparationStatus.WARMING.value
        )
        intent_readiness_values.append((
            str(intent_id),
            readiness_status,
            [],
            checked_at,
        ))
        if not resolved:
            continue
        member_values.append((
            str(revision.revision_id),
            str(security_id),
            member.ticker,
            member.kind.value,
            member.priority,
            member.enabled,
        ))
        readiness_values.append((
            str(revision.revision_id),
            str(security_id),
            member.kind.value,
            (
                readiness_status
            ),
            [],
            checked_at,
        ))
    execute_values(
        cursor,
        "INSERT INTO rt.realtime_desired_member_intents ("
        "intent_id,revision_id,ticker,member_kind,priority,enabled,"
        "requested_at"
        ") VALUES %s ON CONFLICT DO NOTHING",
        intent_values,
        page_size=_DESIRED_MEMBER_BATCH_SIZE,
    )
    execute_values(
        cursor,
        "INSERT INTO rt.realtime_desired_intent_readiness ("
        "intent_id,status,reasons,checked_at"
        ") VALUES %s ON CONFLICT DO NOTHING",
        intent_readiness_values,
        page_size=_DESIRED_MEMBER_BATCH_SIZE,
    )
    if binding_values:
        execute_values(
            cursor,
            "INSERT INTO rt.realtime_desired_member_bindings ("
            "intent_id,revision_id,security_id,security_revision_id,"
            "instrument_id,identity_sha256,source_lineage,resolved_at"
            ") VALUES %s ON CONFLICT DO NOTHING",
            binding_values,
            page_size=_DESIRED_MEMBER_BATCH_SIZE,
        )
    if member_values:
        execute_values(
            cursor,
            "INSERT INTO rt.realtime_desired_members ("
            "revision_id,security_id,ticker,member_kind,priority,enabled"
            ") VALUES %s ON CONFLICT DO NOTHING",
            member_values,
            page_size=_DESIRED_MEMBER_BATCH_SIZE,
        )
    if readiness_values:
        execute_values(
            cursor,
            "INSERT INTO rt.realtime_desired_member_readiness ("
            "revision_id,security_id,member_kind,status,reasons,checked_at"
            ") VALUES %s ON CONFLICT DO NOTHING",
            readiness_values,
            page_size=_DESIRED_MEMBER_BATCH_SIZE,
        )
    _persist_equity_preparation_work_cursor(
        cursor,
        revision,
        security_ids=security_ids,
        instrument_ids=instrument_ids,
        source_base=source_base,
        checked_at=checked_at,
    )


def _equity_preparation_targets(
    revision: DesiredStateRevision,
) -> tuple[tuple[str, int, dict[str, object]], ...]:
    calendar = OptionExchangeCalendar("XNYS")
    history_through = calendar.previous_session_date(
        revision.effective_session_date
    )
    plan = build_realtime_equity_portal_history_plan(
        history_through,
        equity_ticker_count=1,
    )
    expected_counts = {
        f"{interval}:{basis}": count
        for interval, basis, count in plan.expected_counts_per_ticker
    }
    common = {
        "contract": "advanced-equity-preparation-v1",
        "effective_session_date": (
            revision.effective_session_date.isoformat()
        ),
        "history_through_session": history_through.isoformat(),
        "history_policy_sha256": plan.policy_sha256,
    }
    by_stage = {
        "REFERENCE": {
            **common,
            "window_start": plan.adjusted_sessions[0].isoformat(),
            "window_end": history_through.isoformat(),
            "require_sector": True,
            "require_corporate_action_coverage": True,
        },
        "ADJUSTED_DAILY": {
            **common,
            "adjustment_basis": "ADJUSTED",
            "interval_sessions": {
                "1d": [
                    session.isoformat()
                    for session in plan.adjusted_sessions
                ],
                "1wk": [
                    session.isoformat()
                    for session in plan.weekly_sessions
                ],
                "1mo": [
                    session.isoformat()
                    for session in plan.monthly_sessions
                ],
            },
            "expected_counts": {
                f"{interval}:ADJUSTED": expected_counts[
                    f"{interval}:ADJUSTED"
                ]
                for interval in ("1d", "1wk", "1mo")
            },
        },
        "NATIVE_INTRADAY": {
            **common,
            "interval_sessions": {
                "1m": [
                    session.isoformat()
                    for session in plan.one_minute_sessions
                ],
                "5m": [
                    session.isoformat()
                    for session in plan.raw_sessions
                ],
            },
            "expected_counts": {
                "1m:RAW_UNADJUSTED": expected_counts[
                    "1m:RAW_UNADJUSTED"
                ],
                "5m:RAW_UNADJUSTED": expected_counts[
                    "5m:RAW_UNADJUSTED"
                ],
            },
        },
        "DERIVED_INTERVALS": {
            **common,
            "source_interval": "5m",
            "source_sessions": [
                session.isoformat()
                for session in plan.raw_sessions
            ],
            "target_intervals": ["15m", "30m", "1h", "1d"],
            "expected_counts": {
                f"{interval}:RAW_UNADJUSTED": expected_counts[
                    f"{interval}:RAW_UNADJUSTED"
                ]
                for interval in ("15m", "30m", "1h", "1d")
            },
        },
        "TECHNICAL_WARMUP": {
            **common,
            "required_adjusted_daily_bars": 210,
            "required_derived_hourly_bars": 200,
            "required_adjusted_intervals": {
                f"{interval}:ADJUSTED": expected_counts[
                    f"{interval}:ADJUSTED"
                ]
                for interval in ("1d", "1wk", "1mo")
            },
            "declared_products": [
                "DAILY_SCREENING",
                "HOURLY_SCREENING",
                "STOCK_ALERTS",
                "PORTAL_HISTORY",
            ],
            "history_policy_version": (
                REALTIME_EQUITY_PORTAL_HISTORY_POLICY.version
            ),
        },
    }
    return tuple(
        (stage, order, by_stage[stage])
        for stage, order in EQUITY_PREPARATION_STAGES
    )


def _persist_equity_preparation_work_cursor(
    cursor,
    revision: DesiredStateRevision,
    *,
    security_ids: dict[str, UUID],
    instrument_ids: dict[str, int],
    source_base: set[UUID],
    checked_at: datetime,
) -> None:
    warming = tuple(
        member
        for member in revision.members
        if member.enabled
        and member.kind in {
            DesiredMemberKind.EQUITY,
            DesiredMemberKind.SECTOR_ETF,
        }
        and (
            member.ticker not in security_ids
            or security_ids[member.ticker] not in source_base
        )
    )
    if not warming:
        return
    targets = _equity_preparation_targets(revision)
    execute_values(
        cursor,
        "INSERT INTO rt.realtime_equity_preparation_work ("
        "revision_id,intent_id,security_id,instrument_id,member_kind,"
        "ticker,stage,stage_order,status,target,next_retry_at"
        ") VALUES %s ON CONFLICT DO NOTHING",
        [
            (
                str(revision.revision_id),
                str(desired_member_intent_id(
                    revision.revision_id,
                    member.ticker,
                    member.kind,
                )),
                (
                    None
                    if member.ticker not in security_ids
                    else str(security_ids[member.ticker])
                ),
                instrument_ids.get(member.ticker),
                member.kind.value,
                member.ticker,
                stage,
                stage_order,
                "PENDING",
                Json(target),
                checked_at,
            )
            for member in warming
            for stage, stage_order, target in targets
        ],
        page_size=_DESIRED_MEMBER_BATCH_SIZE,
    )


def record_desired_state_revision_cursor(
    cursor,
    change: DesiredStateChange,
    *,
    observed_at: datetime,
    calendar: OptionExchangeCalendar | None = None,
) -> DesiredStateRevision:
    observed = _utc(observed_at, "desired state observed_at")
    return _record_desired_state_revision_cursor(
        cursor,
        change,
        observed_at=observed,
        effective_session_date=effective_next_session(
            observed,
            calendar=calendar,
        ),
        calendar=calendar,
    )


def record_desired_state_rollover_revision_cursor(
    cursor,
    change: DesiredStateChange,
    *,
    effective_session_date: date,
    recorded_at: datetime,
    calendar: OptionExchangeCalendar | None = None,
) -> DesiredStateRevision:
    return _record_desired_state_revision_cursor(
        cursor,
        change,
        observed_at=_utc(recorded_at, "desired rollover recorded_at"),
        effective_session_date=effective_session_date,
        calendar=calendar,
    )


def _record_desired_state_revision_cursor(
    cursor,
    change: DesiredStateChange,
    *,
    observed_at: datetime,
    effective_session_date: date,
    calendar: OptionExchangeCalendar | None,
) -> DesiredStateRevision:
    observed = _utc(observed_at, "desired state observed_at")
    exchange = calendar or OptionExchangeCalendar("XNYS")
    if (
        not isinstance(effective_session_date, date)
        or exchange.session_on_or_after(effective_session_date)
        != effective_session_date
    ):
        raise ValueError(
            "desired state rollover target must be an XNYS session"
        )
    source = _load_source_control_plane_cursor(cursor, change.source_cohort_id)
    policy_reference, policy_sha256 = _selection_policy_cursor(cursor, change, source)
    security_ids, instrument_ids = _stored_security_identities_cursor(
        cursor,
        change.members,
        observed_at=observed,
        allow_missing=True,
    )
    unresolved_options = tuple(
        member.ticker
        for member in change.members
        if member.enabled
        and member.kind is DesiredMemberKind.OPTION_UNDERLYING
        and member.ticker not in security_ids
    )
    if unresolved_options and change.source_slot_version is not None:
        raise ValueError(
            "Options desired members require stored identities: "
            + ",".join(sorted(unresolved_options))
        )
    revision = build_desired_state_revision(
        change,
        effective_session_date=effective_session_date,
        recorded_at=observed,
        security_ids=security_ids,
        selection_policy_reference=policy_reference,
        selection_policy_sha256=policy_sha256,
    )
    source_base, source_underlyings = _source_member_security_cursor(cursor, source)
    option_members = (
        ()
        if change.source_slot_version is None
        else _source_option_members_cursor(
            cursor,
            cohort_id=change.source_cohort_id,
            version=change.source_slot_version,
        )
    )
    if change.source_slot_version is not None:
        _validate_default_exact_four(revision, option_members)
    option_securities = _ready_option_securities(option_members)
    cursor.execute(
        "INSERT INTO rt.realtime_desired_revisions ("
        "revision_id,revision_sha256,effective_session_date,"
        "source_cohort_id,source_slot_version,selection_policy_reference,"
        "selection_policy_sha256,enabled,recorded_at) VALUES ("
        "%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (revision_id) DO NOTHING",
        (
            str(revision.revision_id),
            revision.revision_sha256,
            revision.effective_session_date,
            str(revision.source_cohort_id),
            revision.source_slot_version,
            revision.selection_policy_reference,
            revision.selection_policy_sha256,
            revision.enabled,
            revision.recorded_at,
        ),
    )
    _persist_desired_members_cursor(
        cursor,
        revision,
        security_ids=security_ids,
        instrument_ids=instrument_ids,
        source_base=source_base,
        source_underlyings=source_underlyings,
        ready_option_securities=option_securities,
        checked_at=observed,
    )
    stored = load_desired_state_revision_cursor(cursor, revision.revision_id)
    expected_identity = (
        revision.revision_id,
        revision.revision_sha256,
        revision.effective_session_date,
        revision.source_cohort_id,
        revision.source_slot_version,
        revision.selection_policy_reference,
        revision.selection_policy_sha256,
        revision.enabled,
        revision.members,
        revision.security_ids,
    )
    stored_identity = (
        stored.revision_id,
        stored.revision_sha256,
        stored.effective_session_date,
        stored.source_cohort_id,
        stored.source_slot_version,
        stored.selection_policy_reference,
        stored.selection_policy_sha256,
        stored.enabled,
        stored.members,
        stored.security_ids,
    )
    if stored_identity != expected_identity:
        raise ValueError("stored desired state revision conflicts with its identity")
    preparations = load_desired_member_preparations_cursor(
        cursor,
        revision.revision_id,
    )
    if len(preparations) != len(revision.members):
        raise ValueError("stored desired state readiness is incomplete")
    _select_current_desired_state_revision_cursor(
        cursor,
        stored,
        selected_at=max(observed, stored.recorded_at),
    )
    return stored


def _lock_desired_revision_cursor(cursor, revision_id: UUID) -> None:
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (f"advanced-realtime-desired-revision:{revision_id}",),
    )


def set_desired_member_preparation_cursor(
    cursor,
    revision_id: UUID,
    *,
    ticker: str,
    kind: DesiredMemberKind,
    status: MemberPreparationStatus,
    checked_at: datetime,
    reasons: tuple[str, ...] = (),
) -> DesiredMemberPreparation:
    checked = _utc(checked_at, "desired member checked_at")
    _lock_desired_revision_cursor(cursor, revision_id)
    if not isinstance(kind, DesiredMemberKind):
        raise TypeError("desired member readiness kind is invalid")
    if not isinstance(status, MemberPreparationStatus):
        raise TypeError("desired member readiness status is invalid")
    normalized_ticker = ticker.strip().upper()
    normalized_reasons = tuple(reason.strip() for reason in reasons)
    if any(not reason for reason in normalized_reasons):
        raise ValueError("desired member readiness reasons cannot be blank")
    if status is MemberPreparationStatus.READY and normalized_reasons:
        raise ValueError("READY desired member cannot carry failure reasons")
    if status is MemberPreparationStatus.FAILED and not normalized_reasons:
        raise ValueError("FAILED desired member requires a reason")
    intent_id = desired_member_intent_id(
        revision_id,
        normalized_ticker,
        kind,
    )
    cursor.execute(
        "UPDATE rt.realtime_desired_intent_readiness AS readiness "
        "SET status=%s,reasons=%s,checked_at=%s,updated_at=clock_timestamp() "
        "FROM rt.realtime_desired_member_intents AS intent "
        "LEFT JOIN rt.realtime_desired_member_bindings AS binding "
        "ON binding.intent_id=intent.intent_id "
        "WHERE readiness.intent_id=%s "
        "AND intent.intent_id=readiness.intent_id "
        "AND intent.revision_id=%s AND intent.ticker=%s "
        "AND intent.member_kind=%s "
        "RETURNING binding.security_id::text,intent.enabled",
        (
            status.value,
            list(normalized_reasons),
            checked,
            str(intent_id),
            str(revision_id),
            normalized_ticker,
            kind.value,
        ),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("desired member readiness target is unavailable")
    if row[0] is not None:
        cursor.execute(
            "UPDATE rt.realtime_desired_member_readiness "
            "SET status=%s,reasons=%s,checked_at=%s,"
            "updated_at=clock_timestamp() "
            "WHERE revision_id=%s AND security_id=%s "
            "AND member_kind=%s",
            (
                status.value,
                list(normalized_reasons),
                checked,
                str(revision_id),
                str(row[0]),
                kind.value,
            ),
        )
    return DesiredMemberPreparation(
        revision_id=revision_id,
        security_id=(
            None if row[0] is None else UUID(str(row[0]))
        ),
        ticker=normalized_ticker,
        kind=kind,
        enabled=bool(row[1]),
        status=status,
        reasons=normalized_reasons,
        checked_at=checked,
    )


def bind_desired_member_identity_cursor(
    cursor,
    revision_id: UUID,
    *,
    ticker: str,
    kind: DesiredMemberKind,
    security_id: UUID,
    security_revision_id: UUID,
    checked_at: datetime,
    source_lineage: dict[str, object],
) -> DesiredMemberPreparation:
    checked = _utc(checked_at, "desired member identity checked_at")
    if kind not in {
        DesiredMemberKind.EQUITY,
        DesiredMemberKind.SECTOR_ETF,
    }:
        raise ValueError(
            "C4 identity binding supports only Equity members"
        )
    if (
        not isinstance(security_id, UUID)
        or security_id.int == 0
        or not isinstance(security_revision_id, UUID)
        or security_revision_id.int == 0
        or not source_lineage
    ):
        raise ValueError("desired member resolved identity is invalid")
    normalized_ticker = ticker.strip().upper()
    intent_id = desired_member_intent_id(
        revision_id,
        normalized_ticker,
        kind,
    )
    _lock_desired_revision_cursor(cursor, revision_id)
    cursor.execute(
        "SELECT intent.priority,intent.enabled,readiness.status,"
        "readiness.reasons,readiness.checked_at "
        "FROM rt.realtime_desired_member_intents AS intent "
        "JOIN rt.realtime_desired_intent_readiness AS readiness "
        "ON readiness.intent_id=intent.intent_id "
        "WHERE intent.intent_id=%s AND intent.revision_id=%s "
        "AND intent.ticker=%s AND intent.member_kind=%s",
        (
            str(intent_id),
            str(revision_id),
            normalized_ticker,
            kind.value,
        ),
    )
    intent = cursor.fetchone()
    if intent is None:
        raise ValueError("desired member unresolved intent is unavailable")
    cursor.execute(
        "SELECT identity.security_id,identity.revision_id,"
        "identity.asset_type,identity.security_type FROM ("
        "SELECT DISTINCT ON (revision.security_id) "
        "revision.security_id::text AS security_id,"
        "revision.revision_id::text AS revision_id,"
        "security.asset_type,revision.security_type,"
        "revision.effective_from,revision.observed_at "
        "FROM ref.security_revisions AS revision "
        "JOIN ref.securities AS security USING(security_id) "
        "WHERE revision.ticker=%s AND revision.active=true "
        "AND revision.calendar_id='XNYS' "
        "AND revision.effective_from<=%s "
        "AND (revision.effective_to IS NULL "
        "OR revision.effective_to>%s) "
        "AND revision.observed_at<=%s "
        "ORDER BY revision.security_id,revision.effective_from DESC,"
        "revision.observed_at DESC,revision.revision_id DESC"
        ") AS identity ORDER BY identity.security_id",
        (
            normalized_ticker,
            checked,
            checked,
            checked,
        ),
    )
    identities = tuple(cursor.fetchall())
    if len(identities) != 1:
        raise ValueError(
            f"desired member {normalized_ticker} does not resolve "
            "to one active stored identity"
        )
    identity = identities[0]
    if (
        UUID(str(identity[0])) != security_id
        or UUID(str(identity[1])) != security_revision_id
    ):
        raise ValueError("desired member provider identity conflicts")
    if (
        kind is DesiredMemberKind.SECTOR_ETF
        and str(identity[2]) != "ETF"
        and str(identity[3]) != "ETF"
    ):
        raise ValueError(
            f"desired sector ETF {normalized_ticker} "
            "is not a stored ETF identity"
        )
    instrument_id = ensure_equity_instrument_cursor(
        cursor,
        security_id,
    )
    identity_sha256 = canonical_sha256({
        "version": "advanced-realtime-member-binding-v1",
        "intent_id": intent_id,
        "security_id": security_id,
        "security_revision_id": security_revision_id,
        "instrument_id": instrument_id,
    })
    cursor.execute(
        "INSERT INTO rt.realtime_desired_member_bindings ("
        "intent_id,revision_id,security_id,security_revision_id,"
        "instrument_id,identity_sha256,source_lineage,resolved_at"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (intent_id) DO NOTHING",
        (
            str(intent_id),
            str(revision_id),
            str(security_id),
            str(security_revision_id),
            instrument_id,
            identity_sha256,
            Json(source_lineage),
            checked,
        ),
    )
    cursor.execute(
        "SELECT security_id::text,security_revision_id::text,"
        "instrument_id,identity_sha256 "
        "FROM rt.realtime_desired_member_bindings "
        "WHERE intent_id=%s",
        (str(intent_id),),
    )
    stored = cursor.fetchone()
    if stored != (
        str(security_id),
        str(security_revision_id),
        instrument_id,
        identity_sha256,
    ):
        raise ValueError("desired member identity binding conflicts")
    cursor.execute(
        "INSERT INTO rt.realtime_desired_members ("
        "revision_id,security_id,ticker,member_kind,priority,enabled"
        ") VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        (
            str(revision_id),
            str(security_id),
            normalized_ticker,
            kind.value,
            int(intent[0]),
            bool(intent[1]),
        ),
    )
    cursor.execute(
        "INSERT INTO rt.realtime_desired_member_readiness ("
        "revision_id,security_id,member_kind,status,reasons,checked_at"
        ") VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        (
            str(revision_id),
            str(security_id),
            kind.value,
            str(intent[2]),
            list(intent[3]),
            intent[4],
        ),
    )
    cursor.execute(
        "UPDATE rt.realtime_equity_preparation_work SET "
        "security_id=%s,instrument_id=%s,updated_at=clock_timestamp() "
        "WHERE revision_id=%s AND intent_id=%s "
        "AND security_id IS NULL AND instrument_id IS NULL",
        (
            str(security_id),
            instrument_id,
            str(revision_id),
            str(intent_id),
        ),
    )
    cursor.execute(
        "SELECT intent.intent_id::text,intent.priority,intent.enabled,"
        "readiness.status,readiness.reasons,readiness.checked_at "
        "FROM rt.realtime_desired_member_intents AS intent "
        "JOIN rt.realtime_desired_intent_readiness AS readiness "
        "USING(intent_id) "
        "WHERE intent.revision_id=%s AND intent.ticker=%s "
        "AND intent.member_kind='OPTION_UNDERLYING'",
        (str(revision_id), normalized_ticker),
    )
    option_intent = cursor.fetchone()
    if option_intent is not None:
        option_intent_id = UUID(str(option_intent[0]))
        option_identity_sha256 = canonical_sha256({
            "version": "advanced-realtime-member-binding-v1",
            "intent_id": option_intent_id,
            "security_id": security_id,
            "security_revision_id": security_revision_id,
            "instrument_id": instrument_id,
        })
        cursor.execute(
            "INSERT INTO rt.realtime_desired_member_bindings ("
            "intent_id,revision_id,security_id,security_revision_id,"
            "instrument_id,identity_sha256,source_lineage,resolved_at"
            ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (intent_id) DO NOTHING",
            (
                str(option_intent_id),
                str(revision_id),
                str(security_id),
                str(security_revision_id),
                instrument_id,
                option_identity_sha256,
                Json({
                    **source_lineage,
                    "inherited_from_member_kind": kind.value,
                }),
                checked,
            ),
        )
        cursor.execute(
            "INSERT INTO rt.realtime_desired_members ("
            "revision_id,security_id,ticker,member_kind,priority,enabled"
            ") VALUES (%s,%s,%s,'OPTION_UNDERLYING',%s,%s) "
            "ON CONFLICT DO NOTHING",
            (
                str(revision_id),
                str(security_id),
                normalized_ticker,
                int(option_intent[1]),
                bool(option_intent[2]),
            ),
        )
        cursor.execute(
            "INSERT INTO rt.realtime_desired_member_readiness ("
            "revision_id,security_id,member_kind,status,reasons,checked_at"
            ") VALUES (%s,%s,'OPTION_UNDERLYING',%s,%s,%s) "
            "ON CONFLICT DO NOTHING",
            (
                str(revision_id),
                str(security_id),
                str(option_intent[3]),
                list(option_intent[4]),
                option_intent[5],
            ),
        )
    return DesiredMemberPreparation(
        revision_id=revision_id,
        security_id=security_id,
        ticker=normalized_ticker,
        kind=kind,
        enabled=bool(intent[1]),
        status=MemberPreparationStatus(str(intent[2])),
        reasons=tuple(str(reason) for reason in intent[3]),
        checked_at=_utc(
            intent[4],
            "stored desired intent readiness checked_at",
        ),
    )


def promote_prepared_desired_member_cursor(
    cursor,
    revision_id: UUID,
    *,
    ticker: str,
    kind: DesiredMemberKind,
    checked_at: datetime,
    calendar: OptionExchangeCalendar | None = None,
    runtime_catalog: AdvancedIngressRuntimeCatalog | None = None,
) -> DesiredStateReconciliation:
    checked = _utc(checked_at, "prepared member promotion checked_at")
    exchange = calendar or OptionExchangeCalendar("XNYS")
    revision = load_desired_state_revision_cursor(cursor, revision_id)
    catalog = runtime_catalog or load_advanced_ingress_runtime_catalog(
        DEFAULT_ADVANCED_INGRESS_RUNTIME_POLICY
    )
    target_count = len({
        member.ticker
        for member in revision.members
        if member.enabled
        and member.kind in {
            DesiredMemberKind.EQUITY,
            DesiredMemberKind.SECTOR_ETF,
        }
    })
    activation_at = exchange.session_open(
        revision.effective_session_date
    ) - timedelta(
        seconds=(
            catalog.profile_for_target(
                target_count
            ).settings.controller_warmup_seconds
        )
    )
    snapshot = _load_desired_snapshot_cursor(cursor, revision_id)
    if snapshot is None and checked >= activation_at:
        activated = reconcile_desired_state_revision_cursor(
            cursor,
            revision_id,
            materialized_at=activation_at,
            calendar=exchange,
            runtime_catalog=catalog,
            allow_partial_activation=True,
        )
        snapshot = activated.snapshot
        if snapshot is None:
            return activated
    preparation = set_desired_member_preparation_cursor(
        cursor,
        revision_id,
        ticker=ticker,
        kind=kind,
        status=MemberPreparationStatus.READY,
        checked_at=checked,
    )
    if preparation.security_id is None:
        raise ValueError(
            "prepared desired member identity is unresolved"
        )
    if snapshot is None:
        return DesiredStateReconciliation(
            state="ACTIVATION_PENDING",
            revision=revision,
            preparations=load_desired_member_preparations_cursor(
                cursor,
                revision_id,
            ),
            snapshot=None,
            reason_codes=(
                f"SESSION_ACTIVATION_PENDING:{activation_at.isoformat()}",
            ),
        )
    cursor.execute(
        "SELECT count(*) FROM rt.subscription_members AS member "
        "JOIN rt.instruments AS instrument "
        "ON instrument.instrument_id=member.instrument_id "
        "WHERE member.manifest_id=%s AND instrument.security_id=%s "
        "AND member.channel='AM'",
        (str(snapshot.manifest_id), str(preparation.security_id)),
    )
    if int(cursor.fetchone()[0]) == 1:
        return DesiredStateReconciliation(
            state="READY",
            revision=revision,
            preparations=load_desired_member_preparations_cursor(
                cursor,
                revision_id,
            ),
            snapshot=snapshot,
        )
    cursor.execute(
        "SELECT EXISTS ("
        "SELECT 1 FROM rt.advanced_ingress_plans "
        "WHERE session_date=%s"
        ")",
        (snapshot.session_date,),
    )
    session_plan_pinned = bool(cursor.fetchone()[0])
    target_session = (
        exchange.next_session_date(snapshot.session_date)
        if checked >= activation_at or session_plan_pinned
        else snapshot.session_date
    )
    promoted = record_desired_state_rollover_revision_cursor(
        cursor,
        DesiredStateChange(
            source_cohort_id=snapshot.cohort_id,
            source_slot_version=snapshot.slot_set_version,
            members=revision.members,
            enabled=revision.enabled,
        ),
        effective_session_date=target_session,
        recorded_at=checked,
        calendar=exchange,
    )
    cursor.execute(
        "UPDATE rt.realtime_desired_intent_readiness AS target_readiness "
        "SET status=source_readiness.status,"
        "reasons=source_readiness.reasons,"
        "checked_at=source_readiness.checked_at,"
        "updated_at=clock_timestamp() "
        "FROM rt.realtime_desired_member_intents AS target_intent,"
        "rt.realtime_desired_member_intents AS source_intent,"
        "rt.realtime_desired_intent_readiness AS source_readiness "
        "WHERE target_intent.revision_id=%s "
        "AND source_intent.revision_id=%s "
        "AND target_readiness.intent_id=target_intent.intent_id "
        "AND source_readiness.intent_id=source_intent.intent_id "
        "AND target_intent.ticker=source_intent.ticker "
        "AND target_intent.member_kind=source_intent.member_kind",
        (str(promoted.revision_id), str(revision_id)),
    )
    cursor.execute(
        "UPDATE rt.realtime_desired_member_readiness AS readiness "
        "SET status=intent_readiness.status,"
        "reasons=intent_readiness.reasons,"
        "checked_at=intent_readiness.checked_at,"
        "updated_at=clock_timestamp() "
        "FROM rt.realtime_desired_member_bindings AS binding "
        "JOIN rt.realtime_desired_intent_readiness AS intent_readiness "
        "ON intent_readiness.intent_id=binding.intent_id "
        "WHERE readiness.revision_id=%s "
        "AND binding.revision_id=readiness.revision_id "
        "AND binding.security_id=readiness.security_id",
        (str(promoted.revision_id),),
    )
    cursor.execute(
        "UPDATE rt.realtime_equity_preparation_work AS target "
        "SET status=CASE WHEN source.status='RUNNING' "
        "THEN 'RETRY' ELSE source.status END,"
        "security_id=source.security_id,"
        "instrument_id=source.instrument_id,"
        "watermark=source.watermark,coverage=source.coverage,"
        "source_lineage=source.source_lineage || %s::jsonb,"
        "attempts=source.attempts,"
        "failure_streak=source.failure_streak,"
        "next_retry_at=source.next_retry_at,"
        "lease_owner=NULL,lease_epoch=source.lease_epoch,"
        "lease_until=NULL,terminal_reason=source.terminal_reason,"
        "blocked_reason=source.blocked_reason,"
        "provider_request_count=source.provider_request_count,"
        "response_bytes=source.response_bytes,"
        "completed_at=source.completed_at,updated_at=clock_timestamp() "
        "FROM rt.realtime_equity_preparation_work AS source "
        "WHERE target.revision_id=%s AND source.revision_id=%s "
        "AND target.ticker=source.ticker "
        "AND target.member_kind=source.member_kind "
        "AND target.stage=source.stage",
        (
            Json({"carried_from_revision": str(revision_id)}),
            str(promoted.revision_id),
            str(revision_id),
        ),
    )
    promoted_activation_at = exchange.session_open(
        promoted.effective_session_date
    ) - timedelta(
        seconds=(
            catalog.profile_for_target(
                target_count
            ).settings.controller_warmup_seconds
        )
    )
    if checked < promoted_activation_at:
        return DesiredStateReconciliation(
            state="ACTIVATION_PENDING",
            revision=promoted,
            preparations=load_desired_member_preparations_cursor(
                cursor,
                promoted.revision_id,
            ),
            snapshot=None,
            reason_codes=(
                "SESSION_ACTIVATION_PENDING:"
                f"{promoted_activation_at.isoformat()}",
            ),
        )
    return reconcile_desired_state_revision_cursor(
        cursor,
        promoted.revision_id,
        materialized_at=checked,
        calendar=exchange,
        runtime_catalog=catalog,
        allow_partial_activation=True,
    )


def _load_desired_snapshot_cursor(
    cursor,
    revision_id: UUID,
) -> DesiredSessionSnapshot | None:
    cursor.execute(
        "SELECT session_date,manifest_id::text,cohort_id::text,"
        "slot_set_version,snapshot_sha256,materialized_at "
        "FROM rt.realtime_desired_session_snapshots WHERE revision_id=%s",
        (str(revision_id),),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return DesiredSessionSnapshot(
        revision_id=revision_id,
        session_date=row[0],
        manifest_id=UUID(str(row[1])),
        cohort_id=UUID(str(row[2])),
        slot_set_version=None if row[3] is None else int(row[3]),
        snapshot_sha256=str(row[4]),
        materialized_at=_utc(row[5], "stored desired snapshot materialized_at"),
    )


def _equity_instrument_ids_cursor(
    cursor,
    security_ids: dict[str, UUID],
) -> dict[str, int]:
    cursor.execute(
        "SELECT security_id::text,instrument_id FROM rt.instruments "
        "WHERE asset_class='EQUITY' AND security_id=ANY(%s::uuid[]) "
        "ORDER BY security_id,instrument_id",
        ([str(value) for value in security_ids.values()],),
    )
    rows_by_security: dict[UUID, list[int]] = {
        security_id: [] for security_id in security_ids.values()
    }
    for security_id, instrument_id in cursor.fetchall():
        rows_by_security.setdefault(UUID(str(security_id)), []).append(
            int(instrument_id)
        )
    result: dict[str, int] = {}
    for ticker, security_id in sorted(security_ids.items()):
        rows = rows_by_security[security_id]
        if len(rows) != 1:
            raise ValueError(
                f"desired member {ticker} has an ambiguous stored instrument"
            )
        result[ticker] = rows[0]
    return result


def reconcile_desired_state_revision_cursor(
    cursor,
    revision_id: UUID,
    *,
    materialized_at: datetime,
    calendar: OptionExchangeCalendar | None = None,
    runtime_catalog: AdvancedIngressRuntimeCatalog | None = None,
    allow_partial_activation: bool = False,
) -> DesiredStateReconciliation:
    materialized = _utc(materialized_at, "desired snapshot materialized_at")
    _lock_desired_revision_cursor(cursor, revision_id)
    revision = load_desired_state_revision_cursor(cursor, revision_id)
    preparations = load_desired_member_preparations_cursor(cursor, revision_id)
    if len(preparations) != len(revision.members):
        raise ValueError("desired state readiness is incomplete")
    existing = _load_desired_snapshot_cursor(cursor, revision_id)
    if existing is not None:
        warming = tuple(
            f"MEMBER_WARMING:{row.ticker}:{row.kind.value}"
            for row in preparations
            if row.enabled
            and row.status is MemberPreparationStatus.WARMING
        )
        return DesiredStateReconciliation(
            state="READY",
            revision=revision,
            preparations=preparations,
            snapshot=existing,
            reason_codes=warming,
        )
    if not revision.enabled:
        return DesiredStateReconciliation(
            state="DISABLED",
            revision=revision,
            preparations=preparations,
            snapshot=None,
        )
    exchange = calendar or OptionExchangeCalendar("XNYS")
    catalog = runtime_catalog or load_advanced_ingress_runtime_catalog(
        DEFAULT_ADVANCED_INGRESS_RUNTIME_POLICY
    )
    intended_target = len({
        member.ticker
        for member in revision.members
        if member.enabled
        and member.kind in {
            DesiredMemberKind.EQUITY,
            DesiredMemberKind.SECTOR_ETF,
        }
    })
    activation_at = exchange.session_open(
        revision.effective_session_date
    ) - timedelta(
        seconds=(
            catalog.profile_for_target(
                intended_target
            ).settings.controller_warmup_seconds
        )
    )
    warming_members = tuple(
        row
        for row in preparations
        if row.enabled and row.status is MemberPreparationStatus.WARMING
    )
    warming_reason_codes = tuple(
        f"MEMBER_WARMING:{row.ticker}:{row.kind.value}"
        for row in warming_members
    )
    source = _load_source_control_plane_cursor(cursor, revision.source_cohort_id)
    source_base, source_underlyings = _source_member_security_cursor(
        cursor,
        source,
    )
    failed_baseline = tuple(
        row
        for row in preparations
        if row.enabled
        and row.status is MemberPreparationStatus.FAILED
        and (
            (
                row.kind in {
                    DesiredMemberKind.EQUITY,
                    DesiredMemberKind.SECTOR_ETF,
                }
                and row.security_id in source_base
            )
            or (
                row.kind is DesiredMemberKind.OPTION_UNDERLYING
                and row.security_id in source_underlyings
            )
        )
    )
    if failed_baseline:
        reason_codes = tuple(
            f"BASELINE_MEMBER_FAILED:{row.ticker}:{row.kind.value}"
            for row in failed_baseline
        )
        return DesiredStateReconciliation(
            state="BLOCKED",
            revision=revision,
            preparations=preparations,
            snapshot=None,
            reason_codes=reason_codes,
        )
    if materialized < activation_at:
        return DesiredStateReconciliation(
            state="ACTIVATION_PENDING",
            revision=revision,
            preparations=preparations,
            snapshot=None,
            reason_codes=(
                f"SESSION_ACTIVATION_PENDING:{activation_at.isoformat()}",
            ),
        )
    if warming_members and not allow_partial_activation:
        return DesiredStateReconciliation(
            state="WARMING",
            revision=revision,
            preparations=preparations,
            snapshot=None,
            reason_codes=warming_reason_codes,
        )
    ready_base = tuple(
        row
        for row in preparations
        if row.enabled
        and row.status is MemberPreparationStatus.READY
        and row.security_id is not None
        and row.kind in {
            DesiredMemberKind.EQUITY,
            DesiredMemberKind.SECTOR_ETF,
        }
    )
    if not ready_base:
        raise ValueError("desired state has no READY Equity or sector ETF members")
    ready_options = tuple(
        row
        for row in preparations
        if row.enabled
        and row.status is MemberPreparationStatus.READY
        and row.security_id is not None
        and row.kind is DesiredMemberKind.OPTION_UNDERLYING
    )
    base_tickers = tuple(sorted(row.ticker for row in ready_base))
    option_tickers = tuple(sorted(row.ticker for row in ready_options))
    if not set(option_tickers).issubset(base_tickers):
        raise ValueError("READY Options underlyings require READY Equity membership")
    effective_at = exchange.session_open(revision.effective_session_date)
    runtime_configuration_sha256 = _runtime_configuration_sha256(
        source,
        universe_target=len(base_tickers),
        runtime_catalog=catalog,
    )
    capability_universe = UniverseManifest(
        version=f"advanced-equity-{len(base_tickers)}-am-v1",
        equity_tickers=base_tickers,
        option_underlyings=(),
    )
    capability = _fresh_capability_profile_cursor(
        cursor,
        source,
        desired_universe_sha256=capability_universe.sha256,
        desired_configuration_sha256=(
            runtime_configuration_sha256
        ),
        required_through=exchange.session_close(
            revision.effective_session_date
        ),
        allow_source_universe=(
            {row.security_id for row in ready_base} == source_base
        ),
    )
    if capability is None:
        return DesiredStateReconciliation(
            state="REFRESH_REQUIRED",
            revision=revision,
            preparations=preparations,
            snapshot=None,
            reason_codes=(
                f"CAPABILITY_REFRESH_REQUIRED:{CAPABILITY_REFRESH_OWNER}",
            ),
        )
    capability_profile_id, capability_profile_sha256 = capability
    security_ids = dict(revision.security_ids)
    equity_members = tuple(
        SubscriptionMember(
            asset_class=AssetClass.EQUITY,
            provider_symbol=row.ticker,
            underlying=row.ticker,
            channel=MarketChannel.MINUTE_AGGREGATE,
            priority=next(
                member.priority
                for member in revision.members
                if member.ticker == row.ticker
                and member.kind == row.kind
            ),
        )
        for row in ready_base
    )
    stored_options = (
        ()
        if revision.source_slot_version is None
        else _source_option_members_cursor(
            cursor,
            cohort_id=revision.source_cohort_id,
            version=revision.source_slot_version,
        )
    )
    ticker_by_security = {
        security_id: ticker for ticker, security_id in security_ids.items()
    }
    selected_options = tuple(
        row for row in stored_options
        if ticker_by_security.get(row.security_id) in option_tickers
    )
    if (
        option_tickers
        and revision.source_slot_version is not None
        and not selected_options
    ):
        raise ValueError("desired Options underlyings have no stored slot members")
    if any(row.purpose is SlotSetPurpose.RESERVE for row in selected_options):
        raise ValueError("C2 cannot generalize stored reserve-slot policy")
    option_members = tuple(
        SubscriptionMember(
            asset_class=AssetClass.OPTION,
            provider_symbol=row.contract_ticker,
            underlying=ticker_by_security[row.security_id],
            channel=row.channel,
            priority=row.priority,
            purpose=SubscriptionPurpose(row.purpose.value),
            catalog_version_id=row.catalog_version_id,
        )
        for row in selected_options
    )
    allocation_policy_sha256 = canonical_sha256({
        "version": "advanced-realtime-desired-allocation-v1",
        "revision_sha256": revision.revision_sha256,
        "selection_policy_sha256": revision.selection_policy_sha256,
    })
    snapshot_option_tickers = (
        option_tickers
        if revision.source_slot_version is not None
        else ()
    )
    snapshot_universe = UniverseManifest(
        version="advanced-realtime-desired-session-v1",
        equity_tickers=base_tickers,
        option_underlyings=snapshot_option_tickers,
    )
    manifest = SubscriptionManifest(
        version="advanced-realtime-desired-session-v1",
        session_date=revision.effective_session_date,
        universe=snapshot_universe,
        capability_profile_sha256=capability_profile_sha256,
        allocation_policy_sha256=allocation_policy_sha256,
        members=equity_members + option_members,
        maximum_option_quote_contracts=(
            1000 if snapshot_option_tickers else 0
        ),
    )
    instrument_ids = _equity_instrument_ids_cursor(
        cursor,
        {ticker: security_ids[ticker] for ticker in base_tickers},
    )
    if snapshot_option_tickers:
        manifest_id = option_universe_manifest_id(manifest)
        persist_option_universe_manifest_cursor(
            cursor,
            manifest,
            manifest_id=manifest_id,
            profile_id=capability_profile_id,
            effective_at=effective_at,
            equity_instrument_ids=instrument_ids,
        )
    else:
        manifest_id = equity_universe_manifest_id(manifest)
        persist_equity_universe_manifest_cursor(
            cursor,
            manifest,
            manifest_id=manifest_id,
            profile_id=capability_profile_id,
            effective_at=effective_at,
            equity_instrument_ids=instrument_ids,
        )
    cohort_id, _cohort_sha256 = persist_shadow_cohort_cursor(
        cursor,
        manifest_id=manifest_id,
        configuration_sha256=runtime_configuration_sha256,
        model_bundle_sha256=source.model_bundle_sha256,
        valuation_sha256=source.valuation_sha256,
        effective_at=effective_at,
    )
    options_enabled = bool(snapshot_option_tickers)
    options_tier = source.provider_tier if options_enabled else None
    persist_tier_cohort_binding_cursor(
        cursor,
        TierCohortBinding(
            cohort_id=cohort_id,
            profile_id=capability_profile_id,
            provider_tier=source.provider_tier,
            ready_state=(
                TierState.ADVANCED_READY
                if source.provider_tier is ProviderTier.MASSIVE_ADVANCED
                else TierState.DEVELOPER_READY
            ),
            source_manifest_sha256=(
                option_universe_manifest_sha256(manifest)
                if snapshot_option_tickers
                else manifest.sha256
            ),
            valuation_policy_sha256=source.valuation_sha256,
            capabilities=product_capabilities(
                source.provider_tier,
                options_tier,
            ),
            disabled_detectors=product_disabled_detectors(
                source.provider_tier,
                options_tier,
            ),
            effective_at=effective_at,
            options_enabled=options_enabled,
        ),
    )
    if cohort_id != revision.source_cohort_id:
        history_before = exchange.session_open(
            revision.effective_session_date
        )
        authorization_reference = (
            f"desired-preparation:{revision.revision_id}"
        )
        binding_sha256 = canonical_sha256({
            "version": "advanced-equity-preparation-history-v1",
            "target_cohort_id": cohort_id,
            "source_cohort_id": revision.source_cohort_id,
            "source_provider_tier": source.provider_tier.value,
            "history_before": history_before,
            "source_manifest_sha256": source.manifest_sha256,
            "authorization_reference": authorization_reference,
        })
        cursor.execute(
            "INSERT INTO rt.cohort_history_bindings ("
            "target_cohort_id,source_cohort_id,source_provider_tier,"
            "history_before,source_manifest_sha256,"
            "authorization_reference,binding_sha256"
            ") VALUES (%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (target_cohort_id) DO NOTHING",
            (
                str(cohort_id),
                str(revision.source_cohort_id),
                source.provider_tier.value,
                history_before,
                source.manifest_sha256,
                authorization_reference,
                binding_sha256,
            ),
        )
        cursor.execute(
            "SELECT binding_sha256 FROM rt.cohort_history_bindings "
            "WHERE target_cohort_id=%s",
            (str(cohort_id),),
        )
        if tuple(cursor.fetchone() or ()) != (binding_sha256,):
            raise ValueError(
                "desired preparation history binding conflicts"
            )
    slot_set_version = None
    if snapshot_option_tickers:
        slot_set = SlotSetVersion(
            cohort_id=cohort_id,
            version=1,
            kind=SlotSetKind.PRE_SESSION,
            effective_at=effective_at,
            policy_sha256=revision.selection_policy_sha256,
            members=tuple(
                SlotSetMember(
                    instrument_id=row.instrument_id,
                    channel=row.channel,
                    purpose=row.purpose,
                    priority=row.priority,
                    catalog_version_id=row.catalog_version_id,
                )
                for row in selected_options
            ),
        )
        persist_and_activate_slot_set_cursor(
            cursor,
            slot_set,
            activated_at=effective_at,
        )
        slot_set_version = slot_set.version
    snapshot_sha256 = canonical_sha256({
        "version": "advanced-realtime-desired-snapshot-v1",
        "revision_sha256": revision.revision_sha256,
        "session_date": revision.effective_session_date,
        "manifest_id": manifest_id,
        "cohort_id": cohort_id,
        "slot_set_version": slot_set_version,
    })
    snapshot = DesiredSessionSnapshot(
        revision_id=revision.revision_id,
        session_date=revision.effective_session_date,
        manifest_id=manifest_id,
        cohort_id=cohort_id,
        slot_set_version=slot_set_version,
        snapshot_sha256=snapshot_sha256,
        materialized_at=materialized,
    )
    cursor.execute(
        "INSERT INTO rt.realtime_desired_session_snapshots ("
        "revision_id,session_date,manifest_id,cohort_id,slot_set_version,"
        "snapshot_sha256,materialized_at) VALUES (%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (revision_id) DO NOTHING",
        (
            str(snapshot.revision_id),
            snapshot.session_date,
            str(snapshot.manifest_id),
            str(snapshot.cohort_id),
            snapshot.slot_set_version,
            snapshot.snapshot_sha256,
            snapshot.materialized_at,
        ),
    )
    stored_snapshot = _load_desired_snapshot_cursor(cursor, revision_id)
    if stored_snapshot != snapshot:
        raise ValueError("stored desired session snapshot conflicts with its identity")
    return DesiredStateReconciliation(
        state="READY",
        revision=revision,
        preparations=preparations,
        snapshot=stored_snapshot,
        reason_codes=warming_reason_codes,
    )


def apply_desired_state(
    connection,
    change: DesiredStateChange,
    *,
    observed_at: datetime,
    calendar: OptionExchangeCalendar | None = None,
) -> DesiredStateReconciliation:
    try:
        with connection.cursor() as cursor:
            revision = record_desired_state_revision_cursor(
                cursor,
                change,
                observed_at=observed_at,
                calendar=calendar,
            )
            result = reconcile_desired_state_revision_cursor(
                cursor,
                revision.revision_id,
                materialized_at=observed_at,
                calendar=calendar,
            )
        connection.commit()
        return result
    except Exception:
        connection.rollback()
        raise


def update_desired_member_preparation(
    connection,
    revision_id: UUID,
    *,
    ticker: str,
    kind: DesiredMemberKind,
    status: MemberPreparationStatus,
    checked_at: datetime,
    reasons: tuple[str, ...] = (),
) -> DesiredMemberPreparation:
    try:
        with connection.cursor() as cursor:
            if status is MemberPreparationStatus.READY:
                if reasons:
                    raise ValueError(
                        "READY desired member cannot carry failure reasons"
                    )
                reconciliation = promote_prepared_desired_member_cursor(
                    cursor,
                    revision_id,
                    ticker=ticker,
                    kind=kind,
                    checked_at=checked_at,
                )
                result = next(
                    row
                    for row in reconciliation.preparations
                    if row.ticker == ticker.strip().upper()
                    and row.kind is kind
                )
            else:
                result = set_desired_member_preparation_cursor(
                    cursor,
                    revision_id,
                    ticker=ticker,
                    kind=kind,
                    status=status,
                    checked_at=checked_at,
                    reasons=reasons,
                )
                reconcile_desired_state_revision_cursor(
                    cursor,
                    revision_id,
                    materialized_at=checked_at,
                )
        connection.commit()
        return result
    except Exception:
        connection.rollback()
        raise


def reconcile_desired_state(
    connection,
    revision_id: UUID,
    *,
    materialized_at: datetime,
    calendar: OptionExchangeCalendar | None = None,
) -> DesiredStateReconciliation:
    try:
        with connection.cursor() as cursor:
            result = reconcile_desired_state_revision_cursor(
                cursor,
                revision_id,
                materialized_at=materialized_at,
                calendar=calendar,
            )
        connection.commit()
        return result
    except Exception:
        connection.rollback()
        raise
