from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from uuid import UUID

from options.calendar import OptionExchangeCalendar
from psycopg2.extras import execute_values

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


def _load_source_control_plane_cursor(
    cursor,
    cohort_id: UUID,
) -> _SourceControlPlane:
    cursor.execute(
        "SELECT manifest.manifest_id::text,manifest.profile_id::text,"
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
        profile_id=UUID(str(row[1])),
        profile_sha256=str(row[2]),
        provider_tier=ProviderTier(str(row[3])),
        universe_sha256=str(row[4]),
        capability_config_sha256=str(row[5]),
        configuration_sha256=str(row[6]),
        model_bundle_sha256=str(row[7]),
        valuation_sha256=str(row[8]),
    )


def _fresh_capability_profile_cursor(
    cursor,
    source: _SourceControlPlane,
    *,
    desired_universe_sha256: str,
    required_through: datetime,
) -> tuple[UUID, str] | None:
    required = _utc(required_through, "capability required_through")
    cursor.execute(
        "SELECT profile_id::text,profile_sha256 "
        "FROM rt.capability_profiles "
        "WHERE provider_tier=%s AND config_sha256=%s "
        "AND universe_sha256=ANY(%s) AND status='PASS' "
        "AND expires_at>=%s "
        "ORDER BY (universe_sha256=%s) DESC,"
        "expires_at DESC,checked_at DESC,profile_id DESC "
        "LIMIT 1",
        (
            source.provider_tier.value,
            source.capability_config_sha256,
            [desired_universe_sha256, source.universe_sha256],
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
        "SELECT member.ticker,member.member_kind,member.enabled,"
        "member.priority,member.security_id::text "
        "FROM rt.realtime_desired_members AS member "
        "WHERE member.revision_id=%s "
        "ORDER BY member.ticker,member.member_kind",
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
    security_ids = {
        str(member[0]): UUID(str(member[4]))
        for member in member_rows
    }
    if len(security_ids) != len({member.ticker for member in members}):
        raise ValueError("desired state revision member identities diverged")
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
        "SELECT member.security_id::text,member.ticker,member.member_kind,"
        "member.enabled,readiness.status,readiness.reasons,"
        "readiness.checked_at "
        "FROM rt.realtime_desired_members AS member "
        "JOIN rt.realtime_desired_member_readiness AS readiness "
        "ON readiness.revision_id=member.revision_id "
        "AND readiness.security_id=member.security_id "
        "AND readiness.member_kind=member.member_kind "
        "WHERE member.revision_id=%s "
        "ORDER BY member.ticker,member.member_kind",
        (str(revision_id),),
    )
    return tuple(
        DesiredMemberPreparation(
            revision_id=revision_id,
            security_id=UUID(str(row[0])),
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
    source_base: set[UUID],
    source_underlyings: set[UUID],
    ready_option_securities: set[UUID],
    checked_at: datetime,
) -> None:
    member_values = []
    readiness_values = []
    for member in revision.members:
        security_id = security_ids[member.ticker]
        member_values.append((
            str(revision.revision_id),
            str(security_id),
            member.ticker,
            member.kind.value,
            member.priority,
            member.enabled,
        ))
        ready = (
            not member.enabled
            or (
                member.kind in {
                    DesiredMemberKind.EQUITY,
                    DesiredMemberKind.SECTOR_ETF,
                }
                and security_id in source_base
            )
            or (
                member.kind is DesiredMemberKind.OPTION_UNDERLYING
                and security_id in source_underlyings
                and security_id in ready_option_securities
            )
        )
        readiness_values.append((
            str(revision.revision_id),
            str(security_id),
            member.kind.value,
            (
                MemberPreparationStatus.READY.value
                if ready
                else MemberPreparationStatus.WARMING.value
            ),
            [],
            checked_at,
        ))
    execute_values(
        cursor,
        "INSERT INTO rt.realtime_desired_members ("
        "revision_id,security_id,ticker,member_kind,priority,enabled"
        ") VALUES %s ON CONFLICT DO NOTHING",
        member_values,
        page_size=_DESIRED_MEMBER_BATCH_SIZE,
    )
    execute_values(
        cursor,
        "INSERT INTO rt.realtime_desired_member_readiness ("
        "revision_id,security_id,member_kind,status,reasons,checked_at"
        ") VALUES %s ON CONFLICT DO NOTHING",
        readiness_values,
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
    security_ids, _instrument_ids = _stored_security_identities_cursor(
        cursor,
        change.members,
        observed_at=observed,
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
    cursor.execute(
        "SELECT count(*) FROM rt.realtime_desired_session_snapshots "
        "WHERE revision_id=%s",
        (str(revision_id),),
    )
    if int(cursor.fetchone()[0]) != 0:
        raise ValueError(
            "desired member readiness cannot change after immutable snapshot materialization"
        )
    cursor.execute(
        "UPDATE rt.realtime_desired_member_readiness AS readiness "
        "SET status=%s,reasons=%s,checked_at=%s,updated_at=clock_timestamp() "
        "FROM rt.realtime_desired_members AS member "
        "WHERE readiness.revision_id=%s "
        "AND readiness.security_id=member.security_id "
        "AND readiness.member_kind=member.member_kind "
        "AND member.revision_id=%s AND member.ticker=%s "
        "AND member.member_kind=%s "
        "RETURNING readiness.security_id::text,member.enabled",
        (
            status.value,
            list(normalized_reasons),
            checked,
            str(revision_id),
            str(revision_id),
            normalized_ticker,
            kind.value,
        ),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("desired member readiness target is unavailable")
    return DesiredMemberPreparation(
        revision_id=revision_id,
        security_id=UUID(str(row[0])),
        ticker=normalized_ticker,
        kind=kind,
        enabled=bool(row[1]),
        status=status,
        reasons=normalized_reasons,
        checked_at=checked,
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
) -> DesiredStateReconciliation:
    materialized = _utc(materialized_at, "desired snapshot materialized_at")
    _lock_desired_revision_cursor(cursor, revision_id)
    revision = load_desired_state_revision_cursor(cursor, revision_id)
    preparations = load_desired_member_preparations_cursor(cursor, revision_id)
    if len(preparations) != len(revision.members):
        raise ValueError("desired state readiness is incomplete")
    existing = _load_desired_snapshot_cursor(cursor, revision_id)
    if existing is not None:
        return DesiredStateReconciliation(
            state="READY",
            revision=revision,
            preparations=preparations,
            snapshot=existing,
        )
    if not revision.enabled:
        return DesiredStateReconciliation(
            state="DISABLED",
            revision=revision,
            preparations=preparations,
            snapshot=None,
        )
    warming_members = tuple(
        row
        for row in preparations
        if row.enabled and row.status is MemberPreparationStatus.WARMING
    )
    if warming_members:
        reason_codes = tuple(
            f"MEMBER_WARMING:{row.ticker}:{row.kind.value}"
            for row in warming_members
        )
        return DesiredStateReconciliation(
            state="WARMING",
            revision=revision,
            preparations=preparations,
            snapshot=None,
            reason_codes=reason_codes,
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
    ready_base = tuple(
        row
        for row in preparations
        if row.enabled
        and row.status is MemberPreparationStatus.READY
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
        and row.kind is DesiredMemberKind.OPTION_UNDERLYING
    )
    base_tickers = tuple(sorted(row.ticker for row in ready_base))
    option_tickers = tuple(sorted(row.ticker for row in ready_options))
    if not set(option_tickers).issubset(base_tickers):
        raise ValueError("READY Options underlyings require READY Equity membership")
    exchange = calendar or OptionExchangeCalendar("XNYS")
    effective_at = exchange.session_open(revision.effective_session_date)
    capability_universe = UniverseManifest(
        version=f"advanced-equity-{len(base_tickers)}-am-v1",
        equity_tickers=base_tickers,
        option_underlyings=(),
    )
    capability = _fresh_capability_profile_cursor(
        cursor,
        source,
        desired_universe_sha256=capability_universe.sha256,
        required_through=exchange.session_close(
            revision.effective_session_date
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
    universe = UniverseManifest(
        version="advanced-realtime-desired-session-v1",
        equity_tickers=base_tickers,
        option_underlyings=option_tickers,
    )
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
    if option_tickers and not selected_options:
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
    manifest = SubscriptionManifest(
        version="advanced-realtime-desired-session-v1",
        session_date=revision.effective_session_date,
        universe=universe,
        capability_profile_sha256=capability_profile_sha256,
        allocation_policy_sha256=allocation_policy_sha256,
        members=equity_members + option_members,
        maximum_option_quote_contracts=1000 if option_tickers else 0,
    )
    instrument_ids = _equity_instrument_ids_cursor(
        cursor,
        {ticker: security_ids[ticker] for ticker in base_tickers},
    )
    if option_tickers:
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
        configuration_sha256=source.configuration_sha256,
        model_bundle_sha256=source.model_bundle_sha256,
        valuation_sha256=source.valuation_sha256,
        effective_at=effective_at,
    )
    options_enabled = bool(option_tickers)
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
                if option_tickers
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
    slot_set_version = None
    if option_tickers:
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
