"""Bounded reads of immutable Equity history and its explicit source binding."""
from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from uuid import UUID

from options.calendar import OptionExchangeCalendar

from .contracts import canonical_sha256
from .equity_bar_adapter import (
    MAXIMUM_HISTORY_BINDING_DEPTH,
    RealtimeEquityBarFact,
)
from .equity_interval_finalizer import _bar_revision
from .preflight import ProviderTier


class EquityHistoryIdentityError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class EquityHistoryScope:
    cohort_id: UUID
    provider_tier: ProviderTier
    history_before: datetime
    binding_sha256: str

    def __post_init__(self) -> None:
        if (
            self.history_before.tzinfo is None
            or self.history_before.utcoffset() is None
            or len(self.binding_sha256) != 64
            or any(value not in "0123456789abcdef" for value in self.binding_sha256)
            or self.provider_tier not in {
                ProviderTier.MASSIVE_ADVANCED, ProviderTier.MASSIVE_DEVELOPER,
            }
        ):
            raise EquityHistoryIdentityError("invalid Equity history scope")

    def matches(self, row: Mapping) -> bool:
        return (
            str(row["cohort_id"]) == str(self.cohort_id)
            and row["provider_tier"] == self.provider_tier.value
            and row["bar_end"] < self.history_before
        )

    def payload(self) -> dict[str, str]:
        return {
            "cohort_id": str(self.cohort_id),
            "provider_tier": self.provider_tier.value,
            "history_before": self.history_before.isoformat(),
            "binding_sha256": self.binding_sha256,
        }

    @classmethod
    def from_payload(cls, value: Mapping) -> EquityHistoryScope:
        if set(value) != {"cohort_id", "provider_tier", "history_before", "binding_sha256"}:
            raise EquityHistoryIdentityError("Equity history scope fields diverged")
        return cls(
            UUID(str(value["cohort_id"])),
            ProviderTier(value["provider_tier"]),
            datetime.fromisoformat(str(value["history_before"])),
            str(value["binding_sha256"]),
        )


@dataclass(frozen=True, slots=True)
class EquityHistoryRead:
    rows: tuple[dict, ...]
    binding_sha256: str | None

    @property
    def lineage(self) -> dict[str, object]:
        return {
            "binding_sha256": self.binding_sha256,
            "bars": [
                {
                    "bar_revision_id": str(row["bar_revision_id"]),
                    "cohort_id": str(row["cohort_id"]),
                    "provider_tier": str(row["provider_tier"]),
                    "payload_sha256": str(row["payload_sha256"]),
                }
                for row in self.rows
            ],
        }

    @property
    def facts(self) -> tuple[RealtimeEquityBarFact, ...]:
        return tuple(
            RealtimeEquityBarFact(
                cohort_id=UUID(str(row["cohort_id"])),
                provider_tier=ProviderTier(row["provider_tier"]),
                instrument_id=int(row["instrument_id"]),
                revision=_bar_revision(row),
                journal_generation=(
                    UUID(str(row["journal_generation"]))
                    if row["journal_generation"] is not None else None
                ),
                stream_session_id=(
                    UUID(str(row["stream_session_id"]))
                    if row["stream_session_id"] is not None else None
                ),
                source_offset=row["source_offset"],
            )
            for row in self.rows
        )


def equity_history_coordinates(
    calendar: OptionExchangeCalendar,
    interval: str,
    session: date,
) -> tuple[tuple[datetime, datetime], ...]:
    opened = calendar.session_open(session)
    closed = calendar.session_close(session)
    if interval in {"1wk", "1mo"}:
        start = (
            session - timedelta(days=session.weekday())
            if interval == "1wk"
            else session.replace(day=1)
        )
        opened = calendar.session_open(calendar.session_on_or_after(start))
    if interval in {"1d", "1wk", "1mo"}:
        return ((opened, closed),)
    seconds = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600}
    duration = timedelta(seconds=seconds[interval])
    coordinates = []
    while opened < closed:
        end = min(opened + duration, closed)
        coordinates.append((opened, end))
        opened = end
    return tuple(coordinates)


def _history_scope_cursor(cursor, cohort_id: UUID):
    cursor.execute(
        "SELECT profile.provider_tier AS target_provider_tier,"
        "binding.source_cohort_id,binding.source_provider_tier,"
        "binding.history_before,binding.source_manifest_sha256,"
        "binding.binding_sha256,binding.created_at AS binding_created_at,"
        "source_manifest.manifest_sha256 AS actual_source_manifest_sha256,"
        "source_profile.provider_tier AS actual_source_provider_tier "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
        "JOIN rt.capability_profiles AS profile USING(profile_id) "
        "LEFT JOIN rt.cohort_history_bindings AS binding "
        "ON binding.target_cohort_id=cohort.cohort_id "
        "LEFT JOIN rt.cohorts AS source "
        "ON source.cohort_id=binding.source_cohort_id "
        "LEFT JOIN rt.subscription_manifests AS source_manifest "
        "ON source_manifest.manifest_id=source.manifest_id "
        "LEFT JOIN rt.capability_profiles AS source_profile "
        "ON source_profile.profile_id=source_manifest.profile_id "
        "WHERE cohort.cohort_id=%s",
        (str(cohort_id),),
    )
    scope = cursor.fetchone()
    if scope is None:
        raise EquityHistoryIdentityError("Equity history target cohort is absent")
    return scope


def _history_binding_scope_cursor(cursor, cohort_id: UUID):
    cursor.execute(
        "SELECT binding.source_cohort_id,binding.source_provider_tier,"
        "binding.history_before,binding.source_manifest_sha256,"
        "binding.binding_sha256,binding.created_at AS binding_created_at,"
        "source_manifest.manifest_sha256 AS actual_source_manifest_sha256 "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
        "LEFT JOIN rt.cohort_history_bindings AS binding "
        "ON binding.target_cohort_id=cohort.cohort_id "
        "LEFT JOIN rt.cohorts AS source "
        "ON source.cohort_id=binding.source_cohort_id "
        "LEFT JOIN rt.subscription_manifests AS source_manifest "
        "ON source_manifest.manifest_id=source.manifest_id "
        "WHERE cohort.cohort_id=%s",
        (str(cohort_id),),
    )
    scope = cursor.fetchone()
    if scope is None:
        raise EquityHistoryIdentityError("Equity history target cohort is absent")
    return scope


def validate_equity_history_scope_chain_cursor(
    cursor,
    *,
    cohort_id: UUID,
    checked_at: datetime,
    expected_chain,
) -> tuple[EquityHistoryScope, ...]:
    if (
        checked_at.tzinfo is None
        or checked_at.utcoffset() is None
        or not 1 < len(expected_chain) <= MAXIMUM_HISTORY_BINDING_DEPTH
    ):
        raise ValueError("Equity history scope chain validation is invalid")
    expected = tuple(
        EquityHistoryScope.from_payload(value) for value in expected_chain
    )
    scope = _history_binding_scope_cursor(cursor, cohort_id)
    visited = {cohort_id}
    cutoff = None
    for required in expected:
        if (
            scope["source_cohort_id"] is None
            or scope["binding_created_at"] > checked_at
        ):
            raise EquityHistoryIdentityError(
                "Equity history scope chain is incomplete"
            )
        source_id = UUID(str(scope["source_cohort_id"]))
        if (
            scope["source_manifest_sha256"]
            != scope["actual_source_manifest_sha256"]
            or source_id in visited
        ):
            raise EquityHistoryIdentityError(
                "Equity history source identity diverged"
            )
        cutoff = (
            min(cutoff, scope["history_before"])
            if cutoff is not None
            else scope["history_before"]
        )
        actual = EquityHistoryScope(
            source_id,
            ProviderTier(scope["source_provider_tier"]),
            cutoff,
            str(scope["binding_sha256"]),
        )
        if actual != required:
            raise EquityHistoryIdentityError(
                "Equity history scope chain changed"
            )
        visited.add(source_id)
        scope = _history_binding_scope_cursor(cursor, source_id)
    if (
        scope["source_cohort_id"] is not None
        and scope["binding_created_at"] <= checked_at
    ):
        raise EquityHistoryIdentityError(
            "Equity history scope chain is truncated"
        )
    return expected


def resolve_equity_history_scopes_cursor(
    cursor, *, cohort_id: UUID, checked_at: datetime,
) -> tuple[str, tuple[EquityHistoryScope, ...]]:
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        raise ValueError("Equity history scope clock must be timezone-aware")
    scope = _history_scope_cursor(cursor, cohort_id)
    target_tier = str(scope["target_provider_tier"])
    visited = {cohort_id}
    histories = []
    cutoff = None
    for _depth in range(MAXIMUM_HISTORY_BINDING_DEPTH):
        if (
            scope["source_cohort_id"] is None
            or scope["binding_created_at"] > checked_at
        ):
            break
        source_id = UUID(str(scope["source_cohort_id"]))
        if (
            scope["source_manifest_sha256"] != scope["actual_source_manifest_sha256"]
            or scope["source_provider_tier"] != scope["actual_source_provider_tier"]
            or source_id in visited
        ):
            raise EquityHistoryIdentityError("Equity history source identity diverged")
        visited.add(source_id)
        cutoff = min(cutoff, scope["history_before"]) if cutoff is not None else scope["history_before"]
        histories.append(EquityHistoryScope(
            source_id, ProviderTier(scope["source_provider_tier"]), cutoff,
            str(scope["binding_sha256"]),
        ))
        scope = _history_scope_cursor(cursor, source_id)
    if (
        scope["source_cohort_id"] is not None
        and scope["binding_created_at"] <= checked_at
    ):
        raise EquityHistoryIdentityError(
            "Equity history binding depth exceeds "
            f"{MAXIMUM_HISTORY_BINDING_DEPTH}"
        )
    return target_tier, tuple(histories)


def load_equity_history_cursor(
    cursor,
    *,
    cohort_id: UUID,
    security_id: UUID,
    ticker: str,
    interval: str,
    adjusted: bool,
    sessions: tuple[date, ...],
    checked_at: datetime,
    source_kind: str | None = None,
    adjustments_through: date | None = None,
) -> EquityHistoryRead:
    """Resolve the explicit bindings, preserving every causal cutoff."""
    if (
        not sessions
        or len(sessions) > 1_000
        or tuple(sorted(set(sessions))) != sessions
        or checked_at.tzinfo is None
        or checked_at.utcoffset() is None
        or interval not in {"1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"}
        or (adjustments_through is not None and adjustments_through < sessions[-1])
    ):
        raise ValueError("invalid bounded Equity history read")
    target_tier, histories = resolve_equity_history_scopes_cursor(
        cursor, cohort_id=cohort_id, checked_at=checked_at,
    )
    predicates = ["(revision.cohort_id=%s AND revision.provider_tier=%s)"]
    parameters = [str(cohort_id), target_tier]
    for history in histories:
        predicates.append(
            "(revision.cohort_id=%s AND revision.provider_tier=%s AND revision.bar_end<%s)"
        )
        parameters.extend((
            str(history.cohort_id), history.provider_tier.value, history.history_before,
        ))
    binding_hashes = [history.binding_sha256 for history in histories]
    scope_sql = "(" + " OR ".join(predicates) + ")"
    revision_sql = (
        "AND revision.bar_revision_id=current.bar_revision_id "
        "AND revision.observed_at=current.observed_at "
        if source_kind is None else "AND revision.source_kind=%s "
    )
    if source_kind is not None:
        parameters.append(source_kind)
    weekly_revalidation = (
        "EXISTS (SELECT 1 FROM rt.realtime_equity_preparation_chunks AS chunk "
        "JOIN rt.realtime_desired_revisions AS desired USING(revision_id) "
        "WHERE desired.source_cohort_id=revision.cohort_id "
        "AND chunk.security_id=revision.security_id "
        "AND chunk.committed_at=revision.observed_at "
        "AND chunk.created_at<=%s "
        "AND chunk.stage='ADJUSTED_DAILY' "
        "AND chunk.fact_count BETWEEN 1 AND 1000 "
        "AND chunk.source_lineage->>'coordinate_contract'='MASSIVE_WEEK_SUNDAY_START_V2' "
        "AND chunk.source_lineage->>'whole_interval_revalidated'='true' "
        "AND chunk.source_lineage->>'endpoint_class'='STOCKS_AGGREGATES_1WK' "
        "AND revision.source_kind='NATIVE_REST')"
        if interval == "1wk" and adjusted else "FALSE"
    )
    cursor.execute(
        "SELECT DISTINCT ON (revision.cohort_id,revision.bar_start) "
        "revision.*,current.updated_at,"
        + weekly_revalidation + " AS weekly_coordinate_revalidated,"
        +
        "EXISTS (SELECT 1 FROM ref.corporate_action_revisions AS action "
        "WHERE action.security_id=revision.security_id "
        "AND action.action_type='SPLIT' "
        "AND action.effective_date>revision.session_date "
        "AND action.effective_date<=%s AND action.received_at<=%s "
        "AND NOT EXISTS (SELECT 1 FROM ref.corporate_action_revisions AS prior "
        "WHERE prior.security_id=action.security_id "
        "AND prior.source=action.source AND prior.source_key=action.source_key "
        "AND prior.payload_sha256=action.payload_sha256 "
        "AND prior.received_at<=revision.observed_at)) "
        "AS stale_adjustment_lineage "
        "FROM rt.bar_revisions AS revision "
        "JOIN rt.bar_catalog AS catalog "
        "ON catalog.bar_revision_id=revision.bar_revision_id "
        "AND catalog.observed_at=revision.observed_at "
        "JOIN rt.bar_current AS current "
        "ON revision.cohort_id=current.cohort_id "
        "AND revision.provider_tier=current.provider_tier "
        "AND revision.instrument_id=current.instrument_id "
        "AND revision.interval=current.interval "
        "AND revision.adjustment_basis=current.adjustment_basis "
        "AND revision.session_scope=current.session_scope "
        "AND revision.bar_start=current.bar_start "
        "AND revision.bar_end=current.bar_end "
        "WHERE " + scope_sql + " " + revision_sql
        + "AND revision.security_id=%s AND revision.ticker=%s "
        "AND revision.interval=%s AND revision.session_scope='RTH' "
        "AND revision.adjustment_basis=%s "
        "AND revision.session_date=ANY(%s) AND revision.is_final "
        "AND revision.bar_end<=%s "
        "AND revision.observed_at<=%s AND revision.created_at<=%s "
        "AND current.updated_at<=%s "
        "AND (revision.replay_available_at IS NULL "
        "OR revision.replay_available_at<=%s) "
        "AND (revision.provider_published_at IS NULL "
        "OR revision.provider_published_at<=%s) "
        "ORDER BY revision.cohort_id,revision.bar_start,"
        "revision.observed_at DESC,revision.bar_revision_id",
        (
            *((checked_at,) if interval == "1wk" and adjusted else ()),
            adjustments_through or sessions[-1], checked_at, *parameters,
            str(security_id), ticker, interval,
            "ADJUSTED" if adjusted else "RAW_UNADJUSTED",
            list(sessions), checked_at, checked_at, checked_at,
            checked_at, checked_at, checked_at,
        ),
    )
    selected = {}
    for fetched in cursor.fetchall():
        row = dict(fetched)
        coordinate = (row["bar_start"], row["bar_end"])
        previous = selected.get(coordinate)
        if previous is not None:
            if adjusted and row["stale_adjustment_lineage"] != previous["stale_adjustment_lineage"]:
                if previous["stale_adjustment_lineage"]:
                    selected[coordinate] = row
                continue
            revalidated = bool(row.get("weekly_coordinate_revalidated"))
            previous_revalidated = bool(previous.get("weekly_coordinate_revalidated"))
            if interval == "1wk" and adjusted and revalidated != previous_revalidated:
                identity = ("security_id", "ticker", "session_date")
                if any(previous[key] != row[key] for key in identity):
                    raise EquityHistoryIdentityError("weekly correction identity diverged")
                if revalidated:
                    selected[coordinate] = row
                continue
            values = (
                "security_id", "ticker", "session_date", "source_kind",
                "open_price", "high_price", "low_price", "close_price",
                "volume", "vwap", "transaction_count", "payload_sha256",
            )
            if any(previous[key] != row[key] for key in values):
                raise EquityHistoryIdentityError(
                    "Equity history coordinate has divergent sources"
                )
            if UUID(str(previous["cohort_id"])) == cohort_id:
                continue
        selected[coordinate] = row
    return EquityHistoryRead(
        rows=tuple(selected[key] for key in sorted(selected)),
        binding_sha256=(
            None if not binding_hashes else binding_hashes[0]
            if len(binding_hashes) == 1
            else canonical_sha256({"binding_chain": binding_hashes})
        ),
    )
