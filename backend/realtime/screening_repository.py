"""Target-local loader and atomic offline screening generation producer."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from typing import Callable, Container, Iterable
from uuid import UUID

import exchange_calendars

from .database_access import RealtimeDatabaseRole
from .equity_portal_history import build_realtime_equity_portal_history_plan
from .portal_projection_repository import persist_screening_generation_cursor
from .preflight import ProviderTier
from .screening_projection import RealtimeScreeningInput, produce_realtime_screening
from .worker_database import RealtimeProcessDatabase

MAX_SOURCE_LINEAGE_ROUNDS = 8
BAR_VISIBILITY_CLOCKS = 5

# A bar is visible under (cutoff, known_by) when the system had recorded it by `known_by` (observed_at,
# created_at, replay_available_at, provider_published_at) and it ended by `cutoff` (the market boundary).
# Every clock is an immutable column of an immutable row (rt.bar_revisions rejects UPDATE and DELETE), so
# visibility only grows when either clock advances: visible(c0, k0) is a subset of visible(c1, k1) for
# c0 <= c1 and k0 <= k1. The default known_by is the cutoff, the single clock every caller used before.
_BAR_VISIBLE_AT = (
    "observed_at<=%s AND created_at<=%s "
    "AND (replay_available_at IS NULL OR replay_available_at<=%s) "
    "AND (provider_published_at IS NULL OR provider_published_at<=%s) AND bar_end<=%s "
)
# Given visibility under the new clocks, "not visible under the old ones" is exactly "some clock lies after
# its old bound". observed_at, created_at and bar_end are NOT NULL; a NULL optional clock never blocks
# visibility, so `clock>%s` (NULL is not true) never makes such a row newly visible. bar_end stays a clause of
# its own: with known_by after the cutoff a bar can end after the old cutoff yet have been recorded before the
# old known_by.
_BAR_NOT_VISIBLE_AT = (
    "AND (observed_at>%s OR created_at>%s OR replay_available_at>%s "
    "OR provider_published_at>%s OR bar_end>%s) "
)


@dataclass(frozen=True)
class BarClocks:
    """The two clocks of bar visibility: `cutoff` bounds bar_end (the market boundary), `known_by` bounds
    when the system recorded the bar. known_by is not before the cutoff."""

    cutoff: datetime
    known_by: datetime

    def params(self) -> tuple:
        """In the order of the visibility predicates: observed_at, created_at, replay_available_at,
        provider_published_at (known_by), then bar_end (cutoff)."""
        return (*((self.known_by,) * (BAR_VISIBILITY_CLOCKS - 1)), self.cutoff)


@dataclass(frozen=True)
class BarScope:
    """Everything the bar scan depends on except the cutoff."""

    cohort_id: UUID
    provider_tier: str
    tickers: tuple[str, ...]
    include_portal_history: bool
    history_params: tuple


BAR_IDENTITY_FIELDS = (
    "cohort", "tier", "tickers", "portal_history", "window", "strategy_details", "session",
    "source_generation", "manifest_id", "manifest_sha256", "members",
)


@dataclass(frozen=True)
class BarRequest:
    """What a bar loader receives. `identity` (ordered as BAR_IDENTITY_FIELDS) is everything except the
    clocks that defines the retained bar set: reuse across cutoffs and known_by values requires an equal
    identity. `slot` groups the identities of one cohort, tier and load flavour so a successor replaces its
    predecessor."""

    scope: BarScope
    cutoff: datetime
    known_by: datetime
    identity: tuple
    slot: tuple

    @property
    def clocks(self) -> BarClocks:
        return BarClocks(self.cutoff, self.known_by)


@lru_cache(maxsize=16)
def screening_history_window(session: date, *, include_portal_history: bool) -> tuple[date, tuple]:
    """The first daily session and the history-window parameters shared by every bar query. A pure function
    of the session (the portal plan costs about 130 ms), so it is computed once per session."""
    calendar = exchange_calendars.get_calendar("XNYS")
    if not calendar.is_session(str(session)):
        raise ValueError("Screening requires an exchange session")
    start = calendar.session_offset(str(session), -282).date()
    nominal_start = calendar.session_offset(str(session), -19).date()
    raw_start = calendar.session_offset(str(session), -89).date()
    portal_plan = (
        build_realtime_equity_portal_history_plan(session)
        if include_portal_history else None
    )
    from equity.screening_hourly import hourly_slots
    slots = hourly_slots(calendar, calendar.session_close(str(session)).to_pydatetime())
    history_params = (
        (start, session, portal_plan.weekly_start, portal_plan.weekly_end,
         portal_plan.monthly_start, session, raw_start, session) if include_portal_history else
        (start, session, nominal_start, session, slots[0][0], slots[-1][1])
    )
    return start, history_params


def bar_history_predicate(include_portal_history: bool) -> str:
    return (
        "AND ((adjustment_basis='ADJUSTED' AND interval='1d' "
        "AND session_date BETWEEN %s AND %s) OR (adjustment_basis='ADJUSTED' "
        "AND interval='1wk' AND session_date BETWEEN %s AND %s) OR (adjustment_basis='ADJUSTED' "
        "AND interval='1mo' AND session_date BETWEEN %s AND %s) "
        "OR (adjustment_basis='RAW_UNADJUSTED' "
        "AND interval IN ('1m','5m','15m','30m','1h','1d') AND session_date BETWEEN %s AND %s)) "
        if include_portal_history else
        "AND ((interval='1d' AND adjustment_basis='ADJUSTED' AND session_date BETWEEN %s AND %s) "
        "OR (interval='1d' AND adjustment_basis='RAW_UNADJUSTED' AND session_date BETWEEN %s AND %s) "
        "OR (interval='1h' AND adjustment_basis='RAW_UNADJUSTED' AND bar_start>=%s AND bar_end<=%s)) "
    )


def source_precedence_case(include_portal_history: bool) -> str:
    """Higher wins; the winner of a bar identity orders by this DESC, then observed_at DESC, created_at DESC,
    bar_revision_id."""
    return (
        "CASE "
        "WHEN adjustment_basis='ADJUSTED' AND interval IN ('1d','1wk','1mo') AND source_kind='NATIVE_REST' THEN 5 "
        "WHEN adjustment_basis='RAW_UNADJUSTED' AND interval IN ('1m','5m') AND source_kind='NATIVE_REST' THEN 5 "
        "WHEN adjustment_basis='RAW_UNADJUSTED' AND interval IN ('15m','30m','1h','1d') AND source_kind='DERIVED' THEN 5 "
        "WHEN source_kind='RECONCILED' THEN 4 WHEN source_kind='NATIVE_REST' THEN 3 "
        "WHEN source_kind='DERIVED' THEN 2 ELSE 1 END"
        if include_portal_history else
        "CASE source_kind WHEN 'RECONCILED' THEN 4 WHEN 'NATIVE_REST' THEN 3 "
        "WHEN 'DERIVED' THEN 2 ELSE 1 END"
    )


def bar_columns(include_portal_history: bool) -> str:
    extra = (
        "availability_mode, vwap, transaction_count, ingestion_segment_id, "
        "journal_generation, stream_session_id, source_offset, provider_published_at, "
        if include_portal_history else ""
    )
    return (
        "bar_revision_id, cohort_id, provider_tier, security_id, instrument_id, ticker, "
        "interval, session_date, session_scope, adjustment_basis, bar_start, bar_end, "
        "open_price::float8 AS open, high_price::float8 AS high, low_price::float8 AS low, "
        "close_price::float8 AS close, volume::float8, source_kind, source_revision_ids, "
        "observed_at, replay_available_at, is_final, payload_sha256, " + extra + "created_at"
    )


def _bar_candidate_sql(include_portal_history: bool) -> str:
    return (
        "cohort_id=%s AND provider_tier=%s AND ticker=ANY(%s) AND session_scope='RTH' AND is_final AND "
        + _BAR_VISIBLE_AT + bar_history_predicate(include_portal_history)
    )


def _bar_candidate_params(scope: BarScope, clocks: BarClocks) -> tuple:
    return (str(scope.cohort_id), scope.provider_tier, list(scope.tickers),
            *clocks.params(), *scope.history_params)


def select_winning_bars(cursor, scope: BarScope, clocks: BarClocks, *, with_state: bool = False):
    """One row per (ticker, interval, adjustment_basis, bar_start): the winning final RTH revision visible under
    `clocks`, ordered by that identity. `with_state` also reports each winner's source rank and the number of
    candidate revisions visible under `clocks` (the integrity count of the delta loader), read in the same
    statement and so from the same snapshot."""
    include = scope.include_portal_history
    candidate, params = _bar_candidate_sql(include), _bar_candidate_params(scope, clocks)
    state = ""
    if with_state:
        state = (", " + source_precedence_case(include) + " AS src_rank, "
                 "(SELECT count(*) FROM rt.bar_revisions WHERE " + candidate + ") AS visible_total")
        params = (*params, *params)
    cursor.execute(
        "SELECT DISTINCT ON (ticker, interval, adjustment_basis, bar_start) "
        + bar_columns(include) + state + " FROM rt.bar_revisions WHERE " + candidate
        + "ORDER BY ticker, interval, adjustment_basis, bar_start, "
        + source_precedence_case(include) + " DESC, observed_at DESC, created_at DESC, bar_revision_id",
        params,
    )
    rows = [dict(row) for row in cursor.fetchall()]
    if not with_state:
        return rows
    ranked, total = [], 0
    for row in rows:
        total = row.pop("visible_total")
        ranked.append((row, row.pop("src_rank")))
    return ranked, total


def select_new_bars(cursor, scope: BarScope, previous: BarClocks, current: BarClocks):
    """The candidate revisions visible under `current` and not visible under `previous`, each with its source
    rank, plus the number of candidates visible under `current` (one statement, one snapshot)."""
    include = scope.include_portal_history
    candidate, params = _bar_candidate_sql(include), _bar_candidate_params(scope, current)
    cursor.execute(
        "SELECT counted.visible_total, new_bars.* FROM "
        "(SELECT count(*) AS visible_total FROM rt.bar_revisions WHERE " + candidate + ") AS counted "
        "LEFT JOIN (SELECT " + bar_columns(include) + ", " + source_precedence_case(include) + " AS src_rank "
        "FROM rt.bar_revisions WHERE " + candidate + _BAR_NOT_VISIBLE_AT + ") AS new_bars ON true",
        (*params, *params, *previous.params()),
    )
    rows = [dict(row) for row in cursor.fetchall()]
    total = rows[0]["visible_total"]
    ranked = []
    for row in rows:
        del row["visible_total"]
        rank = row.pop("src_rank")
        if row["bar_revision_id"] is not None:
            ranked.append((row, rank))
    return ranked, total


def select_source_bars(cursor, scope: BarScope, revision_ids, clocks: BarClocks):
    """Derived-bar source revisions by id, visible under `clocks` (any scope, interval or window), ordered by id."""
    cursor.execute(
        "SELECT " + bar_columns(scope.include_portal_history) + " "
        "FROM rt.bar_revisions WHERE bar_revision_id=ANY(%s::uuid[]) AND cohort_id=%s "
        "AND provider_tier=%s AND observed_at<=%s AND created_at<=%s "
        "AND (replay_available_at IS NULL OR replay_available_at<=%s) "
        "AND (provider_published_at IS NULL OR provider_published_at<=%s) AND is_final AND bar_end<=%s "
        "ORDER BY bar_revision_id",
        (revision_ids, str(scope.cohort_id), scope.provider_tier, *clocks.params()),
    )
    return [dict(row) for row in cursor.fetchall()]


def referenced_source_ids(rows: Iterable[dict]) -> set[str]:
    return {str(source) for row in rows for source in row["source_revision_ids"]}


def close_source_lineage(initial_missing: Iterable[str], indexed_ids: Container[str],
                         fetch_sources: Callable[[list[str]], list[dict]]) -> list[dict]:
    """Rows for the source revisions referenced (transitively) but not yet indexed, in discovery order.

    `initial_missing` are the referenced ids absent from `indexed_ids`; `fetch_sources(sorted_ids)` returns
    exactly those rows (ordered by revision id within a round) or fewer when evidence is missing."""
    tail: dict[str, dict] = {}
    missing = set(initial_missing)
    for _ in range(MAX_SOURCE_LINEAGE_ROUNDS):
        if not missing:
            break
        sources = fetch_sources(sorted(missing))
        if {str(row["bar_revision_id"]) for row in sources} != missing:
            raise ValueError("Realtime screening derived source evidence is incomplete")
        tail.update((str(row["bar_revision_id"]), row) for row in sources)
        missing = {source for source in referenced_source_ids(sources)
                   if source not in tail and source not in indexed_ids}
    else:
        raise ValueError("Realtime screening derived source lineage exceeds its bounded depth")
    return list(tail.values())


def load_full_bars(cursor, request: BarRequest) -> tuple:
    """The oracle: every retained winner and its source closure, read from scratch."""
    scope, clocks = request.scope, request.clocks
    indexed = {str(row["bar_revision_id"]): row for row in select_winning_bars(cursor, scope, clocks)}
    tail = close_source_lineage(
        (source for source in referenced_source_ids(indexed.values()) if source not in indexed), indexed,
        lambda ids: select_source_bars(cursor, scope, ids, clocks),
    )
    return (*indexed.values(), *tail)


def load_realtime_screening_input(cursor, *, cohort_id: UUID, session: date,
                                  cutoff: datetime, source_generation: int,
                                  include_portal_history: bool = False,
                                  include_strategy_action_details: bool = False,
                                  known_by: datetime | None = None) -> RealtimeScreeningInput:
    """`cutoff` is the market boundary and the identity of the input; `known_by` (timezone-aware, not before
    the cutoff; default the cutoff) is the instant by which a bar must have been recorded (observed_at,
    created_at, replay_available_at, provider_published_at). Bars ending after the cutoff are never loaded."""
    return load_screening_input(
        cursor, cohort_id=cohort_id, session=session, cutoff=cutoff, source_generation=source_generation,
        include_portal_history=include_portal_history,
        include_strategy_action_details=include_strategy_action_details,
        known_by=known_by, bar_loader=load_full_bars,
    )


def load_screening_input(cursor, *, cohort_id: UUID, session: date,
                         cutoff: datetime, source_generation: int,
                         include_portal_history: bool,
                         include_strategy_action_details: bool,
                         known_by: datetime | None,
                         bar_loader: Callable[[object, BarRequest], tuple]) -> RealtimeScreeningInput:
    """The screening input with the bar set supplied by `bar_loader` (the full read or a retained one).
    Manifest, members, actions and coverage are read on every call, with the cutoff."""
    start, history_params = screening_history_window(session, include_portal_history=include_portal_history)
    requested_known_by = known_by
    if known_by is None:
        known_by = cutoff
    elif known_by.tzinfo is None or known_by.utcoffset() is None:
        raise ValueError("Screening known_by must be timezone-aware")
    elif cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("Screening cutoff must be timezone-aware")
    elif known_by < cutoff:
        raise ValueError("Screening known_by must not precede the cutoff")
    cursor.execute(
        "SELECT manifest.manifest_id, manifest.manifest_sha256, manifest.stock_count, "
        "profile.provider_tier FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest USING (manifest_id) "
        "JOIN rt.capability_profiles AS profile USING (profile_id) "
        "WHERE cohort.cohort_id=%s AND cohort.created_at<=%s AND cohort.effective_at<=%s "
        "AND manifest.created_at<=%s AND profile.created_at<=%s",
        (str(cohort_id), cutoff, cutoff, cutoff, cutoff),
    )
    manifest = cursor.fetchone()
    if manifest is None:
        raise ValueError("Exact realtime screening cohort/manifest is unavailable")
    cursor.execute(
        "SELECT reference.security_id, reference.ticker, "
        "reference.revision_id AS security_revision_id, reference.company_name, "
        "reference.security_type, reference.exchange, reference.active, reference.cik, "
        "reference.composite_figi, reference.share_class_figi, reference.sic_code, "
        "reference.sic_description, reference.sector, reference.industry, reference.list_date, "
        "reference.delisted_date, reference.weighted_shares, reference.free_float, "
        "reference.free_float_percent, reference.market_cap, reference.effective_from, "
        "reference.observed_at, reference.payload_sha256, instrument.instrument_id "
        "FROM rt.subscription_members AS member "
        "JOIN rt.instruments AS instrument USING (instrument_id) "
        "JOIN ref.security_revisions AS reference USING (security_id) "
        "WHERE member.manifest_id=%s AND instrument.asset_class='EQUITY' AND member.channel='AM' "
        "AND reference.effective_from<=%s AND (reference.effective_to IS NULL OR reference.effective_to>%s) "
        "AND reference.observed_at<=%s ORDER BY reference.ticker",
        (str(manifest["manifest_id"]), cutoff, cutoff, cutoff),
    )
    members = tuple(dict(row) for row in cursor.fetchall())
    if len(members) != manifest["stock_count"]:
        raise ValueError("Exact realtime screening reference membership is incomplete")
    scope = BarScope(
        cohort_id=cohort_id, provider_tier=manifest["provider_tier"],
        tickers=tuple(m["ticker"] for m in members),
        include_portal_history=include_portal_history, history_params=history_params,
    )
    request = BarRequest(
        scope=scope, cutoff=cutoff, known_by=known_by,
        identity=(
            str(cohort_id), scope.provider_tier, scope.tickers, include_portal_history, history_params,
            include_strategy_action_details, session, source_generation,
            str(manifest["manifest_id"]), manifest["manifest_sha256"],
            tuple((str(m["security_id"]), m["ticker"], str(m.get("instrument_id"))) for m in members),
        ),
        slot=(str(cohort_id), scope.provider_tier, include_portal_history, include_strategy_action_details),
    )
    bars = tuple(bar_loader(cursor, request))
    security_ids = [str(m["security_id"]) for m in members]
    action_details = "cash_amount::text, ex_date, new_ticker, raw_payload, " if include_strategy_action_details else ""
    cursor.execute(
        "SELECT action_id, security_id, ticker, action_type, effective_date, "
        "split_from::text, split_to::text, payload_sha256, received_at, source, source_key, observed_at, "
        + action_details + "COUNT(*) OVER (PARTITION BY security_id,source,action_type,source_key,observed_at,received_at) AS observation_ties "
        "FROM ref.corporate_action_revisions AS action WHERE security_id=ANY(%s::uuid[]) "
        "AND received_at<=%s AND observed_at<=%s "
        "AND NOT EXISTS (SELECT 1 FROM ref.corporate_action_revisions AS newer "
        "WHERE newer.supersedes_action_id=action.action_id AND newer.received_at<=%s) "
        "AND NOT EXISTS (SELECT 1 FROM ref.corporate_action_revisions AS newer "
        "WHERE newer.security_id=action.security_id AND newer.source=action.source "
        "AND newer.action_type=action.action_type AND newer.source_key=action.source_key "
        "AND newer.received_at<=%s AND newer.observed_at<=%s "
        "AND (newer.received_at>action.received_at OR (newer.received_at=action.received_at "
        "AND newer.observed_at>action.observed_at))) "
        "ORDER BY ticker, effective_date, action_id",
        (security_ids, cutoff, cutoff, cutoff, cutoff, cutoff),
    )
    actions = tuple(dict(row) for row in cursor.fetchall())
    if any(row.get("observation_ties", 1) != 1 for row in actions):
        raise ValueError("Realtime corporate-action observations conflict at the same known-by time")
    cursor.execute(
        "SELECT coverage_id, security_id, action_type, window_start, window_end, "
        "response_action_count, response_sha256, payload_sha256, received_at "
        "FROM ref.corporate_action_coverage WHERE security_id=ANY(%s::uuid[]) "
        "AND action_type='SPLIT' AND window_start<=%s AND window_end>=%s AND received_at<=%s "
        "ORDER BY security_id, received_at DESC, coverage_id",
        (security_ids, start, session, cutoff),
    )
    coverage = tuple(dict(row) for row in cursor.fetchall())
    return RealtimeScreeningInput(
        cohort_id=cohort_id, provider_tier=ProviderTier(manifest["provider_tier"]),
        source_manifest_sha256=manifest["manifest_sha256"], source_generation=source_generation,
        session=session, cutoff=cutoff, members=members, bars=bars, actions=actions, coverage=coverage,
        known_by=requested_known_by,
    )


def materialize_realtime_screening(database: RealtimeProcessDatabase, *, cohort_id: UUID,
                                   session: date, cutoff: datetime, source_generation: int):
    if not isinstance(database, RealtimeProcessDatabase) or database.process_plan.role is not RealtimeDatabaseRole.COMPUTE:
        raise ValueError("Realtime screening production requires the target-local COMPUTE role")
    with database.transaction(dict_cursor=True, isolation_level="REPEATABLE READ") as cursor:
        inputs = load_realtime_screening_input(
            cursor, cohort_id=cohort_id, session=session, cutoff=cutoff, source_generation=source_generation,
        )
        generation = produce_realtime_screening(inputs)
        persist_screening_generation_cursor(cursor, generation)
    return generation
