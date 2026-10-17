from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from uuid import UUID

from psycopg2.extras import execute_batch, execute_values, register_uuid

from .equity_bar_adapter import RealtimeEquityBarFact


register_uuid()


MAX_BAR_PERSIST_BATCH = 1_000
BAR_BATCH_PAGE_SIZE = 100


def _current_source_precedence(alias: str) -> str:
    return (
        "CASE "
        f"WHEN {alias}.adjustment_basis='ADJUSTED' AND {alias}.interval='1d' "
        f"AND {alias}.source_kind='NATIVE_REST' THEN 5 "
        f"WHEN {alias}.adjustment_basis='ADJUSTED' AND {alias}.interval IN ('1wk','1mo') "
        f"AND {alias}.source_kind='DERIVED' THEN 5 "
        f"WHEN {alias}.adjustment_basis='RAW_UNADJUSTED' AND {alias}.interval IN ('1m','5m') "
        f"AND {alias}.source_kind='NATIVE_REST' THEN 5 "
        f"WHEN {alias}.adjustment_basis='RAW_UNADJUSTED' "
        f"AND {alias}.interval IN ('15m','30m','1h','1d') "
        f"AND {alias}.source_kind='DERIVED' THEN 5 "
        f"WHEN {alias}.source_kind='RECONCILED' THEN 4 "
        f"WHEN {alias}.source_kind='NATIVE_REST' THEN 3 "
        f"WHEN {alias}.source_kind='DERIVED' THEN 2 ELSE 1 END"
    )


def persist_realtime_equity_bars_cursor(
    cursor,
    facts: Sequence[RealtimeEquityBarFact],
    *,
    values_executor=execute_values,
    batch_executor=execute_batch,
) -> None:
    rows = tuple(facts)
    if not 1 <= len(rows) <= MAX_BAR_PERSIST_BATCH:
        raise ValueError("realtime bar persistence requires 1-1000 facts")
    cohort_tiers = {(row.cohort_id, row.provider_tier) for row in rows}
    if len(cohort_tiers) != 1:
        raise ValueError("realtime bar batch must belong to one cohort and provider tier")
    revision_ids = tuple(row.revision.bar_revision_id for row in rows)
    if len(revision_ids) != len(set(revision_ids)):
        raise ValueError("realtime bar batch revision IDs must be unique")
    id_values = [str(value) for value in revision_ids]
    # The catalog owns identity; the partitioned body is keyed by (id, observed_at).
    values_executor(
        cursor,
        "INSERT INTO rt.bar_catalog ("
        "bar_revision_id, cohort_id, provider_tier, instrument_id, interval, "
        "bar_start, bar_end, observed_at, session_date) VALUES %s "
        "ON CONFLICT DO NOTHING",
        [
            (
                str(fact.revision.bar_revision_id),
                str(fact.cohort_id),
                fact.provider_tier.value,
                fact.instrument_id,
                fact.revision.interval,
                fact.revision.bar_start,
                fact.revision.bar_end,
                fact.revision.system_observed_at,
                fact.revision.session_date,
            )
            for fact in rows
        ],
        page_size=BAR_BATCH_PAGE_SIZE,
    )
    cursor.execute(
        "SELECT bar_revision_id, cohort_id, provider_tier, instrument_id, interval, "
        "bar_start, bar_end, observed_at, session_date FROM rt.bar_catalog "
        "WHERE bar_revision_id=ANY(%s::uuid[])",
        (id_values,),
    )
    stored_catalog = {
        str(row[0]): (str(row[1]), *row[2:]) for row in cursor.fetchall()
    }
    expected_catalog = {
        str(fact.revision.bar_revision_id): (
            str(fact.cohort_id),
            fact.provider_tier.value,
            fact.instrument_id,
            fact.revision.interval,
            fact.revision.bar_start,
            fact.revision.bar_end,
            fact.revision.system_observed_at,
            fact.revision.session_date,
        )
        for fact in rows
    }
    for fact in rows:
        key = str(fact.revision.bar_revision_id)
        observed = stored_catalog.get(key)
        expected = expected_catalog[key]
        if observed == expected:
            continue
        content_idempotent_replay = (
            observed is not None
            and observed[:6] == expected[:6]
            and observed[7:] == expected[7:]
            and observed[6] <= expected[6]
        )
        if not content_idempotent_replay:
            raise ValueError(
                "persisted realtime bar revision identity conflict"
            )
    values_executor(
        cursor,
        "INSERT INTO rt.bar_revisions ("
        "bar_revision_id, cohort_id, provider_tier, security_id, instrument_id, "
        "ticker, interval, session_date, session_scope, adjustment_basis, "
        "bar_start, bar_end, open_price, high_price, low_price, close_price, "
        "volume, vwap, transaction_count, source_kind, availability_mode, "
        "is_final, observed_at, replay_available_at, provider_published_at, "
        "ingestion_segment_id, journal_generation, stream_session_id, "
        "source_offset, source_revision_ids, supersedes_revision_id, "
        "payload_sha256, quality_codes) VALUES %s "
        "ON CONFLICT DO NOTHING",
        [
            (
                str(fact.revision.bar_revision_id),
                str(fact.cohort_id),
                fact.provider_tier.value,
                str(fact.revision.security_id),
                fact.instrument_id,
                fact.revision.ticker,
                fact.revision.interval,
                fact.revision.session_date,
                fact.revision.session_scope.value,
                "ADJUSTED" if fact.revision.adjusted else "RAW_UNADJUSTED",
                fact.revision.bar_start,
                fact.revision.bar_end,
                fact.revision.open_price,
                fact.revision.high_price,
                fact.revision.low_price,
                fact.revision.close_price,
                fact.revision.volume,
                fact.revision.vwap,
                fact.revision.transaction_count,
                fact.revision.source_kind.value,
                fact.revision.availability_mode.value,
                fact.revision.is_final,
                fact.revision.system_observed_at,
                fact.revision.replay_available_at,
                fact.revision.provider_published_at,
                str(fact.revision.ingestion_segment_id)
                if fact.revision.ingestion_segment_id is not None else None,
                str(fact.journal_generation)
                if fact.journal_generation is not None else None,
                str(fact.stream_session_id)
                if fact.stream_session_id is not None else None,
                fact.source_offset,
                list(fact.revision.source_bar_revision_ids),
                str(fact.revision.supersedes_bar_revision_id)
                if fact.revision.supersedes_bar_revision_id is not None else None,
                fact.revision.payload_sha256,
                list(fact.revision.quality_codes),
            )
            for fact in rows
        ],
        page_size=BAR_BATCH_PAGE_SIZE,
    )
    observed_bounds = [
        stored_catalog[str(fact.revision.bar_revision_id)][6]
        for fact in rows
    ] + [
        fact.revision.system_observed_at
        for fact in rows
    ]
    cursor.execute(
        "SELECT bar_revision_id, cohort_id, provider_tier, instrument_id, "
        "payload_sha256 FROM rt.bar_revisions "
        "WHERE bar_revision_id=ANY(%s::uuid[]) "
        "AND observed_at BETWEEN %s AND %s",
        (id_values, min(observed_bounds), max(observed_bounds)),
    )
    stored = {
        str(row[0]): (
            str(row[1]), row[2], row[3], row[4]
        )
        for row in cursor.fetchall()
    }
    expected = {
        str(fact.revision.bar_revision_id): (
            str(fact.cohort_id),
            fact.provider_tier.value,
            fact.instrument_id,
            fact.revision.payload_sha256,
        )
        for fact in rows
    }
    if stored != expected:
        raise ValueError("persisted realtime bar revision identity conflict")
    batch_executor(
        cursor,
        "INSERT INTO rt.bar_current ("
        "cohort_id, provider_tier, instrument_id, interval, bar_start, "
        "session_scope, adjustment_basis, source_kind, bar_revision_id, bar_end, observed_at"
        ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (cohort_id, instrument_id, interval, bar_start, "
        "session_scope, adjustment_basis) DO UPDATE SET "
        "provider_tier=EXCLUDED.provider_tier, source_kind=EXCLUDED.source_kind, "
        "bar_revision_id=EXCLUDED.bar_revision_id, "
        "observed_at=EXCLUDED.observed_at, updated_at=clock_timestamp() "
        "WHERE (rt.bar_current.provider_tier=EXCLUDED.provider_tier "
        "OR EXCLUDED.provider_tier=("
        "SELECT profile.provider_tier FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
        "JOIN rt.capability_profiles AS profile USING(profile_id) "
        "WHERE cohort.cohort_id=rt.bar_current.cohort_id"
        ")) "
        "AND rt.bar_current.bar_end=EXCLUDED.bar_end AND ("
        + _current_source_precedence("EXCLUDED") + " > "
        + _current_source_precedence("rt.bar_current")
        + " OR (rt.bar_current.source_kind=EXCLUDED.source_kind "
        "AND rt.bar_current.observed_at < EXCLUDED.observed_at))",
        [
            (
                str(fact.cohort_id),
                fact.provider_tier.value,
                fact.instrument_id,
                fact.revision.interval,
                fact.revision.bar_start,
                fact.revision.session_scope.value,
                "ADJUSTED" if fact.revision.adjusted else "RAW_UNADJUSTED",
                fact.revision.source_kind.value,
                str(fact.revision.bar_revision_id),
                fact.revision.bar_end,
                stored_catalog[
                    str(fact.revision.bar_revision_id)
                ][6],
            )
            for fact in rows
        ],
        page_size=BAR_BATCH_PAGE_SIZE,
    )


def persist_realtime_equity_bars(
    connection,
    facts: Sequence[RealtimeEquityBarFact],
    *,
    values_executor=execute_values,
    batch_executor=execute_batch,
) -> None:
    try:
        with connection.cursor() as cursor:
            persist_realtime_equity_bars_cursor(
                cursor,
                facts,
                values_executor=values_executor,
                batch_executor=batch_executor,
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


_CURRENT_BAR_COLUMNS = (
    "bar_revision_id", "bar_start", "bar_end", "open_price", "high_price", "low_price", "close_price",
    "volume", "payload_sha256",
)
_BAR_INTERVALS = frozenset({"1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"})
_BAR_BASES = frozenset({"RAW_UNADJUSTED", "ADJUSTED"})


def load_current_bar_groups_cursor(
    cursor,
    *,
    cohort_id: UUID,
    provider_tier: str,
    instrument_id: int,
    security_id: UUID,
    ticker: str,
    groups: Mapping[str, tuple[datetime, datetime]],
    history_source_cohort_id: UUID | None = None,
    history_source_provider_tier: str | None = None,
    history_before: datetime | None = None,
) -> dict[str, list[dict]]:
    """The bars `rt.bar_current` points at for one ticker, per lineage-head group (`interval:basis`).

    `groups` maps a group key to its (first bar start, last bar end); a group holds the final RTH bars of
    that interval and basis inside the range, ordered by bar start. Each returned row carries the
    revision id and payload hash the caller recomputes the lineage chain over, plus the OHLCV values. A
    bar revised after the head was published reads as a different revision id; a bar since retired reads
    as a missing row; neither is repaired here. Bars after the head's last bar end are not read, so a
    later cycle's appended bars never disturb an earlier publication."""
    if not groups:
        return {}
    predicates, parameters = [], []
    for key, (first_bar_start, last_bar_end) in groups.items():
        interval, _, basis = key.partition(":")
        if interval not in _BAR_INTERVALS or basis not in _BAR_BASES:
            raise ValueError("bar group key must be interval:basis")
        predicates.append(
            "(cur.interval=%s AND cur.adjustment_basis=%s AND cur.bar_start>=%s AND cur.bar_end<=%s)"
        )
        parameters.extend((interval, basis, first_bar_start, last_bar_end))
    scope_sql = "(cur.cohort_id=%s AND cur.provider_tier=%s)"
    scope_params: tuple = (str(cohort_id), provider_tier)
    if history_source_cohort_id is not None:
        if (
            history_source_provider_tier is None
            or history_before is None
        ):
            raise ValueError("bar group history binding is incomplete")
        scope_sql = (
            "(" + scope_sql
            + " OR (cur.cohort_id=%s AND cur.provider_tier=%s "
            "AND cur.bar_end<%s))"
        )
        scope_params = (
            *scope_params,
            str(history_source_cohort_id),
            history_source_provider_tier,
            history_before,
        )
    cursor.execute(
        "SELECT cur.interval AS interval, cur.adjustment_basis AS adjustment_basis, "
        "revision.bar_revision_id AS bar_revision_id, revision.bar_start AS bar_start, "
        "revision.bar_end AS bar_end, revision.open_price AS open_price, "
        "revision.high_price AS high_price, revision.low_price AS low_price, "
        "revision.close_price AS close_price, revision.volume AS volume, "
        "revision.payload_sha256 AS payload_sha256 "
        "FROM rt.bar_current AS cur "
        "JOIN rt.bar_revisions AS revision ON revision.bar_revision_id=cur.bar_revision_id "
        "AND revision.observed_at=cur.observed_at AND revision.cohort_id=cur.cohort_id "
        "AND revision.provider_tier=cur.provider_tier AND revision.instrument_id=cur.instrument_id "
        "AND revision.interval=cur.interval "
        "WHERE " + scope_sql + " AND cur.instrument_id=%s "
        "AND cur.session_scope='RTH' AND revision.security_id=%s AND revision.ticker=%s "
        "AND revision.is_final AND (" + " OR ".join(predicates) + ") "
        "ORDER BY cur.interval, cur.adjustment_basis, cur.bar_start",
        (
            *scope_params,
            instrument_id,
            str(security_id),
            ticker,
            *parameters,
        ),
    )
    by_group: dict[str, list[dict]] = {key: [] for key in groups}
    for fetched in cursor.fetchall():
        row = fetched if isinstance(fetched, Mapping) else dict(
            zip(("interval", "adjustment_basis", *_CURRENT_BAR_COLUMNS), fetched)
        )
        by_group[f"{row['interval']}:{row['adjustment_basis']}"].append(row)
    return by_group


def load_current_bar_heads_cursor(
    cursor,
    *,
    cohort_id: UUID,
    provider_tier: str,
    targets: Sequence[tuple[int, UUID, str, str, datetime, datetime]],
    with_sources: bool = False,
) -> dict[tuple[str, str], list[dict]]:
    """The bars `rt.bar_current` points at for many tickers in one statement.

    `targets` are (instrument id, security id, ticker, group key `interval:basis`, first bar start, last bar
    end), one per lineage-head group to recompute. The result maps (security id, group key) to the group's
    final RTH bars ordered by bar start, each with the revision id and payload hash the caller recomputes the
    chain over (and, with `with_sources`, its `source_revision_ids`). Same contract as
    `load_current_bar_groups_cursor`: a revised bar reads as a different revision, a retired one as a missing
    row, nothing is repaired, and bars after a group's last bar end are never read."""
    result: dict[tuple[str, str], list[dict]] = {}
    if not targets:
        return result
    instruments, securities, tickers, intervals, bases, firsts, lasts = ([] for _ in range(7))
    for instrument_id, security_id, ticker, key, first_bar_start, last_bar_end in targets:
        interval, _, basis = key.partition(":")
        if interval not in _BAR_INTERVALS or basis not in _BAR_BASES:
            raise ValueError("bar group key must be interval:basis")
        instruments.append(int(instrument_id))
        securities.append(str(security_id))
        tickers.append(ticker)
        intervals.append(interval)
        bases.append(basis)
        firsts.append(first_bar_start)
        lasts.append(last_bar_end)
        result[(str(security_id), key)] = []
    cursor.execute(
        "SELECT target.security_id::text AS security_id, cur.interval AS interval, "
        "cur.adjustment_basis AS adjustment_basis, revision.bar_revision_id AS bar_revision_id, "
        "revision.bar_start AS bar_start, revision.bar_end AS bar_end, "
        "revision.payload_sha256 AS payload_sha256"
        + (", revision.source_revision_ids AS source_revision_ids" if with_sources else "") + " "
        "FROM unnest(%s::bigint[], %s::uuid[], %s::text[], %s::text[], %s::text[], %s::timestamptz[], "
        "%s::timestamptz[]) AS target(instrument_id, security_id, ticker, bar_interval, basis, "
        "first_bar_start, last_bar_end) "
        "JOIN rt.bar_current AS cur ON cur.cohort_id=%s AND cur.provider_tier=%s "
        "AND cur.instrument_id=target.instrument_id AND cur.interval=target.bar_interval "
        "AND cur.adjustment_basis=target.basis AND cur.session_scope='RTH' "
        "AND cur.bar_start>=target.first_bar_start AND cur.bar_end<=target.last_bar_end "
        "JOIN rt.bar_revisions AS revision ON revision.bar_revision_id=cur.bar_revision_id "
        "AND revision.observed_at=cur.observed_at AND revision.cohort_id=cur.cohort_id "
        "AND revision.provider_tier=cur.provider_tier AND revision.instrument_id=cur.instrument_id "
        "AND revision.interval=cur.interval AND revision.security_id=target.security_id "
        "AND revision.ticker=target.ticker "
        "WHERE revision.is_final "
        "ORDER BY target.security_id, cur.interval, cur.adjustment_basis, cur.bar_start",
        (instruments, securities, tickers, intervals, bases, firsts, lasts, str(cohort_id), provider_tier),
    )
    for fetched in cursor.fetchall():
        row = fetched if isinstance(fetched, Mapping) else dict(zip(
            ("security_id", "interval", "adjustment_basis", "bar_revision_id", "bar_start", "bar_end",
             "payload_sha256", *(("source_revision_ids",) if with_sources else ())), fetched))
        result[(row["security_id"], f"{row['interval']}:{row['adjustment_basis']}")].append(row)
    return result