from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from uuid import UUID

from psycopg2.extras import execute_batch, execute_values, register_uuid

from .equity_bar_adapter import (
    MAXIMUM_HISTORY_BINDING_DEPTH,
    RealtimeEquityBarFact,
)


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


# G102 A4 (design 4.3): one current bar per (instrument, interval, bar start, session scope, basis) across every
# writer, in today's read order: the source precedence above, then the later observation, then the provider tier
# (real-time Advanced over delayed Developer, only between equal observations), then the later write, then the
# revision id. The A4 rehearsal on a copy of live (Nov 7) showed that putting the tier first overrides the precedence
# table: Advanced provider weekly bars displaced the derived weekly bars it prefers (32 of 61 values differed for
# AAPL).
BAR_SELECTION_POLICY = "bar-selection-v1"
BAR_KEY_COLUMNS = ("instrument_id", "interval", "bar_start", "session_scope", "adjustment_basis")
_HEAD_VALUE_COLUMNS = (
    "bar_revision_id", "provider_tier", "source_kind", "session_date", "bar_end", "observed_at", "is_final",
    "open_price", "high_price", "low_price", "close_price", "volume", "vwap", "transaction_count", "payload_sha256",
)


def _provider_tier_rank(alias: str) -> str:
    return f"CASE WHEN {alias}.provider_tier='MASSIVE_ADVANCED' THEN 2 ELSE 1 END"


def _revision_created_at(alias: str) -> str:
    """The write time of the revision a head row (or EXCLUDED) names: one primary-key lookup in the partitioned
    revisions, since `rt.bar_head` does not copy it."""
    return (
        "(SELECT revision_clock.created_at FROM rt.bar_revisions AS revision_clock "
        f"WHERE revision_clock.bar_revision_id={alias}.bar_revision_id "
        f"AND revision_clock.observed_at={alias}.observed_at)"
    )


def _winner_rank(alias: str, created_at: str) -> str:
    """The `bar-selection-v1` winner tuple before the revision id (target design 4.3), higher wins, as row elements.
    The same terms as every ORDER BY (`_selection_order`)."""
    return f"{_current_source_precedence(alias)},{alias}.observed_at,{_provider_tier_rank(alias)},{created_at}"


def _head_replaces(new: str, old: str, *, new_created_at: str | None = None) -> str:
    """True when revision `new` beats the current head `old` on the whole `bar-selection-v1` tuple: precedence,
    observation, tier, the revision's write time, then the lower revision id. Nov 10 (review R1): the upsert used to
    let the later write win an exact tie on precedence, observation and tier, so replaying the losing revision could
    flip the head; now the result is the same in any arrival order (INV-2), and a revision never replaces itself.

    One row comparison: the ids are swapped in the last element, so on a full tie the lower id wins and an equal id
    (the same revision) never wins. `new_created_at` lets the writer pass the new revision's write time as a value;
    each lookup in the partitioned revisions costs planning time (Nov 10 copy: 6.4 ms with four, against 0.06 ms)."""
    created = new_created_at or _revision_created_at(new)
    return (
        f"ROW({_winner_rank(new, created)},{old}.bar_revision_id)"
        f">ROW({_winner_rank(old, _revision_created_at(old))},{new}.bar_revision_id)"
    )


def _selection_order(alias: str) -> str:
    """ORDER BY terms that put the `bar-selection-v1` winner first among revisions of one key (the same tuple as
    `_head_replaces`)."""
    return (
        f"{_current_source_precedence(alias)} DESC,{alias}.observed_at DESC,{_provider_tier_rank(alias)} DESC,"
        f"{alias}.created_at DESC,{alias}.bar_revision_id"
    )


_HEAD_INSERT = (
    "INSERT INTO rt.bar_head (" + ",".join((*BAR_KEY_COLUMNS, *_HEAD_VALUE_COLUMNS)) + ","
    "selection_policy,written_by_cohort_id) "
)
_HEAD_CONFLICT = (
    "ON CONFLICT (" + ",".join(BAR_KEY_COLUMNS) + ") DO UPDATE SET "
    + ",".join(f"{column}=EXCLUDED.{column}" for column in _HEAD_VALUE_COLUMNS if column != "bar_end")
    + ",selection_policy=EXCLUDED.selection_policy,written_by_cohort_id=EXCLUDED.written_by_cohort_id,"
    "updated_at=clock_timestamp() WHERE rt.bar_head.bar_end=EXCLUDED.bar_end AND "
    + _head_replaces("EXCLUDED", "rt.bar_head", new_created_at="%s::timestamptz")
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
        "payload_sha256, observed_at, created_at FROM rt.bar_revisions "
        "WHERE bar_revision_id=ANY(%s::uuid[]) "
        "AND observed_at BETWEEN %s AND %s",
        (id_values, min(observed_bounds), max(observed_bounds)),
    )
    fetched = cursor.fetchall()
    stored = {
        str(row[0]): (
            str(row[1]), row[2], row[3], row[4]
        )
        for row in fetched
    }
    # The write time of the revision each head row will name (the one at the catalog's observation), for the winner
    # tuple; replays read the original row's time.
    written_at = {
        str(row[0]): row[6] for row in fetched if row[5] == stored_catalog[str(row[0])][6]
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
    if set(written_at) != set(expected):
        raise ValueError("persisted realtime bar revision write time is unavailable")
    # G102 A4 (design 4.4): the current bar lives only in the cohort-free head. rt.bar_current is no longer
    # written (user, Nov 7: the target design runs Monday, no entries into old tables).
    batch_executor(
        cursor,
        _HEAD_INSERT + "VALUES (" + ",".join(["%s"] * (len(BAR_KEY_COLUMNS) + len(_HEAD_VALUE_COLUMNS) + 2))
        + ") " + _HEAD_CONFLICT,
        [
            (
                fact.instrument_id,
                fact.revision.interval,
                fact.revision.bar_start,
                fact.revision.session_scope.value,
                "ADJUSTED" if fact.revision.adjusted else "RAW_UNADJUSTED",
                str(fact.revision.bar_revision_id),
                fact.provider_tier.value,
                fact.revision.source_kind.value,
                fact.revision.session_date,
                fact.revision.bar_end,
                stored_catalog[str(fact.revision.bar_revision_id)][6],
                fact.revision.is_final,
                fact.revision.open_price,
                fact.revision.high_price,
                fact.revision.low_price,
                fact.revision.close_price,
                fact.revision.volume,
                fact.revision.vwap,
                fact.revision.transaction_count,
                fact.revision.payload_sha256,
                BAR_SELECTION_POLICY,
                str(fact.cohort_id),
                written_at[str(fact.revision.bar_revision_id)],  # the conflict clause's new write time
            )
            for fact in rows
        ],
        page_size=BAR_BATCH_PAGE_SIZE,
    )


def backfill_bar_head_cursor(cursor, *, instrument_id: int) -> int:
    """G102 A4 (design 4.7 step 1): fill one instrument's head from every cohort's current bars, choosing across
    cohorts by `bar-selection-v1`. Idempotent and resumable per instrument (existing head rows are kept: the writer
    already chose them by the same policy). Returns the rows inserted."""
    # The migrated row keeps the time its pointer was last updated, so causal reads (`updated_at <= checked_at`)
    # mean what they meant on rt.bar_current. The A4 rehearsal (Nov 7) caught the backfill time standing in for it:
    # every history read before the backfill came back empty.
    cursor.execute(
        _HEAD_INSERT.replace(",written_by_cohort_id) ", ",written_by_cohort_id,updated_at) ")
        + "SELECT DISTINCT ON (revision.instrument_id,revision.interval,revision.bar_start,"
        "revision.session_scope,revision.adjustment_basis) "
        + ",".join(f"revision.{column}" for column in (*BAR_KEY_COLUMNS, *_HEAD_VALUE_COLUMNS))
        + ",%s,revision.cohort_id,cur.updated_at FROM rt.bar_current AS cur "
        "JOIN rt.bar_revisions AS revision ON revision.bar_revision_id=cur.bar_revision_id "
        "AND revision.observed_at=cur.observed_at "
        "WHERE cur.instrument_id=%s "
        "ORDER BY revision.instrument_id,revision.interval,revision.bar_start,revision.session_scope,"
        "revision.adjustment_basis," + _selection_order("revision") + " "
        "ON CONFLICT DO NOTHING",
        (BAR_SELECTION_POLICY, instrument_id),
    )
    return max(0, int(getattr(cursor, "rowcount", 0) or 0))


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
    history_source_scopes: Sequence[tuple[UUID, str, datetime]] = (),
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
    if history_source_scopes:
        if len(history_source_scopes) > MAXIMUM_HISTORY_BINDING_DEPTH:
            raise ValueError("bar group history scope depth is too large")
        for source_id, source_tier, before in history_source_scopes:
            if (
                not isinstance(source_id, UUID)
                or source_tier not in {"MASSIVE_ADVANCED", "MASSIVE_DEVELOPER"}
                or before.tzinfo is None or before.utcoffset() is None
            ):
                raise ValueError("bar group history source identity is invalid")
            scope_sql += " OR (cur.cohort_id=%s AND cur.provider_tier=%s AND cur.bar_end<%s)"
            scope_params = (*scope_params, str(source_id), source_tier, before)
        scope_sql = "(" + scope_sql + ")"
    elif history_source_cohort_id is not None:
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
        "SELECT DISTINCT ON (cur.interval,cur.adjustment_basis,cur.bar_start) "
        "cur.interval AS interval, cur.adjustment_basis AS adjustment_basis, "
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
        "ORDER BY cur.interval,cur.adjustment_basis,cur.bar_start,"
        + _current_source_precedence("cur")
        + " DESC,revision.observed_at DESC,revision.created_at DESC,revision.bar_revision_id",
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

_HEAD_ROW_COLUMNS = (
    "instrument_id", "interval", "adjustment_basis", "bar_revision_id", "bar_start", "bar_end", "session_date",
    "observed_at", "source_kind", "provider_tier", "open_price", "high_price", "low_price", "close_price", "volume",
    "vwap", "transaction_count", "payload_sha256",
)


def _head_rows_sql(target_sql: str, *, with_sources: bool, as_of: datetime | None) -> str:
    """The final bars of every target range from `rt.bar_head`; with `as_of` (a rerun's cutoff, design 4.6), a key
    whose head was observed later is read from `rt.bar_revisions` as it stood then (same selection order), and a
    key first observed later is left out. `target_sql` must expose instrument_id, bar_interval, basis, scope,
    first_bar_start, last_bar_end and key_text."""
    columns = ",".join(f"chosen.{column}" for column in _HEAD_ROW_COLUMNS)
    sources = ",chosen.source_revision_ids" if with_sources else ""
    head = (
        "SELECT target.key_text,head.* FROM " + target_sql + " "
        "JOIN rt.bar_head AS head ON head.instrument_id=target.instrument_id "
        "AND head.interval=target.bar_interval AND head.adjustment_basis=target.basis "
        "AND head.session_scope=target.scope AND head.bar_start>=target.first_bar_start "
        "AND head.bar_end<=target.last_bar_end"
    )
    current = (
        "SELECT candidate.key_text," + ",".join(f"candidate.{column}" for column in _HEAD_ROW_COLUMNS)
        + ",candidate.is_final"
        + (",revision.source_revision_ids" if with_sources else "")
        + " FROM candidate"
        + (
            " JOIN rt.bar_revisions AS revision ON revision.bar_revision_id=candidate.bar_revision_id "
            "AND revision.observed_at=candidate.observed_at"
            if with_sources else ""
        )
        + (" WHERE candidate.observed_at<=%(as_of)s" if as_of is not None else "")
    )
    corrected = "" if as_of is None else (
        " UNION ALL SELECT candidate.key_text,"
        + ",".join(f"revision.{column}" for column in _HEAD_ROW_COLUMNS)
        + ",revision.is_final" + (",revision.source_revision_ids" if with_sources else "")
        + " FROM candidate CROSS JOIN LATERAL (SELECT revision.* FROM rt.bar_revisions AS revision "
        "WHERE revision.instrument_id=candidate.instrument_id AND revision.interval=candidate.interval "
        "AND revision.bar_end=candidate.bar_end AND revision.bar_start=candidate.bar_start "
        "AND revision.session_scope=candidate.session_scope "
        "AND revision.adjustment_basis=candidate.adjustment_basis AND revision.observed_at<=%(as_of)s "
        "ORDER BY " + _selection_order("revision") + " LIMIT 1) AS revision "
        "WHERE candidate.observed_at>%(as_of)s"
    )
    return (
        "WITH candidate AS (" + head + "), chosen AS (" + current + corrected + ") "
        "SELECT chosen.key_text," + columns + sources + " FROM chosen WHERE chosen.is_final "
        "ORDER BY chosen.key_text,chosen.interval,chosen.adjustment_basis,chosen.bar_start"
    )


def _bar_group(key: str) -> tuple[str, str]:
    interval, _, basis = key.partition(":")
    if interval not in _BAR_INTERVALS or basis not in _BAR_BASES:
        raise ValueError("bar group key must be interval:basis")
    return interval, basis


def load_bar_heads_cursor(
    cursor,
    *,
    targets: Sequence[tuple[int, UUID, str, str, datetime, datetime]],
    with_sources: bool = False,
    as_of: datetime | None = None,
) -> dict[tuple[str, str], list[dict]]:
    """G102 A4: `load_current_bar_heads_cursor` without the cohort, the tier or history bindings.

    `targets` are (instrument id, security id, ticker, group key `interval:basis`, first bar start, last bar end).
    The result maps (security id, group key) to the group's final RTH bars ordered by bar start, each with the
    revision id and payload hash a lineage recomputation needs and the OHLCV values (and, with `with_sources`,
    `source_revision_ids`). The instrument must belong to the security (checked against `rt.instruments`)."""
    result: dict[tuple[str, str], list[dict]] = {}
    if not targets:
        return result
    instruments, securities, keys, intervals, bases, firsts, lasts = ([] for _ in range(7))
    for instrument_id, security_id, _ticker, key, first_bar_start, last_bar_end in targets:
        interval, basis = _bar_group(key)
        instruments.append(int(instrument_id))
        securities.append(str(security_id))
        keys.append(f"{security_id}|{key}")
        intervals.append(interval)
        bases.append(basis)
        firsts.append(first_bar_start)
        lasts.append(last_bar_end)
        result[(str(security_id), key)] = []
    target_sql = (
        "(SELECT target.*,'RTH'::varchar AS scope FROM unnest(%(instruments)s::bigint[],%(securities)s::uuid[],"
        "%(keys)s::text[],%(intervals)s::text[],%(bases)s::text[],%(firsts)s::timestamptz[],"
        "%(lasts)s::timestamptz[]) AS target(instrument_id,security_id,key_text,bar_interval,basis,"
        "first_bar_start,last_bar_end) JOIN rt.instruments AS instrument "
        "ON instrument.instrument_id=target.instrument_id AND instrument.security_id=target.security_id) AS target"
    )
    cursor.execute(
        _head_rows_sql(target_sql, with_sources=with_sources, as_of=as_of),
        dict(instruments=instruments, securities=securities, keys=keys, intervals=intervals, bases=bases,
             firsts=firsts, lasts=lasts, as_of=as_of),
    )
    names = ("key_text", *_HEAD_ROW_COLUMNS, *(("source_revision_ids",) if with_sources else ()))
    for fetched in cursor.fetchall():
        row = dict(fetched) if isinstance(fetched, Mapping) else dict(zip(names, fetched))
        security_id, _, key = row.pop("key_text").partition("|")
        result[(security_id, key)].append(row)
    return result


def load_bar_groups_cursor(
    cursor,
    *,
    instrument_id: int,
    security_id: UUID,
    groups: Mapping[str, tuple[datetime, datetime]],
    as_of: datetime | None = None,
) -> dict[str, list[dict]]:
    """G102 A4: `load_current_bar_groups_cursor` for one instrument without the cohort, the tier or history
    scopes: per group key `interval:basis`, the final RTH bars of its (first bar start, last bar end) range."""
    if not groups:
        return {}
    loaded = load_bar_heads_cursor(
        cursor,
        targets=tuple((instrument_id, security_id, "", key, first, last) for key, (first, last) in groups.items()),
        as_of=as_of,
    )
    return {key: loaded[(str(security_id), key)] for key in groups}


def load_bar_range_cursor(
    cursor,
    *,
    instrument_ids: Sequence[int],
    interval: str,
    basis: str,
    start: datetime,
    end: datetime,
    scope: str = "RTH",
    as_of: datetime | None = None,
) -> dict[int, list[dict]]:
    """G102 A4: the final bars of one interval and basis for many instruments over [start, end), from the head
    (point in time with `as_of`). Maps instrument id to its bars ordered by bar start."""
    _bar_group(f"{interval}:{basis}")
    if scope not in {"RTH", "EXTENDED", "FULL_DAY"}:
        raise ValueError("bar session scope is invalid")
    ids = sorted({int(value) for value in instrument_ids})
    result: dict[int, list[dict]] = {value: [] for value in ids}
    if not ids:
        return result
    target_sql = (
        "(SELECT target.instrument_id,target.instrument_id::text AS key_text,%(interval)s::varchar AS bar_interval,"
        "%(basis)s::varchar AS basis,%(scope)s::varchar AS scope,%(start)s::timestamptz AS first_bar_start,"
        "%(end)s::timestamptz AS last_bar_end FROM unnest(%(ids)s::bigint[]) AS target(instrument_id)) AS target"
    )
    cursor.execute(
        _head_rows_sql(target_sql, with_sources=False, as_of=as_of),
        dict(ids=ids, interval=interval, basis=basis, scope=scope, start=start, end=end, as_of=as_of),
    )
    for fetched in cursor.fetchall():
        row = dict(fetched) if isinstance(fetched, Mapping) else dict(zip(("key_text", *_HEAD_ROW_COLUMNS), fetched))
        row.pop("key_text")
        result[int(row["instrument_id"])].append(row)
    return result


# Known-by clock sets (target INV-3). The target is one contract with all four clocks (matrix A4.5). Until then each
# reader names the set it has always used, so moving it onto this read changes only the as-of fallback: Nov 9's 5m bars
# were written up to 54 s after they were observed, so the full contract would newly drop a cutoff bar the Options
# readers accept today.
KNOWN_BY_OBSERVED = ("observed_at",)
KNOWN_BY_ALL_CLOCKS = ("observed_at", "created_at", "replay_available_at", "provider_published_at")
_BAR_SCOPES = frozenset({"RTH", "EXTENDED", "FULL_DAY"})


def _known_by(alias: str, clocks: Sequence[str]) -> str:
    terms = []
    for clock in clocks:
        if clock in ("observed_at", "created_at"):
            terms.append(f"{alias}.{clock}<=%(known_by)s")
        elif clock in ("replay_available_at", "provider_published_at"):
            terms.append(f"({alias}.{clock} IS NULL OR {alias}.{clock}<=%(known_by)s)")
        else:
            raise ValueError("unknown known-by clock")
    return "(" + " AND ".join(terms) + ")"


def load_known_bar_revisions_cursor(
    cursor,
    *,
    instrument_ids: Sequence[int],
    interval: str,
    basis: str,
    known_by: datetime,
    start: datetime,
    end: datetime | None = None,
    scopes: Sequence[str] = ("RTH",),
    latest: int | None = None,
    clocks: Sequence[str] = KNOWN_BY_OBSERVED,
) -> dict[int, list[dict]]:
    """Nov 10 review R2 (target 4.5, INV-3): every column of the revision that was current at `known_by` for each key
    of one interval and basis, per instrument, ordered by bar start.

    Keys are the heads with `start <= bar_start <= known_by` (and `bar_end <= end` when given); with `latest`, only the
    latest that many keys per instrument. A key whose head revision is known by `known_by` under `clocks` reads the head;
    a key corrected later reads the best revision known by then (the shared selection order, same `bar_end`) instead of
    dropping out; a key with no revision known by then is absent. Final and non-final revisions are both returned."""
    _bar_group(f"{interval}:{basis}")
    scopes = tuple(scopes)
    if not scopes or not set(scopes) <= _BAR_SCOPES:
        raise ValueError("bar session scope is invalid")
    if tuple(clocks) not in (KNOWN_BY_OBSERVED, KNOWN_BY_ALL_CLOCKS):
        raise ValueError("known-by clocks must be one of the named sets")
    for value in (known_by, start, end):
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("known-by read clocks must be timezone-aware")
    if latest is not None and not 1 <= int(latest) <= 1_000:
        raise ValueError("latest key count must be 1-1000")
    ids = sorted({int(value) for value in instrument_ids})
    result: dict[int, list[dict]] = {value: [] for value in ids}
    if not ids:
        return result
    window = (
        "head.instrument_id=target.instrument_id AND head.interval=%(interval)s "
        "AND head.adjustment_basis=%(basis)s AND head.session_scope=ANY(%(scopes)s::text[]) "
        "AND head.bar_start>=%(start)s AND head.bar_start<=%(known_by)s"
        + (" AND head.bar_end<=%(end)s" if end is not None else "")
    )
    targets = "unnest(%(ids)s::bigint[]) AS target(instrument_id)"
    keys = (
        "SELECT head.* FROM " + targets + " JOIN rt.bar_head AS head ON " + window
        if latest is None else
        "SELECT head.* FROM " + targets + " CROSS JOIN LATERAL (SELECT head.* FROM rt.bar_head AS head WHERE "
        + window + " ORDER BY head.bar_start DESC LIMIT %(latest)s) AS head"
    )
    known_candidate = _known_by("candidate", clocks)
    cursor.execute(
        "WITH candidate AS (SELECT revision.* FROM (" + keys + ") AS head "
        "JOIN rt.bar_revisions AS revision ON revision.bar_revision_id=head.bar_revision_id "
        "AND revision.observed_at=head.observed_at) "
        "SELECT chosen.* FROM (SELECT candidate.* FROM candidate WHERE " + known_candidate + " "
        "UNION ALL SELECT revision.* FROM candidate CROSS JOIN LATERAL (SELECT revision.* FROM rt.bar_revisions "
        "AS revision WHERE revision.instrument_id=candidate.instrument_id AND revision.interval=candidate.interval "
        "AND revision.bar_end=candidate.bar_end AND revision.bar_start=candidate.bar_start "
        "AND revision.session_scope=candidate.session_scope AND revision.adjustment_basis=candidate.adjustment_basis "
        "AND " + _known_by("revision", clocks) + " ORDER BY " + _selection_order("revision") + " LIMIT 1) "
        "AS revision WHERE NOT " + known_candidate + ") AS chosen "
        "ORDER BY chosen.instrument_id,chosen.bar_start,chosen.session_scope",
        dict(ids=ids, interval=interval, basis=basis, scopes=list(scopes), start=start, end=end, known_by=known_by,
             latest=latest),
    )
    names = None
    for fetched in cursor.fetchall():
        if isinstance(fetched, Mapping):
            row = dict(fetched)
        else:
            names = names or [column[0] for column in cursor.description]
            row = dict(zip(names, fetched))
        result[int(row["instrument_id"])].append(row)
    return result
