"""Persistence for RT-05 pre-session normalization diagnostics (rt.normalization_diagnostics).

Rows are immutable and monthly-partitioned by created_at. The id is derived from the stage run,
underlyer and diagnostic content, and created_at is supplied by the caller (the stage start time),
so replaying a stage records each diagnostic once. Rejection counts are per reason, not a
partition of raw rows, and no credential or authenticated URL is ever stored.
"""
from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import Json

from .contracts import canonical_sha256
from .option_pre_session_producer import PreSessionNormalizationDiagnostics

_COLUMNS = (
    "diagnostic_id, cohort_id, stage_run_id, underlyer, request_ids, requested_from, requested_to, "
    "returned_from, returned_to, bar_count, pages_fetched, pagination_complete, bar_acquisition, price_basis, "
    "observation_min, observation_max, raw_rows, usable_mark_count, candidates, rejection_counts, "
    "rejection_samples, created_at"
)


def diagnostic_id(cohort_id: UUID, stage_run_id: UUID | None, diagnostic: PreSessionNormalizationDiagnostics) -> UUID:
    content = canonical_sha256({
        "underlyer": diagnostic.underlyer, "bar_acquisition": diagnostic.bar_acquisition,
        "requested_from": diagnostic.requested_from, "requested_to": diagnostic.requested_to,
        "returned_from": diagnostic.returned_from, "returned_to": diagnostic.returned_to,
        "bar_count": diagnostic.bar_count, "pages_fetched": diagnostic.pages_fetched,
        "pagination_complete": diagnostic.pagination_complete, "price_basis": diagnostic.price_basis,
        "observation_min": diagnostic.observation_min, "observation_max": diagnostic.observation_max,
        "raw_rows": diagnostic.raw_rows, "usable_mark_count": diagnostic.usable_mark_count,
        "candidates": diagnostic.candidates, "rejection_counts": dict(diagnostic.rejection_counts),
    })
    return uuid5(NAMESPACE_URL, f"stockscreener:normalization-diagnostic:{cohort_id}:{stage_run_id}:{content}")


def persist_normalization_diagnostics(
    cursor,
    cohort_id: UUID,
    diagnostics: Iterable[PreSessionNormalizationDiagnostics],
    *,
    created_at: datetime,
    stage_run_id: UUID | None = None,
    request_ids_by_underlyer: dict[str, Sequence[UUID]] | None = None,
) -> int:
    """Insert diagnostics idempotently; return the number of rows newly written."""
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise ValueError("diagnostic created_at must be timezone-aware")
    request_ids = request_ids_by_underlyer or {}
    written = 0
    for diagnostic in diagnostics:
        cursor.execute(
            f"INSERT INTO rt.normalization_diagnostics ({_COLUMNS}) VALUES "
            "(%s, %s, %s, %s, %s::uuid[], %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (diagnostic_id, created_at) DO NOTHING",
            (
                str(diagnostic_id(cohort_id, stage_run_id, diagnostic)), str(cohort_id),
                str(stage_run_id) if stage_run_id else None, diagnostic.underlyer,
                [str(value) for value in request_ids.get(diagnostic.underlyer, ())],
                diagnostic.requested_from, diagnostic.requested_to, diagnostic.returned_from,
                diagnostic.returned_to, diagnostic.bar_count, diagnostic.pages_fetched,
                diagnostic.pagination_complete, diagnostic.bar_acquisition, diagnostic.price_basis,
                diagnostic.observation_min, diagnostic.observation_max, diagnostic.raw_rows,
                diagnostic.usable_mark_count, diagnostic.candidates, Json(dict(diagnostic.rejection_counts)),
                Json([]), created_at,
            ),
        )
        written += cursor.rowcount
    return written


def load_normalization_diagnostics(cursor, cohort_id: UUID, underlyer: str, *,
                                   limit: int = 20) -> list[dict[str, object]]:
    if not 1 <= limit <= 500:
        raise ValueError("diagnostic read limit must be in [1, 500]")
    cursor.execute(
        "SELECT underlyer, bar_acquisition, bar_count, pages_fetched, pagination_complete, price_basis, raw_rows, "
        "usable_mark_count, candidates, rejection_counts, created_at FROM rt.normalization_diagnostics "
        "WHERE cohort_id = %s AND underlyer = %s ORDER BY created_at DESC LIMIT %s",
        (str(cohort_id), underlyer.upper(), limit))
    names = ("underlyer", "bar_acquisition", "bar_count", "pages_fetched", "pagination_complete", "price_basis",
             "raw_rows", "usable_mark_count", "candidates", "rejection_counts", "created_at")
    return [dict(zip(names, row)) for row in cursor.fetchall()]
