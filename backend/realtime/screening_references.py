"""Screening generations by reference (V2-9 follow-up): what a stored generation keeps of its bars and how a
reader verifies and recomposes them.

A stored generation's manifest used to list every source bar (5 MB for nine names, about 226 MB for 400).
It now carries, per member, the lineage heads of the daily bars its row was computed from and of the
hourly window behind its hourly context, plus a few continuity chains and identities (see
`screening_projection._daily_lineage`). The bars themselves stay reachable through `rt.bar_current`: a
reader recomputes each head over the bars current inside its recorded range and compares, exactly as the
Ticker Workspace reader does for its charts, and a head that no longer matches fails closed. This module is
pure: it builds the lookups the repository executes and checks what they returned; it runs no SQL.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from uuid import UUID

from .contracts import canonical_sha256
from .lineage_head import revision_chain_sha256
from .ticker_references import BarLineageChanged, verify_chart_group


def is_compact(manifest: Mapping[str, object]) -> bool:
    """True for a generation stored with lineage heads (older generations listed every bar)."""
    return isinstance(manifest.get("lineage_contract"), Mapping)


def _head(head: Mapping[str, object] | None, *, name: str) -> Mapping[str, Mapping[str, object]]:
    """The groups of a stored lineage head, after checking the head's own digest."""
    if head is None:
        return {}
    groups = head["groups"]
    if canonical_sha256(groups) != head["sha256"] or head["bars"] != sum(group["count"] for group in groups.values()):
        raise ValueError(f"Screening {name} lineage head identity drift")
    return groups


def _range(group: Mapping[str, object]) -> tuple[datetime, datetime]:
    return datetime.fromisoformat(group["first_bar_start"]), datetime.fromisoformat(group["last_bar_end"])


def bar_targets(payload: Mapping[str, object], security_ids=None):
    """The (instrument, security, ticker, group key, first bar start, last bar end) lookups that recompute
    every stored head of the requested members (all of them by default)."""
    tickers = {row["security_id"]: row["ticker"] for row in payload["rows"]}
    targets = []
    for security_id, record in payload["lineage"].items():
        if security_ids is not None and security_id not in security_ids:
            continue
        hourly = payload["hourly_lineage"][security_id]["bar_lineage"]
        for head, name in ((record["bar_lineage"], "daily"), (hourly, "hourly")):
            for key, group in _head(head, name=name).items():
                first, last = _range(group)
                targets.append((record["instrument_id"], UUID(security_id), tickers[security_id], key, first, last))
    return targets


def _chain(rows: Sequence[Mapping[str, object]]) -> str:
    return revision_chain_sha256((row["bar_revision_id"], row["payload_sha256"]) for row in rows)


def verify_member(payload: Mapping[str, object], security_id: str, rows_by_group, row=None) -> None:
    """Recompute one member's heads (and the chains and ids derived from them) over the bars read."""
    record, hourly = payload["lineage"][security_id], payload["hourly_lineage"][security_id]
    for head, name in ((record["bar_lineage"], "daily"), (hourly["bar_lineage"], "hourly")):
        for key, group in _head(head, name=name).items():
            verify_chart_group(group, rows_by_group.get((security_id, key), ()))
    adjusted = rows_by_group[(security_id, "1d:ADJUSTED")]
    raw = rows_by_group[(security_id, "1d:RAW_UNADJUSTED")]
    bars = rows_by_group.get((security_id, "1h:RAW_UNADJUSTED"), ())
    if row is None:
        row = next(row for row in payload["rows"] if row["security_id"] == security_id)
    if (
        _chain(adjusted[1:]) != record["adjusted_without_oldest_sha256"]
        or _chain(adjusted[:-1]) != record["adjusted_without_latest_sha256"]
        or str(raw[-1]["bar_revision_id"]) != record["current_quote_bar_id"]
        or str(raw[-2]["bar_revision_id"]) != record["previous_quote_bar_id"]
        or row["source_bar_id"] != record["current_quote_bar_id"]
        or hourly["selected_bar_id"] not in (None, str(bars[-1]["bar_revision_id"]) if bars else None)
    ):
        raise BarLineageChanged("Screening bar lineage changed since publication")


def verify_bar_lineage(payload: Mapping[str, object], rows_by_group, security_ids=None) -> None:
    """Fail closed unless every requested member's bars are exactly the ones its generation was built from."""
    rows = {row["security_id"]: row for row in payload["rows"]}
    for security_id in payload["lineage"]:
        if security_ids is None or security_id in security_ids:
            verify_member(payload, security_id, rows_by_group, rows[security_id])


def compose_hourly_lineage(
    payload: Mapping[str, object], security_id: str, rows_by_group,
) -> dict[str, object]:
    """The per-bar hourly lineage a stored generation used to embed, recomposed from the verified bars."""
    hourly = payload["hourly_lineage"][security_id]
    rows = rows_by_group.get((security_id, "1h:RAW_UNADJUSTED"), ())
    verify_member(payload, security_id, rows_by_group)
    return {
        "bars": [str(row["bar_revision_id"]) for row in rows],
        "selected_bar_id": hourly["selected_bar_id"],
        "reconstructed_from_30m": {},
        "derived_source_revision_ids": {
            str(row["bar_revision_id"]): [str(source) for source in row["source_revision_ids"]] for row in rows
        },
    }
