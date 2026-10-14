"""Ticker Workspace payloads by reference (V2-9 follow-up): the compact stored form and its composition.

A stored hot workspace carries `chart_refs` and `evidence_index` instead of the chart points and the
evidence rows (1.76 MB per ticker-cycle while they were embedded, about 60 KB without). The bodies are
composed on read from `rt.bar_current` and `rt.equity_evidence` as of the release lineage head and
verified against it. This module holds what both halves share: the compact form the producer stores, the
embedded shapes the reader recreates, and the pure verification of what a read returned. It runs no SQL.
"""
from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from uuid import UUID

from .contracts import canonical_sha256
from .lineage_head import revision_chain_sha256

CHART_ADJUSTED_INTERVALS = frozenset({"1d", "1wk", "1mo"})


class BarLineageChanged(LookupError):
    """The bars now current for a published chart no longer match the lineage head it was published with."""


def chart_basis(interval: str) -> str:
    return "ADJUSTED" if interval in CHART_ADJUSTED_INTERVALS else "RAW_UNADJUSTED"


def chart_group_key(interval: str) -> str:
    """The lineage-head group (`interval:basis`) a chart interval is the whole retained history of."""
    return f"{interval}:{chart_basis(interval)}"


def evidence_ref(row) -> dict[str, str]:
    """What a stored workspace keeps of one evidence row: enough to resolve it and to pin its content."""
    return dict(
        evidence_id=str(row.evidence_id), evidence_type=row.evidence_type.value,
        payload_sha256=row.payload_sha256,
    )


def _value(value):
    return getattr(value, "value", value)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def embedded_evidence_row(
    *, evidence_id, payload_sha256, payload, source_revision_ids, market_time, observed_at, quality_codes,
    evidence_type, analysis_run_id, security_revision_id, latest_bar_revision_id, source_name, source_version,
    payload_schema_version, valid_until, quality_state, evidence_role, fundamental_report_ids,
) -> dict[str, object]:
    """One evidence row in the shape the workspace payload embedded before it was served by reference."""
    return {
        "evidence_id": str(evidence_id), "payload_sha256": payload_sha256, "payload": payload,
        "source_revision_ids": [str(value) for value in source_revision_ids],
        "market_time": _iso(market_time), "observed_at": _iso(observed_at),
        "quality_codes": list(quality_codes), "evidence_type": _value(evidence_type),
        "analysis_run_id": str(analysis_run_id), "security_revision_id": str(security_revision_id),
        "latest_bar_revision_id": str(latest_bar_revision_id), "source_name": source_name,
        "source_version": source_version, "payload_schema_version": payload_schema_version,
        "valid_until": _iso(valid_until) if valid_until else None,
        "quality_state": _value(quality_state), "evidence_role": _value(evidence_role),
        "fundamental_report_ids": [str(value) for value in fundamental_report_ids],
    }


def embedded_evidence_row_of(row) -> dict[str, object]:
    """The embedded shape of a materialized `EquityEvidence` row (profiling and equivalence checks)."""
    return embedded_evidence_row(
        evidence_id=row.evidence_id, payload_sha256=row.payload_sha256, payload=json.loads(row.payload_json),
        source_revision_ids=row.source_revision_ids, market_time=row.market_time, observed_at=row.observed_at,
        quality_codes=row.quality_codes, evidence_type=row.evidence_type, analysis_run_id=row.analysis_run_id,
        security_revision_id=row.security_revision_id, latest_bar_revision_id=row.latest_bar_revision_id,
        source_name=row.source_name, source_version=row.source_version,
        payload_schema_version=row.payload_schema_version, valid_until=row.valid_until,
        quality_state=row.quality_state, evidence_role=row.evidence_role,
        fundamental_report_ids=row.fundamental_report_ids,
    )


def chart_points(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    """Chart points of bar rows already ordered by bar start; the arithmetic the producer applies to facts."""
    return [
        dict(
            time=int(row["bar_start"].timestamp()), open=round(float(row["open_price"]), 2),
            high=round(float(row["high_price"]), 2), low=round(float(row["low_price"]), 2),
            close=round(float(row["close_price"]), 2), volume=int(row["volume"]),
        ) for row in rows
    ]


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value)


def chart_group_ranges(manifest: Mapping[str, object], chart_refs: Mapping[str, str]):
    """The (first bar start, last bar end) of every chart's lineage-head group, after checking the head's
    own digest. A reference to a group the head does not carry is an identity drift."""
    lineage = manifest["bar_lineage"]
    groups = lineage["groups"]
    if canonical_sha256(groups) != lineage["sha256"]:
        raise ValueError("Ticker bar lineage head identity drift")
    ranges = {}
    for interval, key in chart_refs.items():
        head = groups.get(key)
        if key != chart_group_key(interval) or head is None:
            raise ValueError("Ticker chart reference identity drift")
        ranges[key] = (_instant(head["first_bar_start"]), _instant(head["last_bar_end"]))
    return ranges


def verify_chart_group(head: Mapping[str, object], rows: Sequence[Mapping[str, object]]) -> None:
    """Recompute the group head over the rows that were read and compare it with the published one."""
    if (
        not rows or len(rows) != head["count"]
        or str(rows[0]["bar_revision_id"]) != head["first_revision_id"]
        or str(rows[-1]["bar_revision_id"]) != head["last_revision_id"]
        or rows[0]["bar_start"] != _instant(head["first_bar_start"])
        or rows[-1]["bar_end"] != _instant(head["last_bar_end"])
        or revision_chain_sha256(
            (row["bar_revision_id"], row["payload_sha256"]) for row in rows
        ) != head["chain_sha256"]
    ):
        raise BarLineageChanged("Ticker bar lineage changed since publication")


def compose_charts(
    manifest: Mapping[str, object], chart_refs: Mapping[str, str], chart_market_times: Mapping[str, str],
    rows_by_group: Mapping[str, Sequence[Mapping[str, object]]],
) -> dict[str, list[dict[str, object]]]:
    """The chart bodies the payload used to embed, verified against the manifest's lineage head."""
    groups = manifest["bar_lineage"]["groups"]
    charts = {}
    for interval, key in chart_refs.items():
        head, rows = groups[key], rows_by_group.get(key, ())
        verify_chart_group(head, rows)
        if _instant(chart_market_times[interval]) != _instant(head["last_bar_end"]):
            raise ValueError("Ticker chart market time identity drift")
        charts[interval] = chart_points(rows)
    return charts


def evidence_expectations(evidence_index: Mapping[str, Sequence[Mapping[str, str]]]) -> dict[UUID, tuple[str, str]]:
    """evidence id -> (type, payload hash) the stored index pins; one id never carries two identities."""
    expected: dict[UUID, tuple[str, str]] = {}
    for rows in evidence_index.values():
        for ref in rows:
            identity = (ref["evidence_type"], ref["payload_sha256"])
            if expected.setdefault(UUID(ref["evidence_id"]), identity) != identity:
                raise ValueError("Ticker evidence reference identity conflict")
    return expected


def compose_evidence(
    evidence_index: Mapping[str, Sequence[Mapping[str, str]]], bodies: Mapping[UUID, Mapping[str, object]],
) -> dict[str, list[dict[str, object]]]:
    """The evidence mapping the payload used to embed (identity -> rows in publication order)."""
    return {
        identity: [embedded_evidence_row(**bodies[UUID(ref["evidence_id"])]) for ref in refs]
        for identity, refs in evidence_index.items()
    }
